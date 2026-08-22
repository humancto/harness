//! `FanoutController` — bounded streaming dispatch (roadmap 4.1,
//! PRD §14.7, ADR-0023).
//!
//! *Never materialize all sub-tasks.* The controller pulls items from a
//! lazy source only when a window slot frees up, so at most
//! `window` runner futures — and therefore at most `window` sub-task
//! rows, when the runner submits inside its future — exist at once.
//! Memory and store pressure are O(window), not O(total).
//!
//! The controller is deliberately **pure**: no store, no leases, no
//! QUIC, no runtime handle. It bounds futures; the caller's runner
//! closure decides what a "sub-task" is (an in-process call, a store
//! row + terminal await, …). The deadline is a caller-supplied future
//! so the crate stays runtime-agnostic.
//!
//! Shape: [`FanoutController::stream`] returns a pull-based
//! [`futures::Stream`]. Backpressure is structural — a consumer that
//! stops polling stops refill (pre-work for 4.7) — and cancellation is
//! `Drop`: dropping the stream drops the in-flight futures.

use std::future::Future as _;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::future::BoxFuture;
use futures::stream::{BoxStream, FuturesUnordered, Stream, StreamExt as _};
use harness_core::PartialPolicy;

/// Window sizing policy (ADR-0023).
#[derive(Debug, Clone, Copy)]
pub enum WindowPolicy {
    /// A fixed window, floored at 1 (a zero window could never make
    /// progress).
    Fixed(usize),
    /// PRD §14.7's `factor × N_workers`, clamped to `[min, max]`.
    PerWorkers { factor: usize, min: usize, max: usize },
}

impl WindowPolicy {
    /// The §14.7 default: `2 × N_workers`, clamped to `[4, 64]`.
    /// min = 4 keeps single-worker fan-outs from serializing; max = 64
    /// matches the per-peer outbound queue bound (ADR-0017) and the
    /// mesh-meta target cap.
    #[must_use]
    pub const fn default_per_workers() -> Self {
        Self::PerWorkers {
            factor: 2,
            min: 4,
            max: 64,
        }
    }

    /// Effective window for a live-worker count. Never returns 0.
    #[must_use]
    pub fn effective(self, workers: usize) -> usize {
        match self {
            Self::Fixed(n) => n.max(1),
            Self::PerWorkers { factor, min, max } => {
                let min = min.max(1);
                let max = max.max(min);
                factor.saturating_mul(workers).clamp(min, max)
            }
        }
    }
}

/// Terminal outcome of one sub-task, as reported by the runner future.
#[derive(Debug, Clone)]
pub enum ItemOutcome<O> {
    Ok(O),
    Failed(String),
    TimedOut,
}

/// One event from a running fan-out.
#[derive(Debug, Clone)]
pub enum FanoutEvent<O> {
    /// One sub-task reached a terminal outcome. `index` is the item's
    /// position in source order — callers resolve provenance (which
    /// item this was) against their own materialized item list.
    Item { index: u64, outcome: ItemOutcome<O> },
    /// The fan-out is finished. Always the final event, exactly once.
    End(FanoutSummary),
}

/// Why the fan-out ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndReason {
    /// The source ran dry and every pulled item completed.
    SourceDrained,
    /// `PartialPolicy::FailFast` and an item failed or timed out.
    FailedFast,
    /// The caller-supplied deadline resolved.
    DeadlineExceeded,
}

/// Final accounting for one fan-out.
#[derive(Debug, Clone)]
pub struct FanoutSummary {
    /// Items pulled from the source (== runner futures created).
    pub pulled: u64,
    pub ok: u64,
    pub failed: u64,
    pub timed_out: u64,
    /// In-flight runner futures dropped by `FailFast` or the deadline.
    /// Their side effects (e.g. remote sub-task rows) keep running to
    /// their own timeouts — the ADR-0022 orphan rule bounds that.
    pub dropped_in_flight: u64,
    pub reason: EndReason,
}

/// Everything needed to run one bounded fan-out.
///
/// The runner MUST perform its side effects (row insertion, dispatch)
/// inside the returned future, not inside the closure body — that is
/// what makes the window bound the store, not just memory. Runner
/// futures must also tolerate being dropped mid-flight (cancellation
/// is `Drop`).
pub struct FanoutSpec<I, O> {
    /// Lazy item source. Iterator-shaped callers use
    /// `futures::stream::iter(..)`; async sources may yield `Pending`.
    pub source: BoxStream<'static, I>,
    /// Spawns one sub-task. Called with the item's source-order index.
    pub run: Box<dyn FnMut(u64, I) -> BoxFuture<'static, ItemOutcome<O>> + Send>,
    pub window: WindowPolicy,
    /// Live-worker count, re-read on every refill attempt so the window
    /// tracks nodes joining/leaving. Snapshot callers pass a constant.
    pub live_workers: Box<dyn Fn() -> usize + Send>,
    /// Optional global deadline (e.g. `Box::pin(tokio::time::sleep(..))`).
    pub deadline: Option<BoxFuture<'static, ()>>,
    /// `FailFast` → the first `Failed`/`TimedOut` item ends the fan-out
    /// and drops all in-flight work. `ReturnPartial` continues past
    /// failures. `Wait` is aliased to `ReturnPartial` until 4.5 defines
    /// federated wait semantics (ADR-0023).
    pub on_failure: PartialPolicy,
}

impl<I, O> std::fmt::Debug for FanoutSpec<I, O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FanoutSpec")
            .field("window", &self.window)
            .field("on_failure", &self.on_failure)
            .finish_non_exhaustive()
    }
}

/// Entry point for bounded fan-outs.
#[derive(Debug)]
pub struct FanoutController;

impl FanoutController {
    /// Run the spec as an event stream. State is O(window): at most
    /// `window` boxed runner futures plus counters. Dropping the stream
    /// cancels all in-flight runner futures and the source.
    pub fn stream<I, O>(spec: FanoutSpec<I, O>) -> FanoutStream<I, O>
    where
        I: Send + 'static,
        O: Send + 'static,
    {
        FanoutStream {
            source: spec.source,
            source_done: false,
            run: spec.run,
            in_flight: FuturesUnordered::new(),
            window: spec.window,
            live_workers: spec.live_workers,
            deadline: spec.deadline,
            fail_fast: matches!(spec.on_failure, PartialPolicy::FailFast),
            next_index: 0,
            ok: 0,
            failed: 0,
            timed_out: 0,
            dropped_in_flight: 0,
            pending_end: None,
            finished: false,
        }
    }
}

/// The event stream returned by [`FanoutController::stream`].
///
/// All fields are `Unpin` (`BoxStream`, `FuturesUnordered`, boxed
/// futures), so the hand-rolled [`Stream`] impl needs no projection.
pub struct FanoutStream<I, O> {
    source: BoxStream<'static, I>,
    source_done: bool,
    run: Box<dyn FnMut(u64, I) -> BoxFuture<'static, ItemOutcome<O>> + Send>,
    in_flight: FuturesUnordered<BoxFuture<'static, (u64, ItemOutcome<O>)>>,
    window: WindowPolicy,
    live_workers: Box<dyn Fn() -> usize + Send>,
    deadline: Option<BoxFuture<'static, ()>>,
    fail_fast: bool,
    next_index: u64,
    ok: u64,
    failed: u64,
    timed_out: u64,
    dropped_in_flight: u64,
    pending_end: Option<FanoutSummary>,
    finished: bool,
}

impl<I, O> std::fmt::Debug for FanoutStream<I, O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FanoutStream")
            .field("in_flight", &self.in_flight.len())
            .field("pulled", &self.next_index)
            .field("finished", &self.finished)
            .finish_non_exhaustive()
    }
}

impl<I, O> FanoutStream<I, O> {
    fn summary(&self, reason: EndReason) -> FanoutSummary {
        FanoutSummary {
            pulled: self.next_index,
            ok: self.ok,
            failed: self.failed,
            timed_out: self.timed_out,
            dropped_in_flight: self.dropped_in_flight,
            reason,
        }
    }

    /// Drop every in-flight future (cancelling it) and fuse the source.
    fn abandon_in_flight(&mut self) {
        self.dropped_in_flight += u64::try_from(self.in_flight.len()).unwrap_or(u64::MAX);
        self.in_flight.clear();
        self.source_done = true;
    }
}

impl<I, O> Stream for FanoutStream<I, O>
where
    I: Send + 'static,
    O: Send + 'static,
{
    type Item = FanoutEvent<O>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.finished {
            return Poll::Ready(None);
        }
        // A queued terminal End wins over everything — including a
        // deadline that fired between the fatal Item and this poll
        // (review MINOR-2: the summary must keep the FailFast reason
        // and drop count).
        if let Some(summary) = this.pending_end.take() {
            this.finished = true;
            return Poll::Ready(Some(FanoutEvent::End(summary)));
        }
        if let Some(deadline) = this.deadline.as_mut() {
            if deadline.as_mut().poll(cx).is_ready() {
                this.deadline = None; // never poll a resolved future again
                this.abandon_in_flight();
                this.finished = true;
                return Poll::Ready(Some(FanoutEvent::End(
                    this.summary(EndReason::DeadlineExceeded),
                )));
            }
        }
        // Refill: pull while under the (recomputed) window. "Refill on
        // completion" is literally the consumer's next poll after an
        // Item event lands here.
        while !this.source_done {
            let window = this.window.effective((this.live_workers)());
            if this.in_flight.len() >= window {
                break;
            }
            match this.source.poll_next_unpin(cx) {
                Poll::Ready(Some(item)) => {
                    let index = this.next_index;
                    this.next_index += 1;
                    let fut = (this.run)(index, item);
                    this.in_flight
                        .push(Box::pin(async move { (index, fut.await) }));
                }
                Poll::Ready(None) => this.source_done = true,
                // Source waker registered; in-flight polls below still run.
                Poll::Pending => break,
            }
        }
        match this.in_flight.poll_next_unpin(cx) {
            Poll::Ready(Some((index, outcome))) => {
                let fatal = match &outcome {
                    ItemOutcome::Ok(_) => {
                        this.ok += 1;
                        false
                    }
                    ItemOutcome::Failed(_) => {
                        this.failed += 1;
                        this.fail_fast
                    }
                    ItemOutcome::TimedOut => {
                        this.timed_out += 1;
                        this.fail_fast
                    }
                };
                if fatal {
                    this.abandon_in_flight();
                    this.pending_end = Some(this.summary(EndReason::FailedFast));
                }
                Poll::Ready(Some(FanoutEvent::Item { index, outcome }))
            }
            Poll::Ready(None) | Poll::Pending => {
                if this.source_done && this.in_flight.is_empty() {
                    this.finished = true;
                    return Poll::Ready(Some(FanoutEvent::End(
                        this.summary(EndReason::SourceDrained),
                    )));
                }
                Poll::Pending
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use futures::channel::oneshot;
    use futures::task::noop_waker;
    use futures::FutureExt as _;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// A controllable fake sub-task: resolves when the test fires its
    /// oneshot; counts live instances via a drop guard.
    struct Guard(Arc<AtomicUsize>);
    impl Drop for Guard {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    struct Harness {
        /// Senders for not-yet-released items, keyed by index.
        gates: Arc<parking_lot::Mutex<Vec<(u64, oneshot::Sender<ItemOutcome<u64>>)>>>,
        /// Live (spawned, not yet dropped) runner futures.
        live: Arc<AtomicUsize>,
        /// Peak of `live`.
        peak: Arc<AtomicUsize>,
        pulled: Arc<AtomicUsize>,
    }

    impl Harness {
        fn new() -> Self {
            Self {
                gates: Arc::new(parking_lot::Mutex::new(Vec::new())),
                live: Arc::new(AtomicUsize::new(0)),
                peak: Arc::new(AtomicUsize::new(0)),
                pulled: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn runner(&self) -> Box<dyn FnMut(u64, u64) -> BoxFuture<'static, ItemOutcome<u64>> + Send>
        {
            let gates = self.gates.clone();
            let live = self.live.clone();
            let peak = self.peak.clone();
            let pulled = self.pulled.clone();
            Box::new(move |index, _item| {
                pulled.fetch_add(1, Ordering::SeqCst);
                let n = live.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(n, Ordering::SeqCst);
                let (tx, rx) = oneshot::channel();
                gates.lock().push((index, tx));
                let guard = Guard(live.clone());
                Box::pin(async move {
                    let _guard = guard;
                    rx.await.unwrap_or(ItemOutcome::Failed("gate dropped".into()))
                })
            })
        }

        /// Release the oldest gated item with the given outcome.
        fn release_oldest(&self, outcome: ItemOutcome<u64>) -> u64 {
            let (index, tx) = self.gates.lock().remove(0);
            tx.send(outcome).ok().expect("receiver alive");
            index
        }

        fn gated(&self) -> usize {
            self.gates.lock().len()
        }
    }

    fn spec_over(
        n: u64,
        h: &Harness,
        window: WindowPolicy,
        on_failure: PartialPolicy,
    ) -> FanoutSpec<u64, u64> {
        FanoutSpec {
            source: futures::stream::iter(0..n).boxed(),
            run: h.runner(),
            window,
            live_workers: Box::new(|| 1),
            deadline: None,
            on_failure,
        }
    }

    /// Poll once with a noop waker.
    fn poll<O>(s: &mut FanoutStream<u64, O>) -> Poll<Option<FanoutEvent<O>>>
    where
        O: Send + 'static,
    {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        Pin::new(s).poll_next(&mut cx)
    }

    /// Drain all currently-ready events.
    fn drain<O>(s: &mut FanoutStream<u64, O>) -> Vec<FanoutEvent<O>>
    where
        O: Send + 'static,
    {
        let mut out = Vec::new();
        while let Poll::Ready(Some(ev)) = poll(s) {
            out.push(ev);
        }
        out
    }

    #[test]
    fn t01_window_math() {
        let d = WindowPolicy::default_per_workers();
        assert_eq!(d.effective(0), 4); // clamped to min — progress guaranteed
        assert_eq!(d.effective(1), 4);
        assert_eq!(d.effective(2), 4);
        assert_eq!(d.effective(3), 6);
        assert_eq!(d.effective(5), 10);
        assert_eq!(d.effective(32), 64);
        assert_eq!(d.effective(1000), 64); // clamped to max
        assert_eq!(WindowPolicy::Fixed(0).effective(99), 1); // never zero
        assert_eq!(WindowPolicy::Fixed(7).effective(0), 7);
        // Degenerate min > max: max wins the floor, still non-zero.
        let weird = WindowPolicy::PerWorkers {
            factor: 1,
            min: 10,
            max: 2,
        };
        assert_eq!(weird.effective(100), 10);
    }

    #[test]
    fn t02_o_window_at_scale_10k() {
        let h = Harness::new();
        let total: u64 = 10_000;
        let mut s = FanoutController::stream(spec_over(
            total,
            &h,
            WindowPolicy::Fixed(4),
            PartialPolicy::ReturnPartial,
        ));
        let mut items = 0u64;
        let mut end = None;
        loop {
            for ev in drain(&mut s) {
                match ev {
                    FanoutEvent::Item { .. } => items += 1,
                    FanoutEvent::End(sum) => end = Some(sum),
                }
            }
            if end.is_some() {
                break;
            }
            // Invariants at every step: never more than window live, and
            // the source cursor never runs ahead of completions + window.
            assert!(h.live.load(Ordering::SeqCst) <= 4);
            let pulled = h.pulled.load(Ordering::SeqCst) as u64;
            assert!(pulled <= items + 4, "pulled {pulled} vs completed {items}");
            h.release_oldest(ItemOutcome::Ok(0));
        }
        let sum = end.expect("End");
        assert_eq!(items, total);
        assert_eq!(sum.pulled, total);
        assert_eq!(sum.ok, total);
        assert_eq!(sum.reason, EndReason::SourceDrained);
        assert_eq!(h.peak.load(Ordering::SeqCst), 4);
        assert!(matches!(poll(&mut s), Poll::Ready(None)));
    }

    #[test]
    fn t03_refill_ordering_lockstep() {
        let h = Harness::new();
        let mut s = FanoutController::stream(spec_over(
            10,
            &h,
            WindowPolicy::Fixed(3),
            PartialPolicy::ReturnPartial,
        ));
        assert!(matches!(poll(&mut s), Poll::Pending));
        assert_eq!(h.pulled.load(Ordering::SeqCst), 3);
        for step in 0..7u64 {
            let released = h.release_oldest(ItemOutcome::Ok(0));
            assert_eq!(released, step, "indices pulled in source order");
            let evs = drain(&mut s);
            assert_eq!(evs.len(), 1);
            let FanoutEvent::Item { index, .. } = &evs[0] else {
                panic!("expected Item");
            };
            assert_eq!(*index, step);
            // Exactly one new pull per completion.
            assert_eq!(h.pulled.load(Ordering::SeqCst) as u64, 4 + step);
        }
    }

    #[test]
    fn t04_dynamic_window_grow_and_shrink() {
        let h = Harness::new();
        let workers = Arc::new(AtomicUsize::new(2));
        let w = workers.clone();
        let spec = FanoutSpec {
            source: futures::stream::iter(0..100u64).boxed(),
            run: h.runner(),
            window: WindowPolicy::PerWorkers {
                factor: 2,
                min: 1,
                max: 64,
            },
            live_workers: Box::new(move || w.load(Ordering::SeqCst)),
            deadline: None,
            on_failure: PartialPolicy::ReturnPartial,
        };
        let mut s = FanoutController::stream(spec);
        assert!(matches!(poll(&mut s), Poll::Pending));
        assert_eq!(h.live.load(Ordering::SeqCst), 4); // 2×2
        workers.store(4, Ordering::SeqCst);
        assert!(matches!(poll(&mut s), Poll::Pending));
        assert_eq!(h.live.load(Ordering::SeqCst), 8); // grew on next refill
        workers.store(1, Ordering::SeqCst);
        // Shrink is lazy: nothing cancelled…
        assert!(matches!(poll(&mut s), Poll::Pending));
        assert_eq!(h.live.load(Ordering::SeqCst), 8);
        // …and no refill until we drain below 2×1 = 2.
        for _ in 0..6 {
            h.release_oldest(ItemOutcome::Ok(0));
            let _ = drain(&mut s);
        }
        assert_eq!(h.live.load(Ordering::SeqCst), 2);
        assert_eq!(h.pulled.load(Ordering::SeqCst), 8); // still no new pulls
        h.release_oldest(ItemOutcome::Ok(0));
        let _ = drain(&mut s);
        assert_eq!(h.pulled.load(Ordering::SeqCst), 9); // refill resumed at window 2
    }

    #[test]
    fn t05_fail_fast_drops_in_flight_and_stops_pulling() {
        let h = Harness::new();
        let mut s = FanoutController::stream(spec_over(
            50,
            &h,
            WindowPolicy::Fixed(4),
            PartialPolicy::FailFast,
        ));
        assert!(matches!(poll(&mut s), Poll::Pending));
        h.release_oldest(ItemOutcome::Ok(1));
        let _ = drain(&mut s);
        h.release_oldest(ItemOutcome::Failed("boom".into()));
        let evs = drain(&mut s);
        assert_eq!(evs.len(), 2, "fatal Item then End");
        assert!(matches!(
            &evs[0],
            FanoutEvent::Item {
                outcome: ItemOutcome::Failed(_),
                ..
            }
        ));
        let FanoutEvent::End(sum) = &evs[1] else {
            panic!("expected End");
        };
        assert_eq!(sum.reason, EndReason::FailedFast);
        assert_eq!(sum.ok, 1);
        assert_eq!(sum.failed, 1);
        // 5 pulled (4 + 1 refill), 2 completed → 3 dropped in flight.
        assert_eq!(sum.dropped_in_flight, 3);
        assert_eq!(h.live.load(Ordering::SeqCst), 0, "drop guards fired");
        assert_eq!(h.pulled.load(Ordering::SeqCst), 5, "source not pulled further");
        assert!(matches!(poll(&mut s), Poll::Ready(None)));
    }

    #[test]
    fn t06_return_partial_continues_past_failures() {
        let h = Harness::new();
        let mut s = FanoutController::stream(spec_over(
            6,
            &h,
            WindowPolicy::Fixed(2),
            PartialPolicy::ReturnPartial,
        ));
        let mut end = None;
        let mut items = 0;
        let mut flip = false;
        loop {
            for ev in drain(&mut s) {
                match ev {
                    FanoutEvent::Item { .. } => items += 1,
                    FanoutEvent::End(sum) => end = Some(sum),
                }
            }
            if end.is_some() {
                break;
            }
            let outcome = if flip {
                ItemOutcome::Failed("x".into())
            } else {
                ItemOutcome::Ok(0)
            };
            flip = !flip;
            h.release_oldest(outcome);
        }
        let sum = end.expect("End");
        assert_eq!(items, 6);
        assert_eq!(sum.ok, 3);
        assert_eq!(sum.failed, 3);
        assert_eq!(sum.reason, EndReason::SourceDrained);
    }

    #[test]
    fn t07_wait_policy_behaves_like_return_partial() {
        let h = Harness::new();
        let mut s = FanoutController::stream(spec_over(
            3,
            &h,
            WindowPolicy::Fixed(3),
            PartialPolicy::Wait,
        ));
        assert!(matches!(poll(&mut s), Poll::Pending));
        h.release_oldest(ItemOutcome::Failed("x".into()));
        let evs = drain(&mut s);
        // Failure is not fatal under Wait (aliased to ReturnPartial).
        assert_eq!(evs.len(), 1);
        assert!(matches!(evs[0], FanoutEvent::Item { .. }));
        assert_eq!(h.gated(), 2, "remaining items still in flight");
    }

    #[test]
    fn t08_cancellation_drains_on_drop() {
        let h = Harness::new();
        let mut s = FanoutController::stream(spec_over(
            50,
            &h,
            WindowPolicy::Fixed(5),
            PartialPolicy::ReturnPartial,
        ));
        assert!(matches!(poll(&mut s), Poll::Pending));
        assert_eq!(h.live.load(Ordering::SeqCst), 5);
        drop(s);
        assert_eq!(h.live.load(Ordering::SeqCst), 0, "all drop guards fired");
    }

    #[test]
    fn t09_deadline_ends_with_drop_count() {
        let h = Harness::new();
        let (dl_tx, dl_rx) = oneshot::channel::<()>();
        let spec = FanoutSpec {
            source: futures::stream::iter(0..50u64).boxed(),
            run: h.runner(),
            window: WindowPolicy::Fixed(3),
            live_workers: Box::new(|| 1),
            deadline: Some(Box::pin(dl_rx.map(|_| ())) as BoxFuture<'static, ()>),
            on_failure: PartialPolicy::ReturnPartial,
        };
        let mut s = FanoutController::stream(spec);
        assert!(matches!(poll(&mut s), Poll::Pending));
        dl_tx.send(()).expect("deadline");
        let evs = drain(&mut s);
        assert_eq!(evs.len(), 1);
        let FanoutEvent::End(sum) = &evs[0] else {
            panic!("expected End");
        };
        assert_eq!(sum.reason, EndReason::DeadlineExceeded);
        assert_eq!(sum.dropped_in_flight, 3);
        assert_eq!(h.live.load(Ordering::SeqCst), 0);
        assert_eq!(h.pulled.load(Ordering::SeqCst), 3, "no pulls after deadline");
        assert!(matches!(poll(&mut s), Poll::Ready(None)));
    }

    #[test]
    fn t10_fail_fast_end_wins_over_deadline_race() {
        // MINOR-2: deadline fires between the fatal Item and the queued
        // End — the End must keep the FailFast reason and drop count.
        let h = Harness::new();
        let (dl_tx, dl_rx) = oneshot::channel::<()>();
        let spec = FanoutSpec {
            source: futures::stream::iter(0..10u64).boxed(),
            run: h.runner(),
            window: WindowPolicy::Fixed(4),
            live_workers: Box::new(|| 1),
            deadline: Some(Box::pin(dl_rx.map(|_| ())) as BoxFuture<'static, ()>),
            on_failure: PartialPolicy::FailFast,
        };
        let mut s = FanoutController::stream(spec);
        assert!(matches!(poll(&mut s), Poll::Pending));
        h.release_oldest(ItemOutcome::Failed("boom".into()));
        // Pull exactly the fatal Item; End is now queued.
        let Poll::Ready(Some(FanoutEvent::Item { .. })) = poll(&mut s) else {
            panic!("expected fatal Item");
        };
        dl_tx.send(()).expect("deadline"); // fires in the gap
        let Poll::Ready(Some(FanoutEvent::End(sum))) = poll(&mut s) else {
            panic!("expected End");
        };
        assert_eq!(sum.reason, EndReason::FailedFast);
        assert_eq!(sum.dropped_in_flight, 3);
    }

    #[test]
    fn t11_empty_source() {
        let h = Harness::new();
        let mut s = FanoutController::stream(spec_over(
            0,
            &h,
            WindowPolicy::Fixed(4),
            PartialPolicy::ReturnPartial,
        ));
        let Poll::Ready(Some(FanoutEvent::End(sum))) = poll(&mut s) else {
            panic!("expected immediate End");
        };
        assert_eq!(sum.pulled, 0);
        assert_eq!(sum.ok + sum.failed + sum.timed_out, 0);
        assert_eq!(sum.reason, EndReason::SourceDrained);
        assert!(matches!(poll(&mut s), Poll::Ready(None)));
    }

    /// A source that alternates Pending/Ready, waking via the real waker.
    struct StutterSource {
        remaining: u64,
        ready: bool,
    }
    impl Stream for StutterSource {
        type Item = u64;
        fn poll_next(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Option<Self::Item>> {
            if self.remaining == 0 {
                return Poll::Ready(None);
            }
            if self.ready {
                self.ready = false;
                self.remaining -= 1;
                Poll::Ready(Some(self.remaining))
            } else {
                self.ready = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }

    #[test]
    fn t12_source_slower_than_workers() {
        // Slow source: in_flight stays below window without busy-spin;
        // completion relies on the source's registered waker (counted).
        let h = Harness::new();
        let wake_count = Arc::new(AtomicUsize::new(0));
        struct CountWaker(Arc<AtomicUsize>);
        impl futures::task::ArcWake for CountWaker {
            fn wake_by_ref(arc_self: &Arc<Self>) {
                arc_self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let waker = futures::task::waker(Arc::new(CountWaker(wake_count.clone())));
        let spec = FanoutSpec {
            source: StutterSource {
                remaining: 5,
                ready: false,
            }
            .boxed(),
            run: h.runner(),
            window: WindowPolicy::Fixed(4),
            live_workers: Box::new(|| 1),
            deadline: None,
            on_failure: PartialPolicy::ReturnPartial,
        };
        let mut s = FanoutController::stream(spec);
        let mut cx = Context::from_waker(&waker);
        // First poll: source is Pending → 0 pulled, wake scheduled.
        assert!(Pin::new(&mut s).poll_next(&mut cx).is_pending());
        assert_eq!(h.pulled.load(Ordering::SeqCst), 0);
        assert!(wake_count.load(Ordering::SeqCst) > 0, "waker registered, not lost");
        // Each re-poll (as the wake requests) pulls exactly one more.
        for expected in 1..=4usize {
            assert!(Pin::new(&mut s).poll_next(&mut cx).is_pending());
            assert_eq!(h.pulled.load(Ordering::SeqCst), expected);
        }
        // Window full at 4: the 5th item is not pulled yet.
        assert!(Pin::new(&mut s).poll_next(&mut cx).is_pending());
        assert_eq!(h.pulled.load(Ordering::SeqCst), 4);
        // Complete everything.
        let mut done = 0;
        let mut ended = false;
        while !ended {
            if h.gated() > 0 {
                h.release_oldest(ItemOutcome::Ok(0));
            }
            while let Poll::Ready(Some(ev)) = Pin::new(&mut s).poll_next(&mut cx) {
                match ev {
                    FanoutEvent::Item { .. } => done += 1,
                    FanoutEvent::End(sum) => {
                        assert_eq!(sum.reason, EndReason::SourceDrained);
                        ended = true;
                    }
                }
            }
        }
        assert_eq!(done, 5);
    }

    #[test]
    fn t13_timed_out_counts_and_fail_fast_on_timeout() {
        let h = Harness::new();
        let mut s = FanoutController::stream(spec_over(
            5,
            &h,
            WindowPolicy::Fixed(2),
            PartialPolicy::FailFast,
        ));
        assert!(matches!(poll(&mut s), Poll::Pending));
        h.release_oldest(ItemOutcome::TimedOut);
        let evs = drain(&mut s);
        assert_eq!(evs.len(), 2);
        let FanoutEvent::End(sum) = &evs[1] else {
            panic!("expected End");
        };
        assert_eq!(sum.timed_out, 1);
        assert_eq!(sum.reason, EndReason::FailedFast);
    }
}
