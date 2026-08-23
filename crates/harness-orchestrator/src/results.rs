//! `task_results` — the PRD §14.8 caller-facing `Stream<TaskResult>`
//! over a [`FanoutStream`] (roadmap 4.2, ADR-0024).
//!
//! A pure mapping combinator: one `TaskResult::Partial` per completed
//! fan-out item (monotonic `seq` from 0, `progress = completed /
//! expected`), then exactly one summary `Partial` at `progress = 1.0`,
//! then termination. Backpressure and cancellation are inherited from
//! the pull-based inner stream — no channel, no spawned driver (that
//! bridge waits for a detached consumer; ADR-0024).
//!
//! The stream **never yields `TaskResult::Final`**: the signed
//! `FinalResult` stays executor-owned so there is exactly one terminal
//! authority per task. `sig` on emitted partials is zeroed — signing
//! happens at wire boundaries (the `PeerNet` sender), matching the
//! 3.2-stream partial pipeline.
//!
//! The mapper is a single recoverable object (not two closures) so a
//! driver like `mesh_meta::drive_fanout` can accumulate provenance
//! state across `on_item`/`on_end` and take it back with
//! [`TaskResultStream::into_mapper`] once the stream has terminated.

use std::pin::Pin;
use std::task::{Context, Poll};

use futures::stream::{Stream, StreamExt as _};
use harness_core::{NodeId, PartialResult, Signature, TaskId, TaskResult};
use serde_json::Value as JsonValue;

use crate::fanout::{FanoutEvent, FanoutStream, FanoutSummary, ItemOutcome};

/// Maps fan-out events to `output_chunk`s, accumulating whatever state
/// the caller needs along the way (provenance, successes, failures).
pub trait ResultMapper<O>: Send {
    /// One completed item → its `output_chunk`. Side effects (pushing
    /// into result accumulators, emitting to a progress sink) happen
    /// here; the caller recovers the mapper afterwards.
    fn on_item(&mut self, index: u64, outcome: &ItemOutcome<O>) -> JsonValue;
    /// The terminal summary → the final `output_chunk` (account for
    /// never-completed items here).
    fn on_end(&mut self, summary: &FanoutSummary) -> JsonValue;
}

/// Identity of the fan-out for stamping emitted partials.
#[derive(Debug, Clone, Copy)]
pub struct ResultCtx {
    pub task_id: TaskId,
    /// Node coordinating the fan-out.
    pub node_id: NodeId,
    /// Progress denominator when known up front. `None` (or 0) keeps
    /// per-item `progress` at 0.0 until the summary frame.
    pub expected: Option<u64>,
}

/// Wrap a fan-out into a `Stream<TaskResult>`.
pub fn task_results<I, O, M>(
    stream: FanoutStream<I, O>,
    ctx: ResultCtx,
    mapper: M,
) -> TaskResultStream<I, O, M>
where
    I: Send + 'static,
    O: Send + 'static,
    M: ResultMapper<O>,
{
    TaskResultStream {
        inner: stream,
        ctx,
        mapper,
        seq: 0,
        completed: 0,
        finished: false,
    }
}

/// Detach a [`TaskResultStream`] behind a bounded channel — the
/// spawned-driver + bounded-mpsc bridge ADR-0024 deferred until a
/// detached consumer existed (first consumer: the 4.5 federated
/// coordinator). Returns the driver future — the CALLER spawns it, so
/// this crate stays runtime-agnostic — and the receiving stream.
///
/// Backpressure is the bounded channel: the driver's `send` awaits
/// when the consumer lags. Dropping the receiver aborts the fan-out
/// (the driver drops the inner stream — Drop-cancel) and the driver
/// always resolves to the recovered mapper.
pub fn into_channel<I, O, M>(
    stream: TaskResultStream<I, O, M>,
    capacity: usize,
) -> (
    futures::future::BoxFuture<'static, M>,
    tokio_stream::wrappers::ReceiverStream<TaskResult>,
)
where
    I: Send + 'static,
    O: Send + 'static,
    M: ResultMapper<O> + Unpin + Send + 'static,
{
    let (tx, rx) = tokio::sync::mpsc::channel(capacity.max(1));
    let driver = Box::pin(async move {
        let mut stream = stream;
        loop {
            match futures::StreamExt::next(&mut stream).await {
                Some(item) => {
                    if tx.send(item).await.is_err() {
                        // Receiver dropped: abandon the fan-out
                        // (in-flight runner futures are Drop-cancelled
                        // with the inner stream inside into_mapper).
                        break;
                    }
                }
                None => break,
            }
        }
        stream.into_mapper()
    }) as futures::future::BoxFuture<'static, M>;
    (driver, tokio_stream::wrappers::ReceiverStream::new(rx))
}

/// The stream returned by [`task_results`]. All fields `Unpin`.
pub struct TaskResultStream<I, O, M> {
    inner: FanoutStream<I, O>,
    ctx: ResultCtx,
    mapper: M,
    seq: u64,
    completed: u64,
    finished: bool,
}

impl<I, O, M> std::fmt::Debug for TaskResultStream<I, O, M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskResultStream")
            .field("seq", &self.seq)
            .field("completed", &self.completed)
            .field("finished", &self.finished)
            .finish_non_exhaustive()
    }
}

impl<I, O, M> TaskResultStream<I, O, M> {
    /// Recover the mapper (and its accumulated state). Call after the
    /// stream has terminated; calling earlier abandons the fan-out
    /// (in-flight work is drop-cancelled by the inner stream).
    pub fn into_mapper(self) -> M {
        self.mapper
    }

    fn emit(&mut self, progress: f32, chunk: JsonValue) -> TaskResult {
        let seq = self.seq;
        self.seq += 1;
        TaskResult::Partial(PartialResult {
            task_id: self.ctx.task_id,
            node_id: self.ctx.node_id,
            seq,
            progress,
            output_chunk: chunk,
            sig: Signature::from_bytes([0u8; 64]),
        })
    }

    fn item_progress(&self) -> f32 {
        match self.ctx.expected {
            // f32 mantissa loss is irrelevant at fan-out scales (≤64
            // targets today, and progress is display-only telemetry).
            #[allow(clippy::cast_precision_loss)]
            Some(n) if n > 0 => ((self.completed as f32) / (n as f32)).min(1.0),
            _ => 0.0,
        }
    }
}

impl<I, O, M> Stream for TaskResultStream<I, O, M>
where
    I: Send + 'static,
    O: Send + 'static,
    M: ResultMapper<O> + Unpin,
{
    type Item = TaskResult;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.finished {
            return Poll::Ready(None);
        }
        match this.inner.poll_next_unpin(cx) {
            Poll::Ready(Some(FanoutEvent::Item { index, outcome })) => {
                this.completed += 1;
                let chunk = this.mapper.on_item(index, &outcome);
                let progress = this.item_progress();
                Poll::Ready(Some(this.emit(progress, chunk)))
            }
            Poll::Ready(Some(FanoutEvent::End(summary))) => {
                this.finished = true;
                let chunk = this.mapper.on_end(&summary);
                Poll::Ready(Some(this.emit(1.0, chunk)))
            }
            Poll::Ready(None) => {
                // Inner fused without an End (only possible if the
                // caller kept polling past termination) — mirror it.
                this.finished = true;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    // Progress values are exact fractions of small integers (k/n for
    // n ≤ 64) — strict equality is intentional and stable.
    clippy::float_cmp
)]
mod tests {
    use super::*;
    use crate::fanout::{FanoutController, FanoutSpec, WindowPolicy};
    use futures::channel::oneshot;
    use futures::future::BoxFuture;
    use futures::task::noop_waker;
    use harness_core::PartialPolicy;
    use serde_json::json;
    use std::sync::Arc;

    struct RecordingMapper {
        items: Vec<(u64, String)>,
        ended: u32,
    }

    impl ResultMapper<u64> for RecordingMapper {
        fn on_item(&mut self, index: u64, outcome: &ItemOutcome<u64>) -> JsonValue {
            let kind = match outcome {
                ItemOutcome::Ok(_) => "ok",
                ItemOutcome::Failed(_) => "failed",
                ItemOutcome::TimedOut => "timed_out",
            };
            self.items.push((index, kind.to_string()));
            json!({ "index": index, "outcome": kind })
        }
        fn on_end(&mut self, summary: &FanoutSummary) -> JsonValue {
            self.ended += 1;
            json!({ "summary": { "ok": summary.ok, "failed": summary.failed } })
        }
    }

    type Gates = Arc<parking_lot::Mutex<Vec<oneshot::Sender<ItemOutcome<u64>>>>>;

    fn gated_spec(n: u64, window: usize) -> (FanoutSpec<u64, u64>, Gates) {
        let gates: Gates = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let g = gates.clone();
        let spec = FanoutSpec {
            source: futures::stream::iter(0..n).boxed(),
            run: Box::new(move |_i, _item| {
                let (tx, rx) = oneshot::channel();
                g.lock().push(tx);
                Box::pin(async move {
                    rx.await
                        .unwrap_or_else(|_| ItemOutcome::Failed("gate dropped".into()))
                }) as BoxFuture<'static, ItemOutcome<u64>>
            }),
            window: WindowPolicy::Fixed(window),
            live_workers: Box::new(|| 1),
            deadline: None,
            on_failure: PartialPolicy::ReturnPartial,
        };
        (spec, gates)
    }

    fn ctx_for(expected: Option<u64>) -> ResultCtx {
        ResultCtx {
            task_id: TaskId::new_v7(),
            node_id: NodeId::from_bytes([9; 16]),
            expected,
        }
    }

    fn poll<S: Stream + Unpin>(s: &mut S) -> Poll<Option<S::Item>> {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        s.poll_next_unpin(&mut cx)
    }

    fn partial(r: TaskResult) -> PartialResult {
        match r {
            TaskResult::Partial(p) => p,
            TaskResult::Final(_) => panic!("adapter must never yield Final"),
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn t01_seq_and_progress_over_full_run() {
        let (spec, gates) = gated_spec(4, 2);
        let mapper = RecordingMapper {
            items: vec![],
            ended: 0,
        };
        let mut s = task_results(FanoutController::stream(spec), ctx_for(Some(4)), mapper);
        assert!(poll(&mut s).is_pending());
        let mut emitted = Vec::new();
        while emitted.len() < 5 {
            if let Some(tx) = {
                let mut g = gates.lock();
                if g.is_empty() {
                    None
                } else {
                    Some(g.remove(0))
                }
            } {
                tx.send(ItemOutcome::Ok(0)).expect("send");
            }
            while let Poll::Ready(Some(r)) = poll(&mut s) {
                emitted.push(partial(r));
            }
        }
        // 4 item frames + 1 summary.
        assert_eq!(emitted.len(), 5);
        for (i, p) in emitted.iter().enumerate() {
            assert_eq!(p.seq, i as u64, "seq monotonic from 0");
        }
        let progresses: Vec<f32> = emitted.iter().map(|p| p.progress).collect();
        assert_eq!(progresses, vec![0.25, 0.5, 0.75, 1.0, 1.0]);
        assert_eq!(emitted[4].output_chunk["summary"]["ok"], 4);
        assert!(matches!(poll(&mut s), Poll::Ready(None)));
        let mapper = s.into_mapper();
        assert_eq!(mapper.items.len(), 4);
        assert_eq!(mapper.ended, 1, "on_end exactly once");
    }

    #[test]
    fn t02_unknown_expected_keeps_progress_zero_until_summary() {
        let (spec, gates) = gated_spec(2, 2);
        let mapper = RecordingMapper {
            items: vec![],
            ended: 0,
        };
        let mut s = task_results(FanoutController::stream(spec), ctx_for(None), mapper);
        assert!(poll(&mut s).is_pending());
        for tx in gates.lock().drain(..) {
            tx.send(ItemOutcome::Ok(1)).expect("send");
        }
        let mut emitted = Vec::new();
        while let Poll::Ready(Some(r)) = poll(&mut s) {
            emitted.push(partial(r));
        }
        assert_eq!(emitted.len(), 3);
        assert_eq!(emitted[0].progress, 0.0);
        assert_eq!(emitted[1].progress, 0.0);
        assert_eq!(emitted[2].progress, 1.0, "summary always 1.0");
    }

    #[test]
    fn t03_empty_source_immediate_summary() {
        let (spec, _gates) = gated_spec(0, 2);
        let mapper = RecordingMapper {
            items: vec![],
            ended: 0,
        };
        let mut s = task_results(FanoutController::stream(spec), ctx_for(Some(0)), mapper);
        let Poll::Ready(Some(r)) = poll(&mut s) else {
            panic!("expected immediate summary");
        };
        let p = partial(r);
        assert_eq!(p.seq, 0);
        assert_eq!(p.progress, 1.0);
        assert!(matches!(poll(&mut s), Poll::Ready(None)));
        assert_eq!(s.into_mapper().ended, 1);
    }

    #[test]
    fn t04_failure_outcomes_reach_mapper() {
        let (spec, gates) = gated_spec(2, 2);
        let mapper = RecordingMapper {
            items: vec![],
            ended: 0,
        };
        let mut s = task_results(FanoutController::stream(spec), ctx_for(Some(2)), mapper);
        assert!(poll(&mut s).is_pending());
        let mut g = gates.lock();
        g.remove(0)
            .send(ItemOutcome::Failed("boom".into()))
            .expect("send");
        g.remove(0).send(ItemOutcome::TimedOut).expect("send");
        drop(g);
        let mut emitted = Vec::new();
        while let Poll::Ready(Some(r)) = poll(&mut s) {
            emitted.push(partial(r));
        }
        assert_eq!(emitted.len(), 3);
        let mapper = s.into_mapper();
        let kinds: Vec<&str> = mapper.items.iter().map(|(_, k)| k.as_str()).collect();
        assert!(kinds.contains(&"failed") && kinds.contains(&"timed_out"));
    }

    #[test]
    fn t05_drop_mid_stream_cancels_in_flight() {
        let (spec, gates) = gated_spec(8, 4);
        let mapper = RecordingMapper {
            items: vec![],
            ended: 0,
        };
        let mut s = task_results(FanoutController::stream(spec), ctx_for(Some(8)), mapper);
        assert!(poll(&mut s).is_pending());
        assert_eq!(gates.lock().len(), 4);
        drop(s);
        // Receivers are dropped with the stream: sends now fail.
        for tx in gates.lock().drain(..) {
            assert!(tx.send(ItemOutcome::Ok(0)).is_err(), "in-flight cancelled");
        }
    }

    #[tokio::test]
    async fn t06_into_channel_delivers_and_recovers_mapper() {
        let (spec, gates) = gated_spec(3, 3);
        let mapper = RecordingMapper {
            items: vec![],
            ended: 0,
        };
        let stream = task_results(FanoutController::stream(spec), ctx_for(Some(3)), mapper);
        let (driver, mut rx) = into_channel(stream, 3);
        let handle = tokio::spawn(driver);
        // The driver populates the gates only once polled — wait for it.
        for _ in 0..10_000 {
            if gates.lock().len() == 3 {
                break;
            }
            tokio::task::yield_now().await;
        }
        for tx in gates.lock().drain(..) {
            tx.send(ItemOutcome::Ok(7)).expect("send");
        }
        let mut got = 0;
        while let Some(r) = futures::StreamExt::next(&mut rx).await {
            let _ = partial(r);
            got += 1;
        }
        assert_eq!(got, 4, "3 items + summary through the channel");
        let mapper = handle.await.expect("join");
        assert_eq!(mapper.items.len(), 3);
        assert_eq!(mapper.ended, 1);
    }

    #[tokio::test]
    async fn t07_into_channel_receiver_drop_cancels_fanout() {
        let (spec, gates) = gated_spec(10, 4);
        let mapper = RecordingMapper {
            items: vec![],
            ended: 0,
        };
        let stream = task_results(FanoutController::stream(spec), ctx_for(Some(10)), mapper);
        let (driver, rx) = into_channel(stream, 1);
        let handle = tokio::spawn(driver);
        // Let the fan-out pull its window, then drop the consumer.
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
        assert_eq!(gates.lock().len(), 4, "window in flight");
        // Complete one item so the driver's send hits a dropped receiver.
        drop(rx);
        let tx0 = gates.lock().remove(0);
        tx0.send(ItemOutcome::Ok(0)).expect("send");
        let mapper = handle.await.expect("driver resolves after abort");
        // In-flight gates were dropped with the stream: sends now fail.
        for tx in gates.lock().drain(..) {
            assert!(tx.send(ItemOutcome::Ok(0)).is_err(), "cancelled");
        }
        assert!(mapper.items.len() <= 1);
    }

    #[tokio::test]
    async fn t08_into_channel_empty_source() {
        let (spec, _gates) = gated_spec(0, 2);
        let mapper = RecordingMapper {
            items: vec![],
            ended: 0,
        };
        let stream = task_results(FanoutController::stream(spec), ctx_for(Some(0)), mapper);
        let (driver, mut rx) = into_channel(stream, 1);
        let handle = tokio::spawn(driver);
        let first = futures::StreamExt::next(&mut rx).await.expect("summary");
        assert_eq!(partial(first).progress, 1.0);
        assert!(futures::StreamExt::next(&mut rx).await.is_none());
        assert_eq!(handle.await.expect("join").ended, 1);
    }
}
