//! `PauseState` — the shared backpressure switch (roadmap 4.7,
//! ADR-0029, PRD §14.10/§25.1).
//!
//! The heartbeat `paused` field finally gets its producer: a
//! queue-depth hysteresis latch (auto) OR-ed with an operator flag
//! (the `POST /api/v1/admin/pause` endpoint — also PRD §25.2
//! sleep/wake groundwork). One instance is shared by:
//! - the heartbeat `snapshot_fn` (publishes the effective flag +
//!   the coordination-adjusted queue depth),
//! - the API (`GET /status` surfacing + admin endpoints),
//! - the `DispatchRuntime` (a paused node stops dispatching to
//!   ITSELF too — the `PeerTable` has no self entry, so without this
//!   the local view would be pause-blind; plan review MAJOR-3).
//!
//! `coordinations` subtracts waiting-coordination parents (federated
//! slots + executor coordination permits, worst case 8 + 16 = 24 rows)
//! from the published depth so a coordinator-heavy brain doesn't
//! auto-pause on bookkeeping instead of work (plan review MAJOR-4):
//! heartbeat `queue_depth` means WORK depth as of 4.7.

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;

#[derive(Debug, Default)]
pub(crate) struct PauseState {
    /// Queue-depth hysteresis latch (set/cleared by `update_auto`).
    auto: AtomicBool,
    /// Operator switch (admin API). Sticky until resumed.
    operator: AtomicBool,
    /// Active coordinations (federated slots + coordination permits):
    /// rows at `Running(self)` that are awaiting others' work, not
    /// doing local work.
    coordinations: AtomicU32,
}

impl PauseState {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// The published/enforced flag: operator OR auto.
    pub(crate) fn effective(&self) -> bool {
        self.operator.load(Ordering::Relaxed) || self.auto.load(Ordering::Relaxed)
    }

    pub(crate) fn set_operator(&self, paused: bool) {
        self.operator.store(paused, Ordering::Relaxed);
    }

    pub(crate) fn operator_paused(&self) -> bool {
        self.operator.load(Ordering::Relaxed)
    }

    /// Subtract active coordinations from a raw inflight count: the
    /// WORK depth the heartbeat publishes and the latch evaluates.
    pub(crate) fn work_depth(&self, raw_inflight: u16) -> u16 {
        let coord = u16::try_from(self.coordinations.load(Ordering::Relaxed)).unwrap_or(u16::MAX);
        raw_inflight.saturating_sub(coord)
    }

    /// Hysteresis: latch at `depth >= max`, release only at
    /// `depth <= resume` — no flapping across the 2 s heartbeat
    /// cadence. Returns the new auto state.
    pub(crate) fn update_auto(&self, depth: u16, max: u16, resume: u16) -> bool {
        let was = self.auto.load(Ordering::Relaxed);
        let now = if was { depth > resume } else { depth >= max };
        if now != was {
            self.auto.store(now, Ordering::Relaxed);
            tracing::info!(
                target: "harness.pause",
                depth,
                max,
                resume,
                paused = now,
                "auto-pause latch changed"
            );
        }
        now
    }

    /// RAII guard marking one active coordination (see module docs).
    pub(crate) fn coordination_guard(self: &Arc<Self>) -> CoordinationGuard {
        self.coordinations.fetch_add(1, Ordering::Relaxed);
        CoordinationGuard {
            state: Arc::clone(self),
        }
    }
}

impl harness_api::PauseControl for PauseState {
    fn paused(&self) -> bool {
        self.effective()
    }
    fn operator_paused(&self) -> bool {
        self.operator_paused()
    }
    fn set_operator(&self, paused: bool) {
        self.set_operator(paused);
    }
}

#[derive(Debug)]
pub(crate) struct CoordinationGuard {
    state: Arc<PauseState>,
}

impl Drop for CoordinationGuard {
    fn drop(&mut self) {
        self.state.coordinations.fetch_sub(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn hysteresis_latches_and_releases() {
        let p = PauseState::new();
        assert!(!p.update_auto(3, 4, 2), "below max: not paused");
        assert!(p.update_auto(4, 4, 2), "at max: latch");
        assert!(p.update_auto(3, 4, 2), "between resume and max: HOLD");
        assert!(p.effective());
        assert!(!p.update_auto(2, 4, 2), "at resume: release");
        assert!(!p.effective());
        // One task re-queued does not immediately re-latch.
        assert!(!p.update_auto(3, 4, 2));
    }

    #[test]
    fn operator_or_auto_and_coordination_depth() {
        let p = PauseState::new();
        p.set_operator(true);
        assert!(p.effective(), "operator alone pauses");
        p.set_operator(false);
        assert!(!p.effective());

        assert_eq!(p.work_depth(10), 10);
        let g1 = p.coordination_guard();
        let g2 = p.coordination_guard();
        assert_eq!(p.work_depth(10), 8, "coordinations subtracted");
        drop(g1);
        assert_eq!(p.work_depth(10), 9);
        drop(g2);
        assert_eq!(p.work_depth(1), 1);
        // Saturating: more guards than inflight never underflows.
        let _g = p.coordination_guard();
        assert_eq!(p.work_depth(0), 0);
    }
}
