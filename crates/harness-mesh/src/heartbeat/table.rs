//! Per-node peer table — the local snapshot 1.6's election reads.
//!
//! Each entry tracks the most-recent verified [`Heartbeat`] from a peer
//! plus an `Instant`-based last-seen timestamp for liveness decay. The
//! table is concurrent-safe (`parking_lot::RwLock` internally) so the
//! listener can write while readers (the election state machine, the UI,
//! diagnostics) read without coordination.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use harness_core::{Heartbeat, NodeId};
use parking_lot::RwLock;

/// One peer's most-recent state. Cloneable so callers can take snapshots
/// without holding the read lock.
#[derive(Debug, Clone)]
pub struct PeerEntry {
    pub heartbeat: Heartbeat,
    pub last_seen: Instant,
}

/// Concurrent peer table keyed by `NodeId`. Cheap to clone via `Arc`.
#[derive(Debug, Clone, Default)]
pub struct PeerTable {
    inner: Arc<RwLock<HashMap<NodeId, PeerEntry>>>,
}

impl PeerTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert or update a peer's heartbeat. The caller is expected to
    /// have already verified the signature against the peer's pubkey
    /// (1.4's transport does this on `recv_sequenced`).
    ///
    /// Returns `true` if this was the first heartbeat for this peer
    /// (i.e. a *new* peer joined). Callers (1.5's listener task) use
    /// the return value to decide whether to publish a `peer_added`
    /// vs `peer_updated` event on the API channel.
    pub fn record(&self, hb: Heartbeat) -> bool {
        let id = hb.node_id;
        let prior = self.inner.write().insert(
            id,
            PeerEntry {
                heartbeat: hb,
                last_seen: Instant::now(),
            },
        );
        prior.is_none()
    }

    /// Remove the peer with this id, returning the entry if present.
    pub fn remove(&self, id: &NodeId) -> Option<PeerEntry> {
        self.inner.write().remove(id)
    }

    /// O(1) lookup snapshot.
    #[must_use]
    pub fn get(&self, id: &NodeId) -> Option<PeerEntry> {
        self.inner.read().get(id).cloned()
    }

    /// `true` if the peer is in the table AND its last heartbeat is
    /// within `timeout`. Use [`PEER_TIMEOUT`] as the default.
    #[must_use]
    pub fn is_live(&self, id: &NodeId, timeout: Duration) -> bool {
        self.inner
            .read()
            .get(id)
            .is_some_and(|entry| entry.last_seen.elapsed() < timeout)
    }

    /// Snapshot of all peers, sorted by `node_id` for deterministic
    /// downstream consumers (e.g., UI tables, election tiebreak).
    #[must_use]
    pub fn snapshot(&self) -> Vec<PeerEntry> {
        let g = self.inner.read();
        let mut v: Vec<PeerEntry> = g.values().cloned().collect();
        drop(g);
        v.sort_by(|a, b| a.heartbeat.node_id.cmp(&b.heartbeat.node_id));
        v
    }

    /// Snapshot of peers that are still live within `timeout`.
    #[must_use]
    pub fn live_snapshot(&self, timeout: Duration) -> Vec<PeerEntry> {
        self.inner
            .read()
            .values()
            .filter(|e| e.last_seen.elapsed() < timeout)
            .cloned()
            .collect()
    }

    /// Remove peers whose last heartbeat is older than `timeout`.
    /// Returns the dropped node ids — the caller can broadcast
    /// "peer departed" events from them (1.5's daemon glue).
    pub fn evict_stale(&self, timeout: Duration) -> Vec<NodeId> {
        let mut g = self.inner.write();
        let stale: Vec<NodeId> = g
            .iter()
            .filter(|(_, e)| e.last_seen.elapsed() >= timeout)
            .map(|(id, _)| *id)
            .collect();
        for id in &stale {
            g.remove(id);
        }
        stale
    }

    /// Number of currently-tracked peers (live or otherwise).
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    /// Convenience: empty?
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.read().is_empty()
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use harness_core::{Identity, SemVer, Signature, TaskId};

    fn make_hb(seed: u8, seq: u64) -> Heartbeat {
        let id = Identity::from_secret_bytes(&[seed; 32]);
        Heartbeat {
            node_id: id.node_id(),
            seq,
            timestamp: 1_700_000_000_000,
            queue_depth: 0,
            cpu_busy_pct: 0,
            cpu_pinned_count: 0,
            ram_used_mb: 0,
            ram_total_mb: 0,
            gpu_used_mb: 0,
            gpu_total_mb: 0,
            capabilities_hash: [0u8; 16],
            in_flight: vec![TaskId(uuid::Uuid::nil())],
            leader_belief: id.node_id(),
            brain_score: 0,
            on_battery: false,
            paused: false,
            version: SemVer::new(0, 1, 0),
            sig: Signature::from_bytes([0u8; 64]),
        }
    }

    #[test]
    fn record_and_get() {
        let t = PeerTable::new();
        let hb = make_hb(1, 1);
        let id = hb.node_id;
        t.record(hb.clone());
        let entry = t.get(&id).expect("present");
        assert_eq!(entry.heartbeat.seq, 1);
    }

    #[test]
    fn record_overwrites_with_newer() {
        let t = PeerTable::new();
        let hb1 = make_hb(2, 1);
        let id = hb1.node_id;
        t.record(hb1);
        let hb2 = make_hb(2, 7);
        t.record(hb2);
        assert_eq!(t.get(&id).expect("present").heartbeat.seq, 7);
    }

    #[test]
    fn remove_returns_entry() {
        let t = PeerTable::new();
        let hb = make_hb(3, 1);
        let id = hb.node_id;
        t.record(hb);
        assert!(t.remove(&id).is_some());
        assert!(t.get(&id).is_none());
    }

    #[test]
    fn snapshot_is_sorted_by_node_id() {
        let t = PeerTable::new();
        for seed in [0x10, 0x20, 0x30, 0x40] {
            t.record(make_hb(seed, 1));
        }
        let snap = t.snapshot();
        assert_eq!(snap.len(), 4);
        for w in snap.windows(2) {
            assert!(w[0].heartbeat.node_id < w[1].heartbeat.node_id);
        }
    }

    #[test]
    fn is_live_within_timeout() {
        let t = PeerTable::new();
        let hb = make_hb(4, 1);
        let id = hb.node_id;
        t.record(hb);
        assert!(t.is_live(&id, Duration::from_secs(60)));
        // Sub-millisecond timeout — entry is fresh, but 0ms means
        // already-expired. We use a safer 100us boundary.
        std::thread::sleep(Duration::from_millis(2));
        assert!(!t.is_live(&id, Duration::from_micros(500)));
    }

    #[test]
    fn evict_stale_drops_old_entries() {
        let t = PeerTable::new();
        t.record(make_hb(5, 1));
        t.record(make_hb(6, 1));
        // Both fresh; evict with a tiny timeout means evict both.
        std::thread::sleep(Duration::from_millis(2));
        let dropped = t.evict_stale(Duration::from_micros(500));
        assert_eq!(dropped.len(), 2);
        assert!(t.is_empty());
    }

    #[test]
    fn live_snapshot_only_returns_fresh() {
        let t = PeerTable::new();
        t.record(make_hb(7, 1));
        std::thread::sleep(Duration::from_millis(2));
        let live = t.live_snapshot(Duration::from_micros(500));
        assert!(live.is_empty(), "should be no live peers");
        let live_generous = t.live_snapshot(Duration::from_secs(60));
        assert_eq!(live_generous.len(), 1);
    }

    #[test]
    fn clone_is_shared_reference() {
        let t1 = PeerTable::new();
        let t2 = t1.clone();
        t1.record(make_hb(8, 1));
        // Both views see the same data — Arc-shared internally.
        assert_eq!(t1.len(), 1);
        assert_eq!(t2.len(), 1);
    }
}
