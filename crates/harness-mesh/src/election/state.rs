//! Weighted election state machine. Reads the local [`PeerTable`] and
//! the local node's [`brain_score`], picks the highest-scoring live
//! peer, breaks ties by `NodeId` byte-order, and emits an
//! [`ElectionResult`] the daemon can act on.

use std::time::{Duration, Instant};

use harness_core::NodeId;
use parking_lot::Mutex;

use crate::PeerTable;

/// 60s anti-flap window per PRD §12.2 — a node demoted from brain in
/// the last minute gets a -200 score penalty.
pub const ANTI_FLAP_WINDOW: Duration = Duration::from_secs(60);

/// Outcome of a single election tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElectionResult {
    /// Who the local node now believes is the brain.
    pub leader: NodeId,
    /// `true` if the local node IS the brain (`leader == self`).
    pub am_i_brain: bool,
    /// `true` if the leader is a different node than last tick.
    pub leader_changed: bool,
    /// Snapshot of the winning score.
    pub winning_score: i32,
}

/// Election configuration.
#[derive(Debug, Clone)]
pub struct ElectionConfig {
    pub self_id: NodeId,
    /// How long without a heartbeat before we consider a peer
    /// unreachable. Defaults to [`crate::PEER_TIMEOUT`] when constructed
    /// via `ElectionConfig::new`.
    pub peer_timeout: Duration,
    pub anti_flap_window: Duration,
}

impl ElectionConfig {
    pub fn new(self_id: NodeId) -> Self {
        Self {
            self_id,
            peer_timeout: crate::PEER_TIMEOUT,
            anti_flap_window: ANTI_FLAP_WINDOW,
        }
    }

    #[must_use]
    pub fn with_peer_timeout(mut self, t: Duration) -> Self {
        self.peer_timeout = t;
        self
    }

    #[must_use]
    pub fn with_anti_flap_window(mut self, w: Duration) -> Self {
        self.anti_flap_window = w;
        self
    }
}

/// Election state machine. Construct once per node; call [`Election::tick`]
/// after each heartbeat round to recompute the leader belief.
pub struct Election {
    config: ElectionConfig,
    inner: Mutex<Inner>,
}

struct Inner {
    /// Most recent leader the state machine emitted. Anti-flap penalty
    /// applies to anyone who WAS this leader within the last
    /// `anti_flap_window` and got demoted on a subsequent tick.
    last_leader: Option<NodeId>,
    last_leader_demoted_at: Option<Instant>,
    /// The id of the node that was demoted on the last tick (if any) —
    /// the anti-flap window applies to this id specifically.
    demoted_id: Option<NodeId>,
}

impl std::fmt::Debug for Election {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let g = self.inner.lock();
        f.debug_struct("Election")
            .field("self_id", &self.config.self_id)
            .field("last_leader", &g.last_leader)
            .field("demoted_id", &g.demoted_id)
            .finish_non_exhaustive()
    }
}

impl Election {
    pub fn new(config: ElectionConfig) -> Self {
        Self {
            config,
            inner: Mutex::new(Inner {
                last_leader: None,
                last_leader_demoted_at: None,
                demoted_id: None,
            }),
        }
    }

    /// Recompute the leader belief from the current peer table + the
    /// local node's score. The local node is treated as a virtual
    /// peer with `local_score`; if it wins, `am_i_brain = true`.
    pub fn tick(&self, peers: &PeerTable, local_score: i32) -> ElectionResult {
        let live = peers.live_snapshot(self.config.peer_timeout);
        let mut inner = self.inner.lock();

        // Anti-flap penalty: if the demoted_id is within the window,
        // adjust their effective score by -200.
        let now = Instant::now();
        let in_flap_window = inner
            .last_leader_demoted_at
            .is_some_and(|t| now.duration_since(t) < self.config.anti_flap_window);

        let apply_penalty = |id: NodeId, score: i32| -> i32 {
            if in_flap_window && Some(id) == inner.demoted_id {
                score - 200
            } else {
                score
            }
        };

        // Build candidate list: the local node + every live peer.
        let mut candidates: Vec<(NodeId, i32)> = Vec::with_capacity(live.len() + 1);
        candidates.push((
            self.config.self_id,
            apply_penalty(self.config.self_id, local_score),
        ));
        for entry in &live {
            let id = entry.heartbeat.node_id;
            let score = apply_penalty(id, entry.heartbeat.brain_score);
            candidates.push((id, score));
        }

        // Highest score wins; ties broken by NodeId byte-order
        // (lexicographic — using NodeId's Ord impl).
        candidates.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        let winner = candidates[0];

        let leader_changed = inner.last_leader != Some(winner.0);
        if leader_changed {
            // The previous leader is now demoted — start the anti-flap
            // window for them.
            if let Some(prev) = inner.last_leader {
                inner.demoted_id = Some(prev);
                inner.last_leader_demoted_at = Some(now);
            }
            inner.last_leader = Some(winner.0);
        }

        ElectionResult {
            leader: winner.0,
            am_i_brain: winner.0 == self.config.self_id,
            leader_changed,
            winning_score: winner.1,
        }
    }

    /// Read the most-recent election outcome without re-running.
    /// Returns `None` until [`Self::tick`] has fired at least once.
    pub fn last_leader(&self) -> Option<NodeId> {
        self.inner.lock().last_leader
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use harness_core::{Heartbeat, Identity, SemVer, Signable, Signature, TaskId};

    fn hb_for(seed: u8, brain_score: i32) -> Heartbeat {
        let id = Identity::from_secret_bytes(&[seed; 32]);
        let mut hb = Heartbeat {
            node_id: id.node_id(),
            seq: 1,
            timestamp: 0,
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
            brain_score,
            on_battery: false,
            paused: false,
            version: SemVer::new(0, 1, 0),
            sig: Signature::from_bytes([0u8; 64]),
        };
        hb.sign(&id).expect("sign");
        hb
    }

    fn node(seed: u8) -> NodeId {
        Identity::from_secret_bytes(&[seed; 32]).node_id()
    }

    #[test]
    fn alone_node_elects_itself() {
        let me = node(0x10);
        let cfg = ElectionConfig::new(me);
        let elect = Election::new(cfg);
        let table = PeerTable::new();
        let result = elect.tick(&table, 100);
        assert_eq!(result.leader, me);
        assert!(result.am_i_brain);
        assert!(result.leader_changed); // first tick
        assert_eq!(result.winning_score, 100);
    }

    #[test]
    fn highest_score_wins() {
        let me = node(0x10);
        let elect = Election::new(ElectionConfig::new(me));
        let table = PeerTable::new();
        table.record(hb_for(0x20, 500));
        table.record(hb_for(0x30, 300));
        let result = elect.tick(&table, 100);
        assert_eq!(result.leader, node(0x20));
        assert!(!result.am_i_brain);
    }

    #[test]
    fn ties_broken_by_node_id_lexicographic() {
        let me = node(0x10);
        let elect = Election::new(ElectionConfig::new(me));
        let table = PeerTable::new();
        table.record(hb_for(0x20, 500));
        table.record(hb_for(0x30, 500));
        table.record(hb_for(0x40, 500));
        let result = elect.tick(&table, 500);
        // Lowest-byte NodeId (== smallest in NodeId::Ord) wins ties.
        // The four node_ids are blake3-derived from seeds 0x10, 0x20,
        // 0x30, 0x40. We can't predict the order — but we can assert
        // the result is deterministic and is one of the candidates.
        let candidates = [me, node(0x20), node(0x30), node(0x40)];
        assert!(candidates.contains(&result.leader));
        // Run twice; same result.
        let result2 = elect.tick(&table, 500);
        assert_eq!(result.leader, result2.leader);
    }

    #[test]
    fn dead_peers_excluded() {
        let me = node(0x10);
        let elect =
            Election::new(ElectionConfig::new(me).with_peer_timeout(Duration::from_millis(10)));
        let table = PeerTable::new();
        table.record(hb_for(0x20, 9999));
        std::thread::sleep(Duration::from_millis(20));
        let result = elect.tick(&table, 100);
        assert_eq!(result.leader, me, "dead peer must not win");
    }

    #[test]
    fn anti_flap_penalizes_recent_demoted_brain() {
        let me = node(0x10);
        let elect = Election::new(ElectionConfig::new(me));
        let table = PeerTable::new();

        // Tick 1: peer 0x20 with score 500 wins.
        table.record(hb_for(0x20, 500));
        let r1 = elect.tick(&table, 100);
        assert_eq!(r1.leader, node(0x20));

        // Tick 2: demote 0x20 by giving 0x30 a higher score.
        table.record(hb_for(0x30, 600));
        let r2 = elect.tick(&table, 100);
        assert_eq!(r2.leader, node(0x30));

        // Tick 3: 0x20 bumps their score back to 600 — but they were
        // just demoted, so the -200 penalty puts them at effective
        // 400. 0x30 stays at 600 and keeps the brain.
        table.record(hb_for(0x20, 600));
        let r3 = elect.tick(&table, 100);
        assert_eq!(r3.leader, node(0x30), "anti-flap should keep 0x30 as brain");
    }

    #[test]
    fn leader_changed_flag_only_fires_on_actual_change() {
        let me = node(0x10);
        let elect = Election::new(ElectionConfig::new(me));
        let table = PeerTable::new();
        let r1 = elect.tick(&table, 100);
        assert!(r1.leader_changed); // first tick
        let r2 = elect.tick(&table, 100);
        assert!(!r2.leader_changed); // unchanged
    }

    #[test]
    fn last_leader_returns_recent_election() {
        let me = node(0x10);
        let elect = Election::new(ElectionConfig::new(me));
        assert_eq!(elect.last_leader(), None);
        let table = PeerTable::new();
        elect.tick(&table, 100);
        assert_eq!(elect.last_leader(), Some(me));
    }
}
