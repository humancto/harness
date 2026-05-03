//! Replica primitives for task-state gossip (PRD §17 / Phase 2.5).
//!
//! `harness-store` owns the persistent replicated map; `harness-mesh`
//! gossips changes over QUIC. To break what would otherwise be a dep
//! cycle (`mesh → store`), the small surface they share lives here.
//!
//! ADR-0006 records the choice of a custom LWW map over `automerge`
//! for task state.

use serde::{Deserialize, Serialize};

use crate::identity::{NodeId, Signature};
use crate::ids::TaskId;
use crate::protocol::signable::Signable;

/// Total order over task lifecycle states. Higher = "more advanced."
/// Used as the primary tiebreak in the LWW merge so concurrent
/// transitions always converge to the more-advanced state.
///
/// The numeric values are wire-stable — a future state insert must
/// either pick a fresh number that preserves the partial order or
/// be gated by a wire-format ADR.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
#[repr(u8)]
pub enum ReplicatedState {
    Submitted = 0,
    Planned = 1,
    Dispatched = 2,
    Claimed = 3,
    Running = 4,
    Expired = 5,
    Cancelled = 6,
    Failed = 7,
    Done = 8,
}

impl ReplicatedState {
    /// `true` if this state cannot transition forward — once a task is
    /// `Done`, `Failed`, `Cancelled`, or `Expired`, replicas should
    /// reject regressions.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            ReplicatedState::Done
                | ReplicatedState::Failed
                | ReplicatedState::Cancelled
                | ReplicatedState::Expired
        )
    }
}

/// One row of the replicated task state map. Plus the source node id
/// + wall-clock millis so the LWW merge has deterministic tiebreaks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicatedTaskState {
    pub task_id: TaskId,
    pub state: ReplicatedState,
    /// Unix milliseconds when the source node observed the transition.
    pub at_ms: u64,
    /// Node that emitted this entry — usually the dispatcher (for
    /// state transitions up to `running`) or the worker (for
    /// `done`/`failed`).
    pub source: NodeId,
    /// Optional 256-byte preview of the result output. Always set on
    /// terminal states; `None` otherwise. Bounded so the replica
    /// payload stays small.
    #[serde(default)]
    pub output_preview: Option<Vec<u8>>,
}

impl ReplicatedTaskState {
    /// LWW merge order: `(state, at_ms, source)` lexicographic.
    ///
    /// Returns `true` if `incoming` should replace `self` in the
    /// replica map. Equality returns `false` (no churn).
    #[must_use]
    pub fn supersedes(&self, incoming: &Self) -> bool {
        let lhs = (self.state, self.at_ms, self.source.as_bytes());
        let rhs = (incoming.state, incoming.at_ms, incoming.source.as_bytes());
        rhs > lhs
    }

    /// Take the merge of `self` and `incoming`, mutating `self` in
    /// place if `incoming` wins.
    pub fn merge(&mut self, incoming: Self) {
        if self.supersedes(&incoming) {
            *self = incoming;
        }
    }
}

/// Signed envelope carrying a batch of replica entries. The signature
/// commits to canonical-encoded `entries` (sig zeroed) so a malicious
/// peer can't alter a batch in transit.
///
/// Capped at 256 KiB by the gossip transport in 2.6 (anti-DoS).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaSyncEnvelope {
    pub source: NodeId,
    /// Unix milliseconds when this envelope was assembled.
    pub assembled_at: u64,
    pub entries: Vec<ReplicatedTaskState>,
    pub sig: Signature,
}

impl Signable for ReplicaSyncEnvelope {
    fn sig_field_mut(&mut self) -> &mut Signature {
        &mut self.sig
    }
    fn sig_field(&self) -> &Signature {
        &self.sig
    }
}

/// Trait the gossip layer (`harness-mesh::gossip`) calls to deliver
/// incoming changes. The implementation lives in `harness-store`'s
/// `ReplicatedTaskStore`. This trait keeps mesh ↔ store decoupled.
pub trait ReplicaApplier: Send + Sync {
    /// Apply a verified `ReplicaSyncEnvelope`. Returns the number of
    /// entries that actually changed local state (i.e., LWW-superseded
    /// the existing entry). The mesh layer uses this to decide whether
    /// to re-broadcast the change or treat it as redundant.
    ///
    /// # Errors
    /// Implementation-specific. Most realistic failures are persistence
    /// (`SQLite`) — the gossip layer logs and continues.
    fn apply(&self, envelope: &ReplicaSyncEnvelope) -> Result<usize, ReplicaError>;

    /// Snapshot of the local replica head — `blake3(canonical-bytes(map))`
    /// truncated to 32 bytes. The heartbeat layer piggy-backs this for
    /// anti-entropy: a peer whose head differs initiates a sync.
    fn head(&self) -> [u8; 32];
}

/// Errors from the replica layer.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ReplicaError {
    #[error("replica storage: {0}")]
    Storage(String),

    #[error("invalid envelope: {0}")]
    Invalid(String),

    #[error("signature verify: {0}")]
    Sig(#[from] crate::error::ProtocolError),
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::identity::Identity;

    fn entry(state: ReplicatedState, at_ms: u64, source_byte: u8) -> ReplicatedTaskState {
        ReplicatedTaskState {
            task_id: TaskId::new_v7(),
            state,
            at_ms,
            source: NodeId::from_bytes([source_byte; 16]),
            output_preview: None,
        }
    }

    #[test]
    fn state_priority_is_total_order() {
        assert!(ReplicatedState::Submitted < ReplicatedState::Planned);
        assert!(ReplicatedState::Planned < ReplicatedState::Dispatched);
        assert!(ReplicatedState::Running < ReplicatedState::Done);
        assert!(ReplicatedState::Failed < ReplicatedState::Done);
    }

    #[test]
    fn higher_state_wins_lww() {
        let mut a = entry(ReplicatedState::Running, 1000, 1);
        let b = entry(
            ReplicatedState::Done,
            500, // earlier wall-clock
            1,
        );
        // Same task_id needs aligning for the merge invariant; in
        // practice the replica indexes by task_id externally.
        let task_id = a.task_id;
        let mut b = b;
        b.task_id = task_id;
        a.merge(b.clone());
        assert_eq!(a.state, ReplicatedState::Done);
    }

    #[test]
    fn earlier_state_does_not_supersede() {
        let later = entry(ReplicatedState::Done, 100, 1);
        let earlier = entry(ReplicatedState::Running, 10_000, 9);
        assert!(!later.supersedes(&earlier));
    }

    #[test]
    fn at_ms_breaks_tie_when_state_equal() {
        let a = entry(ReplicatedState::Done, 500, 1);
        let b = entry(ReplicatedState::Done, 1000, 1);
        assert!(a.supersedes(&b));
        assert!(!b.supersedes(&a));
    }

    #[test]
    fn source_node_id_breaks_tie_when_state_and_at_ms_equal() {
        let a = entry(ReplicatedState::Done, 500, 1);
        let b = entry(ReplicatedState::Done, 500, 9);
        assert!(a.supersedes(&b));
        assert!(!b.supersedes(&a));
    }

    #[test]
    fn equal_does_not_supersede_no_churn() {
        let a = entry(ReplicatedState::Done, 500, 5);
        let b = a.clone();
        assert!(!a.supersedes(&b));
    }

    #[test]
    fn terminal_states() {
        assert!(ReplicatedState::Done.is_terminal());
        assert!(ReplicatedState::Failed.is_terminal());
        assert!(ReplicatedState::Cancelled.is_terminal());
        assert!(ReplicatedState::Expired.is_terminal());
        assert!(!ReplicatedState::Running.is_terminal());
        assert!(!ReplicatedState::Submitted.is_terminal());
    }

    #[test]
    fn envelope_round_trips_cbor() {
        let env = ReplicaSyncEnvelope {
            source: NodeId::from_bytes([0x11; 16]),
            assembled_at: 1_700_000_000_000,
            entries: vec![entry(ReplicatedState::Done, 500, 5)],
            sig: Signature::from_bytes([0; 64]),
        };
        let mut buf = Vec::new();
        ciborium::ser::into_writer(&env, &mut buf).expect("encode");
        let back: ReplicaSyncEnvelope = ciborium::de::from_reader(buf.as_slice()).expect("decode");
        assert_eq!(env, back);
    }

    #[test]
    fn envelope_sign_then_verify() {
        let id = Identity::generate();
        let mut env = ReplicaSyncEnvelope {
            source: id.public_key().node_id(),
            assembled_at: 1_700_000_000_000,
            entries: vec![entry(ReplicatedState::Done, 500, 5)],
            sig: Signature::from_bytes([0; 64]),
        };
        env.sign(&id).expect("sign");
        assert!(env.verify_signature(id.public_key()).is_ok());
    }
}
