//! Dispatch errors — typed failures from `Dispatcher::eligible`.

use harness_core::NodeId;
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum DispatchError {
    /// No live peer advertises this capability.
    #[error("no eligible nodes for capability {capability:?}")]
    NoEligibleNodes { capability: String },

    /// `Owner { scope_field }` was selected but the input doesn't carry
    /// that field, the named field isn't a string, or no live peer owns
    /// the resolved scope id.
    #[error("owner routing: {reason}")]
    Owner { reason: String },

    /// `pin_to_node` named a peer we've never heard from or that has
    /// timed out. Distinct from `NoEligibleNodes` so CLI/UI can surface
    /// a precise "node X not on the mesh" error.
    #[error("pinned node {node} is not live")]
    PinnedNodeNotLive { node: NodeId },

    /// `pin_to_scope` named a scope no live peer owns.
    #[error("pinned scope {scope_id:?} has no owner")]
    PinnedScopeUnowned { scope_id: String },

    /// 4.4 (ADR-0026): live advertisers exist but every one is
    /// hard-gated by resource constraints (paused, missing GPU,
    /// memory/VRAM over capacity, pinned-full). Transient by nature —
    /// callers must WAIT (bounded by the task's own deadline), never
    /// terminalize on the eligibility window (plan review BLOCKER-1).
    #[error("all live nodes resource-gated for capability {capability:?}")]
    ResourceGated { capability: String },

    /// `must_be_local` is true and only cloud-tagged nodes were eligible.
    /// Stub today; wiring lands in 3.6 when cloud capabilities ship.
    #[error("must_be_local: no local-tagged eligible nodes")]
    MustBeLocalUnsatisfiable,
}
