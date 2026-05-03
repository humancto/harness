//! Dispatcher — pure routing decision from (Task, Cardinality, mesh state)
//! to a typed `DispatchPlan`. No I/O, no DB. Phase 2.4 plugs round-robin
//! selection + lease-based claiming on top.

mod eligible;
mod filter;
mod live_set;
mod round_robin;

use harness_core::NodeId;

pub use eligible::Dispatcher;
pub use live_set::{LiveSet, StaticLiveSet};
pub use round_robin::RoundRobin;

/// What the dispatcher decided for a single submission. The caller (2.4)
/// turns this into actual sends.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum DispatchPlan {
    /// `Anyone` / `Owner`: one node executes, the result is final.
    Single { node: NodeId },
    /// `Federated`: every listed node executes the (cloned, re-signed)
    /// task; results are merged per `MergeStrategy` by the brain.
    /// Order is deterministic — nodes sorted by `NodeId` for stable
    /// re-dispatch and reproducible tests.
    Federated { nodes: Vec<NodeId> },
}
