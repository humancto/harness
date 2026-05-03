//! `LiveSet` — adapter over "which nodes are live right now," abstracted so
//! we don't need a real `PeerTable` in unit tests. The mesh-side adapter
//! lives in 2.4 wiring (avoids `harness-mesh ↔ harness-orchestrator` cycle).

use std::collections::HashSet;

use harness_core::NodeId;

pub trait LiveSet: Send + Sync {
    fn is_live(&self, node: &NodeId) -> bool;

    fn live_subset(&self, candidates: &[NodeId]) -> Vec<NodeId> {
        candidates
            .iter()
            .copied()
            .filter(|n| self.is_live(n))
            .collect()
    }
}

/// In-memory adapter for tests.
#[derive(Debug, Clone, Default)]
pub struct StaticLiveSet {
    live: HashSet<NodeId>,
}

impl StaticLiveSet {
    /// Build a static live set from any iterable of `NodeId`s. Named
    /// `from_node_ids` rather than `from_iter` to avoid the
    /// `FromIterator` trait collision (`clippy::should_implement_trait`).
    #[must_use]
    pub fn from_node_ids<I: IntoIterator<Item = NodeId>>(iter: I) -> Self {
        Self {
            live: iter.into_iter().collect(),
        }
    }
}

impl LiveSet for StaticLiveSet {
    fn is_live(&self, node: &NodeId) -> bool {
        self.live.contains(node)
    }
}
