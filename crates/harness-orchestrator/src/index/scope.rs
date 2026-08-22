//! `ScopeIndex` — `scope.id` → set of `NodeId`s claiming ownership.

use std::collections::{BTreeMap, BTreeSet};

use harness_core::{NodeId, NodeManifest};
use parking_lot::RwLock;

/// Scopes are unbounded strings (PRD §13.2 — `directory`, `repo`,
/// `photo_library`, `mailbox` ids). Two nodes claiming the same scope id
/// is legal but rare (e.g., a shared NAS mounted on two hosts). The
/// dispatcher routes `Owner` cardinality to *one* of them via tiebreak
/// (lowest `NodeId`); `Federated` routes to all.
#[derive(Debug, Default)]
pub struct ScopeIndex {
    inner: RwLock<BTreeMap<String, BTreeSet<NodeId>>>,
}

impl ScopeIndex {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn upsert_node(&self, manifest: &NodeManifest) {
        let mut g = self.inner.write();
        for nodes in g.values_mut() {
            nodes.remove(&manifest.node_id);
        }
        for scope in &manifest.scopes {
            g.entry(scope.id.clone())
                .or_default()
                .insert(manifest.node_id);
        }
        g.retain(|_, v| !v.is_empty());
    }

    pub fn remove_node(&self, node_id: &NodeId) {
        let mut g = self.inner.write();
        for nodes in g.values_mut() {
            nodes.remove(node_id);
        }
        g.retain(|_, v| !v.is_empty());
    }

    /// Owners of `scope_id`, sorted ascending by `NodeId`.
    #[must_use]
    pub fn owners(&self, scope_id: &str) -> Vec<NodeId> {
        self.inner
            .read()
            .get(scope_id)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default()
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use harness_core::{Identity, NodeManifest, Resources, Scope, SemVer};

    fn manifest_with_scopes(node: u8, scope_ids: &[&str]) -> NodeManifest {
        let id = Identity::generate();
        NodeManifest {
            node_id: NodeId::from_bytes([node; 16]),
            hostname: "h".into(),
            pubkey: *id.public_key(),
            capabilities: vec![],
            scopes: scope_ids
                .iter()
                .map(|id| Scope {
                    kind: "directory".into(),
                    id: (*id).to_string(),
                    label: (*id).to_string(),
                    indexed: false,
                    last_indexed: None,
                })
                .collect(),
            secret_tags: vec![],
            resources: Resources {
                cpu_cores: 0,
                ram_total_mb: 0,
                gpu: None,
                os: "test".into(),
                arch: "test".into(),
            },
            online_since: 0,
            version: SemVer {
                major: 0,
                minor: 1,
                patch: 0,
            },
            sig: harness_core::Signature::from_bytes([0; 64]),
        }
    }

    #[test]
    fn upsert_owner() {
        let idx = ScopeIndex::new();
        idx.upsert_node(&manifest_with_scopes(1, &["~/work"]));
        assert_eq!(idx.owners("~/work"), vec![NodeId::from_bytes([1; 16])]);
    }

    #[test]
    fn two_owners_returned_in_sorted_order() {
        let idx = ScopeIndex::new();
        idx.upsert_node(&manifest_with_scopes(2, &["nas"]));
        idx.upsert_node(&manifest_with_scopes(1, &["nas"]));
        assert_eq!(
            idx.owners("nas"),
            vec![NodeId::from_bytes([1; 16]), NodeId::from_bytes([2; 16])]
        );
    }

    #[test]
    fn remove_node_clears_orphan_buckets() {
        let idx = ScopeIndex::new();
        idx.upsert_node(&manifest_with_scopes(1, &["~/photos"]));
        idx.remove_node(&NodeId::from_bytes([1; 16]));
        assert!(idx.owners("~/photos").is_empty());
    }
}
