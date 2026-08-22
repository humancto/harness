//! `CapabilityIndex` — `(capability_id, semver-major)` → set of `NodeId`s
//! advertising it.

use std::collections::{BTreeMap, BTreeSet};

use harness_core::{NodeId, NodeManifest};
use parking_lot::RwLock;

/// Semver compatibility: a `Task` requesting capability `"echo"` matches a
/// `Capability { id: "echo", version: 0.x.y }` from any node iff
/// `requested_major == advertised_major`. Phase 2.x ships every capability
/// at `0.1.0`; refinement when capabilities reach 1.0 is a Phase 3.x ADR.
#[derive(Debug, Default)]
pub struct CapabilityIndex {
    inner: RwLock<BTreeMap<(String, u16), BTreeSet<NodeId>>>,
}

impl CapabilityIndex {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Replace this node's contribution to the index with the manifest's
    /// declared capabilities. Idempotent.
    pub fn upsert_node(&self, manifest: &NodeManifest) {
        let mut g = self.inner.write();
        for nodes in g.values_mut() {
            nodes.remove(&manifest.node_id);
        }
        for cap in &manifest.capabilities {
            g.entry((cap.id.clone(), cap.version.major))
                .or_default()
                .insert(manifest.node_id);
        }
        g.retain(|_, v| !v.is_empty());
    }

    /// Drop every entry referencing this node. Used when a peer is evicted
    /// from the mesh.
    pub fn remove_node(&self, node_id: &NodeId) {
        let mut g = self.inner.write();
        for nodes in g.values_mut() {
            nodes.remove(node_id);
        }
        g.retain(|_, v| !v.is_empty());
    }

    /// All nodes advertising this capability id, regardless of major
    /// version. Phase 3.x will add a typed `(id, major)` lookup once
    /// versioned capabilities coexist on the same mesh.
    #[must_use]
    pub fn nodes_for(&self, capability: &str) -> Vec<NodeId> {
        let g = self.inner.read();
        g.iter()
            .filter(|((id, _major), _)| id == capability)
            .flat_map(|(_, set)| set.iter().copied())
            .collect()
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use harness_core::{Capability, Cardinality, Identity, NodeManifest, Resources, SemVer};

    pub(crate) fn empty_resources() -> Resources {
        Resources {
            cpu_cores: 0,
            ram_total_mb: 0,
            gpu: None,
            os: "test".into(),
            arch: "test".into(),
        }
    }

    pub(crate) fn empty_hints() -> harness_core::ResourceHints {
        harness_core::ResourceHints {
            cpu_class: harness_core::protocol::CpuClass::Light,
            memory_mb: None,
            gpu_required: false,
            gpu_memory_mb: None,
            network_class: harness_core::protocol::NetworkClass::None,
            disk_io_class: harness_core::protocol::DiskIoClass::None,
            estimated_duration_ms: None,
        }
    }

    pub(crate) fn cap(id: &str, major: u16) -> Capability {
        Capability {
            id: id.into(),
            version: SemVer {
                major,
                minor: 0,
                patch: 0,
            },
            cardinality: Cardinality::Anyone,
            input_schema: serde_json::json!({}),
            output_schema: serde_json::json!({}),
            cost_hint: harness_core::protocol::CostHint::LocalFast,
            tags: vec![],
            rate_limit: None,
            resource_hints: empty_hints(),
            requires_secrets: vec![],
        }
    }

    fn manifest(node: u8, caps: &[Capability]) -> NodeManifest {
        let id = Identity::generate();
        NodeManifest {
            node_id: NodeId::from_bytes([node; 16]),
            hostname: "h".into(),
            pubkey: *id.public_key(),
            capabilities: caps.to_vec(),
            scopes: vec![],
            secret_tags: vec![],
            resources: empty_resources(),
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
    fn upsert_then_lookup() {
        let idx = CapabilityIndex::new();
        idx.upsert_node(&manifest(1, &[cap("echo", 0)]));
        assert_eq!(idx.nodes_for("echo"), vec![NodeId::from_bytes([1; 16])]);
    }

    #[test]
    fn replaces_node_caps_on_re_upsert() {
        let idx = CapabilityIndex::new();
        idx.upsert_node(&manifest(1, &[cap("echo", 0)]));
        idx.upsert_node(&manifest(1, &[cap("shell.exec", 0)]));
        assert!(idx.nodes_for("echo").is_empty());
        assert_eq!(
            idx.nodes_for("shell.exec"),
            vec![NodeId::from_bytes([1; 16])]
        );
    }

    #[test]
    fn remove_node_clears_buckets() {
        let idx = CapabilityIndex::new();
        idx.upsert_node(&manifest(1, &[cap("echo", 0)]));
        idx.remove_node(&NodeId::from_bytes([1; 16]));
        assert!(idx.nodes_for("echo").is_empty());
    }

    #[test]
    fn two_nodes_advertising_same_capability_returns_both() {
        let idx = CapabilityIndex::new();
        idx.upsert_node(&manifest(1, &[cap("echo", 0)]));
        idx.upsert_node(&manifest(2, &[cap("echo", 0)]));
        let mut nodes = idx.nodes_for("echo");
        nodes.sort();
        assert_eq!(
            nodes,
            vec![NodeId::from_bytes([1; 16]), NodeId::from_bytes([2; 16])]
        );
    }

    #[test]
    fn empty_returns_empty_vec_not_panic() {
        let idx = CapabilityIndex::new();
        assert!(idx.nodes_for("anything").is_empty());
    }
}
