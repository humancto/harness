//! `CapabilityRegistry` — the in-process map of `id → Arc<dyn Capability>`.
//!
//! Used by:
//! - the daemon to assemble its `NodeManifest::capabilities` advertisement
//! - the worker (when 2.4's runtime wires through) to look up the
//!   handler for an incoming Task

use std::collections::HashMap;
use std::sync::Arc;

use harness_core::Capability as ManifestEntry;
use parking_lot::RwLock;
use thiserror::Error;

use crate::traits::Capability;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RegistryError {
    #[error("capability {0:?} already registered")]
    Duplicate(String),
}

/// Concurrent registry. Cheaply cloneable (Arc internal).
#[derive(Clone, Default)]
pub struct CapabilityRegistry {
    inner: Arc<RwLock<HashMap<String, Arc<dyn Capability>>>>,
}

impl std::fmt::Debug for CapabilityRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CapabilityRegistry")
            .field("count", &self.inner.read().len())
            .finish_non_exhaustive()
    }
}

impl CapabilityRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a capability. Returns `Err(Duplicate)` if a capability
    /// with the same id is already present.
    pub fn register(&self, cap: Arc<dyn Capability>) -> Result<(), RegistryError> {
        let id = cap.id().to_string();
        let mut g = self.inner.write();
        if g.contains_key(&id) {
            return Err(RegistryError::Duplicate(id));
        }
        g.insert(id, cap);
        Ok(())
    }

    /// Look up a capability by id.
    pub fn get(&self, id: &str) -> Option<Arc<dyn Capability>> {
        self.inner.read().get(id).cloned()
    }

    /// Snapshot of every capability's manifest entry — what
    /// `NodeManifest::capabilities` is built from.
    #[must_use]
    pub fn manifests(&self) -> Vec<ManifestEntry> {
        let g = self.inner.read();
        let mut entries: Vec<ManifestEntry> = g.values().map(|c| c.manifest()).collect();
        entries.sort_by(|a, b| a.id.cmp(&b.id));
        entries
    }

    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.read().is_empty()
    }

    /// IDs sorted ascending — for diagnostics + the public
    /// `GET /api/v1/capabilities` endpoint that lands in a follow-up.
    #[must_use]
    pub fn ids(&self) -> Vec<String> {
        let g = self.inner.read();
        let mut ids: Vec<String> = g.keys().cloned().collect();
        ids.sort();
        ids
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::traits::ExecutionContext;
    use async_trait::async_trait;
    use harness_core::{Cardinality, NodeId, ResourceHints, SemVer, TaskId};

    struct DummyCap(&'static str);

    #[async_trait]
    impl Capability for DummyCap {
        fn id(&self) -> &str {
            self.0
        }

        fn manifest(&self) -> ManifestEntry {
            ManifestEntry {
                id: self.0.to_string(),
                version: SemVer {
                    major: 0,
                    minor: 1,
                    patch: 0,
                },
                cardinality: Cardinality::Anyone,
                input_schema: serde_json::json!({}),
                output_schema: serde_json::json!({}),
                cost_hint: harness_core::protocol::CostHint::LocalFast,
                tags: vec![],
                rate_limit: None,
                resource_hints: ResourceHints {
                    cpu_class: harness_core::protocol::CpuClass::Light,
                    memory_mb: None,
                    gpu_required: false,
                    gpu_memory_mb: None,
                    network_class: harness_core::protocol::NetworkClass::None,
                    disk_io_class: harness_core::protocol::DiskIoClass::None,
                    estimated_duration_ms: None,
                },
            }
        }

        async fn execute(
            &self,
            _ctx: &ExecutionContext,
            input: serde_json::Value,
        ) -> Result<serde_json::Value, crate::traits::CapabilityError> {
            Ok(input)
        }
    }

    #[test]
    fn register_then_get() {
        let r = CapabilityRegistry::new();
        r.register(Arc::new(DummyCap("foo"))).expect("register");
        assert!(r.get("foo").is_some());
        assert!(r.get("bar").is_none());
    }

    #[test]
    fn duplicate_register_errors() {
        let r = CapabilityRegistry::new();
        r.register(Arc::new(DummyCap("foo"))).expect("first");
        let err = r.register(Arc::new(DummyCap("foo"))).unwrap_err();
        assert!(matches!(err, RegistryError::Duplicate(_)));
    }

    #[test]
    fn manifests_returns_sorted() {
        let r = CapabilityRegistry::new();
        r.register(Arc::new(DummyCap("zed"))).expect("zed");
        r.register(Arc::new(DummyCap("alpha"))).expect("alpha");
        r.register(Arc::new(DummyCap("middle"))).expect("middle");
        let ids: Vec<String> = r.manifests().into_iter().map(|m| m.id).collect();
        assert_eq!(ids, vec!["alpha", "middle", "zed"]);
    }

    #[tokio::test]
    async fn execute_via_registry_returns_input_for_dummy() {
        let r = CapabilityRegistry::new();
        r.register(Arc::new(DummyCap("foo"))).expect("register");
        let cap = r.get("foo").expect("present");
        let ctx = ExecutionContext {
            local_node: NodeId::from_bytes([1; 16]),
            local_node_name: Arc::from("self"),
            issued_by: NodeId::from_bytes([2; 16]),
            issued_by_name: Arc::from("issuer"),
            task_id: TaskId::new_v7(),
        };
        let out = cap
            .execute(&ctx, serde_json::json!({"hi": "there"}))
            .await
            .expect("execute");
        assert_eq!(out, serde_json::json!({"hi": "there"}));
    }
}
