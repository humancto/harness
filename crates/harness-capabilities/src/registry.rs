//! `CapabilityRegistry` — the in-process map of `id → Arc<dyn Capability>`.
//!
//! Used by:
//! - the daemon to assemble its `NodeManifest::capabilities` advertisement
//! - the worker (when 2.4's runtime wires through) to look up the
//!   handler for an incoming Task

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use harness_core::{Capability as ManifestEntry, CapabilityRef};
use parking_lot::RwLock;
use thiserror::Error;

use crate::traits::Capability;

type Inner = RwLock<HashMap<String, Arc<dyn Capability>>>;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RegistryError {
    #[error("capability {0:?} already registered")]
    Duplicate(String),
}

/// Concurrent registry. Cheaply cloneable (Arc internal).
#[derive(Clone, Default)]
pub struct CapabilityRegistry {
    inner: Arc<Inner>,
}

/// `Weak` companion to [`CapabilityRegistry`] used by capabilities that
/// need to *observe* the registry's contents without extending its
/// lifetime (e.g. `brain.plan`, which snapshots the available-capability
/// list per `PlanRequest`).
///
/// Holding a strong `Arc<CapabilityRegistry>` in a capability that lives
/// inside the registry would create a refcount cycle and leak the
/// registry on daemon shutdown. `WeakCapabilityRegistry` breaks that
/// cycle: when the daemon drops the last `CapabilityRegistry` clone,
/// `upgrade()` returns `None` and `refs()` yields `Vec::new()`, which
/// surfaces as a clean `Failed("no backend ...")` from the executor.
#[derive(Clone, Debug, Default)]
pub struct WeakCapabilityRegistry {
    inner: Weak<Inner>,
}

impl WeakCapabilityRegistry {
    /// Snapshot the currently-registered capabilities into
    /// [`CapabilityRef`]s. Returns `Vec::new()` if the registry has
    /// been dropped. Always-on; brain-free.
    #[must_use]
    pub fn refs(&self) -> Vec<CapabilityRef> {
        let Some(strong) = self.inner.upgrade() else {
            return Vec::new();
        };
        let g = strong.read();
        g.values()
            .map(|c| {
                let m = c.manifest();
                CapabilityRef {
                    id: m.id,
                    version_major: m.version.major,
                }
            })
            .collect()
    }

    /// Look up one capability without extending the registry's
    /// lifetime. `None` when the id is unregistered or the registry
    /// has been dropped. The 3.11 mesh meta-capabilities use this to
    /// run `fs.*` sub-calls in-process for self-owned scopes.
    #[must_use]
    pub fn get(&self, id: &str) -> Option<Arc<dyn Capability>> {
        let strong = self.inner.upgrade()?;
        let g = strong.read();
        g.get(id).cloned()
    }

    /// Atomic snapshot of `(refs, schemas)` under one `RwLock::read()`
    /// so a concurrent registration cannot give callers inconsistent
    /// views. Available only when the `brain` feature is on (because
    /// `CapabilitySchemaIndex` lives in `harness-brain`).
    ///
    /// Schemas that fail to compile are silently dropped from the
    /// index (with a `tracing::warn!` from the index builder); plans
    /// referencing those caps surface
    /// [`harness_brain::PlanValidationError::UnknownSchema`] at
    /// validation time.
    #[cfg(feature = "brain")]
    #[must_use]
    pub fn snapshot(&self) -> CapabilitySnapshot {
        use harness_brain::CapabilitySchemaIndex;

        let Some(strong) = self.inner.upgrade() else {
            return CapabilitySnapshot {
                refs: Vec::new(),
                schemas: CapabilitySchemaIndex::default(),
            };
        };
        let g = strong.read();
        let mut refs = Vec::with_capacity(g.len());
        let mut schema_pairs = Vec::with_capacity(g.len());
        for cap in g.values() {
            let m = cap.manifest();
            refs.push(CapabilityRef {
                id: m.id.clone(),
                version_major: m.version.major,
            });
            schema_pairs.push((m.id, m.input_schema));
        }
        CapabilitySnapshot {
            refs,
            schemas: CapabilitySchemaIndex::from_pairs(schema_pairs),
        }
    }
}

/// Atomic snapshot of the registry surfaces a planner backend needs:
/// the cap-ref list (id + `version_major`) for prompt construction and
/// the compiled per-cap schema index for plan validation. Built once
/// per `brain.plan` invocation and shared across the escalation chain
/// so every backend sees the same view.
///
/// Phase 3.10+ may extend with `cost_hints` for cost-aware routing —
/// the struct shape is the right place to add the field.
#[cfg(feature = "brain")]
#[derive(Clone, Debug, Default)]
pub struct CapabilitySnapshot {
    pub refs: Vec<CapabilityRef>,
    pub schemas: harness_brain::CapabilitySchemaIndex,
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

    /// Build a [`WeakCapabilityRegistry`] handle that observes this
    /// registry without extending its lifetime. See the
    /// `WeakCapabilityRegistry` doc-comment for the rationale.
    #[must_use]
    pub fn downgrade(&self) -> WeakCapabilityRegistry {
        WeakCapabilityRegistry {
            inner: Arc::downgrade(&self.inner),
        }
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
                requires_secrets: vec![],
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
            tags: std::sync::Arc::from(Vec::<String>::new()),
        };
        let out = cap
            .execute(&ctx, serde_json::json!({"hi": "there"}))
            .await
            .expect("execute");
        assert_eq!(out, serde_json::json!({"hi": "there"}));
    }
}
