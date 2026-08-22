//! [`StoreMeshExec`] — the daemon's implementation of
//! `harness_capabilities::MeshExec` (roadmap 3.11, ADR-0022).
//!
//! Target discovery reads the stored `NodeManifest`s (self included —
//! `PeerNet::new` indexes the self manifest at boot) filtered to live
//! nodes. Self-owned scopes execute in-process through the weak
//! capability registry (no second executor permit — the deadlock-free
//! design from ADR-0022); remote scopes become pinned sub-tasks that
//! the `DispatchRuntime` routes over QUIC.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use harness_capabilities::{
    CapabilityError, ExecutionContext, MeshExec, MeshTarget, SubTaskOutcome, WeakCapabilityRegistry,
};
use harness_core::{
    Constraints, ExecutionPolicy, Identity, NodeId, ResourceHints, RetryPolicy, Signable,
    Signature, Task, TaskId, TraceContext,
};
use harness_mesh::heartbeat::{PeerTable, PEER_TIMEOUT};
use harness_store::{Store, TaskState};

const AWAIT_POLL_MS: u64 = 100;

pub(crate) struct StoreMeshExec {
    store: Store,
    identity: Arc<Identity>,
    registry: WeakCapabilityRegistry,
    peers: PeerTable,
    local_id: NodeId,
}

impl StoreMeshExec {
    pub(crate) fn new(
        store: Store,
        identity: Arc<Identity>,
        registry: WeakCapabilityRegistry,
        peers: PeerTable,
    ) -> Arc<Self> {
        let local_id = identity.node_id();
        Arc::new(Self {
            store,
            identity,
            registry,
            peers,
            local_id,
        })
    }
}

#[async_trait]
impl MeshExec for StoreMeshExec {
    fn targets(&self) -> Vec<MeshTarget> {
        let manifests = match self.store.list_manifests() {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(target: "harness.mesh_meta", ?e, "list_manifests");
                return Vec::new();
            }
        };
        let mut out: Vec<MeshTarget> = manifests
            .into_iter()
            .filter(|m| m.node_id == self.local_id || self.peers.is_live(&m.node_id, PEER_TIMEOUT))
            .map(|m| MeshTarget {
                node_id: m.node_id,
                node_name: if m.hostname.is_empty() {
                    m.node_id.to_string()
                } else {
                    m.hostname.clone()
                },
                is_self: m.node_id == self.local_id,
                scopes: m.scopes.iter().map(|s| s.id.clone()).collect(),
                capabilities: m.capabilities.iter().map(|c| c.id.clone()).collect(),
            })
            .collect();
        // Self first, then by node id — deterministic fan-out order.
        out.sort_by_key(|t| (!t.is_self, t.node_id));
        out
    }

    async fn run_local(
        &self,
        capability: &str,
        ctx: &ExecutionContext,
        input: serde_json::Value,
    ) -> Result<serde_json::Value, CapabilityError> {
        // Recursion guard: meta-capabilities never invoke each other.
        if capability.starts_with("mesh.") {
            return Err(CapabilityError::InvalidInput(
                "mesh.* capabilities cannot be invoked from a mesh wrapper".into(),
            ));
        }
        let Some(cap) = self.registry.get(capability) else {
            return Err(CapabilityError::Failed(format!(
                "capability not available locally: {capability}"
            )));
        };
        cap.execute(ctx, input).await
    }

    fn submit_remote(
        &self,
        capability: &str,
        input: serde_json::Value,
        pin_to: NodeId,
        parent: TaskId,
        timeout_ms: u32,
    ) -> Result<TaskId, CapabilityError> {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
            .unwrap_or(0);
        let mut task = Task {
            id: TaskId::new_v7(),
            parent: Some(parent),
            plan_id: None,
            capability: capability.to_string(),
            input,
            constraints: Constraints {
                pin_to_node: Some(pin_to),
                ..Constraints::default()
            },
            retry: RetryPolicy {
                // No lease-expiry retry: the lease TTL (timeout + slack)
                // always outlives the wrapper's await deadline, so a
                // retry could only ever fire posthumously — pure orphan
                // work (review MINOR-1/2, ADR-0022 §Orphaned sub-tasks).
                max_attempts: 1,
                ..RetryPolicy::default()
            },
            execution: ExecutionPolicy {
                timeout_ms,
                ..ExecutionPolicy::default()
            },
            resource_hints: ResourceHints {
                cpu_class: harness_core::protocol::CpuClass::Light,
                memory_mb: None,
                gpu_required: false,
                gpu_memory_mb: None,
                network_class: harness_core::protocol::NetworkClass::Light,
                disk_io_class: harness_core::protocol::DiskIoClass::Light,
                estimated_duration_ms: None,
            },
            trace_ctx: TraceContext::default(),
            issued_by: self.local_id,
            issued_at: now_ms,
            tags: vec![],
            sig: Signature::from_bytes([0u8; 64]),
        };
        task.sign(&self.identity)
            .map_err(|e| CapabilityError::Failed(format!("sign sub-task: {e}")))?;
        self.store
            .insert_task(&task)
            .map_err(|e| CapabilityError::Failed(format!("insert sub-task: {e}")))?;
        Ok(task.id)
    }

    async fn await_terminal(&self, id: TaskId, deadline: Duration) -> SubTaskOutcome {
        let until = tokio::time::Instant::now() + deadline;
        loop {
            match self.store.task_state(id) {
                Ok(Some(TaskState::Done)) => {
                    return match self.store.load_task_result(id) {
                        Ok(Some(row)) => {
                            SubTaskOutcome::Done(row.output.unwrap_or(serde_json::Value::Null))
                        }
                        _ => SubTaskOutcome::Failed("done without result row".into()),
                    };
                }
                Ok(Some(TaskState::Failed | TaskState::Expired | TaskState::Cancelled)) => {
                    let err = self
                        .store
                        .load_task_result(id)
                        .ok()
                        .flatten()
                        .and_then(|r| r.error)
                        .unwrap_or_else(|| "sub-task failed".into());
                    return SubTaskOutcome::Failed(err);
                }
                Ok(_) => {}
                Err(e) => return SubTaskOutcome::Failed(format!("store error: {e}")),
            }
            if tokio::time::Instant::now() >= until {
                return SubTaskOutcome::TimedOut;
            }
            tokio::time::sleep(Duration::from_millis(AWAIT_POLL_MS)).await;
        }
    }
}
