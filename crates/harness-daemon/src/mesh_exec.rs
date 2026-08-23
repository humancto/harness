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
use harness_store::Store;

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
        // Shared with the federated coordinator (4.5) — signed,
        // parent-linked, pinned, max_attempts=1 (posthumous-work rule).
        let task = crate::subtask::build_pinned_subtask(
            &self.identity,
            capability,
            input,
            pin_to,
            parent,
            timeout_ms,
        )?;
        self.store
            .insert_task(&task)
            .map_err(|e| CapabilityError::Failed(format!("insert sub-task: {e}")))?;
        Ok(task.id)
    }

    async fn await_terminal(&self, id: TaskId, deadline: Duration) -> SubTaskOutcome {
        self.await_terminal_impl(id, deadline).await
    }
}

/// 4.3 (ADR-0025): the DAG executor's daemon services — same store /
/// identity / peer plumbing, unpinned submits with `plan_id` set.
#[async_trait]
impl harness_capabilities::PlanExec for StoreMeshExec {
    fn submit_step(
        &self,
        capability: &str,
        input: serde_json::Value,
        parent: TaskId,
        plan_id: harness_core::PlanId,
        resource_hints: ResourceHints,
        timeout_ms: u32,
    ) -> Result<TaskId, CapabilityError> {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
            .unwrap_or(0);
        let mut task = Task {
            id: TaskId::new_v7(),
            parent: Some(parent),
            plan_id: Some(plan_id),
            capability: capability.to_string(),
            input,
            // UNPINNED — the dispatch runtime places it by cardinality
            // over the live mesh (self included); policy is evaluated
            // on the executing node (rule 10). Deadline (4.7, plan
            // review BLOCKER-2): the step's pre-dispatch wait is
            // bounded by the plan's await budget — no posthumous runs.
            constraints: Constraints {
                deadline: Some(now_ms.saturating_add(u64::from(timeout_ms))),
                ..Constraints::default()
            },
            retry: RetryPolicy {
                // Same posthumous-work rule as mesh sub-tasks
                // (ADR-0022): lease TTL outlives the plan's await.
                max_attempts: 1,
                ..RetryPolicy::default()
            },
            execution: ExecutionPolicy {
                timeout_ms,
                ..ExecutionPolicy::default()
            },
            resource_hints,
            trace_ctx: TraceContext::default(),
            issued_by: self.local_id,
            issued_at: now_ms,
            tags: vec![],
            sig: Signature::from_bytes([0u8; 64]),
        };
        task.sign(&self.identity)
            .map_err(|e| CapabilityError::Failed(format!("sign step: {e}")))?;
        self.store
            .insert_task(&task)
            .map_err(|e| CapabilityError::Failed(format!("insert step: {e}")))?;
        Ok(task.id)
    }

    async fn await_terminal(&self, id: TaskId, deadline: Duration) -> SubTaskOutcome {
        self.await_terminal_impl(id, deadline).await
    }

    fn own_cancelled(&self, id: harness_core::TaskId) -> bool {
        // Local row first: the issuer's cancel endpoint flips it when
        // the plan runs where it was submitted.
        if matches!(
            self.store.task_state(id),
            Ok(Some(harness_store::TaskState::Cancelled))
        ) {
            return true;
        }
        // Remote execution (plan.execute is Anyone): the issuer's cancel
        // touches ITS row and mirrors Cancelled into the gossip layer —
        // it never rewrites this worker's ingested row. The replica view
        // is the cross-node cancellation signal (bounded by gossip
        // latency, which is fine for a spend stop).
        matches!(
            self.store.replica_view_task(id),
            Ok(Some(entry)) if entry.state == harness_core::ReplicatedState::Cancelled
        )
    }

    fn checkpoint_lookup(
        &self,
        plan: harness_core::PlanId,
        node: harness_core::TaskId,
        input_hash: &[u8; 32],
    ) -> Option<serde_json::Value> {
        match self.store.checkpoint_get(plan, node, input_hash) {
            Ok(hit) => hit,
            Err(e) => {
                tracing::warn!(target: "harness.mesh_exec", ?e, "checkpoint lookup");
                None
            }
        }
    }

    fn checkpoint_record(
        &self,
        plan: harness_core::PlanId,
        node: harness_core::TaskId,
        input_hash: &[u8; 32],
        step_row: Option<harness_core::TaskId>,
        output: &serde_json::Value,
    ) {
        // Defensive: `task_id` is NOT NULL, and every caller reaches
        // here from a Done outcome whose row was recorded at submit.
        // Nothing to write if that ever stops holding.
        let Some(row) = step_row else {
            return;
        };
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
            .unwrap_or(0);
        match self
            .store
            .checkpoint_put(plan, node, input_hash, row, output, now)
        {
            Ok(true) => {}
            Ok(false) => tracing::info!(
                target: "harness.mesh_exec",
                task = %row.0,
                "output too large to checkpoint; the step re-runs on resume"
            ),
            Err(e) => tracing::warn!(target: "harness.mesh_exec", ?e, "checkpoint record"),
        }
    }

    fn live_workers(&self) -> usize {
        self.peers.live_snapshot(PEER_TIMEOUT).len() + 1
    }

    fn known_capabilities(&self) -> Vec<harness_core::Capability> {
        // Union over stored manifests (self included — the self
        // manifest is indexed at boot with every registered
        // capability), deduplicated by id.
        let mut seen = std::collections::HashSet::new();
        let mut out = Vec::new();
        match self.store.list_manifests() {
            Ok(manifests) => {
                for m in manifests {
                    // 4.4 (4.3 carry): same liveness predicate as
                    // targets() — a departed peer's capability must not
                    // validate a plan step that then dies at dispatch.
                    if m.node_id != self.local_id && !self.peers.is_live(&m.node_id, PEER_TIMEOUT) {
                        continue;
                    }
                    for cap in m.capabilities {
                        if seen.insert(cap.id.clone()) {
                            out.push(cap);
                        }
                    }
                }
            }
            Err(e) => {
                tracing::warn!(target: "harness.plan_exec", ?e, "list_manifests");
            }
        }
        out
    }
}

impl StoreMeshExec {
    async fn await_terminal_impl(&self, id: TaskId, deadline: Duration) -> SubTaskOutcome {
        crate::subtask::await_terminal(&self.store, id, deadline).await
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use harness_capabilities::PlanExec as _;
    use harness_core::{NodeManifest, Resources, SemVer};

    fn manifest_with_cap(id: &Identity, cap_id: &str) -> NodeManifest {
        let mut m = NodeManifest {
            node_id: id.node_id(),
            hostname: "h".into(),
            pubkey: *id.public_key(),
            capabilities: vec![harness_core::Capability {
                id: cap_id.into(),
                version: SemVer::new(0, 1, 0),
                cardinality: harness_core::Cardinality::Anyone,
                input_schema: serde_json::json!({ "type": "object" }),
                output_schema: serde_json::json!({ "type": "object" }),
                cost_hint: harness_core::protocol::CostHint::LocalFast,
                tags: vec![],
                rate_limit: None,
                resource_hints: harness_core::ResourceHints {
                    cpu_class: harness_core::protocol::CpuClass::Light,
                    memory_mb: None,
                    gpu_required: false,
                    gpu_memory_mb: None,
                    network_class: harness_core::protocol::NetworkClass::None,
                    disk_io_class: harness_core::protocol::DiskIoClass::None,
                    estimated_duration_ms: None,
                },
                requires_secrets: vec![],
            }],
            scopes: vec![],
            secret_tags: vec![],
            resources: Resources {
                cpu_cores: 0,
                ram_total_mb: 0,
                gpu: None,
                os: "test".into(),
                arch: "test".into(),
            },
            online_since: 0,
            version: SemVer::new(0, 1, 0),
            sig: Signature::from_bytes([0; 64]),
        };
        m.sign(id).expect("sign");
        m
    }

    fn live_heartbeat(node: NodeId) -> harness_core::Heartbeat {
        harness_core::Heartbeat {
            node_id: node,
            seq: 1,
            timestamp: 1_700_000_000_000,
            queue_depth: 0,
            cpu_busy_pct: 0,
            cpu_pinned_count: 0,
            ram_used_mb: 0,
            ram_total_mb: 0,
            gpu_used_mb: 0,
            gpu_total_mb: 0,
            capabilities_hash: [0u8; 16],
            replica_head: [0u8; 32],
            in_flight: vec![],
            leader_belief: node,
            brain_score: 0,
            on_battery: false,
            paused: false,
            version: SemVer::new(0, 1, 0),
            sig: Signature::from_bytes([0u8; 64]),
        }
    }

    #[test]
    fn own_cancelled_consumes_replicated_cancellation() {
        // 5.10 (Codex P1 on #61): when plan.execute runs on another
        // node, the issuer's cancel flips ITS row and gossips
        // Cancelled — it never rewrites this worker's ingested row.
        // own_cancelled must consume the replica view or a remote
        // stop request keeps spending.
        let store = Store::open_memory().expect("store");
        let me = Arc::new(Identity::generate());
        let exec = StoreMeshExec::new(
            store.clone(),
            me.clone(),
            harness_capabilities::CapabilityRegistry::new().downgrade(),
            PeerTable::new(),
        );
        let id = harness_core::TaskId::new_v7();
        assert!(!exec.own_cancelled(id), "no row, no replica → false");
        store
            .replica_apply_local(&harness_core::ReplicatedTaskState {
                task_id: id,
                state: harness_core::ReplicatedState::Cancelled,
                at_ms: 1,
                source: me.node_id(),
                output_preview: None,
            })
            .expect("apply");
        assert!(
            exec.own_cancelled(id),
            "gossiped Cancelled must stop the loop"
        );
    }

    #[test]
    fn known_capabilities_filters_dead_peers_keeps_self() {
        // 4.4 (4.3 carry): a departed peer's manifest must not validate
        // plans; self and live peers must.
        let store = Store::open_memory().expect("store");
        let me = Arc::new(Identity::generate());
        let live_peer = Identity::generate();
        let dead_peer = Identity::generate();
        store
            .upsert_manifest(&manifest_with_cap(&me, "self.cap"))
            .expect("self");
        store
            .upsert_manifest(&manifest_with_cap(&live_peer, "live.cap"))
            .expect("live");
        store
            .upsert_manifest(&manifest_with_cap(&dead_peer, "dead.cap"))
            .expect("dead");
        let peers = PeerTable::new();
        peers.record(live_heartbeat(live_peer.node_id()));
        // dead_peer: manifest stored, no heartbeat.
        let exec = StoreMeshExec::new(
            store,
            me,
            harness_capabilities::CapabilityRegistry::new().downgrade(),
            peers,
        );
        let ids: Vec<String> = exec
            .known_capabilities()
            .into_iter()
            .map(|c| c.id)
            .collect();
        assert!(ids.contains(&"self.cap".to_string()), "self always in");
        assert!(ids.contains(&"live.cap".to_string()), "live peer in");
        assert!(
            !ids.contains(&"dead.cap".to_string()),
            "departed peer filtered: {ids:?}"
        );
    }
}
