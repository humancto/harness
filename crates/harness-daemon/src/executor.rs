//! Local executor loop — Phase 3.3a.
//!
//! Walks the PRD §14.1 lifecycle ladder
//! `Submitted → Dispatched → Claimed → Running → Done|Failed` for tasks
//! the local daemon owns. Each hop is a `try_transition_task` CAS so
//! 3.3-fanout can drop in cross-node dispatch without rewriting the
//! executor — the dispatcher will hop `Submitted → Dispatched` on a
//! different node, and this loop will pick up at `Dispatched → Claimed`.
//!
//! See ADR-0009 for the full rationale.

use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use harness_capabilities::traits::ExecutionContext;
use harness_capabilities::CapabilityRegistry;
use harness_core::{NodeId, ReplicatedState, ReplicatedTaskState, TaskId};
use harness_store::{Store, TaskState};
use serde_json::Value as JsonValue;
use tokio::sync::watch;
use tokio::time::MissedTickBehavior;

const POLL_INTERVAL_MS: u64 = 100;
const POLL_BATCH: usize = 8;

/// Per-task local executor. Cheap to clone; all expensive state is
/// behind `Arc`s.
#[derive(Clone)]
pub(crate) struct LocalExecutor {
    store: Store,
    registry: CapabilityRegistry,
    local_node: NodeId,
    local_node_name: Arc<str>,
    sem: Arc<tokio::sync::Semaphore>,
}

impl std::fmt::Debug for LocalExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalExecutor")
            .field("local_node", &self.local_node)
            .field("local_node_name", &self.local_node_name)
            .finish_non_exhaustive()
    }
}

impl LocalExecutor {
    pub(crate) fn new(
        store: Store,
        registry: CapabilityRegistry,
        local_node: NodeId,
        local_node_name: Arc<str>,
        max_concurrent: usize,
    ) -> Self {
        let max = max_concurrent.max(1);
        Self {
            store,
            registry,
            local_node,
            local_node_name,
            sem: Arc::new(tokio::sync::Semaphore::new(max)),
        }
    }

    /// Defaults `max_concurrent` to `available_parallelism().clamp(2, 8)`.
    pub(crate) fn with_default_concurrency(
        store: Store,
        registry: CapabilityRegistry,
        local_node: NodeId,
        local_node_name: Arc<str>,
    ) -> Self {
        let max = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(4)
            .clamp(2, 8);
        Self::new(store, registry, local_node, local_node_name, max)
    }

    /// Drive the loop until `shutdown` flips `true`.
    pub(crate) async fn run_forever(self, mut shutdown: watch::Receiver<bool>) {
        let mut tick = tokio::time::interval(Duration::from_millis(POLL_INTERVAL_MS));
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = tick.tick() => self.poll_once().await,
                _ = shutdown.changed() => return,
            }
        }
    }

    /// One polling iteration. Public so tests can step the loop
    /// deterministically.
    pub(crate) async fn poll_once(&self) {
        let rows = match self.store.list_tasks_by_state(TaskState::Submitted) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(target: "harness.executor", ?e, "list_tasks_by_state");
                return;
            }
        };
        for row in rows.into_iter().take(POLL_BATCH) {
            let id = row.id;
            let cap_id = row.capability;

            // Climb the ladder: Submitted → Dispatched → Claimed → Running.
            // Each CAS targets a single legal hop. If any hop loses (someone
            // else got there first, or the row vanished), abandon and try
            // again next tick.
            match self
                .store
                .try_transition_task(id, TaskState::Submitted, TaskState::Dispatched)
            {
                Ok(true) => {}
                Ok(false) => continue,
                Err(e) => {
                    tracing::warn!(target: "harness.executor", ?e, "submitted→dispatched");
                    continue;
                }
            }
            match self
                .store
                .try_transition_task(id, TaskState::Dispatched, TaskState::Claimed)
            {
                Ok(true) => {}
                Ok(false) => continue,
                Err(e) => {
                    tracing::warn!(target: "harness.executor", ?e, "dispatched→claimed");
                    continue;
                }
            }
            match self
                .store
                .try_transition_task(id, TaskState::Claimed, TaskState::Running)
            {
                Ok(true) => {}
                Ok(false) => continue,
                Err(e) => {
                    tracing::warn!(target: "harness.executor", ?e, "claimed→running");
                    continue;
                }
            }

            self.spawn_execute(id, cap_id).await;
        }
    }

    async fn spawn_execute(&self, id: TaskId, capability: String) {
        let Some(cap) = self.registry.get(&capability) else {
            self.fail_now_sync(id, &format!("capability not found: {capability}"));
            return;
        };
        let task = match self.store.load_task(id) {
            Ok(Some(t)) => t,
            Ok(None) => {
                tracing::warn!(target: "harness.executor", task_id = ?id, "load_task missing");
                return;
            }
            Err(e) => {
                tracing::warn!(target: "harness.executor", ?e, "load_task");
                return;
            }
        };
        let Ok(permit) = self.sem.clone().acquire_owned().await else {
            tracing::error!(target: "harness.executor", "semaphore closed");
            return;
        };

        let store = self.store.clone();
        let local_node = self.local_node;
        let local_node_name = self.local_node_name.clone();

        tokio::spawn(async move {
            let _permit = permit;

            // 3.3a single-daemon: issuer == self. 3.3-fanout will plumb
            // real issuer names from the manifest map. Warn until then.
            if task.issued_by != local_node {
                tracing::warn!(
                    target: "harness.executor",
                    issued_by = ?task.issued_by,
                    "task issued by non-local node; 3.3-fanout will plumb issuer name (ADR-0009)"
                );
            }

            let ctx = ExecutionContext {
                local_node,
                local_node_name: local_node_name.clone(),
                issued_by: task.issued_by,
                issued_by_name: local_node_name.clone(),
                task_id: id,
            };

            // S2: panic boundary. A panicking capability cannot wedge
            // the daemon — write a Failed terminal, free the permit.
            let outcome = std::panic::AssertUnwindSafe(cap.execute(&ctx, task.input))
                .catch_unwind()
                .await;

            let now = now_unix_ms();
            match outcome {
                Ok(Ok(output)) => {
                    let _ = store.try_transition_task(id, TaskState::Running, TaskState::Done);
                    if let Err(e) = store.write_task_result_done(id, &output, now, local_node) {
                        tracing::warn!(target: "harness.executor", ?e, "write_task_result_done");
                    }
                    let _ = store.replica_apply_local(&done_replica(id, now, local_node, &output));
                }
                Ok(Err(e)) => {
                    let msg = e.to_string();
                    let _ = store.try_transition_task(id, TaskState::Running, TaskState::Failed);
                    if let Err(we) = store.write_task_result_failed(id, &msg, now, local_node) {
                        tracing::warn!(target: "harness.executor", ?we, "write_task_result_failed");
                    }
                    let _ = store.replica_apply_local(&failed_replica(id, now, local_node, &msg));
                }
                Err(payload) => {
                    let msg = format!("capability panicked: {}", describe_panic(payload.as_ref()));
                    let _ = store.try_transition_task(id, TaskState::Running, TaskState::Failed);
                    if let Err(we) = store.write_task_result_failed(id, &msg, now, local_node) {
                        tracing::warn!(target: "harness.executor", ?we, "write_task_result_failed (panic)");
                    }
                    let _ = store.replica_apply_local(&failed_replica(id, now, local_node, &msg));
                }
            }
        });
    }

    /// Used when the capability id is unknown — short-circuit to Failed
    /// without trying to climb the rest of the ladder. Caller is at
    /// `Running`.
    fn fail_now_sync(&self, id: TaskId, msg: &str) {
        let now = now_unix_ms();
        let _ = self
            .store
            .try_transition_task(id, TaskState::Running, TaskState::Failed);
        if let Err(e) = self
            .store
            .write_task_result_failed(id, msg, now, self.local_node)
        {
            tracing::warn!(target: "harness.executor", ?e, "write_task_result_failed (fail_now)");
        }
        let _ = self
            .store
            .replica_apply_local(&failed_replica(id, now, self.local_node, msg));
    }
}

fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn describe_panic(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        return (*s).to_string();
    }
    if let Some(s) = payload.downcast_ref::<String>() {
        return s.clone();
    }
    "<non-string panic payload>".to_string()
}

fn done_replica(id: TaskId, at_ms: u64, source: NodeId, output: &JsonValue) -> ReplicatedTaskState {
    // Truncated to the first 256 bytes of `serde_json::to_vec`. NOT
    // parseable JSON — see ADR-0009. UI/dashboards must hit
    // `GET /tasks/<id>` for the full output.
    let preview = serde_json::to_vec(output)
        .ok()
        .map(|v| v.into_iter().take(256).collect::<Vec<u8>>());
    ReplicatedTaskState {
        task_id: id,
        state: ReplicatedState::Done,
        at_ms,
        source,
        output_preview: preview,
    }
}

fn failed_replica(id: TaskId, at_ms: u64, source: NodeId, msg: &str) -> ReplicatedTaskState {
    let preview = msg
        .as_bytes()
        .iter()
        .copied()
        .take(256)
        .collect::<Vec<u8>>();
    ReplicatedTaskState {
        task_id: id,
        state: ReplicatedState::Failed,
        at_ms,
        source,
        output_preview: Some(preview),
    }
}

/// Resolve the local node's mesh hostname. Defaults to OS hostname;
/// `HARNESS_NODE_NAME` env override; falls back to `"unknown"`.
pub(crate) fn default_node_name() -> String {
    if let Ok(s) = std::env::var("HARNESS_NODE_NAME") {
        if !s.is_empty() {
            return s;
        }
    }
    hostname::get()
        .ok()
        .and_then(|s| s.into_string().ok())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::unnecessary_literal_bound
)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use harness_capabilities::traits::{Capability, CapabilityError};
    use harness_capabilities::CapabilityRegistry;
    use harness_core::{
        Capability as ManifestEntry, Cardinality, Constraints, ExecutionPolicy, Identity,
        ResourceHints, RetryPolicy, SemVer, Signable, Signature, Task, TraceContext,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct EchoCap;

    #[async_trait]
    impl Capability for EchoCap {
        fn id(&self) -> &str {
            "echo"
        }
        fn manifest(&self) -> ManifestEntry {
            manifest_for("echo")
        }
        async fn execute(
            &self,
            _: &ExecutionContext,
            input: JsonValue,
        ) -> Result<JsonValue, CapabilityError> {
            Ok(serde_json::json!({"echoed": input}))
        }
    }

    struct PanicCap;

    #[async_trait]
    impl Capability for PanicCap {
        fn id(&self) -> &str {
            "panicker"
        }
        fn manifest(&self) -> ManifestEntry {
            manifest_for("panicker")
        }
        async fn execute(
            &self,
            _: &ExecutionContext,
            _: JsonValue,
        ) -> Result<JsonValue, CapabilityError> {
            panic!("boom from PanicCap")
        }
    }

    /// Counts how many times `execute()` was invoked. Used for the
    /// terminal-idempotence test.
    struct CountingCap(Arc<AtomicUsize>);

    #[async_trait]
    impl Capability for CountingCap {
        fn id(&self) -> &str {
            "counting"
        }
        fn manifest(&self) -> ManifestEntry {
            manifest_for("counting")
        }
        async fn execute(
            &self,
            _: &ExecutionContext,
            _: JsonValue,
        ) -> Result<JsonValue, CapabilityError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(serde_json::json!({"count": self.0.load(Ordering::SeqCst)}))
        }
    }

    fn manifest_for(id: &str) -> ManifestEntry {
        ManifestEntry {
            id: id.to_string(),
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

    fn fresh_store() -> Store {
        Store::open_memory().expect("open memory store")
    }

    fn registry_with(cap: Arc<dyn Capability>) -> CapabilityRegistry {
        let r = CapabilityRegistry::new();
        r.register(cap).expect("register");
        r
    }

    fn dummy_task(id: TaskId, capability: &str, input: JsonValue, issuer: NodeId) -> Task {
        let mut t = Task {
            id,
            parent: None,
            plan_id: None,
            capability: capability.to_string(),
            input,
            constraints: Constraints::default(),
            retry: RetryPolicy::default(),
            execution: ExecutionPolicy::default(),
            resource_hints: ResourceHints {
                cpu_class: harness_core::protocol::CpuClass::Light,
                memory_mb: None,
                gpu_required: false,
                gpu_memory_mb: None,
                network_class: harness_core::protocol::NetworkClass::None,
                disk_io_class: harness_core::protocol::DiskIoClass::None,
                estimated_duration_ms: None,
            },
            trace_ctx: TraceContext::default(),
            issued_by: issuer,
            issued_at: 1_700_000_000_000,
            sig: Signature::from_bytes([0u8; 64]),
        };
        t.sign(&Identity::generate()).expect("sign");
        t
    }

    fn local_node() -> NodeId {
        NodeId::from_bytes([7u8; 16])
    }

    /// Wait up to ~2s for the task to leave the Running state.
    async fn wait_terminal(store: &Store, id: TaskId) -> TaskState {
        for _ in 0..200 {
            let st = store.task_state(id).expect("task_state").expect("present");
            if matches!(st, TaskState::Done | TaskState::Failed) {
                return st;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("task {id:?} did not reach terminal state within 2s");
    }

    // t07 — happy path: Submitted echo runs, lands at Done with output.
    #[tokio::test]
    async fn t07_executor_picks_up_submitted_echo() {
        let store = fresh_store();
        let registry = registry_with(Arc::new(EchoCap));
        let exec = LocalExecutor::new(store.clone(), registry, local_node(), Arc::from("self"), 2);

        let id = TaskId::new_v7();
        store
            .insert_task(&dummy_task(
                id,
                "echo",
                serde_json::json!({"msg": "hi"}),
                local_node(),
            ))
            .expect("insert");

        exec.poll_once().await;
        let st = wait_terminal(&store, id).await;
        assert_eq!(st, TaskState::Done);

        let result = store.load_task_result(id).expect("load").expect("present");
        assert_eq!(
            result.output,
            Some(serde_json::json!({"echoed": {"msg": "hi"}}))
        );
        assert!(result.error.is_none());
    }

    // t08 — unknown capability: Failed with descriptive error.
    #[tokio::test]
    async fn t08_executor_handles_unknown_capability() {
        let store = fresh_store();
        let registry = CapabilityRegistry::new();
        let exec = LocalExecutor::new(store.clone(), registry, local_node(), Arc::from("self"), 1);

        let id = TaskId::new_v7();
        store
            .insert_task(&dummy_task(
                id,
                "does.not.exist",
                serde_json::json!({}),
                local_node(),
            ))
            .expect("insert");

        exec.poll_once().await;
        let st = wait_terminal(&store, id).await;
        assert_eq!(st, TaskState::Failed);

        let result = store.load_task_result(id).expect("load").expect("present");
        assert!(result
            .error
            .as_deref()
            .unwrap_or("")
            .contains("capability not found"));
    }

    // t09 — capability panic does not wedge the daemon (S2 boundary).
    #[tokio::test]
    async fn t09_capability_panic_becomes_failed() {
        let store = fresh_store();
        let registry = registry_with(Arc::new(PanicCap));
        let exec = LocalExecutor::new(store.clone(), registry, local_node(), Arc::from("self"), 1);

        let id = TaskId::new_v7();
        store
            .insert_task(&dummy_task(
                id,
                "panicker",
                serde_json::json!({}),
                local_node(),
            ))
            .expect("insert");

        exec.poll_once().await;
        let st = wait_terminal(&store, id).await;
        assert_eq!(st, TaskState::Failed);

        let result = store.load_task_result(id).expect("load").expect("present");
        assert!(result.error.as_deref().unwrap_or("").contains("panicked"));
    }

    // t10 — terminal idempotence: a Done task is not re-dispatched on
    // subsequent polls. Capability invoked exactly once.
    #[tokio::test]
    async fn t10_terminal_idempotence() {
        let counter = Arc::new(AtomicUsize::new(0));
        let store = fresh_store();
        let registry = registry_with(Arc::new(CountingCap(counter.clone())));
        let exec = LocalExecutor::new(store.clone(), registry, local_node(), Arc::from("self"), 1);

        let id = TaskId::new_v7();
        store
            .insert_task(&dummy_task(
                id,
                "counting",
                serde_json::json!({}),
                local_node(),
            ))
            .expect("insert");

        exec.poll_once().await;
        wait_terminal(&store, id).await;
        // Three more polls — should not re-run.
        exec.poll_once().await;
        exec.poll_once().await;
        exec.poll_once().await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    // t11 — run_forever drains cleanly when shutdown flips.
    #[tokio::test]
    async fn t11_run_forever_exits_on_shutdown() {
        let store = fresh_store();
        let registry = registry_with(Arc::new(EchoCap));
        let exec = LocalExecutor::new(store, registry, local_node(), Arc::from("self"), 1);

        let (tx, rx) = tokio::sync::watch::channel(false);
        let handle = tokio::spawn(async move { exec.run_forever(rx).await });

        // Let the loop run for a tick or two.
        tokio::time::sleep(Duration::from_millis(50)).await;
        tx.send(true).expect("send shutdown");

        // Should resolve well within 500ms.
        let r = tokio::time::timeout(Duration::from_millis(500), handle).await;
        assert!(r.is_ok(), "executor must exit on shutdown");
    }

    // t12 — replica preview is the JSON-encoded prefix-256 (NOT
    // parseable JSON). ADR-0009.
    #[tokio::test]
    async fn t12_replica_preview_set_on_done() {
        let store = fresh_store();
        let registry = registry_with(Arc::new(EchoCap));
        let exec = LocalExecutor::new(store.clone(), registry, local_node(), Arc::from("self"), 1);

        let id = TaskId::new_v7();
        let big = "x".repeat(2000);
        store
            .insert_task(&dummy_task(
                id,
                "echo",
                serde_json::json!({"big": big}),
                local_node(),
            ))
            .expect("insert");

        exec.poll_once().await;
        wait_terminal(&store, id).await;

        let snap = store.replica_snapshot().expect("snapshot");
        let entry = snap
            .iter()
            .find(|e| e.task_id == id)
            .expect("replica entry");
        let preview = entry.output_preview.as_ref().expect("preview present");
        assert!(preview.len() <= 256, "preview must be ≤256 bytes");
        // Preview is NOT necessarily parseable JSON (truncated mid-string).
        // Just sanity-check it starts with the expected JSON prefix.
        assert!(preview.starts_with(b"{\"echoed\":"));
    }
}
