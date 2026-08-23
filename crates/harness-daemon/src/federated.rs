//! `FederatedCoordinator` — real `Cardinality::Federated` dispatch
//! (roadmap 4.5, ADR-0027). Replaces the ADR-0017 "first eligible node"
//! stopgap.
//!
//! On `DispatchPlan::Federated` the dispatch runtime hands the parent
//! task here. The coordinator claims it ATOMICALLY (one
//! `submitted → running(assigned=self)` CAS — the parent is never
//! observable at `Dispatched(self)`, so the local executor can never
//! double-claim it), then a detached driver fans the same input out to
//! every eligible node as pinned signed sub-tasks (4.1
//! `FanoutController`, window = all-eligible for N ≤ 64), streams
//! per-node progress frames into the 4.2 partial pipe, merges terminal
//! outputs per the capability's `MergeStrategy` (`harness-merge`), and
//! terminalizes the parent with per-node [`NodeContribution`]
//! provenance persisted in the V0006 store column.
//!
//! Failure-policy semantics (merge-time, ADR-0027):
//! - `FailFast`: first non-Ok node ends the fan-out (in-flight dropped);
//!   parent Fails with flattened per-node errors; never-run nodes are
//!   `Skipped` in provenance.
//! - `Wait`: every node's contribution is required — any non-Ok when
//!   the fan-out settles fails the parent (full provenance, per-node
//!   errors flattened into the error string). The controller still
//!   drives like `ReturnPartial`; the global deadline binds.
//! - `ReturnPartial`: Ok outputs merge (zero Ok ⇒ Failed); non-Ok nodes
//!   are reported in the merged output's `merge.failures` block.
//!
//! Known carry (ADR-0027 → 4.6): the parent is coordinator-driven, not
//! executor-driven — a daemon crash mid-coordination leaves it at
//! `Running` with no lease until 4.6's orphan sweep.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use futures::StreamExt as _;
use harness_capabilities::{FrameSink, LogFrame, StreamKind, SubTaskOutcome};
use harness_core::protocol::NodeStatus;
use harness_core::{
    Identity, MergeStrategy, NodeContribution, NodeId, PartialPolicy, ReplicatedState,
    ReplicatedTaskState, Task, TaskId,
};
use harness_merge::NodeOutput;
use harness_orchestrator::{
    results, FanoutController, FanoutSpec, FanoutSummary, ItemOutcome, ResultCtx, ResultMapper,
    WindowPolicy,
};
use harness_store::{Store, TaskState};
use parking_lot::Mutex as ParkingMutex;
use serde_json::{json, Value as JsonValue};
use tokio::sync::Semaphore;

/// Concurrent federated coordinations per daemon. try-acquired at
/// dispatch time; exhausted ⇒ the task stays `Submitted` and retries
/// next poll (queueing, never failure — the `ResourceGated` doctrine).
pub(crate) const MAX_FEDERATED_COORDINATORS: usize = 8;
/// Node fan-out ceiling; excess nodes are dropped and reported in the
/// `discovered` frame + output `merge.truncated_nodes` (mirrors
/// `mesh_meta`'s target cap).
pub(crate) const MAX_FEDERATED_NODES: usize = 64;
/// At most this many per-node errors are flattened into a Failed
/// parent's error string.
const MAX_ERRORS_IN_MESSAGE: usize = 5;
/// Per-node error snippet bound (chars) inside the flattened string.
const ERROR_SNIPPET_CHARS: usize = 160;

pub(crate) struct FederatedCoordinator {
    store: Store,
    identity: Arc<Identity>,
    local_id: NodeId,
    /// `PartialStreamer::sink()` — coordinator parents are locally
    /// issued, so frames land straight in the `PartialBuffers` ring
    /// (4.2 pipe). Unset in bare unit fixtures: frames are dropped.
    sink: OnceLock<FrameSink>,
    /// 4.7 (ADR-0029): coordination guards keep the published queue
    /// depth counting WORK, not parents awaiting sub-results.
    pause: OnceLock<Arc<crate::pause::PauseState>>,
    slots: Arc<Semaphore>,
}

impl FederatedCoordinator {
    pub(crate) fn new(store: Store, identity: Arc<Identity>) -> Arc<Self> {
        let local_id = identity.node_id();
        Arc::new(Self {
            store,
            identity,
            local_id,
            sink: OnceLock::new(),
            pause: OnceLock::new(),
            slots: Arc::new(Semaphore::new(MAX_FEDERATED_COORDINATORS)),
        })
    }

    /// Wire the partial-stream sink (lifecycle, after streamer build).
    pub(crate) fn attach_sink(&self, sink: FrameSink) {
        let _ = self.sink.set(sink);
    }

    /// Wire the shared pause switch (4.7, ADR-0029).
    pub(crate) fn attach_pause(&self, pause: Arc<crate::pause::PauseState>) {
        let _ = self.pause.set(pause);
    }

    /// Test ctor: a custom coordination-slot count.
    #[cfg(test)]
    pub(crate) fn with_slots(store: Store, identity: Arc<Identity>, slots: usize) -> Arc<Self> {
        let local_id = identity.node_id();
        Arc::new(Self {
            store,
            identity,
            local_id,
            sink: OnceLock::new(),
            pause: OnceLock::new(),
            slots: Arc::new(Semaphore::new(slots)),
        })
    }

    /// Called from `poll_submitted_once` on `DispatchPlan::Federated`.
    /// Returns `false` when no coordination slot is free — the task
    /// stays `Submitted` and is retried next poll. On `true` the parent
    /// has either been claimed (`submitted → running(self)` CAS) with a
    /// detached driver owning it to a terminal state, or the CAS was
    /// lost to a concurrent claimant (nothing to do).
    pub(crate) fn try_start(
        self: &Arc<Self>,
        task: &Task,
        nodes: Vec<NodeId>,
        merge: MergeStrategy,
        on_node_failure: PartialPolicy,
    ) -> bool {
        let Ok(permit) = Arc::clone(&self.slots).try_acquire_owned() else {
            tracing::debug!(
                target: "harness.federated",
                task = %task.id.0,
                "no coordination slot; task remains submitted"
            );
            return false;
        };
        match self.store.try_start_coordination(task.id, self.local_id) {
            Ok(true) => {}
            Ok(false) => {
                // Another coordinator (or a racing dispatch pass) owns
                // the row — release the slot, do nothing (BLOCKER-1).
                drop(permit);
                return true;
            }
            Err(e) => {
                tracing::warn!(target: "harness.federated", ?e, "try_start_coordination");
                drop(permit);
                return true;
            }
        }
        let driver = Driver {
            store: self.store.clone(),
            identity: self.identity.clone(),
            local_id: self.local_id,
            sink: self.sink.get().cloned(),
            task: task.clone(),
            merge,
            on_node_failure,
        };
        let store = self.store.clone();
        let task_id = task.id;
        let local_id = self.local_id;
        let coord_guard = self
            .pause
            .get()
            .map(crate::pause::PauseState::coordination_guard);
        tokio::spawn(async move {
            // Held for the coordination-depth RAII window (4.7). The
            // `Option<_>` wrapper hides the guard's Drop from clippy.
            #[allow(clippy::no_effect_underscore_binding)]
            let _coord_guard = coord_guard;
            // Panic boundary (diff review MAJOR-1): without it a panic
            // anywhere in the driver (sink closure, merge, terminal
            // writes) unwinds the task, releases the slot, and strands
            // the parent at `Running` with no lease until a daemon
            // restart. Mirror the executor's catch_unwind -> Failed.
            let outcome =
                futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(async move {
                    driver.drive(nodes).await;
                }))
                .await;
            if let Err(payload) = outcome {
                let msg = format!(
                    "federated coordinator panicked: {}",
                    crate::executor::describe_panic(payload.as_ref())
                );
                fail_parent(&store, task_id, local_id, &msg, &[]);
            }
            drop(permit);
        });
        true
    }
}

impl std::fmt::Debug for FederatedCoordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FederatedCoordinator")
            .field("local_id", &self.local_id)
            .field("free_slots", &self.slots.available_permits())
            .finish_non_exhaustive()
    }
}

/// One node's settled contribution, accumulated by the mapper.
#[derive(Debug, Clone)]
struct NodeSettle {
    status: NodeStatus,
    duration_ms: u64,
    output: Option<JsonValue>,
    error: Option<String>,
}

/// The detached per-coordination driver.
struct Driver {
    store: Store,
    identity: Arc<Identity>,
    local_id: NodeId,
    sink: Option<FrameSink>,
    task: Task,
    merge: MergeStrategy,
    on_node_failure: PartialPolicy,
}

impl Driver {
    #[allow(clippy::too_many_lines)]
    async fn drive(self, nodes: Vec<NodeId>) {
        let offered = nodes.len();
        let mut nodes = nodes;
        nodes.truncate(MAX_FEDERATED_NODES);
        let truncated_nodes = offered - nodes.len();
        let n = nodes.len();
        let names = self.node_names(&nodes);
        self.emit(&json!({ "federated": {
            "stage": "discovered",
            "nodes": n,
            "truncated_nodes": truncated_nodes,
        }}));

        // Global budget: the parent's own timeout, floored at 1 s.
        // Anchored at COORDINATION START, not submit — a parent that
        // queued for a slot still gets its full budget (the queueing-
        // never-failure doctrine); `constraints.deadline` remains the
        // dispatch loop's concern, not the coordinator's (ADR-0027).
        let global_ms = u64::from(self.task.execution.timeout_ms).max(1000);
        let until = tokio::time::Instant::now() + Duration::from_millis(global_ms);
        let window = WindowPolicy::default_per_workers().effective(n.max(1));

        // Pull-time instants, shared run-closure ↔ mapper so
        // `NodeContribution::duration_ms` measures in-flight time.
        let pulled_at: Arc<ParkingMutex<HashMap<u64, tokio::time::Instant>>> =
            Arc::new(ParkingMutex::new(HashMap::new()));

        let spec = self.build_spec(&nodes, until, global_ms, &pulled_at);
        let mapper = NodeMapper {
            task_id: self.task.id,
            nodes: nodes.clone(),
            names,
            pulled_at: pulled_at.clone(),
            sink: self.sink.clone(),
            completed: 0,
            settled: vec![None; n],
            summary: None,
        };
        let stream = results::task_results(
            FanoutController::stream(spec),
            ResultCtx {
                task_id: self.task.id,
                node_id: self.local_id,
                expected: Some(u64::try_from(n).unwrap_or(u64::MAX)),
            },
            mapper,
        );
        // The detached-consumer bridge (ADR-0024): the spawned driver
        // future feeds a bounded channel; we drain it here. Frames reach
        // the ring from the mapper as items settle — decoupled from
        // this drain cadence (the 4.7 backpressure seam).
        let (drv, mut rx) = results::into_channel(stream, window);
        let drv = tokio::spawn(drv);
        while rx.next().await.is_some() {}
        let mapper = match drv.await {
            Ok(m) => m,
            Err(e) => {
                self.terminalize_failed(&format!("federated driver panicked: {e}"), &[]);
                return;
            }
        };

        self.emit(&json!({ "federated": { "stage": "merging" } }));
        self.settle(&nodes, mapper, truncated_nodes);
    }

    /// Apply the failure policy + merge, then terminalize the parent.
    fn settle(&self, nodes: &[NodeId], mapper: NodeMapper, truncated_nodes: usize) {
        let n = nodes.len();
        let settled: Vec<NodeSettle> = mapper
            .settled
            .into_iter()
            .map(|s| {
                s.unwrap_or(NodeSettle {
                    // on_end fills unsettled nodes; this is a belt-and-
                    // braces default for a summary-less abort.
                    status: NodeStatus::Skipped,
                    duration_ms: 0,
                    output: None,
                    error: None,
                })
            })
            .collect();
        let ok_count = settled
            .iter()
            .filter(|s| matches!(s.status, NodeStatus::Ok))
            .count();
        let failures: Vec<(NodeId, NodeStatus, String)> = nodes
            .iter()
            .zip(&settled)
            .filter(|(_, s)| !matches!(s.status, NodeStatus::Ok))
            .map(|(node, s)| {
                (
                    *node,
                    s.status,
                    s.error
                        .clone()
                        .unwrap_or_else(|| status_str(s.status).into()),
                )
            })
            .collect();

        // Provenance is built for every terminal path — success or
        // not. item_count = items CONTRIBUTED pre-merge (ADR-0027),
        // filled here so a Wait/FailFast-failed parent still records
        // what each Ok node did contribute (PR #48 review): identical
        // by construction to `Merged::item_counts` (the merge engine
        // counts `extract_items` on each input).
        let provenance: Vec<NodeContribution> = nodes
            .iter()
            .zip(&settled)
            .map(|(node, s)| NodeContribution {
                node_id: *node,
                status: s.status,
                duration_ms: s.duration_ms,
                item_count: match (&s.status, &s.output) {
                    (NodeStatus::Ok, Some(output)) => {
                        u64::try_from(harness_merge::extract_items(output).len())
                            .unwrap_or(u64::MAX)
                    }
                    _ => 0,
                },
            })
            .collect();

        let all_ok = ok_count == n;
        let must_fail = match self.on_node_failure {
            PartialPolicy::ReturnPartial => ok_count == 0,
            // FailFast | Wait (and unknown variants, conservatively):
            // all contributions or nothing.
            _ => !all_ok,
        };
        if must_fail {
            let msg = flatten_errors(self.on_node_failure, ok_count, n, &failures);
            self.terminalize_failed(&msg, &provenance);
            return;
        }

        // Merge Ok outputs, in node order (nodes arrive sorted from
        // DispatchPlan::Federated — deterministic tiebreaks).
        let inputs: Vec<NodeOutput> = nodes
            .iter()
            .zip(&settled)
            .filter(|(_, s)| matches!(s.status, NodeStatus::Ok))
            .map(|(node, s)| NodeOutput {
                node_id: *node,
                output: s.output.clone().unwrap_or(JsonValue::Null),
            })
            .collect();
        let merged = match harness_merge::merge(&self.merge, &inputs) {
            Ok(m) => m,
            Err(e) => {
                self.terminalize_failed(&format!("federated merge: {e}"), &provenance);
                return;
            }
        };
        let mut output = merged.output;
        if let Some(merge_block) = output.get_mut("merge").and_then(JsonValue::as_object_mut) {
            if truncated_nodes > 0 {
                merge_block.insert("truncated_nodes".into(), json!(truncated_nodes));
            }
            if !failures.is_empty() {
                // ReturnPartial-Done: non-Ok nodes ride the output so
                // callers see what's missing (review MAJOR-1).
                let fail_rows: Vec<JsonValue> = failures
                    .iter()
                    .map(|(node, status, error)| {
                        json!({
                            "node": node.to_string(),
                            "status": status_str(*status),
                            "error": snippet(error),
                        })
                    })
                    .collect();
                merge_block.insert("failures".into(), json!(fail_rows));
            }
        }
        self.terminalize_done(&output, &provenance);
    }

    fn build_spec(
        &self,
        nodes: &[NodeId],
        until: tokio::time::Instant,
        global_ms: u64,
        pulled_at: &Arc<ParkingMutex<HashMap<u64, tokio::time::Instant>>>,
    ) -> FanoutSpec<NodeId, JsonValue> {
        let n = nodes.len();
        let store = self.store.clone();
        let identity = self.identity.clone();
        let capability = self.task.capability.clone();
        let input = self.task.input.clone();
        let parent = self.task.id;
        let pulled_at = pulled_at.clone();
        FanoutSpec {
            source: Box::pin(futures::stream::iter(nodes.to_vec())),
            run: Box::new(move |index, node| {
                let store = store.clone();
                let identity = identity.clone();
                let capability = capability.clone();
                let input = input.clone();
                let pulled_at = pulled_at.clone();
                Box::pin(async move {
                    pulled_at.lock().insert(index, tokio::time::Instant::now());
                    // Remaining-budget rule (4.1 MAJOR-1): late pulls get
                    // what's left of the global budget, floored at 1 s.
                    let remaining = until
                        .saturating_duration_since(tokio::time::Instant::now())
                        .max(Duration::from_secs(1));
                    let timeout_ms = u32::try_from(remaining.as_millis()).unwrap_or(u32::MAX);
                    let sub = match crate::subtask::build_pinned_subtask(
                        &identity,
                        &capability,
                        input,
                        node,
                        parent,
                        timeout_ms,
                    ) {
                        Ok(t) => t,
                        Err(e) => return ItemOutcome::Failed(format!("build sub-task: {e}")),
                    };
                    if let Err(e) = store.insert_task(&sub) {
                        return ItemOutcome::Failed(format!("insert sub-task: {e}"));
                    }
                    match crate::subtask::await_terminal(&store, sub.id, remaining).await {
                        SubTaskOutcome::Done(v) => ItemOutcome::Ok(v),
                        SubTaskOutcome::Failed(e) => ItemOutcome::Failed(e),
                        SubTaskOutcome::TimedOut => ItemOutcome::TimedOut,
                    }
                })
            }),
            // For N ≤ 64 the clamp yields window ≥ N — PRD's "all
            // eligible in parallel" — while keeping 4.1's machinery.
            window: WindowPolicy::default_per_workers(),
            live_workers: Box::new(move || n),
            deadline: Some(Box::pin(tokio::time::sleep(Duration::from_millis(
                global_ms,
            )))),
            // Wait drives like ReturnPartial at the controller; its
            // all-contributions rule is enforced at merge time above.
            on_failure: match self.on_node_failure {
                PartialPolicy::Wait => PartialPolicy::ReturnPartial,
                other => other,
            },
        }
    }

    /// Best-effort node-id → hostname map for progress frames.
    fn node_names(&self, nodes: &[NodeId]) -> HashMap<NodeId, String> {
        let mut names = HashMap::new();
        if let Ok(manifests) = self.store.list_manifests() {
            for m in manifests {
                if nodes.contains(&m.node_id) && !m.hostname.is_empty() {
                    names.insert(m.node_id, m.hostname);
                }
            }
        }
        names
    }

    fn emit(&self, chunk: &JsonValue) {
        if let Some(sink) = &self.sink {
            sink(
                self.task.id,
                LogFrame {
                    stream: StreamKind::Progress,
                    line: chunk.to_string(),
                },
            );
        }
    }

    fn terminalize_done(&self, output: &JsonValue, provenance: &[NodeContribution]) {
        let now = now_unix_ms();
        if !self.transition_terminal(TaskState::Done) {
            return;
        }
        if let Err(e) = self.store.write_task_result_done_with_provenance(
            self.task.id,
            output,
            now,
            self.local_id,
            provenance,
        ) {
            tracing::warn!(target: "harness.federated", ?e, "write federated done result");
        }
        let preview = serde_json::to_vec(output)
            .ok()
            .map(|v| v.into_iter().take(256).collect::<Vec<u8>>());
        let _ = self.store.replica_apply_local(&ReplicatedTaskState {
            task_id: self.task.id,
            state: ReplicatedState::Done,
            at_ms: now,
            source: self.local_id,
            output_preview: preview,
        });
    }

    fn terminalize_failed(&self, msg: &str, provenance: &[NodeContribution]) {
        fail_parent(&self.store, self.task.id, self.local_id, msg, provenance);
    }

    fn transition_terminal(&self, terminal: TaskState) -> bool {
        match self
            .store
            .try_transition_task(self.task.id, TaskState::Running, terminal)
        {
            Ok(true) => true,
            Ok(false) => {
                // Cancelled out from under us (operator action) — that
                // terminal owns the row; write nothing.
                tracing::debug!(
                    target: "harness.federated",
                    task = %self.task.id.0,
                    "parent no longer Running at terminalize; skipping"
                );
                false
            }
            Err(e) => {
                tracing::warn!(target: "harness.federated", ?e, "terminalize federated parent");
                false
            }
        }
    }
}

/// `ResultMapper` resolving fan-out indexes back to nodes: accumulates
/// per-node settles, emits one `streaming` frame per settled node
/// (PRD §14.6 step 3), and marks never-settled nodes on end.
struct NodeMapper {
    task_id: TaskId,
    nodes: Vec<NodeId>,
    names: HashMap<NodeId, String>,
    pulled_at: Arc<ParkingMutex<HashMap<u64, tokio::time::Instant>>>,
    sink: Option<FrameSink>,
    completed: u64,
    settled: Vec<Option<NodeSettle>>,
    summary: Option<FanoutSummary>,
}

impl NodeMapper {
    fn emit(&self, chunk: &JsonValue) {
        if let Some(sink) = &self.sink {
            sink(
                self.task_id,
                LogFrame {
                    stream: StreamKind::Progress,
                    line: chunk.to_string(),
                },
            );
        }
    }
}

impl ResultMapper<JsonValue> for NodeMapper {
    fn on_item(&mut self, index: u64, outcome: &ItemOutcome<JsonValue>) -> JsonValue {
        let idx = usize::try_from(index).unwrap_or(usize::MAX);
        let Some(node) = self.nodes.get(idx).copied() else {
            return JsonValue::Null;
        };
        let duration_ms = self.pulled_at.lock().get(&index).map_or(0, |t| {
            u64::try_from(t.elapsed().as_millis()).unwrap_or(u64::MAX)
        });
        let (status, output, error) = match outcome {
            ItemOutcome::Ok(v) => (NodeStatus::Ok, Some(v.clone()), None),
            ItemOutcome::Failed(e) => (NodeStatus::Failed, None, Some(e.clone())),
            ItemOutcome::TimedOut => (NodeStatus::TimedOut, None, None),
        };
        if let Some(slot) = self.settled.get_mut(idx) {
            *slot = Some(NodeSettle {
                status,
                duration_ms,
                output,
                error: error.clone(),
            });
        }
        self.completed += 1;
        let node_name = self
            .names
            .get(&node)
            .cloned()
            .unwrap_or_else(|| node.to_string());
        let chunk = json!({ "federated": {
            "stage": "streaming",
            "node": node.to_string(),
            "node_name": node_name,
            "outcome": status_str(status),
            "completed": self.completed,
            "total": self.nodes.len(),
        }});
        self.emit(&chunk);
        chunk
    }

    fn on_end(&mut self, summary: &FanoutSummary) -> JsonValue {
        // Never-settled nodes: at the deadline, pulled-but-incomplete
        // were in flight (TimedOut) and never-pulled never ran
        // (Skipped); a fail-fast abort marks everything unsettled
        // Skipped — the coordinator chose not to wait, the nodes
        // didn't run out of time (ADR-0027).
        let pulled = self.pulled_at.lock();
        for (idx, slot) in self.settled.iter_mut().enumerate() {
            if slot.is_none() {
                let pulled_instant = u64::try_from(idx)
                    .ok()
                    .and_then(|i| pulled.get(&i).copied());
                *slot = Some(NodeSettle {
                    status: if pulled_instant.is_some()
                        && summary.reason == harness_orchestrator::EndReason::DeadlineExceeded
                    {
                        NodeStatus::TimedOut
                    } else {
                        NodeStatus::Skipped
                    },
                    // A pulled node ran until the fan-out ended — record
                    // the real elapsed time; zero stays reserved for
                    // never-pulled nodes (PR #48 review).
                    duration_ms: pulled_instant.map_or(0, |t| {
                        u64::try_from(t.elapsed().as_millis()).unwrap_or(u64::MAX)
                    }),
                    output: None,
                    error: None,
                });
            }
        }
        drop(pulled);
        self.summary = Some(summary.clone());
        json!({ "federated": {
            "stage": "fanout_settled",
            "ok": summary.ok,
            "failed": summary.failed,
            "timed_out": summary.timed_out,
            "dropped_in_flight": summary.dropped_in_flight,
        }})
    }
}

fn status_str(status: NodeStatus) -> &'static str {
    match status {
        NodeStatus::Ok => "ok",
        NodeStatus::Failed => "failed",
        NodeStatus::TimedOut => "timed_out",
        NodeStatus::Skipped => "skipped",
        _ => "unknown",
    }
}

/// Bounded per-node error snippet (char-boundary safe).
fn snippet(error: &str) -> String {
    if error.chars().count() <= ERROR_SNIPPET_CHARS {
        return error.to_string();
    }
    let mut s: String = error.chars().take(ERROR_SNIPPET_CHARS).collect();
    s.push('…');
    s
}

/// Flatten per-node failures into a bounded parent error string
/// (review MAJOR-1: Failed terminals carry per-node error text).
fn flatten_errors(
    policy: PartialPolicy,
    ok: usize,
    total: usize,
    failures: &[(NodeId, NodeStatus, String)],
) -> String {
    let label = match policy {
        PartialPolicy::Wait => "federated wait",
        PartialPolicy::FailFast => "federated fail-fast",
        _ => "federated",
    };
    let mut msg = format!("{label}: {ok}/{total} nodes ok");
    for (node, status, error) in failures.iter().take(MAX_ERRORS_IN_MESSAGE) {
        msg.push_str("; ");
        msg.push_str(&node.to_string());
        msg.push_str(": ");
        if error.is_empty() {
            msg.push_str(status_str(*status));
        } else {
            msg.push_str(&snippet(error));
        }
    }
    if failures.len() > MAX_ERRORS_IN_MESSAGE {
        msg.push_str(&format!(
            "; …and {} more",
            failures.len() - MAX_ERRORS_IN_MESSAGE
        ));
    }
    msg
}

/// Fail the parent: `Running -> Failed` CAS + result row + replica.
/// Free-standing so the spawn-level panic fallback can reach it after
/// the `Driver` has been consumed (diff review MAJOR-1).
fn fail_parent(
    store: &Store,
    task_id: TaskId,
    local_id: NodeId,
    msg: &str,
    provenance: &[NodeContribution],
) {
    tracing::warn!(
        target: "harness.federated",
        task = %task_id.0,
        %msg,
        "federated task failed"
    );
    let now = now_unix_ms();
    match store.try_transition_task(task_id, TaskState::Running, TaskState::Failed) {
        Ok(true) => {}
        Ok(false) => {
            // Cancelled out from under us (operator action) — that
            // terminal owns the row; write nothing.
            tracing::debug!(
                target: "harness.federated",
                task = %task_id.0,
                "parent no longer Running at terminalize; skipping"
            );
            return;
        }
        Err(e) => {
            tracing::warn!(target: "harness.federated", ?e, "terminalize federated parent");
            return;
        }
    }
    if let Err(e) =
        store.write_task_result_failed_with_provenance(task_id, msg, now, local_id, provenance)
    {
        tracing::warn!(target: "harness.federated", ?e, "write federated failed result");
    }
    let _ = store.replica_apply_local(&ReplicatedTaskState {
        task_id,
        state: ReplicatedState::Failed,
        at_ms: now,
        source: local_id,
        output_preview: Some(msg.as_bytes().iter().copied().take(256).collect()),
    });
}

fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use harness_core::{
        Constraints, ExecutionPolicy, ResourceHints, RetryPolicy, Signable, Signature, TraceContext,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// What the simulated worker does with a node's sub-task.
    #[derive(Clone)]
    enum WorkerBehavior {
        Ok(JsonValue),
        Fail(&'static str),
        Hang,
    }

    fn empty_hints() -> ResourceHints {
        ResourceHints {
            cpu_class: harness_core::protocol::CpuClass::Light,
            memory_mb: None,
            gpu_required: false,
            gpu_memory_mb: None,
            network_class: harness_core::protocol::NetworkClass::None,
            disk_io_class: harness_core::protocol::DiskIoClass::None,
            estimated_duration_ms: None,
        }
    }

    fn parent_task(identity: &Identity, timeout_ms: u32) -> Task {
        let mut t = Task {
            id: TaskId::new_v7(),
            parent: None,
            plan_id: None,
            capability: "fed.test".into(),
            input: serde_json::json!({"q": "x"}),
            constraints: Constraints::default(),
            retry: RetryPolicy::default(),
            execution: ExecutionPolicy {
                timeout_ms,
                ..ExecutionPolicy::default()
            },
            resource_hints: empty_hints(),
            trace_ctx: TraceContext::default(),
            issued_by: identity.node_id(),
            issued_at: 1_700_000_000_000,
            tags: vec![],
            sig: Signature::from_bytes([0u8; 64]),
        };
        t.sign(identity).expect("sign");
        t
    }

    /// Simulated mesh workers: drain Submitted sub-tasks of `parent`,
    /// terminalize them per the behavior for their pinned node. Counts
    /// executions in `executed`.
    fn spawn_workers(
        store: Store,
        parent: TaskId,
        behaviors: HashMap<NodeId, WorkerBehavior>,
        executed: Arc<AtomicUsize>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                let rows = store
                    .list_tasks_by_state(TaskState::Submitted)
                    .unwrap_or_default();
                for row in rows {
                    let Ok(Some(task)) = store.load_task(row.id) else {
                        continue;
                    };
                    if task.parent != Some(parent) {
                        continue;
                    }
                    let Some(node) = task.constraints.pin_to_node else {
                        continue;
                    };
                    let Some(behavior) = behaviors.get(&node).cloned() else {
                        continue;
                    };
                    if matches!(behavior, WorkerBehavior::Hang) {
                        continue; // leave the row Submitted forever
                    }
                    // Emulate the dispatch+executor ladder.
                    if !store.try_dispatch_task(task.id, node).unwrap_or(false) {
                        continue;
                    }
                    executed.fetch_add(1, Ordering::SeqCst);
                    store
                        .transition_task(task.id, TaskState::Claimed)
                        .expect("claim");
                    store
                        .transition_task(task.id, TaskState::Running)
                        .expect("run");
                    let now = now_unix_ms();
                    match behavior {
                        WorkerBehavior::Ok(output) => {
                            store
                                .transition_task(task.id, TaskState::Done)
                                .expect("done");
                            store
                                .write_task_result_done(task.id, &output, now, node)
                                .expect("write done");
                        }
                        WorkerBehavior::Fail(msg) => {
                            store
                                .transition_task(task.id, TaskState::Failed)
                                .expect("fail");
                            store
                                .write_task_result_failed(task.id, msg, now, node)
                                .expect("write failed");
                        }
                        WorkerBehavior::Hang => unreachable!(),
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
    }

    async fn wait_terminal(store: &Store, id: TaskId) -> TaskState {
        loop {
            let state = store.task_state(id).expect("state").expect("present");
            if matches!(
                state,
                TaskState::Done | TaskState::Failed | TaskState::Expired | TaskState::Cancelled
            ) {
                return state;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    type Frames = Arc<ParkingMutex<Vec<(TaskId, LogFrame)>>>;

    fn capture_sink() -> (FrameSink, Frames) {
        let frames: Frames = Arc::new(ParkingMutex::new(Vec::new()));
        let sink_frames = frames.clone();
        let sink: FrameSink = Arc::new(move |task_id, frame| {
            sink_frames.lock().push((task_id, frame));
        });
        (sink, frames)
    }

    fn stages(frames: &Frames, task_id: TaskId) -> Vec<String> {
        frames
            .lock()
            .iter()
            .filter(|(id, _)| *id == task_id)
            .filter_map(|(_, f)| {
                serde_json::from_str::<JsonValue>(&f.line)
                    .ok()
                    .and_then(|v| {
                        v.pointer("/federated/stage")
                            .and_then(JsonValue::as_str)
                            .map(str::to_string)
                    })
            })
            .collect()
    }

    fn three_nodes() -> Vec<NodeId> {
        let mut nodes = vec![
            NodeId::from_bytes([1; 16]),
            NodeId::from_bytes([2; 16]),
            NodeId::from_bytes([3; 16]),
        ];
        nodes.sort();
        nodes
    }

    #[tokio::test(start_paused = true)]
    async fn f01_happy_path_merges_with_full_provenance_and_frames() {
        let store = Store::open_memory().expect("store");
        let identity = Arc::new(Identity::generate());
        let coordinator = FederatedCoordinator::new(store.clone(), identity.clone());
        let (sink, frames) = capture_sink();
        coordinator.attach_sink(sink);

        let task = parent_task(&identity, 30_000);
        store.insert_task(&task).expect("insert parent");
        let nodes = three_nodes();
        let behaviors: HashMap<NodeId, WorkerBehavior> = nodes
            .iter()
            .enumerate()
            .map(|(i, n)| {
                (
                    *n,
                    WorkerBehavior::Ok(serde_json::json!({"items": [{"n": i}]})),
                )
            })
            .collect();
        let executed = Arc::new(AtomicUsize::new(0));
        let workers = spawn_workers(store.clone(), task.id, behaviors, executed.clone());

        assert!(coordinator.try_start(
            &task,
            nodes.clone(),
            MergeStrategy::Concat,
            PartialPolicy::ReturnPartial,
        ));
        // Claimed atomically: never observable at Submitted/Dispatched.
        assert_eq!(store.task_state(task.id).unwrap(), Some(TaskState::Running));

        let state = wait_terminal(&store, task.id).await;
        workers.abort();
        assert_eq!(state, TaskState::Done);
        assert_eq!(executed.load(Ordering::SeqCst), 3, "one sub-task per node");

        let row = store
            .load_task_result(task.id)
            .expect("load")
            .expect("present");
        let output = row.output.expect("output");
        assert_eq!(output["items"].as_array().expect("items").len(), 3);
        assert_eq!(output["merge"]["strategy"], "concat");
        assert!(
            output["merge"].get("failures").is_none(),
            "no failures block on a clean run"
        );
        let provenance = row.provenance.expect("provenance");
        assert_eq!(provenance.len(), 3);
        for (contribution, node) in provenance.iter().zip(&nodes) {
            assert_eq!(contribution.node_id, *node);
            assert_eq!(contribution.status, NodeStatus::Ok);
            assert_eq!(contribution.item_count, 1);
        }

        let stage_list = stages(&frames, task.id);
        assert_eq!(stage_list.first().map(String::as_str), Some("discovered"));
        assert_eq!(
            stage_list.iter().filter(|s| *s == "streaming").count(),
            3,
            "one streaming frame per node: {stage_list:?}"
        );
        assert_eq!(stage_list.last().map(String::as_str), Some("merging"));
    }

    #[tokio::test(start_paused = true)]
    async fn f02_return_partial_merges_survivors_and_reports_failures() {
        let store = Store::open_memory().expect("store");
        let identity = Arc::new(Identity::generate());
        let coordinator = FederatedCoordinator::new(store.clone(), identity.clone());

        let task = parent_task(&identity, 30_000);
        store.insert_task(&task).expect("insert parent");
        let nodes = three_nodes();
        let mut behaviors = HashMap::new();
        behaviors.insert(
            nodes[0],
            WorkerBehavior::Ok(serde_json::json!({"items": [{"n": 0}]})),
        );
        behaviors.insert(nodes[1], WorkerBehavior::Fail("disk on fire"));
        behaviors.insert(
            nodes[2],
            WorkerBehavior::Ok(serde_json::json!({"items": [{"n": 2}]})),
        );
        let executed = Arc::new(AtomicUsize::new(0));
        let workers = spawn_workers(store.clone(), task.id, behaviors, executed.clone());

        assert!(coordinator.try_start(
            &task,
            nodes.clone(),
            MergeStrategy::Concat,
            PartialPolicy::ReturnPartial,
        ));
        let state = wait_terminal(&store, task.id).await;
        workers.abort();
        assert_eq!(state, TaskState::Done, "partial merge still succeeds");

        let row = store
            .load_task_result(task.id)
            .expect("load")
            .expect("present");
        let output = row.output.expect("output");
        assert_eq!(output["items"].as_array().expect("items").len(), 2);
        let failures = output["merge"]["failures"].as_array().expect("failures");
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0]["node"], nodes[1].to_string());
        assert_eq!(failures[0]["status"], "failed");
        assert_eq!(failures[0]["error"], "disk on fire");

        let provenance = row.provenance.expect("provenance");
        assert_eq!(provenance[0].status, NodeStatus::Ok);
        assert_eq!(provenance[1].status, NodeStatus::Failed);
        assert_eq!(provenance[1].item_count, 0);
        assert_eq!(provenance[2].status, NodeStatus::Ok);
    }

    #[tokio::test(start_paused = true)]
    async fn f03_wait_demands_every_contribution() {
        let store = Store::open_memory().expect("store");
        let identity = Arc::new(Identity::generate());
        let coordinator = FederatedCoordinator::new(store.clone(), identity.clone());

        let task = parent_task(&identity, 30_000);
        store.insert_task(&task).expect("insert parent");
        let nodes = three_nodes();
        let mut behaviors = HashMap::new();
        behaviors.insert(
            nodes[0],
            WorkerBehavior::Ok(serde_json::json!({"items": [1]})),
        );
        behaviors.insert(nodes[1], WorkerBehavior::Fail("disk on fire"));
        behaviors.insert(
            nodes[2],
            WorkerBehavior::Ok(serde_json::json!({"items": [2]})),
        );
        let executed = Arc::new(AtomicUsize::new(0));
        let workers = spawn_workers(store.clone(), task.id, behaviors, executed.clone());

        assert!(coordinator.try_start(
            &task,
            nodes.clone(),
            MergeStrategy::Concat,
            PartialPolicy::Wait,
        ));
        let state = wait_terminal(&store, task.id).await;
        workers.abort();
        assert_eq!(
            state,
            TaskState::Failed,
            "Wait: any non-Ok fails the parent"
        );

        let row = store
            .load_task_result(task.id)
            .expect("load")
            .expect("present");
        let error = row.error.expect("error");
        assert!(
            error.contains("federated wait: 2/3 nodes ok"),
            "flattened header: {error}"
        );
        assert!(
            error.contains("disk on fire"),
            "per-node error text: {error}"
        );
        // Full provenance survives on the failure path — including the
        // Ok nodes' item counts (PR #48 review: the Wait/FailFast early
        // return must not zero out real contributions).
        let provenance = row.provenance.expect("provenance");
        assert_eq!(provenance.len(), 3);
        assert_eq!(provenance[0].status, NodeStatus::Ok);
        assert_eq!(provenance[0].item_count, 1);
        assert_eq!(provenance[1].status, NodeStatus::Failed);
        assert_eq!(provenance[1].item_count, 0);
        assert_eq!(provenance[2].item_count, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn f04_fail_fast_aborts_and_skips_unsettled() {
        let store = Store::open_memory().expect("store");
        let identity = Arc::new(Identity::generate());
        let coordinator = FederatedCoordinator::new(store.clone(), identity.clone());

        let task = parent_task(&identity, 30_000);
        store.insert_task(&task).expect("insert parent");
        let nodes = three_nodes();
        let mut behaviors = HashMap::new();
        behaviors.insert(nodes[0], WorkerBehavior::Fail("boom"));
        behaviors.insert(nodes[1], WorkerBehavior::Hang);
        behaviors.insert(nodes[2], WorkerBehavior::Hang);
        let executed = Arc::new(AtomicUsize::new(0));
        let workers = spawn_workers(store.clone(), task.id, behaviors, executed.clone());

        assert!(coordinator.try_start(
            &task,
            nodes.clone(),
            MergeStrategy::Concat,
            PartialPolicy::FailFast,
        ));
        let state = wait_terminal(&store, task.id).await;
        workers.abort();
        assert_eq!(state, TaskState::Failed);

        let row = store
            .load_task_result(task.id)
            .expect("load")
            .expect("present");
        assert!(row.error.expect("error").contains("boom"));
        let provenance = row.provenance.expect("provenance");
        assert_eq!(provenance[0].status, NodeStatus::Failed);
        // The hung nodes were dropped by fail-fast, not timed out.
        assert_eq!(provenance[1].status, NodeStatus::Skipped);
        assert_eq!(provenance[2].status, NodeStatus::Skipped);
    }

    #[tokio::test(start_paused = true)]
    async fn f05_global_timeout_marks_hung_nodes_timed_out() {
        let store = Store::open_memory().expect("store");
        let identity = Arc::new(Identity::generate());
        let coordinator = FederatedCoordinator::new(store.clone(), identity.clone());

        let task = parent_task(&identity, 1_000); // minimum global budget
        store.insert_task(&task).expect("insert parent");
        let nodes = vec![NodeId::from_bytes([1; 16]), NodeId::from_bytes([2; 16])];
        let mut behaviors = HashMap::new();
        behaviors.insert(
            nodes[0],
            WorkerBehavior::Ok(serde_json::json!({"items": [1]})),
        );
        behaviors.insert(nodes[1], WorkerBehavior::Hang);
        let executed = Arc::new(AtomicUsize::new(0));
        let workers = spawn_workers(store.clone(), task.id, behaviors, executed.clone());

        assert!(coordinator.try_start(
            &task,
            nodes.clone(),
            MergeStrategy::Concat,
            PartialPolicy::ReturnPartial,
        ));
        let state = wait_terminal(&store, task.id).await;
        workers.abort();
        assert_eq!(state, TaskState::Done, "partial result from the live node");

        let row = store
            .load_task_result(task.id)
            .expect("load")
            .expect("present");
        let provenance = row.provenance.expect("provenance");
        assert_eq!(provenance[0].status, NodeStatus::Ok);
        assert_eq!(provenance[1].status, NodeStatus::TimedOut);
        // PR #48 review: a pulled node that timed out consumed real time
        // — its duration must reflect that, not read 0.
        assert!(
            provenance[1].duration_ms > 0,
            "timed-out node must record elapsed duration: {provenance:?}"
        );
        let failures = row.output.expect("output")["merge"]["failures"]
            .as_array()
            .expect("failures")
            .clone();
        assert_eq!(failures[0]["status"], "timed_out");
    }

    #[tokio::test(start_paused = true)]
    async fn f06_no_slot_leaves_task_submitted_then_coordinates_when_freed() {
        let store = Store::open_memory().expect("store");
        let identity = Arc::new(Identity::generate());
        let coordinator = FederatedCoordinator::with_slots(store.clone(), identity.clone(), 1);
        let node = NodeId::from_bytes([1; 16]);

        // Occupy the only slot with a coordination over a hung node.
        let hog = parent_task(&identity, 30_000);
        store.insert_task(&hog).expect("insert hog");
        let mut hog_behaviors = HashMap::new();
        hog_behaviors.insert(node, WorkerBehavior::Hang);
        let hog_workers = spawn_workers(
            store.clone(),
            hog.id,
            hog_behaviors,
            Arc::new(AtomicUsize::new(0)),
        );
        assert!(coordinator.try_start(
            &hog,
            vec![node],
            MergeStrategy::Concat,
            PartialPolicy::ReturnPartial,
        ));

        // Second federated task: no slot ⇒ stays Submitted (queueing,
        // never failure — the caller retries next poll).
        let queued = parent_task(&identity, 30_000);
        store.insert_task(&queued).expect("insert queued");
        assert!(!coordinator.try_start(
            &queued,
            vec![node],
            MergeStrategy::Concat,
            PartialPolicy::ReturnPartial,
        ));
        assert_eq!(
            store.task_state(queued.id).unwrap(),
            Some(TaskState::Submitted)
        );

        // Free the slot (the hog times out on its own budget is 30 s —
        // instead let its worker finish now).
        hog_workers.abort();
        let mut done_behaviors = HashMap::new();
        done_behaviors.insert(node, WorkerBehavior::Ok(serde_json::json!({"items": [1]})));
        let workers = spawn_workers(
            store.clone(),
            hog.id,
            done_behaviors.clone(),
            Arc::new(AtomicUsize::new(0)),
        );
        assert_eq!(wait_terminal(&store, hog.id).await, TaskState::Done);
        workers.abort();

        // Retry (the next dispatch poll in production): now it runs.
        let queued_workers = spawn_workers(
            store.clone(),
            queued.id,
            done_behaviors,
            Arc::new(AtomicUsize::new(0)),
        );
        // The slot frees asynchronously after the driver's terminal
        // write — poll try_start like the dispatch loop would.
        loop {
            if coordinator.try_start(
                &queued,
                vec![node],
                MergeStrategy::Concat,
                PartialPolicy::ReturnPartial,
            ) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(wait_terminal(&store, queued.id).await, TaskState::Done);
        queued_workers.abort();
    }

    /// Diff review MAJOR-1: a panic anywhere in the driver must not
    /// strand the parent at `Running` — the spawn-level catch_unwind
    /// terminalizes it Failed and the coordination slot is released.
    #[tokio::test(start_paused = true)]
    async fn f08_driver_panic_terminalizes_failed_and_frees_slot() {
        let store = Store::open_memory().expect("store");
        let identity = Arc::new(Identity::generate());
        let coordinator = FederatedCoordinator::with_slots(store.clone(), identity.clone(), 1);
        // A sink that explodes on the very first stage frame.
        let sink: FrameSink = Arc::new(|_, _| panic!("sink exploded"));
        coordinator.attach_sink(sink);

        let task = parent_task(&identity, 30_000);
        store.insert_task(&task).expect("insert parent");
        assert!(coordinator.try_start(
            &task,
            vec![NodeId::from_bytes([1; 16])],
            MergeStrategy::Concat,
            PartialPolicy::ReturnPartial,
        ));
        let state = wait_terminal(&store, task.id).await;
        assert_eq!(
            state,
            TaskState::Failed,
            "panic must terminalize, not strand"
        );
        let row = store
            .load_task_result(task.id)
            .expect("load")
            .expect("present");
        let error = row.error.expect("error");
        assert!(
            error.contains("panicked") && error.contains("sink exploded"),
            "panic payload surfaces in the error: {error}"
        );

        // The single slot must be free again: a fresh claim succeeds
        // (poll like the dispatch loop — the permit drops after the
        // catch_unwind resolves).
        let task2 = parent_task(&identity, 30_000);
        store.insert_task(&task2).expect("insert second");
        loop {
            if coordinator.try_start(
                &task2,
                vec![NodeId::from_bytes([1; 16])],
                MergeStrategy::Concat,
                PartialPolicy::ReturnPartial,
            ) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// BLOCKER-1 regression: concurrent claim attempts on the same
    /// parent — exactly one coordination runs (N sub-task rows, not 2N),
    /// and the local-executor path can never double-claim because the
    /// parent is never observable at `Dispatched`.
    #[tokio::test(start_paused = true)]
    async fn f07_double_claim_race_spawns_exactly_one_coordination() {
        let store = Store::open_memory().expect("store");
        let identity = Arc::new(Identity::generate());
        let coordinator = FederatedCoordinator::new(store.clone(), identity.clone());

        let task = parent_task(&identity, 30_000);
        store.insert_task(&task).expect("insert parent");
        let nodes = three_nodes();
        let behaviors: HashMap<NodeId, WorkerBehavior> = nodes
            .iter()
            .map(|n| (*n, WorkerBehavior::Ok(serde_json::json!({"items": [1]}))))
            .collect();
        let executed = Arc::new(AtomicUsize::new(0));
        let workers = spawn_workers(store.clone(), task.id, behaviors, executed.clone());

        // Two dispatch passes race on the same Submitted row.
        let c1 = coordinator.clone();
        let c2 = coordinator.clone();
        let t1 = task.clone();
        let t2 = task.clone();
        let n1 = nodes.clone();
        let n2 = nodes.clone();
        let (a, b) = tokio::join!(
            tokio::spawn(async move {
                c1.try_start(&t1, n1, MergeStrategy::Concat, PartialPolicy::ReturnPartial)
            }),
            tokio::spawn(async move {
                c2.try_start(&t2, n2, MergeStrategy::Concat, PartialPolicy::ReturnPartial)
            }),
        );
        // Both report handled (slot was free) — but only one won the CAS.
        assert!(a.unwrap() && b.unwrap());

        assert_eq!(wait_terminal(&store, task.id).await, TaskState::Done);
        workers.abort();
        assert_eq!(
            executed.load(Ordering::SeqCst),
            3,
            "exactly one coordination: one sub-task per node, not two"
        );
        // And the store holds exactly N sub-task rows for this parent.
        let mut sub_rows = 0;
        for state in [TaskState::Done, TaskState::Failed] {
            for row in store.list_tasks_by_state(state).expect("list") {
                if store
                    .load_task(row.id)
                    .expect("load")
                    .is_some_and(|t| t.parent == Some(task.id))
                {
                    sub_rows += 1;
                }
            }
        }
        assert_eq!(sub_rows, 3);
    }
}
