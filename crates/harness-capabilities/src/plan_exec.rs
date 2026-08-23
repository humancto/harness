//! `plan.execute` — the DAG executor driver (roadmap 4.3, ADR-0025).
//!
//! Takes a validated [`Plan`] as input, dispatches steps whose
//! dependencies are satisfied as ordinary signed **unpinned** task rows
//! through the daemon's dispatch runtime (placement by cardinality,
//! leases, and policy-on-the-executing-node all come for free), bounds
//! in-flight steps with the 4.1 `FanoutController` window (rows are
//! O(window), never O(plan)), threads `$task_output` references from
//! completed steps into dependents' inputs, and terminal-izes with an
//! aggregate result keyed by plan-node id.
//!
//! Entry validation is unconditional (repo rule 8): the plan is
//! re-validated against the local registry ∪ manifest-advertised
//! capabilities — including schemas — however trusted the caller.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt as _;
use harness_brain::{validate_plan, CapabilitySchemaIndex, PlanConstraints};
use harness_core::{
    resolve_output_refs, Capability as ManifestEntry, CapabilityRef, Cardinality, Plan, PlanId,
    ResourceHints, TaskId,
};
use harness_orchestrator::{
    DagScheduler, FanoutController, FanoutEvent, FanoutSpec, ItemOutcome, StepOutcome, StepState,
    WindowPolicy, MAX_PLAN_STEPS,
};
use serde_json::{json, Value as JsonValue};

use crate::mesh_meta::SubTaskOutcome;
use crate::traits::{
    Capability, CapabilityError, ExecutionContext, FrameSink, LogFrame, StreamKind,
};

const DEFAULT_TIMEOUT_MS: u64 = 120_000;
const MAX_TIMEOUT_MS: u64 = 600_000;
/// Default per-step timeout when `PlanNode.timeout_ms` is unset
/// (clamped to the remaining plan budget at pull time).
const DEFAULT_STEP_TIMEOUT_MS: u32 = 30_000;

/// Daemon services the DAG driver needs — a sibling of
/// [`crate::mesh_meta::MeshExec`] (which stays pinned-submit only).
#[async_trait]
pub trait PlanExec: Send + Sync + 'static {
    /// Insert a signed, UNPINNED sub-task row in `Submitted`:
    /// `parent` = the plan.execute task, `plan_id` = the plan's id. The
    /// dispatch runtime routes it by cardinality over the live mesh
    /// (self included).
    fn submit_step(
        &self,
        capability: &str,
        input: JsonValue,
        parent: TaskId,
        plan_id: PlanId,
        resource_hints: ResourceHints,
        timeout_ms: u32,
    ) -> Result<TaskId, CapabilityError>;

    /// Await the step row's terminal state, up to `deadline`.
    async fn await_terminal(&self, id: TaskId, deadline: Duration) -> SubTaskOutcome;

    /// Live mesh size (self included) for the 2×N window.
    fn live_workers(&self) -> usize;

    /// Full capability entries — id, version, `input_schema` — from the
    /// local registry ∪ stored manifests. Entry validation builds its
    /// schema index from this union (ADR-0025), so remote-only
    /// capabilities validate for real.
    fn known_capabilities(&self) -> Vec<ManifestEntry>;
}

/// One resolved, validated step handed to the fan-out window.
struct ReadyStep {
    node_id: TaskId,
    capability: String,
    input: JsonValue,
    hints: ResourceHints,
    timeout_ms: u32,
}

/// Per-step record accumulated for the aggregate output.
struct StepRecord {
    capability: String,
    state: StepState,
    output: Option<JsonValue>,
    error: Option<String>,
    row_id: Option<TaskId>,
}

pub struct PlanExecCapability {
    exec: Arc<dyn PlanExec>,
    /// One plan per node: `try_acquire` and fail fast — never queue
    /// while holding an executor permit (plan review BLOCKER-1).
    inflight: Arc<tokio::sync::Semaphore>,
}

impl std::fmt::Debug for PlanExecCapability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlanExecCapability").finish_non_exhaustive()
    }
}

impl PlanExecCapability {
    #[must_use]
    pub fn new(exec: Arc<dyn PlanExec>) -> Self {
        Self {
            exec,
            inflight: Arc::new(tokio::sync::Semaphore::new(1)),
        }
    }
}

/// Register `plan.execute`.
///
/// # Panics
/// If the id is already registered (a wiring bug).
#[allow(clippy::expect_used)]
pub fn enrich_with_plan_exec(registry: &crate::CapabilityRegistry, exec: Arc<dyn PlanExec>) {
    registry
        .register(Arc::new(PlanExecCapability::new(exec)))
        .expect("BUG: plan.execute registered twice");
}

fn manifest() -> ManifestEntry {
    ManifestEntry {
        id: "plan.execute".to_string(),
        version: harness_core::SemVer::new(0, 1, 0),
        // Anyone: any node can coordinate; steps route to their owners.
        cardinality: Cardinality::Anyone,
        input_schema: json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["plan"],
            "properties": {
                "plan": { "type": "object" },
                "estimated_cost_usd": { "type": "number", "minimum": 0 },
                "timeout_ms": { "type": "integer", "minimum": 1000, "maximum": MAX_TIMEOUT_MS },
                "on_failure": { "type": "string", "enum": ["fail_fast", "continue"] }
            }
        }),
        output_schema: json!({ "type": "object" }),
        cost_hint: harness_core::protocol::CostHint::LocalFast,
        tags: vec!["plan".into()],
        rate_limit: None,
        resource_hints: ResourceHints {
            cpu_class: harness_core::protocol::CpuClass::Light,
            memory_mb: None,
            gpu_required: false,
            gpu_memory_mb: None,
            network_class: harness_core::protocol::NetworkClass::Light,
            disk_io_class: harness_core::protocol::DiskIoClass::None,
            estimated_duration_ms: None,
        },
        requires_secrets: vec![],
    }
}

#[async_trait]
impl Capability for PlanExecCapability {
    fn execution_class(&self) -> crate::traits::ExecutionClass {
        // Coordinator: awaits step sub-tasks (ADR-0027 wedge fix).
        crate::traits::ExecutionClass::Coordination
    }

    fn id(&self) -> &'static str {
        "plan.execute"
    }

    fn manifest(&self) -> ManifestEntry {
        manifest()
    }

    async fn execute(
        &self,
        ctx: &ExecutionContext,
        input: JsonValue,
    ) -> Result<JsonValue, CapabilityError> {
        // BLOCKER-1: fail fast under contention, never queue while
        // holding an executor permit.
        let Ok(_permit) = self.inflight.clone().try_acquire_owned() else {
            return Err(CapabilityError::Failed(
                "another plan is already executing on this node".into(),
            ));
        };
        let plan: Plan = serde_json::from_value(
            input
                .get("plan")
                .cloned()
                .ok_or_else(|| CapabilityError::InvalidInput("missing field: plan".into()))?,
        )
        .map_err(|e| CapabilityError::InvalidInput(format!("plan does not decode: {e}")))?;
        if plan.tasks.len() > MAX_PLAN_STEPS {
            return Err(CapabilityError::InvalidInput(format!(
                "plan exceeds {MAX_PLAN_STEPS} steps ({})",
                plan.tasks.len()
            )));
        }
        // Recursion guard: no nested plans, no planner re-entry.
        for node in plan.tasks.values() {
            if node.capability == "plan.execute" || node.capability == "brain.plan" {
                return Err(CapabilityError::InvalidInput(format!(
                    "step capability {} is not allowed inside a plan (no nested plans in 4.3)",
                    node.capability
                )));
            }
        }
        // Entry validation (rule 8) over registry ∪ manifests.
        let entries = self.exec.known_capabilities();
        let refs: Vec<CapabilityRef> = entries
            .iter()
            .map(|c| CapabilityRef {
                id: c.id.clone(),
                version_major: c.version.major,
            })
            .collect();
        let schemas = CapabilitySchemaIndex::from_pairs(
            entries
                .into_iter()
                .map(|c| (c.id, c.input_schema))
                .collect(),
        );
        let estimated_cost = input
            .get("estimated_cost_usd")
            .and_then(JsonValue::as_f64)
            .unwrap_or(0.0);
        validate_plan(
            &plan,
            estimated_cost,
            &PlanConstraints::default(),
            &schemas,
            &refs,
            // 5.3: constraints are `default()` here (must_be_local
            // false) so the locality rule is inert — execution-side
            // locality is the executing node's policy (§10.4). Pass
            // the empty set explicitly rather than a mesh-derived one.
            &std::collections::HashSet::new(),
        )
        .map_err(|e| CapabilityError::InvalidInput(format!("plan validation failed: {e}")))?;

        let timeout = Duration::from_millis(
            input
                .get("timeout_ms")
                .and_then(JsonValue::as_u64)
                .unwrap_or(DEFAULT_TIMEOUT_MS)
                .clamp(1_000, MAX_TIMEOUT_MS),
        );
        let fail_fast = input
            .get("on_failure")
            .and_then(JsonValue::as_str)
            .unwrap_or("fail_fast")
            == "fail_fast";

        drive_plan(DriveArgs {
            exec: self.exec.clone(),
            ctx,
            plan,
            schemas: &schemas,
            timeout,
            fail_fast,
        })
        .await
    }
}

struct DriveArgs<'a> {
    exec: Arc<dyn PlanExec>,
    ctx: &'a ExecutionContext,
    plan: Plan,
    schemas: &'a CapabilitySchemaIndex,
    timeout: Duration,
    fail_fast: bool,
}

#[allow(clippy::too_many_lines)]
async fn drive_plan(args: DriveArgs<'_>) -> Result<JsonValue, CapabilityError> {
    let DriveArgs {
        exec,
        ctx,
        plan,
        schemas,
        timeout,
        fail_fast,
    } = args;
    let started = tokio::time::Instant::now();
    let mut scheduler = DagScheduler::new(&plan)
        .map_err(|e| CapabilityError::InvalidInput(format!("plan graph rejected: {e}")))?;
    // 4.6: sink rides ExecutionContext (ADR-0024 promotion).
    let sink = ctx.frame_sink.clone();
    let mut records: HashMap<TaskId, StepRecord> = plan
        .tasks
        .iter()
        .map(|(&id, node)| {
            (
                id,
                StepRecord {
                    capability: node.capability.clone(),
                    state: StepState::Waiting,
                    output: None,
                    error: None,
                    row_id: None,
                },
            )
        })
        .collect();
    // node-id → row-id, written by runner futures at submit time.
    let row_ids: Arc<parking_lot::Mutex<HashMap<TaskId, TaskId>>> =
        Arc::new(parking_lot::Mutex::new(HashMap::new()));

    let (tx, mut rx) = tokio::sync::mpsc::channel::<ReadyStep>(plan.tasks.len().max(1));
    // Send-order node ids: FanoutEvent::Item.index resolves through it.
    let mut sent: Vec<TaskId> = Vec::new();

    let spec = {
        let run_exec = exec.clone();
        let lw_exec = exec.clone();
        let row_ids = row_ids.clone();
        let run_sink = sink.clone();
        let parent = ctx.task_id;
        let plan_id = plan.id;
        FanoutSpec {
            source: Box::pin(futures::stream::poll_fn(move |cx| rx.poll_recv(cx))),
            run: Box::new(move |_index, step: ReadyStep| {
                let exec = run_exec.clone();
                let row_ids = row_ids.clone();
                let sink = run_sink.clone();
                // Remaining plan budget at pull time (4.1 MAJOR-1 rule),
                // further clamped by the step's own timeout.
                let budget = timeout
                    .saturating_sub(started.elapsed())
                    .max(Duration::from_secs(1))
                    .min(Duration::from_millis(u64::from(step.timeout_ms)));
                #[allow(clippy::cast_possible_truncation)]
                let budget_ms = budget.as_millis().min(u128::from(u32::MAX)) as u32;
                Box::pin(async move {
                    // Row insertion happens HERE — an unpulled step has
                    // no task row (O(window), rule 5).
                    match exec.submit_step(
                        &step.capability,
                        step.input,
                        parent,
                        plan_id,
                        step.hints,
                        budget_ms,
                    ) {
                        Ok(row) => {
                            row_ids.lock().insert(step.node_id, row);
                            // 4.8 (plan review MAJOR-2): announce the
                            // dispatch so the live DAG can light the
                            // node before settle — the settle-time
                            // frames only ever carry terminal states.
                            if let Some(sink) = sink.as_ref() {
                                let chunk = json!({ "step": {
                                    "id": step.node_id.0.to_string(),
                                    "capability": step.capability,
                                    "state": harness_orchestrator::StepState::InFlight.as_str(),
                                    "task_id": row.0.to_string(),
                                }});
                                sink(
                                    parent,
                                    LogFrame {
                                        stream: StreamKind::Progress,
                                        line: chunk.to_string(),
                                    },
                                );
                            }
                            match exec.await_terminal(row, budget).await {
                                SubTaskOutcome::Done(v) => ItemOutcome::Ok(v),
                                SubTaskOutcome::Failed(e) => ItemOutcome::Failed(e),
                                SubTaskOutcome::TimedOut => ItemOutcome::TimedOut,
                            }
                        }
                        Err(e) => ItemOutcome::Failed(format!("submit failed: {e}")),
                    }
                }) as futures::future::BoxFuture<'static, ItemOutcome<JsonValue>>
            }),
            window: WindowPolicy::default_per_workers(),
            live_workers: Box::new(move || lw_exec.live_workers()),
            deadline: Some(Box::pin(tokio::time::sleep(timeout))),
            // Single-layer failure policy (plan review MAJOR-4): the
            // driver implements FailFast itself; the controller never
            // aborts on its own.
            on_failure: harness_core::PartialPolicy::ReturnPartial,
        }
    };
    let mut stream = FanoutController::stream(spec);

    // Seed + per-completion feeding, with resolution failures settling
    // synchronously (they cascade like any failure).
    let mut aborted = false;
    let mut deadline_hit = false;
    let initial = scheduler.take_initial_ready();
    if feed_ready(
        initial,
        &plan,
        &mut scheduler,
        schemas,
        &tx,
        &mut sent,
        &mut records,
        sink.as_ref(),
        ctx.task_id,
        &row_ids,
        fail_fast,
    ) {
        aborted = true;
    }
    if !aborted && !scheduler.is_settled() {
        loop {
            match stream.next().await {
                Some(FanoutEvent::Item { index, outcome }) => {
                    let Some(&node_id) = sent.get(usize::try_from(index).unwrap_or(usize::MAX))
                    else {
                        continue;
                    };
                    let step_outcome = match outcome {
                        ItemOutcome::Ok(v) => StepOutcome::Done(v),
                        ItemOutcome::Failed(e) => StepOutcome::Failed(e),
                        ItemOutcome::TimedOut => StepOutcome::TimedOut,
                    };
                    let fatal = !matches!(step_outcome, StepOutcome::Done(_));
                    let progress = match scheduler.complete(node_id, step_outcome.clone()) {
                        Ok(p) => p,
                        Err(e) => {
                            tracing::warn!(target: "harness.plan_exec", %e, "complete");
                            continue;
                        }
                    };
                    record_settled(&mut records, node_id, &step_outcome, &row_ids);
                    emit_step_frame(sink.as_ref(), ctx.task_id, node_id, &records, &row_ids);
                    for skipped in &progress.newly_skipped {
                        records
                            .entry(*skipped)
                            .and_modify(|r| r.state = StepState::Skipped);
                        emit_step_frame(sink.as_ref(), ctx.task_id, *skipped, &records, &row_ids);
                    }
                    if fatal && fail_fast {
                        aborted = true;
                        break;
                    }
                    if feed_ready(
                        progress.newly_ready,
                        &plan,
                        &mut scheduler,
                        schemas,
                        &tx,
                        &mut sent,
                        &mut records,
                        sink.as_ref(),
                        ctx.task_id,
                        &row_ids,
                        fail_fast,
                    ) {
                        aborted = true;
                        break;
                    }
                    if scheduler.is_settled() {
                        break;
                    }
                }
                Some(FanoutEvent::End(summary)) => {
                    // Controller summary is discarded (DagSummary is
                    // authoritative) — but the reason matters.
                    if summary.reason == harness_orchestrator::EndReason::DeadlineExceeded {
                        deadline_hit = true;
                    }
                    break;
                }
                None => break,
            }
        }
    }
    // Cancel in-flight runner futures (their rows orphan-complete under
    // their own timeouts — ADR-0022) and settle the graph.
    drop(stream);
    drop(tx);
    for skipped in scheduler.skip_remaining() {
        records
            .entry(skipped)
            .and_modify(|r| r.state = StepState::Skipped);
        emit_step_frame(sink.as_ref(), ctx.task_id, skipped, &records, &row_ids);
    }

    let summary = scheduler.summary();
    #[allow(clippy::cast_possible_truncation)]
    let duration_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
    let steps: serde_json::Map<String, JsonValue> = records
        .iter()
        .map(|(id, r)| {
            let mut entry = json!({
                "capability": r.capability,
                "state": r.state.as_str(),
            });
            if let Some(row) = r.row_id {
                entry["task_id"] = json!(row.0.to_string());
            }
            if let Some(o) = &r.output {
                entry["output"] = o.clone();
            }
            if let Some(e) = &r.error {
                entry["error"] = json!(e);
            }
            (id.0.to_string(), entry)
        })
        .collect();
    let aggregate = json!({
        "plan_id": plan.id.0.to_string(),
        "name": plan.name,
        "ok": summary.done,
        "failed": summary.failed,
        "timed_out": summary.timed_out,
        "skipped": summary.skipped,
        "duration_ms": duration_ms,
        "steps": JsonValue::Object(steps),
    });
    if let Some(sink) = &sink {
        let chunk = json!({ "plan_summary": {
            "ok": summary.done, "failed": summary.failed,
            "timed_out": summary.timed_out, "skipped": summary.skipped,
            "total": summary.total,
        }});
        sink(
            ctx.task_id,
            LogFrame {
                stream: StreamKind::Progress,
                line: chunk.to_string(),
            },
        );
    }

    // Terminal rule (ADR-0025).
    let all_done = summary.done == summary.total;
    if all_done || (!aborted && !deadline_hit && summary.done > 0) {
        Ok(aggregate)
    } else {
        let why = if deadline_hit {
            "plan deadline exceeded"
        } else if aborted {
            "plan aborted (fail_fast)"
        } else {
            "no step succeeded"
        };
        Err(CapabilityError::Failed(format!(
            "{why}: {} ok, {} failed, {} timed out, {} skipped of {}",
            summary.done, summary.failed, summary.timed_out, summary.skipped, summary.total
        )))
    }
}

/// Resolve + schema-recheck each ready step and feed it to the window;
/// a resolution/validation failure settles the step as `Failed`
/// synchronously (cascading), which may ready or skip further steps —
/// processed iteratively. Under `fail_fast`, the first such failure
/// stops feeding and returns `true` so the caller aborts the plan
/// (diff review MAJOR-1 — a feed-time failure is a step failure).
#[allow(clippy::too_many_arguments)]
fn feed_ready(
    ready: Vec<TaskId>,
    plan: &Plan,
    scheduler: &mut DagScheduler,
    schemas: &CapabilitySchemaIndex,
    tx: &tokio::sync::mpsc::Sender<ReadyStep>,
    sent: &mut Vec<TaskId>,
    records: &mut HashMap<TaskId, StepRecord>,
    sink: Option<&FrameSink>,
    plan_task: TaskId,
    row_ids: &Arc<parking_lot::Mutex<HashMap<TaskId, TaskId>>>,
    fail_fast: bool,
) -> bool {
    let mut queue = ready;
    while !queue.is_empty() {
        let node_id = queue.remove(0);
        let Some(node) = plan.tasks.get(&node_id) else {
            continue;
        };
        let resolved = resolve_output_refs(&node.input, &|dep| scheduler.output_of(dep).cloned())
            .map_err(|e| e.to_string())
            .and_then(|input| {
                // ADR-0025: ref-bearing nodes deferred plan-time schema
                // validation; re-check the RESOLVED input now. Unknown
                // schema here means the capability was manifest-known but
                // its schema failed to compile — strict reject.
                match schemas.validate(&node.capability, &input) {
                    Ok(()) => Ok(input),
                    Err(errors) => Err(format!("resolved input rejected: {errors:?}")),
                }
            });
        match resolved {
            Ok(input) => {
                sent.push(node_id);
                // Capacity == plan size and each node sent at most
                // once: try_send cannot fail while the receiver lives.
                if tx
                    .try_send(ReadyStep {
                        node_id,
                        capability: node.capability.clone(),
                        input,
                        hints: node.resource_hints.clone(),
                        timeout_ms: node.timeout_ms.unwrap_or(DEFAULT_STEP_TIMEOUT_MS),
                    })
                    .is_err()
                {
                    tracing::error!(target: "harness.plan_exec", "ready channel closed early");
                }
            }
            Err(err) => {
                let outcome = StepOutcome::Failed(err);
                if let Ok(progress) = scheduler.complete(node_id, outcome.clone()) {
                    record_settled(records, node_id, &outcome, row_ids);
                    emit_step_frame(sink, plan_task, node_id, records, row_ids);
                    for skipped in &progress.newly_skipped {
                        records
                            .entry(*skipped)
                            .and_modify(|r| r.state = StepState::Skipped);
                        emit_step_frame(sink, plan_task, *skipped, records, row_ids);
                    }
                    if fail_fast {
                        return true;
                    }
                    queue.extend(progress.newly_ready);
                }
            }
        }
    }
    false
}

fn record_settled(
    records: &mut HashMap<TaskId, StepRecord>,
    node_id: TaskId,
    outcome: &StepOutcome,
    row_ids: &Arc<parking_lot::Mutex<HashMap<TaskId, TaskId>>>,
) {
    if let Some(r) = records.get_mut(&node_id) {
        r.row_id = row_ids.lock().get(&node_id).copied();
        match outcome {
            StepOutcome::Done(v) => {
                r.state = StepState::Done;
                r.output = Some(v.clone());
            }
            StepOutcome::Failed(e) => {
                r.state = StepState::Failed;
                r.error = Some(e.clone());
            }
            StepOutcome::TimedOut => {
                r.state = StepState::TimedOut;
                r.error = Some("timed out".into());
            }
        }
    }
}

fn emit_step_frame(
    sink: Option<&FrameSink>,
    plan_task: TaskId,
    node_id: TaskId,
    records: &HashMap<TaskId, StepRecord>,
    row_ids: &Arc<parking_lot::Mutex<HashMap<TaskId, TaskId>>>,
) {
    let Some(sink) = sink else { return };
    let Some(r) = records.get(&node_id) else {
        return;
    };
    let mut chunk = json!({ "step": {
        "id": node_id.0.to_string(),
        "capability": r.capability,
        "state": r.state.as_str(),
    }});
    if let Some(row) = row_ids.lock().get(&node_id) {
        chunk["step"]["task_id"] = json!(row.0.to_string());
    }
    if let Some(e) = &r.error {
        chunk["step"]["error"] = json!(e);
    }
    sink(
        plan_task,
        LogFrame {
            stream: StreamKind::Progress,
            line: chunk.to_string(),
        },
    );
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::similar_names
)]
mod tests {
    use super::*;
    use harness_core::{NodeId, PlanNode, Signature};
    use parking_lot::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    type Frames = Arc<Mutex<Vec<(TaskId, LogFrame)>>>;
    /// `(row, capability, input, parent, plan_id)`.
    type SubmitRecord = (TaskId, String, JsonValue, TaskId, PlanId);

    /// 4.6: tests capture frames by stamping a recording closure into
    /// the ctx (the executor's role in production).
    fn ctx_with_sink() -> (ExecutionContext, Frames) {
        let frames: Frames = Arc::new(Mutex::new(vec![]));
        let sink_frames = frames.clone();
        let mut c = ctx();
        c.frame_sink = Some(Arc::new(move |task: TaskId, frame: LogFrame| {
            sink_frames.lock().push((task, frame));
        }));
        (c, frames)
    }

    fn ctx() -> ExecutionContext {
        ExecutionContext {
            local_node: NodeId::from_bytes([1; 16]),
            local_node_name: Arc::from("self"),
            issued_by: NodeId::from_bytes([1; 16]),
            issued_by_name: Arc::from("self"),
            task_id: TaskId::new_v7(),
            tags: Arc::from(Vec::<String>::new()),
            frame_sink: None,
        }
    }

    fn hints() -> ResourceHints {
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

    fn echo_entry() -> ManifestEntry {
        ManifestEntry {
            id: "echo".into(),
            version: harness_core::SemVer::new(0, 1, 0),
            cardinality: Cardinality::Anyone,
            input_schema: json!({ "type": "object" }),
            output_schema: json!({ "type": "object" }),
            cost_hint: harness_core::protocol::CostHint::LocalFast,
            tags: vec![],
            rate_limit: None,
            resource_hints: hints(),
            requires_secrets: vec![],
        }
    }

    struct FakePlanExec {
        /// `(row, capability, input, parent, plan_id)` per submit.
        submits: Mutex<Vec<SubmitRecord>>,
        /// Budgets (`timeout_ms`) per submit, in order.
        budgets: Mutex<Vec<u32>>,
        gate: tokio::sync::Semaphore,
        non_terminal: AtomicUsize,
        peak: AtomicUsize,
        workers: usize,
    }

    impl FakePlanExec {
        fn new(workers: usize, open_gate: usize) -> Arc<Self> {
            Arc::new(Self {
                submits: Mutex::new(vec![]),
                budgets: Mutex::new(vec![]),
                gate: tokio::sync::Semaphore::new(open_gate),
                non_terminal: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
                workers,
            })
        }
    }

    #[async_trait]
    impl PlanExec for FakePlanExec {
        fn submit_step(
            &self,
            capability: &str,
            input: JsonValue,
            parent: TaskId,
            plan_id: PlanId,
            _hints: ResourceHints,
            timeout_ms: u32,
        ) -> Result<TaskId, CapabilityError> {
            let row = TaskId::new_v7();
            self.submits
                .lock()
                .push((row, capability.to_string(), input, parent, plan_id));
            self.budgets.lock().push(timeout_ms);
            let n = self.non_terminal.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(n, Ordering::SeqCst);
            Ok(row)
        }
        async fn await_terminal(&self, id: TaskId, _deadline: Duration) -> SubTaskOutcome {
            let permit = self.gate.acquire().await;
            self.non_terminal.fetch_sub(1, Ordering::SeqCst);
            match permit {
                Ok(p) => p.forget(),
                Err(_) => return SubTaskOutcome::Failed("gate closed".into()),
            }
            let input = self
                .submits
                .lock()
                .iter()
                .find(|(row, ..)| *row == id)
                .map_or(JsonValue::Null, |(_, _, input, ..)| input.clone());
            if input.get("fail").is_some() {
                SubTaskOutcome::Failed("step exploded".into())
            } else {
                SubTaskOutcome::Done(json!({ "echoed": input }))
            }
        }
        fn live_workers(&self) -> usize {
            self.workers
        }
        fn known_capabilities(&self) -> Vec<ManifestEntry> {
            vec![echo_entry()]
        }
    }

    fn node_of(id: TaskId, input: JsonValue) -> PlanNode {
        PlanNode {
            id,
            capability: "echo".into(),
            input,
            resource_hints: hints(),
            timeout_ms: None,
        }
    }

    fn plan_json(nodes: Vec<PlanNode>, edges: Vec<(TaskId, TaskId)>) -> JsonValue {
        let plan = Plan {
            id: PlanId::new_v7(),
            name: "test-plan".into(),
            tasks: nodes.into_iter().map(|n| (n.id, n)).collect(),
            edges,
            budget: None,
            checkpoint: None,
            issued_by: NodeId::from_bytes([1; 16]),
            sig: Signature::from_bytes([0u8; 64]),
        };
        serde_json::to_value(&plan).expect("plan json")
    }

    fn sorted_ids(n: usize) -> Vec<TaskId> {
        let mut v: Vec<TaskId> = (0..n).map(|_| TaskId::new_v7()).collect();
        v.sort_unstable();
        v
    }

    #[tokio::test]
    async fn t01_chain_threads_outputs_and_records_rows() {
        let ids = sorted_ids(2);
        let (a, b) = (ids[0], ids[1]);
        let exec = FakePlanExec::new(1, 100);
        let cap = PlanExecCapability::new(exec.clone());
        let c = ctx();
        let input = json!({ "plan": plan_json(
            vec![
                node_of(a, json!({"msg": "one"})),
                node_of(b, json!({"prev": {"$task_output": a.0.to_string(), "pointer": "/echoed/msg"}})),
            ],
            vec![(b, a)],
        )});
        let out = cap.execute(&c, input).await.expect("execute");
        assert_eq!(out["ok"], 2);
        assert_eq!(out["failed"], 0);
        let steps = out["steps"].as_object().expect("steps");
        let b_entry = &steps[&b.0.to_string()];
        assert_eq!(b_entry["state"], "done");
        assert_eq!(b_entry["output"]["echoed"]["prev"], "one", "threaded");
        assert!(b_entry["task_id"].as_str().is_some(), "row id recorded");
        // Rows carried parent + plan_id.
        let submits = exec.submits.lock();
        assert_eq!(submits.len(), 2);
        assert!(submits
            .iter()
            .all(|(_, _, _, parent, _)| *parent == c.task_id));
    }

    #[tokio::test]
    async fn t02_rows_bounded_by_window() {
        // 40 independent steps, workers=2 → window clamp(4,4,64)=4.
        let ids = sorted_ids(40);
        let exec = FakePlanExec::new(2, 0);
        let cap = PlanExecCapability::new(exec.clone());
        let c = ctx();
        let input = json!({ "plan": plan_json(
            ids.iter().map(|&i| node_of(i, json!({"i": i.0.to_string()}))).collect(),
            vec![],
        )});
        let handle = tokio::spawn(async move { cap.execute(&c, input).await });
        let e = exec.clone();
        for _ in 0..10_000 {
            if e.non_terminal.load(Ordering::SeqCst) == 4 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(exec.submits.lock().len(), 4, "unpulled steps have no rows");
        exec.gate.add_permits(40);
        let out = handle.await.expect("join").expect("execute");
        assert_eq!(out["ok"], 40);
        assert_eq!(exec.peak.load(Ordering::SeqCst), 4, "O(window) rows");
    }

    #[tokio::test]
    async fn t03_fail_fast_aborts_and_skips() {
        // a fails; b depends on a (skip-cascade); c independent but
        // gated in flight (dropped on abort → skipped).
        let ids = sorted_ids(3);
        let (a, b, c_id) = (ids[0], ids[1], ids[2]);
        let exec = FakePlanExec::new(1, 0);
        let cap = PlanExecCapability::new(exec.clone());
        let c = ctx();
        let input = json!({ "plan": plan_json(
            vec![
                node_of(a, json!({"fail": true})),
                node_of(b, json!({})),
                node_of(c_id, json!({})),
            ],
            vec![(b, a)],
        )});
        let handle = tokio::spawn(async move { cap.execute(&c, input).await });
        // Let both roots submit, then release only a (which fails).
        let e = exec.clone();
        for _ in 0..10_000 {
            if e.non_terminal.load(Ordering::SeqCst) == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
        // Both a and c are in flight; one permit resolves the FIRST
        // gated await — order isn't guaranteed, so release permits one
        // at a time until the plan settles.
        exec.gate.add_permits(2);
        let err = handle
            .await
            .expect("join")
            .expect_err("fail_fast plan fails");
        let msg = err.to_string();
        assert!(msg.contains("aborted") || msg.contains("failed"), "{msg}");
    }

    #[tokio::test]
    async fn t04_continue_mode_runs_independent_branches() {
        let ids = sorted_ids(3);
        let (a, b, c_id) = (ids[0], ids[1], ids[2]);
        let exec = FakePlanExec::new(1, 100);
        let cap = PlanExecCapability::new(exec.clone());
        let c = ctx();
        let input = json!({
            "on_failure": "continue",
            "plan": plan_json(
                vec![
                    node_of(a, json!({"fail": true})),
                    node_of(b, json!({})),
                    node_of(c_id, json!({"msg": "independent"})),
                ],
                vec![(b, a)],
            ),
        });
        let out = cap.execute(&c, input).await.expect("partial success is Ok");
        assert_eq!(out["ok"], 1);
        assert_eq!(out["failed"], 1);
        assert_eq!(out["skipped"], 1);
        let steps = out["steps"].as_object().unwrap();
        assert_eq!(steps[&b.0.to_string()]["state"], "skipped");
        assert_eq!(steps[&c_id.0.to_string()]["state"], "done");
    }

    #[tokio::test]
    async fn t05_entry_validation_rejects_bad_plans() {
        let exec = FakePlanExec::new(1, 100);
        let cap = PlanExecCapability::new(exec);
        let c = ctx();
        // Unknown capability.
        let a = TaskId::new_v7();
        let mut bad_cap = node_of(a, json!({}));
        bad_cap.capability = "no.such.thing".into();
        let err = cap
            .execute(&c, json!({ "plan": plan_json(vec![bad_cap], vec![]) }))
            .await
            .expect_err("unknown capability");
        assert!(matches!(err, CapabilityError::InvalidInput(_)));
        // Cycle.
        let ids = sorted_ids(2);
        let err = cap
            .execute(
                &c,
                json!({ "plan": plan_json(
                    vec![node_of(ids[0], json!({})), node_of(ids[1], json!({}))],
                    vec![(ids[0], ids[1]), (ids[1], ids[0])],
                )}),
            )
            .await
            .expect_err("cycle");
        assert!(matches!(err, CapabilityError::InvalidInput(_)));
        // Nested plan.
        let n = TaskId::new_v7();
        let mut nested = node_of(n, json!({}));
        nested.capability = "plan.execute".into();
        let err = cap
            .execute(&c, json!({ "plan": plan_json(vec![nested], vec![]) }))
            .await
            .expect_err("nested plan");
        assert!(matches!(err, CapabilityError::InvalidInput(_)));
        // Oversized.
        let big = sorted_ids(MAX_PLAN_STEPS + 1);
        let err = cap
            .execute(
                &c,
                json!({ "plan": plan_json(
                    big.iter().map(|&i| node_of(i, json!({}))).collect(),
                    vec![],
                )}),
            )
            .await
            .expect_err("oversized");
        assert!(matches!(err, CapabilityError::InvalidInput(_)));
    }

    #[tokio::test]
    async fn t06_pointer_miss_fails_step_and_cascades() {
        let ids = sorted_ids(3);
        let (a, b, c_id) = (ids[0], ids[1], ids[2]);
        let exec = FakePlanExec::new(1, 100);
        let cap = PlanExecCapability::new(exec);
        let c = ctx();
        let input = json!({
            "on_failure": "continue",
            "plan": plan_json(
                vec![
                    node_of(a, json!({"msg": "one"})),
                    node_of(b, json!({"x": {"$task_output": a.0.to_string(), "pointer": "/does/not/exist"}})),
                    node_of(c_id, json!({"y": {"$task_output": b.0.to_string()}})),
                ],
                vec![(b, a), (c_id, b)],
            ),
        });
        let out = cap.execute(&c, input).await.expect("a succeeded");
        assert_eq!(out["ok"], 1);
        assert_eq!(out["failed"], 1);
        assert_eq!(out["skipped"], 1);
        let steps = out["steps"].as_object().unwrap();
        assert_eq!(steps[&b.0.to_string()]["state"], "failed");
        assert!(steps[&b.0.to_string()]["error"]
            .as_str()
            .unwrap()
            .contains("pointer"));
        assert_eq!(steps[&c_id.0.to_string()]["state"], "skipped");
    }

    #[tokio::test]
    async fn t07_second_concurrent_plan_fails_fast() {
        let ids = sorted_ids(1);
        let exec = FakePlanExec::new(1, 0); // gate closed: plan 1 runs "forever"
        let cap = Arc::new(PlanExecCapability::new(exec.clone()));
        let c1 = ctx();
        let plan1 = json!({ "plan": plan_json(vec![node_of(ids[0], json!({}))], vec![]) });
        let cap1 = cap.clone();
        let h = tokio::spawn(async move { cap1.execute(&c1, plan1).await });
        let e = exec.clone();
        for _ in 0..10_000 {
            if e.non_terminal.load(Ordering::SeqCst) == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        // Plan 2 must fail immediately, not queue.
        let id2 = TaskId::new_v7();
        let err = cap
            .execute(
                &ctx(),
                json!({ "plan": plan_json(vec![node_of(id2, json!({}))], vec![]) }),
            )
            .await
            .expect_err("second plan rejected");
        assert!(err.to_string().contains("another plan"), "{err}");
        exec.gate.add_permits(10);
        h.await.expect("join").expect("first plan completes");
    }

    #[tokio::test]
    async fn t08_progress_frames_per_step_plus_summary() {
        let ids = sorted_ids(2);
        let (a, b) = (ids[0], ids[1]);
        let exec = FakePlanExec::new(1, 100);
        let cap = PlanExecCapability::new(exec);
        let (c, frames) = ctx_with_sink();
        let task = c.task_id;
        cap.execute(
            &c,
            json!({ "plan": plan_json(
                vec![node_of(a, json!({})), node_of(b, json!({}))],
                vec![(b, a)],
            )}),
        )
        .await
        .expect("execute");
        let captured = frames.lock();
        assert!(captured
            .iter()
            .all(|(t, f)| *t == task && f.stream == StreamKind::Progress));
        let chunks: Vec<JsonValue> = captured
            .iter()
            .map(|(_, f)| serde_json::from_str(&f.line).expect("json"))
            .collect();
        let step_frames: Vec<&JsonValue> = chunks.iter().filter(|c| !c["step"].is_null()).collect();
        // 4.8: each dispatched step emits an in_flight frame at submit
        // time (with task_id) and a settle frame — 2 steps → 4 frames.
        assert_eq!(step_frames.len(), 4);
        let in_flight: Vec<_> = step_frames
            .iter()
            .filter(|f| f["step"]["state"] == "in_flight")
            .collect();
        assert_eq!(in_flight.len(), 2);
        assert!(
            in_flight.iter().all(|f| f["step"]["task_id"].is_string()),
            "in_flight frames carry the row id for drill-down"
        );
        assert_eq!(
            step_frames
                .iter()
                .filter(|f| f["step"]["state"] == "done")
                .count(),
            2
        );
        let summary = chunks
            .iter()
            .find(|c| !c["plan_summary"].is_null())
            .expect("summary frame");
        assert_eq!(summary["plan_summary"]["ok"], 2);
        assert_eq!(summary["plan_summary"]["total"], 2);
    }

    #[tokio::test]
    async fn t11_fail_fast_honors_feed_time_failures() {
        // Diff review MAJOR-1: a pointer-miss at resolution time is a
        // step failure — under fail_fast the plan must abort and
        // terminal-ize Failed, not report Done with failed steps.
        let ids = sorted_ids(3);
        let (a, b, c_id) = (ids[0], ids[1], ids[2]);
        let exec = FakePlanExec::new(1, 100);
        let cap = PlanExecCapability::new(exec.clone());
        let c = ctx();
        let input = json!({ "plan": plan_json(
            vec![
                node_of(a, json!({"msg": "one"})),
                node_of(b, json!({"x": {"$task_output": a.0.to_string(), "pointer": "/nope"}})),
                node_of(c_id, json!({"independent": true})),
            ],
            // c depends on NOTHING but is sequenced after b's failure
            // window via the chain a→b; keep c independent so the test
            // proves the abort stops even unrelated branches.
            vec![(b, a)],
        )});
        let err = cap
            .execute(&c, input)
            .await
            .expect_err("fail_fast plan must fail on a feed-time failure");
        assert!(err.to_string().contains("aborted"), "{err}");
    }

    #[tokio::test(start_paused = true)]
    async fn t09_deadline_skips_unfinished_and_fails_plan() {
        let ids = sorted_ids(3);
        let exec = FakePlanExec::new(1, 0); // nothing ever completes
        let cap = PlanExecCapability::new(exec.clone());
        let c = ctx();
        let err = cap
            .execute(
                &c,
                json!({
                    "timeout_ms": 1000,
                    "plan": plan_json(
                        ids.iter().map(|&i| node_of(i, json!({}))).collect(),
                        vec![],
                    ),
                }),
            )
            .await
            .expect_err("deadline");
        assert!(err.to_string().contains("deadline"), "{err}");
    }

    #[tokio::test]
    async fn t10_late_steps_get_remaining_budget() {
        // Chain a→b with a slow a: b's submitted budget must be less
        // than the full plan budget.
        let ids = sorted_ids(2);
        let (a, b) = (ids[0], ids[1]);
        let exec = FakePlanExec::new(1, 0);
        let cap = PlanExecCapability::new(exec.clone());
        let c = ctx();
        let input = json!({
            "timeout_ms": 60_000,
            "plan": plan_json(
                vec![node_of(a, json!({})), node_of(b, json!({}))],
                vec![(b, a)],
            ),
        });
        let handle = tokio::spawn(async move { cap.execute(&c, input).await });
        let e = exec.clone();
        for _ in 0..10_000 {
            if e.non_terminal.load(Ordering::SeqCst) == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        // Steps default to 30s, clamped by remaining budget (~60s) → 30_000.
        assert_eq!(exec.budgets.lock()[0], 30_000);
        exec.gate.add_permits(2);
        handle.await.expect("join").expect("execute");
        assert_eq!(exec.budgets.lock().len(), 2);
    }
}
