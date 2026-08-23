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
    resolve_output_refs, Capability as ManifestEntry, CapabilityRef, Cardinality, HashFn, Plan,
    PlanId, ResourceHints, TaskId,
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

    /// 5.10 (ADR-0038): has the plan.execute task ITSELF been
    /// cancelled? Checked once per step completion — the stop button
    /// stops new step mints at the next completion boundary. Default
    /// `false` (harness-api's validation-only context has no store).
    fn own_cancelled(&self, _id: TaskId) -> bool {
        false
    }

    /// 5.11 (ADR-0039): the output already recorded for this plan NODE,
    /// if it ran on this same resolved input (the hash is the validity
    /// check, never the identity — plan review BLOCKER-1). A hit
    /// settles the step WITHOUT dispatching it, so an interrupted plan
    /// resumes where it stopped. Default `None`: every step runs.
    fn checkpoint_lookup(
        &self,
        _plan: PlanId,
        _node: TaskId,
        _input_hash: &[u8; 32],
    ) -> Option<JsonValue> {
        None
    }

    /// 5.11: record a completed step's output under its resolved-input
    /// hash. Best-effort — a failure to persist costs a re-run on
    /// resume, never correctness. Default no-op.
    fn checkpoint_record(
        &self,
        _plan: PlanId,
        _node: TaskId,
        _input_hash: &[u8; 32],
        _step_row: Option<TaskId>,
        _output: &JsonValue,
    ) {
    }

    /// 5.11: the plan finished cleanly — its checkpoints exist to
    /// survive interruption, not as a cache, so drop them. NOT called
    /// for budget stops, cancels, aborts or crashes (5.12 resumes
    /// from those). Default no-op.
    fn checkpoint_finish(&self, _plan: PlanId) {}

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
    /// 5.11: this step did not execute in THIS run — its output came
    /// from a checkpoint. The aggregate says so rather than claiming
    /// fresh work (and its cost is not this run's).
    from_checkpoint: bool,
}

pub struct PlanExecCapability {
    exec: Arc<dyn PlanExec>,
    /// 5.8 (ADR-0036): policy default cap for budget-less plans.
    default_plan_budget_usd: Option<f64>,
    /// 5.8: hard ceiling over every plan when set.
    plan_budget_ceiling_usd: Option<f64>,
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
            default_plan_budget_usd: Some(5.0),
            plan_budget_ceiling_usd: None,
        }
    }

    /// Thread the mesh budget policy (5.8, ADR-0036): `default` caps
    /// plans that carry no `Budget`; `ceiling` hard-caps everything
    /// when set (including plan-carried waivers).
    #[must_use]
    pub fn with_budget_policy(mut self, default: Option<f64>, ceiling: Option<f64>) -> Self {
        self.default_plan_budget_usd = default;
        self.plan_budget_ceiling_usd = ceiling;
        self
    }
}

/// Register `plan.execute`.
///
/// # Panics
/// If the id is already registered (a wiring bug).
#[allow(clippy::expect_used)]
pub fn enrich_with_plan_exec(
    registry: &crate::CapabilityRegistry,
    exec: Arc<dyn PlanExec>,
    default_plan_budget_usd: Option<f64>,
    plan_budget_ceiling_usd: Option<f64>,
) {
    registry
        .register(Arc::new(PlanExecCapability::new(exec).with_budget_policy(
            default_plan_budget_usd,
            plan_budget_ceiling_usd,
        )))
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

        // 5.8 (ADR-0036): resolve the effective budget. The plan's
        // own Budget wins (carrying one IS the §17.8 "explicit
        // approval" — planner backends always emit None, pinned by
        // test); otherwise the policy default; the ceiling hard-caps
        // both when set.
        let budget = harness_orchestrator::BudgetTracker::new(
            plan.budget,
            self.default_plan_budget_usd,
            self.plan_budget_ceiling_usd,
        );

        drive_plan(DriveArgs {
            exec: self.exec.clone(),
            ctx,
            plan,
            schemas: &schemas,
            timeout,
            fail_fast,
            budget,
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
    budget: harness_orchestrator::BudgetTracker,
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
        mut budget,
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
                    from_checkpoint: false,
                },
            )
        })
        .collect();
    // node-id → row-id, written by runner futures at submit time.
    let row_ids: Arc<parking_lot::Mutex<HashMap<TaskId, TaskId>>> =
        Arc::new(parking_lot::Mutex::new(HashMap::new()));

    let (tx, mut rx) = tokio::sync::mpsc::channel::<ReadyStep>(plan.tasks.len().max(1));
    // 5.8 Pause mechanism (plan review B1 + Codex P1 on #59): the
    // sender lives in an Option so the driver can DROP it mid-loop,
    // and the fan-out SOURCE checks this flag before polling the
    // channel — closing the sender alone would still let the window
    // refill from `ReadyStep`s already buffered in the channel. With
    // the flag up the source reports drained immediately, in-flight
    // steps settle, and the stream ends with `SourceDrained`.
    let paused = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut tx = Some(tx);
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
            source: Box::pin({
                let paused = paused.clone();
                futures::stream::poll_fn(move |cx| {
                    if paused.load(std::sync::atomic::Ordering::Relaxed) {
                        return std::task::Poll::Ready(None);
                    }
                    rx.poll_recv(cx)
                })
            }),
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
    // 5.8: set once the budget's on_exceed action fires.
    let mut budget_stop: Option<&'static str> = None;
    // 5.11 (ADR-0039): checkpointing is opt-in per plan and only the
    // Sqlite backend is implemented — `File` runs UNcheckpointed with
    // a visible warning frame rather than pretending (the store is the
    // single-file property; a second on-disk format is not this PR).
    let mut checkpoint_hashes: HashMap<TaskId, [u8; 32]> = HashMap::new();
    let checkpoint_hash_fn = match plan.checkpoint.as_ref() {
        Some(cfg) if cfg.enabled => match cfg.storage {
            harness_core::CheckpointStorage::Sqlite => Some(cfg.input_hash_fn),
            // `None` means "off" — no warning owed. `File` and any
            // future variant (#[non_exhaustive]) run UNcheckpointed
            // and say so out loud.
            harness_core::CheckpointStorage::None => None,
            ref other => {
                if let Some(sink) = sink.as_ref() {
                    sink(
                        ctx.task_id,
                        LogFrame {
                            stream: StreamKind::Progress,
                            line: json!({ "warning": format!(
                                "checkpoint storage {other:?} is not implemented; running uncheckpointed"
                            )})
                            .to_string(),
                        },
                    );
                }
                tracing::warn!(
                    target: "harness.plan_exec",
                    ?other,
                    "unsupported checkpoint storage; running uncheckpointed"
                );
                None
            }
        },
        _ => None,
    };
    let checkpointing = checkpoint_hash_fn.is_some();

    let initial = scheduler.take_initial_ready();
    #[allow(clippy::expect_used)]
    match feed_ready(
        initial,
        &plan,
        &mut scheduler,
        schemas,
        tx.as_ref().expect("sender live at initial feed"),
        &mut sent,
        &mut records,
        sink.as_ref(),
        ctx.task_id,
        &row_ids,
        fail_fast,
        checkpoint_hash_fn
            .map(|hash_fn| CheckpointCtx {
                exec: &exec,
                plan_id: plan.id,
                hash_fn,
                hashes: &mut checkpoint_hashes,
            })
            .as_mut(),
    ) {
        FeedOutcome::Aborted => aborted = true,
        FeedOutcome::Cancelled => budget_stop = Some("cancelled"),
        FeedOutcome::Continue => {}
    }
    // 5.11: a cancel seen during the INITIAL feed (a fully-checkpointed
    // replay settles synchronously) leaves nothing in flight — entering
    // the event loop would just wait out the plan deadline.
    if !aborted && budget_stop.is_none() && !scheduler.is_settled() {
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
                    // 5.11: only successful steps are checkpointed —
                    // re-running a failure on resume is the point.
                    if checkpointing {
                        if let (StepOutcome::Done(output), Some(hash)) =
                            (&step_outcome, checkpoint_hashes.get(&node_id))
                        {
                            exec.checkpoint_record(
                                plan.id,
                                node_id,
                                hash,
                                row_ids.lock().get(&node_id).copied(),
                                output,
                            );
                        }
                    }
                    emit_step_frame(sink.as_ref(), ctx.task_id, node_id, &records, &row_ids);
                    for skipped in &progress.newly_skipped {
                        records
                            .entry(*skipped)
                            .and_modify(|r| r.state = StepState::Skipped);
                        emit_step_frame(sink.as_ref(), ctx.task_id, *skipped, &records, &row_ids);
                    }
                    // 5.8: record ACTUAL cost from Done outputs only
                    // (Failed outcomes carry just an error string —
                    // the $0 undercount is frozen by test, ADR-0036).
                    if let StepOutcome::Done(v) = &step_outcome {
                        match budget.record(v) {
                            harness_orchestrator::BudgetVerdict::Ok => {}
                            harness_orchestrator::BudgetVerdict::SoftCrossed {
                                spent_usd,
                                limit_usd,
                            } => {
                                tracing::warn!(
                                    target: "harness.plan_exec",
                                    spent_usd,
                                    limit_usd,
                                    "plan spend crossed the soft budget limit"
                                );
                                emit_budget_frame(
                                    sink.as_ref(),
                                    ctx.task_id,
                                    &json!({ "event": "soft_limit",
                                            "spent_usd": spent_usd,
                                            "limit_usd": limit_usd }),
                                );
                            }
                            harness_orchestrator::BudgetVerdict::Exceeded {
                                spent_usd,
                                cap_usd,
                                action,
                            } => {
                                let action_str = budget_action_str(action);
                                tracing::warn!(
                                    target: "harness.plan_exec",
                                    spent_usd,
                                    cap_usd,
                                    action = action_str,
                                    "plan spend exceeded the budget cap"
                                );
                                emit_budget_frame(
                                    sink.as_ref(),
                                    ctx.task_id,
                                    &json!({ "event": "exceeded",
                                            "spent_usd": spent_usd,
                                            "cap_usd": cap_usd,
                                            "action": action_str }),
                                );
                                match action {
                                    harness_core::protocol::BudgetAction::Notify => {}
                                    harness_core::protocol::BudgetAction::Pause => {
                                        // Raise the pause flag (the source
                                        // stops pulling even BUFFERED steps
                                        // — Codex P1) then drop the sender
                                        // to wake the stream. In-flight
                                        // steps finish and cost-record; the
                                        // stream ends with SourceDrained.
                                        budget_stop = Some("paused_budget");
                                        paused.store(true, std::sync::atomic::Ordering::Relaxed);
                                        tx = None;
                                    }
                                    // Cancel — and any future action
                                    // (#[non_exhaustive]) — aborts:
                                    // the safe default.
                                    _ => {
                                        budget_stop = Some("aborted_budget");
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    // 5.10 (ADR-0038): the stop button. The plan's
                    // own row was cancelled — stop minting at this
                    // completion boundary, exactly like a budget
                    // Cancel (stranded steps settle Skipped below).
                    // A cancel landing AFTER a budget Pause/Cancel
                    // fired keeps the budget status label — no extra
                    // minting either way, only the aggregate's
                    // `status` string differs (diff review m2).
                    if budget_stop.is_none() && exec.own_cancelled(ctx.task_id) {
                        budget_stop = Some("cancelled");
                        break;
                    }
                    if fatal && fail_fast {
                        aborted = true;
                        break;
                    }
                    if let Some(sender) = tx.as_ref() {
                        match feed_ready(
                            progress.newly_ready,
                            &plan,
                            &mut scheduler,
                            schemas,
                            sender,
                            &mut sent,
                            &mut records,
                            sink.as_ref(),
                            ctx.task_id,
                            &row_ids,
                            fail_fast,
                            checkpoint_hash_fn
                                .map(|hash_fn| CheckpointCtx {
                                    exec: &exec,
                                    plan_id: plan.id,
                                    hash_fn,
                                    hashes: &mut checkpoint_hashes,
                                })
                                .as_mut(),
                        ) {
                            FeedOutcome::Aborted => {
                                aborted = true;
                                break;
                            }
                            FeedOutcome::Cancelled => {
                                budget_stop = Some("cancelled");
                                break;
                            }
                            FeedOutcome::Continue => {}
                        }
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
    // Steps stranded by a budget stop are recorded so 5.12 resume can
    // tell budget-parked from failure-cascade skips (review m5).
    let unscheduled = scheduler.skip_remaining();
    for skipped in &unscheduled {
        records
            .entry(*skipped)
            .and_modify(|r| r.state = StepState::Skipped);
        emit_step_frame(sink.as_ref(), ctx.task_id, *skipped, &records, &row_ids);
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
            // 5.11: never claim fresh execution for a replayed step.
            if r.from_checkpoint {
                entry["from_checkpoint"] = json!(true);
            }
            (id.0.to_string(), entry)
        })
        .collect();
    // 5.8 (plan review B2): the task-envelope TaskState set is NOT
    // extended — plan-level outcome lives in this aggregate `status`
    // field. The discriminator is whether the stop actually PARKED
    // work (Codex P2 on #59): a continue-mode failure before a
    // last-step exceed leaves done < total with nothing
    // budget-cancelled — that plan is "done", not aborted (and m3:
    // a last-step cancel never turns a complete plan into an abort).
    let status = match budget_stop {
        Some(s) if !unscheduled.is_empty() => s,
        _ => "done",
    };
    // 5.11 (ADR-0039): a plan that finished cleanly has nothing left to
    // resume — drop its checkpoints. Budget stops, cancels, aborts,
    // deadline hits and crashes KEEP them (that is what 5.12 resumes
    // from). The condition is EVERY step done, not `status == "done"`
    // (plan review BLOCKER-2): a fail-fast abort also reports "done"
    // with `budget_stop` unset, and GC there would delete exactly the
    // successful prefix the operator resubmits for.
    if checkpointing && summary.done == summary.total {
        exec.checkpoint_finish(plan.id);
    }
    // 5.11 (plan review minor): `ok` counts steps that SUCCEEDED, not
    // steps this run executed — a resume reports ok: N with spent_usd
    // 0.00 because replayed steps were paid for in the earlier run.
    // The count makes that legible instead of surprising.
    let replayed = records.values().filter(|r| r.from_checkpoint).count();
    let mut aggregate = json!({
        "plan_id": plan.id.0.to_string(),
        "name": plan.name,
        "status": status,
        "ok": summary.done,
        "replayed": replayed,
        "failed": summary.failed,
        "timed_out": summary.timed_out,
        "skipped": summary.skipped,
        "duration_ms": duration_ms,
        "steps": JsonValue::Object(steps),
    });
    if budget.active() {
        let mut b = json!({
            "spent_usd": budget.spent_usd(),
            "cap_usd": budget.cap_usd(),
            "soft_limit_usd": budget.soft_limit_usd(),
            "action": budget_action_str(budget.action()),
            "triggered": budget.triggered(),
        });
        if budget_stop.is_some() && !unscheduled.is_empty() {
            b["unscheduled"] = json!(unscheduled
                .iter()
                .map(|id| id.0.to_string())
                .collect::<Vec<_>>());
        }
        aggregate["budget"] = b;
    }
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

    // Terminal rule (ADR-0025; 5.8 addition first). A budget stop is
    // a POLICY verdict over meaningful partial results — it returns
    // Ok with the aggregate (status names the stop) rather than
    // discarding the budget figures into an error string (plan
    // review B2/M1). At least one Done step recorded the tripping
    // cost, so this never masks a total failure.
    let all_done = summary.done == summary.total;
    // Precedence (diff review on #59, per the settled plan decision):
    // deadline expiry and fail-fast aborts WIN over a budget stop — a
    // pause that raced a deadline or a failing in-flight step keeps
    // today's Err semantics; only a clean budget stop returns Ok.
    if budget_stop.is_some() && !aborted && !deadline_hit && summary.done > 0 {
        return Ok(aggregate);
    }
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

/// 5.8: serialize a `BudgetAction` the way the wire does
/// (`snake_case`) without going through `serde_json` for an enum.
fn budget_action_str(action: harness_core::protocol::BudgetAction) -> &'static str {
    match action {
        harness_core::protocol::BudgetAction::Pause => "pause",
        harness_core::protocol::BudgetAction::Notify => "notify",
        _ => "cancel",
    }
}

/// 5.8: budget events ride the same Progress stream as step frames —
/// 5.10's Costs UI reads them; today's DAG view ignores unknown keys.
fn emit_budget_frame(sink: Option<&FrameSink>, plan_task: TaskId, body: &JsonValue) {
    if let Some(sink) = sink {
        sink(
            plan_task,
            LogFrame {
                stream: StreamKind::Progress,
                line: json!({ "budget": body }).to_string(),
            },
        );
    }
}

/// Resolve + schema-recheck each ready step and feed it to the window;
/// a resolution/validation failure settles the step as `Failed`
/// synchronously (cascading), which may ready or skip further steps —
/// processed iteratively. Under `fail_fast`, the first such failure
/// stops feeding and returns `true` so the caller aborts the plan
/// (diff review MAJOR-1 — a feed-time failure is a step failure).
#[allow(clippy::too_many_arguments)]
/// 5.11 (ADR-0039): everything `feed_ready` needs to consult and fill
/// the checkpoint store, or `None` when the plan asked for no
/// checkpointing (then the whole path is inert).
struct CheckpointCtx<'a> {
    exec: &'a Arc<dyn PlanExec>,
    plan_id: PlanId,
    hash_fn: HashFn,
    /// node id → resolved-input hash, so the completion arm can record
    /// the output under the SAME key the next run will look up.
    hashes: &'a mut HashMap<TaskId, [u8; 32]>,
}

/// What a `feed_ready` pass concluded. `Continue` is the common case;
/// the other two stop the driver for different reasons and carry
/// different aggregate semantics (fail-fast Err vs a cancel's Ok).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FeedOutcome {
    Continue,
    /// A resolution failure under `fail_fast`.
    Aborted,
    /// 5.11: the plan.execute row was cancelled mid-replay. A
    /// fully-checkpointed resume never reaches the completion arm, so
    /// the stop button is honored here.
    Cancelled,
}

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
    mut checkpoint: Option<&mut CheckpointCtx<'_>>,
) -> FeedOutcome {
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
                // 5.11: a checkpointed input settles the step HERE —
                // no row, no dispatch, no spend. Dependents resolve
                // against the replayed output exactly as if it had
                // just run, and the settle cascades `newly_ready` the
                // same way the failure arm below does.
                if let Some(cp) = checkpoint.as_deref_mut() {
                    match harness_core::input_hash(&input, cp.hash_fn) {
                        Ok(hash) => {
                            if let Some(cached) =
                                cp.exec.checkpoint_lookup(cp.plan_id, node_id, &hash)
                            {
                                let outcome = StepOutcome::Done(cached);
                                if let Ok(progress) = scheduler.complete(node_id, outcome.clone()) {
                                    record_settled(records, node_id, &outcome, row_ids);
                                    if let Some(r) = records.get_mut(&node_id) {
                                        r.from_checkpoint = true;
                                    }
                                    emit_step_frame(sink, plan_task, node_id, records, row_ids);
                                    // A fully-checkpointed resume never
                                    // reaches the Item arm, so the stop
                                    // button is checked HERE too — else
                                    // a cancelled plan replays to "done"
                                    // (plan review minor).
                                    if cp.exec.own_cancelled(plan_task) {
                                        return FeedOutcome::Cancelled;
                                    }
                                    queue.extend(progress.newly_ready);
                                    continue;
                                }
                                // A scheduler that refuses the settle
                                // (impossible for a ready node) falls
                                // through and runs the step for real.
                            }
                            cp.hashes.insert(node_id, hash);
                        }
                        Err(e) => {
                            tracing::warn!(
                                target: "harness.plan_exec",
                                %e,
                                "input not hashable; step runs uncheckpointed"
                            );
                        }
                    }
                }
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
                        return FeedOutcome::Aborted;
                    }
                    queue.extend(progress.newly_ready);
                }
            }
        }
    }
    FeedOutcome::Continue
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

    /// (plan, node) -> (input hash, output) — the fake's checkpoint map.
    type FakeCheckpoints = HashMap<(PlanId, TaskId), ([u8; 32], JsonValue)>;

    struct FakePlanExec {
        /// `(row, capability, input, parent, plan_id)` per submit.
        submits: Mutex<Vec<SubmitRecord>>,
        /// Budgets (`timeout_ms`) per submit, in order.
        budgets: Mutex<Vec<u32>>,
        gate: tokio::sync::Semaphore,
        non_terminal: AtomicUsize,
        peak: AtomicUsize,
        workers: usize,
        /// 5.10: simulates the plan.execute row being cancelled.
        cancelled: std::sync::atomic::AtomicBool,
        /// 5.11: in-memory checkpoint store, keyed like the real one:
        /// (plan, node) -> (input hash, output).
        checkpoints: Mutex<FakeCheckpoints>,
        /// 5.11: plans whose checkpoints were GC'd by the driver.
        gc_calls: Mutex<Vec<PlanId>>,
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
                cancelled: std::sync::atomic::AtomicBool::new(false),
                checkpoints: Mutex::new(HashMap::new()),
                gc_calls: Mutex::new(vec![]),
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
                // 5.8: a step input carrying `cost` reports that
                // dollar figure as its top-level output `cost_usd`.
                let mut out = json!({ "echoed": input });
                if let Some(c) = input.get("cost") {
                    out["cost_usd"] = c.clone();
                }
                SubTaskOutcome::Done(out)
            }
        }
        fn live_workers(&self) -> usize {
            self.workers
        }
        fn own_cancelled(&self, _id: TaskId) -> bool {
            self.cancelled.load(Ordering::SeqCst)
        }
        fn checkpoint_lookup(
            &self,
            plan: PlanId,
            node: TaskId,
            input_hash: &[u8; 32],
        ) -> Option<JsonValue> {
            self.checkpoints
                .lock()
                .get(&(plan, node))
                .filter(|(stored, _)| stored == input_hash)
                .map(|(_, output)| output.clone())
        }
        fn checkpoint_record(
            &self,
            plan: PlanId,
            node: TaskId,
            input_hash: &[u8; 32],
            _step_row: Option<TaskId>,
            output: &JsonValue,
        ) {
            self.checkpoints
                .lock()
                .insert((plan, node), (*input_hash, output.clone()));
        }
        fn checkpoint_finish(&self, plan: PlanId) {
            self.gc_calls.lock().push(plan);
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

    /// A plan asking for `SQLite` checkpointing, with a caller-chosen id
    /// so a test can pre-seed checkpoints for it.
    fn checkpointed_plan_json(
        plan_id: PlanId,
        nodes: Vec<PlanNode>,
        edges: Vec<(TaskId, TaskId)>,
        storage: harness_core::CheckpointStorage,
    ) -> JsonValue {
        let plan = Plan {
            id: plan_id,
            name: "checkpointed-plan".into(),
            tasks: nodes.into_iter().map(|n| (n.id, n)).collect(),
            edges,
            budget: None,
            checkpoint: Some(harness_core::CheckpointConfig {
                enabled: true,
                interval_items: 1,
                storage,
                input_hash_fn: HashFn::Blake3,
            }),
            issued_by: NodeId::from_bytes([1; 16]),
            sig: Signature::from_bytes([0u8; 64]),
        };
        serde_json::to_value(&plan).expect("plan json")
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

    fn plan_json_with_budget(
        nodes: Vec<PlanNode>,
        edges: Vec<(TaskId, TaskId)>,
        budget: harness_core::protocol::Budget,
    ) -> JsonValue {
        let mut v = plan_json(nodes, edges);
        v["budget"] = serde_json::to_value(budget).expect("budget json");
        v
    }

    fn usd(
        cap: Option<f64>,
        soft: Option<f64>,
        on_exceed: harness_core::protocol::BudgetAction,
    ) -> harness_core::protocol::Budget {
        harness_core::protocol::Budget {
            max_cost_usd: cap,
            soft_limit_usd: soft,
            on_exceed,
        }
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
    // ───────────────────────────── 5.8 budget enforcement (ADR-0036)

    use harness_core::protocol::BudgetAction;

    #[tokio::test]
    async fn e01_cancel_aborts_with_budget_aggregate() {
        let ids = sorted_ids(3);
        let (a, b, c_id) = (ids[0], ids[1], ids[2]);
        let exec = FakePlanExec::new(1, 100);
        let cap = PlanExecCapability::new(exec.clone());
        let (c, frames) = ctx_with_sink();
        let input = json!({ "plan": plan_json_with_budget(
            vec![
                node_of(a, json!({"cost": 0.6})),
                node_of(b, json!({"cost": 0.6})),
                node_of(c_id, json!({})),
            ],
            vec![(b, a), (c_id, b)],
            usd(Some(1.0), None, BudgetAction::Cancel),
        )});
        let out = cap
            .execute(&c, input)
            .await
            .expect("Ok, not Err: the aggregate survives");
        assert_eq!(out["status"], "aborted_budget");
        assert_eq!(out["ok"], 2);
        assert_eq!(out["skipped"], 1);
        assert_eq!(out["budget"]["triggered"], true);
        assert!((out["budget"]["spent_usd"].as_f64().expect("spent") - 1.2).abs() < 1e-9);
        let unscheduled = out["budget"]["unscheduled"].as_array().expect("list");
        assert_eq!(unscheduled, &vec![json!(c_id.0.to_string())]);
        // The exceeded frame reached the Progress stream.
        let n = frames
            .lock()
            .iter()
            .filter(|(_, f)| f.line.contains("\"event\":\"exceeded\""))
            .count();
        assert_eq!(n, 1, "exceeded frame emitted exactly once");
    }

    #[tokio::test]
    async fn e02_pause_parks_unfed_steps_and_settles_promptly() {
        // Diamond: a → (b, c) → d. The SECOND of b/c
        // (order-independent: 0.6 + 0.6) blows the $1 cap → Pause;
        // both b and c count and d is never dispatched. (The instant
        // fake settles steps immediately, so genuine mid-flight
        // behavior — window completions cost-recording after pause —
        // is pinned by e08, not here.)
        let ids = sorted_ids(4);
        let (a, b, c_id, d) = (ids[0], ids[1], ids[2], ids[3]);
        let exec = FakePlanExec::new(2, 100);
        let cap = PlanExecCapability::new(exec.clone());
        let c = ctx();
        let input = json!({ "plan": plan_json_with_budget(
            vec![
                node_of(a, json!({})),
                node_of(b, json!({"cost": 0.6})),
                node_of(c_id, json!({"cost": 0.6})),
                node_of(d, json!({})),
            ],
            vec![(b, a), (c_id, a), (d, b), (d, c_id)],
            usd(Some(1.0), None, BudgetAction::Pause),
        )});
        let started = tokio::time::Instant::now();
        let out = cap.execute(&c, input).await.expect("execute");
        // Plan review B1: the pause path must settle promptly via
        // SourceDrained — not hang to the 120s default plan deadline.
        assert!(started.elapsed() < Duration::from_secs(30), "prompt settle");
        assert_eq!(out["status"], "paused_budget");
        assert_eq!(out["ok"], 3, "a, b AND the in-flight c all finished");
        assert_eq!(out["skipped"], 1);
        assert_eq!(
            out["budget"]["unscheduled"].as_array().expect("list"),
            &vec![json!(d.0.to_string())]
        );
        assert!((out["budget"]["spent_usd"].as_f64().expect("spent") - 1.2).abs() < 1e-9);
    }

    #[tokio::test]
    async fn e03_notify_records_but_never_stops() {
        let ids = sorted_ids(3);
        let (a, b, c_id) = (ids[0], ids[1], ids[2]);
        let exec = FakePlanExec::new(1, 100);
        let cap = PlanExecCapability::new(exec.clone());
        let c = ctx();
        let input = json!({ "plan": plan_json_with_budget(
            vec![
                node_of(a, json!({"cost": 0.6})),
                node_of(b, json!({"cost": 0.6})),
                node_of(c_id, json!({})),
            ],
            vec![(b, a), (c_id, b)],
            usd(Some(1.0), None, BudgetAction::Notify),
        )});
        let out = cap.execute(&c, input).await.expect("execute");
        assert_eq!(out["status"], "done");
        assert_eq!(out["ok"], 3, "notify never stops execution");
        assert_eq!(out["budget"]["triggered"], true);
        assert_eq!(out["budget"]["action"], "notify");
    }

    #[tokio::test]
    async fn e04_soft_limit_warns_once_without_stopping() {
        let ids = sorted_ids(3);
        let (a, b, c_id) = (ids[0], ids[1], ids[2]);
        let exec = FakePlanExec::new(1, 100);
        let cap = PlanExecCapability::new(exec.clone());
        let (c, frames) = ctx_with_sink();
        let input = json!({ "plan": plan_json_with_budget(
            vec![
                node_of(a, json!({"cost": 0.6})),
                node_of(b, json!({"cost": 0.6})),
                node_of(c_id, json!({"cost": 0.6})),
            ],
            vec![(b, a), (c_id, b)],
            usd(Some(100.0), Some(1.0), BudgetAction::Cancel),
        )});
        let out = cap.execute(&c, input).await.expect("execute");
        assert_eq!(out["status"], "done");
        assert_eq!(out["ok"], 3);
        assert_eq!(out["budget"]["triggered"], false);
        let n = frames
            .lock()
            .iter()
            .filter(|(_, f)| f.line.contains("\"event\":\"soft_limit\""))
            .count();
        assert_eq!(
            n, 1,
            "soft frame exactly once across two crossings-worth of spend"
        );
    }

    #[tokio::test]
    async fn e05_policy_default_enforces_and_plan_budget_overrides() {
        let ids = sorted_ids(2);
        let (a, b) = (ids[0], ids[1]);
        let nodes = |a: TaskId, b: TaskId| {
            vec![
                node_of(a, json!({"cost": 0.6})),
                node_of(b, json!({"cost": 0.6})),
            ]
        };
        // No plan budget → policy default ($1, Cancel) trips.
        let exec = FakePlanExec::new(1, 100);
        let cap = PlanExecCapability::new(exec).with_budget_policy(Some(1.0), None);
        let out = cap
            .execute(
                &ctx(),
                json!({ "plan": plan_json(nodes(a, b), vec![(b, a)]) }),
            )
            .await
            .expect("execute");
        assert_eq!(out["status"], "done", "cap tripped on the LAST step (m3)");
        assert_eq!(out["budget"]["triggered"], true);
        assert_eq!(out["budget"]["action"], "cancel");

        // Same steps + a THIRD would be cut; prove the default cancels.
        let ids = sorted_ids(3);
        let (a, b, c_id) = (ids[0], ids[1], ids[2]);
        let exec = FakePlanExec::new(1, 100);
        let cap = PlanExecCapability::new(exec).with_budget_policy(Some(1.0), None);
        let mut three = nodes(a, b);
        three.push(node_of(c_id, json!({})));
        let out = cap
            .execute(
                &ctx(),
                json!({ "plan": plan_json(three, vec![(b, a), (c_id, b)]) }),
            )
            .await
            .expect("execute");
        assert_eq!(out["status"], "aborted_budget");

        // A plan-carried budget with a HIGHER cap overrides the default.
        let ids = sorted_ids(2);
        let (a, b) = (ids[0], ids[1]);
        let exec = FakePlanExec::new(1, 100);
        let cap = PlanExecCapability::new(exec).with_budget_policy(Some(1.0), None);
        let out = cap
            .execute(
                &ctx(),
                json!({ "plan": plan_json_with_budget(
                    nodes(a, b), vec![(b, a)],
                    usd(Some(10.0), None, BudgetAction::Cancel),
                )}),
            )
            .await
            .expect("execute");
        assert_eq!(out["status"], "done");
        assert_eq!(out["budget"]["triggered"], false);
    }

    #[tokio::test]
    async fn e06_waiver_disables_default_unless_ceiling_set() {
        let ids = sorted_ids(2);
        let (a, b) = (ids[0], ids[1]);
        let waived = |a: TaskId, b: TaskId| {
            json!({ "plan": plan_json_with_budget(
                vec![
                    node_of(a, json!({"cost": 0.6})),
                    node_of(b, json!({"cost": 0.6})),
                ],
                vec![(b, a)],
                usd(None, None, BudgetAction::Cancel),
            )})
        };
        // Waiver, no ceiling: unlimited — no budget object at all.
        let exec = FakePlanExec::new(1, 100);
        let cap = PlanExecCapability::new(exec).with_budget_policy(Some(0.5), None);
        let out = cap.execute(&ctx(), waived(a, b)).await.expect("execute");
        assert_eq!(out["status"], "done");
        assert!(out.get("budget").is_none(), "no budget in effect");

        // Waiver UNDER a ceiling: the ceiling caps it (M3).
        let ids = sorted_ids(3);
        let (a, b, c_id) = (ids[0], ids[1], ids[2]);
        let exec = FakePlanExec::new(1, 100);
        let cap = PlanExecCapability::new(exec).with_budget_policy(Some(0.5), Some(1.0));
        let input = json!({ "plan": plan_json_with_budget(
            vec![
                node_of(a, json!({"cost": 0.6})),
                node_of(b, json!({"cost": 0.6})),
                node_of(c_id, json!({})),
            ],
            vec![(b, a), (c_id, b)],
            usd(None, None, BudgetAction::Cancel),
        )});
        let out = cap.execute(&ctx(), input).await.expect("execute");
        assert_eq!(out["status"], "aborted_budget");
        assert!((out["budget"]["cap_usd"].as_f64().expect("cap") - 1.0).abs() < 1e-9);
    }

    #[tokio::test]
    async fn e07_failed_steps_contribute_zero_frozen() {
        // M4 freeze: a cost-then-fail step reports $0 (Failed outcomes
        // carry only an error string). 5.9's result-row costs fix it.
        let ids = sorted_ids(2);
        let (a, b) = (ids[0], ids[1]);
        let exec = FakePlanExec::new(1, 100);
        let cap = PlanExecCapability::new(exec.clone());
        let c = ctx();
        let input = json!({
            "plan": plan_json_with_budget(
                vec![
                    node_of(a, json!({"fail": true, "cost": 5.0})),
                    node_of(b, json!({"cost": 0.6})),
                ],
                vec![],
                usd(Some(1.0), None, BudgetAction::Cancel),
            ),
            "on_failure": "continue",
        });
        let out = cap.execute(&c, input).await.expect("execute");
        assert_eq!(out["budget"]["triggered"], false, "failed $5 never counted");
        assert!((out["budget"]["spent_usd"].as_f64().expect("spent") - 0.6).abs() < 1e-9);
    }
    #[tokio::test]
    async fn e08_pause_never_dispatches_buffered_steps() {
        // Codex P1 on #59: a wide fan-out buffers EVERY ready step in
        // the plan-sized channel up front; pausing must stop the
        // window refilling from that buffer, not just close the
        // sender. 6 independent costed steps, workers=1 (window 4),
        // cap $0.5 Pause: the first completion trips the cap; only
        // the already-pulled window may still finish.
        let ids = sorted_ids(6);
        let exec = FakePlanExec::new(1, 100);
        let cap = PlanExecCapability::new(exec.clone());
        let c = ctx();
        let input = json!({ "plan": plan_json_with_budget(
            ids.iter().map(|&i| node_of(i, json!({"cost": 0.6}))).collect(),
            vec![],
            usd(Some(0.5), None, BudgetAction::Pause),
        )});
        let out = cap.execute(&c, input).await.expect("execute");
        assert_eq!(out["status"], "paused_budget");
        let ok = out["ok"].as_u64().expect("ok");
        let skipped = out["skipped"].as_u64().expect("skipped");
        // Window = clamp(2×workers, 4, 64) = 4 for workers=1: the
        // tripping step plus up to 3 already-in-flight finish; the 2
        // steps still sitting in the channel buffer must NOT run
        // (pre-fix, all 6 ran and ok was 6).
        assert!(ok <= 4, "at most the in-flight window finishes, got {ok}");
        assert!(skipped >= 2, "buffered steps must NOT run, got {skipped}");
        assert_eq!(ok + skipped, 6);
        assert!(
            out["budget"]["spent_usd"].as_f64().expect("spent") <= 2.4 + 1e-9,
            "spend bounded by the window"
        );
    }

    #[tokio::test]
    async fn e09_continue_mode_failure_plus_last_step_exceed_is_done() {
        // Codex P2 on #59: a failed step (continue mode) leaves
        // done < total, but a last-step exceed parked NOTHING — the
        // status discriminator is unscheduled work, not done<total.
        let ids = sorted_ids(2);
        let (a, b) = (ids[0], ids[1]);
        let exec = FakePlanExec::new(1, 100);
        let cap = PlanExecCapability::new(exec.clone());
        let c = ctx();
        let input = json!({
            "plan": plan_json_with_budget(
                vec![
                    node_of(a, json!({"fail": true})),
                    node_of(b, json!({"cost": 2.0})),
                ],
                vec![],
                usd(Some(1.0), None, BudgetAction::Cancel),
            ),
            "on_failure": "continue",
        });
        let out = cap.execute(&c, input).await.expect("execute");
        assert_eq!(out["status"], "done", "nothing was budget-parked");
        assert_eq!(out["budget"]["triggered"], true);
        assert_eq!(out["failed"], 1);
        assert!(out["budget"].get("unscheduled").is_none());
    }
    #[tokio::test]
    async fn e10_cancelled_plan_stops_minting_at_the_next_completion() {
        // 5.10 (ADR-0038, plan review B1): the stop button. The
        // plan.execute row is cancelled from the start — after the
        // FIRST step completes, the loop must break: no further
        // submits, stranded steps Skipped, status "cancelled".
        let ids = sorted_ids(3);
        let (a, b, c_id) = (ids[0], ids[1], ids[2]);
        let exec = FakePlanExec::new(1, 100);
        exec.cancelled.store(true, Ordering::SeqCst);
        let cap = PlanExecCapability::new(exec.clone());
        let c = ctx();
        let input = json!({ "plan": plan_json(
            vec![node_of(a, json!({})), node_of(b, json!({})), node_of(c_id, json!({}))],
            vec![(b, a), (c_id, b)],
        )});
        let out = cap.execute(&c, input).await.expect("Ok aggregate");
        assert_eq!(out["status"], "cancelled");
        assert_eq!(out["ok"], 1, "only the in-flight first step finished");
        assert_eq!(out["skipped"], 2);
        assert_eq!(
            exec.submits.lock().len(),
            1,
            "no further mints after cancel"
        );
    }
    // ---------- 5.11 checkpoint store (ADR-0039) ----------

    #[tokio::test]
    async fn e11_checkpointed_steps_settle_without_dispatch() {
        // A 3-chain where the first two steps are already checkpointed:
        // only the third is minted, the aggregate reports all three ok
        // with two marked from_checkpoint, and the dependents resolved
        // against the REPLAYED outputs.
        let ids = sorted_ids(3);
        let (a, b, c_id) = (ids[0], ids[1], ids[2]);
        let plan_id = PlanId::new_v7();
        let exec = FakePlanExec::new(2, 100);

        // Seed exactly what a prior run would have recorded: node a ran
        // on {}, node b on the OutputRef-resolved value of a's output.
        let a_input = json!({});
        let a_out = json!({"echoed": a_input.clone()});
        let b_input = json!({ "from_a": a_out.clone() });
        let b_out = json!({"echoed": b_input.clone()});
        for (node, input, out) in [(a, a_input, a_out), (b, b_input.clone(), b_out.clone())] {
            let h = harness_core::input_hash(&input, HashFn::Blake3).expect("hash");
            exec.checkpoint_record(plan_id, node, &h, None, &out);
        }

        let cap = PlanExecCapability::new(exec.clone());
        let c = ctx();
        let input = json!({ "plan": checkpointed_plan_json(
            plan_id,
            vec![
                node_of(a, json!({})),
                node_of(b, json!({ "from_a": {"$task_output": a.0.to_string()} })),
                node_of(c_id, json!({ "from_b": {"$task_output": b.0.to_string()} })),
            ],
            vec![(b, a), (c_id, b)],
            harness_core::CheckpointStorage::Sqlite,
        )});
        let out = cap.execute(&c, input).await.expect("Ok aggregate");

        assert_eq!(out["ok"], 3, "every step accounted for");
        assert_eq!(out["replayed"], 2);
        assert_eq!(
            exec.submits.lock().len(),
            1,
            "only the un-checkpointed step is dispatched"
        );
        let steps = out["steps"].as_object().expect("steps");
        assert_eq!(steps[&a.0.to_string()]["from_checkpoint"], json!(true));
        assert_eq!(steps[&b.0.to_string()]["from_checkpoint"], json!(true));
        assert!(
            steps[&c_id.0.to_string()].get("from_checkpoint").is_none(),
            "a freshly executed step never claims replay"
        );
        // The dependent resolved against the REPLAYED output.
        let (_, _, minted_input, ..) = exec.submits.lock()[0].clone();
        assert_eq!(minted_input, json!({ "from_b": b_out }));
    }

    #[tokio::test]
    async fn e12_successful_steps_are_recorded_and_gc_runs_only_when_all_done() {
        // Run 1: a clean 2-chain records both steps, then GCs.
        let ids = sorted_ids(2);
        let (a, b) = (ids[0], ids[1]);
        let plan_id = PlanId::new_v7();
        let exec = FakePlanExec::new(2, 100);
        let cap = PlanExecCapability::new(exec.clone());
        let input = json!({ "plan": checkpointed_plan_json(
            plan_id,
            vec![node_of(a, json!({})), node_of(b, json!({}))],
            vec![(b, a)],
            harness_core::CheckpointStorage::Sqlite,
        )});
        let out = cap.execute(&ctx(), input).await.expect("Ok aggregate");
        assert_eq!(out["ok"], 2);
        assert_eq!(out["replayed"], 0);
        assert_eq!(exec.checkpoints.lock().len(), 2, "both steps recorded");
        assert_eq!(
            exec.gc_calls.lock().as_slice(),
            &[plan_id],
            "a fully-done plan drops its checkpoints"
        );

        // Run 2: the second step fails under fail_fast. The successful
        // prefix stays checkpointed and GC must NOT fire — that prefix
        // is exactly what a resubmission resumes from (BLOCKER-2: the
        // aggregate still says status "done" on this path).
        let ids = sorted_ids(2);
        let (a2, b2) = (ids[0], ids[1]);
        let plan2 = PlanId::new_v7();
        let exec2 = FakePlanExec::new(1, 100);
        let cap2 = PlanExecCapability::new(exec2.clone());
        let input2 = json!({ "plan": checkpointed_plan_json(
            plan2,
            vec![node_of(a2, json!({})), node_of(b2, json!({"fail": true}))],
            vec![(b2, a2)],
            harness_core::CheckpointStorage::Sqlite,
        )});
        let _ = cap2.execute(&ctx(), input2).await;
        assert_eq!(exec2.checkpoints.lock().len(), 1, "only the success");
        assert!(
            exec2.gc_calls.lock().is_empty(),
            "an incomplete plan keeps its checkpoints"
        );
    }

    #[tokio::test]
    async fn e13_replayed_steps_do_not_spend_the_budget() {
        // A plan whose steps each cost $4 with a $5 cap: run live it
        // would trip after the first. Fully checkpointed, it completes
        // — replayed work was paid for in the earlier run (ADR-0039).
        let ids = sorted_ids(2);
        let (a, b) = (ids[0], ids[1]);
        let plan_id = PlanId::new_v7();
        let exec = FakePlanExec::new(2, 100);
        let (a_in, b_in) = (json!({"cost": 4.0}), json!({"cost": 4.0, "n": 2}));
        for (node, input) in [(a, a_in.clone()), (b, b_in.clone())] {
            let h = harness_core::input_hash(&input, HashFn::Blake3).expect("hash");
            exec.checkpoint_record(plan_id, node, &h, None, &json!({"echoed": input}));
        }
        let mut plan_value = checkpointed_plan_json(
            plan_id,
            vec![node_of(a, a_in), node_of(b, b_in)],
            vec![(b, a)],
            harness_core::CheckpointStorage::Sqlite,
        );
        plan_value["budget"] = json!({
            "max_cost_usd": 5.0,
            "soft_limit_usd": null,
            "on_exceed": "cancel",
        });
        let cap = PlanExecCapability::new(exec.clone());
        let out = cap
            .execute(&ctx(), json!({ "plan": plan_value }))
            .await
            .expect("Ok aggregate");
        assert_eq!(out["status"], "done");
        assert_eq!(out["ok"], 2);
        assert_eq!(out["replayed"], 2);
        assert_eq!(out["budget"]["spent_usd"], json!(0.0));
        assert!(exec.submits.lock().is_empty(), "nothing dispatched");
    }

    #[tokio::test]
    async fn e14_unsupported_storage_runs_uncheckpointed() {
        // File storage is not implemented: the plan runs for real
        // rather than pretending, and records nothing.
        let ids = sorted_ids(1);
        let a = ids[0];
        let plan_id = PlanId::new_v7();
        let exec = FakePlanExec::new(1, 100);
        let h = harness_core::input_hash(&json!({}), HashFn::Blake3).expect("hash");
        exec.checkpoint_record(plan_id, a, &h, None, &json!("stale"));

        let cap = PlanExecCapability::new(exec.clone());
        let input = json!({ "plan": checkpointed_plan_json(
            plan_id,
            vec![node_of(a, json!({}))],
            vec![],
            harness_core::CheckpointStorage::File { path: "/tmp/cp".into() },
        )});
        let out = cap.execute(&ctx(), input).await.expect("Ok aggregate");
        assert_eq!(out["ok"], 1);
        assert_eq!(out["replayed"], 0, "the seeded checkpoint is not consulted");
        assert_eq!(exec.submits.lock().len(), 1, "the step really ran");
    }

    #[tokio::test]
    async fn e15_cancel_stops_a_fully_checkpointed_replay() {
        // Every step is checkpointed, so the completion arm never runs
        // — the stop button is honored inside the settle path or a
        // cancelled plan replays straight to "done" (plan review).
        let ids = sorted_ids(2);
        let (a, b) = (ids[0], ids[1]);
        let plan_id = PlanId::new_v7();
        let exec = FakePlanExec::new(2, 100);
        exec.cancelled.store(true, Ordering::SeqCst);
        let h = harness_core::input_hash(&json!({}), HashFn::Blake3).expect("hash");
        exec.checkpoint_record(plan_id, a, &h, None, &json!("a"));
        exec.checkpoint_record(plan_id, b, &h, None, &json!("b"));

        let cap = PlanExecCapability::new(exec.clone());
        let input = json!({ "plan": checkpointed_plan_json(
            plan_id,
            vec![node_of(a, json!({})), node_of(b, json!({}))],
            vec![(b, a)],
            harness_core::CheckpointStorage::Sqlite,
        )});
        let out = cap.execute(&ctx(), input).await.expect("Ok aggregate");
        assert_eq!(out["ok"], 1, "the replay stopped at the first boundary");
        assert_eq!(out["skipped"], 1);
        assert!(exec.gc_calls.lock().is_empty(), "not a complete plan");
    }
}
