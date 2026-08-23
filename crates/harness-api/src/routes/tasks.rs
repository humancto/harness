//! `POST /api/v1/tasks` and `GET /api/v1/tasks` — submit + list.

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use harness_core::{
    Constraints, ResourceHints, RetryPolicy, Signable, Signature, Task, TaskId, TraceContext,
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::auth::{is_authenticated, unauthorized};
use crate::state::ApiState;

/// 4.7 (ADR-0029): submitted-backlog admission cap. `POST /tasks` is
/// refused with `429 Too Many Requests` while at least this many rows
/// sit at `Submitted` — the `SQLite` queue is otherwise unbounded and a
/// runaway submitter grows it (and every dispatch-pass scan) without
/// limit. Sub-tasks minted internally (fanout / plans / federated) do
/// not pass through this gate; they are bounded by their windows.
pub const MAX_SUBMITTED_BACKLOG: u64 = 1024;

/// `Retry-After` seconds suggested on 429 — one dispatch-pass cadence.
const RETRY_AFTER_SECS: u64 = 2;

/// Request body for `POST /api/v1/tasks`.
#[derive(Debug, Deserialize)]
pub struct SubmitRequest {
    pub capability: String,
    #[serde(default)]
    pub input: JsonValue,
    #[serde(default)]
    pub constraints: Option<Constraints>,
    /// Optional execution policy override (3.3-fanout: `harness run`
    /// aligns `timeout_ms` so lease TTLs dominate runtime; ADR-0017).
    #[serde(default)]
    pub execution: Option<harness_core::ExecutionPolicy>,
    /// Caller hints (3.5). Optional. Honored variably by capabilities;
    /// e.g. `["interactive"]` opts out of the LLM micro-batcher.
    #[serde(default)]
    pub tags: Vec<String>,
    /// §14.9 resource hints (4.4). Optional; defaults to all-Light —
    /// the scheduler also unions in the capability manifest's hints.
    #[serde(default)]
    pub resource_hints: Option<ResourceHints>,
}

#[derive(Debug, Serialize)]
pub struct SubmitResponse {
    pub task_id: String,
    pub state: String,
}

#[derive(Debug, Serialize)]
pub struct TaskSummaryDto {
    pub id: String,
    pub capability: String,
    pub state: String,
    pub issued_at_ms: u64,
    /// 4.8: parent task id for sub-task grouping (federated/plan
    /// children). Omitted for top-level tasks.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    /// 4.8: plan-run linkage. Omitted for non-plan tasks.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan_id: Option<String>,
}

/// 4.7 admission (ADR-0029): `Some(429)` when the submitted backlog is
/// at the cap. Check-then-insert is deliberately non-transactional:
/// concurrent submits can overshoot the cap by the number of in-flight
/// requests. The cap protects against unbounded growth, not an exact
/// ceiling — an exact one would serialize every submit through a write
/// transaction. A count ERROR fails open: admission is protective, and
/// the insert below surfaces a genuinely broken store as a 500.
pub(crate) fn check_admission(store: &harness_store::Store) -> Option<axum::response::Response> {
    match store.count_tasks_by_state(harness_store::TaskState::Submitted) {
        Ok(backlog) if backlog >= MAX_SUBMITTED_BACKLOG => Some(
            (
                StatusCode::TOO_MANY_REQUESTS,
                [(
                    axum::http::header::RETRY_AFTER,
                    RETRY_AFTER_SECS.to_string(),
                )],
                Json(serde_json::json!({
                    "error": "submitted_backlog_full",
                    "submitted_backlog": backlog,
                    "max_submitted_backlog": MAX_SUBMITTED_BACKLOG,
                })),
            )
                .into_response(),
        ),
        Ok(_) => None,
        Err(err) => {
            tracing::warn!(target: "harness.api.tasks", ?err, "backlog count failed; admitting");
            None
        }
    }
}

/// 5.5 (ADR-0033): THE task-minting path — clamp, build, sign, insert,
/// replica-mirror. Shared by the authenticated `submit_handler` and the
/// signature-validated webhook adapters; never duplicated (the easy
/// mistakes are forgetting the clamp or `replica_apply_local`).
///
/// Callers gate BEFORE minting (auth + admission for the HTTP path;
/// Twilio signature + allowlist + admission for webhooks).
/// 5.10 (ADR-0038, plan review B2): a `plan.execute` submission's
/// task row carries the PLAN's id, parsed from `input.plan.id` — the
/// Costs page joins active plan rows to ledger rows on it. Non-plan
/// submissions (and unparsable plans, which fail validation later
/// anyway) stay `None`.
fn plan_exec_plan_id(req: &SubmitRequest) -> Option<harness_core::PlanId> {
    if req.capability != "plan.execute" {
        return None;
    }
    req.input
        .get("plan")
        .and_then(|p| p.get("id"))
        .and_then(serde_json::Value::as_str)
        .and_then(|s| uuid::Uuid::parse_str(s).ok())
        .map(harness_core::PlanId)
}

pub(crate) fn mint_task(
    state: &ApiState,
    store: &harness_store::Store,
    req: SubmitRequest,
) -> Result<TaskId, &'static str> {
    // Unguarded: `PlanAlreadyRunning` cannot occur, and carrying the
    // id out is harmless if it somehow did.
    mint_task_guarded(state, store, req, None).map(|outcome| match outcome {
        MintOutcome::Minted(id) | MintOutcome::PlanAlreadyRunning(id) => id,
    })
}

/// What [`mint_task_guarded`] did.
pub(crate) enum MintOutcome {
    Minted(TaskId),
    /// 5.12: refused — a non-terminal run of this plan already exists.
    PlanAlreadyRunning(TaskId),
}

/// The mint path, optionally guarded on "no live run of this plan"
/// (5.12, Codex P1 on #63). The guard shares ONE transaction with the
/// insert: checking first and inserting after lets two concurrent
/// resumes both mint a coordinator and run the DAG twice.
pub(crate) fn mint_task_guarded(
    state: &ApiState,
    store: &harness_store::Store,
    req: SubmitRequest,
    guard_plan: Option<harness_core::PlanId>,
) -> Result<MintOutcome, &'static str> {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0);

    let mut task = Task {
        id: TaskId::new_v7(),
        parent: None,
        plan_id: plan_exec_plan_id(&req),
        capability: req.capability,
        input: req.input,
        constraints: req.constraints.unwrap_or_default(),
        retry: RetryPolicy::default(),
        // 4.4 (risk 12): clamp caller-supplied policy into sane bounds
        // BEFORE signing — a u32::MAX timeout would otherwise mint a
        // ~49-day lease (ADR-0026).
        execution: {
            let requested = req.execution.unwrap_or_default();
            let clamped = requested.clamped();
            if clamped != requested {
                tracing::warn!(
                    target: "harness.api.tasks",
                    requested_timeout = requested.timeout_ms,
                    requested_lease = requested.lease_ms,
                    "execution policy clamped at submit"
                );
            }
            clamped
        },
        resource_hints: req.resource_hints.unwrap_or(ResourceHints {
            cpu_class: harness_core::protocol::CpuClass::Light,
            memory_mb: None,
            gpu_required: false,
            gpu_memory_mb: None,
            network_class: harness_core::protocol::NetworkClass::None,
            disk_io_class: harness_core::protocol::DiskIoClass::None,
            estimated_duration_ms: None,
        }),
        trace_ctx: TraceContext::default(),
        issued_by: state.local_node_id,
        issued_at: now_ms,
        tags: req.tags,
        sig: Signature::from_bytes([0u8; 64]),
    };

    if let Err(err) = task.sign(&state.identity) {
        tracing::error!(target: "harness.api.tasks", ?err, "sign task");
        return Err("sign_failed");
    }

    match guard_plan {
        Some(plan_id) => match store.insert_task_unless_plan_live(&task, plan_id) {
            Ok(Some(live)) => return Ok(MintOutcome::PlanAlreadyRunning(live)),
            Ok(None) => {}
            Err(err) => {
                tracing::error!(target: "harness.api.tasks", ?err, "insert task (guarded)");
                return Err("store_insert_failed");
            }
        },
        None => {
            if let Err(err) = store.insert_task(&task) {
                tracing::error!(target: "harness.api.tasks", ?err, "insert task");
                return Err("store_insert_failed");
            }
        }
    }

    // Mirror the initial state into the replica map so any peer
    // querying `/tasks` sees the new submission immediately once 2.7's
    // gossip transport wires up.
    let initial = harness_core::ReplicatedTaskState {
        task_id: task.id,
        state: harness_core::ReplicatedState::Submitted,
        at_ms: now_ms,
        source: state.local_node_id,
        output_preview: None,
    };
    if let Err(err) = store.replica_apply_local(&initial) {
        tracing::warn!(target: "harness.api.tasks", ?err, "replica_apply_local");
    }

    Ok(MintOutcome::Minted(task.id))
}

/// `POST /api/v1/tasks` — sign with the local Identity, persist via Store,
/// return the new task id. The actual dispatch (assigning to a worker
/// across QUIC) lands when the QUIC envelope channels wire in a follow-up.
pub async fn submit_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(req): Json<SubmitRequest>,
) -> axum::response::Response {
    if !is_authenticated(&state.auth, &headers) {
        return unauthorized();
    }
    let Some(store) = state.store.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "error": "store_not_configured" })),
        )
            .into_response();
    };

    if let Some(refusal) = check_admission(store) {
        return refusal;
    }

    match mint_task(&state, store, req) {
        Ok(id) => (
            StatusCode::CREATED,
            Json(SubmitResponse {
                task_id: format!("{}", id.0.as_hyphenated()),
                state: "submitted".into(),
            }),
        )
            .into_response(),
        Err(code) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": code })),
        )
            .into_response(),
    }
}

/// Query surface of `GET /api/v1/tasks` (4.8).
#[derive(Debug, Deserialize)]
pub struct ListQuery {
    /// Exact per-state filter (the pre-4.8 Submitted-only view is
    /// `?state=submitted`). Unknown states → 400.
    pub state: Option<String>,
    /// Exact capability filter (5.10 — the Costs page fetches
    /// `?capability=plan.execute` so a wide plan's step rows cannot
    /// push its own coordinator row out of the page). Composes with
    /// `state`. Unknown capabilities simply match zero rows.
    pub capability: Option<String>,
    /// Row cap for the default recent-across-states listing
    /// (default 50, clamped to [1, 200]).
    pub limit: Option<usize>,
}

/// Default limit for the recent listing.
const LIST_DEFAULT_LIMIT: usize = 50;
/// Hard cap — one indexed page, never a full-table dump.
const LIST_MAX_LIMIT: usize = 200;

/// `GET /api/v1/tasks` — recent tasks across ALL states, newest first
/// (4.8; the pre-4.8 behavior returned Submitted rows only — that view
/// is `?state=submitted`). `parent`/`plan_id` ride along for sub-task
/// grouping in the UI.
pub async fn list_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
    axum::extract::Query(query): axum::extract::Query<ListQuery>,
) -> axum::response::Response {
    if !is_authenticated(&state.auth, &headers) {
        return unauthorized();
    }
    let Some(store) = state.store.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "error": "store_not_configured" })),
        )
            .into_response();
    };
    let limit = query
        .limit
        .unwrap_or(LIST_DEFAULT_LIMIT)
        .clamp(1, LIST_MAX_LIMIT);
    let wanted_state = match &query.state {
        Some(wanted) => match wanted.parse::<harness_store::TaskState>() {
            Ok(task_state) => Some(task_state),
            Err(_) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({ "error": "unknown_state" })),
                )
                    .into_response();
            }
        },
        None => None,
    };
    // The clamp applies to EVERY arm (diff review MINOR-6): terminal
    // states accumulate forever, so filtered views must page exactly
    // like the default listing.
    let rows = if let Some(capability) = &query.capability {
        store.list_recent_tasks_by_capability(capability, wanted_state, limit)
    } else if let Some(task_state) = wanted_state {
        store.list_tasks_by_state_limited(task_state, limit)
    } else {
        store.list_recent_tasks(limit)
    };
    match rows {
        Ok(rows) => {
            let dto: Vec<TaskSummaryDto> = rows
                .into_iter()
                .map(|r| TaskSummaryDto {
                    id: format!("{}", r.id.0.as_hyphenated()),
                    capability: r.capability,
                    state: r.state.as_str().into(),
                    issued_at_ms: r.issued_at,
                    parent: r.parent.map(|p| format!("{}", p.0.as_hyphenated())),
                    plan_id: r.plan_id.map(|p| format!("{}", p.0.as_hyphenated())),
                })
                .collect();
            (StatusCode::OK, Json(dto)).into_response()
        }
        Err(err) => {
            tracing::error!(target: "harness.api.tasks", ?err, "list");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "list_failed" })),
            )
                .into_response()
        }
    }
}

/// `GET /api/v1/tasks/{id}` — full envelope + state + output (if Done)
/// or error (if Failed). The CLI's `harness run` polls this until terminal.
pub async fn get_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id_str): Path<String>,
) -> axum::response::Response {
    if !is_authenticated(&state.auth, &headers) {
        return unauthorized();
    }
    let Some(store) = state.store.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "error": "store_not_configured" })),
        )
            .into_response();
    };

    let Ok(task_id) = parse_task_id(&id_str) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "invalid_task_id" })),
        )
            .into_response();
    };

    let task = match store.load_task(task_id) {
        Ok(Some(t)) => t,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": "not_found" })),
            )
                .into_response();
        }
        Err(err) => {
            tracing::error!(target: "harness.api.tasks", ?err, "load_task");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "load_failed" })),
            )
                .into_response();
        }
    };

    let task_state = match store.task_state(task_id) {
        Ok(Some(s)) => s,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": "not_found" })),
            )
                .into_response();
        }
        Err(err) => {
            tracing::error!(target: "harness.api.tasks", ?err, "task_state");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "state_failed" })),
            )
                .into_response();
        }
    };

    let result = match store.load_task_result(task_id) {
        Ok(r) => r,
        Err(err) => {
            tracing::error!(target: "harness.api.tasks", ?err, "load_task_result");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "result_failed" })),
            )
                .into_response();
        }
    };

    // Build the response — output and error are OMITTED (not null) when
    // the task isn't terminal, so consumers' type narrowing stays tight.
    let mut body = serde_json::json!({
        "id":           format!("{}", task.id.0.as_hyphenated()),
        "capability":   task.capability,
        "state":        task_state.as_str(),
        "input":        task.input,
        "issued_at_ms": task.issued_at,
    });
    // 3.2-stream (ADR-0020): buffered streaming line frames, oldest
    // first, from the in-memory ring (last ~500 per task). OMITTED when
    // the task streamed nothing — non-streaming capabilities keep their
    // response shape unchanged. Best-effort progress only; `output` /
    // `error` from the terminal result stay authoritative.
    let frames = state.partials.frames(task_id);
    if !frames.is_empty() {
        body["partials"] = serde_json::json!(frames);
    }
    // 4.7 (ADR-0029): additive lossiness flag — frames lost to ring
    // eviction locally plus the worker's wire-reported queue drops.
    // OMITTED when nothing was lost (the common case).
    let partials_dropped = state.partials.dropped(task_id);
    if partials_dropped > 0 {
        body["partials_dropped"] = serde_json::json!(partials_dropped);
    }
    if let Some(r) = result {
        body["completed_at_ms"] = serde_json::json!(r.completed_at_ms);
        if let Some(out) = r.output {
            body["output"] = out;
        }
        if let Some(err) = r.error {
            body["error"] = serde_json::json!(err);
        }
        // 4.5 (ADR-0027): federated results carry per-node provenance
        // — [{node_id, status, duration_ms, item_count}]. OMITTED for
        // single-node results (additive JSON, 4.8 UI contract).
        if let Some(provenance) = r.provenance {
            body["provenance"] = serde_json::json!(provenance_rows(&provenance));
        }
    }
    (StatusCode::OK, Json(body)).into_response()
}

fn provenance_rows(provenance: &[harness_core::NodeContribution]) -> Vec<serde_json::Value> {
    provenance
        .iter()
        .map(|c| {
            serde_json::json!({
                "node_id": c.node_id.to_string(),
                "status": match c.status {
                    harness_core::protocol::NodeStatus::Ok => "ok",
                    harness_core::protocol::NodeStatus::Failed => "failed",
                    harness_core::protocol::NodeStatus::TimedOut => "timed_out",
                    harness_core::protocol::NodeStatus::Skipped => "skipped",
                    _ => "unknown",
                },
                "duration_ms": c.duration_ms,
                "item_count": c.item_count,
            })
        })
        .collect()
}

fn parse_task_id(s: &str) -> Result<TaskId, ()> {
    uuid::Uuid::parse_str(s).map(TaskId).map_err(|_| ())
}

/// `POST /api/v1/tasks/{id}/cancel` — the §17.8 stop button (5.10,
/// ADR-0038). Marks a non-terminal row `cancelled`, releases its live
/// leases (late worker results drop at the terminal-lease guard), and
/// mirrors the state to replicas. Record-level stop: an
/// already-executing capability future is not interrupted — but the
/// plan.execute loop checks its own state per completion and stops
/// minting steps, and the executor's terminal writes lose their CAS
/// and skip.
pub async fn cancel_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id_str): Path<String>,
) -> axum::response::Response {
    if !is_authenticated(&state.auth, &headers) {
        return unauthorized();
    }
    let Some(store) = state.store.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "error": "store_not_configured" })),
        )
            .into_response();
    };
    let Ok(task_id) = parse_task_id(&id_str) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "bad_task_id" })),
        )
            .into_response();
    };
    match store.cancel_task(task_id) {
        // `cancel_task` releases the row's live leases in the SAME
        // transaction as the state flip (diff review M1) — no window
        // for an in-between result ingest.
        Ok(harness_store::CancelOutcome::Cancelled) => {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
                .unwrap_or(0);
            let mirror = harness_core::ReplicatedTaskState {
                task_id,
                state: harness_core::ReplicatedState::Cancelled,
                at_ms: now_ms,
                source: state.local_node_id,
                output_preview: None,
            };
            if let Err(err) = store.replica_apply_local(&mirror) {
                tracing::warn!(target: "harness.api.tasks", ?err, "replica_apply_local (cancel)");
            }
            (
                StatusCode::OK,
                Json(serde_json::json!({ "state": "cancelled" })),
            )
                .into_response()
        }
        Ok(harness_store::CancelOutcome::Unknown) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "unknown_task" })),
        )
            .into_response(),
        Ok(harness_store::CancelOutcome::AlreadyTerminal(s)) => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": "already_terminal", "state": s.as_str() })),
        )
            .into_response(),
        Err(err) => {
            tracing::warn!(target: "harness.api.tasks", ?err, "cancel_task");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "cancel_failed" })),
            )
                .into_response()
        }
    }
}

/// Body of `POST /api/v1/tasks/{id}/resume` (5.12). All fields
/// optional: a bare resume re-runs the plan under its original cap.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResumeRequest {
    /// Raise (or lower) the plan's cap for the resumed run. Still
    /// clamped by `[execution] plan_budget_ceiling_usd` at execute
    /// time — the response reports what actually applies.
    #[serde(default)]
    pub max_cost_usd: Option<f64>,
    /// Resume even when steps were in flight when the plan stopped.
    /// Those steps were dispatched and their outcome was never
    /// recorded, so resuming may run them a second time.
    #[serde(default)]
    pub allow_in_flight: bool,
}

/// The unfinished-step lists a stopped plan recorded (5.12).
struct ResumePoint {
    /// The original submission, so a resume keeps the caller's
    /// timeout, failure mode, execution policy, constraints and tags
    /// (diff review MAJOR-2) instead of silently falling back to
    /// defaults that fail-fast at 30s.
    original: Box<Task>,
    plan: JsonValue,
    plan_id: harness_core::PlanId,
    /// The plan ran to completion with nothing left: resuming it would
    /// re-execute every step and its side effects (Codex P2 on #63).
    complete: bool,
    unscheduled: Vec<String>,
    in_flight: Vec<String>,
}

/// Read a terminal `plan.execute` row's aggregate and the plan it ran.
fn resume_point(
    store: &harness_store::Store,
    task_id: TaskId,
) -> Result<ResumePoint, (StatusCode, &'static str)> {
    let task = match store.load_task(task_id) {
        Ok(Some(t)) => t,
        Ok(None) => return Err((StatusCode::NOT_FOUND, "unknown_task")),
        Err(err) => {
            tracing::error!(target: "harness.api.tasks", ?err, "load task (resume)");
            return Err((StatusCode::INTERNAL_SERVER_ERROR, "load_failed"));
        }
    };
    if task.capability != "plan.execute" {
        return Err((StatusCode::CONFLICT, "not_a_plan"));
    }
    let plan = task.input.get("plan").cloned().unwrap_or(JsonValue::Null);
    if !plan.is_object() {
        return Err((StatusCode::CONFLICT, "plan_missing"));
    }
    let aggregate = match store.load_task_result(task_id) {
        Ok(Some(row)) => row.output.unwrap_or(JsonValue::Null),
        // No result row: the plan never finished (a crash, or the boot
        // sweep failed it). Everything the checkpoints do not cover
        // re-runs, and there is no list to warn about.
        Ok(None) => JsonValue::Null,
        Err(err) => {
            tracing::error!(target: "harness.api.tasks", ?err, "load result (resume)");
            return Err((StatusCode::INTERNAL_SERVER_ERROR, "load_failed"));
        }
    };
    // The aggregate is only ONE source, and not the reliable one
    // (diff review BLOCKER-1): `drive_plan` returns Err — so the
    // executor persists an error string and NO aggregate — on exactly
    // the paths that strand dispatched steps: fail-fast abort,
    // deadline expiry, "no step succeeded", and a coordinator crash
    // (no result row at all). The authoritative in-flight source is
    // the plan's own STEP ROWS: a row exists iff the step was really
    // dispatched, and a non-terminal one never settled.
    let ids = |key: &str| -> Vec<String> {
        aggregate
            .get("resume")
            .and_then(|r| r.get(key))
            .and_then(JsonValue::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(ToString::to_string))
                    .collect()
            })
            .unwrap_or_default()
    };
    let Some(plan_id) = plan
        .get("id")
        .and_then(JsonValue::as_str)
        .and_then(|s| uuid::Uuid::parse_str(s).ok())
        .map(harness_core::PlanId)
    else {
        return Err((StatusCode::CONFLICT, "plan_missing"));
    };
    // The SAME predicate the checkpoint sweep uses (diff review
    // MINOR-7): the endpoint refuses to resume exactly the plans whose
    // checkpoints the sweep deletes, so the two must not drift.
    let complete = harness_store::aggregate_is_complete(&aggregate);
    let mut in_flight = ids("in_flight");
    match store.list_tasks_by_plan(plan_id) {
        Ok(rows) => {
            for (row_id, capability, state, _) in rows {
                if capability == "plan.execute" {
                    continue;
                }
                if matches!(
                    state,
                    harness_store::TaskState::Done
                        | harness_store::TaskState::Failed
                        | harness_store::TaskState::Cancelled
                        | harness_store::TaskState::Expired
                ) {
                    continue;
                }
                let id = format!("{}", row_id.0.as_hyphenated());
                if !in_flight.contains(&id) {
                    in_flight.push(id);
                }
            }
        }
        Err(err) => {
            tracing::error!(target: "harness.api.tasks", ?err, "list step rows (resume)");
            return Err((StatusCode::INTERNAL_SERVER_ERROR, "load_failed"));
        }
    }

    Ok(ResumePoint {
        original: Box::new(task),
        plan,
        plan_id,
        complete,
        unscheduled: ids("unscheduled"),
        in_flight,
    })
}

/// Mint the resumed run. Everything but the plan comes from the
/// ORIGINAL submission (diff review MAJOR-2): `input.timeout_ms` /
/// `on_failure`, the execution policy, constraints and tags all shape
/// how a plan runs, and rebuilding a bare request silently downgrades
/// a 10-minute keep-going run into a 2-minute fail-fast one.
fn mint_resumed_plan(
    state: &ApiState,
    store: &harness_store::Store,
    point: &ResumePoint,
    plan_value: JsonValue,
    replayable: usize,
) -> axum::response::Response {
    let mut resumed_input = point.original.input.clone();
    resumed_input["plan"] = plan_value;
    let mut tags = point.original.tags.clone();
    if !tags.iter().any(|t| t == "resume") {
        tags.push("resume".to_string());
    }
    let mint = mint_task_guarded(
        state,
        store,
        SubmitRequest {
            capability: "plan.execute".to_string(),
            input: resumed_input,
            constraints: Some(point.original.constraints.clone()),
            execution: Some(point.original.execution),
            tags,
            resource_hints: Some(point.original.resource_hints.clone()),
        },
        // 5.12: the live-run check shares the insert's transaction.
        Some(point.plan_id),
    );
    match mint {
        Ok(MintOutcome::PlanAlreadyRunning(live_id)) => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "error": "already_running",
                "task_id": format!("{}", live_id.0.as_hyphenated()),
            })),
        )
            .into_response(),
        Ok(MintOutcome::Minted(new_id)) => (
            StatusCode::CREATED,
            Json(serde_json::json!({
                "task_id": format!("{}", new_id.0.as_hyphenated()),
                "plan_id": format!("{}", point.plan_id.0.as_hyphenated()),
                // Checkpoints live on the node that RAN the plan, and a
                // resumed plan.execute is placed by the scheduler
                // (Cardinality::Anyone, no pin) — so this counts what
                // THIS node holds, which may not be where the resumed
                // run lands (diff review MINOR-4).
                "replayable_local": replayable,
                "unscheduled": point.unscheduled,
                "in_flight": point.in_flight,
            })),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": err })),
        )
            .into_response(),
    }
}

/// A resume body: absent means "resume as-is", malformed is a 400.
///
/// `Option<Json<T>>` cannot express that — in axum 0.7 it turns EVERY
/// rejection into `None` (diff review MAJOR-1), so a typo'd field
/// under `deny_unknown_fields` would silently resume at the old cap
/// and report success.
fn resume_body(
    body: Result<Json<ResumeRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<ResumeRequest, axum::response::Response> {
    match body {
        Ok(Json(req)) => Ok(req),
        Err(axum::extract::rejection::JsonRejection::MissingJsonContentType(_)) => {
            Ok(ResumeRequest::default())
        }
        Err(rejection) => Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "bad_request_body",
                "detail": rejection.body_text(),
            })),
        )
            .into_response()),
    }
}

/// The reasons a resume is refused before anything is minted (5.12).
fn resume_refusal(point: &ResumePoint, req: &ResumeRequest) -> Option<axum::response::Response> {
    if point.complete {
        return Some(
            (
                StatusCode::CONFLICT,
                Json(serde_json::json!({
                    "error": "nothing_to_resume",
                    "hint": "this plan finished every step; resuming would re-run all of them",
                })),
            )
                .into_response(),
        );
    }
    if !point.in_flight.is_empty() && !req.allow_in_flight {
        return Some(
            (
                StatusCode::CONFLICT,
                Json(serde_json::json!({
                    "error": "steps_in_flight",
                    "in_flight": point.in_flight,
                    "hint": "these steps were dispatched and never settled; \
                             resuming may run them twice — pass allow_in_flight",
                })),
            )
                .into_response(),
        );
    }
    None
}

/// The id of a non-terminal `plan.execute` row for this plan, if one
/// exists (5.12) — resuming while a run is live would put two
/// coordinators on one plan, both minting steps and side effects.
fn live_plan_run(
    store: &harness_store::Store,
    plan_id: harness_core::PlanId,
) -> Result<Option<TaskId>, (StatusCode, &'static str)> {
    // `list_tasks_by_plan` yields (id, capability, state, parent).
    match store.list_tasks_by_plan(plan_id) {
        Ok(rows) => Ok(rows
            .iter()
            .find(|(_, capability, state, _)| {
                capability == "plan.execute"
                    && !matches!(
                        state,
                        harness_store::TaskState::Done
                            | harness_store::TaskState::Failed
                            | harness_store::TaskState::Cancelled
                            | harness_store::TaskState::Expired
                    )
            })
            .map(|(id, ..)| *id)),
        Err(err) => {
            tracing::error!(target: "harness.api.tasks", ?err, "list plan rows (resume)");
            Err((StatusCode::INTERNAL_SERVER_ERROR, "load_failed"))
        }
    }
}

/// Replace a plan's cap for a resumed run, re-signing it with the
/// local identity (5.12). The plan's signature is not verified at
/// execute time, but a mutated plan should not carry a stale one.
/// The raise is still clamped by `[execution] plan_budget_ceiling_usd`
/// when the plan runs.
fn raise_plan_cap(
    state: &ApiState,
    mut plan_value: JsonValue,
    cap: f64,
) -> Result<JsonValue, (StatusCode, &'static str)> {
    if !cap.is_finite() || cap < 0.0 {
        return Err((StatusCode::BAD_REQUEST, "bad_max_cost_usd"));
    }
    plan_value["budget"] = serde_json::json!({
        "max_cost_usd": cap,
        "soft_limit_usd": plan_value
            .get("budget")
            .and_then(|b| b.get("soft_limit_usd"))
            .cloned()
            .unwrap_or(JsonValue::Null),
        "on_exceed": plan_value
            .get("budget")
            .and_then(|b| b.get("on_exceed"))
            .cloned()
            .unwrap_or_else(|| serde_json::json!("cancel")),
    });
    match serde_json::from_value::<harness_core::Plan>(plan_value.clone()) {
        Ok(mut plan) => {
            // Stamp the signer too, or the artifact verifies against
            // nobody (diff review MINOR-3).
            plan.issued_by = state.local_node_id;
            if let Err(err) = plan.sign(&state.identity) {
                tracing::error!(target: "harness.api.tasks", ?err, "re-sign plan");
            }
            Ok(serde_json::to_value(&plan).unwrap_or(plan_value))
        }
        Err(err) => {
            tracing::warn!(target: "harness.api.tasks", %err, "resume plan re-parse");
            Err((StatusCode::CONFLICT, "plan_malformed"))
        }
    }
}

/// `POST /api/v1/tasks/{id}/resume` — re-run a stopped plan, replaying
/// every step whose checkpoint still stands (5.12, ADR-0040).
///
/// Mints a NEW `plan.execute` row carrying the SAME plan id, which is
/// what makes 5.11's checkpoints hit. It cannot re-dispatch the old
/// row: a plan.execute that ran locally holds no lease, and the boot
/// orphan sweep marks a crashed one `Failed` — a terminal state with
/// no outgoing transition.
pub async fn resume_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id_str): Path<String>,
    body: Result<Json<ResumeRequest>, axum::extract::rejection::JsonRejection>,
) -> axum::response::Response {
    if !is_authenticated(&state.auth, &headers) {
        return unauthorized();
    }
    let Some(store) = state.store.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "error": "store_not_configured" })),
        )
            .into_response();
    };
    let Ok(task_id) = parse_task_id(&id_str) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "bad_task_id" })),
        )
            .into_response();
    };
    // `Option<Json<T>>` turns EVERY rejection into None (axum 0.7), so
    // a typo'd or malformed body would silently resume at the old cap
    // and report success (diff review MAJOR-1). Only a genuinely
    // absent body defaults.
    let req = match resume_body(body) {
        Ok(r) => r,
        Err(refusal) => return refusal,
    };

    // 4.7 (ADR-0029): a resume is an authenticated top-level mint, not
    // a window-bounded internal sub-task — the backlog cap applies
    // (diff review MINOR-1).
    if let Some(refusal) = check_admission(store) {
        return refusal;
    }

    let point = match resume_point(store, task_id) {
        Ok(p) => p,
        Err((code, err)) => {
            return (code, Json(serde_json::json!({ "error": err }))).into_response()
        }
    };
    let plan_id = point.plan_id;

    // Fast path only: this answers before doing any work, but it is
    // NOT the guarantee — two concurrent resumes can both pass it.
    // The authority is the guarded insert below, which checks and
    // inserts in one transaction (Codex P1 on #63).
    match live_plan_run(store, plan_id) {
        Ok(Some(live_id)) => {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({
                    "error": "already_running",
                    "task_id": format!("{}", live_id.0.as_hyphenated()),
                })),
            )
                .into_response()
        }
        Ok(None) => {}
        Err((code, err)) => {
            return (code, Json(serde_json::json!({ "error": err }))).into_response()
        }
    }

    if let Some(refusal) = resume_refusal(&point, &req) {
        return refusal;
    }

    let mut plan_value = point.plan.clone();
    if let Some(cap) = req.max_cost_usd {
        match raise_plan_cap(&state, plan_value, cap) {
            Ok(v) => plan_value = v,
            Err((code, err)) => {
                return (code, Json(serde_json::json!({ "error": err }))).into_response()
            }
        }
    }

    let replayable = store.checkpoint_count(plan_id).unwrap_or(0);
    mint_resumed_plan(&state, store, &point, plan_value, replayable)
}
