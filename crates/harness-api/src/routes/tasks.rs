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
}

/// 4.7 admission (ADR-0029): `Some(429)` when the submitted backlog is
/// at the cap. Check-then-insert is deliberately non-transactional:
/// concurrent submits can overshoot the cap by the number of in-flight
/// requests. The cap protects against unbounded growth, not an exact
/// ceiling — an exact one would serialize every submit through a write
/// transaction. A count ERROR fails open: admission is protective, and
/// the insert below surfaces a genuinely broken store as a 500.
fn check_admission(store: &harness_store::Store) -> Option<axum::response::Response> {
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

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0);

    let mut task = Task {
        id: TaskId::new_v7(),
        parent: None,
        plan_id: None,
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
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": "sign_failed" })),
        )
            .into_response();
    }

    if let Err(err) = store.insert_task(&task) {
        tracing::error!(target: "harness.api.tasks", ?err, "insert task");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": "store_insert_failed" })),
        )
            .into_response();
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

    (
        StatusCode::CREATED,
        Json(SubmitResponse {
            task_id: format!("{}", task.id.0.as_hyphenated()),
            state: "submitted".into(),
        }),
    )
        .into_response()
}

/// `GET /api/v1/tasks?state=<state>` — list tasks from the local store.
/// Currently returns only `submitted` tasks; the full state filter set
/// lands when the dispatcher writes through more transitions.
pub async fn list_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
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
    match store.list_tasks_by_state(harness_store::TaskState::Submitted) {
        Ok(rows) => {
            let dto: Vec<TaskSummaryDto> = rows
                .into_iter()
                .map(|r| TaskSummaryDto {
                    id: format!("{}", r.id.0.as_hyphenated()),
                    capability: r.capability,
                    state: r.state.as_str().into(),
                    issued_at_ms: r.issued_at,
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
