//! `POST /api/v1/tasks` and `GET /api/v1/tasks` — submit + list.

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use harness_core::{
    Constraints, ExecutionPolicy, ResourceHints, RetryPolicy, Signable, Signature, Task, TaskId,
    TraceContext,
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::auth::{is_authenticated, unauthorized};
use crate::state::ApiState;

/// Request body for `POST /api/v1/tasks`.
#[derive(Debug, Deserialize)]
pub struct SubmitRequest {
    pub capability: String,
    #[serde(default)]
    pub input: JsonValue,
    #[serde(default)]
    pub constraints: Option<Constraints>,
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
        issued_by: state.local_node_id,
        issued_at: now_ms,
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
