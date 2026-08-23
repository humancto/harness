//! `POST /api/v1/admin/pause` / `resume` — the operator half of the
//! 4.7 backpressure switch (ADR-0029; PRD §25.1/§25.2). Auth-gated
//! with the same bearer scheme as task submission; 503 when the daemon
//! didn't wire a pause switch (bare test fixtures) or when auth is
//! uninitialized (the standard ADR-0007 posture).

use axum::http::{HeaderMap, StatusCode};
use axum::{extract::State, response::IntoResponse, Json};

use crate::auth::{is_authenticated, unauthorized};
use crate::state::ApiState;

pub async fn pause_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> axum::response::Response {
    set_operator(&state, &headers, true)
}

pub async fn resume_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> axum::response::Response {
    set_operator(&state, &headers, false)
}

fn set_operator(state: &ApiState, headers: &HeaderMap, paused: bool) -> axum::response::Response {
    if !is_authenticated(&state.auth, headers) {
        return unauthorized();
    }
    let Some(pause) = state.pause.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "error": "pause_not_configured" })),
        )
            .into_response();
    };
    pause.set_operator(paused);
    tracing::info!(target: "harness.api", paused, "operator pause switch");
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "operator_paused": pause.operator_paused(),
            "paused": pause.paused(),
        })),
    )
        .into_response()
}
