//! HTTP and WebSocket route handlers.

pub mod admin;
pub mod audit;
pub mod capabilities;
pub mod costs;
pub mod events;
pub mod health;
pub mod peers;
pub mod runs;
pub mod status;
pub mod tasks;
pub mod webhook;

use axum::{
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde_json::json;

use crate::auth;
use crate::state::ApiState;

/// Build the API sub-router. Mount under `/api/v1`.
///
/// The sub-router carries its own `fallback` so unknown paths under
/// `/api/v1/*` always return a JSON `{ "error": "not_found" }` instead
/// of falling through to the outer router's UI fallback.
pub fn api_router(state: ApiState) -> Router {
    Router::new()
        .route("/health", get(health::get_health))
        .route("/status", get(status::get_status))
        .route("/peers", get(peers::get_peers))
        .route("/capabilities", get(capabilities::get_capabilities))
        .route("/costs", get(costs::get_costs))
        .route("/audit", get(audit::list_handler))
        .route("/events", get(events::ws_events))
        .route("/admin/pause", post(admin::pause_handler))
        .route("/admin/resume", post(admin::resume_handler))
        .route("/auth/login", post(auth::login_handler))
        .route("/auth/logout", post(auth::logout_handler))
        .route(
            "/tasks",
            post(tasks::submit_handler).get(tasks::list_handler),
        )
        // axum 0.7 / matchit 0.7 path params use `:id` — the earlier
        // `{id}` spelling registered a LITERAL "{id}" segment (axum 0.8
        // syntax), so `GET /api/v1/tasks/<uuid>` always fell through to
        // the 404 fallback. Found independently by 3.3-gossip and
        // 3.2-stream; regression-tested in runs_websocket.rs and the
        // partials API test.
        .route("/tasks/:id", get(tasks::get_handler))
        .route("/tasks/:id/cancel", post(tasks::cancel_handler))
        .route("/tasks/:id/resume", post(tasks::resume_handler))
        .route("/runs/:id", get(runs::ws_run))
        .fallback(api_not_found)
        .with_state(state)
}

async fn api_not_found() -> impl IntoResponse {
    (
        StatusCode::NOT_FOUND,
        Json(json!({ "error": "not_found", "scope": "api" })),
    )
}
