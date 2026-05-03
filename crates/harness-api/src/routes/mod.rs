//! HTTP and WebSocket route handlers.

pub mod events;
pub mod health;
pub mod peers;
pub mod status;

use axum::{routing::get, Router};

use crate::state::ApiState;

/// Build the API sub-router. Mount under `/api/v1`.
pub fn api_router(state: ApiState) -> Router {
    Router::new()
        .route("/health", get(health::get_health))
        .route("/status", get(status::get_status))
        .route("/peers", get(peers::get_peers))
        .route("/events", get(events::ws_events))
        .with_state(state)
}
