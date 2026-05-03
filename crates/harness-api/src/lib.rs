//! HTTP and WebSocket API surface for the harness daemon.
//!
//! Mounted in the daemon at `127.0.0.1:19198` by default. Two surfaces:
//!
//! 1. **JSON REST** at `/api/v1/{health,status,peers}` — read-only
//!    snapshots of the local node and its peer table.
//! 2. **WebSocket** at `/api/v1/events` — live stream of [`MeshEvent`]
//!    deltas (peer added / updated / evicted, leader changed). Frames
//!    are JSON text frames, one event per frame.
//!
//! Phase 1.10 ships the read-only surface. Submit / Runs (Phase 2.6/2.9)
//! and admin endpoints land later.
//!
//! # Routing precedence
//!
//! [`router`] mounts the API as `Router::new().nest("/api/v1", api)` and
//! the embedded `SvelteKit` UI as `.fallback_service(harness_ui::router())`.
//! API routes always win because `nest` is matched before `fallback`.

#![forbid(unsafe_code)]

pub mod dto;
pub mod event;
pub mod routes;
pub mod state;

pub use dto::{LocalDto, PeerDto, PeersSnapshot, ResourcesDto, StatusDto};
pub use event::{MeshEvent, TableEventBridge};
pub use state::{ApiState, ApiStateBuilder, LocalStatus, ServerHandle};

use axum::Router;

/// Build the daemon's combined `Router`: API at `/api/v1/*`, UI at `/`.
pub fn router(state: ApiState) -> Router {
    let api = routes::api_router(state);
    Router::new()
        .nest("/api/v1", api)
        .fallback_service(harness_ui::router())
}

/// Bind on `addr`, start the axum server, return a [`ServerHandle`] the
/// caller uses for graceful shutdown. The handle owns the bound socket
/// address (useful when `addr.port() == 0`, e.g. tests).
///
/// # Errors
/// Returns the underlying `std::io::Error` if binding fails.
pub async fn serve(addr: std::net::SocketAddr, state: ApiState) -> std::io::Result<ServerHandle> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let local = listener.local_addr()?;

    let app = router(state);
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(async move {
        let server = axum::serve(listener, app).with_graceful_shutdown(async move {
            loop {
                if *shutdown_rx.borrow() {
                    break;
                }
                if shutdown_rx.changed().await.is_err() {
                    break;
                }
            }
        });
        if let Err(err) = server.await {
            tracing::error!(target: "harness.api", ?err, "axum server failed");
        }
    });

    Ok(ServerHandle::new(local, task, shutdown_tx))
}
