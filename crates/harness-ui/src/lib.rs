//! Embedded `SvelteKit` web UI assets.
//!
//! The UI is built by `npm run build` in the workspace-root `ui/`
//! directory and emitted to `ui/build/`. This crate uses `rust_embed`
//! to bundle the contents of that directory into the daemon binary at
//! compile time.
//!
//! At runtime, [`router`] returns an [`axum::Router`] that serves
//! every embedded file at its relative path, with `index.html` as the
//! fallback for any unknown path (SPA history-mode routing).
//!
//! When the UI has not been built (e.g. a Rust-only contributor running
//! `cargo build` without Node installed), every request returns a
//! `404 Not Found` with a one-line hint pointing at the build command.
//! CI sets `HARNESS_UI_REQUIRE=1` (see `build.rs`) so a missing UI
//! build is a hard error in CI — only locally is the missing-UI path
//! tolerated.
//!
//! # Phase 1.10 contract
//!
//! - `GET /`              → `index.html`.
//! - `GET /<path>`        → asset bytes if it exists, else `index.html`.
//! - Content-Type is inferred from the path via `mime_guess`.
//! - Responses are uncached (`cache-control: no-store`) for the dev
//!   loop; Phase 6 hardens with hashed-asset URLs and long-lived caches.
//!
//! Mount this router LAST on the daemon's `axum::Router`, *after*
//! the JSON API at `/api/v1/*`, using `Router::fallback_service`
//! so the API always wins on `/api/*` paths regardless of how the
//! SPA might match deep links.

use axum::{
    body::Body,
    extract::Path as AxumPath,
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "$CARGO_MANIFEST_DIR/../../ui/build/"]
struct Assets;

/// Build the UI router. Mount with `.fallback_service(harness_ui::router())`
/// after nesting the API at `/api/v1`.
pub fn router() -> Router {
    Router::new()
        .route("/", get(serve_index))
        .route("/*path", get(serve_asset))
}

async fn serve_index() -> Response {
    serve_path("index.html")
}

async fn serve_asset(AxumPath(path): AxumPath<String>) -> Response {
    serve_path(&path)
}

fn serve_path(path: &str) -> Response {
    let normalized = path.trim_start_matches('/');

    // Block path traversal at the boundary even though rust-embed
    // already only ever returns embedded paths — defense in depth.
    if normalized.contains("..") {
        return not_found();
    }

    if let Some(asset) = Assets::get(normalized) {
        return file_response(normalized, asset.data.as_ref());
    }

    // SPA fallback: any unknown path serves index.html so client-side
    // routing can render the right view.
    if let Some(asset) = Assets::get("index.html") {
        return file_response("index.html", asset.data.as_ref());
    }

    // UI not built. CI fails earlier (build.rs) so this only fires
    // for local Rust-only contributors.
    not_found()
}

fn file_response(path: &str, bytes: &[u8]) -> Response {
    let mime = mime_guess::from_path(path).first_or_octet_stream();
    let body = Body::from(bytes.to_vec());

    let mut response = (StatusCode::OK, body).into_response();
    let headers = response.headers_mut();
    if let Ok(value) = HeaderValue::from_str(mime.as_ref()) {
        headers.insert(header::CONTENT_TYPE, value);
    }
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

fn not_found() -> Response {
    let body = "harness UI assets are not bundled. \
                Run `cd ui && npm ci && npm run build` and rebuild.\n";
    let mut response = (StatusCode::NOT_FOUND, body).into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}

/// `true` when at least `index.html` is embedded in this build.
/// Useful for the daemon to emit a startup warning on dev builds.
#[must_use]
pub fn has_assets() -> bool {
    Assets::get("index.html").is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn router_compiles() {
        let _ = router();
    }

    #[test]
    fn missing_path_falls_back_to_index_or_404() {
        let response = serve_path("definitely/not/a/real/asset");
        let status = response.status();
        assert!(
            status == StatusCode::OK || status == StatusCode::NOT_FOUND,
            "unexpected status {status}"
        );
    }

    #[test]
    fn path_traversal_is_rejected() {
        let response = serve_path("../etc/passwd");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
