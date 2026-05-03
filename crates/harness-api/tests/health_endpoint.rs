//! `GET /api/v1/health` integration test — exercises the actual router.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::sync::Arc;

use axum::{body::Body, http::Request};
use harness_api::{router, ApiStateBuilder};
use harness_core::Identity;
use http_body_util::BodyExt;
use tower::ServiceExt;

#[tokio::test]
async fn health_returns_ok_true() {
    let id = Arc::new(Identity::generate());
    let state = ApiStateBuilder::new(id, "test").build();
    let app = router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/health")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), 200);
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
    assert_eq!(json["ok"], serde_json::json!(true));
    assert_eq!(json["service"], serde_json::json!("harness-api"));
}

/// API route precedence: an unknown path under `/api/v1/*` must 404
/// from the API router rather than falling through to the SPA's
/// `index.html`. Without this, a typo in an API URL would silently
/// return HTML, hiding the bug from tests that only check status.
#[tokio::test]
async fn unknown_api_path_returns_404_not_spa_html() {
    let id = Arc::new(Identity::generate());
    let state = ApiStateBuilder::new(id, "test").build();
    let app = router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/this-route-does-not-exist")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(
        response.status(),
        404,
        "API misses must 404, not fall through to UI"
    );
    let ct = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_owned();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    let body = String::from_utf8_lossy(&bytes);
    assert!(
        !ct.starts_with("text/html") && !body.contains("<html"),
        "API miss leaked SPA HTML; ct={ct}, body starts with {:?}",
        &body[..body.len().min(80)]
    );
}
