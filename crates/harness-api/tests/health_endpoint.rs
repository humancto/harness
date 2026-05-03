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
