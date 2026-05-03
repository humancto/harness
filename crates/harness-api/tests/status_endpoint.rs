//! `GET /api/v1/status` integration test — shape + values.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::sync::Arc;

use axum::{body::Body, http::Request};
use harness_api::{router, ApiStateBuilder, StatusDto};
use harness_core::Identity;
use http_body_util::BodyExt;
use tower::ServiceExt;

#[tokio::test]
async fn status_endpoint_returns_local_node_detail() {
    let id = Arc::new(Identity::generate());
    let expected_node_id = format!("{}", id.node_id());
    let expected_fp = id.public_key().fingerprint_hex();
    let state = ApiStateBuilder::new(id, "lan-status").build();
    let app = router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/status")
                .body(Body::empty())
                .expect("req"),
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
    let dto: StatusDto = serde_json::from_slice(&bytes).expect("status json");

    assert_eq!(dto.node_id, expected_node_id);
    assert_eq!(dto.pubkey_fp, expected_fp);
    assert_eq!(dto.mesh_name, "lan-status");
    assert_eq!(dto.brain_score, 0);
    assert_eq!(dto.leader_belief, None);
    assert!(!dto.ui_version.is_empty());
}
