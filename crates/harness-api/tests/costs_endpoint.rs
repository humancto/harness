//! 5.9: `GET /api/v1/costs` — auth-gated cost ledger (ADR-0037).
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::sync::Arc;

use axum::{
    body::Body,
    http::{header, Request, StatusCode},
};
use harness_api::{router, ApiStateBuilder, AuthProvider};
use harness_core::{
    Constraints, ExecutionPolicy, Identity, NodeId, PlanId, ResourceHints, RetryPolicy, Signable,
    Signature, Task, TaskId, TraceContext,
};
use harness_mesh::AdminFile;
use harness_store::Store;
use http_body_util::BodyExt;
use tower::ServiceExt;

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn costed_task(identity: &Identity, store: &Store, plan_id: Option<PlanId>, usd: f64) -> TaskId {
    let mut t = Task {
        id: TaskId::new_v7(),
        parent: None,
        plan_id,
        capability: "llm.cloud.claude".into(),
        input: serde_json::json!({}),
        constraints: Constraints::default(),
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
        issued_by: identity.node_id(),
        issued_at: 1,
        tags: vec![],
        sig: Signature::from_bytes([0u8; 64]),
    };
    t.sign(identity).expect("sign");
    store.insert_task(&t).expect("insert");
    store
        .write_task_result_done(
            t.id,
            &serde_json::json!({}),
            now_ms(),
            NodeId::from_bytes([2; 16]),
        )
        .expect("row");
    store.write_result_cost(t.id, usd).expect("cost");
    t.id
}

#[tokio::test]
async fn costs_endpoint_is_auth_gated_and_folds_totals() {
    let identity = Identity::generate();
    let store = Store::open_memory().expect("store");
    let plan = PlanId::new_v7();
    costed_task(&identity, &store, Some(plan), 1.5);
    costed_task(&identity, &store, Some(plan), 0.5);

    let admin = AdminFile::from_password("hunter2").expect("hash");
    let state = ApiStateBuilder::new(Arc::new(Identity::generate()), "test")
        .with_auth(Arc::new(AuthProvider::new(Some(admin))))
        .with_store(store)
        .build();
    let app = router(state);

    // Unauthenticated → 401.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/costs")
                .body(Body::empty())
                .expect("req"),
        )
        .await
        .expect("resp");
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // Login, then read the ledger.
    let login = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/auth/login")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"password":"hunter2"}"#))
                .expect("req"),
        )
        .await
        .expect("login");
    assert_eq!(login.status(), StatusCode::OK);
    let login_body = login.into_body().collect().await.expect("body").to_bytes();
    let token = serde_json::from_slice::<serde_json::Value>(&login_body).expect("json")["token"]
        .as_str()
        .expect("token")
        .to_string();

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/costs")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .expect("req"),
        )
        .await
        .expect("resp");
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = resp.into_body().collect().await.expect("body").to_bytes();
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
    assert_eq!(body["window_days"], 30);
    assert_eq!(body["truncated"], false);
    assert!((body["total_usd"].as_f64().expect("total") - 2.0).abs() < 1e-9);
    assert!((body["today_usd"].as_f64().expect("today") - 2.0).abs() < 1e-9);
    assert_eq!(body["per_plan"].as_array().expect("plans").len(), 1);
    assert!(
        (body["per_plan"][0]["actual_usd"].as_f64().expect("usd") - 2.0).abs() < 1e-9,
        "{body}"
    );
    assert_eq!(body["per_issuer"].as_array().expect("issuers").len(), 1);
}
