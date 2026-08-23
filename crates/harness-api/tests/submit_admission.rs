//! 4.7 (ADR-0029): `POST /tasks` admission — 429 + `Retry-After` once
//! the submitted backlog reaches `MAX_SUBMITTED_BACKLOG`.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::sync::Arc;

use axum::{
    body::Body,
    http::{header, Request, StatusCode},
};
use harness_api::routes::tasks::MAX_SUBMITTED_BACKLOG;
use harness_api::{router, ApiStateBuilder, AuthProvider};
use harness_core::{
    Constraints, ExecutionPolicy, Identity, ResourceHints, RetryPolicy, Signature, Task, TaskId,
    TraceContext,
};
use harness_mesh::AdminFile;
use harness_store::Store;
use http_body_util::BodyExt;
use tower::ServiceExt;

async fn body_json(resp: axum::http::Response<Body>) -> serde_json::Value {
    let bytes = resp.into_body().collect().await.expect("body").to_bytes();
    serde_json::from_slice(&bytes).expect("json")
}

fn filler_task(issued_by: harness_core::NodeId) -> Task {
    Task {
        id: TaskId::new_v7(),
        parent: None,
        plan_id: None,
        capability: "echo".into(),
        input: serde_json::Value::Null,
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
        issued_by,
        issued_at: 1_700_000_000_000,
        tags: vec![],
        sig: Signature::from_bytes([0u8; 64]),
    }
}

fn build(store: Store) -> harness_api::ApiState {
    let id = Arc::new(Identity::generate());
    let admin = AdminFile::from_password("hunter2").expect("hash");
    let auth = Arc::new(AuthProvider::new(Some(admin)));
    ApiStateBuilder::new(id, "test")
        .with_auth(auth)
        .with_store(store)
        .build()
}

async fn login(app: &axum::Router) -> String {
    let resp = app
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
        .expect("resp");
    assert_eq!(resp.status(), StatusCode::OK);
    body_json(resp).await["token"].as_str().unwrap().to_string()
}

async fn submit(app: &axum::Router, token: &str) -> axum::http::Response<Body> {
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/tasks")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"capability":"echo","input":{}}"#))
                .expect("req"),
        )
        .await
        .expect("resp")
}

#[tokio::test]
async fn submit_admitted_below_cap_and_refused_at_cap() {
    let store = Store::open_memory().expect("store");
    let issuer = harness_core::NodeId::from_bytes([9u8; 16]);
    // One below the cap: the next submit is the cap-th row — admitted.
    let backlog = usize::try_from(MAX_SUBMITTED_BACKLOG).unwrap() - 1;
    for _ in 0..backlog {
        store.insert_task(&filler_task(issuer)).expect("insert");
    }
    let state = build(store);
    let app = router(state);
    let token = login(&app).await;

    let resp = submit(&app, &token).await;
    assert_eq!(
        resp.status(),
        StatusCode::CREATED,
        "backlog below cap admits"
    );

    // Backlog now AT the cap — refused with Retry-After + counters.
    let resp = submit(&app, &token).await;
    assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        resp.headers()
            .get(header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok()),
        Some("2")
    );
    let json = body_json(resp).await;
    assert_eq!(json["error"], "submitted_backlog_full");
    assert_eq!(
        json["submitted_backlog"].as_u64(),
        Some(MAX_SUBMITTED_BACKLOG)
    );
    assert_eq!(
        json["max_submitted_backlog"].as_u64(),
        Some(MAX_SUBMITTED_BACKLOG)
    );
}

#[tokio::test]
async fn admission_counts_only_submitted_rows() {
    let store = Store::open_memory().expect("store");
    let issuer = harness_core::NodeId::from_bytes([9u8; 16]);
    for _ in 0..usize::try_from(MAX_SUBMITTED_BACKLOG).unwrap() {
        let t = filler_task(issuer);
        store.insert_task(&t).expect("insert");
        // Drain the row out of `Submitted` — a busy-but-flowing queue
        // (everything dispatched) must not refuse new work.
        store
            .try_transition_task(
                t.id,
                harness_store::TaskState::Submitted,
                harness_store::TaskState::Dispatched,
            )
            .expect("transition");
    }
    let state = build(store);
    let app = router(state);
    let token = login(&app).await;
    let resp = submit(&app, &token).await;
    assert_eq!(
        resp.status(),
        StatusCode::CREATED,
        "non-Submitted rows don't count against admission"
    );
}
