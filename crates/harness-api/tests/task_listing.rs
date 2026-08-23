//! 4.8: `GET /api/v1/tasks` — recent-across-states listing with
//! `parent`/`plan_id` linkage, `?state=` filtering, and limit clamp.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::sync::Arc;

use axum::{
    body::Body,
    http::{header, Request, StatusCode},
};
use harness_api::{router, ApiStateBuilder, AuthProvider};
use harness_core::{
    Constraints, ExecutionPolicy, Identity, PlanId, ResourceHints, RetryPolicy, Signable,
    Signature, Task, TaskId, TraceContext,
};
use harness_mesh::AdminFile;
use harness_store::{Store, TaskState};
use http_body_util::BodyExt;
use tower::ServiceExt;

async fn body_json(resp: axum::http::Response<Body>) -> serde_json::Value {
    let bytes = resp.into_body().collect().await.expect("body").to_bytes();
    serde_json::from_slice(&bytes).expect("json")
}

fn task_at(
    identity: &Identity,
    issued_at: u64,
    parent: Option<TaskId>,
    plan_id: Option<PlanId>,
) -> Task {
    let mut t = Task {
        id: TaskId::new_v7(),
        parent,
        plan_id,
        capability: "echo".into(),
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
        issued_at,
        tags: vec![],
        sig: Signature::from_bytes([0u8; 64]),
    };
    t.sign(identity).expect("sign");
    t
}

async fn get(app: &axum::Router, token: &str, uri: &str) -> axum::http::Response<Body> {
    app.clone()
        .oneshot(
            Request::builder()
                .uri(uri)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .expect("req"),
        )
        .await
        .expect("resp")
}

#[tokio::test]
async fn listing_spans_states_links_children_and_filters() {
    let identity = Identity::generate();
    let store = Store::open_memory().expect("store");
    let done = task_at(&identity, 1_700_000_000_000, None, None);
    store.insert_task(&done).expect("insert");
    for next in [
        TaskState::Dispatched,
        TaskState::Claimed,
        TaskState::Running,
        TaskState::Done,
    ] {
        store.transition_task(done.id, next).expect("hop");
    }
    let plan_id = PlanId(uuid::Uuid::now_v7());
    let child = task_at(&identity, 1_700_000_000_500, Some(done.id), Some(plan_id));
    store.insert_task(&child).expect("insert");

    let api_id = Arc::new(Identity::generate());
    let admin = AdminFile::from_password("hunter2").expect("hash");
    let auth = Arc::new(AuthProvider::new(Some(admin)));
    let state = ApiStateBuilder::new(api_id, "test")
        .with_auth(auth)
        .with_store(store)
        .build();
    let app = router(state);
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
    let token = body_json(login).await["token"]
        .as_str()
        .unwrap()
        .to_string();

    // Default: recent across ALL states, newest first, linkage present.
    let resp = get(&app, &token, "/api/v1/tasks").await;
    assert_eq!(resp.status(), StatusCode::OK);
    let arr = body_json(resp).await;
    let arr = arr.as_array().expect("array");
    assert_eq!(arr.len(), 2, "done AND submitted rows listed");
    assert_eq!(arr[0]["state"], "submitted", "newest first");
    assert_eq!(
        arr[0]["parent"].as_str(),
        Some(format!("{}", done.id.0.as_hyphenated()).as_str()),
        "child row carries its parent for grouping"
    );
    assert_eq!(
        arr[0]["plan_id"].as_str(),
        Some(format!("{}", plan_id.0.as_hyphenated()).as_str())
    );
    assert_eq!(arr[1]["state"], "done");
    assert!(
        arr[1].get("parent").is_none(),
        "top-level rows omit the linkage keys"
    );

    // `?limit=` clamps the page.
    let resp = get(&app, &token, "/api/v1/tasks?limit=1").await;
    let arr = body_json(resp).await;
    assert_eq!(arr.as_array().expect("array").len(), 1);

    // `?state=` keeps exact per-state filtering (the pre-4.8 view).
    let resp = get(&app, &token, "/api/v1/tasks?state=submitted").await;
    let arr = body_json(resp).await;
    let arr = arr.as_array().expect("array");
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["state"], "submitted");

    // The limit clamps the `?state=` arm too (diff review MINOR-6).
    let resp = get(&app, &token, "/api/v1/tasks?state=done&limit=1").await;
    let arr = body_json(resp).await;
    assert_eq!(arr.as_array().expect("array").len(), 1);

    // Unknown state → 400, not an empty 200.
    let resp = get(&app, &token, "/api/v1/tasks?state=bogus").await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    assert_eq!(body_json(resp).await["error"], "unknown_state");
}
