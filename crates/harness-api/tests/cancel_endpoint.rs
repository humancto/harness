//! 5.10: `POST /api/v1/tasks/:id/cancel` — the §17.8 stop button
//! (ADR-0038) — plus the `plan_id` stamp on plan.execute mints.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::sync::Arc;

use axum::{
    body::Body,
    http::{header, Request, StatusCode},
};
use harness_api::{router, ApiStateBuilder, AuthProvider};
use harness_core::{Identity, TaskId};
use harness_mesh::AdminFile;
use harness_store::{Store, TaskState};
use http_body_util::BodyExt;
use tower::ServiceExt;

async fn body_json(resp: axum::http::Response<Body>) -> serde_json::Value {
    let bytes = resp.into_body().collect().await.expect("body").to_bytes();
    serde_json::from_slice(&bytes).expect("json")
}

async fn app_and_token() -> (axum::Router, Store, String) {
    let store = Store::open_memory().expect("store");
    let admin = AdminFile::from_password("hunter2").expect("hash");
    let state = ApiStateBuilder::new(Arc::new(Identity::generate()), "test")
        .with_auth(Arc::new(AuthProvider::new(Some(admin))))
        .with_store(store.clone())
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
        .expect("token")
        .to_string();
    (app, store, token)
}

async fn submit(app: &axum::Router, token: &str, body: serde_json::Value) -> TaskId {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/tasks")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(body.to_string()))
                .expect("req"),
        )
        .await
        .expect("resp");
    assert_eq!(resp.status(), StatusCode::CREATED);
    let id = body_json(resp).await["task_id"]
        .as_str()
        .expect("id")
        .to_string();
    TaskId(id.parse().expect("uuid"))
}

async fn cancel(app: &axum::Router, token: Option<&str>, id: &str) -> axum::http::Response<Body> {
    let mut req = Request::builder()
        .method("POST")
        .uri(format!("/api/v1/tasks/{id}/cancel"));
    if let Some(t) = token {
        req = req.header(header::AUTHORIZATION, format!("Bearer {t}"));
    }
    app.clone()
        .oneshot(req.body(Body::empty()).expect("req"))
        .await
        .expect("resp")
}

#[tokio::test]
async fn cancel_matrix_and_lease_release() {
    let (app, store, token) = app_and_token().await;
    let id = submit(
        &app,
        &token,
        serde_json::json!({"capability": "echo", "input": {}}),
    )
    .await;
    let id_str = format!("{}", id.0.as_hyphenated());

    // Unauthenticated → 401; unknown → 404; malformed → 400.
    assert_eq!(
        cancel(&app, None, &id_str).await.status(),
        StatusCode::UNAUTHORIZED
    );
    let unknown = format!("{}", TaskId::new_v7().0.as_hyphenated());
    assert_eq!(
        cancel(&app, Some(&token), &unknown).await.status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        cancel(&app, Some(&token), "not-a-uuid").await.status(),
        StatusCode::BAD_REQUEST
    );

    // Dispatched row with a live lease: cancel flips the state,
    // releases the lease, and the replica mirrors Cancelled — a late
    // worker result must then drop at the terminal-lease guard.
    let worker = Identity::generate();
    assert!(store
        .try_dispatch_task(id, worker.node_id())
        .expect("dispatch"));
    let lease = store
        .create_lease(id, worker.node_id(), 60_000, 1)
        .expect("lease");
    let resp = cancel(&app, Some(&token), &id_str).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        store.task_state(id).expect("state"),
        Some(TaskState::Cancelled)
    );
    assert_eq!(
        store
            .fetch_lease(lease.lease_id)
            .expect("fetch")
            .expect("lease")
            .state,
        harness_store::LeaseState::Released
    );
    // The lease is terminal: the ingest CAS (pending|claimed →
    // completed) must refuse, which is what drops a late result.
    assert!(!store
        .try_complete_pending_or_claimed(lease.lease_id)
        .expect("cas"));

    // Double-cancel → 409 naming the terminal state.
    let resp = cancel(&app, Some(&token), &id_str).await;
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    assert_eq!(body_json(resp).await["state"], "cancelled");

    // Done rows refuse too.
    let done = submit(
        &app,
        &token,
        serde_json::json!({"capability": "echo", "input": {}}),
    )
    .await;
    for next in [
        TaskState::Dispatched,
        TaskState::Claimed,
        TaskState::Running,
        TaskState::Done,
    ] {
        store.transition_task(done, next).expect("hop");
    }
    let done_str = format!("{}", done.0.as_hyphenated());
    assert_eq!(
        cancel(&app, Some(&token), &done_str).await.status(),
        StatusCode::CONFLICT
    );
}

#[tokio::test]
async fn plan_execute_mints_carry_the_plan_id() {
    // 5.10 (plan review B2): the Costs page joins active plans to
    // ledger rows on task.plan_id — the mint must stamp it from
    // input.plan.id. (Non-plan submissions stay None.)
    let (app, store, token) = app_and_token().await;
    let plan_uuid = uuid::Uuid::now_v7();
    let id = submit(
        &app,
        &token,
        serde_json::json!({
            "capability": "plan.execute",
            "input": {"plan": {"id": plan_uuid.to_string(), "tasks": {}}},
        }),
    )
    .await;
    let task = store.load_task(id).expect("load").expect("present");
    assert_eq!(task.plan_id, Some(harness_core::PlanId(plan_uuid)));

    let plain = submit(
        &app,
        &token,
        serde_json::json!({"capability": "echo", "input": {}}),
    )
    .await;
    let task = store.load_task(plain).expect("load").expect("present");
    assert!(task.plan_id.is_none());
}
