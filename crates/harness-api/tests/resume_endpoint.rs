//! 5.12: `POST /api/v1/tasks/:id/resume` — re-run a stopped plan,
//! replaying whatever its checkpoints still cover (ADR-0040).
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

async fn resume(
    app: &axum::Router,
    token: Option<&str>,
    id: &str,
    body: Option<serde_json::Value>,
) -> axum::http::Response<Body> {
    let mut req = Request::builder()
        .method("POST")
        .uri(format!("/api/v1/tasks/{id}/resume"));
    if let Some(t) = token {
        req = req.header(header::AUTHORIZATION, format!("Bearer {t}"));
    }
    let payload = match body {
        Some(v) => {
            req = req.header(header::CONTENT_TYPE, "application/json");
            Body::from(v.to_string())
        }
        None => Body::empty(),
    };
    app.clone()
        .oneshot(req.body(payload).expect("req"))
        .await
        .expect("resp")
}

fn plan_value(plan_id: uuid::Uuid, cap: f64) -> serde_json::Value {
    // Build through the real type: a hand-rolled JSON plan does not
    // round-trip (NodeId/Signature have their own serde shapes), and
    // the resume path re-parses whatever it is handed.
    let plan = harness_core::Plan {
        id: harness_core::PlanId(plan_id),
        name: "resumable".into(),
        tasks: std::collections::HashMap::new(),
        edges: vec![],
        budget: Some(harness_core::Budget {
            max_cost_usd: Some(cap),
            soft_limit_usd: None,
            on_exceed: harness_core::BudgetAction::Pause,
        }),
        checkpoint: None,
        issued_by: harness_core::NodeId::from_bytes([1; 16]),
        sig: harness_core::Signature::from_bytes([0u8; 64]),
    };
    serde_json::to_value(&plan).expect("plan json")
}

/// Walk a plan.execute row to Done and persist a stopped aggregate.
fn settle_with_aggregate(store: &Store, id: TaskId, aggregate: &serde_json::Value) {
    for next in [
        TaskState::Dispatched,
        TaskState::Claimed,
        TaskState::Running,
        TaskState::Done,
    ] {
        store.transition_task(id, next).expect("hop");
    }
    store
        .write_task_result_done(id, aggregate, 1, harness_core::NodeId::from_bytes([9; 16]))
        .expect("result");
}

#[tokio::test]
async fn resume_matrix_and_mint() {
    let (app, store, token) = app_and_token().await;
    let plan_id = uuid::Uuid::now_v7();
    let parked = uuid::Uuid::now_v7().to_string();

    let id = submit(
        &app,
        &token,
        serde_json::json!({
            "capability": "plan.execute",
            "input": {"plan": plan_value(plan_id, 5.0)},
        }),
    )
    .await;
    let id_str = format!("{}", id.0.as_hyphenated());

    // Auth + shape.
    assert_eq!(
        resume(&app, None, &id_str, None).await.status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        resume(&app, Some(&token), "not-a-uuid", None)
            .await
            .status(),
        StatusCode::BAD_REQUEST
    );
    let unknown = format!("{}", TaskId::new_v7().0.as_hyphenated());
    assert_eq!(
        resume(&app, Some(&token), &unknown, None).await.status(),
        StatusCode::NOT_FOUND
    );

    // A non-plan row refuses.
    let echo = submit(
        &app,
        &token,
        serde_json::json!({"capability": "echo", "input": {}}),
    )
    .await;
    let resp = resume(
        &app,
        Some(&token),
        &format!("{}", echo.0.as_hyphenated()),
        None,
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    assert_eq!(body_json(resp).await["error"], "not_a_plan");

    // The plan is still live: resuming it would mint a second
    // coordinator for the same plan.
    let resp = resume(&app, Some(&token), &id_str, None).await;
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    assert_eq!(body_json(resp).await["error"], "already_running");

    // Park it: done, with one never-dispatched step recorded.
    settle_with_aggregate(
        &store,
        id,
        &serde_json::json!({
            "plan_id": plan_id.to_string(),
            "status": "paused_budget",
            "ok": 1, "failed": 0, "timed_out": 0, "skipped": 1,
            "resume": {"unscheduled": [parked], "in_flight": []},
        }),
    );

    let resp = resume(
        &app,
        Some(&token),
        &id_str,
        Some(serde_json::json!({"max_cost_usd": 20.0})),
    )
    .await;
    let status = resp.status();
    let body = body_json(resp).await;
    assert_eq!(status, StatusCode::CREATED, "body: {body}");
    assert_eq!(body["plan_id"], plan_id.to_string());
    assert_eq!(body["unscheduled"][0], parked.as_str());
    assert_eq!(
        body["replayable"], 0,
        "no checkpoints recorded in this test"
    );

    // The new row carries the SAME plan id — that is what makes 5.11's
    // checkpoints hit — and the raised cap.
    let new_id = TaskId(body["task_id"].as_str().expect("id").parse().expect("uuid"));
    let minted = store.load_task(new_id).expect("load").expect("row");
    assert_eq!(minted.plan_id, Some(harness_core::PlanId(plan_id)));
    assert_eq!(minted.capability, "plan.execute");
    assert!(
        (minted.input["plan"]["budget"]["max_cost_usd"]
            .as_f64()
            .expect("cap")
            - 20.0)
            .abs()
            < f64::EPSILON,
        "the raised cap rides the resumed plan"
    );
    assert!(minted.tags.iter().any(|t| t == "resume"));

    // And now THAT run is live, so a second resume refuses.
    let resp = resume(&app, Some(&token), &id_str, None).await;
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    assert_eq!(body_json(resp).await["error"], "already_running");
}

#[tokio::test]
async fn in_flight_steps_require_an_explicit_opt_in() {
    // A step that was dispatched and never settled may run twice on
    // resume. The endpoint refuses until the caller says that is ok.
    let (app, store, token) = app_and_token().await;
    let plan_id = uuid::Uuid::now_v7();
    let dispatched = uuid::Uuid::now_v7().to_string();
    let id = submit(
        &app,
        &token,
        serde_json::json!({
            "capability": "plan.execute",
            "input": {"plan": plan_value(plan_id, 5.0)},
        }),
    )
    .await;
    settle_with_aggregate(
        &store,
        id,
        &serde_json::json!({
            "plan_id": plan_id.to_string(),
            "status": "cancelled",
            "ok": 1, "failed": 0, "timed_out": 0, "skipped": 1,
            "resume": {"unscheduled": [], "in_flight": [dispatched]},
        }),
    );
    let id_str = format!("{}", id.0.as_hyphenated());

    let resp = resume(&app, Some(&token), &id_str, None).await;
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    let body = body_json(resp).await;
    assert_eq!(body["error"], "steps_in_flight");
    assert_eq!(body["in_flight"][0], dispatched.as_str());

    let resp = resume(
        &app,
        Some(&token),
        &id_str,
        Some(serde_json::json!({"allow_in_flight": true})),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CREATED);
    assert_eq!(body_json(resp).await["in_flight"][0], dispatched.as_str());
}
