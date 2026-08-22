//! Integration tests for `POST /api/v1/auth/login` and `POST /api/v1/tasks`.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::sync::Arc;

use axum::{
    body::Body,
    http::{header, Request, StatusCode},
};
use harness_api::{router, ApiStateBuilder, AuthProvider};
use harness_core::Identity;
use harness_mesh::AdminFile;
use harness_store::Store;
use http_body_util::BodyExt;
use tower::ServiceExt;

async fn body_json(resp: axum::http::Response<Body>) -> serde_json::Value {
    let bytes = resp.into_body().collect().await.expect("body").to_bytes();
    serde_json::from_slice(&bytes).expect("json")
}

fn build_state(initialized: bool) -> harness_api::ApiState {
    let id = Arc::new(Identity::generate());
    let store = Store::open_memory().expect("store");
    let admin = if initialized {
        Some(AdminFile::from_password("hunter2").expect("hash"))
    } else {
        None
    };
    let auth = Arc::new(AuthProvider::new(admin));
    ApiStateBuilder::new(id, "test")
        .with_auth(auth)
        .with_store(store)
        .build()
}

#[tokio::test]
async fn login_succeeds_with_correct_password() {
    let state = build_state(true);
    let app = router(state);
    let resp = app
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
    let json = body_json(resp).await;
    assert!(json["token"].as_str().unwrap().len() == 64);
    assert!(json["expires_at_ms"].as_u64().is_some());
}

#[tokio::test]
async fn login_rejects_wrong_password() {
    let state = build_state(true);
    let app = router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/auth/login")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"password":"WRONG"}"#))
                .expect("req"),
        )
        .await
        .expect("resp");
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn login_returns_503_when_admin_uninitialized() {
    let state = build_state(false);
    let app = router(state);
    let resp = app
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
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    let json = body_json(resp).await;
    assert_eq!(json["error"], "admin_not_initialized");
}

#[tokio::test]
async fn submit_task_unauthenticated_returns_401() {
    let state = build_state(true);
    let app = router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/tasks")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"capability":"echo","input":{}}"#))
                .expect("req"),
        )
        .await
        .expect("resp");
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn submit_task_with_bearer_succeeds() {
    let state = build_state(true);
    let app = router(state.clone());

    // login
    let login_resp = app
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
    assert_eq!(login_resp.status(), StatusCode::OK);
    let token = body_json(login_resp).await["token"]
        .as_str()
        .expect("token")
        .to_string();

    // submit
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/tasks")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(r#"{"capability":"echo","input":{"msg":"hi"}}"#))
                .expect("req"),
        )
        .await
        .expect("resp");
    assert_eq!(resp.status(), StatusCode::CREATED);
    let json = body_json(resp).await;
    assert!(json["task_id"].as_str().is_some());
    assert_eq!(json["state"], "submitted");
}

#[tokio::test]
async fn list_tasks_after_submit_returns_inserted() {
    let state = build_state(true);
    let app = router(state);

    // login
    let login_resp = app
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
    let token = body_json(login_resp).await["token"]
        .as_str()
        .expect("token")
        .to_string();

    // submit two
    for _ in 0..2 {
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/tasks")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(r#"{"capability":"echo","input":{}}"#))
                    .expect("req"),
            )
            .await
            .expect("submit");
        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    // list
    let list_resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/tasks")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .expect("req"),
        )
        .await
        .expect("list");
    assert_eq!(list_resp.status(), StatusCode::OK);
    let json = body_json(list_resp).await;
    let arr = json.as_array().expect("array");
    assert_eq!(arr.len(), 2);
    for entry in arr {
        assert_eq!(entry["capability"], "echo");
        assert_eq!(entry["state"], "submitted");
    }
}

#[tokio::test]
async fn get_task_serves_buffered_partials() {
    // 3.2-stream (ADR-0020): frames appended to the shared ring show up
    // as the task's `partials` array; tasks with no frames omit the key.
    let id = Arc::new(Identity::generate());
    let store = Store::open_memory().expect("store");
    let partials = Arc::new(harness_api::PartialBuffers::new());
    let auth = Arc::new(AuthProvider::new(Some(
        AdminFile::from_password("hunter2").expect("hash"),
    )));
    let state = ApiStateBuilder::new(id, "test")
        .with_auth(auth)
        .with_store(store)
        .with_partials(partials.clone())
        .build();
    let app = router(state);

    let login_resp = app
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
    let token = body_json(login_resp).await["token"]
        .as_str()
        .expect("token")
        .to_string();

    // submit
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/tasks")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    r#"{"capability":"shell.exec","input":{"cmd":"uname"}}"#,
                ))
                .expect("req"),
        )
        .await
        .expect("submit");
    assert_eq!(resp.status(), StatusCode::CREATED);
    let task_id_str = body_json(resp).await["task_id"]
        .as_str()
        .expect("task id")
        .to_string();

    // no frames yet → `partials` omitted
    let get_uri = format!("/api/v1/tasks/{task_id_str}");
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(&get_uri)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .expect("req"),
        )
        .await
        .expect("get");
    let status = resp.status();
    let json = body_json(resp).await;
    assert_eq!(status, StatusCode::OK, "body: {json}");
    assert!(
        json.get("partials").is_none(),
        "partials must be omitted when nothing streamed"
    );

    // simulate the daemon's writers, then re-fetch
    let task_id = harness_core::TaskId(uuid::Uuid::parse_str(&task_id_str).expect("uuid"));
    partials.append(task_id, "stdout", "line-one".into());
    partials.append(task_id, "stderr", "line-two".into());

    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(&get_uri)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .expect("req"),
        )
        .await
        .expect("get 2");
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_json(resp).await;
    let frames = json["partials"].as_array().expect("partials array");
    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0]["seq"], 0);
    assert_eq!(frames[0]["stream"], "stdout");
    assert_eq!(frames[0]["line"], "line-one");
    assert_eq!(frames[1]["stream"], "stderr");
    assert_eq!(frames[1]["line"], "line-two");
}

#[tokio::test]
async fn logout_invalidates_session() {
    let state = build_state(true);
    let app = router(state);

    let login_resp = app
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
    let token = body_json(login_resp).await["token"]
        .as_str()
        .expect("token")
        .to_string();

    // logout
    let logout_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/auth/logout")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .expect("req"),
        )
        .await
        .expect("logout");
    assert_eq!(logout_resp.status(), StatusCode::NO_CONTENT);

    // submit with the now-revoked token → 401
    let submit_resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/tasks")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(r#"{"capability":"echo","input":{}}"#))
                .expect("req"),
        )
        .await
        .expect("submit");
    assert_eq!(submit_resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn submit_clamps_execution_and_carries_resource_hints() {
    // 4.4 (carried risk 12 / ADR-0026): a u32::MAX timeout/lease must be
    // clamped BEFORE signing; declared resource hints must persist.
    let state = build_state(true);
    let app = router(state.clone());
    let login_resp = app
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
    let token = body_json(login_resp).await["token"]
        .as_str()
        .expect("token")
        .to_string();

    let body = serde_json::json!({
        "capability": "echo",
        "input": {"msg": "hi"},
        "execution": {
            "redundancy": 9,
            "timeout_ms": u32::MAX,
            "on_partial": "fail_fast",
            "lease_ms": u32::MAX,
        },
        "resource_hints": {
            "cpu_class": "heavy",
            "memory_mb": 2048,
            "gpu_required": false,
            "gpu_memory_mb": null,
            "network_class": "none",
            "disk_io_class": "none",
            "estimated_duration_ms": null,
        },
    });
    let resp = app
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
    let id_str = body_json(resp).await["task_id"]
        .as_str()
        .expect("id")
        .to_string();
    let id = harness_core::TaskId(id_str.parse().expect("uuid"));
    let store = state.store.clone().expect("store");
    let task = store.load_task(id).expect("load").expect("present");
    assert_eq!(
        task.execution.timeout_ms,
        harness_core::ExecutionPolicy::MAX_TIMEOUT_MS,
        "timeout clamped"
    );
    assert_eq!(
        task.execution.lease_ms,
        harness_core::ExecutionPolicy::MAX_LEASE_MS,
        "lease clamped"
    );
    assert_eq!(task.execution.redundancy, 1, "redundancy normalized");
    assert_eq!(
        task.resource_hints.cpu_class,
        harness_core::protocol::CpuClass::Heavy,
        "hints carried"
    );
    assert_eq!(task.resource_hints.memory_mb, Some(2048));
    // The clamped envelope still verifies (clamped BEFORE signing).
    assert!(harness_core::Signable::verify_signature(&task, state.identity.public_key()).is_ok());
}
