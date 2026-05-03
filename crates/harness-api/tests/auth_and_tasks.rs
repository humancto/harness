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
