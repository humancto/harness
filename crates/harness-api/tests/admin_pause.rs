//! 4.7 (ADR-0029): `POST /api/v1/admin/pause|resume` + `GET /status`
//! `paused` surfacing — the operator half of the backpressure switch.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use axum::{
    body::Body,
    http::{header, Request, StatusCode},
};
use harness_api::{router, ApiStateBuilder, AuthProvider, PauseControl};
use harness_core::Identity;
use harness_mesh::AdminFile;
use http_body_util::BodyExt;
use tower::ServiceExt;

#[derive(Debug, Default)]
struct FakePause {
    operator: AtomicBool,
}

impl PauseControl for FakePause {
    fn paused(&self) -> bool {
        self.operator.load(Ordering::Relaxed)
    }
    fn operator_paused(&self) -> bool {
        self.operator.load(Ordering::Relaxed)
    }
    fn set_operator(&self, paused: bool) {
        self.operator.store(paused, Ordering::Relaxed);
    }
}

async fn body_json(resp: axum::http::Response<Body>) -> serde_json::Value {
    let bytes = resp.into_body().collect().await.expect("body").to_bytes();
    serde_json::from_slice(&bytes).expect("json")
}

fn state_with_pause() -> (harness_api::ApiState, Arc<FakePause>) {
    let id = Arc::new(Identity::generate());
    let admin = AdminFile::from_password("hunter2").expect("hash");
    let auth = Arc::new(AuthProvider::new(Some(admin)));
    let pause = Arc::new(FakePause::default());
    let state = ApiStateBuilder::new(id, "test")
        .with_auth(auth)
        .with_pause(pause.clone())
        .build();
    (state, pause)
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

#[tokio::test]
async fn pause_resume_round_trip_and_status_surfacing() {
    let (state, pause) = state_with_pause();
    let app = router(state);
    let token = login(&app).await;

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/admin/pause")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .expect("req"),
        )
        .await
        .expect("resp");
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_json(resp).await;
    assert_eq!(json["paused"], true);
    assert!(pause.paused(), "switch actually flipped");

    // Status surfaces the effective flag (additive field).
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/status")
                .body(Body::empty())
                .expect("req"),
        )
        .await
        .expect("resp");
    assert_eq!(body_json(resp).await["paused"], true);

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/admin/resume")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .expect("req"),
        )
        .await
        .expect("resp");
    assert_eq!(body_json(resp).await["paused"], false);
    assert!(!pause.paused());
}

#[tokio::test]
async fn pause_requires_auth_and_wiring() {
    // Unauthenticated → 401.
    let (state, _) = state_with_pause();
    let app = router(state);
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/admin/pause")
                .body(Body::empty())
                .expect("req"),
        )
        .await
        .expect("resp");
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // No pause wired (bare fixture) → 503, and status reports false.
    let id = Arc::new(Identity::generate());
    let admin = AdminFile::from_password("hunter2").expect("hash");
    let auth = Arc::new(AuthProvider::new(Some(admin)));
    let bare = ApiStateBuilder::new(id, "test").with_auth(auth).build();
    let app = router(bare);
    let token = login(&app).await;
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/admin/pause")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .expect("req"),
        )
        .await
        .expect("resp");
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/status")
                .body(Body::empty())
                .expect("req"),
        )
        .await
        .expect("resp");
    assert_eq!(body_json(resp).await["paused"], false);
}
