//! 5.13a: `GET /api/v1/audit` — the History feed over the hash chain.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::sync::Arc;

use axum::{
    body::Body,
    http::{header, Request, StatusCode},
};
use harness_api::{router, ApiStateBuilder, AuthProvider};
use harness_core::{AuditAction, AuditActor, AuditRecord, Identity};
use harness_mesh::AdminFile;
use harness_store::Store;
use http_body_util::BodyExt;
use tower::ServiceExt;

async fn body_json(resp: axum::http::Response<Body>) -> serde_json::Value {
    let bytes = resp.into_body().collect().await.expect("body").to_bytes();
    serde_json::from_slice(&bytes).expect("json")
}

async fn app_store_token() -> (axum::Router, Store, String, harness_core::NodeId) {
    let store = Store::open_memory().expect("store");
    let identity = Arc::new(Identity::generate());
    let node = identity.node_id();
    let admin = AdminFile::from_password("hunter2").expect("hash");
    let state = ApiStateBuilder::new(identity, "test")
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
    (app, store, token, node)
}

async fn get(app: &axum::Router, token: Option<&str>, uri: &str) -> axum::http::Response<Body> {
    let mut req = Request::builder().uri(uri);
    if let Some(t) = token {
        req = req.header(header::AUTHORIZATION, format!("Bearer {t}"));
    }
    app.clone()
        .oneshot(req.body(Body::empty()).expect("req"))
        .await
        .expect("resp")
}

#[tokio::test]
async fn audit_feed_lists_filters_and_pages() {
    let (app, store, token, node) = app_store_token().await;

    assert_eq!(
        get(&app, None, "/api/v1/audit").await.status(),
        StatusCode::UNAUTHORIZED
    );

    for (n, action) in [
        (10u64, AuditAction::TaskDispatched),
        (20, AuditAction::ShellDenied),
        (30, AuditAction::CloudEscalated),
    ] {
        store
            .audit_append(
                node,
                &AuditRecord::new(action, AuditActor::System).with_subject(format!("s{n}")),
                n,
            )
            .expect("append");
    }

    let body = body_json(get(&app, Some(&token), "/api/v1/audit").await).await;
    let entries = body["entries"].as_array().expect("entries");
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0]["action"], "cloud.escalated", "newest first");
    assert_eq!(body["verified"], true);
    assert_eq!(body["broken_at_seq"], serde_json::Value::Null);
    assert!(entries[0]["entry_hash"].as_str().expect("hash").len() == 64);

    // Filter, then keyset-page by time.
    let filtered =
        body_json(get(&app, Some(&token), "/api/v1/audit?action=shell.denied").await).await;
    assert_eq!(filtered["entries"].as_array().expect("a").len(), 1);

    let page = body_json(get(&app, Some(&token), "/api/v1/audit?before_ms=20").await).await;
    let ids = page["entries"].as_array().expect("a");
    assert_eq!(ids.len(), 1);
    assert_eq!(ids[0]["at_ms"], 10);

    // A bad node filter is a 400, not a silent empty page.
    assert_eq!(
        get(&app, Some(&token), "/api/v1/audit?node=nothex")
            .await
            .status(),
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn a_tampered_row_shows_as_broken_in_the_feed() {
    // The banner is the whole point: if verification is not visible,
    // the hash chain is decoration.
    let (app, store, token, node) = app_store_token().await;
    for n in 1..=3u64 {
        store
            .audit_append(
                node,
                &AuditRecord::new(AuditAction::TaskDispatched, AuditActor::System)
                    .with_subject(format!("task-{n}")),
                n,
            )
            .expect("append");
    }
    let body = body_json(get(&app, Some(&token), "/api/v1/audit").await).await;
    assert_eq!(body["verified"], true);

    store
        .with_conn(|c| {
            c.execute(
                "UPDATE audit_log SET subject = 'rewritten' WHERE seq = 2",
                [],
            )?;
            Ok(())
        })
        .expect("tamper");

    let body = body_json(get(&app, Some(&token), "/api/v1/audit").await).await;
    assert_eq!(body["verified"], false);
    assert_eq!(body["broken_at_seq"], 2);
    assert_eq!(body["verified_node"], node.to_string());
}

#[tokio::test]
async fn cancel_and_resume_are_recorded() {
    // Two of the §10.6 privileged actions, end to end through the API.
    let (app, store, token, node) = app_store_token().await;
    let submit = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/tasks")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({"capability": "echo", "input": {}}).to_string(),
                ))
                .expect("req"),
        )
        .await
        .expect("submit");
    let task_id = body_json(submit).await["task_id"]
        .as_str()
        .expect("id")
        .to_string();

    let cancel = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/tasks/{task_id}/cancel"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .expect("req"),
        )
        .await
        .expect("cancel");
    assert_eq!(cancel.status(), StatusCode::OK);

    let rows = store
        .audit_recent(None, Some("task.cancelled"), Some(node), 10)
        .expect("recent");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].subject.as_deref(), Some(task_id.as_str()));
    assert_eq!(rows[0].actor, "local_admin", "an operator, not a peer");
}
