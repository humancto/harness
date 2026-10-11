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
    // The verification block says exactly what it covered — a bare
    // "verified" would overstate a page-scoped check.
    assert_eq!(body["verification"]["checked"], true);
    assert_eq!(body["verification"]["verified"], true);
    assert_eq!(body["verification"]["scope"], "page");
    assert_eq!(body["verification"]["from_seq"], 1);
    assert_eq!(body["verification"]["through_seq"], 3);
    assert!(entries[0]["entry_hash"].as_str().expect("hash").len() == 64);

    // Filter, then keyset-page by time.
    let filtered =
        body_json(get(&app, Some(&token), "/api/v1/audit?action=shell.denied").await).await;
    assert_eq!(filtered["entries"].as_array().expect("a").len(), 1);

    // Page with the FULL cursor the response hands back — a bare
    // timestamp would skip rows sharing the boundary millisecond.
    let cursor = &body["next_cursor"];
    let uri = format!(
        "/api/v1/audit?before_ms={}&before_node={}&before_seq={}",
        cursor["before_ms"].as_u64().expect("ms"),
        cursor["before_node"].as_str().expect("node"),
        cursor["before_seq"].as_u64().expect("seq"),
    );
    let page = body_json(get(&app, Some(&token), &uri).await).await;
    assert_eq!(
        page["entries"].as_array().expect("a").len(),
        0,
        "the first page already covered every entry"
    );

    let older = body_json(get(&app, Some(&token), "/api/v1/audit?before_ms=20").await).await;
    let ids = older["entries"].as_array().expect("a");
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
    assert_eq!(body["verification"]["verified"], true);

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
    assert_eq!(body["verification"]["verified"], false);
    assert_eq!(body["verification"]["broken_at_seq"], 2);
    assert_eq!(body["verification"]["node"], node.to_string());
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

/// A stub puller, so the verify endpoint can be driven without a mesh.
#[derive(Debug, Default)]
struct StubPuller {
    calls: std::sync::Mutex<Vec<(harness_core::NodeId, u64, u64)>>,
}

impl harness_api::AuditPuller for StubPuller {
    fn request_range(
        &self,
        _peer: harness_core::NodeId,
        subject: harness_core::NodeId,
        from_seq: u64,
        target_seq: u64,
    ) -> bool {
        self.calls
            .lock()
            .expect("lock")
            .push((subject, from_seq, target_seq));
        true
    }
}

async fn post(app: &axum::Router, token: Option<&str>, uri: &str) -> axum::http::Response<Body> {
    let mut req = Request::builder().method("POST").uri(uri);
    if let Some(t) = token {
        req = req.header(header::AUTHORIZATION, format!("Bearer {t}"));
    }
    app.clone()
        .oneshot(req.body(Body::empty()).expect("req"))
        .await
        .expect("resp")
}

/// 5.13c-2: `POST /api/v1/audit/verify/:node`.
///
/// This test exists because the route was NOT registered on the first
/// push: the edit landed in a file with no route table, the handler
/// was `pub` so nothing warned, the build passed, and the endpoint
/// 404'd while the docs claimed it shipped. A route with no test is a
/// route that can silently disappear.
#[tokio::test]
async fn audit_verify_endpoint_is_routed_and_walks_from_the_previous_pin() {
    let (app, store, token, _first_local) = app_store_token().await;
    let peer = harness_core::NodeId::from_bytes([9u8; 16]);

    // Unauthenticated is refused before anything else.
    assert_eq!(
        post(&app, None, &format!("/api/v1/audit/verify/{peer}"))
            .await
            .status(),
        StatusCode::UNAUTHORIZED
    );

    // Wrong method on a real path is 405, not 404 — proof the path
    // itself is registered.
    assert_eq!(
        get(&app, Some(&token), &format!("/api/v1/audit/verify/{peer}"))
            .await
            .status(),
        StatusCode::METHOD_NOT_ALLOWED
    );

    // No puller wired: service unavailable, not 404.
    assert_eq!(
        post(&app, Some(&token), &format!("/api/v1/audit/verify/{peer}"))
            .await
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );

    // Rebuild with a puller.
    let puller = Arc::new(StubPuller::default());
    let identity = Arc::new(Identity::generate());
    let local = identity.node_id();
    let admin = AdminFile::from_password("hunter2").expect("hash");
    let state = ApiStateBuilder::new(identity, "test")
        .with_auth(Arc::new(AuthProvider::new(Some(admin))))
        .with_store(store.clone())
        .with_audit_puller(puller.clone())
        .build();
    let app2 = router(state);
    let login = app2
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
    let token2 = body_json(login).await["token"]
        .as_str()
        .expect("token")
        .to_string();

    // No pin for that node yet.
    assert_eq!(
        post(
            &app2,
            Some(&token2),
            &format!("/api/v1/audit/verify/{peer}")
        )
        .await
        .status(),
        StatusCode::NOT_FOUND
    );

    // Our own chain is never verified by asking someone else.
    assert_eq!(
        post(
            &app2,
            Some(&token2),
            &format!("/api/v1/audit/verify/{local}")
        )
        .await
        .status(),
        StatusCode::BAD_REQUEST
    );

    // Pins at 5 and 9: the walk starts just past the PREVIOUS pin, not
    // at the target and not always at 1.
    for (seq, hash) in [(5u64, 0xAAu8), (9, 0xBB)] {
        store
            .pin_peer_head(
                &harness_core::AuditHead {
                    node_id: peer,
                    seq,
                    entry_hash: [hash; 32],
                    at_ms: seq,
                    sig: harness_core::Signature::from_bytes([1u8; 64]),
                },
                peer,
                seq * 1_000,
            )
            .expect("pin");
    }
    let resp = post(
        &app2,
        Some(&token2),
        &format!("/api/v1/audit/verify/{peer}"),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    let body = body_json(resp).await;
    assert_eq!(body["target_seq"], serde_json::json!(9));
    assert_eq!(body["from_seq"], serde_json::json!(6));
    assert_eq!(body["started"], serde_json::json!(true));
    assert_eq!(
        puller.calls.lock().expect("lock").as_slice(),
        &[(peer, 6, 9)]
    );
}

/// 5.13c-2, round 4 BLOCKER-2: the verify target is the pin WE saw
/// most recently, never the highest `seq`.
///
/// `seq` is signed by the subject and `PinOutcome::RejectedSeq` only
/// refuses values above `i64::MAX`, so a head signed at exactly
/// `i64::MAX` is pinned and stays the highest-seq pin for that node
/// forever. Selecting on `MAX(seq)` therefore let one validly signed
/// garbage head aim every verify attempt past the honest chain:
/// `reached_pin` never fires, `settle_pin_from_run` is never called,
/// and no pin for that node can ever be corroborated — the subject
/// deciding whether it is auditable at all. This endpoint is the only
/// non-test trigger for a pull, so that was the whole feature.
///
/// ADR-0041 Decision 14 fixed the identical bug on the relay path in
/// 5.13c-1; this is the governing rule of the subsystem — no ordering
/// decision may key on a field the subject of that ordering controls.
#[tokio::test]
async fn audit_verify_target_ignores_an_absurd_subject_chosen_seq() {
    let store = Store::open_memory().expect("store");
    let puller = Arc::new(StubPuller::default());
    let identity = Arc::new(Identity::generate());
    let admin = AdminFile::from_password("hunter2").expect("hash");
    let state = ApiStateBuilder::new(identity, "test")
        .with_auth(Arc::new(AuthProvider::new(Some(admin))))
        .with_store(store.clone())
        .with_audit_puller(puller.clone())
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

    let peer = harness_core::NodeId::from_bytes([7u8; 16]);
    // The garbage head is pinned FIRST, so it is both the highest seq
    // and the oldest observation. Only an `observed_at_ms` ordering
    // can tell the two apart.
    let absurd = u64::try_from(i64::MAX).expect("i64::MAX fits u64");
    for (seq, hash, observed) in [
        (absurd, 0xCCu8, 1_000u64),
        (5, 0xAA, 5_000),
        (9, 0xBB, 9_000),
    ] {
        store
            .pin_peer_head(
                &harness_core::AuditHead {
                    node_id: peer,
                    seq,
                    entry_hash: [hash; 32],
                    at_ms: seq,
                    sig: harness_core::Signature::from_bytes([1u8; 64]),
                },
                peer,
                observed,
            )
            .expect("pin");
    }
    // It really is stored and really is the highest seq, so this test
    // is not passing because the pin was rejected on the way in.
    let pins = store.peer_head_pins(peer).expect("pins");
    assert_eq!(
        pins.last().expect("pin").seq,
        absurd,
        "the absurd head is pinned and is MAX(seq) — the defect's premise"
    );

    let resp = post(&app, Some(&token), &format!("/api/v1/audit/verify/{peer}")).await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    let body = body_json(resp).await;
    assert_eq!(
        body["target_seq"],
        serde_json::json!(9),
        "the newest pin by OUR clock, not the subject's biggest number"
    );
    assert_eq!(body["from_seq"], serde_json::json!(6));
    assert_eq!(
        puller.calls.lock().expect("lock").as_slice(),
        &[(peer, 6, 9)]
    );
}
