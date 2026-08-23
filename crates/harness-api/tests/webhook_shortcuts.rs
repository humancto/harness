//! 5.7 — `POST /webhook/shortcuts` + result GET (ADR-0035). The
//! tests exercise the REAL token scheme (no external account exists
//! to mock — "mock tokens" are tokens minted with a test key) and act
//! as the executor, as in the Twilio suites.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::sync::Arc;
use std::time::Duration;

use axum::{
    body::Body,
    http::{header, Request, StatusCode},
};
use harness_api::routes::webhook::{
    AllowFrom, SeenSids, ShortcutsLedger, WebhookRuntime, MAX_WEBHOOK_DRIVERS,
};
use harness_api::{router, ApiStateBuilder};
use harness_core::Identity;
use harness_store::{Store, TaskState};
use harness_vault::shortcuts_token::mint_token;
use http_body_util::BodyExt;
use tower::ServiceExt;

const KEY: [u8; 32] = [0x5a; 32];

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_secs()
}

fn token() -> String {
    mint_token(&KEY, "test-phone", now(), Some(now() + 3600)).expect("mint")
}

fn secrets_with(entries: &[(&str, &str)]) -> Arc<dyn harness_vault::SecretsStore> {
    use std::io::Write as _;
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("secrets.toml");
    let mut f = std::fs::File::create(&path).expect("create");
    for (k, v) in entries {
        writeln!(f, "\"{k}\" = \"{v}\"").expect("write");
    }
    drop(f);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).expect("chmod");
    }
    Arc::new(harness_vault::PlaintextStore::load_from_path(&path).expect("load"))
}

fn key_secrets() -> Arc<dyn harness_vault::SecretsStore> {
    let hex = hex::encode(KEY);
    secrets_with(&[("secret/shortcuts-signing-key", &hex)])
}

fn runtime(drivers: usize) -> Arc<WebhookRuntime> {
    Arc::new(WebhookRuntime {
        base_url_override: None,
        allow_from: AllowFrom::Senders(std::collections::HashSet::new()),
        twilio_api_base: "http://unused".to_string(),
        drivers: Arc::new(tokio::sync::Semaphore::new(drivers)),
        http: reqwest::Client::new(),
        seen_sids: parking_lot::Mutex::new(SeenSids::default()),
        shortcuts: parking_lot::Mutex::new(ShortcutsLedger::default()),
    })
}

fn app_with(
    secrets: Arc<dyn harness_vault::SecretsStore>,
    rt: Arc<WebhookRuntime>,
) -> (axum::Router, Store) {
    let store = Store::open_memory().expect("store");
    let builder = ApiStateBuilder::new(Arc::new(Identity::generate()), "test")
        .with_store(store.clone())
        .with_webhook_runtime(rt)
        .with_secrets(secrets);
    (router(builder.build()), store)
}

async fn post_goal(
    app: &axum::Router,
    bearer: Option<&str>,
    body: serde_json::Value,
) -> axum::http::Response<Body> {
    let mut req = Request::builder()
        .method("POST")
        .uri("/webhook/shortcuts")
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(t) = bearer {
        req = req.header(header::AUTHORIZATION, format!("Bearer {t}"));
    }
    app.clone()
        .oneshot(req.body(Body::from(body.to_string())).expect("req"))
        .await
        .expect("resp")
}

async fn get_result(
    app: &axum::Router,
    bearer: Option<&str>,
    id: &str,
) -> axum::http::Response<Body> {
    let mut req = Request::builder()
        .method("GET")
        .uri(format!("/webhook/shortcuts/result/{id}"));
    if let Some(t) = bearer {
        req = req.header(header::AUTHORIZATION, format!("Bearer {t}"));
    }
    app.clone()
        .oneshot(req.body(Body::empty()).expect("req"))
        .await
        .expect("resp")
}

async fn body_json(resp: axum::http::Response<Body>) -> serde_json::Value {
    let bytes = resp.into_body().collect().await.expect("body").to_bytes();
    serde_json::from_slice(&bytes).expect("json body")
}

fn complete_task(store: &Store, id: harness_core::TaskId, output: &serde_json::Value) {
    for next in [
        TaskState::Dispatched,
        TaskState::Claimed,
        TaskState::Running,
        TaskState::Done,
    ] {
        store.transition_task(id, next).expect("hop");
    }
    store
        .write_task_result_done(id, output, 1, Identity::generate().node_id())
        .expect("result");
}

fn find_task(store: &Store, capability: &str) -> Option<harness_core::Task> {
    store
        .list_recent_tasks(50)
        .expect("list")
        .into_iter()
        .find(|t| t.capability == capability)
        .and_then(|row| store.load_task(row.id).expect("load"))
}

async fn wait_for_task(store: &Store, capability: &str) -> harness_core::Task {
    for _ in 0..100 {
        if let Some(task) = find_task(store, capability) {
            return task;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("task {capability} never appeared");
}

fn canned_plan() -> serde_json::Value {
    let tid = uuid::Uuid::now_v7();
    let node = harness_core::NodeId::from_bytes([9; 16]);
    let zero_sig = serde_json::Value::Array(vec![serde_json::json!(0); 64]);
    serde_json::json!({
        "plan": {
            "id": uuid::Uuid::now_v7(),
            "name": "canned",
            "tasks": { tid.to_string(): {
                "id": tid.to_string(),
                "capability": "echo",
                "input": {"msg": "hi"},
                "resource_hints": {
                    "cpu_class": "light", "memory_mb": null,
                    "gpu_required": false, "gpu_memory_mb": null,
                    "network_class": "none", "disk_io_class": "none",
                    "estimated_duration_ms": null
                },
                "timeout_ms": null
            }},
            "edges": [],
            "budget": null,
            "checkpoint": null,
            "issued_by": node,
            "sig": zero_sig,
        },
        "confidence": 0.9,
        "rationale": "canned",
        "estimated_cost_usd": 0.0,
        "estimated_duration_ms": 10,
    })
}

/// Both mints carry the shortcuts channel (5.6 MAJOR-1 lesson) and
/// stay clear of the Twilio channels and cloud.
fn assert_channel_tags(task: &harness_core::Task) {
    assert!(task.tags.contains(&"webhook".to_string()));
    assert!(
        task.tags.contains(&"shortcuts".to_string()),
        "{:?}",
        task.tags
    );
    assert!(
        !task.tags.contains(&"whatsapp".to_string()),
        "{:?}",
        task.tags
    );
    assert!(!task.tags.contains(&"sms".to_string()), "{:?}", task.tags);
    assert!(!task.tags.contains(&"cloud_ok".to_string()));
}

/// Test-as-executor: complete brain.plan then plan.execute in the
/// background while the synchronous handler waits.
fn spawn_executor(store: Store) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let plan_row = wait_for_task(&store, "brain.plan").await;
        complete_task(&store, plan_row.id, &canned_plan());
        let exec_row = wait_for_task(&store, "plan.execute").await;
        complete_task(&store, exec_row.id, &serde_json::json!({}));
    })
}

#[tokio::test]
async fn t01_no_or_malformed_signing_key_is_503_fail_closed() {
    // No key at all.
    let (app, store) = app_with(secrets_with(&[]), runtime(MAX_WEBHOOK_DRIVERS));
    let resp = post_goal(&app, Some(&token()), serde_json::json!({"goal": "run: ls"})).await;
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(find_task(&store, "brain.plan").is_none());

    // Malformed key (not 64 hex chars) — fail closed, never a panic.
    let (app, store) = app_with(
        secrets_with(&[("secret/shortcuts-signing-key", "not-hex")]),
        runtime(MAX_WEBHOOK_DRIVERS),
    );
    let resp = post_goal(&app, Some(&token()), serde_json::json!({"goal": "run: ls"})).await;
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(find_task(&store, "brain.plan").is_none());
}

#[tokio::test]
async fn t02_bad_tokens_are_401_and_mint_nothing() {
    use base64::Engine as _;
    let (app, store) = app_with(key_secrets(), runtime(MAX_WEBHOOK_DRIVERS));
    let goal = serde_json::json!({"goal": "run: ls"});

    // Absent, garbage, wrong-key, and expired bearers.
    for bearer in [
        None,
        Some("garbage".to_string()),
        Some(mint_token(&[0x77; 32], "phone", now(), None).expect("mint")),
        Some(mint_token(&KEY, "phone", now() - 7200, Some(now() - 3600)).expect("mint")),
    ] {
        let resp = post_goal(&app, bearer.as_deref(), goal.clone()).await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED, "{bearer:?}");
    }

    // Standard-alphabet / padded re-encodings are rejected (strict
    // base64url, no normalization), as are extra segments.
    let good = token();
    let (payload_b64, mac_b64) = good.split_once('.').expect("dot");
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload_b64)
        .expect("b64");
    let padded = format!(
        "{}.{mac_b64}",
        base64::engine::general_purpose::URL_SAFE.encode(&payload)
    );
    if padded != good {
        let resp = post_goal(&app, Some(&padded), goal.clone()).await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }
    let extra = format!("{good}.extra");
    let resp = post_goal(&app, Some(&extra), goal.clone()).await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    assert!(find_task(&store, "brain.plan").is_none(), "nothing minted");
}

#[tokio::test]
async fn t03_happy_path_sync_reply_with_channel_tags_on_both_mints() {
    let (app, store) = app_with(key_secrets(), runtime(MAX_WEBHOOK_DRIVERS));
    let executor = spawn_executor(store.clone());

    let resp = post_goal(
        &app,
        Some(&token()),
        serde_json::json!({"goal": "run: uname -a"}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(body["status"], "done");
    assert!(
        body["reply"]
            .as_str()
            .expect("reply")
            .contains("✅ done — 1 steps"),
        "{body}"
    );
    assert!(body["task_id"].as_str().is_some());
    executor.await.expect("executor");

    let plan_row = find_task(&store, "brain.plan").expect("plan row");
    assert_eq!(plan_row.input["goal"], "run: uname -a");
    assert_channel_tags(&plan_row);
    let exec_row = find_task(&store, "plan.execute").expect("exec row");
    assert_channel_tags(&exec_row);
}

#[tokio::test]
async fn t04_timeout_then_result_poll() {
    let (app, store) = app_with(key_secrets(), runtime(MAX_WEBHOOK_DRIVERS));

    // Minimum wait, no executor yet → 202 running with the task id.
    let resp = post_goal(
        &app,
        Some(&token()),
        serde_json::json!({"goal": "run: ls", "wait_ms": 1000}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    let body = body_json(resp).await;
    assert_eq!(body["status"], "running");
    let task_id = body["task_id"].as_str().expect("task id").to_string();

    // GET auth + input validation.
    let resp = get_result(&app, Some("bogus"), &task_id).await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let resp = get_result(&app, Some(&token()), "not-a-uuid").await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    // Unknown (never shortcuts-minted) id → 404: the ledger is the
    // authorization scope, so other adapters' tasks are unreachable.
    let foreign = uuid::Uuid::now_v7().to_string();
    let resp = get_result(&app, Some(&token()), &foreign).await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    // While still running, the GET reports running.
    let resp = get_result(&app, Some(&token()), &task_id).await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);

    // Complete the work; the driver lands the outcome in the ledger.
    let plan_row = wait_for_task(&store, "brain.plan").await;
    complete_task(&store, plan_row.id, &canned_plan());
    let exec_row = wait_for_task(&store, "plan.execute").await;
    complete_task(&store, exec_row.id, &serde_json::json!({}));
    let mut last = serde_json::Value::Null;
    for _ in 0..100 {
        let resp = get_result(&app, Some(&token()), &task_id).await;
        if resp.status() == StatusCode::OK {
            last = body_json(resp).await;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(last["status"], "done", "{last}");
    assert!(
        last["reply"].as_str().expect("reply").contains("✅ done"),
        "{last}"
    );
}

#[tokio::test]
async fn t05_request_id_dedup_returns_original_task() {
    let (app, store) = app_with(key_secrets(), runtime(MAX_WEBHOOK_DRIVERS));

    let goal =
        |rid: &str| serde_json::json!({"goal": "run: ls", "request_id": rid, "wait_ms": 1000});
    let first = body_json(post_goal(&app, Some(&token()), goal("r1")).await).await;
    let original_id = first["task_id"].as_str().expect("id").to_string();

    // Retry with the same request_id: NO second mint, and the client
    // that never saw the first response gets the original task_id.
    let retry = body_json(post_goal(&app, Some(&token()), goal("r1")).await).await;
    assert_eq!(retry["task_id"].as_str().expect("id"), original_id);
    let minted = store
        .list_recent_tasks(50)
        .expect("list")
        .into_iter()
        .filter(|t| t.capability == "brain.plan")
        .count();
    assert_eq!(minted, 1, "one mint for both deliveries");

    // A DIFFERENT request_id is a new conversation.
    let other = body_json(post_goal(&app, Some(&token()), goal("r2")).await).await;
    assert_ne!(other["task_id"].as_str().expect("id"), original_id);
    let minted = store
        .list_recent_tasks(50)
        .expect("list")
        .into_iter()
        .filter(|t| t.capability == "brain.plan")
        .count();
    assert_eq!(minted, 2);
}

#[tokio::test]
async fn t06_unknown_fields_and_empty_goals_are_400() {
    let (app, store) = app_with(key_secrets(), runtime(MAX_WEBHOOK_DRIVERS));
    // A `constraints` field is a smuggling attempt → 400 via
    // deny_unknown_fields, not a silent drop.
    let resp = post_goal(
        &app,
        Some(&token()),
        serde_json::json!({"goal": "run: ls", "constraints": {"allow_cloud": true}}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let resp = post_goal(&app, Some(&token()), serde_json::json!({"goal": "  "})).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let resp = post_goal(
        &app,
        Some(&token()),
        serde_json::json!({"tags": ["cloud_ok"]}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    assert!(find_task(&store, "brain.plan").is_none(), "nothing minted");
}

#[tokio::test]
async fn t07_driver_cap_is_429_busy() {
    let (app, store) = app_with(key_secrets(), runtime(0));
    let resp = post_goal(&app, Some(&token()), serde_json::json!({"goal": "run: ls"})).await;
    assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
    let body = body_json(resp).await;
    assert_eq!(body["error"], "mesh_busy");
    assert!(find_task(&store, "brain.plan").is_none(), "nothing minted");
}

#[tokio::test]
async fn t08_oversized_request_id_is_400() {
    let (app, store) = app_with(key_secrets(), runtime(MAX_WEBHOOK_DRIVERS));
    let resp = post_goal(
        &app,
        Some(&token()),
        serde_json::json!({"goal": "run: ls", "request_id": "r".repeat(4000)}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let resp = post_goal(
        &app,
        Some(&token()),
        serde_json::json!({"goal": "run: ls", "request_id": "has\nnewline"}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    assert!(find_task(&store, "brain.plan").is_none(), "nothing minted");
}

#[tokio::test]
async fn t09_remembered_request_id_with_evicted_outcome_is_410_not_a_remint() {
    // Codex P2 on #58: the request map (512) outlives the outcome map
    // (256), so a remembered id can point at an evicted outcome. That
    // retry must NOT mint a second task — it gets 410 result_expired
    // with the original task_id.
    let rt = runtime(MAX_WEBHOOK_DRIVERS);
    let original = harness_core::TaskId::new_v7();
    {
        let mut ledger = rt.shortcuts.lock();
        ledger.admit(original, Some("r-old"));
        for _ in 0..300 {
            ledger.admit(harness_core::TaskId::new_v7(), None);
        }
        assert!(ledger.get(original).is_none(), "outcome evicted");
        assert_eq!(
            ledger.lookup_request("r-old"),
            Some(original),
            "mapping kept"
        );
    }
    let (app, store) = app_with(key_secrets(), rt);
    let resp = post_goal(
        &app,
        Some(&token()),
        serde_json::json!({"goal": "run: ls", "request_id": "r-old"}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::GONE);
    let body = body_json(resp).await;
    assert_eq!(body["error"], "result_expired");
    assert_eq!(
        body["task_id"].as_str().expect("id"),
        format!("{}", original.0.as_hyphenated())
    );
    assert!(find_task(&store, "brain.plan").is_none(), "no second mint");
}

#[tokio::test]
async fn t10_oversized_goal_is_400() {
    let (app, store) = app_with(key_secrets(), runtime(MAX_WEBHOOK_DRIVERS));
    let resp = post_goal(
        &app,
        Some(&token()),
        serde_json::json!({"goal": "g".repeat(5000)}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert_eq!(body["error"], "goal_too_long");
    assert!(find_task(&store, "brain.plan").is_none(), "nothing minted");
}
