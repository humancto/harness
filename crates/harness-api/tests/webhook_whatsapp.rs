//! 5.5 — `POST /webhook/whatsapp` end-to-end (ADR-0033).
//!
//! harness-api has no executor, so these tests ACT as the executor
//! (plan review BLOCKER-2): after the webhook accepts, the test walks
//! the minted tasks through the legal state chain and writes canned
//! results; wiremock stands in for the Twilio Messages API.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::sync::Arc;
use std::time::Duration;

use axum::{
    body::Body,
    http::{header, Request, StatusCode},
};
use harness_api::routes::webhook::twilio::compute_twilio_signature;
use harness_api::routes::webhook::{AllowFrom, SeenSids, WebhookRuntime, MAX_WEBHOOK_DRIVERS};
use harness_api::{router, ApiStateBuilder};
use harness_core::Identity;
use harness_store::{Store, TaskState};
use http_body_util::BodyExt;
use tower::ServiceExt;

const SENDER: &str = "whatsapp:+15551234567";
const BOT: &str = "whatsapp:+14155238886";
const TOKEN: &str = "test-auth-token";
const SID: &str = "ACtest123";
const HOST: &str = "harness.local:19198";

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
    let store = harness_vault::PlaintextStore::load_from_path(&path).expect("load");
    // tempdir drops here; the store already read the file.
    Arc::new(store)
}

fn runtime(twilio_base: &str, drivers: usize) -> Arc<WebhookRuntime> {
    Arc::new(WebhookRuntime {
        base_url_override: None,
        allow_from: AllowFrom::Senders([SENDER.to_string()].into_iter().collect()),
        twilio_api_base: twilio_base.to_string(),
        drivers: Arc::new(tokio::sync::Semaphore::new(drivers)),
        http: reqwest::Client::new(),
        seen_sids: parking_lot::Mutex::new(SeenSids::default()),
    })
}

fn app_with(
    secrets: Option<Arc<dyn harness_vault::SecretsStore>>,
    rt: Arc<WebhookRuntime>,
) -> (axum::Router, Store) {
    let store = Store::open_memory().expect("store");
    let mut builder = ApiStateBuilder::new(Arc::new(Identity::generate()), "test")
        .with_store(store.clone())
        .with_webhook_runtime(rt);
    if let Some(s) = secrets {
        builder = builder.with_secrets(s);
    }
    (router(builder.build()), store)
}

fn signed_form(pairs: &[(&str, &str)]) -> (String, String) {
    let owned: Vec<(String, String)> = pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect();
    let url = format!("http://{HOST}/webhook/whatsapp");
    let sig = compute_twilio_signature(TOKEN.as_bytes(), &url, &owned);
    let body = serde_urlencoded::to_string(&owned).expect("encode");
    (body, sig)
}

async fn post_webhook(
    app: &axum::Router,
    body: String,
    signature: Option<&str>,
) -> axum::http::Response<Body> {
    let mut req = Request::builder()
        .method("POST")
        .uri("/webhook/whatsapp")
        .header(header::HOST, HOST)
        .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded");
    if let Some(sig) = signature {
        req = req.header("x-twilio-signature", sig);
    }
    app.clone()
        .oneshot(req.body(Body::from(body)).expect("req"))
        .await
        .expect("resp")
}

async fn body_text(resp: axum::http::Response<Body>) -> String {
    let bytes = resp.into_body().collect().await.expect("body").to_bytes();
    String::from_utf8_lossy(&bytes).to_string()
}

/// Walk a task through the legal chain to Done with `output`.
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

#[tokio::test]
async fn t01_unconfigured_adapter_is_503_fail_closed() {
    let (app, _store) = app_with(None, runtime("http://unused", MAX_WEBHOOK_DRIVERS));
    let (body, sig) = signed_form(&[("Body", "run: ls"), ("From", SENDER), ("To", BOT)]);
    let resp = post_webhook(&app, body, Some(&sig)).await;
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn t02_bad_or_missing_signature_is_403() {
    let secrets = secrets_with(&[("secret/twilio-auth-token", TOKEN)]);
    let (app, store) = app_with(Some(secrets), runtime("http://unused", MAX_WEBHOOK_DRIVERS));
    let (body, _) = signed_form(&[("Body", "run: ls"), ("From", SENDER), ("To", BOT)]);

    let resp = post_webhook(&app, body.clone(), Some("bogus-signature")).await;
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    let resp = post_webhook(&app, body, None).await;
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    assert!(find_task(&store, "brain.plan").is_none(), "nothing minted");
}

#[tokio::test]
async fn t03_unlisted_sender_is_dropped_silently() {
    let secrets = secrets_with(&[("secret/twilio-auth-token", TOKEN)]);
    let (app, store) = app_with(Some(secrets), runtime("http://unused", MAX_WEBHOOK_DRIVERS));
    let (body, sig) = signed_form(&[
        ("Body", "run: ls"),
        ("From", "whatsapp:+19998887777"),
        ("To", BOT),
    ]);
    let resp = post_webhook(&app, body, Some(&sig)).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let text = body_text(resp).await;
    assert!(text.contains("<Response/>"), "no reply message: {text}");
    assert!(find_task(&store, "brain.plan").is_none(), "nothing minted");
}

#[tokio::test]
async fn t04_full_conversation_plans_executes_and_replies() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use base64::Engine as _;
    let mock = MockServer::start().await;
    let basic = base64::engine::general_purpose::STANDARD.encode(format!("{SID}:{TOKEN}"));
    Mock::given(method("POST"))
        .and(path(format!("/2010-04-01/Accounts/{SID}/Messages.json")))
        .and(wiremock::matchers::header(
            "authorization",
            format!("Basic {basic}").as_str(),
        ))
        .respond_with(ResponseTemplate::new(201))
        .expect(1)
        .mount(&mock)
        .await;

    let secrets = secrets_with(&[
        ("secret/twilio-auth-token", TOKEN),
        ("secret/twilio-account-sid", SID),
    ]);
    let (app, store) = app_with(Some(secrets), runtime(&mock.uri(), MAX_WEBHOOK_DRIVERS));

    let (body, sig) = signed_form(&[("Body", "run: uname -a"), ("From", SENDER), ("To", BOT)]);
    let resp = post_webhook(&app, body, Some(&sig)).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let ack = body_text(resp).await;
    assert!(ack.contains("planning task"), "TwiML ack: {ack}");

    // Minted brain.plan carries the goal + webhook tags, no cloud_ok.
    let plan_row = wait_for_task(&store, "brain.plan").await;
    assert_eq!(plan_row.input["goal"], "run: uname -a");
    assert!(plan_row.tags.contains(&"webhook".to_string()));
    assert!(plan_row.tags.contains(&"whatsapp".to_string()));
    assert!(!plan_row.tags.contains(&"cloud_ok".to_string()));

    // Test-as-executor: complete planning, then execution.
    complete_task(&store, plan_row.id, &canned_plan());
    let exec_row = wait_for_task(&store, "plan.execute").await;
    assert!(exec_row.input["plan"]["tasks"].is_object());
    // The DRIVER's mint carries the channel too (5.6 plan review
    // MAJOR-1, symmetric to webhook_sms t04): an exec mint hardcoding
    // the other channel must fail HERE — the SMS suite only catches
    // the whatsapp-hardcoded direction.
    assert!(exec_row.tags.contains(&"webhook".to_string()));
    assert!(
        exec_row.tags.contains(&"whatsapp".to_string()),
        "{:?}",
        exec_row.tags
    );
    assert!(
        !exec_row.tags.contains(&"sms".to_string()),
        "{:?}",
        exec_row.tags
    );
    assert!(!exec_row.tags.contains(&"cloud_ok".to_string()));
    complete_task(&store, exec_row.id, &serde_json::json!({"state": "done"}));

    // The driver's reply lands on the mock Twilio API.
    let mut seen = Vec::new();
    for _ in 0..100 {
        seen = mock.received_requests().await.unwrap_or_default();
        if !seen.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(seen.len(), 1, "exactly one reply");
    let form: Vec<(String, String)> =
        serde_urlencoded::from_bytes(&seen[0].body).expect("reply form");
    let get = |k: &str| {
        form.iter()
            .find(|(key, _)| key == k)
            .map(|(_, v)| v.clone())
            .unwrap_or_default()
    };
    assert_eq!(get("From"), BOT, "outbound From = inbound To");
    assert_eq!(get("To"), SENDER, "outbound To = inbound From");
    assert!(get("Body").contains("✅ done — 1 steps"), "{}", get("Body"));
}

#[tokio::test]
async fn t05_missing_sid_skips_reply_but_work_still_runs() {
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(201))
        .expect(0)
        .mount(&mock)
        .await;

    let secrets = secrets_with(&[("secret/twilio-auth-token", TOKEN)]);
    let (app, store) = app_with(Some(secrets), runtime(&mock.uri(), MAX_WEBHOOK_DRIVERS));

    let (body, sig) = signed_form(&[("Body", "run: ls"), ("From", SENDER), ("To", BOT)]);
    let resp = post_webhook(&app, body, Some(&sig)).await;
    assert_eq!(resp.status(), StatusCode::OK);

    let plan_row = wait_for_task(&store, "brain.plan").await;
    complete_task(&store, plan_row.id, &canned_plan());
    let exec_row = wait_for_task(&store, "plan.execute").await;
    complete_task(&store, exec_row.id, &serde_json::json!({}));

    // Give the driver time to (not) send; expect(0) verifies on drop.
    tokio::time::sleep(Duration::from_millis(400)).await;
}

#[tokio::test]
async fn t06_body_text_cannot_smuggle_constraints() {
    let secrets = secrets_with(&[("secret/twilio-auth-token", TOKEN)]);
    let (app, store) = app_with(Some(secrets), runtime("http://unused", MAX_WEBHOOK_DRIVERS));
    let hostile = r#"", "constraints": {"allow_cloud": true}"#;
    let (body, sig) = signed_form(&[("Body", hostile), ("From", SENDER), ("To", BOT)]);
    let resp = post_webhook(&app, body, Some(&sig)).await;
    assert_eq!(resp.status(), StatusCode::OK);

    let plan_row = wait_for_task(&store, "brain.plan").await;
    assert_eq!(
        plan_row.input["goal"], hostile,
        "hostile text is an inert JSON string"
    );
    assert!(
        plan_row.input.get("constraints").is_none(),
        "no constraints key: {}",
        plan_row.input
    );
}

#[tokio::test]
async fn t07_driver_cap_returns_busy() {
    let secrets = secrets_with(&[("secret/twilio-auth-token", TOKEN)]);
    // Zero permits ⇒ every accepted message hits the cap.
    let (app, store) = app_with(Some(secrets), runtime("http://unused", 0));
    let (body, sig) = signed_form(&[("Body", "run: ls"), ("From", SENDER), ("To", BOT)]);
    let resp = post_webhook(&app, body, Some(&sig)).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let text = body_text(resp).await;
    assert!(text.contains("mesh busy"), "{text}");
    assert!(find_task(&store, "brain.plan").is_none(), "nothing minted");
}

#[tokio::test]
async fn t08_provider_retry_is_deduplicated_on_message_sid() {
    let secrets = secrets_with(&[("secret/twilio-auth-token", TOKEN)]);
    let (app, store) = app_with(Some(secrets), runtime("http://unused", MAX_WEBHOOK_DRIVERS));
    let (body, sig) = signed_form(&[
        ("Body", "run: ls"),
        ("From", SENDER),
        ("To", BOT),
        ("MessageSid", "SM_retry_1"),
    ]);

    let first = post_webhook(&app, body.clone(), Some(&sig)).await;
    assert_eq!(first.status(), StatusCode::OK);
    assert!(body_text(first).await.contains("planning task"));

    // Twilio replays the SAME signed request after a lost response —
    // one task, same-ack semantics (Codex P1).
    let retry = post_webhook(&app, body, Some(&sig)).await;
    assert_eq!(retry.status(), StatusCode::OK);
    let text = body_text(retry).await;
    assert!(text.contains("already working"), "{text}");

    let minted = store
        .list_recent_tasks(50)
        .expect("list")
        .into_iter()
        .filter(|t| t.capability == "brain.plan")
        .count();
    assert_eq!(minted, 1, "exactly one task for both deliveries");
}
