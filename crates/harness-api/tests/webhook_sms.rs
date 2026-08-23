//! 5.6 — `POST /webhook/sms` (ADR-0034). Channel deltas only: the
//! shared machinery (503/SID-less/smuggle/cap-busy rows) stays
//! covered once in `webhook_whatsapp.rs`. Tests act as the executor
//! (harness-api has no executor loop).

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

/// SMS senders are BARE E.164 — no `whatsapp:` prefix (ADR-0034).
const SENDER: &str = "+15551234567";
const BOT: &str = "+14155238886";
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
    Arc::new(harness_vault::PlaintextStore::load_from_path(&path).expect("load"))
}

fn runtime(twilio_base: &str, allow: &[&str]) -> Arc<WebhookRuntime> {
    Arc::new(WebhookRuntime {
        base_url_override: None,
        allow_from: AllowFrom::Senders(allow.iter().map(|s| (*s).to_string()).collect()),
        twilio_api_base: twilio_base.to_string(),
        drivers: Arc::new(tokio::sync::Semaphore::new(MAX_WEBHOOK_DRIVERS)),
        http: reqwest::Client::new(),
        seen_sids: parking_lot::Mutex::new(SeenSids::default()),
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

/// Sign for a given route path — the signature binds the URL, so an
/// SMS-route delivery must be signed for `/webhook/sms`.
fn signed_form_for(path: &str, pairs: &[(&str, &str)]) -> (String, String) {
    let owned: Vec<(String, String)> = pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect();
    let url = format!("http://{HOST}{path}");
    let sig = compute_twilio_signature(TOKEN.as_bytes(), &url, &owned);
    let body = serde_urlencoded::to_string(&owned).expect("encode");
    (body, sig)
}

async fn post_route(
    app: &axum::Router,
    path: &str,
    body: String,
    signature: &str,
) -> axum::http::Response<Body> {
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(path)
                .header(header::HOST, HOST)
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .header("x-twilio-signature", signature)
                .body(Body::from(body))
                .expect("req"),
        )
        .await
        .expect("resp")
}

async fn body_text(resp: axum::http::Response<Body>) -> String {
    let bytes = resp.into_body().collect().await.expect("body").to_bytes();
    String::from_utf8_lossy(&bytes).to_string()
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

fn assert_channel_tags(task: &harness_core::Task) {
    assert!(task.tags.contains(&"webhook".to_string()));
    assert!(task.tags.contains(&"sms".to_string()), "{:?}", task.tags);
    assert!(
        !task.tags.contains(&"whatsapp".to_string()),
        "a half-parameterized extraction must fail here (plan review MAJOR-1): {:?}",
        task.tags
    );
    assert!(!task.tags.contains(&"cloud_ok".to_string()));
}

#[tokio::test]
async fn t01_bad_signature_is_403() {
    let secrets = secrets_with(&[("secret/twilio-auth-token", TOKEN)]);
    let (app, store) = app_with(secrets, runtime("http://unused", &[SENDER]));
    let (body, _) = signed_form_for("/webhook/sms", &[("Body", "run: ls"), ("From", SENDER)]);
    let resp = post_route(&app, "/webhook/sms", body, "bogus").await;
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    assert!(find_task(&store, "brain.plan").is_none());
}

#[tokio::test]
async fn t02_unlisted_bare_number_is_dropped() {
    let secrets = secrets_with(&[("secret/twilio-auth-token", TOKEN)]);
    let (app, store) = app_with(secrets, runtime("http://unused", &[SENDER]));
    let (body, sig) = signed_form_for(
        "/webhook/sms",
        &[("Body", "run: ls"), ("From", "+19998887777"), ("To", BOT)],
    );
    let resp = post_route(&app, "/webhook/sms", body, &sig).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(body_text(resp).await.contains("<Response/>"));
    assert!(find_task(&store, "brain.plan").is_none());
}

#[tokio::test]
async fn t03_whatsapp_form_entry_never_admits_sms_sender() {
    // Channels are distinct authorization surfaces (ADR-0034): a
    // `whatsapp:+X` allowlist entry does NOT admit SMS from `+X`.
    let secrets = secrets_with(&[("secret/twilio-auth-token", TOKEN)]);
    let (app, store) = app_with(
        secrets,
        runtime("http://unused", &[&format!("whatsapp:{SENDER}")]),
    );
    let (body, sig) = signed_form_for(
        "/webhook/sms",
        &[("Body", "run: ls"), ("From", SENDER), ("To", BOT)],
    );
    let resp = post_route(&app, "/webhook/sms", body, &sig).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(body_text(resp).await.contains("<Response/>"), "dropped");
    assert!(find_task(&store, "brain.plan").is_none(), "nothing minted");
}

#[tokio::test]
async fn t04_full_sms_conversation_with_channel_tags_on_both_mints() {
    use base64::Engine as _;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

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
    let (app, store) = app_with(secrets, runtime(&mock.uri(), &[SENDER]));

    let (body, sig) = signed_form_for(
        "/webhook/sms",
        &[("Body", "run: uname -a"), ("From", SENDER), ("To", BOT)],
    );
    let resp = post_route(&app, "/webhook/sms", body, &sig).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(body_text(resp).await.contains("planning task"));

    let plan_row = wait_for_task(&store, "brain.plan").await;
    assert_eq!(plan_row.input["goal"], "run: uname -a");
    assert_channel_tags(&plan_row);

    complete_task(&store, plan_row.id, &canned_plan());
    let exec_row = wait_for_task(&store, "plan.execute").await;
    // MAJOR-1: the DRIVER's mint carries the channel too.
    assert_channel_tags(&exec_row);
    complete_task(&store, exec_row.id, &serde_json::json!({}));

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
    assert_eq!(get("From"), BOT, "outbound From = inbound To (bare E.164)");
    assert_eq!(get("To"), SENDER, "outbound To = inbound From (bare E.164)");
    assert!(get("Body").contains("✅ done — 1 steps"), "{}", get("Body"));
}

#[tokio::test]
async fn t05_dedup_ring_is_shared_across_the_sms_route() {
    let secrets = secrets_with(&[("secret/twilio-auth-token", TOKEN)]);
    let (app, store) = app_with(secrets, runtime("http://unused", &[SENDER]));
    // Same MessageSid, re-signed for the SMS URL both times (the
    // signature binds the URL — a byte-replay of another route 403s).
    let (body, sig) = signed_form_for(
        "/webhook/sms",
        &[
            ("Body", "run: ls"),
            ("From", SENDER),
            ("To", BOT),
            ("MessageSid", "SM_sms_retry"),
        ],
    );
    let first = post_route(&app, "/webhook/sms", body.clone(), &sig).await;
    assert_eq!(first.status(), StatusCode::OK);
    let retry = post_route(&app, "/webhook/sms", body, &sig).await;
    assert!(body_text(retry).await.contains("already working"));

    let minted = store
        .list_recent_tasks(50)
        .expect("list")
        .into_iter()
        .filter(|t| t.capability == "brain.plan")
        .count();
    assert_eq!(minted, 1, "one task for both deliveries");
}
