//! Phase 3.6a integration tests for `llm.cloud.claude`.
//!
//! Each test points the capability at a `wiremock::MockServer` instead
//! of the real Anthropic endpoint. The `serial_test::serial` attribute
//! is applied where a test mutates env vars, since the Rust test
//! harness runs tests within a binary in parallel.

#![cfg(feature = "llm")]
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::sync::Arc;
use std::time::Duration;

use harness_capabilities::llm_batcher::LlmBatcher;
use harness_capabilities::llm_cloud_claude::{
    enrich_with_llm_cloud_claude, LlmCloudClaudeCapability, ID, SECRET_TAG,
};
use harness_capabilities::traits::{Capability, ExecutionContext};
use harness_capabilities::CapabilityRegistry;
use harness_core::{NodeId, TaskId};
use harness_policy::{Policy, PolicyEngine};
use harness_vault::{PlaintextStore, SecretValue, SecretsStore};
use serde_json::json;
use serial_test::serial;
use tracing_test::traced_test;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

// ───────────────────────────────────────── Test helpers

/// In-memory `SecretsStore` used by tests. Avoids touching disk and
/// avoids env-var mutation for every test that just needs a key.
#[derive(Debug)]
struct TestStore {
    map: std::collections::HashMap<String, Vec<u8>>,
}

impl TestStore {
    fn empty() -> Self {
        Self {
            map: std::collections::HashMap::new(),
        }
    }

    fn with_claude_key(key: &str) -> Self {
        let mut map = std::collections::HashMap::new();
        map.insert(SECRET_TAG.to_string(), key.as_bytes().to_vec());
        Self { map }
    }
}

impl SecretsStore for TestStore {
    fn get(&self, tag: &str) -> Option<SecretValue> {
        self.map
            .get(tag)
            .map(|v| SecretValue::from_bytes(v.clone()))
    }
}

fn ctx() -> ExecutionContext {
    ExecutionContext {
        local_node: NodeId::from_bytes([1; 16]),
        local_node_name: Arc::from("self"),
        issued_by: NodeId::from_bytes([2; 16]),
        issued_by_name: Arc::from("issuer"),
        task_id: TaskId::new_v7(),
        tags: Arc::from(Vec::<String>::new()),
    }
}

fn ctx_interactive() -> ExecutionContext {
    ExecutionContext {
        local_node: NodeId::from_bytes([1; 16]),
        local_node_name: Arc::from("self"),
        issued_by: NodeId::from_bytes([2; 16]),
        issued_by_name: Arc::from("issuer"),
        task_id: TaskId::new_v7(),
        tags: Arc::from(vec!["interactive".to_string()]),
    }
}

/// Build a capability + `base_url` pointed at the given mock server.
/// `policy` defaults to `deny_all()` (which is default-allow for LLM
/// evaluation because the `[llm]` section is `None`).
fn cap_with(secrets: Arc<dyn SecretsStore>, mock: &MockServer) -> LlmCloudClaudeCapability {
    let policy = Arc::new(PolicyEngine::new(Policy::deny_all()));
    // Disable batcher window so submit() takes the dispatch-direct
    // path. Tests that need to exercise the batcher (t21) build a
    // separate batcher with a window.
    let batcher = Arc::new(LlmBatcher::with_window(Duration::ZERO));
    let client = reqwest::Client::new();
    let base_url = url::Url::parse(&format!("{}/", mock.uri())).unwrap();
    LlmCloudClaudeCapability::with_base_url(secrets, policy, batcher, client, base_url)
}

fn cap_with_policy(
    secrets: Arc<dyn SecretsStore>,
    policy: Arc<PolicyEngine>,
    mock: &MockServer,
) -> LlmCloudClaudeCapability {
    let batcher = Arc::new(LlmBatcher::with_window(Duration::ZERO));
    let client = reqwest::Client::new();
    let base_url = url::Url::parse(&format!("{}/", mock.uri())).unwrap();
    LlmCloudClaudeCapability::with_base_url(secrets, policy, batcher, client, base_url)
}

fn ok_body() -> serde_json::Value {
    json!({
        "id": "msg_01",
        "type": "message",
        "role": "assistant",
        "content": [{"type": "text", "text": "hello there"}],
        "model": "claude-3-5-sonnet-20241022",
        "usage": {"input_tokens": 7, "output_tokens": 3},
    })
}

// ───────────────────────────────────────── Tests

#[tokio::test]
async fn t12_happy_path_via_wiremock() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/messages"))
        .and(header("x-api-key", "sk-ant-test"))
        .and(header("anthropic-version", "2023-06-01"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ok_body()))
        .expect(1)
        .mount(&mock)
        .await;

    let secrets: Arc<dyn SecretsStore> = Arc::new(TestStore::with_claude_key("sk-ant-test"));
    let cap = cap_with(secrets, &mock);
    let out = cap
        .execute(
            &ctx(),
            json!({"model": "claude-3-5-sonnet-20241022", "prompt": "hi"}),
        )
        .await
        .expect("happy path must succeed");
    assert_eq!(out["text"], json!("hello there"));
    assert_eq!(out["model"], json!("claude-3-5-sonnet-20241022"));
    assert_eq!(out["prompt_tokens"], json!(7));
    assert_eq!(out["completion_tokens"], json!(3));
}

#[tokio::test]
async fn t13_secret_missing_returns_clear_error() {
    let mock = MockServer::start().await;
    // No expectations: a request would fail the mount-on-drop check.

    let secrets: Arc<dyn SecretsStore> = Arc::new(TestStore::empty());
    let cap = cap_with(secrets, &mock);
    let err = cap
        .execute(
            &ctx(),
            json!({"model": "claude-3-5-sonnet-20241022", "prompt": "hi"}),
        )
        .await
        .expect_err("must short-circuit before HTTP");
    let msg = format!("{err}");
    assert!(msg.contains("not configured"), "msg = {msg}");
    assert!(msg.contains("secret/claude-api-key"), "msg = {msg}");
}

#[tokio::test]
async fn t14_policy_deny_short_circuits_before_http() {
    let mock = MockServer::start().await;
    // No expectations — policy must reject before any HTTP fires.

    let policy_toml = r#"
        [llm]
        allow = []
        deny = [{ model = "claude-3-5-sonnet-20241022" }]
    "#;
    let policy = harness_policy::load_from_str(policy_toml).expect("parse policy");
    let policy = Arc::new(PolicyEngine::new(policy));

    let secrets: Arc<dyn SecretsStore> = Arc::new(TestStore::with_claude_key("sk-ant-test"));
    let cap = cap_with_policy(secrets, policy, &mock);
    let err = cap
        .execute(
            &ctx(),
            json!({"model": "claude-3-5-sonnet-20241022", "prompt": "hi"}),
        )
        .await
        .expect_err("policy must deny");
    let msg = format!("{err}");
    assert!(msg.contains("policy denied"), "msg = {msg}");
}

#[tokio::test]
async fn t15_anthropic_500_returns_failed() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/messages"))
        .respond_with(ResponseTemplate::new(500).set_body_string("upstream blew up"))
        .mount(&mock)
        .await;

    let secrets: Arc<dyn SecretsStore> = Arc::new(TestStore::with_claude_key("sk-ant-test"));
    let cap = cap_with(secrets, &mock);
    let err = cap
        .execute(
            &ctx(),
            json!({"model": "claude-3-5-sonnet-20241022", "prompt": "hi"}),
        )
        .await
        .expect_err("500 must propagate");
    let msg = format!("{err}");
    assert!(msg.contains("500"), "msg = {msg}");
    assert!(msg.contains("upstream blew up"), "msg = {msg}");
}

#[tokio::test]
async fn t16_interactive_bypass_skips_batcher() {
    // The test asserts the interactive path completes without going
    // through the batcher: even with a non-zero window, the request
    // returns immediately rather than waiting `window_ms`.
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ok_body()))
        .expect(1)
        .mount(&mock)
        .await;

    let secrets: Arc<dyn SecretsStore> = Arc::new(TestStore::with_claude_key("sk-ant-test"));
    // Build a cap with a 5-second window — if the interactive path
    // didn't bypass, the test would have to wait that long.
    let policy = Arc::new(PolicyEngine::new(Policy::deny_all()));
    let batcher = Arc::new(LlmBatcher::with_window(Duration::from_secs(5)));
    let client = reqwest::Client::new();
    let base_url = url::Url::parse(&format!("{}/", mock.uri())).unwrap();
    let cap = LlmCloudClaudeCapability::with_base_url(secrets, policy, batcher, client, base_url);

    let started = std::time::Instant::now();
    let out = tokio::time::timeout(
        Duration::from_secs(2),
        cap.execute(
            &ctx_interactive(),
            json!({"model": "claude-3-5-sonnet-20241022", "prompt": "hi"}),
        ),
    )
    .await
    .expect("interactive path must not block on batcher window")
    .expect("dispatch ok");
    assert_eq!(out["text"], json!("hello there"));
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "interactive bypass took too long: {:?}",
        started.elapsed()
    );
}

#[test]
fn t17_capability_id_is_llm_cloud_claude() {
    assert_eq!(ID, "llm.cloud.claude");
    let secrets: Arc<dyn SecretsStore> = Arc::new(PlaintextStore::empty());
    let policy = Arc::new(PolicyEngine::new(Policy::deny_all()));
    let batcher = Arc::new(LlmBatcher::with_window(Duration::ZERO));
    let client = reqwest::Client::new();
    let cap = LlmCloudClaudeCapability::new(secrets, policy, batcher, client);
    assert_eq!(cap.id(), "llm.cloud.claude");
}

#[test]
fn t18_manifest_advertises_cloud_paid_and_requires_secrets() {
    let secrets: Arc<dyn SecretsStore> = Arc::new(PlaintextStore::empty());
    let policy = Arc::new(PolicyEngine::new(Policy::deny_all()));
    let batcher = Arc::new(LlmBatcher::with_window(Duration::ZERO));
    let client = reqwest::Client::new();
    let cap = LlmCloudClaudeCapability::new(secrets, policy, batcher, client);
    let m = cap.manifest();
    assert_eq!(m.id, "llm.cloud.claude");
    assert!(matches!(
        m.cost_hint,
        harness_core::protocol::CostHint::CloudPaid
    ));
    assert!(matches!(m.cardinality, harness_core::Cardinality::Anyone));
    assert_eq!(
        m.requires_secrets,
        vec!["secret/claude-api-key".to_string()]
    );
    assert!(m.tags.iter().any(|t| t == "cloud"));
    assert!(m.tags.iter().any(|t| t == "claude"));
}

#[tokio::test]
#[traced_test]
async fn t19_api_key_never_logged() {
    // A 500 path emits the upstream body in the error message and
    // logs nothing about the key. Verify the captured tracing output
    // never contains the key bytes.
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/messages"))
        .respond_with(ResponseTemplate::new(500).set_body_string("upstream rejected"))
        .mount(&mock)
        .await;

    let key = "sk-ant-VERY-SECRET-DO-NOT-LEAK-12345";
    let secrets: Arc<dyn SecretsStore> = Arc::new(TestStore::with_claude_key(key));
    let cap = cap_with(secrets, &mock);
    let _err = cap
        .execute(
            &ctx(),
            json!({"model": "claude-3-5-sonnet-20241022", "prompt": "hi"}),
        )
        .await
        .expect_err("500 must propagate");
    assert!(
        !logs_contain(key),
        "API key must never appear in tracing output"
    );
}

#[tokio::test]
#[serial]
async fn t20_invalid_header_bytes_rejected_cleanly() {
    let mock = MockServer::start().await;
    // No request expected — header construction must fail first.

    // An API key with a literal newline cannot encode as a HeaderValue.
    let secrets: Arc<dyn SecretsStore> = Arc::new(TestStore::with_claude_key("sk-ant-bad\nkey"));
    let cap = cap_with(secrets, &mock);
    let err = cap
        .execute(
            &ctx(),
            json!({"model": "claude-3-5-sonnet-20241022", "prompt": "hi"}),
        )
        .await
        .expect_err("malformed key must be rejected");
    let msg = format!("{err}");
    assert!(msg.contains("invalid header bytes"), "msg = {msg}");
}

#[tokio::test]
async fn t21_fingerprint_includes_model() {
    // Same prompt + two distinct models must NOT coalesce into a
    // single backend call. Mount a wiremock mock that asserts at
    // least two POSTs arrive within the test window.
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ok_body()))
        .expect(2)
        .mount(&mock)
        .await;

    let secrets: Arc<dyn SecretsStore> = Arc::new(TestStore::with_claude_key("sk-ant-test"));
    // 200ms window is long enough that two siblings would coalesce
    // *iff* they shared a fingerprint.
    let policy = Arc::new(PolicyEngine::new(Policy::deny_all()));
    let batcher = Arc::new(LlmBatcher::with_window(Duration::from_millis(200)));
    let client = reqwest::Client::new();
    let base_url = url::Url::parse(&format!("{}/", mock.uri())).unwrap();
    let cap = LlmCloudClaudeCapability::with_base_url(secrets, policy, batcher, client, base_url);

    let cap1 = cap.clone();
    let cap2 = cap.clone();
    let h1 = tokio::spawn(async move {
        cap1.execute(
            &ctx(),
            json!({"model": "claude-3-5-sonnet-20241022", "prompt": "same"}),
        )
        .await
    });
    let h2 = tokio::spawn(async move {
        cap2.execute(
            &ctx(),
            json!({"model": "claude-3-5-haiku-20241022", "prompt": "same"}),
        )
        .await
    });
    let r1 = h1.await.expect("join1").expect("dispatch1");
    let r2 = h2.await.expect("join2").expect("dispatch2");
    assert_eq!(r1["text"], json!("hello there"));
    assert_eq!(r2["text"], json!("hello there"));
    // wiremock's `.expect(2)` is verified on MockServer drop — an
    // accidental coalesce would surface as a panic at end-of-test.
}

#[test]
fn t22_idempotent_enrich_panics() {
    let registry = CapabilityRegistry::new();
    let secrets: Arc<dyn SecretsStore> = Arc::new(PlaintextStore::empty());
    let policy = Arc::new(PolicyEngine::new(Policy::deny_all()));
    let batcher = Arc::new(LlmBatcher::with_window(Duration::ZERO));
    let client = reqwest::Client::new();

    enrich_with_llm_cloud_claude(
        &registry,
        secrets.clone(),
        policy.clone(),
        batcher.clone(),
        client.clone(),
    );
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        enrich_with_llm_cloud_claude(&registry, secrets, policy, batcher, client);
    }));
    assert!(result.is_err(), "second enrich call must panic");
}
