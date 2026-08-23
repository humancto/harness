//! Phase 3.6-gemini integration tests for `llm.cloud.gemini`.
//!
//! Mechanical mirror of the 3.6a `llm.cloud.claude` test suite: each
//! test points the capability at a `wiremock::MockServer` instead of
//! the real Google Generative Language endpoint.

#![cfg(feature = "llm")]
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::sync::Arc;
use std::time::Duration;

use harness_capabilities::llm_batcher::LlmBatcher;
use harness_capabilities::llm_cloud_gemini::{
    enrich_with_llm_cloud_gemini, LlmCloudGeminiCapability, DEFAULT_MODEL, ID, SECRET_TAG,
};
use harness_capabilities::traits::{Capability, ExecutionContext};
use harness_capabilities::CapabilityRegistry;
use harness_core::{NodeId, TaskId};
use harness_policy::{Policy, PolicyEngine};
use harness_vault::{PlaintextStore, SecretValue, SecretsStore};
use serde_json::json;
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

    fn with_gemini_key(key: &str) -> Self {
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
        frame_sink: None,
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
        frame_sink: None,
    }
}

/// Build a capability + `base_url` pointed at the given mock server.
/// `policy` defaults to `deny_all()` (which is default-allow for LLM
/// evaluation because the `[llm]` section is `None`).
fn cap_with(secrets: Arc<dyn SecretsStore>, mock: &MockServer) -> LlmCloudGeminiCapability {
    let policy = Arc::new(PolicyEngine::new(Policy::deny_all()));
    // Disable batcher window so submit() takes the dispatch-direct
    // path. Tests that exercise the batcher build their own.
    let batcher = Arc::new(LlmBatcher::with_window(Duration::ZERO));
    let client = reqwest::Client::new();
    let base_url = url::Url::parse(&format!("{}/", mock.uri())).unwrap();
    LlmCloudGeminiCapability::with_base_url(secrets, policy, batcher, client, base_url)
}

fn cap_with_policy(
    secrets: Arc<dyn SecretsStore>,
    policy: Arc<PolicyEngine>,
    mock: &MockServer,
) -> LlmCloudGeminiCapability {
    let batcher = Arc::new(LlmBatcher::with_window(Duration::ZERO));
    let client = reqwest::Client::new();
    let base_url = url::Url::parse(&format!("{}/", mock.uri())).unwrap();
    LlmCloudGeminiCapability::with_base_url(secrets, policy, batcher, client, base_url)
}

fn ok_body() -> serde_json::Value {
    json!({
        "candidates": [
            {"content": {"role": "model",
                         "parts": [{"text": "hello "}, {"text": "there"}]},
             "finishReason": "STOP"}
        ],
        "usageMetadata": {"promptTokenCount": 7, "candidatesTokenCount": 3,
                          "totalTokenCount": 10},
        "modelVersion": "gemini-2.0-flash",
    })
}

// ───────────────────────────────────────── Tests

#[tokio::test]
async fn t01_happy_path_via_wiremock() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/models/gemini-2.0-flash:generateContent"))
        .and(header("x-goog-api-key", "AIza-test"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ok_body()))
        .expect(1)
        .mount(&mock)
        .await;

    let secrets: Arc<dyn SecretsStore> = Arc::new(TestStore::with_gemini_key("AIza-test"));
    let cap = cap_with(secrets, &mock);
    let out = cap
        .execute(&ctx(), json!({"model": "gemini-2.0-flash", "prompt": "hi"}))
        .await
        .expect("happy path must succeed");
    // Multi-part responses concatenate, mirroring the 3.6a multi-block
    // behavior.
    assert_eq!(out["text"], json!("hello there"));
    assert_eq!(out["model"], json!("gemini-2.0-flash"));
    assert_eq!(out["prompt_tokens"], json!(7));
    assert_eq!(out["completion_tokens"], json!(3));
}

#[tokio::test]
async fn t02_default_model_applied_when_omitted() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(format!("/models/{DEFAULT_MODEL}:generateContent")))
        .respond_with(ResponseTemplate::new(200).set_body_json(ok_body()))
        .expect(1)
        .mount(&mock)
        .await;

    let secrets: Arc<dyn SecretsStore> = Arc::new(TestStore::with_gemini_key("AIza-test"));
    let cap = cap_with(secrets, &mock);
    let out = cap
        .execute(&ctx(), json!({"prompt": "hi"}))
        .await
        .expect("default-model path must succeed");
    assert_eq!(out["model"], json!(DEFAULT_MODEL));
}

#[tokio::test]
async fn t03_secret_missing_returns_clear_error() {
    let mock = MockServer::start().await;
    // No expectations: a request would fail the mount-on-drop check.

    let secrets: Arc<dyn SecretsStore> = Arc::new(TestStore::empty());
    let cap = cap_with(secrets, &mock);
    let err = cap
        .execute(&ctx(), json!({"prompt": "hi"}))
        .await
        .expect_err("must short-circuit before HTTP");
    let msg = format!("{err}");
    assert!(msg.contains("not configured"), "msg = {msg}");
    assert!(msg.contains("secret/gemini-api-key"), "msg = {msg}");
}

#[tokio::test]
async fn t04_policy_deny_short_circuits_before_http() {
    let mock = MockServer::start().await;
    // No expectations — policy must reject before any HTTP fires.

    let policy_toml = r#"
        [llm]
        allow = []
        deny = [{ model = "gemini-2.0-flash" }]
    "#;
    let policy = harness_policy::load_from_str(policy_toml).expect("parse policy");
    let policy = Arc::new(PolicyEngine::new(policy));

    let secrets: Arc<dyn SecretsStore> = Arc::new(TestStore::with_gemini_key("AIza-test"));
    let cap = cap_with_policy(secrets, policy, &mock);
    // Deny hits the *resolved* model — the input omits `model`, so the
    // default (`gemini-2.0-flash`) is what policy evaluates.
    let err = cap
        .execute(&ctx(), json!({"prompt": "hi"}))
        .await
        .expect_err("policy must deny");
    let msg = format!("{err}");
    assert!(msg.contains("policy denied"), "msg = {msg}");
}

#[tokio::test]
async fn t05_gemini_500_returns_failed() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(format!("/models/{DEFAULT_MODEL}:generateContent")))
        .respond_with(ResponseTemplate::new(500).set_body_string("upstream blew up"))
        .mount(&mock)
        .await;

    let secrets: Arc<dyn SecretsStore> = Arc::new(TestStore::with_gemini_key("AIza-test"));
    let cap = cap_with(secrets, &mock);
    let err = cap
        .execute(&ctx(), json!({"prompt": "hi"}))
        .await
        .expect_err("500 must propagate");
    let msg = format!("{err}");
    assert!(msg.contains("500"), "msg = {msg}");
    assert!(msg.contains("upstream blew up"), "msg = {msg}");
}

#[tokio::test]
async fn t06_timeout_maps_to_failed() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(format!("/models/{DEFAULT_MODEL}:generateContent")))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(ok_body())
                .set_delay(Duration::from_secs(5)),
        )
        .mount(&mock)
        .await;

    let secrets: Arc<dyn SecretsStore> = Arc::new(TestStore::with_gemini_key("AIza-test"));
    let cap = cap_with(secrets, &mock);
    let err = cap
        .execute(&ctx(), json!({"prompt": "hi", "timeout_ms": 200}))
        .await
        .expect_err("timeout must fail");
    let msg = format!("{err}");
    assert!(msg.contains("gemini api unreachable"), "msg = {msg}");
}

#[tokio::test]
async fn t07_interactive_bypass_skips_batcher() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(format!("/models/{DEFAULT_MODEL}:generateContent")))
        .respond_with(ResponseTemplate::new(200).set_body_json(ok_body()))
        .expect(1)
        .mount(&mock)
        .await;

    let secrets: Arc<dyn SecretsStore> = Arc::new(TestStore::with_gemini_key("AIza-test"));
    // Build a cap with a 5-second window — if the interactive path
    // didn't bypass, the test would have to wait that long.
    let policy = Arc::new(PolicyEngine::new(Policy::deny_all()));
    let batcher = Arc::new(LlmBatcher::with_window(Duration::from_secs(5)));
    let client = reqwest::Client::new();
    let base_url = url::Url::parse(&format!("{}/", mock.uri())).unwrap();
    let cap = LlmCloudGeminiCapability::with_base_url(secrets, policy, batcher, client, base_url);

    let started = std::time::Instant::now();
    let out = tokio::time::timeout(
        Duration::from_secs(2),
        cap.execute(&ctx_interactive(), json!({"prompt": "hi"})),
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
fn t08_capability_id_is_llm_cloud_gemini() {
    assert_eq!(ID, "llm.cloud.gemini");
    let secrets: Arc<dyn SecretsStore> = Arc::new(PlaintextStore::empty());
    let policy = Arc::new(PolicyEngine::new(Policy::deny_all()));
    let batcher = Arc::new(LlmBatcher::with_window(Duration::ZERO));
    let client = reqwest::Client::new();
    let cap = LlmCloudGeminiCapability::new(secrets, policy, batcher, client);
    assert_eq!(cap.id(), "llm.cloud.gemini");
}

#[test]
fn t09_manifest_advertises_cloud_paid_and_requires_secrets() {
    let secrets: Arc<dyn SecretsStore> = Arc::new(PlaintextStore::empty());
    let policy = Arc::new(PolicyEngine::new(Policy::deny_all()));
    let batcher = Arc::new(LlmBatcher::with_window(Duration::ZERO));
    let client = reqwest::Client::new();
    let cap = LlmCloudGeminiCapability::new(secrets, policy, batcher, client);
    let m = cap.manifest();
    assert_eq!(m.id, "llm.cloud.gemini");
    assert!(matches!(
        m.cost_hint,
        harness_core::protocol::CostHint::CloudPaid
    ));
    assert!(matches!(m.cardinality, harness_core::Cardinality::Anyone));
    assert_eq!(
        m.requires_secrets,
        vec!["secret/gemini-api-key".to_string()]
    );
    assert!(m.tags.iter().any(|t| t == "cloud"));
    assert!(m.tags.iter().any(|t| t == "gemini"));
}

#[test]
fn t10_input_schema_compiles_and_validates() {
    let secrets: Arc<dyn SecretsStore> = Arc::new(PlaintextStore::empty());
    let policy = Arc::new(PolicyEngine::new(Policy::deny_all()));
    let batcher = Arc::new(LlmBatcher::with_window(Duration::ZERO));
    let client = reqwest::Client::new();
    let cap = LlmCloudGeminiCapability::new(secrets, policy, batcher, client);
    let schema = cap.manifest().input_schema;
    let validator = jsonschema::validator_for(&schema).expect("input schema must compile");

    // Valid inputs.
    assert!(validator.is_valid(&json!({"prompt": "hi"})));
    assert!(validator.is_valid(&json!({
        "prompt": "hi", "model": "gemini-1.5-pro-002", "system": "be brief",
        "temperature": 0.7, "max_tokens": 256, "timeout_ms": 5000
    })));

    // Invalid inputs.
    assert!(!validator.is_valid(&json!({})), "prompt is required");
    assert!(!validator.is_valid(&json!({"prompt": ""})), "minLength 1");
    assert!(
        !validator.is_valid(&json!({"prompt": "hi", "model": "a/b"})),
        "model pattern excludes path separators"
    );
    assert!(
        !validator.is_valid(&json!({"prompt": "hi", "max_tokens": 999_999})),
        "max_tokens above cap"
    );
    assert!(
        !validator.is_valid(&json!({"prompt": "hi", "timeout_ms": 1})),
        "timeout_ms below minimum"
    );
    assert!(
        !validator.is_valid(&json!({"prompt": "hi", "bogus": true})),
        "additionalProperties false"
    );
}

#[tokio::test]
async fn t11_unsafe_model_name_rejected_before_http() {
    let mock = MockServer::start().await;
    // No expectations — validation must reject before any HTTP fires.
    // The model rides in the URL path; a separator or query char must
    // never reach URL construction.
    let secrets: Arc<dyn SecretsStore> = Arc::new(TestStore::with_gemini_key("AIza-test"));
    let cap = cap_with(secrets, &mock);
    for bad in [
        "../evil",
        "flash:streamGenerateContent",
        "flash?key=steal",
        "flash#frag",
        "",
    ] {
        let err = cap
            .execute(&ctx(), json!({"model": bad, "prompt": "hi"}))
            .await
            .expect_err("unsafe model must be rejected");
        let msg = format!("{err}");
        assert!(msg.contains("model"), "model {bad:?}: msg = {msg}");
    }
}

#[tokio::test]
async fn t12_unknown_input_field_rejected() {
    let mock = MockServer::start().await;
    // No expectations — decode must reject before any HTTP fires.

    let secrets: Arc<dyn SecretsStore> = Arc::new(TestStore::with_gemini_key("AIza-test"));
    let cap = cap_with(secrets, &mock);
    let err = cap
        .execute(&ctx(), json!({"prompt": "hi", "bogus": true}))
        .await
        .expect_err("unknown field must be rejected");
    let msg = format!("{err}");
    assert!(msg.contains("decode input"), "msg = {msg}");
}

#[tokio::test]
#[traced_test]
async fn t13_api_key_never_logged_and_not_in_url() {
    // A 500 path emits the upstream body in the error message and
    // logs nothing about the key. Verify the captured tracing output
    // never contains the key bytes, and that the key rode in the
    // header, not the `?key=` query parameter.
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(format!("/models/{DEFAULT_MODEL}:generateContent")))
        .respond_with(ResponseTemplate::new(500).set_body_string("upstream rejected"))
        .mount(&mock)
        .await;

    let key = "AIza-VERY-SECRET-DO-NOT-LEAK-12345";
    let secrets: Arc<dyn SecretsStore> = Arc::new(TestStore::with_gemini_key(key));
    let cap = cap_with(secrets, &mock);
    let _err = cap
        .execute(&ctx(), json!({"prompt": "hi"}))
        .await
        .expect_err("500 must propagate");
    assert!(
        !logs_contain(key),
        "API key must never appear in tracing output"
    );

    let requests = mock.received_requests().await.expect("requests recorded");
    assert_eq!(requests.len(), 1);
    let req = &requests[0];
    assert!(
        req.url.query().is_none(),
        "key must not ride in the query string: {:?}",
        req.url
    );
    assert_eq!(
        req.headers
            .get("x-goog-api-key")
            .map(|v| v.as_bytes().to_vec()),
        Some(key.as_bytes().to_vec())
    );
}

#[tokio::test]
async fn t14_invalid_header_bytes_rejected_cleanly() {
    let mock = MockServer::start().await;
    // No request expected — header construction must fail first.

    // An API key with a literal newline cannot encode as a HeaderValue.
    let secrets: Arc<dyn SecretsStore> = Arc::new(TestStore::with_gemini_key("AIza-bad\nkey"));
    let cap = cap_with(secrets, &mock);
    let err = cap
        .execute(&ctx(), json!({"prompt": "hi"}))
        .await
        .expect_err("malformed key must be rejected");
    let msg = format!("{err}");
    assert!(msg.contains("invalid header bytes"), "msg = {msg}");
}

#[tokio::test]
async fn t15_fingerprint_includes_model() {
    // Same prompt + two distinct models must NOT coalesce into a
    // single backend call. Mount a catch-all mock that asserts two
    // POSTs arrive within the test window.
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ok_body()))
        .expect(2)
        .mount(&mock)
        .await;

    let secrets: Arc<dyn SecretsStore> = Arc::new(TestStore::with_gemini_key("AIza-test"));
    // 200ms window is long enough that two siblings would coalesce
    // *iff* they shared a fingerprint.
    let policy = Arc::new(PolicyEngine::new(Policy::deny_all()));
    let batcher = Arc::new(LlmBatcher::with_window(Duration::from_millis(200)));
    let client = reqwest::Client::new();
    let base_url = url::Url::parse(&format!("{}/", mock.uri())).unwrap();
    let cap = LlmCloudGeminiCapability::with_base_url(secrets, policy, batcher, client, base_url);

    let cap1 = cap.clone();
    let cap2 = cap.clone();
    let h1 = tokio::spawn(async move {
        cap1.execute(
            &ctx(),
            json!({"model": "gemini-2.0-flash", "prompt": "same"}),
        )
        .await
    });
    let h2 = tokio::spawn(async move {
        cap2.execute(&ctx(), json!({"model": "gemini-1.5-pro", "prompt": "same"}))
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
fn t16_idempotent_enrich_panics() {
    let registry = CapabilityRegistry::new();
    let secrets: Arc<dyn SecretsStore> = Arc::new(PlaintextStore::empty());
    let policy = Arc::new(PolicyEngine::new(Policy::deny_all()));
    let batcher = Arc::new(LlmBatcher::with_window(Duration::ZERO));
    let client = reqwest::Client::new();

    enrich_with_llm_cloud_gemini(
        &registry,
        secrets.clone(),
        policy.clone(),
        batcher.clone(),
        client.clone(),
    );
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        enrich_with_llm_cloud_gemini(&registry, secrets, policy, batcher, client);
    }));
    assert!(result.is_err(), "second enrich call must panic");
}

#[tokio::test]
async fn t17_system_prompt_rides_as_system_instruction() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(format!("/models/{DEFAULT_MODEL}:generateContent")))
        .respond_with(ResponseTemplate::new(200).set_body_json(ok_body()))
        .expect(1)
        .mount(&mock)
        .await;

    let secrets: Arc<dyn SecretsStore> = Arc::new(TestStore::with_gemini_key("AIza-test"));
    let cap = cap_with(secrets, &mock);
    cap.execute(
        &ctx(),
        json!({"prompt": "hi", "system": "be brief", "temperature": 0.5}),
    )
    .await
    .expect("dispatch ok");

    let requests = mock.received_requests().await.expect("requests recorded");
    let body: serde_json::Value = serde_json::from_slice(&requests[0].body).expect("json body");
    assert_eq!(
        body["contents"],
        json!([{"role": "user", "parts": [{"text": "hi"}]}])
    );
    assert_eq!(
        body["systemInstruction"],
        json!({"parts": [{"text": "be brief"}]})
    );
    assert_eq!(body["generationConfig"]["maxOutputTokens"], json!(1024));
    assert_eq!(body["generationConfig"]["temperature"], json!(0.5));
}
