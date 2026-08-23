//! Phase 3.4 — `llm.local.<model>` integration tests using wiremock to
//! stub Ollama's `/api/tags` and `/api/generate` endpoints.

#![cfg(feature = "llm")]
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::needless_raw_string_hashes,
    clippy::unreadable_literal
)]

use std::sync::Arc;

use harness_capabilities::llm_local::{
    discover_local_models, parse_ollama_host, LlmLocalCapability, ID_PREFIX,
};
use harness_capabilities::traits::{Capability, CapabilityError, ExecutionContext};
use harness_capabilities::CapabilityRegistry;
use harness_core::{NodeId, TaskId};
use harness_policy::{load_from_str, Policy, PolicyEngine};
use serde_json::json;
use url::Url;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn ctx() -> ExecutionContext {
    ExecutionContext {
        local_node: NodeId::from_bytes([1; 16]),
        local_node_name: Arc::from("self"),
        issued_by: NodeId::from_bytes([2; 16]),
        issued_by_name: Arc::from("issuer"),
        task_id: TaskId::new_v7(),
        tags: std::sync::Arc::from(Vec::<String>::new()),
        frame_sink: None,
    }
}

fn engine_allow_all() -> Arc<PolicyEngine> {
    // [llm] section absent → default-allow.
    Arc::new(PolicyEngine::new(Policy::default()))
}

fn engine_deny_all_llm() -> Arc<PolicyEngine> {
    let p = load_from_str(
        r#"
[llm]
allow = []
deny  = []
"#,
    )
    .expect("policy parse");
    Arc::new(PolicyEngine::new(p))
}

fn make_cap(model: &str, host: &Url, policy: Arc<PolicyEngine>) -> LlmLocalCapability {
    use harness_capabilities::LlmBatcher;
    use std::time::Duration;
    let batcher = Arc::new(LlmBatcher::with_window(Duration::ZERO));
    LlmLocalCapability::new(
        policy,
        model.to_string(),
        host.clone(),
        reqwest::Client::new(),
        batcher,
    )
}

// ─────────────────────────────────────────────────── Discovery

#[tokio::test]
async fn t08_discover_returns_empty_when_endpoint_unreachable() {
    let host = Url::parse("http://127.0.0.1:1").expect("parse"); // port 1 is privileged + bound to nothing
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(500))
        .build()
        .expect("build client");
    let models = discover_local_models(&host, &client).await;
    assert!(models.is_empty(), "got: {models:?}");
}

#[tokio::test]
async fn t09_discover_parses_api_tags_response() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/tags"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "models": [
                { "name": "llama3:latest" },
                { "name": "mistral:7b-instruct" },
            ]
        })))
        .mount(&server)
        .await;

    let host = Url::parse(&server.uri()).expect("parse");
    let client = reqwest::Client::new();
    let mut models = discover_local_models(&host, &client).await;
    models.sort();
    assert_eq!(
        models,
        vec![
            "llama3:latest".to_string(),
            "mistral:7b-instruct".to_string()
        ]
    );
}

// ─────────────────────────────────────────────────── Capability shape

#[test]
fn t10_capability_id_format() {
    let host = Url::parse("http://127.0.0.1:11434").expect("parse");
    let cap = make_cap("llama3:latest", &host, engine_allow_all());
    assert_eq!(cap.id(), "llm.local.llama3:latest");
    assert!(cap.id().starts_with(ID_PREFIX));
}

#[test]
fn t11_manifest_advertises_llm_local_tags_and_local_slow() {
    let host = Url::parse("http://127.0.0.1:11434").expect("parse");
    let cap = make_cap("llama3:latest", &host, engine_allow_all());
    let m = cap.manifest();
    assert!(matches!(m.cardinality, harness_core::Cardinality::Anyone));
    assert!(matches!(
        m.cost_hint,
        harness_core::protocol::CostHint::LocalSlow
    ));
    assert!(m.tags.iter().any(|t| t == "llm"));
    assert!(m.tags.iter().any(|t| t == "local"));
    // gpu_required false: Ollama auto-falls-back to CPU; we don't
    // mis-route the scheduler.
    assert!(!m.resource_hints.gpu_required);
    assert!(matches!(
        m.resource_hints.cpu_class,
        harness_core::protocol::CpuClass::Heavy
    ));
    // Rate-limit tighter than shell.exec — DoS mitigation.
    let rl = m.rate_limit.expect("rate_limit must be set");
    assert_eq!(rl.per_second, 1);
    assert_eq!(rl.burst, 2);
}

// ─────────────────────────────────────────────────── Input validation

#[tokio::test]
async fn t12_invalid_input_missing_prompt() {
    let host = Url::parse("http://127.0.0.1:11434").expect("parse");
    let cap = make_cap("llama3:latest", &host, engine_allow_all());
    let err = cap
        .execute(&ctx(), json!({}))
        .await
        .expect_err("must reject");
    let CapabilityError::InvalidInput(msg) = err else {
        panic!("expected InvalidInput, got {err:?}");
    };
    assert!(msg.contains("prompt"));
}

#[tokio::test]
async fn t13_max_tokens_cap_enforced() {
    let host = Url::parse("http://127.0.0.1:11434").expect("parse");
    let cap = make_cap("llama3:latest", &host, engine_allow_all());
    let err = cap
        .execute(&ctx(), json!({"prompt": "hi", "max_tokens": 999999}))
        .await
        .expect_err("must reject");
    let CapabilityError::InvalidInput(msg) = err else {
        panic!("expected InvalidInput, got {err:?}");
    };
    assert!(msg.contains("max_tokens"));
}

// ─────────────────────────────────────────────────── Policy gate

#[tokio::test]
async fn t14_policy_deny_short_circuits_before_http() {
    // wiremock isn't started — if the capability hit the network, the
    // call would fail with "ollama serve not reachable", not "policy
    // denied". Asserting policy-denied confirms the gate runs first.
    let host = Url::parse("http://127.0.0.1:1").expect("parse");
    let cap = make_cap("llama3:latest", &host, engine_deny_all_llm());
    let err = cap
        .execute(&ctx(), json!({"prompt": "hi"}))
        .await
        .expect_err("must deny");
    let CapabilityError::Failed(msg) = err else {
        panic!("expected Failed, got {err:?}");
    };
    assert!(msg.contains("policy denied"), "got: {msg}");
}

#[tokio::test]
async fn t15_ollama_unreachable_returns_failed() {
    let host = Url::parse("http://127.0.0.1:1").expect("parse");
    let cap = make_cap("llama3:latest", &host, engine_allow_all());
    let err = cap
        .execute(&ctx(), json!({"prompt": "hi", "timeout_ms": 500}))
        .await
        .expect_err("must fail");
    let CapabilityError::Failed(msg) = err else {
        panic!("expected Failed, got {err:?}");
    };
    assert!(msg.contains("ollama serve not reachable"), "got: {msg}");
}

// ─────────────────────────────────────────────────── Happy path

#[tokio::test]
async fn t16_happy_path_via_wiremock() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/generate"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "model": "llama3:latest",
            "response": "hello world",
            "done": true,
            "prompt_eval_count": 3,
            "eval_count": 2,
        })))
        .mount(&server)
        .await;

    let host = Url::parse(&server.uri()).expect("parse");
    let cap = make_cap("llama3:latest", &host, engine_allow_all());
    let out = cap
        .execute(&ctx(), json!({"prompt": "hi"}))
        .await
        .expect("execute");
    assert_eq!(out["text"].as_str().unwrap(), "hello world");
    assert_eq!(out["model"].as_str().unwrap(), "llama3:latest");
    assert_eq!(out["prompt_tokens"].as_u64().unwrap(), 3);
    assert_eq!(out["completion_tokens"].as_u64().unwrap(), 2);
}

// ─────────────────────────────────────────────────── Registry routing

#[test]
fn t17_capability_id_with_colon_routes_through_registry() {
    let host = Url::parse("http://127.0.0.1:11434").expect("parse");
    let cap = make_cap("llama3:latest", &host, engine_allow_all());
    let registry = CapabilityRegistry::new();
    registry.register(Arc::new(cap)).expect("register");
    let resolved = registry.get("llm.local.llama3:latest");
    assert!(
        resolved.is_some(),
        "id with `:` must round-trip through registry"
    );
}

// ─────────────────────────────────────────────────── OLLAMA_HOST parsing
// (unit tests in llm_local.rs cover this; this is a structural check)

#[test]
fn t18_ollama_host_parsing_three_forms() {
    assert!(parse_ollama_host("http://x.com:9000").is_ok());
    assert!(parse_ollama_host("x.com:9000").is_ok());
    let u = parse_ollama_host("x.com").expect("plain host");
    assert_eq!(u.port(), Some(11434));
    assert!(parse_ollama_host("").is_err());
}

// ─────────────────────────────────────────────────── Shared client

#[test]
fn t19_one_reqwest_client_shared_across_caps() {
    // Structural: build two caps from the same `enrich_with_llm_local`
    // flow (here simulated by passing the same Client to both). reqwest
    // doesn't expose pool identity, so this test confirms the wiring
    // shape (both caps construct successfully with a shared client).
    // The real guarantee — connection pool sharing — is documented in
    // reqwest's docs: `Client::clone()` shares the underlying pool.
    use harness_capabilities::LlmBatcher;
    use std::time::Duration;
    let host = Url::parse("http://127.0.0.1:11434").expect("parse");
    let policy = engine_allow_all();
    let shared = reqwest::Client::new();
    let batcher = Arc::new(LlmBatcher::with_window(Duration::from_millis(50)));
    let _cap_a = LlmLocalCapability::new(
        policy.clone(),
        "model-a".to_string(),
        host.clone(),
        shared.clone(),
        batcher.clone(),
    );
    let _cap_b = LlmLocalCapability::new(policy, "model-b".to_string(), host, shared, batcher);
    // No assertion — successful construction is the test.
}
