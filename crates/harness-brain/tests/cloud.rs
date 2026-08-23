//! 5.2 — `CloudBackend` integration tests via wiremock (ADR-0031).

#![cfg(feature = "cloud")]
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::float_cmp
)]

use std::sync::Arc;

use harness_brain::backend::{PlanOutcome, PlanRequest, PlannerBackend};
use harness_brain::cloud::{CloudBackend, CloudKeyProvider, HeaderValue};
use harness_brain::{CapabilitySchemaIndex, PlanConstraints, PlannerError};
use harness_core::{CapabilityRef, NodeId};
use serde_json::json;
use wiremock::matchers::{body_partial_json, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Regression guard (diff review BLOCKER-1): current Anthropic models
/// reject sampling parameters with HTTP 400 — the request body must
/// never carry them.
struct NoSamplingParams;

impl wiremock::Match for NoSamplingParams {
    fn matches(&self, request: &wiremock::Request) -> bool {
        serde_json::from_slice::<serde_json::Value>(&request.body).is_ok_and(|v| {
            v.get("temperature").is_none() && v.get("top_p").is_none() && v.get("top_k").is_none()
        })
    }
}

fn shell_only() -> Vec<CapabilityRef> {
    vec![CapabilityRef {
        id: "shell.exec".to_string(),
        version_major: 0,
    }]
}

fn cloud_req(goal: &str) -> PlanRequest {
    PlanRequest {
        goal: goal.to_string(),
        available_capabilities: shell_only(),
        schemas: CapabilitySchemaIndex::from_pairs(vec![]),
        constraints: PlanConstraints {
            allow_cloud: true,
            ..PlanConstraints::default()
        },
        context: None,
        issuing_node: NodeId::from_bytes([7; 16]),
        repair: None,
    }
}

fn key_provider(key: &'static str) -> CloudKeyProvider {
    Arc::new(move || {
        let mut v = HeaderValue::from_static(key);
        v.set_sensitive(true);
        Some(v)
    })
}

fn backend_with(mock_uri: &str, provider: CloudKeyProvider) -> CloudBackend {
    let base = url::Url::parse(&format!("{mock_uri}/v1/")).unwrap();
    CloudBackend::new(
        base,
        "claude-sonnet-5".to_string(),
        NodeId::from_bytes([1; 16]),
        provider,
    )
    .unwrap()
}

/// Helper: wraps a plan payload in the Messages API envelope, split
/// across two text blocks to prove block concatenation works.
fn anthropic_response(inner: &str) -> serde_json::Value {
    let (a, b) = inner.split_at(inner.len() / 2);
    json!({
        "id": "msg_test",
        "type": "message",
        "role": "assistant",
        "content": [
            {"type": "text", "text": a},
            {"type": "text", "text": b},
        ],
        "model": "claude-sonnet-5",
        "usage": {"input_tokens": 100, "output_tokens": 50},
    })
}

fn llm_plan() -> serde_json::Value {
    json!({
        "plan": {
            "name": "test",
            "tasks": [
                {"id": "a", "capability": "shell.exec", "input": {"cmd": "ls"}},
                {"id": "b", "capability": "shell.exec", "input": {"cmd": "wc"}}
            ],
            // LLM convention: a runs before b.
            "edges": [["a", "b"]]
        },
        "confidence": 0.9,
        "rationale": "test",
        "estimated_cost_usd": 0.01,
        "estimated_duration_ms": 50
    })
}

// ───────────────────────────────────────── Happy path

#[tokio::test]
async fn t01_happy_path_id_headers_and_edge_flip() {
    let mock = MockServer::start().await;
    let inner = serde_json::to_string(&llm_plan()).unwrap();
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .and(header("x-api-key", "sk-ant-test"))
        .and(header("anthropic-version", "2023-06-01"))
        .and(body_partial_json(json!({
            "model": "claude-sonnet-5",
            "max_tokens": 16_384,
            "messages": [{"role": "user"}],
        })))
        .and(NoSamplingParams)
        .respond_with(ResponseTemplate::new(200).set_body_json(anthropic_response(&inner)))
        .expect(1)
        .mount(&mock)
        .await;

    let b = backend_with(&mock.uri(), key_provider("sk-ant-test"));
    assert_eq!(b.id(), "cloud:claude-sonnet-5");

    let outcome = b.plan(&cloud_req("run: ls | wc")).await.expect("ok");
    let resp = match outcome {
        PlanOutcome::Confident(p) => p,
        other => panic!("want Confident, got {other:?}"),
    };
    assert_eq!(resp.confidence, 0.9);
    let plan = resp.plan.as_inner();
    assert_eq!(plan.tasks.len(), 2);
    // LLM said "a before b"; harness edges are (from, to) = "from
    // depends on to", so the single edge must point b-task → a-task.
    assert_eq!(plan.edges.len(), 1);
    let (from, to) = plan.edges[0];
    let from_cap = &plan.tasks[&from].capability;
    let to_cap = &plan.tasks[&to].capability;
    assert_eq!(from_cap, "shell.exec");
    assert_eq!(to_cap, "shell.exec");
    assert_eq!(plan.tasks[&from].input, json!({"cmd": "wc"}));
    assert_eq!(plan.tasks[&to].input, json!({"cmd": "ls"}));
}

// ───────────────────────────────────────── Escalation gating (zero I/O)

#[tokio::test]
async fn t02_allow_cloud_false_is_nomatch_with_zero_requests() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&mock)
        .await;

    let b = backend_with(&mock.uri(), key_provider("sk-ant-test"));
    let mut r = cloud_req("run: ls");
    r.constraints.allow_cloud = false;
    let outcome = b.plan(&r).await.expect("gate is not an error");
    assert!(matches!(outcome, PlanOutcome::NoMatch), "got {outcome:?}");
}

#[tokio::test]
async fn t03_must_be_local_is_nomatch_with_zero_requests() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&mock)
        .await;

    let b = backend_with(&mock.uri(), key_provider("sk-ant-test"));
    let mut r = cloud_req("run: ls");
    r.constraints.must_be_local = true;
    let outcome = b.plan(&r).await.expect("gate is not an error");
    assert!(matches!(outcome, PlanOutcome::NoMatch), "got {outcome:?}");
}

// ───────────────────────────────────────── Missing key

#[tokio::test]
async fn t04_missing_key_is_internal_diagnostic_with_zero_requests() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&mock)
        .await;

    let none_provider: CloudKeyProvider = Arc::new(|| None);
    let b = backend_with(&mock.uri(), none_provider);
    let err = b.plan(&cloud_req("run: ls")).await.expect_err("no key");
    match err {
        PlannerError::Internal(msg) => {
            assert!(
                msg.contains("secret/claude-api-key"),
                "diagnostic names the tag: {msg}"
            );
        }
        other => panic!("want Internal, got {other:?}"),
    }
}

// ───────────────────────────────────────── Transport + decode errors

#[tokio::test]
async fn t05_http_500_is_transport() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(ResponseTemplate::new(500).set_body_string("overloaded"))
        .expect(1)
        .mount(&mock)
        .await;

    let b = backend_with(&mock.uri(), key_provider("sk-ant-test"));
    let err = b.plan(&cloud_req("run: ls")).await.expect_err("500");
    match err {
        PlannerError::Transport(msg) => assert!(msg.contains("500"), "{msg}"),
        other => panic!("want Transport, got {other:?}"),
    }
}

#[tokio::test]
async fn t06_prose_only_response_is_decode() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "content": [{"type": "text", "text": "I cannot plan this."}],
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let b = backend_with(&mock.uri(), key_provider("sk-ant-test"));
    let err = b.plan(&cloud_req("run: ls")).await.expect_err("no JSON");
    assert!(matches!(err, PlannerError::Decode(_)), "got {err:?}");
}

#[tokio::test]
async fn t07_garbage_envelope_is_decode() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_string("not json at all"))
        .expect(1)
        .mount(&mock)
        .await;

    let b = backend_with(&mock.uri(), key_provider("sk-ant-test"));
    let err = b.plan(&cloud_req("run: ls")).await.expect_err("garbage");
    assert!(matches!(err, PlannerError::Decode(_)), "got {err:?}");
}

// ───────────────────────────────────────── Debug redaction

#[test]
fn t08_debug_redacts_key_provider() {
    let provider = key_provider("sk-ant-supersecret");
    let base = url::Url::parse("https://api.anthropic.com/v1/").unwrap();
    let b = CloudBackend::new(
        base,
        "claude-sonnet-5".to_string(),
        NodeId::from_bytes([1; 16]),
        provider,
    )
    .unwrap();
    let dbg = format!("{b:?}");
    assert!(dbg.contains("<redacted>"), "{dbg}");
    assert!(!dbg.contains("supersecret"), "{dbg}");
}
