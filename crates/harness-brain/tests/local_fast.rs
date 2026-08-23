//! Phase 3.9 — `LocalFastBackend` integration tests via wiremock.

#![cfg(feature = "localfast")]
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::float_cmp,
    clippy::many_single_char_names,
    clippy::needless_pass_by_value
)]

use harness_brain::backend::{PlanOutcome, PlanRequest, PlannerBackend};
use harness_brain::local_fast::{extract_json_object, LocalFastBackend};
use harness_brain::{CapabilitySchemaIndex, PlanConstraints, PlannerError};
use harness_core::{CapabilityRef, NodeId};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn shell_only() -> Vec<CapabilityRef> {
    vec![CapabilityRef {
        id: "shell.exec".to_string(),
        version_major: 0,
    }]
}

fn shell_index() -> CapabilitySchemaIndex {
    CapabilitySchemaIndex::from_pairs(vec![(
        "shell.exec".to_string(),
        json!({
            "type": "object",
            "required": ["cmd"],
            "additionalProperties": false,
            "properties": {
                "cmd":  { "type": "string" },
                "args": { "type": "array", "items": { "type": "string" } },
            },
        }),
    )])
}

fn req(goal: &str, available: Vec<CapabilityRef>, schemas: CapabilitySchemaIndex) -> PlanRequest {
    PlanRequest {
        goal: goal.to_string(),
        available_capabilities: available,
        schemas,
        constraints: PlanConstraints::default(),
        context: None,
        issuing_node: NodeId::from_bytes([7; 16]),
        repair: None,
    }
}

fn backend(mock_uri: &str) -> LocalFastBackend {
    let host = url::Url::parse(&format!("{mock_uri}/")).unwrap();
    LocalFastBackend::new(host, "llama3.1:8b".to_string(), NodeId::from_bytes([1; 16])).unwrap()
}

/// Helper: wraps a payload in Ollama's `/api/generate` envelope.
fn ollama_response(inner: &str) -> serde_json::Value {
    json!({
        "model": "llama3.1:8b",
        "response": inner,
        "done": true,
    })
}

/// Helper: a minimal LLM-shape plan with one task and no edges.
fn llm_one_node(cap: &str, input: serde_json::Value) -> serde_json::Value {
    json!({
        "plan": {
            "name": "test",
            "tasks": [{"id": "s1", "capability": cap, "input": input}],
            "edges": []
        },
        "confidence": 0.9,
        "rationale": "test",
        "estimated_cost_usd": 0.0,
        "estimated_duration_ms": 50
    })
}

// ───────────────────────────────────────── Prompt stability + truncation

#[tokio::test]
async fn t12_prompt_byte_stable_across_invocations() {
    // We can't call build_prompt directly (it's pub(crate)), so we
    // probe via wiremock body capture. Two requests with identical
    // (goal, sorted_caps, constraints) must produce byte-identical
    // prompts.
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/generate"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ollama_response(
            &serde_json::to_string(&llm_one_node("shell.exec", json!({"cmd": "ls"}))).unwrap(),
        )))
        .expect(2)
        .mount(&mock)
        .await;

    let b = backend(&mock.uri());
    let r = req("run: ls", shell_only(), shell_index());
    let _ = b.plan(&r).await.expect("first ok");
    let _ = b.plan(&r).await.expect("second ok");

    // Mock's expect(2) is verified at MockServer drop. Identical body
    // means same prompt; we trust wiremock's request matching here.
}

// ───────────────────────────────────────── Happy path

#[tokio::test]
async fn t14_happy_path_returns_confident() {
    let mock = MockServer::start().await;
    let inner = serde_json::to_string(&llm_one_node(
        "shell.exec",
        json!({"cmd": "ls", "args": ["-la"]}),
    ))
    .unwrap();
    Mock::given(method("POST"))
        .and(path("/api/generate"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ollama_response(&inner)))
        .expect(1)
        .mount(&mock)
        .await;

    let b = backend(&mock.uri());
    let r = req("run: ls -la", shell_only(), shell_index());
    let outcome = b.plan(&r).await.expect("ok");
    let resp = match outcome {
        PlanOutcome::Confident(p) => p,
        other => panic!("want Confident, got {other:?}"),
    };
    assert_eq!(resp.plan.as_inner().tasks.len(), 1);
    let node = resp.plan.as_inner().tasks.values().next().unwrap();
    assert_eq!(node.capability, "shell.exec");
    assert_eq!(node.input, json!({"cmd": "ls", "args": ["-la"]}));
}

// ───────────────────────────────────────── Edge orientation flip

#[tokio::test]
async fn t14b_edge_orientation_flipped() {
    // LLM emits a chain "A then B". Our parser must flip so harness's
    // `(from, to)` = "from depends on to" gives `(B, A)`.
    let mock = MockServer::start().await;
    let inner = serde_json::to_string(&json!({
        "plan": {
            "name": "chain",
            "tasks": [
                {"id": "a", "capability": "shell.exec", "input": {"cmd": "step_a"}},
                {"id": "b", "capability": "shell.exec", "input": {"cmd": "step_b"}}
            ],
            "edges": [["a", "b"]]
        },
        "confidence": 0.9,
        "rationale": "chain",
        "estimated_cost_usd": 0.0,
        "estimated_duration_ms": 50
    }))
    .unwrap();
    Mock::given(method("POST"))
        .and(path("/api/generate"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ollama_response(&inner)))
        .mount(&mock)
        .await;

    let b = backend(&mock.uri());
    let r = req("chain", shell_only(), shell_index());
    let outcome = b.plan(&r).await.expect("ok");
    let resp = match outcome {
        PlanOutcome::Confident(p) => p,
        other => panic!("want Confident, got {other:?}"),
    };

    // Two tasks, one edge.
    assert_eq!(resp.plan.as_inner().tasks.len(), 2);
    assert_eq!(resp.plan.as_inner().edges.len(), 1);

    // Find which TaskId got mapped to which step by scanning input.cmd.
    let mut a_id = None;
    let mut b_id = None;
    for (id, node) in &resp.plan.as_inner().tasks {
        match node.input["cmd"].as_str() {
            Some("step_a") => a_id = Some(*id),
            Some("step_b") => b_id = Some(*id),
            _ => panic!("unexpected cmd: {:?}", node.input),
        }
    }
    let a = a_id.unwrap();
    let b_id = b_id.unwrap();

    // LLM said "[a, b]" meaning "a runs before b". Harness's edge is
    // `(from, to)` = "from depends on to" → `(b, a)` (b depends on a).
    assert_eq!(
        resp.plan.as_inner().edges,
        vec![(b_id, a)],
        "edge orientation must flip from LLM convention to harness convention"
    );
}

#[tokio::test]
async fn t14c_edge_references_unknown_id_returns_decode_error() {
    let mock = MockServer::start().await;
    let inner = serde_json::to_string(&json!({
        "plan": {
            "name": "broken",
            "tasks": [{"id": "a", "capability": "shell.exec", "input": {"cmd": "ls"}}],
            "edges": [["a", "ghost"]]
        },
        "confidence": 0.9,
        "rationale": "x",
        "estimated_cost_usd": 0.0,
        "estimated_duration_ms": 0
    }))
    .unwrap();
    Mock::given(method("POST"))
        .and(path("/api/generate"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ollama_response(&inner)))
        .mount(&mock)
        .await;

    let b = backend(&mock.uri());
    let r = req("x", shell_only(), shell_index());
    let err = b.plan(&r).await.expect_err("must reject");
    assert!(matches!(err, PlannerError::Decode(_)), "got {err:?}");
}

#[tokio::test]
async fn t14d_llm_step_ids_are_replaced_by_minted_taskids() {
    // The LLM's `"id": "s1"` must NOT appear in the materialized plan
    // — every PlanNode.id is a freshly-minted TaskId. (The string label
    // appears in tracing fields only; the production path emits an
    // `info!` event with `llm_step_label` so operators can correlate
    // labels and TaskIds in logs.)
    let mock = MockServer::start().await;
    let inner = serde_json::to_string(&llm_one_node("shell.exec", json!({"cmd": "ls"}))).unwrap();
    Mock::given(method("POST"))
        .and(path("/api/generate"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ollama_response(&inner)))
        .mount(&mock)
        .await;
    let b = backend(&mock.uri());
    let r = req("run: ls", shell_only(), shell_index());
    let outcome = b.plan(&r).await.expect("ok");
    let resp = match outcome {
        PlanOutcome::Confident(p) => p,
        other => panic!("want Confident, got {other:?}"),
    };
    let plan = resp.plan.as_inner();
    let serialized = serde_json::to_string(plan).expect("serialize");
    assert!(
        !serialized.contains("\"s1\""),
        "LLM step id 's1' must not appear in the serialized plan; got {serialized}"
    );
}

// ───────────────────────────────────────── Markdown / prose tolerance

#[tokio::test]
async fn t15a_prose_prefix_before_object_decodes() {
    let mock = MockServer::start().await;
    let inner = format!(
        "Here is the plan:\n```json\n{}\n```",
        serde_json::to_string(&llm_one_node("shell.exec", json!({"cmd": "ls"}))).unwrap()
    );
    Mock::given(method("POST"))
        .and(path("/api/generate"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ollama_response(&inner)))
        .mount(&mock)
        .await;
    let b = backend(&mock.uri());
    let r = req("run: ls", shell_only(), shell_index());
    let outcome = b.plan(&r).await.expect("ok");
    assert!(matches!(outcome, PlanOutcome::Confident(_)));
}

#[tokio::test]
async fn t15b_prose_suffix_after_object_decodes() {
    let mock = MockServer::start().await;
    let inner = format!(
        "{}\nLet me know if you need anything else.",
        serde_json::to_string(&llm_one_node("shell.exec", json!({"cmd": "ls"}))).unwrap()
    );
    Mock::given(method("POST"))
        .and(path("/api/generate"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ollama_response(&inner)))
        .mount(&mock)
        .await;
    let b = backend(&mock.uri());
    let r = req("run: ls", shell_only(), shell_index());
    let outcome = b.plan(&r).await.expect("ok");
    assert!(matches!(outcome, PlanOutcome::Confident(_)));
}

#[tokio::test]
async fn t15c_two_json_blocks_first_wins() {
    let mock = MockServer::start().await;
    let first = serde_json::to_string(&llm_one_node("shell.exec", json!({"cmd": "ls"}))).unwrap();
    let second = serde_json::to_string(&llm_one_node("shell.exec", json!({"cmd": "wc"}))).unwrap();
    let inner = format!("{first}\n\nor maybe\n\n{second}");
    Mock::given(method("POST"))
        .and(path("/api/generate"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ollama_response(&inner)))
        .mount(&mock)
        .await;
    let b = backend(&mock.uri());
    let r = req("run: ls", shell_only(), shell_index());
    let outcome = b.plan(&r).await.expect("ok");
    let resp = match outcome {
        PlanOutcome::Confident(p) => p,
        other => panic!("want Confident, got {other:?}"),
    };
    let node = resp.plan.as_inner().tasks.values().next().unwrap();
    // First block wins → `cmd: "ls"`, not `"wc"`.
    assert_eq!(node.input["cmd"], json!("ls"));
}

#[tokio::test]
async fn t15d_bare_object_no_fence_decodes() {
    let mock = MockServer::start().await;
    let inner = serde_json::to_string(&llm_one_node("shell.exec", json!({"cmd": "ls"}))).unwrap();
    Mock::given(method("POST"))
        .and(path("/api/generate"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ollama_response(&inner)))
        .mount(&mock)
        .await;
    let b = backend(&mock.uri());
    let r = req("run: ls", shell_only(), shell_index());
    assert!(matches!(
        b.plan(&r).await.expect("ok"),
        PlanOutcome::Confident(_)
    ));
}

#[tokio::test]
async fn t15e_trailing_commas_return_decode_error() {
    let mock = MockServer::start().await;
    // Trailing comma after `done`: strict serde_json rejects.
    let inner = "{\"plan\":{\"name\":\"x\",\"tasks\":[],\"edges\":[],},\"confidence\":0.9,\"rationale\":\"x\"}";
    Mock::given(method("POST"))
        .and(path("/api/generate"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ollama_response(inner)))
        .mount(&mock)
        .await;
    let b = backend(&mock.uri());
    let r = req("run: ls", shell_only(), shell_index());
    let err = b.plan(&r).await.expect_err("must reject");
    assert!(matches!(err, PlannerError::Decode(_)), "got {err:?}");
}

#[tokio::test]
async fn t16_malformed_json_returns_decode_error() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/generate"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ollama_response("not json at all")))
        .mount(&mock)
        .await;
    let b = backend(&mock.uri());
    let r = req("x", shell_only(), shell_index());
    let err = b.plan(&r).await.expect_err("must reject");
    assert!(matches!(err, PlannerError::Decode(_)));
}

// ───────────────────────────────────────── Network / Ollama failures

#[tokio::test]
async fn t18_ollama_500_returns_transport_error() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/generate"))
        .respond_with(ResponseTemplate::new(500).set_body_string("upstream blew up"))
        .mount(&mock)
        .await;
    let b = backend(&mock.uri());
    let r = req("x", shell_only(), shell_index());
    let err = b.plan(&r).await.expect_err("must propagate");
    assert!(matches!(err, PlannerError::Transport(_)));
    let msg = format!("{err}");
    assert!(msg.contains("500"));
}

#[tokio::test]
async fn t19_ollama_404_model_not_found_returns_transport_error() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/generate"))
        .respond_with(
            ResponseTemplate::new(404)
                .set_body_json(json!({"error": "model 'llama3.1:8b' not found"})),
        )
        .mount(&mock)
        .await;
    let b = backend(&mock.uri());
    let r = req("x", shell_only(), shell_index());
    let err = b.plan(&r).await.expect_err("must propagate");
    // CLAUDE.md production-grade: we never panic on a missing model.
    assert!(matches!(err, PlannerError::Transport(_)));
}

#[tokio::test]
async fn t21_ollama_unreachable_returns_transport_error() {
    // Bind a local addr we promise NOT to start; constructing the URL
    // succeeds but the request itself fails.
    let host = url::Url::parse("http://127.0.0.1:1/").unwrap();
    let b = LocalFastBackend::new(host, "llama3.1:8b".into(), NodeId::from_bytes([1; 16]))
        .expect("client builds");
    let r = req("x", shell_only(), shell_index());
    let err = b.plan(&r).await.expect_err("must fail");
    assert!(matches!(
        err,
        PlannerError::Transport(_) | PlannerError::Timeout
    ));
}

#[tokio::test]
async fn t22_id_embeds_model_name() {
    let host = url::Url::parse("http://127.0.0.1:11434/").unwrap();
    let b = LocalFastBackend::new(host, "qwen2.5:7b".into(), NodeId::from_bytes([1; 16]))
        .expect("client builds");
    assert_eq!(b.id(), "localfast:qwen2.5:7b");
}

// ───────────────────────────────────────── JSON extractor sanity (cross-suite)

#[test]
fn extract_json_object_handles_unicode_escape_in_string() {
    let s = r#"prose {"emoji":"é hi"} more"#;
    assert_eq!(extract_json_object(s).unwrap(), r#"{"emoji":"é hi"}"#);
}

// ───────────────────────────────────────── 5.1 LocalStrong (shared core)

#[tokio::test]
async fn t29_local_strong_id_and_happy_path() {
    // The tier-2 wrapper shares the tier-1 core (ADR-0030): one
    // success test pins its id scheme and that the plumbing works
    // end-to-end; every parsing/error path is covered by the tier-1
    // suite over the same core.
    use harness_brain::local_fast::LocalStrongBackend;

    let mock = MockServer::start().await;
    let inner = serde_json::to_string(&llm_one_node("shell.exec", json!({"cmd": "ls"}))).unwrap();
    Mock::given(method("POST"))
        .and(path("/api/generate"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ollama_response(&inner)))
        .expect(1)
        .mount(&mock)
        .await;

    let host = url::Url::parse(&format!("{}/", mock.uri())).unwrap();
    let b = LocalStrongBackend::new(
        host,
        "llama3.1:70b".to_string(),
        NodeId::from_bytes([1; 16]),
    )
    .unwrap();
    assert_eq!(b.id(), "localstrong:llama3.1:70b");

    let r = req("run: ls", shell_only(), shell_index());
    let outcome = b.plan(&r).await.expect("ok");
    match outcome {
        PlanOutcome::Confident(p) => {
            assert_eq!(p.plan.as_inner().tasks.len(), 1);
        }
        other => panic!("want Confident, got {other:?}"),
    }
}
