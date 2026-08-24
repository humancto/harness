//! Phase 3.5 — `LlmBatcher` end-to-end tests via wiremock.
//!
//! Pinpoints the four behaviors that matter:
//! 1. Identical requests within window coalesce into one backend call.
//! 2. Different prompts dispatch separately.
//! 3. Outside-window calls dispatch separately.
//! 4. First-caller cancellation does not strand siblings (B4).

#![cfg(feature = "llm")]
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::needless_raw_string_hashes
)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use harness_capabilities::traits::{Capability, ExecutionContext};
use harness_capabilities::LlmLocalCapability;
use harness_capabilities::{Fingerprint, LlmBatcher};
use harness_core::{NodeId, TaskId};
use harness_policy::{Policy, PolicyEngine};
use serde_json::json;
use url::Url;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const WINDOW_MS: u64 = 200;

fn ctx_with_tags(tags: Vec<String>) -> ExecutionContext {
    ExecutionContext {
        local_node: NodeId::from_bytes([1; 16]),
        local_node_name: Arc::from("self"),
        issued_by: NodeId::from_bytes([2; 16]),
        issued_by_name: Arc::from("issuer"),
        task_id: TaskId::new_v7(),
        tags: Arc::from(tags),
        frame_sink: None,
        audit: None,
    }
}

fn engine_allow_all() -> Arc<PolicyEngine> {
    Arc::new(PolicyEngine::new(Policy::default()))
}

fn make_cap(model: &str, host: &Url, batcher: Arc<LlmBatcher>) -> LlmLocalCapability {
    LlmLocalCapability::new(
        engine_allow_all(),
        model.to_string(),
        host.clone(),
        reqwest::Client::new(),
        batcher,
    )
}

// ─────────────────────────────────────── Coalescing

#[tokio::test]
async fn t01_two_identical_requests_in_window_coalesce() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/generate"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "model": "m",
            "response": "hello",
            "done": true,
        })))
        .expect(1) // critical: exactly one backend call
        .mount(&server)
        .await;

    let host = Url::parse(&server.uri()).expect("parse");
    let batcher = Arc::new(LlmBatcher::with_window(Duration::from_millis(WINDOW_MS)));
    let cap = make_cap("m", &host, batcher);

    let ctx_a = ctx_with_tags(vec![]);
    let ctx_b = ctx_with_tags(vec![]);
    let (a, b) = tokio::join!(
        cap.execute(&ctx_a, json!({"prompt": "hi"})),
        cap.execute(&ctx_b, json!({"prompt": "hi"})),
    );
    let a = a.expect("a");
    let b = b.expect("b");
    assert_eq!(a["text"].as_str().unwrap(), "hello");
    assert_eq!(b["text"].as_str().unwrap(), "hello");
    // wiremock asserts `.expect(1)` on drop.
}

#[tokio::test]
async fn t02_two_different_prompts_dispatch_separately() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/generate"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "model": "m",
            "response": "ok",
            "done": true,
        })))
        .expect(2) // two distinct prompts → two calls
        .mount(&server)
        .await;

    let host = Url::parse(&server.uri()).expect("parse");
    let batcher = Arc::new(LlmBatcher::with_window(Duration::from_millis(WINDOW_MS)));
    let cap = make_cap("m", &host, batcher);

    let ctx_a = ctx_with_tags(vec![]);
    let ctx_b = ctx_with_tags(vec![]);
    let (a, b) = tokio::join!(
        cap.execute(&ctx_a, json!({"prompt": "first"})),
        cap.execute(&ctx_b, json!({"prompt": "second"})),
    );
    a.expect("a");
    b.expect("b");
}

#[tokio::test]
async fn t03_outside_window_dispatches_separately() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/generate"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "model": "m",
            "response": "ok",
            "done": true,
        })))
        .expect(2)
        .mount(&server)
        .await;

    let host = Url::parse(&server.uri()).expect("parse");
    let batcher = Arc::new(LlmBatcher::with_window(Duration::from_millis(WINDOW_MS)));
    let cap = make_cap("m", &host, batcher);

    let ctx_a = ctx_with_tags(vec![]);
    cap.execute(&ctx_a, json!({"prompt": "hi"}))
        .await
        .expect("a");
    // Wait past the window so the next call starts fresh.
    tokio::time::sleep(Duration::from_millis(WINDOW_MS + 50)).await;
    cap.execute(&ctx_a, json!({"prompt": "hi"}))
        .await
        .expect("b");
}

// ─────────────────────────────────────── Bypass

#[tokio::test]
async fn t04_interactive_tag_bypasses_batcher() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/generate"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "model": "m",
            "response": "ok",
            "done": true,
        })))
        .expect(2) // tag bypasses batcher → two calls even for identical prompt
        .mount(&server)
        .await;

    let host = Url::parse(&server.uri()).expect("parse");
    let batcher = Arc::new(LlmBatcher::with_window(Duration::from_millis(WINDOW_MS)));
    let cap = make_cap("m", &host, batcher);

    let ctx = ctx_with_tags(vec!["interactive".to_string()]);
    let (a, b) = tokio::join!(
        cap.execute(&ctx, json!({"prompt": "hi"})),
        cap.execute(&ctx, json!({"prompt": "hi"})),
    );
    a.expect("a");
    b.expect("b");
}

#[tokio::test]
async fn t05_zero_window_bypasses_batcher() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/generate"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "model": "m",
            "response": "ok",
            "done": true,
        })))
        .expect(2)
        .mount(&server)
        .await;

    let host = Url::parse(&server.uri()).expect("parse");
    // Zero window → batcher disabled.
    let batcher = Arc::new(LlmBatcher::with_window(Duration::ZERO));
    let cap = make_cap("m", &host, batcher);

    let ctx = ctx_with_tags(vec![]);
    let (a, b) = tokio::join!(
        cap.execute(&ctx, json!({"prompt": "hi"})),
        cap.execute(&ctx, json!({"prompt": "hi"})),
    );
    a.expect("a");
    b.expect("b");
}

// ─────────────────────────────────────── Error fanout

#[tokio::test]
async fn t06_error_fans_out_to_all_senders() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/generate"))
        .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
        .mount(&server)
        .await;

    let host = Url::parse(&server.uri()).expect("parse");
    let batcher = Arc::new(LlmBatcher::with_window(Duration::from_millis(WINDOW_MS)));
    let cap = make_cap("m", &host, batcher);

    let ctx = ctx_with_tags(vec![]);
    let (a, b) = tokio::join!(
        cap.execute(&ctx, json!({"prompt": "hi"})),
        cap.execute(&ctx, json!({"prompt": "hi"})),
    );
    let err_a = a.expect_err("a must fail");
    let err_b = b.expect_err("b must fail");
    let (msg_a, msg_b) = (err_a.to_string(), err_b.to_string());
    assert!(msg_a.contains("ollama returned"));
    assert!(msg_b.contains("ollama returned"));
}

// ─────────────────────────────────────── Cancellation safety (B4)

#[tokio::test]
async fn t07_first_caller_cancellation_does_not_strand_siblings() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/generate"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "model": "m",
            "response": "alive",
            "done": true,
        })))
        .expect(1) // first caller cancels; sibling still triggers exactly 1 backend call
        .mount(&server)
        .await;

    let host = Url::parse(&server.uri()).expect("parse");
    let batcher = Arc::new(LlmBatcher::with_window(Duration::from_millis(WINDOW_MS)));
    let cap = Arc::new(make_cap("m", &host, batcher));

    let ctx = ctx_with_tags(vec![]);

    // Caller A submits, then is dropped via abort.
    let cap_a = cap.clone();
    let ctx_a = ctx.clone();
    let handle_a =
        tokio::spawn(async move { cap_a.execute(&ctx_a, json!({"prompt": "shared"})).await });
    // Tiny pause so A's submit registers in the batcher.
    tokio::time::sleep(Duration::from_millis(20)).await;
    handle_a.abort();

    // Caller B submits same fingerprint, within the window. B must
    // still receive a result — the spawned timer task survives A's
    // cancellation.
    let res_b = cap
        .execute(&ctx, json!({"prompt": "shared"}))
        .await
        .expect("B must receive a result");
    assert_eq!(res_b["text"].as_str().unwrap(), "alive");
}

// ─────────────────────────────────────── Direct batcher unit (no HTTP)

#[tokio::test]
async fn t08_direct_batcher_coalesces_dispatch_calls() {
    let batcher = Arc::new(LlmBatcher::with_window(Duration::from_millis(WINDOW_MS)));
    let counter = Arc::new(AtomicUsize::new(0));

    let fp = Fingerprint([42u8; 32]);
    let dispatch = {
        let counter = counter.clone();
        move || {
            let counter = counter.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(serde_json::json!({"text": "ok"}))
            }
        }
    };

    let b1 = batcher.clone();
    let d1 = dispatch.clone();
    let b2 = batcher.clone();
    let d2 = dispatch.clone();
    let (r1, r2) = tokio::join!(async move { b1.submit(fp, d1).await }, async move {
        b2.submit(fp, d2).await
    },);
    r1.expect("r1");
    r2.expect("r2");
    assert_eq!(
        counter.load(Ordering::SeqCst),
        1,
        "dispatch closure must be invoked exactly once for two coalesced submits"
    );
}
