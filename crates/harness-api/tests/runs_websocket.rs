//! `WS /api/v1/runs/:task_id` integration tests (3.3-gossip / ADR-0019).
//!
//! Bind a real loopback socket per test; connect with
//! `tokio-tungstenite`; drive task transitions through the store and
//! assert the JSON frames + the terminal close. Also carries the
//! regression test for the `/tasks/:id` axum-0.7 path-param fix.
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::items_after_statements
)]

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use harness_api::{serve, ApiStateBuilder, AuthProvider};
use harness_core::{
    Constraints, ExecutionPolicy, Identity, ResourceHints, RetryPolicy, Signable, Signature, Task,
    TaskId, TraceContext,
};
use harness_mesh::AdminFile;
use harness_store::{Store, TaskState};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::Message;

fn empty_hints() -> ResourceHints {
    ResourceHints {
        cpu_class: harness_core::protocol::CpuClass::Light,
        memory_mb: None,
        gpu_required: false,
        gpu_memory_mb: None,
        network_class: harness_core::protocol::NetworkClass::None,
        disk_io_class: harness_core::protocol::DiskIoClass::None,
        estimated_duration_ms: None,
    }
}

fn insert_signed_task(store: &Store, identity: &Identity) -> TaskId {
    let mut task = Task {
        id: TaskId::new_v7(),
        parent: None,
        plan_id: None,
        capability: "echo".into(),
        input: serde_json::json!({"msg": "hi"}),
        constraints: Constraints::default(),
        retry: RetryPolicy::default(),
        execution: ExecutionPolicy::default(),
        resource_hints: empty_hints(),
        trace_ctx: TraceContext::default(),
        issued_by: identity.node_id(),
        issued_at: 1_700_000_000_000,
        tags: vec![],
        sig: Signature::from_bytes([0u8; 64]),
    };
    task.sign(identity).expect("sign");
    store.insert_task(&task).expect("insert");
    task.id
}

struct Fixture {
    identity: Arc<Identity>,
    store: Store,
    addr: std::net::SocketAddr,
    server: harness_api::ServerHandle,
}

async fn boot() -> Fixture {
    let identity = Arc::new(Identity::generate());
    let store = Store::open_memory().expect("store");
    let auth = Arc::new(AuthProvider::new(Some(
        AdminFile::from_password("hunter2").expect("hash"),
    )));
    let state = ApiStateBuilder::new(identity.clone(), "runs-ws-test")
        .with_auth(auth)
        .with_store(store.clone())
        .build();
    let server = serve("127.0.0.1:0".parse().unwrap(), state)
        .await
        .expect("bind");
    let addr = server.local_addr();
    Fixture {
        identity,
        store,
        addr,
        server,
    }
}

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn connect(addr: std::net::SocketAddr, task_id: TaskId) -> Ws {
    let url = format!("ws://{addr}/api/v1/runs/{}", task_id.0.as_hyphenated());
    let (ws, _resp) = tokio_tungstenite::connect_async(url)
        .await
        .expect("ws connect");
    ws
}

async fn next_json(ws: &mut Ws) -> serde_json::Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let remaining = deadline
            .checked_duration_since(tokio::time::Instant::now())
            .expect("ws frame timeout");
        let frame = tokio::time::timeout(remaining, ws.next())
            .await
            .expect("ws frame timeout")
            .expect("ws stream ended")
            .expect("ws frame ok");
        match frame {
            Message::Text(t) => return serde_json::from_str(&t).expect("json frame"),
            // Tungstenite surfaces pings transparently, but skip any
            // non-text control frame defensively.
            Message::Ping(_) | Message::Pong(_) => continue,
            other => panic!("expected text frame, got {other:?}"),
        }
    }
}

/// Drain until the server's close frame (or stream end). Panics if a
/// text frame arrives after the terminal event.
async fn expect_close(ws: &mut Ws) {
    let frame = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("close timeout");
    match frame {
        None | Some(Err(_) | Ok(Message::Close(_))) => {}
        Some(Ok(other)) => panic!("expected close after terminal frame, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_streams_states_to_done_with_output_then_closes() {
    let f = boot().await;
    let id = insert_signed_task(&f.store, &f.identity);
    let mut ws = connect(f.addr, id).await;

    // Connect-time state arrives without waiting for a transition.
    let first = next_json(&mut ws).await;
    assert_eq!(first["state"], "submitted");
    assert!(first.get("output").is_none(), "no output before terminal");
    assert!(first.get("error").is_none(), "no error before terminal");

    // Walk the row to Done + write the result.
    let node = f.identity.node_id();
    assert!(f.store.try_dispatch_task(id, node).expect("dispatch"));
    for (from, to) in [
        (TaskState::Dispatched, TaskState::Claimed),
        (TaskState::Claimed, TaskState::Running),
        (TaskState::Running, TaskState::Done),
    ] {
        assert!(f.store.try_transition_task(id, from, to).expect("hop"));
    }
    f.store
        .write_task_result_done(id, &serde_json::json!({"echoed": {"msg": "hi"}}), 42, node)
        .expect("result");

    // The 250 ms poll may batch the intermediate hops; the contract is
    // "every pushed frame is a state change, ending in the terminal
    // frame with output".
    let mut seen_states = vec![];
    let terminal = loop {
        let ev = next_json(&mut ws).await;
        let state = ev["state"].as_str().expect("state").to_string();
        seen_states.push(state.clone());
        if state == "done" {
            break ev;
        }
        assert!(
            matches!(state.as_str(), "dispatched" | "claimed" | "running"),
            "unexpected intermediate state {state}"
        );
    };
    assert_eq!(
        terminal["output"],
        serde_json::json!({"echoed": {"msg": "hi"}})
    );
    assert!(terminal.get("error").is_none());
    // No duplicate frames: every state appears at most once.
    let mut dedup = seen_states.clone();
    dedup.dedup();
    assert_eq!(seen_states, dedup, "frames must be pushed only on change");

    expect_close(&mut ws).await;
    f.server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_failed_task_streams_error_then_closes() {
    let f = boot().await;
    let id = insert_signed_task(&f.store, &f.identity);

    // Fail it BEFORE connecting: the very first frame is the terminal.
    let node = f.identity.node_id();
    f.store
        .try_transition_task(id, TaskState::Submitted, TaskState::Failed)
        .expect("fail hop");
    f.store
        .write_task_result_failed(id, "undispatchable: no eligible node", 42, node)
        .expect("result");

    let mut ws = connect(f.addr, id).await;
    let ev = next_json(&mut ws).await;
    assert_eq!(ev["state"], "failed");
    assert_eq!(ev["error"], "undispatchable: no eligible node");
    assert!(ev.get("output").is_none());
    expect_close(&mut ws).await;
    f.server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_unknown_task_rejected_before_upgrade() {
    let f = boot().await;
    let url = format!(
        "ws://{}/api/v1/runs/{}",
        f.addr,
        TaskId::new_v7().0.as_hyphenated()
    );
    let result = tokio_tungstenite::connect_async(url).await;
    assert!(result.is_err(), "unknown task id must refuse the upgrade");
    f.server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_invalid_task_id_rejected() {
    let f = boot().await;
    let url = format!("ws://{}/api/v1/runs/not-a-uuid", f.addr);
    let result = tokio_tungstenite::connect_async(url).await;
    assert!(result.is_err(), "malformed task id must refuse the upgrade");
    f.server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_origin_check_rejects_foreign_origin() {
    let f = boot().await;
    let id = insert_signed_task(&f.store, &f.identity);
    let url = format!("ws://{}/api/v1/runs/{}", f.addr, id.0.as_hyphenated());
    let mut req = url.as_str().into_client_request().expect("build req");
    req.headers_mut()
        .insert("Origin", "https://evil.example".parse().unwrap());
    let result = tokio_tungstenite::connect_async(req).await;
    assert!(result.is_err(), "foreign origin must be rejected");
    f.server.shutdown().await;
}

/// Regression: `/tasks/:id` (axum 0.7 syntax). The earlier `{id}`
/// spelling registered a literal path, so this GET always hit the API
/// 404 fallback. `harness run` polls this endpoint.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_task_by_id_over_http_resolves() {
    let f = boot().await;
    let id = insert_signed_task(&f.store, &f.identity);

    let client = tokio::net::TcpStream::connect(f.addr).await.expect("tcp");
    let (mut read_half, mut write_half) = client.into_split();
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Login for a bearer token first (the endpoint is authenticated).
    let login_body = r#"{"password":"hunter2"}"#;
    let login = format!(
        "POST /api/v1/auth/login HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n{}",
        f.addr,
        login_body.len(),
        login_body
    );
    write_half
        .write_all(login.as_bytes())
        .await
        .expect("write login");
    let mut buf = vec![0u8; 8192];
    let n = read_half.read(&mut buf).await.expect("read login");
    let login_resp = String::from_utf8_lossy(&buf[..n]).to_string();
    assert!(
        login_resp.starts_with("HTTP/1.1 200"),
        "login: {login_resp}"
    );
    let body_start = login_resp.find("\r\n\r\n").expect("header end") + 4;
    let token = serde_json::from_str::<serde_json::Value>(login_resp[body_start..].trim())
        .expect("login json")["token"]
        .as_str()
        .expect("token")
        .to_string();

    let get = format!(
        "GET /api/v1/tasks/{} HTTP/1.1\r\nHost: {}\r\nAuthorization: Bearer {}\r\nConnection: close\r\n\r\n",
        id.0.as_hyphenated(),
        f.addr,
        token
    );
    write_half
        .write_all(get.as_bytes())
        .await
        .expect("write get");
    let mut resp = String::new();
    read_half.read_to_string(&mut resp).await.expect("read get");
    assert!(
        resp.starts_with("HTTP/1.1 200"),
        "GET /api/v1/tasks/:id must resolve, got: {resp}"
    );
    assert!(resp.contains("\"state\":\"submitted\""), "body: {resp}");

    f.server.shutdown().await;
}
