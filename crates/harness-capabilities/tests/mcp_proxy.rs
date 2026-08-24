//! Integration tests for 3.7 `mcp.proxy` (`mcp.<server>.<tool>`).
//!
//! Uses a dependency-free Python mock MCP server (stdio JSON-RPC,
//! newline-delimited) written to a tempdir per test — enough protocol
//! for `initialize` / `tools/list` / `tools/call`. Unix-gated like
//! `shell.exec`'s tests (CI runs ubuntu + macos; both ship python3).

#![cfg(all(feature = "mcp", unix))]
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::needless_raw_string_hashes,
    clippy::items_after_statements
)]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use harness_capabilities::traits::{CapabilityError, ExecutionContext};
use harness_capabilities::{
    enrich_with_mcp, enrich_with_mcp_from_path, CapabilityRegistry, McpConfig, McpConfigError,
};
use harness_core::{Cardinality, NodeId, TaskId};
use harness_policy::{load_from_str, Policy, PolicyEngine};
use serde_json::json;

/// Mock MCP server: newline-delimited JSON-RPC over stdio. Tools:
/// `add` (returns a+b), `boom` (isError result), `die` (exits the
/// process). Set `MOCK_MCP_CALL_LOG` to append each tools/call name
/// to a file (used to prove policy denies never reach the server).
const MOCK_SERVER_PY: &str = r#"
import json, os, sys

CALL_LOG = os.environ.get("MOCK_MCP_CALL_LOG")

def send(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()

TOOLS = [
    {
        "name": "add",
        "description": "Add two numbers",
        "inputSchema": {
            "type": "object",
            "properties": {"a": {"type": "number"}, "b": {"type": "number"}},
            "required": ["a", "b"],
        },
    },
    {"name": "boom", "description": "Always fails", "inputSchema": {"type": "object"}},
    {"name": "die", "description": "Kills the server", "inputSchema": {"type": "object"}},
    {"name": "bad name!", "description": "Unregisterable", "inputSchema": {"type": "object"}},
]

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    msg = json.loads(line)
    method = msg.get("method")
    mid = msg.get("id")
    if method == "initialize":
        send({"jsonrpc": "2.0", "id": mid, "result": {
            "protocolVersion": msg["params"]["protocolVersion"],
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "mock-mcp", "version": "0.0.1"},
        }})
    elif method == "notifications/initialized":
        pass
    elif method == "tools/list":
        send({"jsonrpc": "2.0", "id": mid, "result": {"tools": TOOLS}})
    elif method == "tools/call":
        params = msg.get("params") or {}
        name = params.get("name")
        args = params.get("arguments") or {}
        if CALL_LOG:
            with open(CALL_LOG, "a") as f:
                f.write(str(name) + "\n")
        if name == "add":
            total = args.get("a", 0) + args.get("b", 0)
            send({"jsonrpc": "2.0", "id": mid, "result": {
                "content": [{"type": "text", "text": str(total)}],
                "structuredContent": {"sum": total},
                "isError": False,
            }})
        elif name == "boom":
            send({"jsonrpc": "2.0", "id": mid, "result": {
                "content": [{"type": "text", "text": "kaboom: the mock exploded"}],
                "isError": True,
            }})
        elif name == "die":
            os._exit(7)
        else:
            send({"jsonrpc": "2.0", "id": mid,
                  "error": {"code": -32601, "message": "unknown tool " + str(name)}})
    elif mid is not None:
        send({"jsonrpc": "2.0", "id": mid,
              "error": {"code": -32601, "message": "method not found: " + str(method)}})
"#;

fn write_mock_server(dir: &Path) -> PathBuf {
    let path = dir.join("mock_mcp_server.py");
    std::fs::write(&path, MOCK_SERVER_PY).expect("write mock server");
    path
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
        audit: None,
    }
}

fn allow_all_mcp_policy() -> Arc<PolicyEngine> {
    let p = load_from_str(
        r#"
[mcp]
allow = [{ server = "mock" }]
"#,
    )
    .expect("policy parse");
    Arc::new(PolicyEngine::new(p))
}

fn mock_config(script: &Path, env: &[(&str, &str)]) -> McpConfig {
    let mut toml = format!(
        "[[server]]\nname = \"mock\"\ncommand = \"python3\"\nargs = [{script:?}]\n",
        script = script.display()
    );
    if !env.is_empty() {
        toml.push_str("[server.env]\n");
        for (k, v) in env {
            toml.push_str(&format!("{k} = {v:?}\n"));
        }
    }
    McpConfig::parse(&toml).expect("mock config parse")
}

/// Registry preloaded with the mock server's tools.
async fn registry_with_mock(
    script: &Path,
    policy: Arc<PolicyEngine>,
    env: &[(&str, &str)],
) -> CapabilityRegistry {
    let registry = CapabilityRegistry::new();
    enrich_with_mcp(&registry, policy, &mock_config(script, env)).await;
    registry
}

// ───────────────────────────────────────── Discovery + manifest

#[tokio::test]
async fn t01_discovery_registers_tools_with_schema() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let script = write_mock_server(tmp.path());
    let registry = registry_with_mock(&script, allow_all_mcp_policy(), &[]).await;

    let mut ids = registry.ids();
    ids.sort();
    assert_eq!(ids, vec!["mcp.mock.add", "mcp.mock.boom", "mcp.mock.die"]);

    let add = registry.get("mcp.mock.add").expect("mcp.mock.add");
    let manifest = add.manifest();
    assert_eq!(manifest.id, "mcp.mock.add");
    assert_eq!(manifest.cardinality, Cardinality::Anyone);
    assert_eq!(manifest.tags, vec!["mcp".to_string()]);
    // input_schema is the MCP tool's inputSchema, verbatim.
    assert_eq!(
        manifest.input_schema,
        json!({
            "type": "object",
            "properties": {"a": {"type": "number"}, "b": {"type": "number"}},
            "required": ["a", "b"],
        })
    );
}

#[tokio::test]
async fn t02_tool_with_unregisterable_name_is_skipped() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let script = write_mock_server(tmp.path());
    let registry = registry_with_mock(&script, allow_all_mcp_policy(), &[]).await;
    // "bad name!" contains a space + '!' → skipped, everything else
    // still registered (see t01 for the full id list).
    assert!(!registry.ids().iter().any(|id| id.contains("bad")));
    assert_eq!(registry.ids().len(), 3);
}

// ───────────────────────────────────────── Call round-trips

#[tokio::test]
async fn t03_call_round_trip_returns_tool_output() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let script = write_mock_server(tmp.path());
    let registry = registry_with_mock(&script, allow_all_mcp_policy(), &[]).await;

    let add = registry.get("mcp.mock.add").expect("mcp.mock.add");
    let out = add
        .execute(&ctx(), json!({"a": 1, "b": 2}))
        .await
        .expect("add must succeed");

    // Pass-through MCP result shape (ADR-0018).
    assert_eq!(out["structuredContent"], json!({"sum": 3}));
    assert_eq!(out["content"][0]["type"], "text");
    assert_eq!(out["content"][0]["text"], "3");
    assert_ne!(out["isError"], json!(true));
}

#[tokio::test]
async fn t04_is_error_result_maps_to_capability_error() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let script = write_mock_server(tmp.path());
    let registry = registry_with_mock(&script, allow_all_mcp_policy(), &[]).await;

    let boom = registry.get("mcp.mock.boom").expect("mcp.mock.boom");
    let err = boom
        .execute(&ctx(), json!({}))
        .await
        .expect_err("boom must fail");
    let CapabilityError::Failed(msg) = err else {
        panic!("expected Failed, got {err:?}");
    };
    assert!(msg.contains("mock/boom"), "{msg}");
    assert!(msg.contains("kaboom: the mock exploded"), "{msg}");
}

#[tokio::test]
async fn t05_non_object_input_is_invalid() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let script = write_mock_server(tmp.path());
    let registry = registry_with_mock(&script, allow_all_mcp_policy(), &[]).await;

    let add = registry.get("mcp.mock.add").expect("mcp.mock.add");
    let err = add
        .execute(&ctx(), json!([1, 2]))
        .await
        .expect_err("array input must be rejected");
    assert!(matches!(err, CapabilityError::InvalidInput(_)), "{err:?}");
}

// ───────────────────────────────────────── Dead server

#[tokio::test]
async fn t06_dead_server_calls_error_cleanly() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let script = write_mock_server(tmp.path());
    let registry = registry_with_mock(&script, allow_all_mcp_policy(), &[]).await;

    let die = registry.get("mcp.mock.die").expect("mcp.mock.die");
    let add = registry.get("mcp.mock.add").expect("mcp.mock.add");

    // First call kills the subprocess mid-request → clean Failed.
    let err = die
        .execute(&ctx(), json!({}))
        .await
        .expect_err("die must fail");
    let CapabilityError::Failed(msg) = err else {
        panic!("expected Failed, got {err:?}");
    };
    assert!(msg.contains("mock"), "{msg}");

    // Subsequent calls on the same (now dead) server also fail with a
    // clear error — no auto-restart in 3.7 (ADR-0018). The transport
    // teardown races the executor, so accept either the fast-path
    // "not running" message or a transport-level failure.
    let err = add
        .execute(&ctx(), json!({"a": 1, "b": 1}))
        .await
        .expect_err("post-death call must fail");
    let CapabilityError::Failed(msg) = err else {
        panic!("expected Failed, got {err:?}");
    };
    assert!(
        msg.contains("not running") || msg.contains("call failed"),
        "unexpected message: {msg}"
    );
}

// ───────────────────────────────────────── Policy gate

#[tokio::test]
async fn t07_policy_deny_blocks_before_subprocess_sees_the_call() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let script = write_mock_server(tmp.path());
    let call_log = tmp.path().join("calls.log");
    let call_log_str = call_log.display().to_string();

    // Deny-all policy (no [mcp] section) but the server itself runs —
    // discovery (initialize + tools/list) is not a tool call and is
    // not policy-gated.
    let deny_all = Arc::new(PolicyEngine::new(Policy::deny_all()));
    let registry = registry_with_mock(
        &script,
        deny_all,
        &[("MOCK_MCP_CALL_LOG", call_log_str.as_str())],
    )
    .await;

    let add = registry.get("mcp.mock.add").expect("mcp.mock.add");
    let err = add
        .execute(&ctx(), json!({"a": 1, "b": 2}))
        .await
        .expect_err("must be denied");
    let CapabilityError::Failed(msg) = err else {
        panic!("expected Failed, got {err:?}");
    };
    assert!(msg.contains("policy denied"), "{msg}");
    assert!(
        msg.contains("no [mcp].allow rule matched mock/add"),
        "{msg}"
    );

    // The mock appends every tools/call to MOCK_MCP_CALL_LOG. A denied
    // call must never reach the subprocess.
    assert!(
        !call_log.exists(),
        "denied call reached the MCP server: {:?}",
        std::fs::read_to_string(&call_log)
    );
}

#[tokio::test]
async fn t08_policy_allow_reaches_subprocess() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let script = write_mock_server(tmp.path());
    let call_log = tmp.path().join("calls.log");
    let call_log_str = call_log.display().to_string();

    let registry = registry_with_mock(
        &script,
        allow_all_mcp_policy(),
        &[("MOCK_MCP_CALL_LOG", call_log_str.as_str())],
    )
    .await;

    let add = registry.get("mcp.mock.add").expect("mcp.mock.add");
    add.execute(&ctx(), json!({"a": 2, "b": 3}))
        .await
        .expect("allowed call succeeds");
    let log = std::fs::read_to_string(&call_log).expect("call log written");
    assert_eq!(log.trim(), "add");
}

// ───────────────────────────────────────── Config loading

#[tokio::test]
async fn t09_missing_config_registers_nothing() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let registry = CapabilityRegistry::new();
    enrich_with_mcp_from_path(
        &registry,
        allow_all_mcp_policy(),
        &tmp.path().join("mcp.toml"),
    )
    .await
    .expect("missing mcp.toml is fine");
    assert!(registry.ids().is_empty());
}

#[tokio::test]
async fn t10_config_parse_error_is_fatal() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let path = tmp.path().join("mcp.toml");
    std::fs::write(&path, "[[server]\nname = broken").expect("write");
    let registry = CapabilityRegistry::new();
    let err = enrich_with_mcp_from_path(&registry, allow_all_mcp_policy(), &path)
        .await
        .expect_err("parse error must surface");
    assert!(matches!(err, McpConfigError::Parse { .. }), "{err:?}");
    assert!(registry.ids().is_empty());
}

#[tokio::test]
async fn t11_invalid_server_name_is_fatal() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let path = tmp.path().join("mcp.toml");
    std::fs::write(
        &path,
        "[[server]]\nname = \"Bad Name\"\ncommand = \"true\"\n",
    )
    .expect("write");
    let registry = CapabilityRegistry::new();
    let err = enrich_with_mcp_from_path(&registry, allow_all_mcp_policy(), &path)
        .await
        .expect_err("invalid server name must surface");
    assert!(matches!(err, McpConfigError::InvalidName(_)), "{err:?}");
}

#[tokio::test]
async fn t12_unstartable_server_is_skipped_not_fatal() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let path = tmp.path().join("mcp.toml");
    // Command that does not exist: spawn fails, server skipped,
    // enrich still succeeds (daemon must boot).
    std::fs::write(
        &path,
        "[[server]]\nname = \"ghost\"\ncommand = \"/nonexistent/definitely-not-a-binary\"\n",
    )
    .expect("write");
    let registry = CapabilityRegistry::new();
    enrich_with_mcp_from_path(&registry, allow_all_mcp_policy(), &path)
        .await
        .expect("unstartable server is best-effort");
    assert!(registry.ids().is_empty());
}
