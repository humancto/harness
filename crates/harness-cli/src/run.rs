//! `harness run` — fleet exec (PRD §16.1). Phase 3.3.
//!
//! Targets:
//! - default / `--on self` — run on the local daemon's node.
//! - `--on <node>` — run on one named mesh node (name or node-id
//!   prefix); the task is pinned via `constraints.pin_to_node` and the
//!   mesh dispatcher routes it over QUIC.
//! - `--all` — one task per live node (self + peers), outputs
//!   interleaved with `[node-name]` prefixes.
//! - `--where <expr>` — like `--all` over the nodes matching the
//!   predicate expression: space-separated AND of `cap:<capability>`,
//!   `node:<name-substring>`, `os:<os-substring>`.
//!
//! Exit code: single target → the command's exit code (124 on timeout);
//! multi target → 0 iff every node exited 0, else the first non-zero.

use std::str::FromStr;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use futures::stream::{FuturesUnordered, StreamExt};
use harness_core::NodeId;
use serde_json::{json, Value as JsonValue};

use crate::{obtain_session_token, resolve_root, RunArgs};

const POLL_INITIAL_MS: u64 = 100;
const POLL_MAX_MS: u64 = 800;
const CLI_DEADLINE_SLACK_MS: u64 = 5_000;
const DEFAULT_CAPABILITY_TIMEOUT_MS: u64 = 60_000;
/// Extra headroom on `execution.timeout_ms` over the shell timeout so
/// the capability-level timeout fires first (clean `timed_out` output)
/// and the mesh lease TTL dominates both (ADR-0017).
const EXECUTION_TIMEOUT_SLACK_MS: u64 = 5_000;

/// Outcome of a `harness run` invocation. The binary returns `code` as
/// its process exit code; `stdout` / `stderr` strings have already had
/// the `[<node-name>]` prefix applied.
#[derive(Debug, Default)]
pub struct RunOutcome {
    pub stdout: String,
    pub stderr: String,
    pub code: i32,
}

/// One resolvable run target.
#[derive(Debug, Clone)]
struct NodeRef {
    /// Display label: manifest hostname when known, else node-id hex.
    label: String,
    node_id: NodeId,
    os: Option<String>,
    capabilities: Vec<String>,
    is_self: bool,
}

/// Async entry point. The daemon binary calls this from a multi-thread
/// runtime via `SyncOutcome::RunRequested`.
pub async fn run_run(args: RunArgs) -> Result<RunOutcome> {
    if args.argv.is_empty() {
        bail!("argv after `--` is empty");
    }
    let cmd = args.argv[0].clone();
    let cmd_args: Vec<String> = args.argv.iter().skip(1).cloned().collect();

    // Canonicalize --cwd client-side so it resolves from the user's cwd.
    let cwd_str = match args.cwd.as_ref() {
        Some(p) => Some(canonicalize_cwd(p)?),
        None => None,
    };

    let timeout_ms = args.timeout_ms.unwrap_or(DEFAULT_CAPABILITY_TIMEOUT_MS);
    let mut input = json!({
        "cmd":  cmd,
        "args": cmd_args,
        "timeout_ms": timeout_ms,
    });
    if let Some(cwd) = cwd_str {
        input["cwd"] = json!(cwd);
    }

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .context("build reqwest client")?;
    let api_base = args.api.trim_end_matches('/').to_string();

    // Authenticate first — the mesh view (peers) needs it anyway.
    let root = resolve_root(args.root.clone())?;
    let token = obtain_session_token(&root, &api_base)
        .await
        .context("authenticate to daemon (run `harness admin set-password` if not yet set)")?;

    // Resolve targets against the live mesh view.
    let mesh = fetch_mesh_view(&client, &api_base, &token)
        .await
        .context("fetch mesh view (GET /api/v1/peers)")?;
    let targets = resolve_targets(&args, &mesh)?;

    // Fan out: one pinned task per target, polled concurrently. Outputs
    // are appended in completion order; `[node]` prefixes disambiguate.
    let cli_deadline = Duration::from_millis(timeout_ms + CLI_DEADLINE_SLACK_MS);
    let mut in_flight = FuturesUnordered::new();
    for target in targets {
        let client = client.clone();
        let api_base = api_base.clone();
        let token = token.clone();
        let input = input.clone();
        in_flight.push(async move {
            let outcome = run_on_node(
                &client,
                &api_base,
                &token,
                &input,
                timeout_ms,
                cli_deadline,
                &target,
            )
            .await;
            (target, outcome)
        });
    }

    let mut combined = RunOutcome::default();
    while let Some((target, outcome)) = in_flight.next().await {
        match outcome {
            Ok(o) => {
                combined.stdout.push_str(&o.stdout);
                combined.stderr.push_str(&o.stderr);
                if o.code != 0 && combined.code == 0 {
                    combined.code = o.code;
                }
            }
            Err(e) => {
                combined
                    .stderr
                    .push_str(&format!("[{}] error: {e:#}\n", target.label));
                if combined.code == 0 {
                    combined.code = 1;
                }
            }
        }
    }
    Ok(combined)
}

/// Submit + poll one node.
async fn run_on_node(
    client: &reqwest::Client,
    api_base: &str,
    token: &str,
    input: &JsonValue,
    timeout_ms: u64,
    cli_deadline: Duration,
    target: &NodeRef,
) -> Result<RunOutcome> {
    let task_id = submit_task(client, api_base, token, input, timeout_ms, target)
        .await
        .context("submit shell.exec task")?;
    let envelope = poll_until_terminal(client, api_base, token, &task_id, cli_deadline).await?;
    Ok(format_outcome(&envelope, &target.label))
}

/// Self + peers with names / os / capabilities from `GET /api/v1/peers`.
async fn fetch_mesh_view(
    client: &reqwest::Client,
    api_base: &str,
    token: &str,
) -> Result<Vec<NodeRef>> {
    let url = format!("{api_base}/api/v1/peers");
    let resp = client
        .get(&url)
        .bearer_auth(token)
        .send()
        .await
        .context("GET /peers")?;
    if !resp.status().is_success() {
        bail!("GET /peers failed: HTTP {}", resp.status());
    }
    let body: JsonValue = resp.json().await.context("decode /peers")?;

    let mut nodes = Vec::new();
    if let Some(local) = body.get("local") {
        if let Some(node) = node_ref_from_dto(local, true) {
            nodes.push(node);
        }
    }
    if let Some(peers) = body.get("peers").and_then(|p| p.as_array()) {
        for peer in peers {
            if let Some(node) = node_ref_from_dto(peer, false) {
                nodes.push(node);
            }
        }
    }
    if nodes.is_empty() {
        bail!("mesh view is empty (daemon returned no local node)");
    }
    Ok(nodes)
}

fn node_ref_from_dto(dto: &JsonValue, is_self: bool) -> Option<NodeRef> {
    let node_id_hex = dto.get("node_id")?.as_str()?;
    let node_id = NodeId::from_str(node_id_hex).ok()?;
    let node_name = dto
        .get("node_name")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty());
    let label = node_name.map_or_else(|| node_id_hex.to_string(), str::to_string);
    // Self has no manifest-backed os in the DTO; the CLI talks to the
    // local daemon, so the compile-time OS is the truth for self.
    let os = dto
        .get("os")
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .or_else(|| is_self.then(|| std::env::consts::OS.to_string()));
    let capabilities = dto
        .get("capabilities_summary")
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|c| c.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    Some(NodeRef {
        label,
        node_id,
        os,
        capabilities,
        is_self,
    })
}

fn resolve_targets(args: &RunArgs, mesh: &[NodeRef]) -> Result<Vec<NodeRef>> {
    if args.all {
        return Ok(mesh.to_vec());
    }
    if let Some(expr) = args.where_.as_deref() {
        let matched = filter_where(mesh, expr)?;
        if matched.is_empty() {
            bail!("--where {expr:?} matches no live node");
        }
        return Ok(matched);
    }
    let self_target = || {
        mesh.iter()
            .find(|n| n.is_self)
            .cloned()
            .map(|n| vec![n])
            .ok_or_else(|| anyhow::anyhow!("local node missing from the daemon's mesh view"))
    };
    match args.on.as_deref() {
        None => self_target(),
        Some(sel) if sel.eq_ignore_ascii_case("self") => self_target(),
        Some(sel) => {
            let sel_lower = sel.to_ascii_lowercase();
            let by_name = mesh
                .iter()
                .find(|n| n.label.eq_ignore_ascii_case(sel))
                .cloned();
            let by_id = || {
                mesh.iter()
                    .find(|n| format!("{}", n.node_id).starts_with(&sel_lower))
                    .cloned()
            };
            if let Some(node) = by_name.or_else(by_id) {
                Ok(vec![node])
            } else {
                let known: Vec<&str> = mesh.iter().map(|n| n.label.as_str()).collect();
                bail!("unknown node {sel:?}; known nodes: {}", known.join(", "));
            }
        }
    }
}

/// `--where` v1 grammar: space-separated AND of `cap:<capability>` |
/// `node:<name-substring>` | `os:<os-substring>` (ADR-0017).
fn filter_where(mesh: &[NodeRef], expr: &str) -> Result<Vec<NodeRef>> {
    let predicates: Vec<&str> = expr.split_whitespace().collect();
    if predicates.is_empty() {
        bail!("--where expression is empty");
    }
    let mut out = Vec::new();
    'nodes: for node in mesh {
        for pred in &predicates {
            let matched = match pred.split_once(':') {
                Some(("cap", want)) => node.capabilities.iter().any(|c| c == want),
                Some(("node", want)) => node
                    .label
                    .to_ascii_lowercase()
                    .contains(&want.to_ascii_lowercase()),
                Some(("os", want)) => node
                    .os
                    .as_deref()
                    .unwrap_or("")
                    .to_ascii_lowercase()
                    .contains(&want.to_ascii_lowercase()),
                _ => bail!(
                    "unsupported --where predicate {pred:?}; supported: \
                     cap:<capability>, node:<name-substring>, os:<os-substring>"
                ),
            };
            if !matched {
                continue 'nodes;
            }
        }
        out.push(node.clone());
    }
    Ok(out)
}

fn canonicalize_cwd(p: &std::path::Path) -> Result<String> {
    let canonical = std::fs::canonicalize(p)
        .with_context(|| format!("--cwd {p:?} does not resolve to an existing path"))?;
    if !canonical.is_dir() {
        bail!("--cwd {p:?} is not a directory");
    }
    canonical
        .to_str()
        .map(str::to_string)
        .ok_or_else(|| anyhow::anyhow!("--cwd {p:?} is not valid UTF-8"))
}

async fn submit_task(
    client: &reqwest::Client,
    api_base: &str,
    token: &str,
    input: &JsonValue,
    timeout_ms: u64,
    target: &NodeRef,
) -> Result<String> {
    let url = format!("{api_base}/api/v1/tasks");
    // Pin every task to its resolved node — including self, so the
    // dispatcher's routing decision is deterministic under `--all`.
    let pin: Vec<u8> = target.node_id.as_bytes().to_vec();
    // `execution.timeout_ms` tracks the shell timeout (+slack) so the
    // mesh lease TTL dominates runtime (ADR-0017 / R2).
    let execution_timeout =
        u32::try_from(timeout_ms.saturating_add(EXECUTION_TIMEOUT_SLACK_MS)).unwrap_or(u32::MAX);
    let resp = client
        .post(&url)
        .bearer_auth(token)
        .json(&json!({
            "capability": "shell.exec",
            "input": input,
            "constraints": {
                "deadline": null,
                "max_cost_usd": null,
                "must_be_local": false,
                "require_tags": [],
                "exclude_tags": [],
                "pin_to_node": pin,
                "pin_to_scope": null,
            },
            "execution": {
                "redundancy": 1,
                "timeout_ms": execution_timeout,
                "on_partial": "fail_fast",
                "lease_ms": 10_000,
            },
        }))
        .send()
        .await
        .context("submit POST")?;
    let status = resp.status();
    let body: JsonValue = resp.json().await.context("decode submit response")?;
    if !status.is_success() {
        bail!("submit failed: HTTP {status} — {body}");
    }
    body["task_id"]
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| anyhow::anyhow!("submit response missing task_id: {body}"))
}

pub(crate) async fn poll_until_terminal(
    client: &reqwest::Client,
    api_base: &str,
    token: &str,
    task_id: &str,
    deadline: Duration,
) -> Result<JsonValue> {
    let url = format!("{api_base}/api/v1/tasks/{task_id}");
    let started = Instant::now();
    let mut backoff = Duration::from_millis(POLL_INITIAL_MS);

    loop {
        if started.elapsed() >= deadline {
            bail!("task did not complete within deadline");
        }
        let resp = client
            .get(&url)
            .bearer_auth(token)
            .send()
            .await
            .context("poll GET")?;
        if !resp.status().is_success() {
            bail!("poll failed: HTTP {}", resp.status());
        }
        let body: JsonValue = resp.json().await.context("decode poll response")?;
        let state = body.get("state").and_then(|s| s.as_str()).unwrap_or("");
        if matches!(state, "done" | "failed" | "expired" | "cancelled") {
            return Ok(body);
        }

        // Sleep up to (deadline - elapsed) so we don't overshoot.
        let remaining = deadline.saturating_sub(started.elapsed());
        let next = backoff.min(remaining);
        if next.is_zero() {
            bail!("task did not complete within deadline");
        }
        tokio::time::sleep(next).await;
        backoff = (backoff * 2).min(Duration::from_millis(POLL_MAX_MS));
    }
}

fn format_outcome(envelope: &JsonValue, node_label: &str) -> RunOutcome {
    let state = envelope.get("state").and_then(|s| s.as_str()).unwrap_or("");

    // Every non-done terminal renders its error text (failed, expired,
    // cancelled — R9): a silent exit-1 with no message is unactionable.
    if state != "done" {
        let err = envelope
            .get("error")
            .and_then(|e| e.as_str())
            .unwrap_or("unknown error");
        return RunOutcome {
            stderr: format!("[{node_label}] {state}: {err}\n"),
            stdout: String::new(),
            code: 1,
        };
    }

    // Done — render shell.exec output shape.
    let output = envelope.get("output").cloned().unwrap_or(JsonValue::Null);
    let stdout = output.get("stdout").and_then(|v| v.as_str()).unwrap_or("");
    let stderr = output.get("stderr").and_then(|v| v.as_str()).unwrap_or("");
    let timed_out = output
        .get("timed_out")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let exit_code = output
        .get("exit_code")
        .and_then(serde_json::Value::as_i64)
        .and_then(|i| i32::try_from(i).ok());

    let code = if timed_out {
        124
    } else {
        exit_code.unwrap_or(1)
    };

    RunOutcome {
        stdout: prefix_lines(node_label, stdout),
        stderr: prefix_lines(node_label, stderr),
        code,
    }
}

fn prefix_lines(node_label: &str, body: &str) -> String {
    if body.is_empty() {
        return String::new();
    }
    let mut out = String::with_capacity(body.len() + body.matches('\n').count() * 16);
    for line in body.split_inclusive('\n') {
        out.push('[');
        out.push_str(node_label);
        out.push_str("] ");
        out.push_str(line);
    }
    // If body didn't end with \n, append one for clean terminal output.
    if !body.ends_with('\n') {
        out.push('\n');
    }
    out
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    fn node(label: &str, byte: u8, os: &str, caps: &[&str], is_self: bool) -> NodeRef {
        NodeRef {
            label: label.to_string(),
            node_id: NodeId::from_bytes([byte; 16]),
            os: Some(os.to_string()),
            capabilities: caps.iter().map(|s| (*s).to_string()).collect(),
            is_self,
        }
    }

    fn mesh() -> Vec<NodeRef> {
        vec![
            node("laptop-a", 1, "macos", &["echo", "shell.exec"], true),
            node(
                "tower-b",
                2,
                "linux",
                &["echo", "shell.exec", "llm.local.phi"],
                false,
            ),
            node("pi-c", 3, "linux", &["echo"], false),
        ]
    }

    fn args_with(all: bool, on: Option<&str>, where_: Option<&str>) -> RunArgs {
        RunArgs {
            all,
            on: on.map(str::to_string),
            where_: where_.map(str::to_string),
            timeout_ms: None,
            cwd: None,
            root: None,
            api: "http://127.0.0.1:19198".into(),
            argv: vec!["uname".into()],
        }
    }

    #[test]
    fn resolve_default_is_self() {
        let t = resolve_targets(&args_with(false, None, None), &mesh()).expect("resolve");
        assert_eq!(t.len(), 1);
        assert!(t[0].is_self);
    }

    #[test]
    fn resolve_on_self_keyword() {
        let t = resolve_targets(&args_with(false, Some("SELF"), None), &mesh()).expect("resolve");
        assert_eq!(t.len(), 1);
        assert_eq!(t[0].label, "laptop-a");
    }

    #[test]
    fn resolve_on_name_case_insensitive() {
        let t =
            resolve_targets(&args_with(false, Some("Tower-B"), None), &mesh()).expect("resolve");
        assert_eq!(t.len(), 1);
        assert_eq!(t[0].label, "tower-b");
    }

    #[test]
    fn resolve_on_node_id_prefix() {
        let id_hex = format!("{}", NodeId::from_bytes([3; 16]));
        let prefix = &id_hex[..8];
        let t = resolve_targets(&args_with(false, Some(prefix), None), &mesh()).expect("resolve");
        assert_eq!(t[0].label, "pi-c");
    }

    #[test]
    fn resolve_on_unknown_lists_known_nodes() {
        let err = resolve_targets(&args_with(false, Some("ghost"), None), &mesh())
            .expect_err("unknown node");
        let msg = err.to_string();
        assert!(msg.contains("unknown node"), "got: {msg}");
        assert!(
            msg.contains("laptop-a") && msg.contains("tower-b"),
            "got: {msg}"
        );
    }

    #[test]
    fn resolve_all_returns_every_node() {
        let t = resolve_targets(&args_with(true, None, None), &mesh()).expect("resolve");
        assert_eq!(t.len(), 3);
    }

    #[test]
    fn where_cap_exact_match() {
        let t = filter_where(&mesh(), "cap:llm.local.phi").expect("filter");
        assert_eq!(t.len(), 1);
        assert_eq!(t[0].label, "tower-b");
    }

    #[test]
    fn where_os_substring_and_node_substring_and() {
        let t = filter_where(&mesh(), "os:linux node:pi").expect("filter");
        assert_eq!(t.len(), 1);
        assert_eq!(t[0].label, "pi-c");
    }

    #[test]
    fn where_unsupported_predicate_errors_with_grammar() {
        let err = filter_where(&mesh(), "tag=gpu").expect_err("bad predicate");
        assert!(err.to_string().contains("cap:<capability>"));
    }

    #[test]
    fn where_no_match_surfaces_in_resolve() {
        let err = resolve_targets(&args_with(false, None, Some("os:plan9")), &mesh())
            .expect_err("no match");
        assert!(err.to_string().contains("matches no live node"));
    }

    #[test]
    fn format_outcome_renders_error_for_all_non_done_terminals() {
        for state in ["failed", "expired", "cancelled"] {
            let env = serde_json::json!({"state": state, "error": "boom"});
            let out = format_outcome(&env, "n1");
            assert_eq!(out.code, 1, "{state}");
            assert!(out.stderr.contains(state), "{state}: {}", out.stderr);
            assert!(out.stderr.contains("boom"), "{state}: {}", out.stderr);
        }
    }

    #[test]
    fn prefix_lines_handles_multiline() {
        let out = prefix_lines("host-a", "one\ntwo\nthree\n");
        assert_eq!(out, "[host-a] one\n[host-a] two\n[host-a] three\n");
    }

    #[test]
    fn prefix_lines_appends_trailing_newline_if_absent() {
        let out = prefix_lines("h", "no newline");
        assert_eq!(out, "[h] no newline\n");
    }

    #[test]
    fn prefix_lines_empty_body_is_empty() {
        assert_eq!(prefix_lines("h", ""), "");
    }

    #[test]
    fn format_outcome_done_uses_exit_code() {
        let env = json!({
            "state": "done",
            "output": {
                "stdout": "hello\n",
                "stderr": "",
                "exit_code": 0,
                "timed_out": false,
                "stdout_truncated_bytes": 0,
                "stderr_truncated_bytes": 0,
            }
        });
        let r = format_outcome(&env, "h");
        assert_eq!(r.code, 0);
        assert_eq!(r.stdout, "[h] hello\n");
        assert_eq!(r.stderr, "");
    }

    #[test]
    fn format_outcome_timed_out_uses_124() {
        let env = json!({
            "state": "done",
            "output": {
                "stdout": "",
                "stderr": "",
                "exit_code": null,
                "timed_out": true,
            }
        });
        let r = format_outcome(&env, "h");
        assert_eq!(r.code, 124);
    }

    #[test]
    fn format_outcome_failed_renders_error_to_stderr() {
        let env = json!({
            "state": "failed",
            "error": "policy denied: no allow rule matched cat",
        });
        let r = format_outcome(&env, "h");
        assert_eq!(r.code, 1);
        assert!(r.stderr.contains("policy denied"));
        assert_eq!(r.stdout, "");
    }
}
