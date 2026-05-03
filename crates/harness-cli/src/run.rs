//! `harness run` — submit a `shell.exec` task to the local daemon and
//! poll until terminal. Phase 3.3a.
//!
//! Cross-node dispatch (`--all`, `--on <other-host>`, `--where`) lives
//! in 3.3-fanout. This file errors out on those targets with a clear
//! pointer to ROADMAP.md.

use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use serde_json::{json, Value as JsonValue};

use crate::{obtain_session_token, resolve_root, RunArgs};

const FANOUT_DEFERRED_MSG: &str =
    "cross-node dispatch lands in 3.3-fanout (see ROADMAP.md item 3.3-fanout)";
const POLL_INITIAL_MS: u64 = 100;
const POLL_MAX_MS: u64 = 800;
const CLI_DEADLINE_SLACK_MS: u64 = 5_000;
const DEFAULT_CAPABILITY_TIMEOUT_MS: u64 = 60_000;

/// Outcome of a `harness run` invocation. The binary returns `code` as
/// its process exit code; `stdout` / `stderr` strings have already had
/// the `[<node-name>]` prefix applied.
#[derive(Debug, Default)]
pub struct RunOutcome {
    pub stdout: String,
    pub stderr: String,
    pub code: i32,
}

/// Async entry point. The daemon binary calls this from a multi-thread
/// runtime via `SyncOutcome::RunRequested`.
pub async fn run_run(args: RunArgs) -> Result<RunOutcome> {
    // 1. Validate target. Single-host only in 3.3a.
    if args.all {
        bail!("--all: {FANOUT_DEFERRED_MSG}");
    }
    if args.where_.is_some() {
        bail!("--where: {FANOUT_DEFERRED_MSG}");
    }
    if let Some(target) = args.on.as_deref() {
        if !target.eq_ignore_ascii_case("self") {
            bail!("--on <node>: {FANOUT_DEFERRED_MSG}");
        }
    }

    if args.argv.is_empty() {
        bail!("argv after `--` is empty");
    }
    let cmd = args.argv[0].clone();
    let cmd_args: Vec<String> = args.argv.iter().skip(1).cloned().collect();

    // 2. Canonicalize --cwd client-side so it resolves from the user's cwd.
    let cwd_str = match args.cwd.as_ref() {
        Some(p) => Some(canonicalize_cwd(p)?),
        None => None,
    };

    // 3. Build the JSON input.
    let timeout_ms = args.timeout_ms.unwrap_or(DEFAULT_CAPABILITY_TIMEOUT_MS);
    let mut input = json!({
        "cmd":  cmd,
        "args": cmd_args,
        "timeout_ms": timeout_ms,
    });
    if let Some(cwd) = cwd_str {
        input["cwd"] = json!(cwd);
    }

    // 4. Probe `/api/v1/status` for the daemon's node name (best-effort).
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .context("build reqwest client")?;
    let api_base = args.api.trim_end_matches('/').to_string();
    let node_label = fetch_node_name(&client, &api_base)
        .await
        .unwrap_or_else(|| "local".to_string());

    // 5. Authenticate.
    let root = resolve_root(args.root)?;
    let token = obtain_session_token(&root, &api_base)
        .await
        .context("authenticate to daemon (run `harness admin set-password` if not yet set)")?;

    // 6. Submit.
    let task_id = submit_task(&client, &api_base, &token, &input)
        .await
        .context("submit shell.exec task")?;

    // 7. Poll with backoff. Deadline = capability timeout + slack.
    let cli_deadline = Duration::from_millis(timeout_ms + CLI_DEADLINE_SLACK_MS);
    let envelope = poll_until_terminal(&client, &api_base, &token, &task_id, cli_deadline).await?;

    // 8/9. Render output.
    Ok(format_outcome(&envelope, &node_label))
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

async fn fetch_node_name(client: &reqwest::Client, api_base: &str) -> Option<String> {
    let url = format!("{api_base}/api/v1/status");
    let resp = client.get(&url).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let v: JsonValue = resp.json().await.ok()?;
    // Try a few likely fields for forward-compat.
    v.get("local")
        .and_then(|l| l.get("node_name"))
        .or_else(|| v.get("node_name"))
        .or_else(|| v.get("mesh_name"))
        .and_then(|x| x.as_str())
        .map(str::to_string)
}

async fn submit_task(
    client: &reqwest::Client,
    api_base: &str,
    token: &str,
    input: &JsonValue,
) -> Result<String> {
    let url = format!("{api_base}/api/v1/tasks");
    let resp = client
        .post(&url)
        .bearer_auth(token)
        .json(&json!({
            "capability": "shell.exec",
            "input": input,
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

async fn poll_until_terminal(
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

    if state == "failed" {
        let err = envelope
            .get("error")
            .and_then(|e| e.as_str())
            .unwrap_or("unknown error");
        return RunOutcome {
            stderr: format!("[{node_label}] error: {err}\n"),
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
