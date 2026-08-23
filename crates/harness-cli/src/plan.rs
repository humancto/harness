//! `harness plan` / `harness exec` — CLI front-ends for the planner
//! and the 4.3 DAG executor (PRD §19, ADR-0025).
//!
//! `harness plan "<goal>"` submits a `brain.plan` task and renders the
//! resulting DAG as a topologically-ordered listing (dry run — nothing
//! executes). `harness exec "<goal>"` plans, then submits the plan to
//! `plan.execute` and renders per-step progress incrementally from the
//! partials frames (same TTY-gated pattern as `harness search`).

use std::time::Duration;

use anyhow::{bail, Context, Result};
use serde_json::{json, Value as JsonValue};

use crate::run::RunOutcome;
use crate::{obtain_session_token, resolve_root, PlanArgs};

const CLI_SLACK_MS: u64 = 5_000;
/// Generous default for whole-plan execution (review MINOR-9): plans
/// chain steps, so the envelope default of 30 s would kill any real
/// critical path.
const DEFAULT_EXEC_TIMEOUT_MS: u64 = 120_000;
const MAX_EXEC_TIMEOUT_MS: u64 = 600_000;
/// 5.1/5.2 (ADR-0030/0031): the planning budget must cover the full
/// escalation chain — `LocalFast` (30 s) + `LocalStrong` (120 s) +
/// Cloud (60 s) + Template — with slack, or a slow tier starves the
/// Template fallback. The user's `--timeout-ms` overrides it.
const DEFAULT_PLAN_TIMEOUT_MS: u64 = 240_000;

/// `harness plan "<goal>"` — plan only, render the DAG.
pub async fn run_plan(args: PlanArgs) -> Result<RunOutcome> {
    let (client, api_base, token) = connect(&args).await?;
    let plan = obtain_plan(&client, &api_base, &token, &args).await?;
    Ok(render_plan(&plan))
}

/// `harness exec "<goal>"` — plan, then execute the plan.
pub async fn run_exec(args: PlanArgs) -> Result<RunOutcome> {
    let (client, api_base, token) = connect(&args).await?;
    let plan = obtain_plan(&client, &api_base, &token, &args).await?;
    let timeout_ms = args
        .timeout_ms
        .unwrap_or(DEFAULT_EXEC_TIMEOUT_MS)
        .clamp(1_000, MAX_EXEC_TIMEOUT_MS);
    let mut input = json!({ "plan": plan, "timeout_ms": timeout_ms });
    if args.keep_going {
        input["on_failure"] = json!("continue");
    }
    let envelope = submit_and_poll(
        &client,
        &api_base,
        &token,
        "plan.execute",
        input,
        timeout_ms,
        true,
    )
    .await?;
    let state = envelope
        .get("state")
        .and_then(JsonValue::as_str)
        .unwrap_or("");
    if state != "done" {
        let err = envelope
            .get("error")
            .and_then(JsonValue::as_str)
            .unwrap_or("unknown error");
        bail!("plan execution {state}: {err}");
    }
    let output = envelope.get("output").cloned().unwrap_or(JsonValue::Null);
    Ok(render_exec(&output))
}

async fn connect(args: &PlanArgs) -> Result<(reqwest::Client, String, String)> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .context("build reqwest client")?;
    let api_base = args.api.trim_end_matches('/').to_string();
    let root = resolve_root(args.root.clone())?;
    let token = obtain_session_token(&root, &api_base)
        .await
        .context("authenticate to daemon (run `harness admin set-password` if not yet set)")?;
    Ok((client, api_base, token))
}

/// Build the `brain.plan` input. `--cloud` is the CLI's per-task
/// cloud opt-in (5.2, ADR-0031): without an explicit
/// `constraints.allow_cloud: true` (or a `cloud_ok` task tag) the
/// brain.plan executor narrows `allow_cloud` to false, so the cloud
/// tier would be unreachable from the CLI even on a policy-approved
/// mesh (Codex review P1).
fn plan_input(goal: &str, cloud: bool) -> JsonValue {
    if cloud {
        json!({ "goal": goal, "constraints": { "allow_cloud": true } })
    } else {
        json!({ "goal": goal })
    }
}

/// Run `brain.plan` for the goal and extract the plan JSON.
async fn obtain_plan(
    client: &reqwest::Client,
    api_base: &str,
    token: &str,
    args: &PlanArgs,
) -> Result<JsonValue> {
    // 5.1 (review MAJOR-2): the user's --timeout-ms governs planning
    // too — previously it was silently ignored here.
    let plan_timeout_ms = args
        .timeout_ms
        .unwrap_or(DEFAULT_PLAN_TIMEOUT_MS)
        .clamp(1_000, MAX_EXEC_TIMEOUT_MS);
    let input = plan_input(&args.goal, args.cloud);
    let envelope = submit_and_poll(
        client,
        api_base,
        token,
        "brain.plan",
        input,
        plan_timeout_ms,
        false,
    )
    .await?;
    let state = envelope
        .get("state")
        .and_then(JsonValue::as_str)
        .unwrap_or("");
    if state != "done" {
        let err = envelope
            .get("error")
            .and_then(JsonValue::as_str)
            .unwrap_or("unknown error");
        bail!("planning {state}: {err}");
    }
    envelope
        .pointer("/output/plan")
        .cloned()
        .filter(|p| !p.is_null())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "planner produced no plan (output: {})",
                envelope.get("output").cloned().unwrap_or(JsonValue::Null)
            )
        })
}

async fn submit_and_poll(
    client: &reqwest::Client,
    api_base: &str,
    token: &str,
    capability: &str,
    input: JsonValue,
    timeout_ms: u64,
    render_progress: bool,
) -> Result<JsonValue> {
    let execution_timeout =
        u32::try_from(timeout_ms.saturating_add(CLI_SLACK_MS)).unwrap_or(u32::MAX);
    let url = format!("{api_base}/api/v1/tasks");
    let resp = client
        .post(&url)
        .bearer_auth(token)
        .json(&json!({
            "capability": capability,
            "input": input,
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
    let task_id = body["task_id"]
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| anyhow::anyhow!("submit response missing task_id: {body}"))?;
    let deadline = Duration::from_millis(timeout_ms + 2 * CLI_SLACK_MS);
    let progress_tty = render_progress && std::io::IsTerminal::is_terminal(&std::io::stderr());
    crate::run::poll_until_terminal_with(client, api_base, token, &task_id, deadline, |frames| {
        if !progress_tty {
            return;
        }
        for frame in frames {
            if let Some(line) = step_progress_line(frame) {
                // Interactive telemetry: stderr, TTY-only.
                #[allow(clippy::print_stderr)]
                {
                    eprintln!("{line}");
                }
            }
        }
    })
    .await
}

/// Render one `partials` frame as a step-progress line (4.3 chunks).
fn step_progress_line(frame: &JsonValue) -> Option<String> {
    if frame.get("stream").and_then(JsonValue::as_str) != Some("progress") {
        return None;
    }
    let chunk: JsonValue = serde_json::from_str(frame.get("line")?.as_str()?).ok()?;
    let step = chunk.get("step")?;
    let capability = step.get("capability").and_then(JsonValue::as_str)?;
    let state = step.get("state").and_then(JsonValue::as_str)?;
    let short = step
        .get("id")
        .and_then(JsonValue::as_str)
        .map_or("?", |s| &s[..s.len().min(8)]);
    Some(match step.get("error").and_then(JsonValue::as_str) {
        Some(err) => format!("step {short} ({capability}): {state} — {err}"),
        None => format!("step {short} ({capability}): {state}"),
    })
}

/// Topologically-ordered text listing of a plan DAG.
fn render_plan(plan: &JsonValue) -> RunOutcome {
    let name = plan.get("name").and_then(JsonValue::as_str).unwrap_or("?");
    let tasks = plan
        .get("tasks")
        .and_then(JsonValue::as_object)
        .cloned()
        .unwrap_or_default();
    let edges: Vec<(String, String)> = plan
        .get("edges")
        .and_then(JsonValue::as_array)
        .map(|es| {
            es.iter()
                .filter_map(|e| {
                    let pair = e.as_array()?;
                    Some((
                        pair.first()?.as_str()?.to_string(),
                        pair.get(1)?.as_str()?.to_string(),
                    ))
                })
                .collect()
        })
        .unwrap_or_default();
    // Kahn order over "(from, to) = from depends on to".
    let mut remaining: std::collections::HashMap<&str, usize> =
        tasks.keys().map(|k| (k.as_str(), 0)).collect();
    for (from, _) in &edges {
        if let Some(r) = remaining.get_mut(from.as_str()) {
            *r += 1;
        }
    }
    let mut order: Vec<String> = Vec::new();
    let mut ready: Vec<&str> = {
        let mut v: Vec<&str> = remaining
            .iter()
            .filter(|(_, &r)| r == 0)
            .map(|(&k, _)| k)
            .collect();
        v.sort_unstable();
        v
    };
    while let Some(id) = ready.pop() {
        order.push(id.to_string());
        for (from, to) in &edges {
            if to == id {
                if let Some(r) = remaining.get_mut(from.as_str()) {
                    *r -= 1;
                    if *r == 0 {
                        ready.push(from.as_str());
                        ready.sort_unstable();
                    }
                }
            }
        }
    }
    let mut out = format!("plan: {name} ({} steps)\n", tasks.len());
    for id in &order {
        let node = &tasks[id.as_str()];
        let cap = node
            .get("capability")
            .and_then(JsonValue::as_str)
            .unwrap_or("?");
        let deps: Vec<&str> = edges
            .iter()
            .filter(|(from, _)| from == id)
            .map(|(_, to)| &to[..to.len().min(8)])
            .collect();
        let short = &id[..id.len().min(8)];
        let input_preview = node
            .get("input")
            .map(|i| truncate_chars(&i.to_string(), 60))
            .unwrap_or_default();
        if deps.is_empty() {
            out.push_str(&format!("  {short}  {cap}  {input_preview}\n"));
        } else {
            out.push_str(&format!(
                "  {short}  {cap}  {input_preview}  ⇐ depends on [{}]\n",
                deps.join(", ")
            ));
        }
    }
    if order.len() != tasks.len() {
        out.push_str("  (warning: cycle detected in listing — validation will reject this plan)\n");
    }
    RunOutcome {
        stdout: out,
        stderr: String::new(),
        code: 0,
    }
}

/// Truncate at a char boundary at or below `max_bytes` (JSON strings
/// carry raw UTF-8 — a byte slice can split a multi-byte char and
/// panic; diff review MAJOR-3).
fn truncate_chars(s: &str, max_bytes: usize) -> String {
    if s.len() <= max_bytes {
        return s.to_string();
    }
    let mut cut = max_bytes;
    while cut > 0 && !s.is_char_boundary(cut) {
        cut -= 1;
    }
    format!("{}…", &s[..cut])
}

/// Render a plan.execute aggregate.
fn render_exec(output: &JsonValue) -> RunOutcome {
    let mut out = format!(
        "plan {}: {} ok, {} failed, {} timed out, {} skipped ({} ms)\n",
        output
            .get("name")
            .and_then(JsonValue::as_str)
            .unwrap_or("?"),
        output.get("ok").and_then(JsonValue::as_u64).unwrap_or(0),
        output
            .get("failed")
            .and_then(JsonValue::as_u64)
            .unwrap_or(0),
        output
            .get("timed_out")
            .and_then(JsonValue::as_u64)
            .unwrap_or(0),
        output
            .get("skipped")
            .and_then(JsonValue::as_u64)
            .unwrap_or(0),
        output
            .get("duration_ms")
            .and_then(JsonValue::as_u64)
            .unwrap_or(0),
    );
    // Exit code from the aggregate counts, not error-string presence
    // (diff review MINOR-7).
    let bad = output
        .get("failed")
        .and_then(JsonValue::as_u64)
        .unwrap_or(0)
        + output
            .get("timed_out")
            .and_then(JsonValue::as_u64)
            .unwrap_or(0)
        + output
            .get("skipped")
            .and_then(JsonValue::as_u64)
            .unwrap_or(0);
    if let Some(steps) = output.get("steps").and_then(JsonValue::as_object) {
        let mut ids: Vec<&String> = steps.keys().collect();
        ids.sort_unstable();
        for id in ids {
            let s = &steps[id];
            let short = &id[..id.len().min(8)];
            let cap = s
                .get("capability")
                .and_then(JsonValue::as_str)
                .unwrap_or("?");
            let state = s.get("state").and_then(JsonValue::as_str).unwrap_or("?");
            match s.get("error").and_then(JsonValue::as_str) {
                Some(err) => out.push_str(&format!("  {short}  {cap}  {state} — {err}\n")),
                None => out.push_str(&format!("  {short}  {cap}  {state}\n")),
            }
        }
    }
    RunOutcome {
        stdout: out,
        stderr: String::new(),
        code: i32::from(bad > 0),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn plan_input_carries_cloud_opt_in_only_when_flagged() {
        // Codex review P1: without an explicit constraint the
        // executor's cloud_ok gate narrows allow_cloud to false, so
        // the flag must translate into the constraint verbatim.
        let default = plan_input("run: ls", false);
        assert_eq!(default, json!({ "goal": "run: ls" }));
        let opted_in = plan_input("run: ls", true);
        assert_eq!(
            opted_in,
            json!({ "goal": "run: ls", "constraints": { "allow_cloud": true } })
        );
    }

    #[test]
    fn plan_listing_is_topological_with_deps() {
        let a = "aaaaaaaa-0000-7000-8000-000000000001";
        let b = "bbbbbbbb-0000-7000-8000-000000000002";
        let plan = json!({
            "name": "demo",
            "tasks": {
                a: { "id": a, "capability": "echo", "input": {"msg": "one"} },
                b: { "id": b, "capability": "echo", "input": {"x": 1} },
            },
            "edges": [[b, a]],
        });
        let out = render_plan(&plan);
        assert_eq!(out.code, 0);
        let a_pos = out.stdout.find("aaaaaaaa").expect("a listed");
        let b_pos = out.stdout.find("bbbbbbbb").expect("b listed");
        assert!(
            a_pos < b_pos,
            "dependency listed before dependent:\n{}",
            out.stdout
        );
        assert!(out.stdout.contains("⇐ depends on [aaaaaaaa]"));
    }

    #[test]
    fn plan_listing_truncates_non_ascii_previews_safely() {
        // Diff review MAJOR-3: byte-slicing a UTF-8 JSON string panics
        // on a char boundary; the preview must truncate char-safely.
        let a = "aaaaaaaa-0000-7000-8000-000000000001";
        let long_unicode = "é".repeat(80); // 2 bytes per char
        let plan = json!({
            "name": "unicode",
            "tasks": { a: { "id": a, "capability": "echo", "input": {"msg": long_unicode} } },
            "edges": [],
        });
        let out = render_plan(&plan); // must not panic
        assert_eq!(out.code, 0);
        assert!(out.stdout.contains('…'), "preview truncated");
    }

    #[test]
    fn exec_render_reports_counts_and_exit_code() {
        let ok = json!({
            "name": "demo", "ok": 2, "failed": 0, "timed_out": 0, "skipped": 0,
            "duration_ms": 12,
            "steps": {
                "aaaaaaaa-0000-7000-8000-000000000001":
                    { "capability": "echo", "state": "done" },
            },
        });
        assert_eq!(render_exec(&ok).code, 0);
        let partial = json!({
            "name": "demo", "ok": 1, "failed": 1, "timed_out": 0, "skipped": 1,
            "duration_ms": 12,
            "steps": {
                "aaaaaaaa-0000-7000-8000-000000000001":
                    { "capability": "echo", "state": "failed", "error": "boom" },
            },
        });
        let out = render_exec(&partial);
        assert_eq!(out.code, 1, "failures exit non-zero");
        assert!(out.stdout.contains("boom"));
    }

    #[test]
    fn step_progress_line_renders_and_skips() {
        let frame = json!({
            "stream": "progress", "seq": 0,
            "line": json!({"step": {
                "id": "aaaaaaaa-0000-7000-8000-000000000001",
                "capability": "echo", "state": "done"
            }}).to_string(),
        });
        assert_eq!(
            step_progress_line(&frame).expect("line"),
            "step aaaaaaaa (echo): done"
        );
        let mesh_frame = json!({
            "stream": "progress", "seq": 1,
            "line": r#"{"target":{"node":"x","node_name":"p","scope":"s"},"outcome":"ok"}"#,
        });
        assert!(
            step_progress_line(&mesh_frame).is_none(),
            "not a step chunk"
        );
        let stdout_frame = json!({ "stream": "stdout", "seq": 2, "line": "hi" });
        assert!(step_progress_line(&stdout_frame).is_none());
    }
}
