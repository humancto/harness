//! `harness search` / `harness grep` — CLI front-ends for the 3.11
//! federated `mesh.search` / `mesh.grep` capabilities. Submit one task
//! to the local daemon; the mesh wrapper does the fan-out + merge.

use std::time::Duration;

use anyhow::{bail, Context, Result};
use serde_json::{json, Value as JsonValue};

use crate::run::RunOutcome;
use crate::{obtain_session_token, resolve_root, QueryArgs};

const DEFAULT_TIMEOUT_MS: u64 = 30_000;
const CLI_SLACK_MS: u64 = 5_000;

/// `harness grep <pattern>` → `mesh.grep`.
pub async fn run_grep(args: QueryArgs) -> Result<RunOutcome> {
    let mut input = json!({ "pattern": args.term });
    if args.literal {
        input["literal"] = json!(true);
    }
    if args.ignore_case {
        input["ignore_case"] = json!(true);
    }
    let output = run_mesh_query(&args, "mesh.grep", input).await?;
    Ok(render_grep(&output))
}

/// `harness search <query>` → `mesh.search`.
pub async fn run_search(args: QueryArgs) -> Result<RunOutcome> {
    let mut input = json!({ "query": args.term });
    if let Some(limit) = args.limit {
        input["limit"] = json!(limit);
    }
    let output = run_mesh_query(&args, "mesh.search", input).await?;
    Ok(render_search(&output))
}

const MAX_WRAPPER_TIMEOUT_MS: u64 = 120_000;

async fn run_mesh_query(
    args: &QueryArgs,
    capability: &str,
    mut input: JsonValue,
) -> Result<JsonValue> {
    let requested = args.timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS);
    // The wrapper clamps to 120 s server-side; clamp here too so the
    // CLI doesn't wait longer than the mesh ever will (review NIT-4).
    let timeout_ms = requested.min(MAX_WRAPPER_TIMEOUT_MS);
    if requested > timeout_ms {
        #[allow(clippy::print_stderr)]
        {
            eprintln!("note: --timeout-ms clamped to {MAX_WRAPPER_TIMEOUT_MS} (mesh maximum)");
        }
    }
    input["timeout_ms"] = json!(timeout_ms);

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .context("build reqwest client")?;
    let api_base = args.api.trim_end_matches('/').to_string();
    let root = resolve_root(args.root.clone())?;
    let token = obtain_session_token(&root, &api_base)
        .await
        .context("authenticate to daemon (run `harness admin set-password` if not yet set)")?;

    // The wrapper itself needs headroom beyond its internal timeout.
    let execution_timeout =
        u32::try_from(timeout_ms.saturating_add(CLI_SLACK_MS)).unwrap_or(u32::MAX);
    let url = format!("{api_base}/api/v1/tasks");
    let resp = client
        .post(&url)
        .bearer_auth(&token)
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
    // 4.2: render per-target progress to stderr as frames arrive — only
    // when stderr is a TTY, so piped/scripted invocations stay clean.
    let progress_tty = std::io::IsTerminal::is_terminal(&std::io::stderr());
    let envelope = crate::run::poll_until_terminal_with(
        &client,
        &api_base,
        &token,
        &task_id,
        deadline,
        |frames| {
            if !progress_tty {
                return;
            }
            for frame in frames {
                if let Some(line) = progress_line(frame) {
                    // Progress is interactive telemetry: stderr, TTY-only.
                    #[allow(clippy::print_stderr)]
                    {
                        eprintln!("{line}");
                    }
                }
            }
        },
    )
    .await?;
    let state = envelope.get("state").and_then(|s| s.as_str()).unwrap_or("");
    if state != "done" {
        let err = envelope
            .get("error")
            .and_then(|e| e.as_str())
            .unwrap_or("unknown error");
        bail!("{capability} {state}: {err}");
    }
    Ok(envelope.get("output").cloned().unwrap_or(JsonValue::Null))
}

/// Render one `partials` frame as a progress line, or `None` for
/// frames that aren't per-target progress (stdout/stderr lines, the
/// summary chunk — the final rendered output already covers those).
fn progress_line(frame: &JsonValue) -> Option<String> {
    if frame.get("stream").and_then(JsonValue::as_str) != Some("progress") {
        return None;
    }
    let chunk: JsonValue = serde_json::from_str(frame.get("line")?.as_str()?).ok()?;
    let target = chunk.get("target")?;
    let node_name = target.get("node_name").and_then(JsonValue::as_str)?;
    let scope = target.get("scope").and_then(JsonValue::as_str)?;
    let completed = chunk.get("completed").and_then(JsonValue::as_u64)?;
    let total = chunk.get("total").and_then(JsonValue::as_u64)?;
    let outcome = chunk.get("outcome").and_then(JsonValue::as_str)?;
    Some(match outcome {
        "ok" => {
            let items = chunk.get("items").and_then(JsonValue::as_u64).unwrap_or(0);
            format!("[{completed}/{total}] {node_name}/{scope}: ok ({items} matches)")
        }
        other => {
            let err = chunk
                .get("error")
                .and_then(JsonValue::as_str)
                .unwrap_or(other);
            format!("[{completed}/{total}] {node_name}/{scope}: {other} — {err}")
        }
    })
}

/// True when the fan-out found zero targets at all — distinct from a
/// clean query with no matches (review MINOR-3).
fn no_targets(output: &JsonValue) -> bool {
    let p = output.get("provenance");
    let get = |k: &str| {
        p.and_then(|p| p.get(k))
            .and_then(JsonValue::as_u64)
            .unwrap_or(0)
    };
    get("targets_ok") == 0 && get("targets_failed") == 0
}

fn render_failures(output: &JsonValue, out: &mut RunOutcome) {
    if let Some(failures) = output.get("failures").and_then(JsonValue::as_array) {
        for f in failures {
            out.stderr.push_str(&format!(
                "[{}/{}] error: {}\n",
                f["node_name"].as_str().unwrap_or("?"),
                f["scope"].as_str().unwrap_or("?"),
                f["error"].as_str().unwrap_or("unknown"),
            ));
        }
    }
    if let Some(truncated) = output
        .get("provenance")
        .and_then(|p| p.get("truncated_targets"))
        .and_then(JsonValue::as_u64)
    {
        if truncated > 0 {
            out.stderr.push_str(&format!(
                "note: {truncated} targets truncated (fan-out cap)\n"
            ));
        }
    }
}

fn render_grep(output: &JsonValue) -> RunOutcome {
    let mut out = RunOutcome::default();
    let mut total = 0usize;
    if let Some(results) = output.get("results").and_then(JsonValue::as_array) {
        for block in results {
            let node = block["node_name"].as_str().unwrap_or("?");
            let scope = block["scope"].as_str().unwrap_or("?");
            if let Some(matches) = block
                .get("result")
                .and_then(|r| r.get("matches"))
                .and_then(JsonValue::as_array)
            {
                for m in matches {
                    total += 1;
                    // fs.grep MatchDto fields: path / line_number / line.
                    out.stdout.push_str(&format!(
                        "[{node}/{scope}] {}:{}: {}\n",
                        m["path"].as_str().unwrap_or("?"),
                        m["line_number"].as_u64().unwrap_or(0),
                        m["line"].as_str().unwrap_or(""),
                    ));
                }
            }
        }
    }
    render_failures(output, &mut out);
    if total == 0 {
        if no_targets(output) {
            out.stderr
                .push_str("no live nodes advertise fs.grep with configured scopes\n");
            out.code = 2;
        } else {
            out.stderr.push_str("no matches\n");
            // Exit 1 for a clean empty result; 2 when some targets
            // failed (matches may exist on the unreachable nodes).
            out.code = if out.stderr.contains("error:") { 2 } else { 1 };
        }
    }
    out
}

fn render_search(output: &JsonValue) -> RunOutcome {
    let mut out = RunOutcome::default();
    let hits = output
        .get("hits")
        .and_then(JsonValue::as_array)
        .cloned()
        .unwrap_or_default();
    for (i, hit) in hits.iter().enumerate() {
        out.stdout.push_str(&format!(
            "{:>3}. [{}/{}] {} (score {:.2})\n",
            i + 1,
            hit["node_name"].as_str().unwrap_or("?"),
            hit["scope"].as_str().unwrap_or("?"),
            hit["path"].as_str().unwrap_or("?"),
            hit["score"].as_f64().unwrap_or(0.0),
        ));
        if let Some(snippet) = hit.get("snippet").and_then(JsonValue::as_str) {
            let clean = snippet.replace('\n', " ");
            out.stdout.push_str(&format!("     {clean}\n"));
        }
    }
    render_failures(output, &mut out);
    if hits.is_empty() {
        if no_targets(output) {
            out.stderr
                .push_str("no live nodes advertise fs.search with configured scopes\n");
            out.code = 2;
        } else {
            out.stderr.push_str("no hits\n");
            out.code = if out.stderr.contains("error:") { 2 } else { 1 };
        }
    }
    out
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn progress_line_renders_ok_and_failure_variants() {
        let ok = serde_json::json!({
            "stream": "progress",
            "seq": 0,
            "line": serde_json::json!({
                "target": {"node": "x", "node_name": "peer", "scope": "notes"},
                "outcome": "ok", "items": 12,
                "completed": 3, "total": 9
            }).to_string(),
        });
        assert_eq!(
            progress_line(&ok).expect("ok line"),
            "[3/9] peer/notes: ok (12 matches)"
        );
        let failed = serde_json::json!({
            "stream": "progress",
            "seq": 1,
            "line": serde_json::json!({
                "target": {"node": "x", "node_name": "l2", "scope": "src"},
                "outcome": "failed", "error": "deadline exceeded",
                "completed": 4, "total": 9
            }).to_string(),
        });
        assert_eq!(
            progress_line(&failed).expect("failed line"),
            "[4/9] l2/src: failed — deadline exceeded"
        );
        let timed = serde_json::json!({
            "stream": "progress",
            "seq": 2,
            "line": serde_json::json!({
                "target": {"node": "x", "node_name": "l3", "scope": "a"},
                "outcome": "timed_out", "error": "timed out",
                "completed": 5, "total": 9
            }).to_string(),
        });
        assert_eq!(
            progress_line(&timed).expect("timed line"),
            "[5/9] l3/a: timed_out — timed out"
        );
    }

    #[test]
    fn progress_line_skips_non_progress_and_summary_frames() {
        let stdout = serde_json::json!({ "stream": "stdout", "seq": 0, "line": "hi" });
        assert!(progress_line(&stdout).is_none());
        let summary = serde_json::json!({
            "stream": "progress",
            "seq": 1,
            "line": r#"{"summary":{"total":2,"ok":2,"failed":0,"timed_out":0,"truncated_targets":0}}"#,
        });
        assert!(progress_line(&summary).is_none(), "summary is not a line");
        let garbage = serde_json::json!({ "stream": "progress", "seq": 2, "line": "not json" });
        assert!(progress_line(&garbage).is_none());
    }

    #[test]
    fn grep_renders_matches_with_origin_prefix() {
        let out = render_grep(&serde_json::json!({
            "results": [
                { "node_name": "a", "scope": "docs",
                  "result": { "matches": [
                      { "path": "x.md", "line_number": 3, "line": "hit" } ] } },
            ],
            "failures": [],
            "provenance": { "truncated_targets": 0 },
        }));
        assert_eq!(out.stdout, "[a/docs] x.md:3: hit\n");
        assert_eq!(out.code, 0);
    }

    #[test]
    fn grep_no_targets_is_distinct_from_no_matches() {
        // Zero targets at all → exit 2 with an actionable message.
        let out = render_grep(&serde_json::json!({
            "results": [], "failures": [],
            "provenance": { "targets_ok": 0, "targets_failed": 0, "truncated_targets": 0 }
        }));
        assert_eq!(out.code, 2);
        assert!(out.stderr.contains("no live nodes advertise"));

        // Targets ran, nothing matched → classic grep exit 1.
        let out = render_grep(&serde_json::json!({
            "results": [ { "node_name": "a", "scope": "docs",
                           "result": { "matches": [] } } ],
            "failures": [],
            "provenance": { "targets_ok": 1, "targets_failed": 0, "truncated_targets": 0 }
        }));
        assert_eq!(out.code, 1);
        assert!(out.stderr.contains("no matches"));

        // No matches AND some targets failed → exit 2.
        let out = render_grep(&serde_json::json!({
            "results": [],
            "failures": [ { "node_name": "b", "scope": "x", "error": "dead" } ],
            "provenance": { "targets_ok": 0, "targets_failed": 1, "truncated_targets": 0 }
        }));
        assert_eq!(out.code, 2);
    }

    #[test]
    fn search_ranks_and_annotates() {
        let out = render_search(&serde_json::json!({
            "hits": [
                { "node_name": "b", "scope": "notes", "path": "n.md", "score": 4.5, "snippet": "s1\ns2" },
            ],
            "failures": [ { "node_name": "c", "scope": "x", "error": "dead" } ],
        }));
        assert!(out.stdout.contains("[b/notes] n.md (score 4.50)"));
        assert!(out.stdout.contains("s1 s2"));
        assert!(out.stderr.contains("[c/x] error: dead"));
    }

    #[test]
    fn truncation_is_surfaced() {
        let mut out = RunOutcome::default();
        render_failures(
            &serde_json::json!({ "failures": [], "provenance": { "truncated_targets": 5 } }),
            &mut out,
        );
        assert!(out.stderr.contains("5 targets truncated"));
    }
}
