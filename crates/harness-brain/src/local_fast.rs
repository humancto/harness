//! `LocalFast` planner backend (Phase 3.9, PRD §15.1 tier 1) —
//! Ollama-backed, sync `/api/generate`, ~5-30s per goal.
//!
//! Pipeline:
//! 1. [`build_prompt`] — byte-stable prompt with capability list
//!    (id + required-fields projection), constraints, two worked
//!    examples, and the user goal at the end. 8KB byte cap with
//!    "always-include" pin + sorted-id truncation.
//! 2. POST `/api/generate` with `{model, prompt, stream: false}`.
//! 3. [`extract_json_object`] — brace-balanced JSON extractor that
//!    tolerates prose-prefix, prose-suffix, fenced/unfenced output,
//!    and multi-block (first wins).
//! 4. `serde_json::from_str::<LlmPlanResponse>` (strict; trailing
//!    commas → `Decode` error → executor escalates).
//! 5. Server-side rewrite: mint fresh `TaskId`s, rewrite edges through
//!    a `String → TaskId` map, FLIP orientation (LLM emits `(A, B)`
//!    meaning "A runs before B"; harness emits `(from, to)` meaning
//!    "from depends on to" — so we emit `(B, A)`).
//!
//! Confidence is whatever the LLM reports, clamped to `[0.0, 1.0]`.
//! `estimated_cost_usd` defaults to `0.0` (Ollama is local; the
//! capability-execution layer charges nothing). `confidence_threshold`
//! enforcement lives at the brain.plan executor, NOT here.

#![allow(clippy::items_after_statements)]

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use harness_core::protocol::ResourceHints;
use harness_core::{NodeId, Plan, PlanId, PlanNode, Signature, TaskId, Unsigned};
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};
use url::Url;

use crate::backend::{PlanOutcome, PlanRequest, PlanResponse, PlannerBackend};
use crate::error::PlannerError;
use crate::schema::CapabilitySchemaIndex;

const DEFAULT_TIMEOUT_MS: u64 = 30_000;
/// Byte cap on the prompt sent to Ollama. Bytes (not tokens) for
/// byte-stable cache friendliness; ~2300 tokens at 3.5 chars/token.
const PROMPT_BYTE_CAP: usize = 8 * 1024;

/// Capabilities that always make it into the prompt before sorted-id
/// truncation. These are the prefixes the Template backend matches; if
/// `LocalFast` is asked to plan a `run:`/`fetch:`/`summarize:`/`search:`
/// goal it must see them.
static ALWAYS_INCLUDE: &[&str] = &["shell.exec", "http.fetch", "doc.summarize", "mesh.search"];

/// Phase 3.9 `LocalFast` backend.
#[derive(Debug, Clone)]
pub struct LocalFastBackend {
    host: Url,
    client: reqwest::Client,
    model: String,
    local_node: NodeId,
    /// `"localfast:<model>"`. Stored once at construction so `id()`
    /// returns `&str` cheaply.
    id: String,
}

impl LocalFastBackend {
    /// Construct a `LocalFast` backend.
    ///
    /// # Errors
    /// Returns `Err(reqwest::Error)` if the underlying HTTP client
    /// builder fails (rare; surfaces TLS-init / OS-resource issues).
    pub fn new(host: Url, model: String, local_node: NodeId) -> Result<Self, reqwest::Error> {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(60))
            .build()?;
        let id = format!("localfast:{model}");
        Ok(Self {
            host,
            client,
            model,
            local_node,
            id,
        })
    }
}

#[async_trait]
impl PlannerBackend for LocalFastBackend {
    fn id(&self) -> &str {
        &self.id
    }

    async fn plan(&self, req: &PlanRequest) -> Result<PlanOutcome, PlannerError> {
        let prompt = build_prompt(req);
        let url = self
            .host
            .join("api/generate")
            .map_err(|e| PlannerError::Internal(format!("bad ollama host: {e}")))?;
        let body = json!({
            "model":  &self.model,
            "prompt": prompt,
            "stream": false,
        });

        let resp = self
            .client
            .post(url)
            .json(&body)
            .timeout(Duration::from_millis(DEFAULT_TIMEOUT_MS))
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() {
                    PlannerError::Timeout
                } else {
                    PlannerError::Transport(format!("ollama unreachable: {e}"))
                }
            })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(PlannerError::Transport(format!(
                "ollama returned {status}: {text}"
            )));
        }

        #[derive(Deserialize)]
        struct OllamaResp {
            response: String,
        }
        let r: OllamaResp = resp
            .json()
            .await
            .map_err(|e| PlannerError::Decode(format!("decode /api/generate envelope: {e}")))?;

        let json_text = extract_json_object(&r.response).ok_or_else(|| {
            PlannerError::Decode("no JSON object found in LLM response".to_string())
        })?;

        let llm: LlmPlanResponse = serde_json::from_str(json_text)
            .map_err(|e| PlannerError::Decode(format!("decode plan response: {e}")))?;

        let response = build_response(llm, self.local_node)?;
        Ok(PlanOutcome::Confident(Box::new(response)))
    }
}

// ───────────────────────────────────────── Prompt construction

fn build_prompt(req: &PlanRequest) -> String {
    let header = "\
You are a task planner for a typed mesh of capabilities. Output a single JSON \
object with this exact shape:

{
  \"plan\": {
    \"name\": \"<short label>\",
    \"tasks\": [
      {\"id\": \"<unique string>\", \"capability\": \"<one of the available ids>\", \"input\": {...}}
    ],
    \"edges\": [[\"<task_id_a>\", \"<task_id_b>\"]]
  },
  \"confidence\": 0.0-1.0,
  \"rationale\": \"<one sentence>\",
  \"estimated_cost_usd\": 0.0,
  \"estimated_duration_ms\": 0
}

Edge convention: each pair [\"A\",\"B\"] means \"A runs before B\" — i.e., A is a \
prerequisite of B. The planner runtime translates this to its own edge \
representation; you do not need to know about that.

Examples:

Goal: run: ls -la
{\"plan\":{\"name\":\"shell-ls\",\"tasks\":[{\"id\":\"s1\",\"capability\":\"shell.exec\",\"input\":{\"cmd\":\"ls\",\"args\":[\"-la\"]}}],\"edges\":[]},\"confidence\":0.9,\"rationale\":\"single shell command\",\"estimated_cost_usd\":0.0,\"estimated_duration_ms\":50}

Goal: read README.md
{\"plan\":{\"name\":\"read-readme\",\"tasks\":[{\"id\":\"r1\",\"capability\":\"shell.exec\",\"input\":{\"cmd\":\"cat\",\"args\":[\"README.md\"]}}],\"edges\":[]},\"confidence\":0.85,\"rationale\":\"single file read\",\"estimated_cost_usd\":0.0,\"estimated_duration_ms\":20}

";

    // Cap list — projected to id + required-fields.
    let projections = project_capabilities(&req.available_capabilities, &req.schemas);

    // Fixed-overhead bytes (header + constraints + goal). Reserve room
    // for them; the cap list takes whatever's left.
    let constraints_block = format_constraints(&req.constraints);
    let goal_line = format!("\nGoal: {}\n", req.goal);
    let fixed_overhead = header.len() + constraints_block.len() + goal_line.len() + 64;
    let cap_budget = PROMPT_BYTE_CAP.saturating_sub(fixed_overhead);

    let cap_block = render_capabilities(&projections, cap_budget);

    let mut out = String::with_capacity(PROMPT_BYTE_CAP);
    out.push_str(header);
    out.push_str(&cap_block);
    out.push('\n');
    out.push_str(&constraints_block);
    out.push_str(&goal_line);
    out
}

fn format_constraints(c: &crate::backend::PlanConstraints) -> String {
    format!(
        "\nConstraints:\n- max_cost_usd: {}\n- must_be_local: {}\n- plan_max_nodes: {}\n",
        c.max_cost_usd
            .map_or_else(|| "none".to_string(), |v| format!("{v:.2}")),
        c.must_be_local,
        c.plan_max_nodes
            .map_or_else(|| "none".to_string(), |v| v.to_string()),
    )
}

#[derive(Debug, Clone)]
struct CapProjection {
    id: String,
    /// Comma-separated `<name>: <type>` for each required field. Empty
    /// when the cap has no schema (e.g. input-override path) or no
    /// required fields.
    required_summary: String,
}

fn project_capabilities(
    caps: &[crate::backend::CapabilityRef],
    schemas: &CapabilitySchemaIndex,
) -> Vec<CapProjection> {
    let mut out: Vec<CapProjection> = caps
        .iter()
        .map(|c| {
            let summary = schemas
                .get(&c.id)
                .and_then(|_v| schemas_required_summary(&c.id, schemas))
                .unwrap_or_default();
            CapProjection {
                id: c.id.clone(),
                required_summary: summary,
            }
        })
        .collect();
    // Sort by id so prompts are byte-stable across invocations.
    out.sort_by(|a, b| a.id.cmp(&b.id));
    out
}

/// Best-effort projection: we hold a compiled `Validator` but the
/// upstream API doesn't expose the raw schema, so we cannot extract
/// the `required` field from a compiled validator. Today we return
/// `None` (no field summary); a future enhancement may stash the raw
/// schema next to the validator. Documented in ADR-0014 §11.
fn schemas_required_summary(_cap_id: &str, _schemas: &CapabilitySchemaIndex) -> Option<String> {
    None
}

fn render_capabilities(projections: &[CapProjection], byte_budget: usize) -> String {
    let mut out = String::from("Available capabilities:\n");
    let mut total_cap_bytes = 0usize;

    // Pin always-include caps first so they survive truncation.
    let mut emitted: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for must in ALWAYS_INCLUDE {
        if let Some(p) = projections.iter().find(|p| p.id == *must) {
            let line = render_one(p);
            if total_cap_bytes + line.len() > byte_budget {
                break;
            }
            out.push_str(&line);
            total_cap_bytes += line.len();
            emitted.insert(p.id.as_str());
        }
    }

    let mut truncated = 0usize;
    for p in projections {
        if emitted.contains(p.id.as_str()) {
            continue;
        }
        let line = render_one(p);
        if total_cap_bytes + line.len() > byte_budget {
            truncated += 1;
            continue;
        }
        out.push_str(&line);
        total_cap_bytes += line.len();
    }

    if truncated > 0 {
        tracing::warn!(
            truncated_caps = truncated,
            budget_bytes = byte_budget,
            "LocalFast prompt: truncated capabilities to fit byte budget"
        );
    }
    out
}

fn render_one(p: &CapProjection) -> String {
    if p.required_summary.is_empty() {
        format!("- {}\n", p.id)
    } else {
        format!("- {} — required: {}\n", p.id, p.required_summary)
    }
}

// ───────────────────────────────────────── JSON extraction

/// Extract the first top-level JSON object from `s`, ignoring any
/// prose / markdown fences before or after. Walks bytes tracking
/// string-literal state and brace depth.
///
/// Returns `None` if no balanced `{...}` is found.
#[must_use]
pub fn extract_json_object(s: &str) -> Option<&str> {
    let bytes = s.as_bytes();
    let mut start: Option<usize> = None;
    let mut depth: i32 = 0;
    let mut in_string = false;
    let mut escape = false;

    for (i, &b) in bytes.iter().enumerate() {
        if in_string {
            if escape {
                escape = false;
            } else if b == b'\\' {
                escape = true;
            } else if b == b'"' {
                in_string = false;
            }
            continue;
        }
        match b {
            b'"' => in_string = true,
            b'{' => {
                if start.is_none() {
                    start = Some(i);
                }
                depth += 1;
            }
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    if let Some(s_idx) = start {
                        return Some(&s[s_idx..=i]);
                    }
                }
            }
            _ => {}
        }
    }
    None
}

// ───────────────────────────────────────── LLM-side shape

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LlmPlanResponse {
    plan: LlmPlan,
    confidence: f64,
    #[serde(default)]
    rationale: String,
    #[serde(default)]
    estimated_cost_usd: f64,
    #[serde(default)]
    estimated_duration_ms: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LlmPlan {
    #[serde(default)]
    name: String,
    tasks: Vec<LlmPlanNode>,
    #[serde(default)]
    edges: Vec<(String, String)>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LlmPlanNode {
    id: String,
    capability: String,
    #[serde(default)]
    input: JsonValue,
}

// ───────────────────────────────────────── Server-side rewrite

fn build_response(llm: LlmPlanResponse, local_node: NodeId) -> Result<PlanResponse, PlannerError> {
    if llm.plan.tasks.is_empty() {
        return Err(PlannerError::Decode(
            "LLM emitted plan with zero tasks".to_string(),
        ));
    }

    // 1. Mint fresh TaskIds; remember the LLM's string ids for edge
    //    rewrite. The LLM's labels are tracing-only — putting them
    //    into PlanNode.input would fail every cap with
    //    `additionalProperties: false`.
    let id_map: HashMap<String, TaskId> = llm
        .plan
        .tasks
        .iter()
        .map(|n| (n.id.clone(), TaskId::new_v7()))
        .collect();

    let mut tasks: HashMap<TaskId, PlanNode> = HashMap::with_capacity(llm.plan.tasks.len());
    for n in llm.plan.tasks {
        let task_id = id_map[&n.id];
        // info! (not debug!) so the rewrite is visible in production
        // logs without RUST_LOG=debug — operators tracing a planning
        // failure want to see what step labels became which TaskIds.
        // No explicit `target:` so `tracing_test` (which filters by
        // module path) sees this event in the test harness.
        tracing::info!(
            llm_step_label = %n.id,
            minted_task_id = ?task_id,
            capability = %n.capability,
            "rewrote LLM step id"
        );
        tasks.insert(
            task_id,
            PlanNode {
                id: task_id,
                capability: n.capability,
                input: n.input,
                resource_hints: ResourceHints {
                    cpu_class: harness_core::protocol::CpuClass::Light,
                    memory_mb: None,
                    gpu_required: false,
                    gpu_memory_mb: None,
                    network_class: harness_core::protocol::NetworkClass::None,
                    disk_io_class: harness_core::protocol::DiskIoClass::None,
                    estimated_duration_ms: None,
                },
                timeout_ms: None,
            },
        );
    }

    // 2. Rewrite + flip edges. LLM "(A, B) = A runs before B"; harness
    //    "(from, to) = from depends on to". So harness emits (B, A) —
    //    "B depends on A" = "A must complete first." Round-trip
    //    invariant: any chain the LLM emits validates as acyclic via
    //    `validate_plan`.
    let mut edges: Vec<(TaskId, TaskId)> = Vec::with_capacity(llm.plan.edges.len());
    for (a_str, b_str) in llm.plan.edges {
        let a = id_map.get(&a_str).ok_or_else(|| {
            PlannerError::Decode(format!("edge references unknown task id {a_str:?}"))
        })?;
        let b = id_map.get(&b_str).ok_or_else(|| {
            PlannerError::Decode(format!("edge references unknown task id {b_str:?}"))
        })?;
        // Flip: harness `(from, to)` = "from depends on to".
        edges.push((*b, *a));
    }

    let plan = Plan {
        id: PlanId::new_v7(),
        name: llm.plan.name,
        tasks,
        edges,
        budget: None,
        checkpoint: None,
        issued_by: local_node,
        sig: Signature::from_bytes([0u8; Signature::LEN]),
    };

    Ok(PlanResponse {
        plan: Unsigned(plan),
        confidence: llm.confidence.clamp(0.0, 1.0),
        rationale: llm.rationale,
        estimated_cost_usd: llm.estimated_cost_usd,
        estimated_duration_ms: llm.estimated_duration_ms,
        fallback_plan: None,
    })
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod unit_tests {
    use super::*;

    #[test]
    fn extract_bare_object() {
        assert_eq!(extract_json_object("{\"a\":1}").unwrap(), "{\"a\":1}");
    }

    #[test]
    fn extract_with_prose_prefix_and_suffix() {
        let s = "Here is the plan:\n```json\n{\"a\":1}\n```\nLet me know.";
        assert_eq!(extract_json_object(s).unwrap(), "{\"a\":1}");
    }

    #[test]
    fn extract_handles_braces_in_strings() {
        let s = r#"prose {"label":"{not a brace}","x":1} more"#;
        assert_eq!(
            extract_json_object(s).unwrap(),
            r#"{"label":"{not a brace}","x":1}"#
        );
    }

    #[test]
    fn extract_handles_escaped_quotes() {
        let s = r#"junk {"a":"hello \"world\""} junk"#;
        assert_eq!(
            extract_json_object(s).unwrap(),
            r#"{"a":"hello \"world\""}"#
        );
    }

    #[test]
    fn extract_first_of_two_blocks() {
        let s = r#"{"a":1} stuff {"b":2}"#;
        assert_eq!(extract_json_object(s).unwrap(), r#"{"a":1}"#);
    }

    #[test]
    fn extract_unbalanced_returns_none() {
        assert!(extract_json_object("{\"a\":1").is_none());
    }

    #[test]
    fn extract_no_object_returns_none() {
        assert!(extract_json_object("hello world").is_none());
    }
}
