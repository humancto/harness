//! Shared LLM-planner plumbing (5.2, ADR-0031) — prompt construction,
//! JSON extraction, and the LLM-side response shape + server-side
//! rewrite. Used by every LLM-backed tier: `LocalFast` / `LocalStrong`
//! (Ollama, `localfast` feature) and `Cloud` (Anthropic, `cloud`
//! feature). Pure move out of `local_fast.rs`; the pipeline semantics
//! are documented there and unchanged.

use std::collections::HashMap;

use harness_core::protocol::ResourceHints;
use harness_core::{NodeId, Plan, PlanId, PlanNode, Signature, TaskId, Unsigned};
use serde::Deserialize;
use serde_json::Value as JsonValue;

use crate::backend::{PlanRequest, PlanResponse};
use crate::error::PlannerError;
use crate::schema::CapabilitySchemaIndex;

/// Capabilities that always make it into the prompt before sorted-id
/// truncation. These are the prefixes the Template backend matches; if
/// an LLM tier is asked to plan a `run:`/`fetch:`/`summarize:`/`search:`
/// goal it must see them.
static ALWAYS_INCLUDE: &[&str] = &["shell.exec", "http.fetch", "doc.summarize", "mesh.search"];

// ───────────────────────────────────────── Prompt construction

pub(crate) fn build_prompt(req: &PlanRequest, prompt_byte_cap: usize) -> String {
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

    // Fixed-overhead bytes (header + constraints + repair + goal).
    // Reserve room for them; the cap list takes whatever's left.
    let constraints_block = format_constraints(&req.constraints);
    let repair_block = req.repair.as_deref().map_or_else(String::new, repair_block);
    let goal_line = format!("\nGoal: {}\n", req.goal);
    let fixed_overhead =
        header.len() + constraints_block.len() + repair_block.len() + goal_line.len() + 64;
    let cap_budget = prompt_byte_cap.saturating_sub(fixed_overhead);

    let cap_block = render_capabilities(&projections, cap_budget);

    let mut out = String::with_capacity(prompt_byte_cap);
    out.push_str(header);
    out.push_str(&cap_block);
    out.push('\n');
    out.push_str(&constraints_block);
    out.push_str(&repair_block);
    out.push_str(&goal_line);
    out
}

/// 5.3 (ADR-0032): replanning repair hint, byte-capped so a verbose
/// validation error (e.g. `SchemaViolation` embedding instance
/// values) cannot starve the capability list — or burn paid tokens
/// on the cloud tier.
const REPAIR_BYTE_CAP: usize = 1024;

fn repair_block(error: &str) -> String {
    let mut e = error;
    if e.len() > REPAIR_BYTE_CAP {
        // Char-safe truncation: back off to a boundary.
        let mut cut = REPAIR_BYTE_CAP;
        while cut > 0 && !e.is_char_boundary(cut) {
            cut -= 1;
        }
        e = &e[..cut];
    }
    format!("\nYour previous plan failed validation: {e}\nEmit a corrected plan that fixes exactly this.\n")
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
            "planner prompt: truncated capabilities to fit byte budget"
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
                // Clamp at zero: a stray `}` in prose before the
                // first `{` (LLM saying "the closing `}` belongs to
                // the previous block, here's the plan: {...}") must
                // NOT desync the counter and consume the real
                // object's closer.
                if depth > 0 {
                    depth -= 1;
                    if depth == 0 {
                        if let Some(s_idx) = start {
                            return Some(&s[s_idx..=i]);
                        }
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
pub(crate) struct LlmPlanResponse {
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

pub(crate) fn build_response(
    llm: LlmPlanResponse,
    local_node: NodeId,
) -> Result<PlanResponse, PlannerError> {
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
    fn built_plans_never_carry_a_budget() {
        // 5.8 (ADR-0036): §17.8's "a plan-carried Budget is explicit
        // approval" model relies on the planner being UNABLE to
        // self-approve. Two layers, both pinned here: the LLM wire
        // schema has no budget field (deny_unknown_fields makes one a
        // parse error), and the built Plan hardcodes budget: None.
        let raw = r#"{"plan":{"name":"t","tasks":[{"id":"a","capability":"echo",
            "input":{}}],"edges":[]},"confidence":0.9}"#;
        let llm: LlmPlanResponse = serde_json::from_str(raw).expect("parse");
        let resp = build_response(llm, NodeId::from_bytes([7; 16])).expect("build");
        assert!(resp.plan.0.budget.is_none(), "planner minted a budget");

        let smuggled = r#"{"plan":{"name":"t","budget":{"max_cost_usd":null,
            "soft_limit_usd":null,"on_exceed":"notify"},
            "tasks":[{"id":"a","capability":"echo","input":{}}],"edges":[]},
            "confidence":0.9}"#;
        assert!(
            serde_json::from_str::<LlmPlanResponse>(smuggled).is_err(),
            "a budget field in the LLM response must be a parse error"
        );
    }

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

    #[test]
    fn extract_handles_stray_close_in_prose() {
        // Stray `}` in prose before the real object — must not
        // desync the brace counter.
        let s = "} prose {\"a\":1}";
        assert_eq!(extract_json_object(s).unwrap(), "{\"a\":1}");
    }

    #[test]
    fn extract_handles_multiple_stray_closes() {
        let s = "}}} eventually {\"x\":42} more";
        assert_eq!(extract_json_object(s).unwrap(), "{\"x\":42}");
    }

    fn req_with_repair(repair: Option<String>) -> crate::backend::PlanRequest {
        crate::backend::PlanRequest {
            goal: "run: ls".to_string(),
            available_capabilities: vec![],
            schemas: CapabilitySchemaIndex::default(),
            constraints: crate::backend::PlanConstraints::default(),
            context: None,
            issuing_node: NodeId::from_bytes([7; 16]),
            repair,
        }
    }

    #[test]
    fn prompt_includes_repair_block_only_when_set() {
        let plain = build_prompt(&req_with_repair(None), 8 * 1024);
        assert!(!plain.contains("failed validation"));

        let repaired = build_prompt(
            &req_with_repair(Some("plan has a cycle".to_string())),
            8 * 1024,
        );
        assert!(repaired.contains("Your previous plan failed validation: plan has a cycle"));
        assert!(repaired.contains("Emit a corrected plan"));
        // Repair sits before the goal line so the goal stays last
        // (rfind: the header's worked examples also contain "Goal:").
        let r = repaired.find("failed validation").unwrap();
        let g = repaired.rfind("\nGoal:").unwrap();
        assert!(r < g, "repair block precedes the goal line");
    }

    #[test]
    fn repair_block_truncates_char_safely_at_cap() {
        // 2-byte chars straddling the cap must not panic and must
        // stay under cap + framing.
        let long = "é".repeat(REPAIR_BYTE_CAP); // 2×cap bytes
        let block = repair_block(&long);
        assert!(block.len() < REPAIR_BYTE_CAP + 128);
        assert!(block.contains("failed validation"));
    }
}
