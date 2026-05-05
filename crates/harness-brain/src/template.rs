//! Template backend (PRD §15.6) — the lowest planner tier. Pure
//! pattern matching against a static prefix table; no LLM, no I/O,
//! deterministic. Demoes on a model-less laptop with no internet.
//!
//! Template's confidence is hard-coded at [`TEMPLATE_CONFIDENCE`] (0.6)
//! — below `mesh.planning.confidence_threshold` (default 0.7 per PRD
//! §15.2), so a 3.9 `LocalFast` backend returning 0.85+ wins the
//! escalation chain. Phase 3.8 has no threshold check at the executor
//! (every `Confident(_)` outcome is accepted) so Template plans ship
//! uncontested when `LocalFast` is not registered.

use std::collections::HashMap;

use async_trait::async_trait;
use harness_core::protocol::{CpuClass, DiskIoClass, NetworkClass, ResourceHints};
use harness_core::{NodeId, Plan, PlanId, PlanNode, Signature, TaskId, Unsigned};
use serde_json::json;

use crate::backend::{PlanOutcome, PlanRequest, PlanResponse, PlannerBackend};
use crate::error::PlannerError;

/// Confidence Template returns on a successful match. Below
/// `mesh.planning.confidence_threshold` (0.7) by design.
pub const TEMPLATE_CONFIDENCE: f64 = 0.6;

/// Backend identifier — re-exposed for the executor's diagnostic
/// strings.
pub const BACKEND_ID: &str = "template";

/// Static pattern table. Adding a new prefix is one row + one
/// `emit_*` function — no enum, no match arms. The capability column
/// gates whether the matched plan is emitted: if the capability is
/// absent from `req.available_capabilities`, the outcome is
/// [`PlanOutcome::MatchedButUnsupported`].
static PATTERNS: &[(&str, &str)] = &[
    ("run:", "shell.exec"),
    ("shell:", "shell.exec"),
    ("search:", "mesh.search"),
    ("fetch:", "http.fetch"),
    ("summarize:", "doc.summarize"),
    ("schedule:", "schedule.cron"),
];

/// Pure pattern-match planner — the Phase 3.8 default tier.
#[derive(Debug, Clone)]
pub struct TemplateBackend {
    /// Local node id — embedded as `Plan::issued_by` on emitted plans.
    /// Bound at construction so per-call has no `NodeId` lookup.
    local_node: NodeId,
}

impl TemplateBackend {
    #[must_use]
    pub fn new(local_node: NodeId) -> Self {
        Self { local_node }
    }
}

#[async_trait]
impl PlannerBackend for TemplateBackend {
    fn id(&self) -> &str {
        BACKEND_ID
    }

    async fn plan(&self, req: &PlanRequest) -> Result<PlanOutcome, PlannerError> {
        let goal = req.goal.trim_start();

        // Find the matching prefix. No match → silent escalate.
        let Some(&(pattern, capability)) = PATTERNS.iter().find(|(p, _)| goal.starts_with(p))
        else {
            return Ok(PlanOutcome::NoMatch);
        };

        // Capability gate: if the cap isn't registered locally (or in
        // the caller-supplied override list), surface the diagnostic.
        let cap_available = req
            .available_capabilities
            .iter()
            .any(|c| c.id == capability);
        if !cap_available {
            tracing::debug!(
                pattern = pattern,
                missing_cap = capability,
                "template prefix matched but capability unavailable; escalating with diagnostic"
            );
            return Ok(PlanOutcome::MatchedButUnsupported {
                matched_pattern: pattern,
                missing_capability: capability.to_string(),
            });
        }

        // Body extraction: everything after the prefix, trimmed of
        // whitespace.
        let body = goal.get(pattern.len()..).map(str::trim).unwrap_or_default();

        // Today only `shell.exec` has an emit path; the other
        // capabilities (`mesh.search`, `http.fetch`, `doc.summarize`,
        // `schedule.cron`) light up automatically when the
        // corresponding `emit_*` is added in their phases.
        if capability == "shell.exec" {
            self.emit_shell(pattern, body)
        } else {
            tracing::warn!(
                pattern = pattern,
                capability = capability,
                "template matched a prefix whose capability has no emit path yet"
            );
            Ok(PlanOutcome::NoMatch)
        }
    }
}

impl TemplateBackend {
    /// Wrapped in `Result` for symmetry with `PlannerBackend::plan` —
    /// the body never returns `Err` today, but a future shell-emit
    /// path that wants to surface a transport error (e.g. signing the
    /// plan) drops in cleanly.
    #[allow(clippy::unnecessary_wraps)]
    fn emit_shell(&self, pattern: &'static str, body: &str) -> Result<PlanOutcome, PlannerError> {
        // Body parse failures (unclosed quote, empty argv) are NOT
        // backend errors — they're user-input shape problems. Surface
        // as NoMatch so a future LocalFast backend can rescue the
        // goal.
        let Some(tokens) = shlex::split(body) else {
            tracing::warn!(
                pattern = pattern,
                "template body failed to parse via shlex (likely unclosed quote)"
            );
            return Ok(PlanOutcome::NoMatch);
        };
        let Some((cmd, rest)) = tokens.split_first() else {
            return Ok(PlanOutcome::NoMatch);
        };
        let args: Vec<String> = rest.to_vec();

        // Build the one-task plan. `shell.exec`'s schema (verified
        // against crates/harness-capabilities/src/shell.rs) takes
        // {cmd: "ls", args: ["-la"]} — `cmd` is the program path,
        // `args` is the argv tail. NO shell expansion: shell.exec
        // calls `Command::new` not `sh -c`, so $VAR / backticks /
        // redirects pass through as literal argv elements. ADR-0013 §9.
        let task_id = TaskId::new_v7();
        let node = PlanNode {
            id: task_id,
            capability: "shell.exec".to_string(),
            input: json!({ "cmd": cmd, "args": args }),
            resource_hints: ResourceHints {
                cpu_class: CpuClass::Light,
                memory_mb: None,
                gpu_required: false,
                gpu_memory_mb: None,
                network_class: NetworkClass::None,
                disk_io_class: DiskIoClass::Light,
                estimated_duration_ms: None,
            },
            timeout_ms: None,
        };

        let mut tasks = HashMap::with_capacity(1);
        tasks.insert(task_id, node);
        let plan = Plan {
            id: PlanId::new_v7(),
            name: format!("template:{pattern}"),
            tasks,
            edges: vec![],
            budget: None,
            checkpoint: None,
            issued_by: self.local_node,
            // Unsigned — the dispatcher signs before fan-out.
            sig: Signature::from_bytes([0u8; Signature::LEN]),
        };

        Ok(PlanOutcome::Confident(Box::new(PlanResponse {
            plan: Unsigned(plan),
            confidence: TEMPLATE_CONFIDENCE,
            rationale: format!(
                "matched template pattern {pattern:?} → emitted single shell.exec task"
            ),
            estimated_cost_usd: 0.0,
            estimated_duration_ms: 0,
            fallback_plan: None,
        })))
    }
}
