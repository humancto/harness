//! Errors produced by the planner runtime.

use harness_core::TaskId;
use thiserror::Error;

/// Backend transport / protocol failures. Pure-pattern backends like
/// [`crate::template::TemplateBackend`] never produce these; LLM-backed
/// backends (3.9+) emit `Transport` for HTTP issues and `Decode` for
/// malformed responses.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum PlannerError {
    #[error("backend transport failed: {0}")]
    Transport(String),

    #[error("backend response malformed: {0}")]
    Decode(String),

    #[error("backend timed out")]
    Timeout,

    #[error("backend internal error: {0}")]
    Internal(String),
}

/// Validation failures for a planner-emitted [`harness_core::Plan`].
///
/// Phase 3.8 shipped the structural-well-formedness subset (`Empty`,
/// `DanglingEdge`, `Cycle`, `UnknownCapability`). Phase 3.9 layers on
/// `SchemaViolation` (input doesn't match the cap's `input_schema`),
/// `UnknownSchema` (cap is registered but its schema didn't compile,
/// or the input-override path supplied a foreign cap), and
/// `CostExceeded` (planner's `estimated_cost_usd` over the
/// `max_cost_usd` cap).
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum PlanValidationError {
    #[error("plan has no tasks")]
    Empty,

    #[error("edge ({from:?}, {to:?}) references task id not in plan.tasks")]
    DanglingEdge { from: TaskId, to: TaskId },

    #[error("plan has a cycle")]
    Cycle,

    #[error("task {task:?} references capability {cap:?} which is not in available_capabilities")]
    UnknownCapability { task: TaskId, cap: String },

    #[error("task {task:?} input fails schema for capability {cap:?}: {errors:?}")]
    SchemaViolation {
        task: TaskId,
        cap: String,
        errors: Vec<String>,
    },

    #[error(
        "task {task:?} references capability {cap:?} whose input_schema is not compiled \
         (foreign cap, or schema failed to compile at registry build time)"
    )]
    UnknownSchema { task: TaskId, cap: String },

    #[error("estimated cost ${estimated_usd:.4} exceeds cap ${max_usd:.4}")]
    CostExceeded { estimated_usd: f64, max_usd: f64 },
}
