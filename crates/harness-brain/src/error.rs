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

/// Structural well-formedness failures for a planner-emitted [`harness_core::Plan`].
///
/// Phase 3.8 ships well-formedness checks only (acyclic, caps-exist,
/// non-empty, dangling-edge); 3.9 layers on JSON-Schema validation,
/// cost-cap, and `must_be_local` consistency.
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
}
