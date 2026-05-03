//! Executor — the part of the orchestrator that runs *on* the executing
//! node, after the dispatcher has chosen it. Phase 3.1 adds only the
//! policy gate; 3.2 will land the actual `shell.exec` capability that
//! consumes it.

use harness_policy::{Decision, EvalContext, PolicyEngine};
use thiserror::Error;

/// Errors a capability execution path can return *before* the capability
/// itself is invoked. Distinct from `DispatchError` (which is about
/// routing decisions on the dispatcher node) — these errors are raised
/// on the executor node.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ExecutorError {
    #[error("policy denied: {reason}")]
    PolicyDenied { reason: String },
}

/// Consult the local policy engine for an action. Returns `Ok(())` on
/// allow, `Err(PolicyDenied { reason })` on deny. The reason is what the
/// engine produced — fine to surface back to the caller (it's not a
/// secret; it just names the rule that fired).
pub fn policy_check(engine: &PolicyEngine, ctx: &EvalContext<'_>) -> Result<(), ExecutorError> {
    match engine.evaluate(ctx) {
        Decision::Allow => Ok(()),
        Decision::Deny { reason } => Err(ExecutorError::PolicyDenied { reason }),
        // `Decision` is `#[non_exhaustive]`; a future variant we don't
        // yet know how to translate must fail closed.
        _ => Err(ExecutorError::PolicyDenied {
            reason: "unknown policy decision (fail-closed)".to_string(),
        }),
    }
}
