//! The `Capability` trait — the contract every built-in (and future
//! WASM-sandboxed) capability implements.

use async_trait::async_trait;
use harness_core::{Capability as ManifestEntry, NodeId};
use serde_json::Value as JsonValue;
use thiserror::Error;

/// Per-call execution context — what's known to a capability about the
/// caller and the local environment, plus the cancellation token tied
/// to the lease deadline.
#[derive(Debug, Clone)]
pub struct ExecutionContext {
    /// Local node id — capabilities embed this in any `Cost` / per-task
    /// `provenance` entries they emit.
    pub local_node: NodeId,
    /// Issuer of the task — for audit + provenance.
    pub issued_by: NodeId,
    /// `task.id` from the envelope.
    pub task_id: harness_core::TaskId,
}

/// Errors a capability can return from `execute`.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CapabilityError {
    #[error("invalid input: {0}")]
    InvalidInput(String),

    #[error("execution failed: {0}")]
    Failed(String),

    #[error("cancelled")]
    Cancelled,

    #[error("not implemented")]
    NotImplemented,
}

/// One capability — a typed unit of work the daemon advertises and can
/// execute. Capabilities are stateless across calls; per-call state
/// lives in the future returned by `execute`.
#[async_trait]
pub trait Capability: Send + Sync + 'static {
    /// Stable id, e.g. `"echo"` or `"shell.exec"`. Must match the id
    /// used in `Task::capability` and the `Cardinality` registration in
    /// the manifest.
    fn id(&self) -> &str;

    /// Manifest entry — the wire-format `Capability` the daemon
    /// embeds in `NodeManifest::capabilities`. The dispatcher uses
    /// `id`, `version.major`, `cardinality`, and the schema hash from
    /// this entry.
    fn manifest(&self) -> ManifestEntry;

    /// Execute one call. The runtime promises to respect the lease
    /// deadline — capabilities don't need to track time themselves
    /// for cancellation; they should periodically `await` to allow
    /// the runtime to abort on cancel.
    ///
    /// # Errors
    /// Capability-defined `CapabilityError` variants. The dispatcher
    /// translates these into the wire-format `Status::Failed` /
    /// `TimedOut` / `Cancelled` per PRD §13.4.
    async fn execute(
        &self,
        ctx: &ExecutionContext,
        input: JsonValue,
    ) -> Result<JsonValue, CapabilityError>;
}
