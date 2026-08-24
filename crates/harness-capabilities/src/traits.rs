//! The `Capability` trait — the contract every built-in (and future
//! WASM-sandboxed) capability implements.

use std::sync::Arc;

use async_trait::async_trait;
use harness_core::{Capability as ManifestEntry, NodeId};
use serde_json::Value as JsonValue;
use thiserror::Error;

/// Per-call execution context — what's known to a capability about the
/// caller and the local environment, plus the cancellation token tied
/// to the lease deadline.
#[derive(Clone)]
pub struct ExecutionContext {
    /// Local node id — capabilities embed this in any `Cost` / per-task
    /// `provenance` entries they emit.
    pub local_node: NodeId,
    /// Mesh hostname for the local node. `Arc<str>` so cloning a context
    /// is one ref-bump rather than a fresh allocation. `"unknown"` if
    /// not yet assigned (early daemon-startup window).
    pub local_node_name: Arc<str>,
    /// Issuer of the task — for audit + provenance.
    pub issued_by: NodeId,
    /// Best-known mesh hostname for the issuer; `"unknown"` if the task
    /// arrived without a resolvable name.
    pub issued_by_name: Arc<str>,
    /// `task.id` from the envelope.
    pub task_id: harness_core::TaskId,
    /// Caller hints from the `Task` envelope. Honored variably by
    /// capabilities — `llm.local.*` reads `"interactive"` to bypass
    /// the micro-batcher (3.5). `Arc<[String]>` for cheap clone.
    pub tags: Arc<[String]>,
    /// Streaming frame sink (4.6, promoted from
    /// `MeshExec::progress_sink` / `PlanExec::progress_sink` per
    /// ADR-0024's deferral): the executor stamps the daemon's
    /// partial-stream sink here so any capability can emit `Progress`
    /// frames without bespoke plumbing. `None` disables emission.
    pub frame_sink: Option<FrameSink>,
    /// 5.13a (ADR-0041): where privileged actions are recorded. The
    /// executor stamps the daemon's store-backed sink here, so a
    /// capability can audit without depending on `harness-store`
    /// (this crate is core-only by design — plan review BLOCKER-1).
    /// `None` records nothing: bare fixtures and the validation-only
    /// API context have no chain to append to.
    pub audit: Option<std::sync::Arc<dyn harness_core::AuditSink>>,
}

/// 5.13a: who asked for an execution. A task issued elsewhere is a
/// peer; one this node issued is the daemon acting on a local
/// submission. Deliberately never carries caller-supplied text
/// (plan review MAJOR-2).
#[must_use]
pub fn audit_actor(ctx: &ExecutionContext) -> harness_core::AuditActor {
    if ctx.issued_by == ctx.local_node {
        harness_core::AuditActor::System
    } else {
        harness_core::AuditActor::Peer {
            node: ctx.issued_by,
        }
    }
}

impl ExecutionContext {
    /// Record a privileged action, if this context has a sink.
    pub fn audit(&self, record: harness_core::AuditRecord) {
        if let Some(sink) = &self.audit {
            sink.record(record);
        }
    }
}

impl std::fmt::Debug for ExecutionContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Manual: `FrameSink` is an `Arc<dyn Fn>` and not `Debug`.
        f.debug_struct("ExecutionContext")
            .field("local_node", &self.local_node)
            .field("local_node_name", &self.local_node_name)
            .field("issued_by", &self.issued_by)
            .field("issued_by_name", &self.issued_by_name)
            .field("task_id", &self.task_id)
            .field("tags", &self.tags)
            .field("frame_sink", &self.frame_sink.is_some())
            .field("audit", &self.audit.is_some())
            .finish()
    }
}

/// Which child stream a [`LogFrame`] came from. Serializes as
/// `"stdout"` / `"stderr"` / `"progress"` — the same strings the wire
/// `PartialResult::output_chunk` and the API `partials` array carry.
///
/// `Progress` (4.2, ADR-0024) carries structured per-target fan-out
/// telemetry: `line` is a small JSON object, not child output. Old
/// nodes drop unknown kinds at the issuer-side allowlist — graceful
/// mixed-version behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamKind {
    Stdout,
    Stderr,
    Progress,
}

impl StreamKind {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            StreamKind::Stdout => "stdout",
            StreamKind::Stderr => "stderr",
            StreamKind::Progress => "progress",
        }
    }
}

/// One line of child output, emitted by a streaming-capable capability
/// (today: `shell.exec` built with `ShellExecCapability::with_frame_sink`)
/// as it arrives — before the process exits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogFrame {
    pub stream: StreamKind,
    /// One line, newline stripped (lossy UTF-8). Bounded — an unbroken
    /// run of bytes with no `\n` is chunked at 8 KiB (ADR-0020).
    pub line: String,
}

/// Sink for streaming line frames. Called from the capability's reader
/// tasks *while the child is still running* — implementations must be
/// non-blocking (enqueue / append; never await, never do I/O inline).
/// The `TaskId` is the executing task's id from [`ExecutionContext`],
/// so one sink instance can serve every concurrent task.
pub type FrameSink = Arc<dyn Fn(harness_core::TaskId, LogFrame) + Send + Sync>;

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

/// Which executor pool a capability runs under (4.5, ADR-0027 —
/// closing ADR-0022's cross-node wedge).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionClass {
    /// Does real local work — bounded by the executor's CPU-sized pool.
    Work,
    /// Spends its runtime awaiting OTHER tasks' results (`mesh.*`,
    /// `plan.execute`). Bounded by a wider dedicated pool, so held
    /// permits can never starve the sub-tasks they await.
    Coordination,
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

    /// Which executor pool this capability runs under. Defaulted to
    /// [`ExecutionClass::Work`] — only coordinators (capabilities whose
    /// body mostly awaits other tasks) override.
    fn execution_class(&self) -> ExecutionClass {
        ExecutionClass::Work
    }

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
