//! Mesh meta-capabilities (roadmap 3.11, PRD §16.8): `mesh.grep` and
//! `mesh.search` — federated wrappers that fan a query out to every
//! node/scope advertising the underlying `fs.*` capability and merge
//! the results.
//!
//! Design (ADR-0022):
//! - Fan-out unit is a **(node, scope) pair**. Targets come from the
//!   stored `NodeManifest`s (self included) filtered to live nodes and
//!   to nodes advertising the wrapped capability.
//! - **Self-owned scopes execute in-process** through the (weak)
//!   capability registry — no store round-trip, and critically no
//!   second executor permit, which removes the wrapper-starves-its-own-
//!   sub-task deadlock. Remote scopes go through the normal dispatch
//!   path as pinned sub-tasks (`Task.parent` = the wrapper's task id).
//! - Merge: `mesh.grep` → **Concat** with per-item `{node, scope}`
//!   provenance; `mesh.search` → flatten + sort by score descending
//!   (the PRD's *Rerank* default degrades to score-sort until a
//!   reranker capability exists — Phase 5).
//! - Bounds: at most [`MAX_FANOUT_TARGETS`] pairs (excess reported in
//!   `truncated_targets`, never silently dropped); per-call global
//!   timeout (`timeout_ms`, default 30 s, cap 120 s); per-node failures
//!   land in `failures` while successful nodes still return results
//!   (`ReturnPartial` semantics).
//! - Recursion guard: only `fs.*` sub-capabilities are ever invoked.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt as _;
use harness_core::{Capability as ManifestEntry, Cardinality, NodeId, PartialPolicy, TaskId};
use harness_orchestrator::{
    task_results, EndReason, FanoutController, FanoutSpec, FanoutSummary, ItemOutcome, ResultCtx,
    ResultMapper, WindowPolicy,
};
use serde_json::{json, Value as JsonValue};

use crate::traits::{Capability, CapabilityError, ExecutionContext, FrameSink, LogFrame, StreamKind};

/// Ceiling on fan-out (node, scope) pairs per call.
pub const MAX_FANOUT_TARGETS: usize = 64;
/// Concurrent in-process `fs.*` scans per wrapper call (review MAJOR-2).
const LOCAL_SCAN_CONCURRENCY: usize = 4;
const DEFAULT_TIMEOUT_MS: u64 = 30_000;
const MAX_TIMEOUT_MS: u64 = 120_000;

/// One fan-out destination: a node and the scopes it owns.
#[derive(Debug, Clone)]
pub struct MeshTarget {
    pub node_id: NodeId,
    pub node_name: String,
    pub is_self: bool,
    pub scopes: Vec<String>,
    pub capabilities: Vec<String>,
}

/// Terminal outcome of a remote sub-task.
#[derive(Debug, Clone)]
pub enum SubTaskOutcome {
    Done(JsonValue),
    Failed(String),
    TimedOut,
}

/// The daemon-side services a mesh meta-capability needs: target
/// discovery, in-process execution for self-owned scopes, and remote
/// sub-task dispatch. Implemented in `harness-daemon` over the store,
/// identity, peer table, and (weak) capability registry.
#[async_trait]
pub trait MeshExec: Send + Sync + 'static {
    /// Live fan-out targets (self first), from stored manifests.
    fn targets(&self) -> Vec<MeshTarget>;

    /// Execute a capability in-process (self-owned scopes). MUST refuse
    /// `mesh.*` ids (recursion guard belongs to the implementation too).
    async fn run_local(
        &self,
        capability: &str,
        ctx: &ExecutionContext,
        input: JsonValue,
    ) -> Result<JsonValue, CapabilityError>;

    /// Submit a pinned remote sub-task (signed locally, routed by the
    /// dispatch runtime). Returns the sub-task id.
    fn submit_remote(
        &self,
        capability: &str,
        input: JsonValue,
        pin_to: NodeId,
        parent: TaskId,
        timeout_ms: u32,
    ) -> Result<TaskId, CapabilityError>;

    /// Await the sub-task's terminal result, up to `deadline`.
    async fn await_terminal(&self, id: TaskId, deadline: Duration) -> SubTaskOutcome;

    /// Sink for per-target progress frames (4.2, ADR-0024). `None`
    /// (the default) disables emission — pure unit-test fakes stay
    /// silent, and the daemon wires the partial-stream sink in.
    fn progress_sink(&self) -> Option<crate::traits::FrameSink> {
        None
    }
}

fn clamp_timeout(input: &JsonValue) -> u64 {
    input
        .get("timeout_ms")
        .and_then(JsonValue::as_u64)
        .unwrap_or(DEFAULT_TIMEOUT_MS)
        .clamp(1_000, MAX_TIMEOUT_MS)
}

/// Shared fan-out driver for both wrappers (rewired through
/// `FanoutController` in 4.1 — ADR-0023). `sub_input(scope)` builds the
/// per-scope `fs.*` input.
///
/// Two controller instances run concurrently: self-owned scopes through
/// a `Fixed(LOCAL_SCAN_CONCURRENCY)` window (ADR-0022's "at most 4
/// concurrent scans" bound stands), remote pairs through the §14.7
/// `2 × N_workers` window — so remote sub-task **rows are inserted
/// O(window) at a time**, never all up front, while local and remote
/// work still overlap.
async fn fan_out(
    exec: &Arc<dyn MeshExec>,
    ctx: &ExecutionContext,
    sub_capability: &'static str,
    sub_input: Arc<dyn Fn(&str) -> JsonValue + Send + Sync>,
    timeout: Duration,
) -> FanOutOutcome {
    let (pairs, truncated_targets) = collect_pairs(exec, sub_capability);
    let (local_pairs, remote_pairs): (Vec<_>, Vec<_>) =
        pairs.into_iter().partition(|(t, _)| t.is_self);

    // Late-pulled remote pairs get the REMAINING wrapper budget, not the
    // full timeout (diff review MAJOR-1): before 4.1 every row was
    // submitted at T≈0, so row timeout_ms and lease TTL expired in
    // wall-clock alignment with the wrapper deadline; windowed pull must
    // recompute the budget at pull time to keep ADR-0022's "sub-task
    // timeout ≤ wrapper deadline" orphan bound. Floor of 1 s so a pair
    // pulled at the wire never gets a degenerate zero-timeout row.
    let started = tokio::time::Instant::now();
    let remaining_budget = move || {
        timeout
            .saturating_sub(started.elapsed())
            .max(Duration::from_secs(1))
    };

    let local_spec = {
        let exec = exec.clone();
        let ctx = ctx.clone();
        let sub_input = sub_input.clone();
        FanoutSpec {
            source: futures::stream::iter(local_pairs.clone()).boxed(),
            run: Box::new(move |_index, (_target, scope): (MeshTarget, String)| {
                let exec = exec.clone();
                let ctx = ctx.clone();
                let input = sub_input(&scope);
                Box::pin(async move {
                    match tokio::time::timeout(timeout, exec.run_local(sub_capability, &ctx, input))
                        .await
                    {
                        Ok(Ok(v)) => ItemOutcome::Ok(v),
                        Ok(Err(e)) => ItemOutcome::Failed(e.to_string()),
                        Err(_) => ItemOutcome::TimedOut,
                    }
                }) as futures::future::BoxFuture<'static, ItemOutcome<JsonValue>>
            }),
            window: WindowPolicy::Fixed(LOCAL_SCAN_CONCURRENCY),
            live_workers: Box::new(|| 1),
            deadline: Some(Box::pin(tokio::time::sleep(timeout))),
            on_failure: PartialPolicy::ReturnPartial,
        }
    };

    let remote_spec = {
        let exec = exec.clone();
        let task_id = ctx.task_id;
        // Distinct remote node count drives the 2×N window (constant
        // snapshot — targets are already live-filtered; dynamic
        // recompute belongs to the long-running consumers, 4.5).
        let workers = {
            let mut ids: Vec<NodeId> = remote_pairs.iter().map(|(t, _)| t.node_id).collect();
            ids.sort_unstable();
            ids.dedup();
            ids.len()
        };
        FanoutSpec {
            source: futures::stream::iter(remote_pairs.clone()).boxed(),
            run: Box::new(move |_index, (target, scope): (MeshTarget, String)| {
                let exec = exec.clone();
                let input = sub_input(&scope);
                let budget = remaining_budget();
                #[allow(clippy::cast_possible_truncation)]
                let budget_ms = budget.as_millis().min(u128::from(u32::MAX)) as u32;
                Box::pin(async move {
                    // Row insertion happens HERE, inside the pulled
                    // future — an unpulled pair has no task row.
                    match exec.submit_remote(
                        sub_capability,
                        input,
                        target.node_id,
                        task_id,
                        budget_ms,
                    ) {
                        Ok(id) => match exec.await_terminal(id, budget).await {
                            SubTaskOutcome::Done(v) => ItemOutcome::Ok(v),
                            SubTaskOutcome::Failed(e) => ItemOutcome::Failed(e),
                            SubTaskOutcome::TimedOut => ItemOutcome::TimedOut,
                        },
                        Err(e) => ItemOutcome::Failed(format!("submit failed: {e}")),
                    }
                }) as futures::future::BoxFuture<'static, ItemOutcome<JsonValue>>
            }),
            window: WindowPolicy::default_per_workers(),
            live_workers: Box::new(move || workers),
            deadline: Some(Box::pin(tokio::time::sleep(timeout))),
            on_failure: PartialPolicy::ReturnPartial,
        }
    };

    // 4.2 (ADR-0024): per-target progress frames. One shared counter
    // set spans both controllers; the total is every pair either
    // controller will attempt. `total` here is post-truncation.
    let total = u64::try_from(local_pairs.len() + remote_pairs.len()).unwrap_or(u64::MAX);
    let counters = Arc::new(ProgressCounters::new(total));
    let sink = exec.progress_sink();
    let items_key = if sub_capability == "fs.search" {
        "hits"
    } else {
        "matches"
    };
    let dctx = |pairs_total: &[(MeshTarget, String)]| DriveCtx {
        task_id: ctx.task_id,
        node_id: ctx.local_node,
        sink: sink.clone(),
        counters: counters.clone(),
        items_key,
        expected: u64::try_from(pairs_total.len()).unwrap_or(u64::MAX),
    };
    let local_ctx = dctx(&local_pairs);
    let remote_ctx = dctx(&remote_pairs);

    let ((mut successes, mut failures), (remote_ok, remote_fail)) = tokio::join!(
        drive_fanout(local_spec, local_pairs, local_ctx),
        drive_fanout(remote_spec, remote_pairs, remote_ctx),
    );
    successes.extend(remote_ok);
    failures.extend(remote_fail);

    // Exactly one summary frame per wrapper call, after both
    // controllers settle — the two per-controller summaries are
    // internals and never emitted.
    if let Some(sink) = &sink {
        use std::sync::atomic::Ordering;
        let chunk = json!({ "summary": {
            "total": total,
            "ok": counters.ok.load(Ordering::SeqCst),
            "failed": counters.failed.load(Ordering::SeqCst),
            "timed_out": counters.timed_out.load(Ordering::SeqCst),
            "truncated_targets": truncated_targets,
        }});
        sink(
            ctx.task_id,
            LogFrame {
                stream: StreamKind::Progress,
                line: chunk.to_string(),
            },
        );
    }
    // Merged order is completion-ordered under the controller; sort both
    // lists by (node, scope) so wrapper output stays deterministic.
    successes.sort_by(|a, b| (a.0.node_id, &a.1).cmp(&(b.0.node_id, &b.1)));
    failures.sort_by(|a, b| {
        let key = |v: &JsonValue| {
            (
                v["node"].as_str().unwrap_or("").to_string(),
                v["scope"].as_str().unwrap_or("").to_string(),
            )
        };
        key(a).cmp(&key(b))
    });

    FanOutOutcome {
        successes,
        failures,
        truncated_targets,
    }
}

/// Enumerate (target, scope) fan-out pairs for `sub_capability`,
/// bounded at [`MAX_FANOUT_TARGETS`] with the excess reported.
fn collect_pairs(
    exec: &Arc<dyn MeshExec>,
    sub_capability: &str,
) -> (Vec<(MeshTarget, String)>, usize) {
    let mut pairs: Vec<(MeshTarget, String)> = Vec::new();
    for target in exec.targets() {
        if !target.capabilities.iter().any(|c| c == sub_capability) {
            continue;
        }
        for scope in &target.scopes {
            pairs.push((target.clone(), scope.clone()));
        }
    }
    let truncated_targets = pairs.len().saturating_sub(MAX_FANOUT_TARGETS);
    pairs.truncate(MAX_FANOUT_TARGETS);
    (pairs, truncated_targets)
}

/// Shared per-wrapper progress accounting (4.2, ADR-0024). Atomics for
/// `Send` bounds only: both `drive_fanout` futures are polled by one
/// `tokio::join!` on the wrapper's task, so updates never race and
/// every emitted frame is self-consistent — do not "fix" this into a
/// lock.
struct ProgressCounters {
    completed: std::sync::atomic::AtomicU64,
    ok: std::sync::atomic::AtomicU64,
    /// Excludes timeouts — those are counted in `timed_out`.
    failed: std::sync::atomic::AtomicU64,
    timed_out: std::sync::atomic::AtomicU64,
    total: u64,
}

impl ProgressCounters {
    fn new(total: u64) -> Self {
        Self {
            completed: std::sync::atomic::AtomicU64::new(0),
            ok: std::sync::atomic::AtomicU64::new(0),
            failed: std::sync::atomic::AtomicU64::new(0),
            timed_out: std::sync::atomic::AtomicU64::new(0),
            total,
        }
    }
}

/// Per-controller context for [`drive_fanout`].
struct DriveCtx {
    task_id: TaskId,
    node_id: NodeId,
    sink: Option<FrameSink>,
    counters: Arc<ProgressCounters>,
    /// JSON key holding the per-target result array (`"matches"` for
    /// grep, `"hits"` for search) — for the frame's `items` count.
    items_key: &'static str,
    /// This controller's pair count (the `task_results` denominator).
    expected: u64,
}

/// [`ResultMapper`] over one controller's (node, scope) pairs: resolves
/// `index` provenance, accumulates successes/failures, updates the
/// shared counters, and emits one progress frame per completed target.
/// Pairs never completed when the fan-out ends early (deadline or
/// fail-fast; in-flight-dropped or never pulled) get explicit failure
/// entries — nothing disappears silently, whatever `PartialPolicy` a
/// future caller passes.
struct PairMapper {
    pairs: Vec<(MeshTarget, String)>,
    seen: Vec<bool>,
    successes: Vec<(MeshTarget, String, JsonValue)>,
    failures: Vec<JsonValue>,
    ctx: DriveCtx,
}

impl PairMapper {
    fn emit(&self, chunk: &JsonValue) {
        if let Some(sink) = &self.ctx.sink {
            sink(
                self.ctx.task_id,
                LogFrame {
                    stream: StreamKind::Progress,
                    line: chunk.to_string(),
                },
            );
        }
    }
}

impl ResultMapper<JsonValue> for PairMapper {
    fn on_item(&mut self, index: u64, outcome: &ItemOutcome<JsonValue>) -> JsonValue {
        use std::sync::atomic::Ordering;
        let idx = usize::try_from(index).unwrap_or(usize::MAX);
        let Some((target, scope)) = self.pairs.get(idx) else {
            return JsonValue::Null;
        };
        self.seen[idx] = true;
        let c = &self.ctx.counters;
        c.completed.fetch_add(1, Ordering::SeqCst);
        let (kind, items, error) = match outcome {
            ItemOutcome::Ok(v) => {
                c.ok.fetch_add(1, Ordering::SeqCst);
                let items = v
                    .get(self.ctx.items_key)
                    .and_then(JsonValue::as_array)
                    .map(Vec::len);
                self.successes.push((target.clone(), scope.clone(), v.clone()));
                ("ok", items, None)
            }
            ItemOutcome::Failed(e) => {
                c.failed.fetch_add(1, Ordering::SeqCst);
                self.failures.push(failure_entry(target, scope, e));
                ("failed", None, Some(e.clone()))
            }
            ItemOutcome::TimedOut => {
                c.timed_out.fetch_add(1, Ordering::SeqCst);
                self.failures.push(failure_entry(target, scope, "timed out"));
                ("timed_out", None, Some("timed out".into()))
            }
        };
        let mut chunk = json!({
            "target": {
                "node": target.node_id.to_string(),
                "node_name": target.node_name,
                "scope": scope,
            },
            "outcome": kind,
            "completed": c.completed.load(Ordering::SeqCst),
            "total": c.total,
            "ok": c.ok.load(Ordering::SeqCst),
            "failed": c.failed.load(Ordering::SeqCst),
            "timed_out": c.timed_out.load(Ordering::SeqCst),
        });
        if let Some(n) = items {
            chunk["items"] = json!(n);
        }
        if let Some(e) = error {
            chunk["error"] = json!(e);
        }
        self.emit(&chunk);
        chunk
    }

    fn on_end(&mut self, summary: &FanoutSummary) -> JsonValue {
        let early_end_error = match summary.reason {
            EndReason::SourceDrained => None,
            EndReason::DeadlineExceeded => Some("deadline exceeded before completion"),
            // Unreachable today (both specs pass ReturnPartial) but a
            // 4.5 caller with FailFast must not lose pairs silently.
            EndReason::FailedFast => Some("aborted by fail-fast before completion"),
        };
        if let Some(error) = early_end_error {
            for idx in 0..self.pairs.len() {
                if !self.seen[idx] {
                    let (target, scope) = &self.pairs[idx];
                    self.failures.push(failure_entry(target, scope, error));
                }
            }
        }
        // Per-controller summary chunk — consumed by the stream but
        // never emitted to the sink; fan_out sends the single merged
        // summary frame after both controllers settle.
        JsonValue::Null
    }
}

/// Drive one controller through [`task_results`] to termination and
/// recover the accumulated (successes, failures).
async fn drive_fanout(
    spec: FanoutSpec<(MeshTarget, String), JsonValue>,
    pairs: Vec<(MeshTarget, String)>,
    dctx: DriveCtx,
) -> (Vec<(MeshTarget, String, JsonValue)>, Vec<JsonValue>) {
    let result_ctx = ResultCtx {
        task_id: dctx.task_id,
        node_id: dctx.node_id,
        expected: Some(dctx.expected),
    };
    let seen = vec![false; pairs.len()];
    let mapper = PairMapper {
        seen,
        successes: Vec::new(),
        failures: Vec::new(),
        pairs,
        ctx: dctx,
    };
    let mut stream = task_results(FanoutController::stream(spec), result_ctx, mapper);
    // The §14.8 Stream<TaskResult> is the engine here; the wrapper's
    // deliverable is the merged terminal output, so the per-item
    // TaskResults are consumed (their side effects live in the mapper).
    while stream.next().await.is_some() {}
    let mapper = stream.into_mapper();
    (mapper.successes, mapper.failures)
}

struct FanOutOutcome {
    successes: Vec<(MeshTarget, String, JsonValue)>,
    failures: Vec<JsonValue>,
    truncated_targets: usize,
}

fn failure_entry(target: &MeshTarget, scope: &str, error: &str) -> JsonValue {
    json!({
        "node": target.node_id.to_string(),
        "node_name": target.node_name,
        "scope": scope,
        "error": error,
    })
}

fn provenance(outcome: &FanOutOutcome) -> JsonValue {
    json!({
        "targets_ok": outcome.successes.len(),
        "targets_failed": outcome.failures.len(),
        "truncated_targets": outcome.truncated_targets,
    })
}

fn manifest_for(id: &str) -> ManifestEntry {
    // Per-capability schemas (review NIT-1): grep takes pattern +
    // regex knobs; search takes query + limit. Both take timeout_ms
    // and limit (grep forwards it as fs.grep's max_results).
    let input_schema = if id == "mesh.grep" {
        json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["pattern"],
            "properties": {
                "pattern": { "type": "string" },
                "literal": { "type": "boolean" },
                "ignore_case": { "type": "boolean" },
                "file_glob": { "type": "string" },
                "limit": { "type": "integer", "minimum": 1, "maximum": 1000 },
                "timeout_ms": { "type": "integer", "minimum": 1000, "maximum": 120_000 }
            }
        })
    } else {
        json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["query"],
            "properties": {
                "query": { "type": "string" },
                "limit": { "type": "integer", "minimum": 1, "maximum": 1000 },
                "timeout_ms": { "type": "integer", "minimum": 1000, "maximum": 120_000 }
            }
        })
    };
    ManifestEntry {
        id: id.to_string(),
        version: harness_core::SemVer::new(0, 1, 0),
        // Anyone: any node can coordinate a federated query — the data
        // access happens on the owning nodes via `fs.*` (Owner) calls.
        cardinality: Cardinality::Anyone,
        input_schema,
        output_schema: json!({ "type": "object" }),
        cost_hint: harness_core::protocol::CostHint::LocalFast,
        tags: vec!["mesh".into()],
        rate_limit: None,
        resource_hints: harness_core::ResourceHints {
            cpu_class: harness_core::protocol::CpuClass::Light,
            memory_mb: None,
            gpu_required: false,
            gpu_memory_mb: None,
            network_class: harness_core::protocol::NetworkClass::Light,
            disk_io_class: harness_core::protocol::DiskIoClass::None,
            estimated_duration_ms: None,
        },
        requires_secrets: vec![],
    }
}

/// `mesh.grep` — federated `fs.grep` (merge: Concat, PRD §14.6).
pub struct MeshGrepCapability {
    exec: Arc<dyn MeshExec>,
}

impl std::fmt::Debug for MeshGrepCapability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MeshGrepCapability").finish_non_exhaustive()
    }
}

impl MeshGrepCapability {
    #[must_use]
    pub fn new(exec: Arc<dyn MeshExec>) -> Self {
        Self { exec }
    }
}

#[async_trait]
impl Capability for MeshGrepCapability {
    fn id(&self) -> &'static str {
        "mesh.grep"
    }

    fn manifest(&self) -> ManifestEntry {
        manifest_for("mesh.grep")
    }

    async fn execute(
        &self,
        ctx: &ExecutionContext,
        input: JsonValue,
    ) -> Result<JsonValue, CapabilityError> {
        let Some(pattern) = input.get("pattern").and_then(JsonValue::as_str) else {
            return Err(CapabilityError::InvalidInput(
                "missing required string field: pattern".into(),
            ));
        };
        let timeout = Duration::from_millis(clamp_timeout(&input));
        let pattern = pattern.to_string();
        let literal = input.get("literal").cloned();
        let ignore_case = input.get("ignore_case").cloned();
        let file_glob = input.get("file_glob").cloned();
        // Per-scope match bound (review MINOR-4): forwarded as
        // fs.grep's max_results; merged output is therefore bounded by
        // MAX_FANOUT_TARGETS × limit.
        let limit = input
            .get("limit")
            .and_then(JsonValue::as_u64)
            .unwrap_or(100)
            .clamp(1, 1000);
        let sub_input: Arc<dyn Fn(&str) -> JsonValue + Send + Sync> = Arc::new(move |scope| {
            let mut v = json!({ "scope": scope, "pattern": pattern, "max_results": limit });
            if let Some(l) = &literal {
                v["literal"] = l.clone();
            }
            if let Some(i) = &ignore_case {
                v["ignore_case"] = i.clone();
            }
            if let Some(g) = &file_glob {
                v["file_glob"] = g.clone();
            }
            v
        });
        let outcome = fan_out(&self.exec, ctx, "fs.grep", sub_input, timeout).await;

        // Concat merge: every per-scope result block keeps its origin.
        let results: Vec<JsonValue> = outcome
            .successes
            .iter()
            .map(|(target, scope, output)| {
                json!({
                    "node": target.node_id.to_string(),
                    "node_name": target.node_name,
                    "scope": scope,
                    "result": output,
                })
            })
            .collect();
        Ok(json!({
            "results": results,
            "failures": outcome.failures,
            "provenance": provenance(&outcome),
        }))
    }
}

/// `mesh.search` — federated `fs.search` (merge: score-sort; the PRD's
/// Rerank default arrives with a reranker capability in Phase 5).
pub struct MeshSearchCapability {
    exec: Arc<dyn MeshExec>,
}

impl std::fmt::Debug for MeshSearchCapability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MeshSearchCapability")
            .finish_non_exhaustive()
    }
}

/// Register both mesh meta-capabilities. Called by the daemon after the
/// `fs.*` capabilities so the manifest advertises the full set.
///
/// # Panics
/// If either id is already registered (a wiring bug, same discipline
/// as the daemon's other `expect`-on-duplicate registrations).
#[allow(clippy::expect_used)]
pub fn enrich_with_mesh_meta(registry: &crate::CapabilityRegistry, exec: Arc<dyn MeshExec>) {
    registry
        .register(Arc::new(MeshGrepCapability::new(exec.clone())))
        .expect("BUG: mesh.grep registered twice");
    registry
        .register(Arc::new(MeshSearchCapability::new(exec)))
        .expect("BUG: mesh.search registered twice");
}

impl MeshSearchCapability {
    #[must_use]
    pub fn new(exec: Arc<dyn MeshExec>) -> Self {
        Self { exec }
    }
}

#[async_trait]
impl Capability for MeshSearchCapability {
    fn id(&self) -> &'static str {
        "mesh.search"
    }

    fn manifest(&self) -> ManifestEntry {
        manifest_for("mesh.search")
    }

    async fn execute(
        &self,
        ctx: &ExecutionContext,
        input: JsonValue,
    ) -> Result<JsonValue, CapabilityError> {
        let Some(query) = input.get("query").and_then(JsonValue::as_str) else {
            return Err(CapabilityError::InvalidInput(
                "missing required string field: query".into(),
            ));
        };
        let timeout = Duration::from_millis(clamp_timeout(&input));
        let limit = input
            .get("limit")
            .and_then(JsonValue::as_u64)
            .unwrap_or(50)
            .clamp(1, 1000) as usize;
        let query = query.to_string();
        let sub_limit = limit;
        let sub_input: Arc<dyn Fn(&str) -> JsonValue + Send + Sync> =
            Arc::new(move |scope| json!({ "scope": scope, "query": query, "limit": sub_limit }));
        let outcome = fan_out(&self.exec, ctx, "fs.search", sub_input, timeout).await;

        // Flatten hits, annotate origin, sort by score descending.
        let mut hits: Vec<JsonValue> = Vec::new();
        for (target, scope, output) in &outcome.successes {
            if let Some(scope_hits) = output.get("hits").and_then(JsonValue::as_array) {
                for hit in scope_hits {
                    let mut h = hit.clone();
                    if let Some(obj) = h.as_object_mut() {
                        obj.insert("node".into(), json!(target.node_id.to_string()));
                        obj.insert("node_name".into(), json!(target.node_name));
                        obj.insert("scope".into(), json!(scope));
                    }
                    hits.push(h);
                }
            }
        }
        hits.sort_by(|a, b| {
            let score = |v: &JsonValue| v.get("score").and_then(JsonValue::as_f64).unwrap_or(0.0);
            score(b)
                .partial_cmp(&score(a))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        hits.truncate(limit);
        Ok(json!({
            "hits": hits,
            "failures": outcome.failures,
            "provenance": provenance(&outcome),
        }))
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use parking_lot::Mutex;

    fn ctx() -> ExecutionContext {
        ExecutionContext {
            local_node: NodeId::from_bytes([1; 16]),
            local_node_name: Arc::from("self"),
            issued_by: NodeId::from_bytes([1; 16]),
            issued_by_name: Arc::from("self"),
            task_id: TaskId::new_v7(),
            tags: Arc::from(Vec::<String>::new()),
        }
    }

    struct FakeExec {
        targets: Vec<MeshTarget>,
        /// (capability, scope-json) log of local runs.
        local_calls: Mutex<Vec<(String, JsonValue)>>,
        remote_calls: Mutex<Vec<(String, NodeId, JsonValue)>>,
        remote_outcome: SubTaskOutcome,
        local_fails: bool,
        /// 4.2: captured progress frames when a sink is installed.
        frames: Option<Arc<Mutex<Vec<(TaskId, LogFrame)>>>>,
    }

    impl FakeExec {
        fn new(targets: Vec<MeshTarget>, remote_outcome: SubTaskOutcome) -> Arc<Self> {
            Arc::new(Self {
                targets,
                local_calls: Mutex::new(vec![]),
                remote_calls: Mutex::new(vec![]),
                remote_outcome,
                local_fails: false,
                frames: None,
            })
        }

        fn with_sink(
            targets: Vec<MeshTarget>,
            remote_outcome: SubTaskOutcome,
        ) -> (Arc<Self>, Arc<Mutex<Vec<(TaskId, LogFrame)>>>) {
            let frames = Arc::new(Mutex::new(vec![]));
            let exec = Arc::new(Self {
                targets,
                local_calls: Mutex::new(vec![]),
                remote_calls: Mutex::new(vec![]),
                remote_outcome,
                local_fails: false,
                frames: Some(frames.clone()),
            });
            (exec, frames)
        }
    }

    #[async_trait]
    impl MeshExec for FakeExec {
        fn targets(&self) -> Vec<MeshTarget> {
            self.targets.clone()
        }
        async fn run_local(
            &self,
            capability: &str,
            _ctx: &ExecutionContext,
            input: JsonValue,
        ) -> Result<JsonValue, CapabilityError> {
            self.local_calls
                .lock()
                .push((capability.to_string(), input.clone()));
            if self.local_fails {
                return Err(CapabilityError::Failed("local boom".into()));
            }
            if capability == "fs.search" {
                Ok(json!({ "hits": [{ "path": "local.md", "score": 2.0, "snippet": "x" }] }))
            } else {
                Ok(json!({ "matches": [{ "path": "local.md", "line": 1 }], "truncated": false }))
            }
        }
        fn submit_remote(
            &self,
            capability: &str,
            input: JsonValue,
            pin_to: NodeId,
            _parent: TaskId,
            _timeout_ms: u32,
        ) -> Result<TaskId, CapabilityError> {
            self.remote_calls
                .lock()
                .push((capability.to_string(), pin_to, input));
            Ok(TaskId::new_v7())
        }
        async fn await_terminal(&self, _id: TaskId, _deadline: Duration) -> SubTaskOutcome {
            self.remote_outcome.clone()
        }
        fn progress_sink(&self) -> Option<crate::traits::FrameSink> {
            self.frames.as_ref().map(|frames| {
                let frames = frames.clone();
                Arc::new(move |task_id: TaskId, frame: LogFrame| {
                    frames.lock().push((task_id, frame));
                }) as crate::traits::FrameSink
            })
        }
    }

    fn target(byte: u8, name: &str, is_self: bool, scopes: &[&str], caps: &[&str]) -> MeshTarget {
        MeshTarget {
            node_id: NodeId::from_bytes([byte; 16]),
            node_name: name.into(),
            is_self,
            scopes: scopes.iter().map(|s| (*s).to_string()).collect(),
            capabilities: caps.iter().map(|s| (*s).to_string()).collect(),
        }
    }

    #[tokio::test]
    async fn t01_grep_concat_merges_local_and_remote_with_provenance() {
        let exec = FakeExec::new(
            vec![
                target(1, "self", true, &["docs"], &["fs.grep"]),
                target(2, "peer", false, &["notes"], &["fs.grep"]),
            ],
            SubTaskOutcome::Done(json!({ "matches": [{ "path": "remote.md", "line": 7 }] })),
        );
        let cap = MeshGrepCapability::new(exec.clone());
        let out = cap
            .execute(&ctx(), json!({ "pattern": "needle" }))
            .await
            .expect("execute");
        let results = out["results"].as_array().expect("results");
        assert_eq!(results.len(), 2);
        assert_eq!(out["provenance"]["targets_ok"], 2);
        assert_eq!(out["provenance"]["targets_failed"], 0);
        // Origins present + distinct.
        let scopes: Vec<&str> = results
            .iter()
            .map(|r| r["scope"].as_str().unwrap())
            .collect();
        assert!(scopes.contains(&"docs") && scopes.contains(&"notes"));
        // Local ran in-process; remote was pinned to the peer.
        assert_eq!(exec.local_calls.lock().len(), 1);
        let remote = exec.remote_calls.lock();
        assert_eq!(remote.len(), 1);
        assert_eq!(remote[0].1, NodeId::from_bytes([2; 16]));
        assert_eq!(remote[0].2["scope"], "notes");
    }

    #[tokio::test]
    async fn t02_search_flattens_and_sorts_by_score() {
        let exec = FakeExec::new(
            vec![
                target(1, "self", true, &["docs"], &["fs.search"]),
                target(2, "peer", false, &["notes"], &["fs.search"]),
            ],
            SubTaskOutcome::Done(
                json!({ "hits": [{ "path": "remote.md", "score": 9.5, "snippet": "y" }] }),
            ),
        );
        let cap = MeshSearchCapability::new(exec);
        let out = cap
            .execute(&ctx(), json!({ "query": "term sheet" }))
            .await
            .expect("execute");
        let hits = out["hits"].as_array().expect("hits");
        assert_eq!(hits.len(), 2);
        // Remote hit (9.5) outranks local (2.0).
        assert_eq!(hits[0]["path"], "remote.md");
        assert_eq!(hits[0]["node_name"], "peer");
        assert_eq!(hits[1]["scope"], "docs");
    }

    #[tokio::test]
    async fn t03_partial_failure_returns_successes_plus_failure_entries() {
        let exec = FakeExec::new(
            vec![
                target(1, "self", true, &["docs"], &["fs.grep"]),
                target(2, "peer", false, &["notes"], &["fs.grep"]),
            ],
            SubTaskOutcome::Failed("worker exploded".into()),
        );
        let cap = MeshGrepCapability::new(exec);
        let out = cap
            .execute(&ctx(), json!({ "pattern": "x" }))
            .await
            .expect("execute");
        assert_eq!(out["results"].as_array().unwrap().len(), 1);
        let failures = out["failures"].as_array().unwrap();
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0]["node_name"], "peer");
        assert!(failures[0]["error"]
            .as_str()
            .unwrap()
            .contains("worker exploded"));
        assert_eq!(out["provenance"]["targets_failed"], 1);
    }

    #[tokio::test]
    async fn t04_targets_without_capability_are_skipped() {
        let exec = FakeExec::new(
            vec![
                target(1, "self", true, &["docs"], &["fs.grep"]),
                target(2, "no-fs", false, &["notes"], &["echo"]),
            ],
            SubTaskOutcome::Done(json!({ "matches": [] })),
        );
        let cap = MeshGrepCapability::new(exec.clone());
        let out = cap
            .execute(&ctx(), json!({ "pattern": "x" }))
            .await
            .expect("execute");
        assert_eq!(out["results"].as_array().unwrap().len(), 1);
        assert!(exec.remote_calls.lock().is_empty());
    }

    #[tokio::test]
    async fn t05_fanout_truncation_is_reported_not_silent() {
        let scopes: Vec<String> = (0..100).map(|i| format!("s{i}")).collect();
        let scope_refs: Vec<&str> = scopes.iter().map(String::as_str).collect();
        let exec = FakeExec::new(
            vec![target(1, "self", true, &scope_refs, &["fs.grep"])],
            SubTaskOutcome::Done(json!({ "matches": [] })),
        );
        let cap = MeshGrepCapability::new(exec);
        let out = cap
            .execute(&ctx(), json!({ "pattern": "x" }))
            .await
            .expect("execute");
        assert_eq!(out["results"].as_array().unwrap().len(), MAX_FANOUT_TARGETS);
        assert_eq!(
            out["provenance"]["truncated_targets"],
            100 - MAX_FANOUT_TARGETS
        );
    }

    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Gauged fake for the 4.1 O(window) proofs: counts non-terminal
    /// remote rows (+1 on submit, −1 when its await resolves) and live
    /// local scans; both resolve only when the test releases permits.
    struct GaugedExec {
        targets: Vec<MeshTarget>,
        /// `timeout_ms` of every `submit_remote` call, in call order.
        budgets: Mutex<Vec<u32>>,
        submits: AtomicUsize,
        non_terminal: AtomicUsize,
        non_terminal_peak: AtomicUsize,
        local_live: AtomicUsize,
        local_peak: AtomicUsize,
        remote_gate: tokio::sync::Semaphore,
        local_gate: tokio::sync::Semaphore,
    }

    impl GaugedExec {
        fn new(targets: Vec<MeshTarget>) -> Arc<Self> {
            Arc::new(Self {
                targets,
                budgets: Mutex::new(vec![]),
                submits: AtomicUsize::new(0),
                non_terminal: AtomicUsize::new(0),
                non_terminal_peak: AtomicUsize::new(0),
                local_live: AtomicUsize::new(0),
                local_peak: AtomicUsize::new(0),
                remote_gate: tokio::sync::Semaphore::new(0),
                local_gate: tokio::sync::Semaphore::new(0),
            })
        }
    }

    #[async_trait]
    impl MeshExec for GaugedExec {
        fn targets(&self) -> Vec<MeshTarget> {
            self.targets.clone()
        }
        async fn run_local(
            &self,
            _capability: &str,
            _ctx: &ExecutionContext,
            _input: JsonValue,
        ) -> Result<JsonValue, CapabilityError> {
            let n = self.local_live.fetch_add(1, Ordering::SeqCst) + 1;
            self.local_peak.fetch_max(n, Ordering::SeqCst);
            let permit = self.local_gate.acquire().await;
            self.local_live.fetch_sub(1, Ordering::SeqCst);
            permit
                .map_err(|_| CapabilityError::Failed("gate closed".into()))?
                .forget();
            Ok(json!({ "matches": [], "truncated": false }))
        }
        fn submit_remote(
            &self,
            _capability: &str,
            _input: JsonValue,
            _pin_to: NodeId,
            _parent: TaskId,
            timeout_ms: u32,
        ) -> Result<TaskId, CapabilityError> {
            self.budgets.lock().push(timeout_ms);
            self.submits.fetch_add(1, Ordering::SeqCst);
            let n = self.non_terminal.fetch_add(1, Ordering::SeqCst) + 1;
            self.non_terminal_peak.fetch_max(n, Ordering::SeqCst);
            Ok(TaskId::new_v7())
        }
        async fn await_terminal(&self, _id: TaskId, _deadline: Duration) -> SubTaskOutcome {
            let permit = self.remote_gate.acquire().await;
            self.non_terminal.fetch_sub(1, Ordering::SeqCst);
            match permit {
                Ok(p) => p.forget(),
                Err(_) => return SubTaskOutcome::Failed("gate closed".into()),
            }
            SubTaskOutcome::Done(json!({ "matches": [], "truncated": false }))
        }
    }

    fn wide_target(byte: u8, scope_count: usize) -> MeshTarget {
        let scopes = (0..scope_count).map(|i| format!("s{byte}-{i}")).collect();
        MeshTarget {
            node_id: NodeId::from_bytes([byte; 16]),
            node_name: format!("n{byte}"),
            is_self: false,
            scopes,
            capabilities: vec!["fs.grep".into()],
        }
    }

    async fn yield_until(cond: impl Fn() -> bool) {
        for _ in 0..10_000 {
            if cond() {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("condition never reached");
    }

    #[tokio::test]
    async fn t07_remote_rows_bounded_by_window_2_nodes() {
        // 2 remote nodes × 20 scopes = 40 pairs; window = clamp(2×2,4,64) = 4.
        let exec = GaugedExec::new(vec![wide_target(2, 20), wide_target(3, 20)]);
        let cap = MeshGrepCapability::new(exec.clone());
        let c = ctx();
        let handle = tokio::spawn(async move { cap.execute(&c, json!({ "pattern": "x" })).await });
        let e = exec.clone();
        yield_until(move || e.non_terminal.load(Ordering::SeqCst) == 4).await;
        // Window full: no further rows exist yet.
        assert_eq!(exec.submits.load(Ordering::SeqCst), 4);
        exec.remote_gate.add_permits(40);
        let out = handle.await.expect("join").expect("execute");
        assert_eq!(out["results"].as_array().unwrap().len(), 40);
        assert_eq!(exec.submits.load(Ordering::SeqCst), 40, "all pairs ran");
        assert_eq!(
            exec.non_terminal_peak.load(Ordering::SeqCst),
            4,
            "never more than window non-terminal rows"
        );
    }

    #[tokio::test]
    async fn t08_window_scales_with_worker_count_3_nodes() {
        // 3 nodes → window = clamp(2×3,4,64) = 6 — above the min-clamp
        // floor, proving 2×N drives refill (review MINOR-6).
        let exec = GaugedExec::new(vec![
            wide_target(2, 10),
            wide_target(3, 10),
            wide_target(4, 10),
        ]);
        let cap = MeshGrepCapability::new(exec.clone());
        let c = ctx();
        let handle = tokio::spawn(async move { cap.execute(&c, json!({ "pattern": "x" })).await });
        let e = exec.clone();
        yield_until(move || e.non_terminal.load(Ordering::SeqCst) == 6).await;
        exec.remote_gate.add_permits(30);
        let out = handle.await.expect("join").expect("execute");
        assert_eq!(out["results"].as_array().unwrap().len(), 30);
        assert_eq!(exec.non_terminal_peak.load(Ordering::SeqCst), 6);
    }

    #[tokio::test]
    async fn t09_local_scans_stay_bounded_at_4_on_wide_mesh() {
        // Self with 12 scopes + 3 remote nodes: the remote window is 6,
        // but local scans keep ADR-0022's dedicated bound of 4.
        let mut self_target = wide_target(1, 12);
        self_target.is_self = true;
        self_target.node_name = "self".into();
        let exec = GaugedExec::new(vec![
            self_target,
            wide_target(2, 2),
            wide_target(3, 2),
            wide_target(4, 2),
        ]);
        exec.remote_gate.add_permits(6); // remotes finish freely
        let cap = MeshGrepCapability::new(exec.clone());
        let c = ctx();
        let handle = tokio::spawn(async move { cap.execute(&c, json!({ "pattern": "x" })).await });
        let e = exec.clone();
        yield_until(move || e.local_live.load(Ordering::SeqCst) == 4).await;
        exec.local_gate.add_permits(12);
        let out = handle.await.expect("join").expect("execute");
        assert_eq!(out["results"].as_array().unwrap().len(), 18);
        assert_eq!(
            exec.local_peak.load(Ordering::SeqCst),
            4,
            "local scan concurrency stays at LOCAL_SCAN_CONCURRENCY"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn t10_deadline_never_pulls_remaining_pairs() {
        // 1 node × 30 scopes, window 4, nothing ever completes: the
        // 1 s (min-clamped) deadline fires with 26 pairs unpulled —
        // those never became task rows, and every pair is accounted for
        // in failures[].
        let exec = GaugedExec::new(vec![wide_target(2, 30)]);
        let cap = MeshGrepCapability::new(exec.clone());
        let out = cap
            .execute(&ctx(), json!({ "pattern": "x", "timeout_ms": 1000 }))
            .await
            .expect("execute");
        assert_eq!(
            exec.submits.load(Ordering::SeqCst),
            4,
            "unpulled pairs never submitted"
        );
        assert_eq!(out["results"].as_array().unwrap().len(), 0);
        let failures = out["failures"].as_array().unwrap();
        assert_eq!(failures.len(), 30, "every pair accounted for");
        assert!(failures
            .iter()
            .all(|f| f["error"].as_str().unwrap().contains("deadline exceeded")));
        assert_eq!(out["provenance"]["targets_failed"], 30);
    }

    #[tokio::test]
    async fn t12_progress_frames_per_target_plus_one_summary() {
        let (exec, frames) = FakeExec::with_sink(
            vec![
                target(1, "self", true, &["docs"], &["fs.grep"]),
                target(2, "peer", false, &["notes"], &["fs.grep"]),
            ],
            SubTaskOutcome::Done(json!({ "matches": [{ "path": "r.md", "line": 7 }] })),
        );
        let cap = MeshGrepCapability::new(exec);
        let c = ctx();
        let task_id = c.task_id;
        cap.execute(&c, json!({ "pattern": "x" }))
            .await
            .expect("execute");
        let captured = frames.lock();
        assert_eq!(captured.len(), 3, "2 per-target + 1 summary");
        assert!(captured.iter().all(|(id, f)| *id == task_id
            && f.stream == StreamKind::Progress));
        let chunks: Vec<JsonValue> = captured
            .iter()
            .map(|(_, f)| serde_json::from_str(&f.line).expect("json line"))
            .collect();
        let targets: Vec<&JsonValue> = chunks.iter().filter(|c| !c["target"].is_null()).collect();
        assert_eq!(targets.len(), 2);
        // Counts are monotone and self-consistent per frame.
        for t in &targets {
            assert_eq!(t["outcome"], "ok");
            assert_eq!(t["total"], 2);
            assert_eq!(t["items"], 1);
            let completed = t["completed"].as_u64().unwrap();
            assert!(completed >= 1 && completed <= 2);
            assert_eq!(t["ok"].as_u64().unwrap(), completed);
        }
        let scopes: Vec<&str> = targets
            .iter()
            .map(|t| t["target"]["scope"].as_str().unwrap())
            .collect();
        assert!(scopes.contains(&"docs") && scopes.contains(&"notes"));
        let summary = chunks
            .iter()
            .find(|c| !c["summary"].is_null())
            .expect("summary frame");
        assert_eq!(summary["summary"]["total"], 2);
        assert_eq!(summary["summary"]["ok"], 2);
        assert_eq!(summary["summary"]["failed"], 0);
        assert_eq!(summary["summary"]["timed_out"], 0);
        assert_eq!(summary["summary"]["truncated_targets"], 0);
    }

    #[tokio::test]
    async fn t13_progress_failure_frame_carries_error_and_summary_counts() {
        let (exec, frames) = FakeExec::with_sink(
            vec![
                target(1, "self", true, &["docs"], &["fs.grep"]),
                target(2, "peer", false, &["notes"], &["fs.grep"]),
            ],
            SubTaskOutcome::Failed("worker exploded".into()),
        );
        let cap = MeshGrepCapability::new(exec);
        cap.execute(&ctx(), json!({ "pattern": "x" }))
            .await
            .expect("execute");
        let chunks: Vec<JsonValue> = frames
            .lock()
            .iter()
            .map(|(_, f)| serde_json::from_str(&f.line).expect("json line"))
            .collect();
        let failed = chunks
            .iter()
            .find(|c| c["outcome"] == "failed")
            .expect("failed frame");
        assert_eq!(failed["target"]["node_name"], "peer");
        assert!(failed["error"].as_str().unwrap().contains("worker exploded"));
        let summary = chunks
            .iter()
            .find(|c| !c["summary"].is_null())
            .expect("summary");
        assert_eq!(summary["summary"]["ok"], 1);
        assert_eq!(summary["summary"]["failed"], 1);
        assert_eq!(summary["summary"]["timed_out"], 0);
    }

    #[tokio::test]
    async fn t14_sinkless_exec_emits_nothing_and_output_is_unchanged() {
        // The default progress_sink (None) keeps every existing test
        // green; this pins that the sink is genuinely optional.
        let exec = FakeExec::new(
            vec![target(1, "self", true, &["docs"], &["fs.grep"])],
            SubTaskOutcome::Done(json!({ "matches": [] })),
        );
        assert!(exec.frames.is_none());
        let cap = MeshGrepCapability::new(exec);
        let out = cap
            .execute(&ctx(), json!({ "pattern": "x" }))
            .await
            .expect("execute");
        assert_eq!(out["results"].as_array().unwrap().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn t11_late_pulled_pairs_get_remaining_budget() {
        // Diff review MAJOR-1: a pair pulled after time has passed must
        // carry the REMAINING wrapper budget as its row timeout, so the
        // ADR-0022 "sub-task timeout ≤ wrapper deadline" bound holds
        // under windowed pull.
        let exec = GaugedExec::new(vec![wide_target(2, 8)]);
        let cap = MeshGrepCapability::new(exec.clone());
        let c = ctx();
        let handle = tokio::spawn(async move {
            cap.execute(&c, json!({ "pattern": "x", "timeout_ms": 2000 }))
                .await
        });
        let e = exec.clone();
        yield_until(move || e.non_terminal.load(Ordering::SeqCst) == 4).await;
        assert!(exec.budgets.lock().iter().take(4).all(|&b| b == 2000));
        tokio::time::advance(Duration::from_millis(500)).await;
        exec.remote_gate.add_permits(8);
        let out = handle.await.expect("join").expect("execute");
        assert_eq!(out["results"].as_array().unwrap().len(), 8);
        let budgets = exec.budgets.lock().clone();
        assert_eq!(budgets.len(), 8);
        assert!(
            budgets[4..].iter().all(|&b| b <= 1500),
            "late pulls carry remaining budget, got {budgets:?}"
        );
    }

    #[tokio::test]
    async fn t06_missing_required_field_is_invalid_input() {
        let exec = FakeExec::new(vec![], SubTaskOutcome::TimedOut);
        let grep = MeshGrepCapability::new(exec.clone());
        let err = grep.execute(&ctx(), json!({})).await.expect_err("invalid");
        assert!(matches!(err, CapabilityError::InvalidInput(_)));
        let search = MeshSearchCapability::new(exec);
        let err = search
            .execute(&ctx(), json!({}))
            .await
            .expect_err("invalid");
        assert!(matches!(err, CapabilityError::InvalidInput(_)));
    }
}
