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
use harness_core::{Capability as ManifestEntry, Cardinality, NodeId, TaskId};
use serde_json::{json, Value as JsonValue};

use crate::traits::{Capability, CapabilityError, ExecutionContext};

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
}

fn clamp_timeout(input: &JsonValue) -> u64 {
    input
        .get("timeout_ms")
        .and_then(JsonValue::as_u64)
        .unwrap_or(DEFAULT_TIMEOUT_MS)
        .clamp(1_000, MAX_TIMEOUT_MS)
}

/// Shared fan-out driver for both wrappers. `sub_input(scope)` builds
/// the per-scope `fs.*` input.
async fn fan_out(
    exec: &Arc<dyn MeshExec>,
    ctx: &ExecutionContext,
    sub_capability: &str,
    sub_input: &(dyn Fn(&str) -> JsonValue + Send + Sync),
    timeout: Duration,
) -> FanOutOutcome {
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

    let mut successes: Vec<(MeshTarget, String, JsonValue)> = Vec::new();
    let mut failures: Vec<JsonValue> = Vec::new();

    // Self-owned scopes run in-process, concurrently with the remote
    // waits below; remotes are submitted first so their dispatch
    // overlaps our local work.
    let mut remote_handles: Vec<(MeshTarget, String, Result<TaskId, CapabilityError>)> = Vec::new();
    let mut local_pairs: Vec<(MeshTarget, String)> = Vec::new();
    #[allow(clippy::cast_possible_truncation)]
    let sub_timeout_ms = timeout.as_millis().min(u128::from(u32::MAX)) as u32;
    for (target, scope) in pairs {
        if target.is_self {
            local_pairs.push((target, scope));
        } else {
            let submitted = exec.submit_remote(
                sub_capability,
                sub_input(&scope),
                target.node_id,
                ctx.task_id,
                sub_timeout_ms,
            );
            remote_handles.push((target, scope, submitted));
        }
    }

    let local_futs = local_pairs.into_iter().map(|(target, scope)| {
        let exec = exec.clone();
        let input = sub_input(&scope);
        async move {
            let res =
                tokio::time::timeout(timeout, exec.run_local(sub_capability, ctx, input)).await;
            (target, scope, res)
        }
    });
    let remote_futs = remote_handles
        .into_iter()
        .map(|(target, scope, submitted)| {
            let exec = exec.clone();
            async move {
                let outcome = match submitted {
                    Ok(id) => exec.await_terminal(id, timeout).await,
                    Err(e) => SubTaskOutcome::Failed(format!("submit failed: {e}")),
                };
                (target, scope, outcome)
            }
        });

    // Local scans are bounded (review MAJOR-2 — "bounded channels
    // everywhere"): at most LOCAL_SCAN_CONCURRENCY fs.* walks run at
    // once on this node; remotes are bounded by the remote executor.
    let (local_results, remote_results) = tokio::join!(
        futures::stream::iter(local_futs)
            .buffer_unordered(LOCAL_SCAN_CONCURRENCY)
            .collect::<Vec<_>>(),
        futures::future::join_all(remote_futs),
    );

    for (target, scope, res) in local_results {
        match res {
            Ok(Ok(output)) => successes.push((target, scope, output)),
            Ok(Err(e)) => failures.push(failure_entry(&target, &scope, &e.to_string())),
            Err(_) => failures.push(failure_entry(&target, &scope, "timed out")),
        }
    }
    for (target, scope, outcome) in remote_results {
        match outcome {
            SubTaskOutcome::Done(output) => successes.push((target, scope, output)),
            SubTaskOutcome::Failed(e) => failures.push(failure_entry(&target, &scope, &e)),
            SubTaskOutcome::TimedOut => {
                failures.push(failure_entry(&target, &scope, "timed out"));
            }
        }
    }

    FanOutOutcome {
        successes,
        failures,
        truncated_targets,
    }
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
        let sub_input = move |scope: &str| {
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
        };
        let outcome = fan_out(&self.exec, ctx, "fs.grep", &sub_input, timeout).await;

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
        let sub_input =
            move |scope: &str| json!({ "scope": scope, "query": query, "limit": sub_limit });
        let outcome = fan_out(&self.exec, ctx, "fs.search", &sub_input, timeout).await;

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
    }

    impl FakeExec {
        fn new(targets: Vec<MeshTarget>, remote_outcome: SubTaskOutcome) -> Arc<Self> {
            Arc::new(Self {
                targets,
                local_calls: Mutex::new(vec![]),
                remote_calls: Mutex::new(vec![]),
                remote_outcome,
                local_fails: false,
            })
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
