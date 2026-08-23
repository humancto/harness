//! Eligibility computation — pure routing.

use std::sync::Arc;

use harness_core::{Cardinality, NodeId, Task};

use crate::dispatcher::{filter, live_set::LiveSet, round_robin::RoundRobin, DispatchPlan};
use crate::error::DispatchError;
use crate::index::{CapabilityIndex, ScopeIndex};

/// The indexes are `Arc`-shared since 3.3-fanout: the daemon's announce
/// path writes them (manifest upserts) while the dispatch runtime reads
/// them through this type. Both index types are internally locked.
#[derive(Debug, Default)]
pub struct Dispatcher {
    capabilities: Arc<CapabilityIndex>,
    scopes: Arc<ScopeIndex>,
}

impl Dispatcher {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Build around externally-owned (shared) indexes.
    #[must_use]
    pub fn with_indexes(capabilities: Arc<CapabilityIndex>, scopes: Arc<ScopeIndex>) -> Self {
        Self {
            capabilities,
            scopes,
        }
    }

    pub fn capability_index(&self) -> &CapabilityIndex {
        &self.capabilities
    }

    pub fn scope_index(&self) -> &ScopeIndex {
        &self.scopes
    }

    /// Pure routing decision. No I/O. Deterministic given the same inputs
    /// (live set, indexes). Returns the set of nodes that should execute,
    /// shaped by `Cardinality`.
    ///
    /// `Cardinality::Federated::merge` and `on_node_failure` are unused
    /// here — the dispatcher returns the eligible node set and the brain
    /// (4.5) consumes the strategy when streaming results back.
    ///
    /// # Errors
    /// Returns a typed [`DispatchError`] when no live peer can run the
    /// task (no advertiser, all dead, scope unowned, etc.).
    pub fn eligible<L: LiveSet>(
        &self,
        task: &Task,
        cardinality: &Cardinality,
        live: &L,
    ) -> Result<DispatchPlan, DispatchError> {
        // Owner input validation runs BEFORE any live-set consultation
        // (4.6, PR #49 review): a missing/non-string scope field is a
        // PERMANENT input error and must never be observable as
        // "bench filtered someone, then Owner failed" — the daemon's
        // breaker remap treats that combination as a transient to wait
        // out. Validating first keeps the two failure classes disjoint.
        if let Cardinality::Owner { scope_field } = cardinality {
            read_scope_field(task, scope_field)?;
        }
        let mut candidates = self.capabilities.nodes_for(&task.capability);
        candidates = live.live_subset(&candidates);
        candidates = filter::apply_constraints(candidates, &task.constraints, &self.scopes, live)?;
        if candidates.is_empty() {
            return Err(DispatchError::NoEligibleNodes {
                capability: task.capability.clone(),
            });
        }
        match cardinality {
            Cardinality::Anyone => {
                // Lowest-NodeId tiebreak. Phase 2.4 swaps this for
                // round-robin once the cursor table lands.
                candidates.sort();
                Ok(DispatchPlan::Single {
                    node: candidates[0],
                })
            }
            Cardinality::Owner { scope_field } => {
                let scope_id = read_scope_field(task, scope_field)?;
                let owners = self.scopes.owners(&scope_id);
                let owners = live.live_subset(&owners);
                let mut intersected: Vec<NodeId> = owners
                    .into_iter()
                    .filter(|n| candidates.contains(n))
                    .collect();
                if intersected.is_empty() {
                    return Err(DispatchError::Owner {
                        reason: format!(
                            "no live owner of scope {scope_id:?} advertises {:?}",
                            task.capability
                        ),
                    });
                }
                intersected.sort();
                Ok(DispatchPlan::Single {
                    node: intersected[0],
                })
            }
            Cardinality::Federated { .. } => {
                // 4.5 (ADR-0027): a pinned federated task is a federated
                // SUB-task (or an operator pin) — it must EXECUTE on the
                // pin, not re-coordinate, or every sub-task would
                // recursively fan out. Anyone/Owner pin behavior is
                // untouched. `apply_constraints` already narrowed
                // candidates to the pin.
                if let Some(pin) = task.constraints.pin_to_node {
                    return Ok(DispatchPlan::Single { node: pin });
                }
                candidates.sort();
                Ok(DispatchPlan::Federated {
                    nodes: candidates,
                    excluded: vec![],
                })
            }
            // `Cardinality` is `#[non_exhaustive]`; future variants should
            // be a hard failure here so we never silently mis-route.
            other => Err(DispatchError::Owner {
                reason: format!("unsupported cardinality variant: {other:?}"),
            }),
        }
    }

    /// Like [`Self::eligible`] but use a round-robin selector to break
    /// ties in `Anyone` cardinality. The selector's cursor advances on
    /// every successful call. Pass the same `RoundRobin` across
    /// dispatches to actually round-robin.
    ///
    /// `Owner` and `Federated` paths are unchanged from
    /// [`Self::eligible`] — the round-robin selector only applies to
    /// `Anyone`.
    ///
    /// # Errors
    /// Same set as [`Self::eligible`].
    pub fn eligible_with_rr<L: LiveSet>(
        &self,
        task: &Task,
        cardinality: &Cardinality,
        live: &L,
        rr: &RoundRobin,
    ) -> Result<DispatchPlan, DispatchError> {
        // Owner input validation runs BEFORE any live-set consultation
        // (4.6, PR #49 review): a missing/non-string scope field is a
        // PERMANENT input error and must never be observable as
        // "bench filtered someone, then Owner failed" — the daemon's
        // breaker remap treats that combination as a transient to wait
        // out. Validating first keeps the two failure classes disjoint.
        if let Cardinality::Owner { scope_field } = cardinality {
            read_scope_field(task, scope_field)?;
        }
        let mut candidates = self.capabilities.nodes_for(&task.capability);
        candidates = live.live_subset(&candidates);
        candidates = filter::apply_constraints(candidates, &task.constraints, &self.scopes, live)?;
        if candidates.is_empty() {
            return Err(DispatchError::NoEligibleNodes {
                capability: task.capability.clone(),
            });
        }
        match cardinality {
            Cardinality::Anyone => {
                candidates.sort();
                let chosen = rr.next(&task.capability, &candidates)?;
                Ok(DispatchPlan::Single { node: chosen })
            }
            other => self.eligible(task, other, live),
        }
    }

    /// Resource-aware `Anyone` selection (roadmap 4.4, ADR-0026):
    /// candidates are filtered exactly as [`Self::eligible_with_rr`],
    /// then ranked by [`crate::dispatcher::score::fit_score`]. Hard-gated
    /// (score-0) nodes are dropped; among survivors, every node within
    /// [`Self::TIE_EPSILON`] of the best score forms a tie band resolved
    /// by the round-robin cursor — determinism for equal inputs,
    /// starvation avoidance, and exact round-robin behavior when scores
    /// are uniform (equal-capacity fleets with today's unpopulated
    /// heartbeats).
    ///
    /// `Owner` / `Federated` (4.7, ADR-0029): no longer delegated to the
    /// `LoadView`-less [`Self::eligible`] — every arm now consults
    /// `loads` so a live-but-`paused` node WAITS work instead of
    /// receiving it:
    /// - a live-but-paused **pin** ⇒ `ResourceGated` (a DEAD pin stays
    ///   `PinnedNodeNotLive` — dead ≠ paused; the pause gate consults
    ///   only live nodes, so the fast-terminal path is untouched);
    /// - a paused **owner** is dropped from the intersected owner set;
    ///   all owners paused ⇒ `ResourceGated` (owners wait, never
    ///   silently reroute outside the owner set);
    /// - unpinned **Federated** sets exclude paused candidates into
    ///   `DispatchPlan::Federated::excluded` (Skipped in provenance —
    ///   federated stays score-BLIND but is no longer pressure-DEAF);
    ///   all paused ⇒ `ResourceGated`.
    ///
    /// # Errors
    /// [`DispatchError::ResourceGated`] when candidates exist but every
    /// one is hard-gated or paused — a transient condition the caller
    /// must treat as "wait", never as terminal (plan review BLOCKER-1).
    /// Otherwise the same set as [`Self::eligible`].
    pub fn eligible_scored<L: LiveSet, V: crate::dispatcher::score::LoadView>(
        &self,
        task: &Task,
        hints: &harness_core::ResourceHints,
        cardinality: &Cardinality,
        live: &L,
        loads: &V,
        rr: &RoundRobin,
    ) -> Result<DispatchPlan, DispatchError> {
        // Owner input validation runs BEFORE any live-set consultation
        // (4.6, PR #49 review): a missing/non-string scope field is a
        // PERMANENT input error and must never be observable as
        // "bench filtered someone, then Owner failed" — the daemon's
        // breaker remap treats that combination as a transient to wait
        // out. Validating first keeps the two failure classes disjoint.
        if let Cardinality::Owner { scope_field } = cardinality {
            read_scope_field(task, scope_field)?;
        }
        let mut candidates = self.capabilities.nodes_for(&task.capability);
        candidates = live.live_subset(&candidates);
        candidates = filter::apply_constraints(candidates, &task.constraints, &self.scopes, live)?;
        if candidates.is_empty() {
            return Err(DispatchError::NoEligibleNodes {
                capability: task.capability.clone(),
            });
        }
        candidates.sort();
        match cardinality {
            Cardinality::Anyone => {
                // Anyone (pins included) gates paused inside fit_score —
                // a paused node scores 0 and drops here.
                let scored: Vec<(NodeId, f64)> = candidates
                    .iter()
                    .map(|n| {
                        (
                            *n,
                            crate::dispatcher::score::fit_score(
                                hints,
                                &loads.snapshot(n),
                                loads.success_rate(n),
                            ),
                        )
                    })
                    .filter(|(_, s)| *s > 0.0)
                    .collect();
                if scored.is_empty() {
                    return Err(DispatchError::ResourceGated {
                        capability: task.capability.clone(),
                    });
                }
                let best = scored
                    .iter()
                    .map(|(_, s)| *s)
                    .fold(f64::NEG_INFINITY, f64::max);
                let tied: Vec<NodeId> = scored
                    .iter()
                    .filter(|(_, s)| *s >= best * (1.0 - Self::TIE_EPSILON))
                    .map(|(n, _)| *n)
                    .collect();
                // `tied` inherits the sorted candidate order; uniform
                // scores → tied == candidates → identical RR sequence
                // to eligible_with_rr.
                let chosen = rr.next(&task.capability, &tied)?;
                Ok(DispatchPlan::Single { node: chosen })
            }
            Cardinality::Owner { scope_field } => {
                let scope_id = read_scope_field(task, scope_field)?;
                let owners = self.scopes.owners(&scope_id);
                let owners = live.live_subset(&owners);
                let mut intersected: Vec<NodeId> = owners
                    .into_iter()
                    .filter(|n| candidates.contains(n))
                    .collect();
                if intersected.is_empty() {
                    return Err(DispatchError::Owner {
                        reason: format!(
                            "no live owner of scope {scope_id:?} advertises {:?}",
                            task.capability
                        ),
                    });
                }
                intersected.sort();
                intersected.retain(|n| !loads.snapshot(n).paused);
                if intersected.is_empty() {
                    return Err(DispatchError::ResourceGated {
                        capability: task.capability.clone(),
                    });
                }
                Ok(DispatchPlan::Single {
                    node: intersected[0],
                })
            }
            Cardinality::Federated { .. } => {
                // 4.5 (ADR-0027): a pinned federated task is a federated
                // SUB-task (or an operator pin) — it must EXECUTE on the
                // pin, not re-coordinate. `apply_constraints` already
                // narrowed candidates to the (live) pin.
                if let Some(pin) = task.constraints.pin_to_node {
                    if loads.snapshot(&pin).paused {
                        return Err(DispatchError::ResourceGated {
                            capability: task.capability.clone(),
                        });
                    }
                    return Ok(DispatchPlan::Single { node: pin });
                }
                let (nodes, excluded): (Vec<NodeId>, Vec<NodeId>) = candidates
                    .into_iter()
                    .partition(|n| !loads.snapshot(n).paused);
                if nodes.is_empty() {
                    return Err(DispatchError::ResourceGated {
                        capability: task.capability.clone(),
                    });
                }
                Ok(DispatchPlan::Federated { nodes, excluded })
            }
            // `Cardinality` is `#[non_exhaustive]`; future variants are
            // a hard failure so we never silently mis-route.
            other => Err(DispatchError::Owner {
                reason: format!("unsupported cardinality variant: {other:?}"),
            }),
        }
    }

    /// Scores within this relative band of the best are ties, resolved
    /// round-robin (determinism + starvation avoidance).
    pub const TIE_EPSILON: f64 = 0.01;
}

fn read_scope_field(task: &Task, field: &str) -> Result<String, DispatchError> {
    match task.input.get(field) {
        Some(serde_json::Value::String(s)) => Ok(s.clone()),
        Some(other) => Err(DispatchError::Owner {
            reason: format!("input field {field:?} is {} not string", type_of(other)),
        }),
        None => Err(DispatchError::Owner {
            reason: format!("input missing required scope field {field:?}"),
        }),
    }
}

fn type_of(v: &serde_json::Value) -> &'static str {
    match v {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "bool",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::dispatcher::live_set::StaticLiveSet;
    use harness_core::{
        Capability, Cardinality, Constraints, ExecutionPolicy, Identity, MergeStrategy,
        NodeManifest, PartialPolicy, Resources, RetryPolicy, Scope, SemVer, Signature, TaskId,
        TraceContext,
    };

    fn empty_resources() -> Resources {
        Resources {
            cpu_cores: 0,
            ram_total_mb: 0,
            gpu: None,
            os: "test".into(),
            arch: "test".into(),
        }
    }

    fn empty_hints() -> harness_core::ResourceHints {
        harness_core::ResourceHints {
            cpu_class: harness_core::protocol::CpuClass::Light,
            memory_mb: None,
            gpu_required: false,
            gpu_memory_mb: None,
            network_class: harness_core::protocol::NetworkClass::None,
            disk_io_class: harness_core::protocol::DiskIoClass::None,
            estimated_duration_ms: None,
        }
    }

    fn cap(id: &str) -> Capability {
        Capability {
            id: id.into(),
            version: SemVer {
                major: 0,
                minor: 1,
                patch: 0,
            },
            cardinality: Cardinality::Anyone,
            input_schema: serde_json::json!({}),
            output_schema: serde_json::json!({}),
            cost_hint: harness_core::protocol::CostHint::LocalFast,
            tags: vec![],
            rate_limit: None,
            resource_hints: empty_hints(),
            requires_secrets: vec![],
        }
    }

    fn manifest(node: u8, caps: &[Capability], scope_ids: &[&str]) -> NodeManifest {
        let id = Identity::generate();
        NodeManifest {
            node_id: NodeId::from_bytes([node; 16]),
            hostname: "h".into(),
            pubkey: *id.public_key(),
            capabilities: caps.to_vec(),
            scopes: scope_ids
                .iter()
                .map(|id| Scope {
                    kind: "directory".into(),
                    id: (*id).to_string(),
                    label: (*id).to_string(),
                    indexed: false,
                    last_indexed: None,
                })
                .collect(),
            secret_tags: vec![],
            resources: empty_resources(),
            online_since: 0,
            version: SemVer {
                major: 0,
                minor: 1,
                patch: 0,
            },
            sig: Signature::from_bytes([0; 64]),
        }
    }

    fn task(capability: &str, input: serde_json::Value) -> Task {
        Task {
            id: TaskId::new_v7(),
            parent: None,
            plan_id: None,
            capability: capability.into(),
            input,
            constraints: Constraints::default(),
            retry: RetryPolicy::default(),
            execution: ExecutionPolicy::default(),
            resource_hints: harness_core::ResourceHints {
                cpu_class: harness_core::protocol::CpuClass::Light,
                memory_mb: None,
                gpu_required: false,
                gpu_memory_mb: None,
                network_class: harness_core::protocol::NetworkClass::None,
                disk_io_class: harness_core::protocol::DiskIoClass::None,
                estimated_duration_ms: None,
            },
            trace_ctx: TraceContext::default(),
            issued_by: NodeId::from_bytes([0xFF; 16]),
            issued_at: 0,
            tags: Vec::new(),
            sig: Signature::from_bytes([0; 64]),
        }
    }

    fn build(nodes: &[(u8, Vec<Capability>, Vec<&str>)]) -> (Dispatcher, StaticLiveSet) {
        let d = Dispatcher::new();
        for (n, caps, scopes) in nodes {
            let m = manifest(*n, caps, scopes);
            d.capability_index().upsert_node(&m);
            d.scope_index().upsert_node(&m);
        }
        let live = StaticLiveSet::from_node_ids(
            nodes.iter().map(|(n, _, _)| NodeId::from_bytes([*n; 16])),
        );
        (d, live)
    }

    // ---------- Anyone ----------

    #[test]
    fn anyone_single_node_returns_that_node() {
        let (d, live) = build(&[(1, vec![cap("echo")], vec![])]);
        let t = task("echo", serde_json::json!({}));
        match d.eligible(&t, &Cardinality::Anyone, &live).unwrap() {
            DispatchPlan::Single { node } => assert_eq!(node, NodeId::from_bytes([1; 16])),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn anyone_picks_lowest_node_id_deterministically() {
        let (d, live) = build(&[
            (3, vec![cap("echo")], vec![]),
            (1, vec![cap("echo")], vec![]),
            (2, vec![cap("echo")], vec![]),
        ]);
        let t = task("echo", serde_json::json!({}));
        match d.eligible(&t, &Cardinality::Anyone, &live).unwrap() {
            DispatchPlan::Single { node } => assert_eq!(node, NodeId::from_bytes([1; 16])),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn anyone_no_eligible_nodes_errors() {
        let (d, live) = build(&[(1, vec![cap("shell.exec")], vec![])]);
        let t = task("echo", serde_json::json!({}));
        let err = d.eligible(&t, &Cardinality::Anyone, &live).unwrap_err();
        assert!(matches!(err, DispatchError::NoEligibleNodes { .. }));
    }

    #[test]
    fn anyone_dead_node_filtered_out() {
        let d = Dispatcher::new();
        let m = manifest(1, &[cap("echo")], &[]);
        d.capability_index().upsert_node(&m);
        let live = StaticLiveSet::default(); // no live nodes
        let t = task("echo", serde_json::json!({}));
        let err = d.eligible(&t, &Cardinality::Anyone, &live).unwrap_err();
        assert!(matches!(err, DispatchError::NoEligibleNodes { .. }));
    }

    // ---------- Owner ----------

    #[test]
    fn owner_routes_to_owner_of_scope() {
        let (d, live) = build(&[
            (1, vec![cap("fs.read")], vec!["~/work"]),
            (2, vec![cap("fs.read")], vec![]),
        ]);
        let t = task("fs.read", serde_json::json!({"path": "~/work"}));
        let card = Cardinality::Owner {
            scope_field: "path".into(),
        };
        match d.eligible(&t, &card, &live).unwrap() {
            DispatchPlan::Single { node } => assert_eq!(node, NodeId::from_bytes([1; 16])),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn owner_missing_input_field_errors() {
        let (d, live) = build(&[(1, vec![cap("fs.read")], vec!["~/work"])]);
        let t = task("fs.read", serde_json::json!({}));
        let err = d
            .eligible(
                &t,
                &Cardinality::Owner {
                    scope_field: "path".into(),
                },
                &live,
            )
            .unwrap_err();
        assert!(matches!(err, DispatchError::Owner { .. }));
    }

    #[test]
    fn owner_non_string_field_errors() {
        let (d, live) = build(&[(1, vec![cap("fs.read")], vec!["~/work"])]);
        let t = task("fs.read", serde_json::json!({"path": 42}));
        let err = d
            .eligible(
                &t,
                &Cardinality::Owner {
                    scope_field: "path".into(),
                },
                &live,
            )
            .unwrap_err();
        match err {
            DispatchError::Owner { reason } => assert!(reason.contains("not string")),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn owner_no_live_owner_errors() {
        let (d, live) = build(&[(1, vec![cap("fs.read")], vec![])]);
        let t = task("fs.read", serde_json::json!({"path": "~/elsewhere"}));
        let err = d
            .eligible(
                &t,
                &Cardinality::Owner {
                    scope_field: "path".into(),
                },
                &live,
            )
            .unwrap_err();
        assert!(matches!(err, DispatchError::Owner { .. }));
    }

    #[test]
    fn owner_with_two_owners_picks_lowest_node_id() {
        let (d, live) = build(&[
            (5, vec![cap("fs.read")], vec!["nas"]),
            (2, vec![cap("fs.read")], vec!["nas"]),
        ]);
        let t = task("fs.read", serde_json::json!({"path": "nas"}));
        let card = Cardinality::Owner {
            scope_field: "path".into(),
        };
        match d.eligible(&t, &card, &live).unwrap() {
            DispatchPlan::Single { node } => assert_eq!(node, NodeId::from_bytes([2; 16])),
            other => panic!("unexpected: {other:?}"),
        }
    }

    // ---------- Federated ----------

    fn federated() -> Cardinality {
        Cardinality::Federated {
            merge: MergeStrategy::Concat,
            on_node_failure: PartialPolicy::ReturnPartial,
        }
    }

    #[test]
    fn federated_returns_all_eligible_sorted() {
        let (d, live) = build(&[
            (3, vec![cap("mesh.search")], vec![]),
            (1, vec![cap("mesh.search")], vec![]),
            (2, vec![cap("mesh.search")], vec![]),
        ]);
        let t = task("mesh.search", serde_json::json!({"q": "x"}));
        match d.eligible(&t, &federated(), &live).unwrap() {
            DispatchPlan::Federated { nodes, .. } => assert_eq!(
                nodes,
                vec![
                    NodeId::from_bytes([1; 16]),
                    NodeId::from_bytes([2; 16]),
                    NodeId::from_bytes([3; 16]),
                ]
            ),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn federated_empty_returns_error() {
        let (d, live) = build(&[(1, vec![cap("echo")], vec![])]);
        let t = task("mesh.search", serde_json::json!({}));
        let err = d.eligible(&t, &federated(), &live).unwrap_err();
        assert!(matches!(err, DispatchError::NoEligibleNodes { .. }));
    }

    // ---------- Constraints ----------

    #[test]
    fn pin_to_node_overrides_capability_filter() {
        let (d, live) = build(&[
            (1, vec![cap("echo")], vec![]),
            (2, vec![cap("echo")], vec![]),
        ]);
        let mut t = task("echo", serde_json::json!({}));
        t.constraints.pin_to_node = Some(NodeId::from_bytes([2; 16]));
        match d.eligible(&t, &Cardinality::Anyone, &live).unwrap() {
            DispatchPlan::Single { node } => assert_eq!(node, NodeId::from_bytes([2; 16])),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn pin_to_node_dead_errors() {
        let (d, live) = build(&[(1, vec![cap("echo")], vec![])]);
        let mut t = task("echo", serde_json::json!({}));
        t.constraints.pin_to_node = Some(NodeId::from_bytes([9; 16]));
        let err = d.eligible(&t, &Cardinality::Anyone, &live).unwrap_err();
        assert!(matches!(err, DispatchError::PinnedNodeNotLive { .. }));
    }

    #[test]
    fn pin_to_scope_intersects_with_capability() {
        let (d, live) = build(&[
            (1, vec![cap("fs.read")], vec!["~/work"]),
            (2, vec![cap("fs.read")], vec![]),
        ]);
        let mut t = task("fs.read", serde_json::json!({}));
        t.constraints.pin_to_scope = Some("~/work".into());
        match d.eligible(&t, &Cardinality::Anyone, &live).unwrap() {
            DispatchPlan::Single { node } => assert_eq!(node, NodeId::from_bytes([1; 16])),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn pin_to_scope_unowned_errors() {
        let (d, live) = build(&[(1, vec![cap("fs.read")], vec![])]);
        let mut t = task("fs.read", serde_json::json!({}));
        t.constraints.pin_to_scope = Some("~/missing".into());
        let err = d.eligible(&t, &Cardinality::Anyone, &live).unwrap_err();
        assert!(matches!(err, DispatchError::PinnedScopeUnowned { .. }));
    }

    #[test]
    fn pin_to_node_and_scope_intersect() {
        let (d, live) = build(&[
            (1, vec![cap("fs.read")], vec!["~/work"]),
            (2, vec![cap("fs.read")], vec!["~/work"]),
        ]);
        let mut t = task("fs.read", serde_json::json!({}));
        t.constraints.pin_to_node = Some(NodeId::from_bytes([2; 16]));
        t.constraints.pin_to_scope = Some("~/work".into());
        match d.eligible(&t, &Cardinality::Anyone, &live).unwrap() {
            DispatchPlan::Single { node } => assert_eq!(node, NodeId::from_bytes([2; 16])),
            other => panic!("unexpected: {other:?}"),
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic, dead_code)]
mod tests_support {
    use super::*;
    use crate::dispatcher::live_set::StaticLiveSet;
    use harness_core::{
        Capability, Cardinality, Constraints, ExecutionPolicy, Identity, NodeManifest, Resources,
        RetryPolicy, SemVer, Signature, TaskId, TraceContext,
    };

    pub(super) fn empty_hints_pub() -> harness_core::ResourceHints {
        harness_core::ResourceHints {
            cpu_class: harness_core::protocol::CpuClass::Light,
            memory_mb: None,
            gpu_required: false,
            gpu_memory_mb: None,
            network_class: harness_core::protocol::NetworkClass::None,
            disk_io_class: harness_core::protocol::DiskIoClass::None,
            estimated_duration_ms: None,
        }
    }

    pub(super) fn echo_cap() -> Capability {
        Capability {
            id: "echo".into(),
            version: SemVer {
                major: 0,
                minor: 1,
                patch: 0,
            },
            cardinality: Cardinality::Anyone,
            input_schema: serde_json::json!({}),
            output_schema: serde_json::json!({}),
            cost_hint: harness_core::protocol::CostHint::LocalFast,
            tags: vec![],
            rate_limit: None,
            resource_hints: empty_hints_pub(),
            requires_secrets: vec![],
        }
    }

    /// Three live nodes (ids 1..3) all advertising `echo`.
    pub(super) fn build3() -> (Dispatcher, StaticLiveSet) {
        let d = Dispatcher::new();
        for n in [1u8, 2, 3] {
            let id = Identity::generate();
            let m = NodeManifest {
                node_id: NodeId::from_bytes([n; 16]),
                hostname: "h".into(),
                pubkey: *id.public_key(),
                capabilities: vec![echo_cap()],
                scopes: vec![],
                secret_tags: vec![],
                resources: Resources {
                    cpu_cores: 0,
                    ram_total_mb: 0,
                    gpu: None,
                    os: "test".into(),
                    arch: "test".into(),
                },
                online_since: 0,
                version: SemVer {
                    major: 0,
                    minor: 1,
                    patch: 0,
                },
                sig: Signature::from_bytes([0; 64]),
            };
            d.capability_index().upsert_node(&m);
            d.scope_index().upsert_node(&m);
        }
        let live = StaticLiveSet::from_node_ids([1u8, 2, 3].map(|n| NodeId::from_bytes([n; 16])));
        (d, live)
    }

    pub(super) fn task_for(capability: &str) -> Task {
        Task {
            id: TaskId::new_v7(),
            parent: None,
            plan_id: None,
            capability: capability.into(),
            input: serde_json::json!({}),
            constraints: Constraints::default(),
            retry: RetryPolicy::default(),
            execution: ExecutionPolicy::default(),
            resource_hints: empty_hints_pub(),
            trace_ctx: TraceContext::default(),
            issued_by: NodeId::from_bytes([9; 16]),
            issued_at: 0,
            tags: vec![],
            sig: Signature::from_bytes([0; 64]),
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod scored_tests {
    use super::tests_support::*;
    use super::*;
    use crate::dispatcher::live_set::StaticLiveSet;
    use crate::dispatcher::score::{NodeSnapshot, StaticLoadView};
    use crate::dispatcher::RoundRobin;
    use harness_core::Cardinality;

    fn node(n: u8) -> NodeId {
        NodeId::from_bytes([n; 16])
    }

    fn pick(plan: DispatchPlan) -> NodeId {
        match plan {
            DispatchPlan::Single { node } => node,
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn s01_uniform_loads_reproduce_rr_sequence_exactly() {
        // Regression lock: equal-capacity candidates (all-default
        // snapshots) must round-robin identically to eligible_with_rr.
        let (d, live) = build3();
        let loads = StaticLoadView::default();
        let rr_scored = RoundRobin::new();
        let rr_plain = RoundRobin::new();
        let t = task_for("echo");
        for _ in 0..6 {
            let a = pick(
                d.eligible_scored(
                    &t,
                    &empty_hints_pub(),
                    &Cardinality::Anyone,
                    &live,
                    &loads,
                    &rr_scored,
                )
                .unwrap(),
            );
            let b = pick(
                d.eligible_with_rr(&t, &Cardinality::Anyone, &live, &rr_plain)
                    .unwrap(),
            );
            assert_eq!(a, b, "scored selection must match RR when uniform");
        }
    }

    #[test]
    fn s02_loaded_node_loses_argmax() {
        let (d, live) = build3();
        let mut loads = StaticLoadView::default();
        for n in [1u8, 2, 3] {
            loads.snapshots.insert(
                node(n),
                NodeSnapshot {
                    cpu_cores: 4,
                    ..NodeSnapshot::default()
                },
            );
        }
        // Node 1 is heavily loaded.
        loads.snapshots.get_mut(&node(1)).unwrap().assigned_inflight = 6;
        let rr = RoundRobin::new();
        let t = task_for("echo");
        for _ in 0..4 {
            let chosen = pick(
                d.eligible_scored(
                    &t,
                    &empty_hints_pub(),
                    &Cardinality::Anyone,
                    &live,
                    &loads,
                    &rr,
                )
                .unwrap(),
            );
            assert_ne!(chosen, node(1), "loaded node must lose argmax");
        }
    }

    #[test]
    fn s03_all_gated_is_resource_gated_error() {
        let (d, live) = build3();
        let mut loads = StaticLoadView::default();
        for n in [1u8, 2, 3] {
            loads.snapshots.insert(
                node(n),
                NodeSnapshot {
                    paused: true,
                    ..NodeSnapshot::default()
                },
            );
        }
        let rr = RoundRobin::new();
        let t = task_for("echo");
        let err = d
            .eligible_scored(
                &t,
                &empty_hints_pub(),
                &Cardinality::Anyone,
                &live,
                &loads,
                &rr,
            )
            .unwrap_err();
        assert!(matches!(err, DispatchError::ResourceGated { .. }));
    }

    #[test]
    fn s04_gated_node_excluded_from_rotation() {
        let (d, live) = build3();
        let mut loads = StaticLoadView::default();
        loads.snapshots.insert(
            node(2),
            NodeSnapshot {
                paused: true,
                ..NodeSnapshot::default()
            },
        );
        let rr = RoundRobin::new();
        let t = task_for("echo");
        for _ in 0..6 {
            let chosen = pick(
                d.eligible_scored(
                    &t,
                    &empty_hints_pub(),
                    &Cardinality::Anyone,
                    &live,
                    &loads,
                    &rr,
                )
                .unwrap(),
            );
            assert_ne!(chosen, node(2), "paused node never selected");
        }
    }

    #[test]
    fn s05_heterogeneous_cores_prefer_bigger_node() {
        // Deliberate new behavior (review MAJOR-3): capacity-
        // proportional placement on heterogeneous fleets.
        let (d, live) = build3();
        let mut loads = StaticLoadView::default();
        loads.snapshots.insert(
            node(1),
            NodeSnapshot {
                cpu_cores: 12,
                ..NodeSnapshot::default()
            },
        );
        for n in [2u8, 3] {
            loads.snapshots.insert(
                node(n),
                NodeSnapshot {
                    cpu_cores: 2,
                    ..NodeSnapshot::default()
                },
            );
        }
        let rr = RoundRobin::new();
        let t = task_for("echo");
        let chosen = pick(
            d.eligible_scored(
                &t,
                &empty_hints_pub(),
                &Cardinality::Anyone,
                &live,
                &loads,
                &rr,
            )
            .unwrap(),
        );
        assert_eq!(chosen, node(1), "idle big node wins first placement");
        // Load it up: placement rebalances to the small nodes.
        loads.snapshots.get_mut(&node(1)).unwrap().assigned_inflight = 20;
        let chosen = pick(
            d.eligible_scored(
                &t,
                &empty_hints_pub(),
                &Cardinality::Anyone,
                &live,
                &loads,
                &rr,
            )
            .unwrap(),
        );
        assert_ne!(chosen, node(1), "loaded big node loses");
    }

    #[test]
    fn s06_tie_band_alternates_round_robin() {
        let (d, live) = build3();
        let mut loads = StaticLoadView::default();
        // Nodes 1 and 2 identical; node 3 clearly worse (outside band).
        for n in [1u8, 2] {
            loads.snapshots.insert(
                node(n),
                NodeSnapshot {
                    cpu_cores: 4,
                    ..NodeSnapshot::default()
                },
            );
        }
        loads.snapshots.insert(
            node(3),
            NodeSnapshot {
                cpu_cores: 4,
                assigned_inflight: 7,
                ..NodeSnapshot::default()
            },
        );
        let rr = RoundRobin::new();
        let t = task_for("echo");
        let picks: Vec<NodeId> = (0..4)
            .map(|_| {
                pick(
                    d.eligible_scored(
                        &t,
                        &empty_hints_pub(),
                        &Cardinality::Anyone,
                        &live,
                        &loads,
                        &rr,
                    )
                    .unwrap(),
                )
            })
            .collect();
        assert!(picks.iter().all(|p| *p != node(3)), "worse node excluded");
        assert!(
            picks.windows(2).all(|w| w[0] != w[1]),
            "tie band alternates: {picks:?}"
        );
    }

    #[test]
    fn s07_pinned_federated_routes_single_to_pin() {
        // 4.5 (ADR-0027): federated sub-tasks are pinned and must
        // EXECUTE, not re-coordinate.
        let (d, live) = build3();
        let mut t = task_for("echo");
        let pin = NodeId::from_bytes([2; 16]);
        t.constraints.pin_to_node = Some(pin);
        let fed = harness_core::Cardinality::Federated {
            merge: harness_core::protocol::MergeStrategy::Concat,
            on_node_failure: harness_core::PartialPolicy::ReturnPartial,
        };
        match d.eligible(&t, &fed, &live).unwrap() {
            DispatchPlan::Single { node } => assert_eq!(node, pin),
            other => panic!("must be Single: {other:?}"),
        }
        // Unpinned federated still fans out to all.
        let t = task_for("echo");
        match d.eligible(&t, &fed, &live).unwrap() {
            DispatchPlan::Federated { nodes, .. } => assert_eq!(nodes.len(), 3),
            other => panic!("must be Federated: {other:?}"),
        }
        // Anyone + pin is untouched (regression).
        let mut t = task_for("echo");
        t.constraints.pin_to_node = Some(pin);
        match d
            .eligible(&t, &harness_core::Cardinality::Anyone, &live)
            .unwrap()
        {
            DispatchPlan::Single { node } => assert_eq!(node, pin),
            other => panic!("unexpected: {other:?}"),
        }
    }

    // ---------- 4.7 (ADR-0029): pause gates in every arm ----------

    fn fed() -> Cardinality {
        Cardinality::Federated {
            merge: harness_core::protocol::MergeStrategy::Concat,
            on_node_failure: harness_core::PartialPolicy::ReturnPartial,
        }
    }

    fn paused_view(paused: &[u8]) -> StaticLoadView {
        let mut loads = StaticLoadView::default();
        for n in paused {
            loads.snapshots.insert(
                node(*n),
                NodeSnapshot {
                    paused: true,
                    ..NodeSnapshot::default()
                },
            );
        }
        loads
    }

    /// Three live `echo` nodes; nodes 1 and 2 own scope `"nas"`.
    fn build3_nas_owners() -> (Dispatcher, StaticLiveSet) {
        let (d, live) = build3();
        for n in [1u8, 2] {
            let id = harness_core::Identity::generate();
            let m = harness_core::NodeManifest {
                node_id: node(n),
                hostname: "h".into(),
                pubkey: *id.public_key(),
                capabilities: vec![echo_cap()],
                scopes: vec![harness_core::Scope {
                    kind: "directory".into(),
                    id: "nas".into(),
                    label: "nas".into(),
                    indexed: false,
                    last_indexed: None,
                }],
                secret_tags: vec![],
                resources: harness_core::Resources {
                    cpu_cores: 0,
                    ram_total_mb: 0,
                    gpu: None,
                    os: "test".into(),
                    arch: "test".into(),
                },
                online_since: 0,
                version: harness_core::SemVer {
                    major: 0,
                    minor: 1,
                    patch: 0,
                },
                sig: harness_core::Signature::from_bytes([0; 64]),
            };
            d.scope_index().upsert_node(&m);
        }
        (d, live)
    }

    #[test]
    fn s08_paused_federated_pin_waits_dead_pin_stays_terminal() {
        let (d, live) = build3();
        let rr = RoundRobin::new();
        let mut t = task_for("echo");
        t.constraints.pin_to_node = Some(node(2));

        // Live-but-paused pin ⇒ ResourceGated (wait, never fail).
        let err = d
            .eligible_scored(
                &t,
                &empty_hints_pub(),
                &fed(),
                &live,
                &paused_view(&[2]),
                &rr,
            )
            .unwrap_err();
        assert!(matches!(err, DispatchError::ResourceGated { .. }));

        // DEAD pin stays PinnedNodeNotLive — dead ≠ paused (m08's
        // fast-terminal path untouched).
        t.constraints.pin_to_node = Some(node(9));
        let err = d
            .eligible_scored(
                &t,
                &empty_hints_pub(),
                &fed(),
                &live,
                &paused_view(&[]),
                &rr,
            )
            .unwrap_err();
        assert!(matches!(err, DispatchError::PinnedNodeNotLive { .. }));

        // Unpaused live pin executes (regression).
        t.constraints.pin_to_node = Some(node(2));
        match d
            .eligible_scored(
                &t,
                &empty_hints_pub(),
                &fed(),
                &live,
                &paused_view(&[]),
                &rr,
            )
            .unwrap()
        {
            DispatchPlan::Single { node: n } => assert_eq!(n, node(2)),
            other => panic!("must be Single: {other:?}"),
        }
    }

    #[test]
    fn s09_federated_set_excludes_paused_all_paused_waits() {
        let (d, live) = build3();
        let rr = RoundRobin::new();
        let t = task_for("echo");

        match d
            .eligible_scored(
                &t,
                &empty_hints_pub(),
                &fed(),
                &live,
                &paused_view(&[2]),
                &rr,
            )
            .unwrap()
        {
            DispatchPlan::Federated { nodes, excluded } => {
                assert_eq!(nodes, vec![node(1), node(3)]);
                assert_eq!(excluded, vec![node(2)], "paused node visible, not fanned");
            }
            other => panic!("must be Federated: {other:?}"),
        }

        let err = d
            .eligible_scored(
                &t,
                &empty_hints_pub(),
                &fed(),
                &live,
                &paused_view(&[1, 2, 3]),
                &rr,
            )
            .unwrap_err();
        assert!(matches!(err, DispatchError::ResourceGated { .. }));
    }

    #[test]
    fn s10_paused_owners_wait_never_reroute() {
        let (d, live) = build3_nas_owners();
        let rr = RoundRobin::new();
        let mut t = task_for("echo");
        t.input = serde_json::json!({"path": "nas"});
        let card = Cardinality::Owner {
            scope_field: "path".into(),
        };

        // Lowest owner paused → the OTHER owner serves (still an owner).
        match d
            .eligible_scored(
                &t,
                &empty_hints_pub(),
                &card,
                &live,
                &paused_view(&[1]),
                &rr,
            )
            .unwrap()
        {
            DispatchPlan::Single { node: n } => assert_eq!(n, node(2)),
            other => panic!("unexpected: {other:?}"),
        }

        // ALL owners paused ⇒ ResourceGated — wait, never reroute to a
        // non-owner (node 3 advertises echo but does not own "nas").
        let err = d
            .eligible_scored(
                &t,
                &empty_hints_pub(),
                &card,
                &live,
                &paused_view(&[1, 2]),
                &rr,
            )
            .unwrap_err();
        assert!(matches!(err, DispatchError::ResourceGated { .. }));
    }

    #[test]
    fn s11_unpaused_owner_and_federated_match_eligible_exactly() {
        // Regression lock: with no pause anywhere, the scored non-Anyone
        // arms decide identically to the LoadView-less eligible().
        let (d, live) = build3_nas_owners();
        let rr = RoundRobin::new();
        let loads = StaticLoadView::default();

        let mut t = task_for("echo");
        t.input = serde_json::json!({"path": "nas"});
        let card = Cardinality::Owner {
            scope_field: "path".into(),
        };
        assert_eq!(
            d.eligible_scored(&t, &empty_hints_pub(), &card, &live, &loads, &rr)
                .unwrap(),
            d.eligible(&t, &card, &live).unwrap()
        );

        let t = task_for("echo");
        assert_eq!(
            d.eligible_scored(&t, &empty_hints_pub(), &fed(), &live, &loads, &rr)
                .unwrap(),
            d.eligible(&t, &fed(), &live).unwrap()
        );
    }
}
