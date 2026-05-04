//! Eligibility computation — pure routing.

use harness_core::{Cardinality, NodeId, Task};

use crate::dispatcher::{filter, live_set::LiveSet, round_robin::RoundRobin, DispatchPlan};
use crate::error::DispatchError;
use crate::index::{CapabilityIndex, ScopeIndex};

#[derive(Debug, Default)]
pub struct Dispatcher {
    capabilities: CapabilityIndex,
    scopes: ScopeIndex,
}

impl Dispatcher {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
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
                candidates.sort();
                Ok(DispatchPlan::Federated { nodes: candidates })
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
            DispatchPlan::Federated { nodes } => assert_eq!(
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
