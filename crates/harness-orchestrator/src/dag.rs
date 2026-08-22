//! `DagScheduler` — incremental topological scheduling over a validated
//! [`Plan`] (roadmap 4.3, PRD §14.4, ADR-0025).
//!
//! Pure: no store, no runtime, no I/O. The scheduler tracks per-step
//! state, computes the ready set as dependencies settle, cascades
//! failure to transitive dependents (which are never dispatched — a
//! skipped step has no task row), and retains step outputs only while
//! an unsettled dependent may still reference them.
//!
//! Edge orientation follows ADR-0002: `(from, to)` means "**from
//! depends on to**" — `to` must complete before `from` starts. Edges
//! are deduplicated at construction; self-edges are cycles.
//!
//! Defense in depth (repo rule 8): the scheduler re-checks acyclicity,
//! dangling edges, and size even though `validate_plan` already did —
//! a bad graph is a constructor error, never a hang.

use std::collections::{HashMap, HashSet};

use harness_core::{Plan, TaskId};
use serde_json::Value as JsonValue;

/// Bound on plan size accepted by the executor — matches the mesh-meta
/// fan-out target cap and the ADR-0017 per-peer queue bound.
pub const MAX_PLAN_STEPS: usize = 64;

/// Lifecycle of one plan step inside the scheduler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepState {
    /// Dependencies not yet satisfied.
    Waiting,
    /// Returned from a ready set; the driver owns it now.
    InFlight,
    Done,
    Failed,
    TimedOut,
    /// Never dispatched: a transitive dependency failed, or the plan
    /// was aborted.
    Skipped,
}

impl StepState {
    #[must_use]
    pub fn is_terminal(self) -> bool {
        !matches!(self, Self::Waiting | Self::InFlight)
    }

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Waiting => "waiting",
            Self::InFlight => "in_flight",
            Self::Done => "done",
            Self::Failed => "failed",
            Self::TimedOut => "timed_out",
            Self::Skipped => "skipped",
        }
    }
}

/// Terminal outcome the driver reports for one in-flight step.
#[derive(Debug, Clone)]
pub enum StepOutcome {
    Done(JsonValue),
    Failed(String),
    TimedOut,
}

/// Constructor / bookkeeping errors. Every variant is a caller bug or
/// an invalid graph — the scheduler never hangs on one.
#[derive(Debug, thiserror::Error)]
pub enum DagError {
    #[error("plan has a dependency cycle")]
    Cycle,
    #[error("edge references unknown step {}", .0.0)]
    DanglingEdge(TaskId),
    #[error("plan is empty")]
    Empty,
    #[error("plan exceeds {MAX_PLAN_STEPS} steps ({0})")]
    TooLarge(usize),
    #[error("step {} is not in-flight", .0.0)]
    NotInFlight(TaskId),
}

/// Result of recording one terminal step.
#[derive(Debug, Clone, Default)]
pub struct Progress {
    /// Steps whose dependencies all just completed — dispatch these
    /// next. Sorted by `TaskId` for deterministic dispatch order.
    pub newly_ready: Vec<TaskId>,
    /// Steps transitively skipped because a dependency failed or timed
    /// out. Sorted by `TaskId`.
    pub newly_skipped: Vec<TaskId>,
}

/// Aggregate counts over every step.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DagSummary {
    pub total: usize,
    pub done: usize,
    pub failed: usize,
    pub timed_out: usize,
    pub skipped: usize,
}

/// See the module docs.
pub struct DagScheduler {
    /// step → deduplicated prerequisites.
    deps: HashMap<TaskId, Vec<TaskId>>,
    /// prerequisite → steps that depend on it (deduplicated).
    dependents: HashMap<TaskId, Vec<TaskId>>,
    /// step → count of unsettled prerequisites.
    remaining: HashMap<TaskId, usize>,
    states: HashMap<TaskId, StepState>,
    /// Outputs of `Done` steps retained while ≥1 dependent is
    /// unsettled.
    outputs: HashMap<TaskId, JsonValue>,
    /// Done step → count of its dependents not yet settled.
    pending_refs: HashMap<TaskId, usize>,
    in_flight_count: usize,
    waiting_count: usize,
}

impl std::fmt::Debug for DagScheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DagScheduler")
            .field("total", &self.states.len())
            .field("in_flight", &self.in_flight_count)
            .field("waiting", &self.waiting_count)
            .finish_non_exhaustive()
    }
}

impl DagScheduler {
    /// Build the dependency graph from a plan. Rejects empty and
    /// oversized plans, dangling edges, self-edges, and cycles.
    /// Duplicate edges are deduplicated (they would otherwise strand a
    /// step's `remaining` count above its settled-prerequisite count).
    pub fn new(plan: &Plan) -> Result<Self, DagError> {
        if plan.tasks.is_empty() {
            return Err(DagError::Empty);
        }
        if plan.tasks.len() > MAX_PLAN_STEPS {
            return Err(DagError::TooLarge(plan.tasks.len()));
        }
        let mut edge_set: HashSet<(TaskId, TaskId)> = HashSet::new();
        for &(from, to) in &plan.edges {
            if !plan.tasks.contains_key(&from) {
                return Err(DagError::DanglingEdge(from));
            }
            if !plan.tasks.contains_key(&to) {
                return Err(DagError::DanglingEdge(to));
            }
            if from == to {
                return Err(DagError::Cycle);
            }
            edge_set.insert((from, to));
        }
        let mut deps: HashMap<TaskId, Vec<TaskId>> = HashMap::new();
        let mut dependents: HashMap<TaskId, Vec<TaskId>> = HashMap::new();
        let mut remaining: HashMap<TaskId, usize> = HashMap::new();
        for &id in plan.tasks.keys() {
            deps.insert(id, Vec::new());
            dependents.insert(id, Vec::new());
            remaining.insert(id, 0);
        }
        for &(from, to) in &edge_set {
            deps.entry(from).or_default().push(to);
            dependents.entry(to).or_default().push(from);
            *remaining.entry(from).or_default() += 1;
        }
        // Kahn's algorithm over the deduplicated graph.
        {
            let mut indeg = remaining.clone();
            let mut queue: Vec<TaskId> = indeg
                .iter()
                .filter(|(_, &d)| d == 0)
                .map(|(&id, _)| id)
                .collect();
            let mut visited = 0usize;
            while let Some(id) = queue.pop() {
                visited += 1;
                for &d in dependents.get(&id).map_or(&[][..], Vec::as_slice) {
                    if let Some(r) = indeg.get_mut(&d) {
                        *r -= 1;
                        if *r == 0 {
                            queue.push(d);
                        }
                    }
                }
            }
            if visited != plan.tasks.len() {
                return Err(DagError::Cycle);
            }
        }
        let total = plan.tasks.len();
        let states = plan
            .tasks
            .keys()
            .map(|&id| (id, StepState::Waiting))
            .collect();
        Ok(Self {
            deps,
            dependents,
            remaining,
            states,
            outputs: HashMap::new(),
            pending_refs: HashMap::new(),
            in_flight_count: 0,
            waiting_count: total,
        })
    }

    /// Zero-in-degree steps, marked `InFlight`. Sorted by `TaskId`.
    pub fn take_initial_ready(&mut self) -> Vec<TaskId> {
        let mut ready: Vec<TaskId> = self
            .remaining
            .iter()
            .filter(|(id, &r)| r == 0 && self.states[id] == StepState::Waiting)
            .map(|(&id, _)| id)
            .collect();
        ready.sort_unstable();
        for id in &ready {
            self.mark_in_flight(*id);
        }
        ready
    }

    /// Record a terminal outcome for an in-flight step.
    ///
    /// On `Failed`/`TimedOut`, every transitive dependent moves
    /// `Waiting → Skipped` and is never dispatched. Retained outputs
    /// whose last referencing dependent just settled are freed.
    pub fn complete(&mut self, step: TaskId, outcome: StepOutcome) -> Result<Progress, DagError> {
        if self.states.get(&step) != Some(&StepState::InFlight) {
            return Err(DagError::NotInFlight(step));
        }
        self.in_flight_count -= 1;
        let mut progress = Progress::default();
        let failed = match outcome {
            StepOutcome::Done(output) => {
                self.states.insert(step, StepState::Done);
                let dependents = self.dependents[&step].len();
                if dependents > 0 {
                    self.outputs.insert(step, output);
                    self.pending_refs.insert(step, dependents);
                }
                false
            }
            StepOutcome::Failed(_) => {
                self.states.insert(step, StepState::Failed);
                true
            }
            StepOutcome::TimedOut => {
                self.states.insert(step, StepState::TimedOut);
                true
            }
        };
        self.settle_prereqs_of(step);
        if failed {
            // Cascade BEFORE readiness bookkeeping so no dependent of a
            // failed step can slip into the ready set.
            let mut stack = vec![step];
            while let Some(cur) = stack.pop() {
                let next: Vec<TaskId> = self.dependents[&cur]
                    .iter()
                    .copied()
                    .filter(|d| self.states[d] == StepState::Waiting)
                    .collect();
                for d in next {
                    self.states.insert(d, StepState::Skipped);
                    self.waiting_count -= 1;
                    self.settle_prereqs_of(d);
                    progress.newly_skipped.push(d);
                    stack.push(d);
                }
            }
            progress.newly_skipped.sort_unstable();
        } else {
            for d in self.dependents[&step].clone() {
                if self.states[&d] != StepState::Waiting {
                    continue;
                }
                let Some(r) = self.remaining.get_mut(&d) else {
                    continue; // unreachable: every step is tracked
                };
                *r -= 1;
                if *r == 0 {
                    progress.newly_ready.push(d);
                }
            }
            progress.newly_ready.sort_unstable();
            for id in &progress.newly_ready {
                self.mark_in_flight(*id);
            }
        }
        // Stall-proof invariant: waiting steps always have an in-flight
        // ancestor. Unreachable post-validation; if a future bug breaks
        // it, fail visibly (skip the strays) instead of hanging.
        if self.in_flight_count == 0 && self.waiting_count > 0 {
            debug_assert!(
                false,
                "DAG invariant broken: waiting steps with nothing in flight"
            );
            let strays: Vec<TaskId> = self
                .states
                .iter()
                .filter(|(_, &s)| s == StepState::Waiting)
                .map(|(&id, _)| id)
                .collect();
            for id in strays {
                self.states.insert(id, StepState::Skipped);
                self.waiting_count -= 1;
                self.settle_prereqs_of(id);
                progress.newly_skipped.push(id);
            }
            progress.newly_skipped.sort_unstable();
        }
        Ok(progress)
    }

    /// Abort: every non-terminal step → `Skipped` (fail-fast / deadline
    /// path). Returns the skipped ids, sorted.
    pub fn skip_remaining(&mut self) -> Vec<TaskId> {
        let mut skipped: Vec<TaskId> = self
            .states
            .iter()
            .filter(|(_, s)| !s.is_terminal())
            .map(|(&id, _)| id)
            .collect();
        skipped.sort_unstable();
        for id in &skipped {
            match self.states[id] {
                StepState::Waiting => self.waiting_count -= 1,
                StepState::InFlight => self.in_flight_count -= 1,
                _ => {}
            }
            self.states.insert(*id, StepState::Skipped);
        }
        self.outputs.clear();
        self.pending_refs.clear();
        skipped
    }

    /// A `Done` step's retained output — present only while ≥1 of its
    /// dependents is unsettled.
    #[must_use]
    pub fn output_of(&self, step: TaskId) -> Option<&JsonValue> {
        self.outputs.get(&step)
    }

    #[must_use]
    pub fn is_settled(&self) -> bool {
        self.in_flight_count == 0 && self.waiting_count == 0
    }

    #[must_use]
    pub fn state(&self, step: TaskId) -> Option<StepState> {
        self.states.get(&step).copied()
    }

    #[must_use]
    pub fn summary(&self) -> DagSummary {
        let mut s = DagSummary {
            total: self.states.len(),
            ..DagSummary::default()
        };
        for state in self.states.values() {
            match state {
                StepState::Done => s.done += 1,
                StepState::Failed => s.failed += 1,
                StepState::TimedOut => s.timed_out += 1,
                StepState::Skipped => s.skipped += 1,
                StepState::Waiting | StepState::InFlight => {}
            }
        }
        s
    }

    /// This step's deduplicated prerequisites (its declared
    /// dependencies).
    #[must_use]
    pub fn deps_of(&self, step: TaskId) -> &[TaskId] {
        self.deps.get(&step).map_or(&[][..], Vec::as_slice)
    }

    fn mark_in_flight(&mut self, id: TaskId) {
        self.states.insert(id, StepState::InFlight);
        self.waiting_count -= 1;
        self.in_flight_count += 1;
    }

    /// A step settled — release its prerequisites' retained outputs
    /// once their last dependent settles.
    fn settle_prereqs_of(&mut self, step: TaskId) {
        for p in self.deps[&step].clone() {
            if let Some(refs) = self.pending_refs.get_mut(&p) {
                *refs -= 1;
                if *refs == 0 {
                    self.pending_refs.remove(&p);
                    self.outputs.remove(&p);
                }
            }
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::many_single_char_names
)]
mod tests {
    use super::*;
    use harness_core::{NodeId, PlanId, PlanNode, ResourceHints, Signature};
    use serde_json::json;

    fn hints() -> ResourceHints {
        ResourceHints {
            cpu_class: harness_core::protocol::CpuClass::Light,
            memory_mb: None,
            gpu_required: false,
            gpu_memory_mb: None,
            network_class: harness_core::protocol::NetworkClass::None,
            disk_io_class: harness_core::protocol::DiskIoClass::None,
            estimated_duration_ms: None,
        }
    }

    fn node(id: TaskId) -> PlanNode {
        PlanNode {
            id,
            capability: "echo".into(),
            input: json!({}),
            resource_hints: hints(),
            timeout_ms: None,
        }
    }

    fn plan_of(ids: &[TaskId], edges: Vec<(TaskId, TaskId)>) -> Plan {
        Plan {
            id: PlanId::new_v7(),
            name: "t".into(),
            tasks: ids.iter().map(|&i| (i, node(i))).collect(),
            edges,
            budget: None,
            checkpoint: None,
            issued_by: NodeId::from_bytes([1; 16]),
            sig: Signature::from_bytes([0u8; 64]),
        }
    }

    fn ids(n: usize) -> Vec<TaskId> {
        let mut v: Vec<TaskId> = (0..n).map(|_| TaskId::new_v7()).collect();
        v.sort_unstable();
        v
    }

    #[test]
    fn t01_chain_executes_in_order() {
        let v = ids(3);
        let (a, b, c) = (v[0], v[1], v[2]);
        // c depends on b depends on a.
        let mut s = DagScheduler::new(&plan_of(&v, vec![(b, a), (c, b)])).expect("new");
        assert_eq!(s.take_initial_ready(), vec![a]);
        let p = s.complete(a, StepOutcome::Done(json!(1))).expect("a");
        assert_eq!(p.newly_ready, vec![b]);
        let p = s.complete(b, StepOutcome::Done(json!(2))).expect("b");
        assert_eq!(p.newly_ready, vec![c]);
        let p = s.complete(c, StepOutcome::Done(json!(3))).expect("c");
        assert!(p.newly_ready.is_empty());
        assert!(s.is_settled());
        assert_eq!(s.summary().done, 3);
    }

    #[test]
    fn t02_diamond_join_waits_for_both() {
        let v = ids(4);
        let (a, b, c, d) = (v[0], v[1], v[2], v[3]);
        // b,c depend on a; d depends on both b and c.
        let mut s =
            DagScheduler::new(&plan_of(&v, vec![(b, a), (c, a), (d, b), (d, c)])).expect("new");
        assert_eq!(s.take_initial_ready(), vec![a]);
        let p = s.complete(a, StepOutcome::Done(json!("a"))).expect("a");
        assert_eq!(p.newly_ready, vec![b, c]);
        let p = s.complete(b, StepOutcome::Done(json!("b"))).expect("b");
        assert!(p.newly_ready.is_empty(), "d must wait for c");
        let p = s.complete(c, StepOutcome::Done(json!("c"))).expect("c");
        assert_eq!(p.newly_ready, vec![d]);
        // b and c retained for d; a freed (both dependents settled).
        assert!(s.output_of(a).is_none());
        assert_eq!(s.output_of(b), Some(&json!("b")));
        s.complete(d, StepOutcome::Done(json!("d"))).expect("d");
        assert!(s.is_settled());
        assert!(s.output_of(b).is_none(), "freed after d settled");
    }

    #[test]
    fn t03_failure_skips_transitive_dependents_only() {
        let v = ids(5);
        let (a, b, c, d, e) = (v[0], v[1], v[2], v[3], v[4]);
        // Branch 1: b→a, d→b. Branch 2: c (root), e→c. a fails.
        let mut s = DagScheduler::new(&plan_of(&v, vec![(b, a), (d, b), (e, c)])).expect("new");
        assert_eq!(s.take_initial_ready(), vec![a, c]);
        let p = s
            .complete(a, StepOutcome::Failed("boom".into()))
            .expect("a");
        assert_eq!(p.newly_skipped, vec![b, d], "transitive skip is exact");
        assert!(p.newly_ready.is_empty());
        // Independent branch continues.
        let p = s.complete(c, StepOutcome::Done(json!("c"))).expect("c");
        assert_eq!(p.newly_ready, vec![e]);
        s.complete(e, StepOutcome::Done(json!("e"))).expect("e");
        assert!(s.is_settled());
        let sum = s.summary();
        assert_eq!((sum.done, sum.failed, sum.skipped), (2, 1, 2));
    }

    #[test]
    fn t04_timeout_cascades_like_failure() {
        let v = ids(2);
        let (a, b) = (v[0], v[1]);
        let mut s = DagScheduler::new(&plan_of(&v, vec![(b, a)])).expect("new");
        s.take_initial_ready();
        let p = s.complete(a, StepOutcome::TimedOut).expect("a");
        assert_eq!(p.newly_skipped, vec![b]);
        assert!(s.is_settled());
        assert_eq!(s.summary().timed_out, 1);
    }

    #[test]
    fn t05_cycle_and_self_edge_rejected() {
        let v = ids(2);
        let (a, b) = (v[0], v[1]);
        assert!(matches!(
            DagScheduler::new(&plan_of(&v, vec![(a, b), (b, a)])),
            Err(DagError::Cycle)
        ));
        assert!(matches!(
            DagScheduler::new(&plan_of(&v, vec![(a, a)])),
            Err(DagError::Cycle)
        ));
    }

    #[test]
    fn t06_duplicate_edges_do_not_strand_readiness() {
        let v = ids(2);
        let (a, b) = (v[0], v[1]);
        // (b, a) twice: without dedup, b's remaining would stay at 1
        // after a completes and the plan would hang.
        let mut s = DagScheduler::new(&plan_of(&v, vec![(b, a), (b, a)])).expect("new");
        assert_eq!(s.take_initial_ready(), vec![a]);
        let p = s.complete(a, StepOutcome::Done(json!(1))).expect("a");
        assert_eq!(p.newly_ready, vec![b], "duplicate edge deduplicated");
        s.complete(b, StepOutcome::Done(json!(2))).expect("b");
        assert!(s.is_settled());
    }

    #[test]
    fn t07_rejects_empty_oversized_dangling() {
        assert!(matches!(
            DagScheduler::new(&plan_of(&[], vec![])),
            Err(DagError::Empty)
        ));
        let big = ids(MAX_PLAN_STEPS + 1);
        assert!(matches!(
            DagScheduler::new(&plan_of(&big, vec![])),
            Err(DagError::TooLarge(n)) if n == MAX_PLAN_STEPS + 1
        ));
        let v = ids(1);
        let ghost = TaskId::new_v7();
        assert!(matches!(
            DagScheduler::new(&plan_of(&v, vec![(v[0], ghost)])),
            Err(DagError::DanglingEdge(_))
        ));
    }

    #[test]
    fn t08_complete_requires_in_flight() {
        let v = ids(2);
        let (a, b) = (v[0], v[1]);
        let mut s = DagScheduler::new(&plan_of(&v, vec![(b, a)])).expect("new");
        // b is Waiting, never returned ready.
        assert!(matches!(
            s.complete(b, StepOutcome::Done(json!(1))),
            Err(DagError::NotInFlight(_))
        ));
        // Completing a step twice is also rejected.
        s.take_initial_ready();
        s.complete(a, StepOutcome::Done(json!(1))).expect("a");
        assert!(matches!(
            s.complete(a, StepOutcome::Done(json!(1))),
            Err(DagError::NotInFlight(_))
        ));
    }

    #[test]
    fn t09_skip_remaining_aborts_everything_unsettled() {
        let v = ids(3);
        let (a, b, c) = (v[0], v[1], v[2]);
        let mut s = DagScheduler::new(&plan_of(&v, vec![(b, a), (c, b)])).expect("new");
        s.take_initial_ready();
        s.complete(a, StepOutcome::Done(json!(1))).expect("a");
        // b is now InFlight, c Waiting. Abort.
        let skipped = s.skip_remaining();
        assert_eq!(skipped, vec![b, c]);
        assert!(s.is_settled());
        assert!(s.output_of(a).is_none(), "retained outputs dropped");
        let sum = s.summary();
        assert_eq!((sum.done, sum.skipped), (1, 2));
    }

    #[test]
    fn t10_deterministic_ready_order_and_deps_accessor() {
        let v = ids(4);
        let (a, b, c, d) = (v[0], v[1], v[2], v[3]);
        let mut s = DagScheduler::new(&plan_of(&v, vec![(d, a), (d, b), (d, c)])).expect("new");
        // All three roots ready, sorted.
        assert_eq!(s.take_initial_ready(), vec![a, b, c]);
        let mut deps = s.deps_of(d).to_vec();
        deps.sort_unstable();
        assert_eq!(deps, vec![a, b, c]);
        // Complete out of order; d becomes ready only after the last.
        s.complete(b, StepOutcome::Done(json!("b"))).expect("b");
        s.complete(c, StepOutcome::Done(json!("c"))).expect("c");
        let p = s.complete(a, StepOutcome::Done(json!("a"))).expect("a");
        assert_eq!(p.newly_ready, vec![d]);
    }

    #[test]
    fn t11_no_stall_state_reachable_in_mixed_run() {
        // Fuzz-ish: a 10-step layered graph completed in varying orders
        // with one failure never reaches "waiting but nothing in
        // flight and not settled".
        let v = ids(10);
        let mut edges = Vec::new();
        for layer in 1..5 {
            for i in 0..2 {
                let child = v[layer * 2 + i];
                edges.push((child, v[(layer - 1) * 2]));
                edges.push((child, v[(layer - 1) * 2 + 1]));
            }
        }
        let mut s = DagScheduler::new(&plan_of(&v, edges)).expect("new");
        let mut in_flight = s.take_initial_ready();
        let mut step_n = 0usize;
        while !s.is_settled() {
            assert!(
                !in_flight.is_empty(),
                "stall: not settled but nothing in flight"
            );
            let step = in_flight.remove(step_n % in_flight.len());
            step_n += 1;
            let outcome = if step_n == 4 {
                StepOutcome::Failed("boom".into())
            } else {
                StepOutcome::Done(json!(step_n))
            };
            let p = s.complete(step, outcome).expect("complete");
            in_flight.extend(p.newly_ready);
        }
        let sum = s.summary();
        assert_eq!(
            sum.done + sum.failed + sum.timed_out + sum.skipped,
            sum.total
        );
        assert_eq!(sum.failed, 1);
    }
}
