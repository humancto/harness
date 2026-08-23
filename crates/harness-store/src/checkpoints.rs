//! 5.11 (ADR-0039): checkpoint rows for `plan.execute` DAG steps.
//!
//! One row per `(plan_id, node_id)` — the output that plan node
//! produced, plus the hash of the capability + resolved input it
//! produced it FOR.
//! On resume the plan loop recomputes each ready node's input hash and
//! settles the step from this table when the hash still matches,
//! instead of dispatching it.
//!
//! Keyed on the NODE, validated by the hash (plan review BLOCKER-1):
//! hashing the input alone would collapse two legitimately distinct
//! steps that share an input, and collide across capabilities.
//!
//! LOCAL DERIVED DATA: never gossiped (the replica stream carries
//! `ReplicatedTaskState` only). Rows are dropped when their plan
//! completes fully — a checkpoint survives interruption, it is not a
//! result cache.

use harness_core::{PlanId, TaskId};
use rusqlite::{params, OptionalExtension};
use serde_json::Value as JsonValue;

use crate::error::StoreError;
use crate::open::Store;

/// Serialized-output ceiling for one checkpoint row (256 KiB). A step
/// whose output exceeds it is simply not checkpointed — it re-runs on
/// resume. `plan.execute` caps a plan at 64 steps, so the worst case
/// stays bounded (`MAX_PLAN_STEPS` × this).
pub const MAX_CHECKPOINT_OUTPUT_BYTES: usize = 256 * 1024;

impl Store {
    /// Record a plan node's output plus the hash of the resolved input
    /// it ran on. Upsert per `(plan, node)`: a re-planned node with a
    /// different input overwrites the stale row.
    ///
    /// Returns `false` (writing nothing) when the serialized output
    /// exceeds [`MAX_CHECKPOINT_OUTPUT_BYTES`] — the step re-runs on
    /// resume rather than turning this table into a blob store.
    ///
    /// # Errors
    /// Underlying sqlite errors, or a non-serializable output.
    pub fn checkpoint_put(
        &self,
        plan_id: PlanId,
        node_id: TaskId,
        input_hash: &[u8; 32],
        task_id: TaskId,
        output: &JsonValue,
        now_ms: u64,
    ) -> Result<bool, StoreError> {
        let encoded = serde_json::to_string(output)
            .map_err(|e| StoreError::Cbor(format!("encode checkpoint output: {e}")))?;
        if encoded.len() > MAX_CHECKPOINT_OUTPUT_BYTES {
            return Ok(false);
        }
        self.with_conn(|c| {
            c.execute(
                "INSERT INTO checkpoints(plan_id, node_id, input_hash, task_id, output, created_at)
                      VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(plan_id, node_id) DO UPDATE SET
                      input_hash = excluded.input_hash,
                      task_id    = excluded.task_id,
                      output     = excluded.output,
                      created_at = excluded.created_at",
                params![
                    plan_id.0.as_bytes(),
                    node_id.0.as_bytes(),
                    &input_hash[..],
                    task_id.0.as_bytes(),
                    encoded,
                    i64::try_from(now_ms).unwrap_or(i64::MAX),
                ],
            )?;
            Ok(true)
        })
    }

    /// The output recorded for this plan node, but ONLY if it ran on
    /// the same resolved input (`input_hash` match). A node whose
    /// input changed — or whose stored JSON no longer parses — reads
    /// as a miss and re-runs; a corrupt checkpoint must never wedge a
    /// plan or replay a stale answer.
    ///
    /// # Errors
    /// Underlying sqlite errors.
    pub fn checkpoint_get(
        &self,
        plan_id: PlanId,
        node_id: TaskId,
        input_hash: &[u8; 32],
    ) -> Result<Option<JsonValue>, StoreError> {
        self.with_conn(|c| {
            let row: Option<(Vec<u8>, String)> = c
                .query_row(
                    "SELECT input_hash, output FROM checkpoints
                      WHERE plan_id = ?1 AND node_id = ?2",
                    params![plan_id.0.as_bytes(), node_id.0.as_bytes()],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;
            let Some((stored_hash, text)) = row else {
                return Ok(None);
            };
            if stored_hash.as_slice() != &input_hash[..] {
                return Ok(None);
            }
            Ok(match serde_json::from_str(&text) {
                Ok(v) => Some(v),
                Err(e) => {
                    tracing::warn!(
                        target: "harness.store.checkpoints",
                        %e,
                        "unparseable checkpoint output; treating as a miss"
                    );
                    None
                }
            })
        })
    }

    /// How many checkpoints this plan holds (tests + observability).
    ///
    /// # Errors
    /// Underlying sqlite errors.
    pub fn checkpoint_count(&self, plan_id: PlanId) -> Result<usize, StoreError> {
        self.with_conn(|c| {
            let n: i64 = c.query_row(
                "SELECT count(*) FROM checkpoints WHERE plan_id = ?1",
                params![plan_id.0.as_bytes()],
                |r| r.get(0),
            )?;
            Ok(usize::try_from(n).unwrap_or(0))
        })
    }

    /// Drop every checkpoint for a plan — called when the plan
    /// completes fully. Returns the number of rows deleted.
    ///
    /// # Errors
    /// Underlying sqlite errors.
    pub fn checkpoint_delete_plan(&self, plan_id: PlanId) -> Result<usize, StoreError> {
        self.with_conn(|c| {
            let n = c.execute(
                "DELETE FROM checkpoints WHERE plan_id = ?1",
                params![plan_id.0.as_bytes()],
            )?;
            Ok(n)
        })
    }

    /// Drop checkpoints for every plan that is **durably and fully
    /// finished**: its own `plan.execute` row is `done`, its result is
    /// persisted, that result's aggregate reports every step done, and
    /// no run of the same plan is currently in flight.
    ///
    /// All four conditions are load-bearing:
    ///
    /// - *durable* (Codex P1 on #62) — deleting inside the plan driver
    ///   opens a crash window where the rows are gone but the plan is
    ///   not recorded done, so a resubmission re-runs every step.
    /// - *fully finished* (diff review B1) — `drive_plan` returns `Ok`
    ///   (and the executor writes `done` + a result) for partial plans
    ///   too: a continue-mode run with a failed step, or a budget
    ///   pause parking half the graph. Those are exactly the plans an
    ///   operator resubmits, so their checkpoints must survive.
    /// - *not in flight* (diff review M1) — a plan id's earlier
    ///   terminal row satisfies the first two conditions forever, so a
    ///   resubmission of the same plan would have the checkpoints it
    ///   is writing swept out from under it mid-run.
    ///
    /// Joins on the `tasks.plan_id` stamp added in 5.10. Returns the
    /// number of rows deleted.
    ///
    /// # Errors
    /// Underlying sqlite errors.
    pub fn checkpoint_sweep_completed_plans(&self) -> Result<usize, StoreError> {
        let finished = self.completed_plans_with_checkpoints()?;
        let mut deleted = 0;
        for plan_id in finished {
            deleted += self.checkpoint_delete_plan(plan_id)?;
        }
        Ok(deleted)
    }

    /// The plan ids from [`Self::checkpoint_sweep_completed_plans`]'s
    /// predicate. Split out so the aggregate check is plain Rust
    /// rather than SQL `json_extract`.
    fn completed_plans_with_checkpoints(&self) -> Result<Vec<PlanId>, StoreError> {
        self.with_conn(|c| {
            let mut stmt = c.prepare(
                "SELECT DISTINCT cp.plan_id, r.output
                   FROM checkpoints cp
                   JOIN tasks t
                     ON t.plan_id = cp.plan_id
                    AND t.capability = 'plan.execute'
                    AND t.state = 'done'
                   JOIN task_results r ON r.task_id = t.id
                  WHERE NOT EXISTS (
                     SELECT 1 FROM tasks live
                      WHERE live.plan_id = cp.plan_id
                        AND live.capability = 'plan.execute'
                        AND live.state NOT IN ('done', 'failed', 'cancelled', 'expired')
                  )",
            )?;
            let rows = stmt
                .query_map([], |r| {
                    Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, Option<String>>(1)?))
                })?
                .collect::<Result<Vec<_>, _>>()?;
            let mut out = Vec::new();
            for (raw_id, output) in rows {
                let Some(text) = output else { continue };
                let Ok(aggregate) = serde_json::from_str::<JsonValue>(&text) else {
                    continue;
                };
                if !aggregate_is_complete(&aggregate) {
                    continue;
                }
                if let Ok(bytes) = <[u8; 16]>::try_from(raw_id.as_slice()) {
                    out.push(PlanId(uuid::Uuid::from_bytes(bytes)));
                }
            }
            Ok(out)
        })
    }

    /// Boot sweep (plan review minor): a plan that crashed and is never
    /// resumed keeps its checkpoints forever. Drop rows older than
    /// `cutoff_ms`. Returns the number deleted.
    ///
    /// # Errors
    /// Underlying sqlite errors.
    pub fn checkpoint_sweep_older_than(&self, cutoff_ms: u64) -> Result<usize, StoreError> {
        self.with_conn(|c| {
            let n = c.execute(
                "DELETE FROM checkpoints WHERE created_at < ?1",
                params![i64::try_from(cutoff_ms).unwrap_or(i64::MAX)],
            )?;
            Ok(n)
        })
    }
}

/// A `plan.execute` aggregate describes a plan with nothing left to
/// do: it ran to `done` with no failed, timed-out or skipped step.
/// Anything else — a continue-mode failure, a budget pause, a
/// cancel — is a plan someone may resubmit (diff review B1). A result
/// whose shape we cannot read is treated as INCOMPLETE: keeping
/// checkpoints costs rows, dropping them costs re-executed side
/// effects.
pub fn aggregate_is_complete(aggregate: &JsonValue) -> bool {
    let zero = |key: &str| aggregate.get(key).and_then(JsonValue::as_u64) == Some(0);
    aggregate.get("status").and_then(JsonValue::as_str) == Some("done")
        && aggregate.get("ok").and_then(JsonValue::as_u64).is_some()
        && zero("failed")
        && zero("timed_out")
        && zero("skipped")
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use harness_core::HashFn;

    fn plan() -> PlanId {
        PlanId(uuid::Uuid::now_v7())
    }

    fn hash_of(v: &JsonValue) -> [u8; 32] {
        harness_core::step_hash("echo", v, HashFn::Blake3).expect("hash")
    }

    #[test]
    fn c01_put_get_round_trip_and_upsert() {
        let s = Store::open_memory().expect("store");
        let (p, node) = (plan(), TaskId::new_v7());
        let h = hash_of(&serde_json::json!({"a": 1}));

        assert!(s.checkpoint_get(p, node, &h).expect("get").is_none());
        assert!(s
            .checkpoint_put(
                p,
                node,
                &h,
                TaskId::new_v7(),
                &serde_json::json!({"out": 1}),
                1
            )
            .expect("put"));
        assert_eq!(
            s.checkpoint_get(p, node, &h).expect("get"),
            Some(serde_json::json!({"out": 1}))
        );
        // Upsert on (plan, node): newer output wins, still one row.
        assert!(s
            .checkpoint_put(
                p,
                node,
                &h,
                TaskId::new_v7(),
                &serde_json::json!({"out": 2}),
                2
            )
            .expect("put"));
        assert_eq!(
            s.checkpoint_get(p, node, &h).expect("get"),
            Some(serde_json::json!({"out": 2}))
        );
        assert_eq!(s.checkpoint_count(p).expect("count"), 1);
    }

    #[test]
    fn c02_identical_inputs_in_one_plan_do_not_collapse() {
        // Plan review BLOCKER-1: two distinct nodes with the SAME
        // resolved input (a notify.send at the start and the end of a
        // plan) must each run — an input-only key would skip the
        // second and report it ok.
        let s = Store::open_memory().expect("store");
        let p = plan();
        let (first, second) = (TaskId::new_v7(), TaskId::new_v7());
        let h = hash_of(&serde_json::json!({"msg": "deploy finished"}));

        s.checkpoint_put(
            p,
            first,
            &h,
            TaskId::new_v7(),
            &serde_json::json!("sent"),
            1,
        )
        .expect("put");
        assert!(
            s.checkpoint_get(p, second, &h).expect("get").is_none(),
            "the other node's row must not settle this step"
        );
        assert!(s.checkpoint_get(p, first, &h).expect("get").is_some());
    }

    #[test]
    fn c03_a_changed_input_invalidates_the_row() {
        let s = Store::open_memory().expect("store");
        let (p, node) = (plan(), TaskId::new_v7());
        let old = hash_of(&serde_json::json!({"path": "/a"}));
        let new = hash_of(&serde_json::json!({"path": "/b"}));
        s.checkpoint_put(p, node, &old, TaskId::new_v7(), &serde_json::json!("a"), 1)
            .expect("put");
        assert!(
            s.checkpoint_get(p, node, &new).expect("get").is_none(),
            "a re-planned node with a different input re-runs"
        );
        assert!(s.checkpoint_get(p, node, &old).expect("get").is_some());
    }

    #[test]
    fn c04_oversized_output_is_refused_not_stored() {
        let s = Store::open_memory().expect("store");
        let (p, node) = (plan(), TaskId::new_v7());
        let h = hash_of(&serde_json::json!({"big": true}));
        let big = serde_json::json!({ "blob": "x".repeat(MAX_CHECKPOINT_OUTPUT_BYTES + 1) });
        assert!(
            !s.checkpoint_put(p, node, &h, TaskId::new_v7(), &big, 1)
                .expect("put"),
            "oversized output refused"
        );
        assert!(
            s.checkpoint_get(p, node, &h).expect("get").is_none(),
            "nothing stored — the step re-runs on resume"
        );
    }

    #[test]
    fn c05_plans_are_isolated_and_gc_is_per_plan() {
        let s = Store::open_memory().expect("store");
        let (p1, p2) = (plan(), plan());
        let node = TaskId::new_v7();
        let h = hash_of(&serde_json::json!({"same": 1}));
        s.checkpoint_put(p1, node, &h, TaskId::new_v7(), &serde_json::json!("one"), 1)
            .expect("put");
        s.checkpoint_put(p2, node, &h, TaskId::new_v7(), &serde_json::json!("two"), 1)
            .expect("put");
        assert_eq!(
            s.checkpoint_get(p1, node, &h).expect("get"),
            Some(serde_json::json!("one")),
            "same node id in another plan is a different checkpoint"
        );

        assert_eq!(s.checkpoint_delete_plan(p1).expect("gc"), 1);
        assert!(s.checkpoint_get(p1, node, &h).expect("get").is_none());
        assert_eq!(
            s.checkpoint_get(p2, node, &h).expect("get"),
            Some(serde_json::json!("two")),
            "the other plan's checkpoints survive"
        );
    }

    #[test]
    fn c06_corrupt_row_reads_as_a_miss() {
        let s = Store::open_memory().expect("store");
        let (p, node) = (plan(), TaskId::new_v7());
        let h = hash_of(&serde_json::json!(1));
        s.with_conn(|c| {
            c.execute(
                "INSERT INTO checkpoints(plan_id, node_id, input_hash, task_id, output, created_at)
                      VALUES (?1, ?2, ?3, ?4, '{not json', 1)",
                params![
                    p.0.as_bytes(),
                    node.0.as_bytes(),
                    &h[..],
                    TaskId::new_v7().0.as_bytes()
                ],
            )?;
            Ok(())
        })
        .expect("seed");
        assert!(
            s.checkpoint_get(p, node, &h).expect("get").is_none(),
            "a corrupt checkpoint must not wedge the plan"
        );
    }

    /// Insert a `plan.execute` row for `plan`, walk it to `state`,
    /// and optionally persist `aggregate` as its result.
    fn seed_plan_row(
        s: &Store,
        plan: PlanId,
        state: crate::TaskState,
        aggregate: Option<&JsonValue>,
    ) -> TaskId {
        use harness_core::{Identity, Signable};
        let me = Identity::generate();
        let mut task = harness_core::Task {
            id: TaskId::new_v7(),
            parent: None,
            plan_id: Some(plan),
            capability: "plan.execute".into(),
            input: serde_json::json!({}),
            constraints: harness_core::Constraints::default(),
            retry: harness_core::RetryPolicy::default(),
            execution: harness_core::ExecutionPolicy::default(),
            resource_hints: harness_core::ResourceHints {
                cpu_class: harness_core::protocol::CpuClass::Light,
                memory_mb: None,
                gpu_required: false,
                gpu_memory_mb: None,
                network_class: harness_core::protocol::NetworkClass::None,
                disk_io_class: harness_core::protocol::DiskIoClass::None,
                estimated_duration_ms: None,
            },
            trace_ctx: harness_core::TraceContext::default(),
            issued_by: me.node_id(),
            issued_at: 1,
            tags: vec![],
            sig: harness_core::Signature::from_bytes([0u8; 64]),
        };
        task.sign(&me).expect("sign");
        s.insert_task(&task).expect("insert");
        for next in [
            crate::TaskState::Dispatched,
            crate::TaskState::Claimed,
            crate::TaskState::Running,
            crate::TaskState::Done,
        ] {
            if next as u8 > state as u8 {
                break;
            }
            s.transition_task(task.id, next).expect("hop");
            if next == state {
                break;
            }
        }
        if let Some(agg) = aggregate {
            s.write_task_result_done(task.id, agg, 2, me.node_id())
                .expect("result");
        }
        task.id
    }

    fn complete_aggregate() -> JsonValue {
        serde_json::json!({
            "status": "done", "ok": 2, "failed": 0, "timed_out": 0, "skipped": 0
        })
    }

    fn seed_checkpoint(s: &Store, plan: PlanId) -> ([u8; 32], TaskId) {
        let node = TaskId::new_v7();
        let h = hash_of(&serde_json::json!({}));
        s.checkpoint_put(
            plan,
            node,
            &h,
            TaskId::new_v7(),
            &serde_json::json!("out"),
            1,
        )
        .expect("put");
        (h, node)
    }

    #[test]
    fn c07b_completed_plan_sweep_needs_a_durable_result() {
        // Codex P1 on #62: checkpoints outlive the driver and are
        // dropped only once the plan's OWN row is done AND its result
        // is persisted — a crash between those two points must still
        // find the checkpoints there.
        let s = Store::open_memory().expect("store");
        let p = plan();
        let (h, node) = seed_checkpoint(&s, p);

        seed_plan_row(&s, p, crate::TaskState::Done, None);
        assert_eq!(
            s.checkpoint_sweep_completed_plans().expect("sweep"),
            0,
            "no result row yet — the checkpoints must survive"
        );
        assert!(s.checkpoint_get(p, node, &h).expect("get").is_some());

        // Result persisted and complete: now the plan is finished.
        let task = seed_plan_row(&s, p, crate::TaskState::Done, Some(&complete_aggregate()));
        assert!(s.load_task_result(task).expect("load").is_some());
        assert_eq!(s.checkpoint_sweep_completed_plans().expect("sweep"), 1);
        assert!(s.checkpoint_get(p, node, &h).expect("get").is_none());
    }

    #[test]
    fn c07c_partial_plans_keep_their_checkpoints() {
        // Diff review B1: `drive_plan` returns Ok — and the executor
        // writes `done` + a result — for plans that did NOT finish: a
        // continue-mode run with a failed step, and a budget pause
        // parking half the graph. Those are exactly the plans an
        // operator resubmits, so a durable-but-partial result must
        // NOT sweep their checkpoints.
        for aggregate in [
            serde_json::json!({"status": "done", "ok": 3, "failed": 1, "timed_out": 0, "skipped": 1}),
            serde_json::json!({"status": "paused_budget", "ok": 2, "failed": 0, "timed_out": 0, "skipped": 3}),
            serde_json::json!({"status": "done", "ok": 1, "failed": 0, "timed_out": 1, "skipped": 0}),
            // An aggregate we cannot read is treated as incomplete.
            serde_json::json!({"unexpected": "shape"}),
        ] {
            let s = Store::open_memory().expect("store");
            let p = plan();
            let (h, node) = seed_checkpoint(&s, p);
            seed_plan_row(&s, p, crate::TaskState::Done, Some(&aggregate));

            assert_eq!(
                s.checkpoint_sweep_completed_plans().expect("sweep"),
                0,
                "partial plan swept: {aggregate}"
            );
            assert!(
                s.checkpoint_get(p, node, &h).expect("get").is_some(),
                "checkpoints must survive for {aggregate}"
            );
        }
    }

    #[test]
    fn c07d_an_in_flight_rerun_is_never_swept() {
        // Diff review M1: an earlier terminal row for a plan id
        // satisfies the durability predicate forever. A resubmission
        // writing fresh checkpoints must not have them deleted mid-run
        // by the periodic sweeper.
        let s = Store::open_memory().expect("store");
        let p = plan();
        seed_plan_row(&s, p, crate::TaskState::Done, Some(&complete_aggregate()));
        // Run 2 of the same plan is in flight, and has recorded a step.
        seed_plan_row(&s, p, crate::TaskState::Running, None);
        let (h, node) = seed_checkpoint(&s, p);

        assert_eq!(
            s.checkpoint_sweep_completed_plans().expect("sweep"),
            0,
            "a live run's checkpoints are not the old run's leftovers"
        );
        assert!(s.checkpoint_get(p, node, &h).expect("get").is_some());
    }

    #[test]
    fn c07_age_sweep_drops_only_stale_rows() {
        let s = Store::open_memory().expect("store");
        let p = plan();
        let h = hash_of(&serde_json::json!(1));
        let (old_node, fresh_node) = (TaskId::new_v7(), TaskId::new_v7());
        s.checkpoint_put(
            p,
            old_node,
            &h,
            TaskId::new_v7(),
            &serde_json::json!("old"),
            100,
        )
        .expect("put");
        s.checkpoint_put(
            p,
            fresh_node,
            &h,
            TaskId::new_v7(),
            &serde_json::json!("new"),
            900,
        )
        .expect("put");

        assert_eq!(s.checkpoint_sweep_older_than(500).expect("sweep"), 1);
        assert!(s.checkpoint_get(p, old_node, &h).expect("get").is_none());
        assert!(s.checkpoint_get(p, fresh_node, &h).expect("get").is_some());
    }
}
