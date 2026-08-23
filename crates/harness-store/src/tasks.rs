//! Task CRUD — store + load + state-transition the §13.3 envelopes.

use std::str::FromStr;

use harness_core::{NodeId, Signable, Signature, Task, TaskId};
use rusqlite::{params, OptionalExtension};
use serde::{Deserialize, Serialize};

use crate::error::StoreError;
use crate::open::Store;

/// Task lifecycle states (PRD §14.1). Stored as a TEXT column with a
/// CHECK constraint so the DB itself rejects an out-of-band write.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    Submitted,
    Planned,
    Dispatched,
    Claimed,
    Running,
    Done,
    Failed,
    Expired,
    Cancelled,
}

impl TaskState {
    pub fn as_str(&self) -> &'static str {
        match self {
            TaskState::Submitted => "submitted",
            TaskState::Planned => "planned",
            TaskState::Dispatched => "dispatched",
            TaskState::Claimed => "claimed",
            TaskState::Running => "running",
            TaskState::Done => "done",
            TaskState::Failed => "failed",
            TaskState::Expired => "expired",
            TaskState::Cancelled => "cancelled",
        }
    }

    /// Lifecycle transitions per PRD §14.1. Centralized so every callsite
    /// agrees on what's legal.
    ///
    /// `Submitted → Failed` is a documented supervisor hop added by
    /// 3.3-fanout (ADR-0017): a task that stays undispatchable past its
    /// deadline window (no eligible node, pinned node never live) is
    /// failed terminally by the `DispatchService` without inventing a
    /// fake `Dispatched → Claimed → Failed` history.
    #[must_use]
    pub fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (
                TaskState::Submitted,
                TaskState::Planned
                    | TaskState::Dispatched
                    | TaskState::Cancelled
                    | TaskState::Failed
            ) | (
                TaskState::Planned,
                TaskState::Dispatched | TaskState::Cancelled
            ) | (
                TaskState::Dispatched,
                TaskState::Claimed | TaskState::Cancelled | TaskState::Expired
            ) | (
                TaskState::Claimed,
                TaskState::Running
                    | TaskState::Failed
                    | TaskState::Expired
                    // 5.10 (ADR-0038): operator cancel reaches claimed
                    // rows too — the stop button must not race-lose to
                    // a worker claim.
                    | TaskState::Cancelled
            ) | (
                TaskState::Running,
                TaskState::Done | TaskState::Failed | TaskState::Cancelled | TaskState::Expired
            )
        )
    }
}

impl FromStr for TaskState {
    type Err = StoreError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "submitted" => TaskState::Submitted,
            "planned" => TaskState::Planned,
            "dispatched" => TaskState::Dispatched,
            "claimed" => TaskState::Claimed,
            "running" => TaskState::Running,
            "done" => TaskState::Done,
            "failed" => TaskState::Failed,
            "expired" => TaskState::Expired,
            "cancelled" => TaskState::Cancelled,
            other => {
                return Err(StoreError::InvalidTransition {
                    from: other.into(),
                    to: "<invalid>".into(),
                })
            }
        })
    }
}

/// Step rows of one plan: `(row id, capability, state, parent id)`.
pub type PlanStepRows = Vec<(TaskId, String, TaskState, Option<TaskId>)>;

/// One row of the `tasks` table, suitable for read-back without
/// reconstructing the full `Task` envelope (see [`Store::load_task`] for
/// that). Surface for CLI / UI listings.
#[derive(Debug, Clone)]
pub struct TaskRow {
    pub id: TaskId,
    pub capability: String,
    pub state: TaskState,
    pub issued_by: NodeId,
    pub issued_at: u64,
    pub completed_by: Option<NodeId>,
    pub started_at: Option<u64>,
    pub finished_at: Option<u64>,
    /// 4.8: sub-task linkage for UI grouping (federated/plan children).
    pub parent: Option<TaskId>,
    /// 4.8: plan-run linkage (`plan.execute` step rows).
    pub plan_id: Option<harness_core::PlanId>,
}

impl Store {
    /// Insert a new `Task` in `submitted` state. The full envelope is
    /// canonical-encoded (sig zeroed) and the detached signature is
    /// stored alongside, mirroring the 1.8 trust-store pattern.
    ///
    /// # Errors
    /// `StoreError::Sqlite` for primary-key collision (re-submit) or
    /// constraint violation; `StoreError::Cbor` if encoding fails.
    pub fn insert_task(&self, task: &Task) -> Result<(), StoreError> {
        let bytes = task
            .canonical_bytes()
            .map_err(|e| StoreError::Cbor(e.to_string()))?;
        let sig = task.sig_field().to_bytes();
        self.with_conn(|c| {
            c.execute(
                "INSERT INTO tasks (
                    id, parent_id, plan_id, capability, state,
                    issued_by, issued_at, canonical_cbor, signature
                ) VALUES (?, ?, ?, ?, 'submitted', ?, ?, ?, ?)",
                params![
                    task.id.0.as_bytes(),
                    task.parent.map(|p| p.0.as_bytes().to_vec()),
                    task.plan_id.map(|p| p.0.as_bytes().to_vec()),
                    task.capability,
                    task.issued_by.as_bytes(),
                    i64::try_from(task.issued_at).unwrap_or(i64::MAX),
                    bytes,
                    sig.as_slice(),
                ],
            )?;
            Ok(())
        })
    }

    /// Reconstruct a `Task` from its stored canonical bytes + signature.
    /// Returns `Ok(None)` if the row is absent.
    pub fn load_task(&self, id: TaskId) -> Result<Option<Task>, StoreError> {
        self.with_conn(|c| {
            let row = c
                .query_row(
                    "SELECT canonical_cbor, signature FROM tasks WHERE id = ?",
                    params![id.0.as_bytes()],
                    |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, Vec<u8>>(1)?)),
                )
                .optional()?;
            let Some((cbor, sig_bytes)) = row else {
                return Ok(None);
            };
            let mut task: Task = ciborium::de::from_reader(cbor.as_slice())
                .map_err(|e| StoreError::Cbor(e.to_string()))?;
            let sig_arr: [u8; 64] = sig_bytes
                .as_slice()
                .try_into()
                .map_err(|_| StoreError::Cbor("signature must be 64 bytes".into()))?;
            *task.sig_field_mut() = Signature::from_bytes(sig_arr);
            Ok(Some(task))
        })
    }

    /// Read the lifecycle state of a task, if present.
    pub fn task_state(&self, id: TaskId) -> Result<Option<TaskState>, StoreError> {
        self.with_conn(|c| {
            let s: Option<String> = c
                .query_row(
                    "SELECT state FROM tasks WHERE id = ?",
                    params![id.0.as_bytes()],
                    |r| r.get(0),
                )
                .optional()?;
            s.map(|s| s.parse()).transpose()
        })
    }

    /// Validate a state transition against PRD §14.1 then persist it.
    ///
    /// # Errors
    /// `StoreError::NotFound` if the task does not exist;
    /// `StoreError::InvalidTransition` if the next state is illegal from
    /// the current.
    pub fn transition_task(&self, id: TaskId, next: TaskState) -> Result<(), StoreError> {
        let current = self.task_state(id)?.ok_or(StoreError::NotFound)?;
        if !current.can_transition_to(next) {
            return Err(StoreError::InvalidTransition {
                from: current.as_str().into(),
                to: next.as_str().into(),
            });
        }
        self.with_conn(|c| {
            c.execute(
                "UPDATE tasks SET state = ? WHERE id = ?",
                params![next.as_str(), id.0.as_bytes()],
            )?;
            Ok(())
        })
    }

    /// Compare-and-set state transition. Returns `Ok(true)` if the row
    /// was at `from` and is now at `to`; `Ok(false)` if it was at some
    /// other state or absent (caller lost the race).
    ///
    /// # Errors
    /// `StoreError::InvalidTransition` if `from → to` is not a legal
    /// hop per PRD §14.1 (programmer error — caught up-front before
    /// the SQL fires).
    pub fn try_transition_task(
        &self,
        id: TaskId,
        from: TaskState,
        to: TaskState,
    ) -> Result<bool, StoreError> {
        if !from.can_transition_to(to) {
            return Err(StoreError::InvalidTransition {
                from: from.as_str().into(),
                to: to.as_str().into(),
            });
        }
        self.with_conn(|c| {
            let n = c.execute(
                "UPDATE tasks SET state = ?1 WHERE id = ?2 AND state = ?3",
                params![to.as_str(), id.0.as_bytes(), from.as_str()],
            )?;
            Ok(n == 1)
        })
    }

    /// CAS `submitted → dispatched` AND record which node the task was
    /// routed to, in one UPDATE. Returns `Ok(true)` if this call won the
    /// race, `Ok(false)` if the row was in another state or absent.
    ///
    /// This is the 3.3-fanout "claim ownership" hop from ADR-0009: only
    /// the `DispatchService` performs it; the executor claims from
    /// `dispatched` onward.
    pub fn try_dispatch_task(&self, id: TaskId, node: NodeId) -> Result<bool, StoreError> {
        self.with_conn(|c| {
            let n = c.execute(
                "UPDATE tasks SET state = 'dispatched', assigned_node = ?1
                  WHERE id = ?2 AND state = 'submitted'",
                params![node.as_bytes(), id.0.as_bytes()],
            )?;
            Ok(n == 1)
        })
    }

    /// 4.5 (ADR-0027): CAS `submitted → running` AND assign the local
    /// node, in one UPDATE. The Federated coordinator claims the *parent*
    /// task with this before fanning sub-tasks out; the executor never
    /// sees the row because it only claims from `dispatched`.
    ///
    /// This is a deliberate synthetic hop — the parent skips `dispatched`
    /// because no single worker runs it; the coordinator on this node
    /// "runs" it by orchestrating sub-tasks. Returns `Ok(true)` if this
    /// call won the claim, `Ok(false)` if another coordinator (or a
    /// concurrent dispatch pass) got there first — the loser must release
    /// its coordinator slot and do nothing.
    pub fn try_start_coordination(&self, id: TaskId, node: NodeId) -> Result<bool, StoreError> {
        self.with_conn(|c| {
            let n = c.execute(
                "UPDATE tasks SET state = 'running', assigned_node = ?1
                  WHERE id = ?2 AND state = 'submitted'",
                params![node.as_bytes(), id.0.as_bytes()],
            )?;
            Ok(n == 1)
        })
    }

    /// 4.6 (ADR-0028): boot orphan sweep, remote-issued arm — reset a
    /// crash-stranded `claimed|running` row back to
    /// `dispatched(assigned=node)` so the executor re-executes it after
    /// a restart (at-least-once, the re-dispatch doctrine). A synthetic
    /// hop (running → dispatched is not a ladder transition), precedent
    /// ADR-0017/0027; guarded on the row still being assigned to
    /// `node`. Call ONLY from the boot sweep, before any loops spawn.
    pub fn sweep_orphan_to_dispatched(&self, id: TaskId, node: NodeId) -> Result<bool, StoreError> {
        self.with_conn(|c| {
            let n = c.execute(
                "UPDATE tasks SET state = 'dispatched'
                  WHERE id = ?1 AND state IN ('claimed', 'running')
                    AND assigned_node = ?2",
                params![id.0.as_bytes(), node.as_bytes()],
            )?;
            Ok(n == 1)
        })
    }

    /// 4.6 (ADR-0028): boot orphan sweep, locally-issued arm — fail a
    /// crash-stranded `claimed|running` row terminally (there is no
    /// lease and no coordinator to recover it). Guarded like
    /// [`Store::sweep_orphan_to_dispatched`].
    pub fn sweep_orphan_to_failed(&self, id: TaskId, node: NodeId) -> Result<bool, StoreError> {
        self.with_conn(|c| {
            let n = c.execute(
                "UPDATE tasks SET state = 'failed'
                  WHERE id = ?1 AND state IN ('claimed', 'running')
                    AND assigned_node = ?2",
                params![id.0.as_bytes(), node.as_bytes()],
            )?;
            Ok(n == 1)
        })
    }

    /// Worker-side ingest of a remote assignment: insert the task with the
    /// row born at `dispatched`, assigned to `assigned_node` (the local
    /// node). Idempotent — a re-delivered assignment after a reconnect is
    /// a no-op (`Ok(false)`); the `dispatched → claimed` CAS already
    /// guarantees single execution.
    pub fn insert_task_dispatched(
        &self,
        task: &Task,
        assigned_node: NodeId,
    ) -> Result<bool, StoreError> {
        let bytes = task
            .canonical_bytes()
            .map_err(|e| StoreError::Cbor(e.to_string()))?;
        let sig = task.sig_field().to_bytes();
        self.with_conn(|c| {
            let n = c.execute(
                "INSERT OR IGNORE INTO tasks (
                    id, parent_id, plan_id, capability, state,
                    issued_by, issued_at, canonical_cbor, signature,
                    assigned_node
                ) VALUES (?, ?, ?, ?, 'dispatched', ?, ?, ?, ?, ?)",
                params![
                    task.id.0.as_bytes(),
                    task.parent.map(|p| p.0.as_bytes().to_vec()),
                    task.plan_id.map(|p| p.0.as_bytes().to_vec()),
                    task.capability,
                    task.issued_by.as_bytes(),
                    i64::try_from(task.issued_at).unwrap_or(i64::MAX),
                    bytes,
                    sig.as_slice(),
                    assigned_node.as_bytes(),
                ],
            )?;
            Ok(n == 1)
        })
    }

    /// Which node the task is currently assigned to (`None` while
    /// `submitted` or for pre-3.3 rows).
    pub fn assigned_node(&self, id: TaskId) -> Result<Option<NodeId>, StoreError> {
        self.with_conn(|c| {
            let blob: Option<Option<Vec<u8>>> = c
                .query_row(
                    "SELECT assigned_node FROM tasks WHERE id = ?",
                    params![id.0.as_bytes()],
                    |r| r.get(0),
                )
                .optional()?;
            blob.flatten()
                .map(|b| {
                    <[u8; 16]>::try_from(b.as_slice())
                        .map(NodeId::from_bytes)
                        .map_err(|_| StoreError::Cbor("assigned_node must be 16 bytes".into()))
                })
                .transpose()
        })
    }

    /// List tasks in `state` assigned to `assigned` (or with no
    /// assignment when `assigned` is `None`), oldest first — dispatch
    /// and execution loops drain FIFO.
    pub fn list_tasks_by_state_assigned(
        &self,
        state: TaskState,
        assigned: Option<NodeId>,
    ) -> Result<Vec<TaskRow>, StoreError> {
        self.with_conn(|c| {
            let mut stmt = c.prepare(
                "SELECT id, capability, state, issued_by, issued_at,
                        completed_by, started_at, finished_at,
                        parent_id, plan_id
                   FROM tasks
                  WHERE state = ?1
                    AND ((?2 IS NULL AND assigned_node IS NULL)
                         OR assigned_node = ?2)
               ORDER BY issued_at ASC",
            )?;
            let rows = stmt
                .query_map(
                    params![state.as_str(), assigned.map(|n| n.as_bytes().to_vec())],
                    |r| Ok(row_to_task_row(r)),
                )?
                .collect::<Result<Vec<_>, _>>()?;
            rows.into_iter().collect::<Result<Vec<_>, _>>()
        })
    }

    /// List tasks in a given state, newest first by `issued_at` (Uuid v7
    /// is also sortable but we explicitly order by the indexed column).
    /// In-flight (Dispatched|Claimed|Running) task counts grouped by
    /// `assigned_node` — the issuer-side load signal for 4.4 scoring.
    pub fn count_inflight_by_node(
        &self,
    ) -> Result<std::collections::HashMap<NodeId, u32>, StoreError> {
        self.with_conn(|c| {
            let mut stmt = c.prepare(
                "SELECT assigned_node, COUNT(*)
                   FROM tasks
                  WHERE state IN ('dispatched','claimed','running')
                    AND assigned_node IS NOT NULL
               GROUP BY assigned_node",
            )?;
            let rows = stmt
                .query_map([], |r| {
                    let blob: Vec<u8> = r.get(0)?;
                    let n: i64 = r.get(1)?;
                    Ok((blob, n))
                })?
                .collect::<Result<Vec<_>, _>>()?;
            let mut out = std::collections::HashMap::with_capacity(rows.len());
            for (blob, n) in rows {
                let arr: [u8; 16] = blob
                    .as_slice()
                    .try_into()
                    .map_err(|_| StoreError::Cbor("assigned_node must be 16 bytes".into()))?;
                out.insert(
                    NodeId::from_bytes(arr),
                    u32::try_from(n).unwrap_or(u32::MAX),
                );
            }
            Ok(out)
        })
    }

    /// Step rows of one plan (4.3): `(row id, capability, state,
    /// parent id)` ordered by issue time. The 4.8 UI DAG view and the
    /// plan E2E tests map rows to plan nodes through the aggregate
    /// output's `steps[*].task_id`.
    pub fn list_tasks_by_plan(
        &self,
        plan_id: harness_core::PlanId,
    ) -> Result<PlanStepRows, StoreError> {
        self.with_conn(|c| {
            let mut stmt = c.prepare(
                "SELECT id, capability, state, parent_id
                   FROM tasks
                  WHERE plan_id = ?
               ORDER BY issued_at ASC, id ASC",
            )?;
            let rows = stmt
                .query_map(params![plan_id.0.as_bytes()], |r| {
                    let id_blob: Vec<u8> = r.get(0)?;
                    let capability: String = r.get(1)?;
                    let state: String = r.get(2)?;
                    let parent_blob: Option<Vec<u8>> = r.get(3)?;
                    Ok((id_blob, capability, state, parent_blob))
                })?
                .collect::<Result<Vec<_>, _>>()?;
            rows.into_iter()
                .map(|(id_blob, capability, state, parent_blob)| {
                    let id_arr: [u8; 16] = id_blob
                        .as_slice()
                        .try_into()
                        .map_err(|_| StoreError::Cbor("task id must be 16 bytes".into()))?;
                    let state = state
                        .parse::<TaskState>()
                        .map_err(|_| StoreError::Cbor(format!("bad state {state}")))?;
                    let parent = parent_blob
                        .map(|b| {
                            <[u8; 16]>::try_from(b.as_slice())
                                .map_err(|_| StoreError::Cbor("parent id must be 16 bytes".into()))
                        })
                        .transpose()?
                        .map(|arr| TaskId(uuid::Uuid::from_bytes(arr)));
                    Ok((
                        TaskId(uuid::Uuid::from_bytes(id_arr)),
                        capability,
                        state,
                        parent,
                    ))
                })
                .collect()
        })
    }

    /// Indexed `COUNT(*)` over `idx_tasks_by_state` — the 4.7
    /// (ADR-0029) submit-admission check, which must not pay
    /// `list_tasks_by_state`'s row materialization on every `POST`.
    pub fn count_tasks_by_state(&self, state: TaskState) -> Result<u64, StoreError> {
        self.with_conn(|c| {
            let n: i64 = c.query_row(
                "SELECT COUNT(*) FROM tasks WHERE state = ?",
                params![state.as_str()],
                |r| r.get(0),
            )?;
            Ok(u64::try_from(n).unwrap_or(0))
        })
    }

    /// Most-recent tasks across ALL states (4.8 runs listing), newest
    /// first over `idx_tasks_by_issued_at`.
    pub fn list_recent_tasks(&self, limit: usize) -> Result<Vec<TaskRow>, StoreError> {
        self.with_conn(|c| {
            let mut stmt = c.prepare(
                "SELECT id, capability, state, issued_by, issued_at,
                        completed_by, started_at, finished_at,
                        parent_id, plan_id
                   FROM tasks
               ORDER BY issued_at DESC
                  LIMIT ?",
            )?;
            let rows = stmt
                .query_map(params![i64::try_from(limit).unwrap_or(i64::MAX)], |r| {
                    Ok(row_to_task_row(r))
                })?
                .collect::<Result<Vec<_>, _>>()?;
            rows.into_iter().collect::<Result<Vec<_>, _>>()
        })
    }

    /// Like [`Self::list_tasks_by_state`] with a row cap — the 4.8
    /// API listing's `?state=` arm (one page, never a full-table dump;
    /// terminal states accumulate forever).
    pub fn list_tasks_by_state_limited(
        &self,
        state: TaskState,
        limit: usize,
    ) -> Result<Vec<TaskRow>, StoreError> {
        self.with_conn(|c| {
            let mut stmt = c.prepare(
                "SELECT id, capability, state, issued_by, issued_at,
                        completed_by, started_at, finished_at,
                        parent_id, plan_id
                   FROM tasks
                  WHERE state = ?1
               ORDER BY issued_at DESC
                  LIMIT ?2",
            )?;
            let rows = stmt
                .query_map(
                    params![state.as_str(), i64::try_from(limit).unwrap_or(i64::MAX)],
                    |r| Ok(row_to_task_row(r)),
                )?
                .collect::<Result<Vec<_>, _>>()?;
            rows.into_iter().collect::<Result<Vec<_>, _>>()
        })
    }

    pub fn list_tasks_by_state(&self, state: TaskState) -> Result<Vec<TaskRow>, StoreError> {
        self.with_conn(|c| {
            let mut stmt = c.prepare(
                "SELECT id, capability, state, issued_by, issued_at,
                        completed_by, started_at, finished_at,
                        parent_id, plan_id
                   FROM tasks
                  WHERE state = ?
               ORDER BY issued_at DESC",
            )?;
            let rows = stmt
                .query_map(params![state.as_str()], |r| Ok(row_to_task_row(r)))?
                .collect::<Result<Vec<_>, _>>()?;
            rows.into_iter().collect::<Result<Vec<_>, _>>()
        })
    }
}

fn row_to_task_row(r: &rusqlite::Row<'_>) -> Result<TaskRow, StoreError> {
    let id_blob: Vec<u8> = r.get(0)?;
    let id_arr: [u8; 16] = id_blob
        .as_slice()
        .try_into()
        .map_err(|_| StoreError::Cbor("task id must be 16 bytes".into()))?;
    let issued_blob: Vec<u8> = r.get(3)?;
    let issued_arr: [u8; 16] = issued_blob
        .as_slice()
        .try_into()
        .map_err(|_| StoreError::Cbor("issued_by must be 16 bytes".into()))?;
    let completed_by: Option<Vec<u8>> = r.get(5)?;
    let completed_by = completed_by
        .map(|b| {
            <[u8; 16]>::try_from(b.as_slice())
                .map(NodeId::from_bytes)
                .map_err(|_| StoreError::Cbor("completed_by must be 16 bytes".into()))
        })
        .transpose()?;
    let parent = r
        .get::<_, Option<Vec<u8>>>(8)?
        .map(|b| {
            <[u8; 16]>::try_from(b.as_slice())
                .map_err(|_| StoreError::Cbor("parent id must be 16 bytes".into()))
        })
        .transpose()?
        .map(|arr| TaskId(uuid::Uuid::from_bytes(arr)));
    let plan_id = r
        .get::<_, Option<Vec<u8>>>(9)?
        .map(|b| {
            <[u8; 16]>::try_from(b.as_slice())
                .map_err(|_| StoreError::Cbor("plan id must be 16 bytes".into()))
        })
        .transpose()?
        .map(|arr| harness_core::PlanId(uuid::Uuid::from_bytes(arr)));
    Ok(TaskRow {
        id: TaskId(uuid::Uuid::from_bytes(id_arr)),
        capability: r.get(1)?,
        state: r.get::<_, String>(2)?.parse()?,
        issued_by: NodeId::from_bytes(issued_arr),
        issued_at: u64::try_from(r.get::<_, i64>(4)?).unwrap_or(0),
        completed_by,
        parent,
        plan_id,
        started_at: r
            .get::<_, Option<i64>>(6)?
            .map(|v| u64::try_from(v).unwrap_or(0)),
        finished_at: r
            .get::<_, Option<i64>>(7)?
            .map(|v| u64::try_from(v).unwrap_or(0)),
    })
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use harness_core::{
        Constraints, ExecutionPolicy, Identity, ResourceHints, RetryPolicy, Signature, TraceContext,
    };

    fn empty_hints() -> ResourceHints {
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

    fn signed_task(id: &Identity) -> Task {
        let mut t = Task {
            id: TaskId::new_v7(),
            parent: None,
            plan_id: None,
            capability: "echo".into(),
            input: serde_json::json!({"msg": "hi"}),
            constraints: Constraints::default(),
            retry: RetryPolicy::default(),
            execution: ExecutionPolicy::default(),
            resource_hints: empty_hints(),
            trace_ctx: TraceContext::default(),
            issued_by: id.public_key().node_id(),
            issued_at: 1_700_000_000_000,
            tags: Vec::new(),
            sig: Signature::from_bytes([0u8; 64]),
        };
        t.sign(id).expect("sign");
        t
    }

    #[test]
    fn insert_then_load_round_trips() {
        let s = Store::open_memory().expect("open");
        let id = Identity::generate();
        let task = signed_task(&id);
        s.insert_task(&task).expect("insert");
        let loaded = s.load_task(task.id).expect("load").expect("present");
        assert_eq!(loaded, task);
        assert!(loaded.verify_signature(id.public_key()).is_ok());
    }

    /// 4.8: the runs listing — newest first across states, limit
    /// honored, and `parent`/`plan_id` round-trip for UI grouping.
    #[test]
    fn list_recent_tasks_orders_limits_and_links() {
        let s = Store::open_memory().expect("open");
        let id = Identity::generate();
        let mut older = signed_task(&id);
        older.issued_at = 1_700_000_000_000;
        older.sign(&id).expect("sign");
        s.insert_task(&older).expect("insert");
        s.transition_task(older.id, TaskState::Dispatched)
            .expect("hop");

        let plan_id = harness_core::PlanId(uuid::Uuid::now_v7());
        let mut child = signed_task(&id);
        child.issued_at = 1_700_000_000_500;
        child.parent = Some(older.id);
        child.plan_id = Some(plan_id);
        child.sign(&id).expect("sign");
        s.insert_task(&child).expect("insert");

        let rows = s.list_recent_tasks(50).expect("list");
        assert_eq!(rows.len(), 2, "all states listed");
        assert_eq!(rows[0].id, child.id, "newest first");
        assert_eq!(rows[0].parent, Some(older.id), "parent round-trips");
        assert_eq!(rows[0].plan_id, Some(plan_id), "plan_id round-trips");
        assert_eq!(rows[1].state, TaskState::Dispatched);
        assert_eq!(rows[1].parent, None);

        let limited = s.list_recent_tasks(1).expect("list");
        assert_eq!(limited.len(), 1);
        assert_eq!(limited[0].id, child.id);
    }

    #[test]
    fn task_state_starts_at_submitted() {
        let s = Store::open_memory().expect("open");
        let id = Identity::generate();
        let task = signed_task(&id);
        s.insert_task(&task).expect("insert");
        assert_eq!(s.task_state(task.id).unwrap(), Some(TaskState::Submitted));
    }

    #[test]
    fn transition_legal() {
        let s = Store::open_memory().expect("open");
        let id = Identity::generate();
        let task = signed_task(&id);
        s.insert_task(&task).expect("insert");
        s.transition_task(task.id, TaskState::Dispatched)
            .expect("dispatch");
        s.transition_task(task.id, TaskState::Claimed)
            .expect("claim");
        s.transition_task(task.id, TaskState::Running).expect("run");
        s.transition_task(task.id, TaskState::Done).expect("done");
        assert_eq!(s.task_state(task.id).unwrap(), Some(TaskState::Done));
    }

    #[test]
    fn try_start_coordination_claims_once_and_assigns_self() {
        let s = Store::open_memory().expect("open");
        let id = Identity::generate();
        let task = signed_task(&id);
        s.insert_task(&task).expect("insert");

        let me = id.public_key().node_id();
        assert!(s.try_start_coordination(task.id, me).expect("first claim"));
        assert_eq!(s.task_state(task.id).unwrap(), Some(TaskState::Running));
        assert_eq!(s.assigned_node(task.id).unwrap(), Some(me));

        // A second claim (another coordinator racing, or a dispatch pass)
        // must lose: the row is no longer `submitted`.
        let other = NodeId::from_bytes([9; 16]);
        assert!(!s.try_start_coordination(task.id, other).expect("second"));
        assert_eq!(s.assigned_node(task.id).unwrap(), Some(me));

        // And try_dispatch_task can no longer steal the row either.
        assert!(!s.try_dispatch_task(task.id, other).expect("dispatch"));
    }

    #[test]
    fn illegal_transition_is_rejected() {
        let s = Store::open_memory().expect("open");
        let id = Identity::generate();
        let task = signed_task(&id);
        s.insert_task(&task).expect("insert");
        // submitted -> running is illegal (must go through dispatched/claimed).
        let err = s.transition_task(task.id, TaskState::Running).unwrap_err();
        assert!(matches!(err, StoreError::InvalidTransition { .. }));
    }

    #[test]
    fn list_by_state_returns_inserted_tasks() {
        let s = Store::open_memory().expect("open");
        let id = Identity::generate();
        let t1 = signed_task(&id);
        let t2 = signed_task(&id);
        s.insert_task(&t1).expect("insert 1");
        s.insert_task(&t2).expect("insert 2");
        let rows = s.list_tasks_by_state(TaskState::Submitted).expect("list");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn load_missing_returns_none() {
        let s = Store::open_memory().expect("open");
        let missing = TaskId::new_v7();
        assert!(s.load_task(missing).expect("load").is_none());
    }

    #[test]
    fn transition_missing_errors_not_found() {
        let s = Store::open_memory().expect("open");
        let missing = TaskId::new_v7();
        let err = s.transition_task(missing, TaskState::Done).unwrap_err();
        assert!(matches!(err, StoreError::NotFound));
    }
}

/// 5.10 (ADR-0038): outcome of an operator cancel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelOutcome {
    /// The row was non-terminal and is now `Cancelled`.
    Cancelled,
    /// No such task.
    Unknown,
    /// Already terminal — nothing changed; the state is named.
    AlreadyTerminal(TaskState),
}

impl Store {
    /// Atomically cancel a NON-terminal task (5.10, ADR-0038): one
    /// transaction reads the state and flips it to `cancelled`, so a
    /// concurrent dispatch/claim cannot slip between read and write.
    /// Callers also release the task's live leases (the API handler
    /// does) so late worker results drop at the terminal-lease guard.
    ///
    /// # Errors
    /// Underlying sqlite errors.
    pub fn cancel_task(&self, id: TaskId) -> Result<CancelOutcome, StoreError> {
        self.with_conn(|c| {
            let tx = c.unchecked_transaction()?;
            let state: Option<String> = tx
                .query_row(
                    "SELECT state FROM tasks WHERE id = ?1",
                    params![id.0.as_bytes()],
                    |r| r.get(0),
                )
                .optional()?;
            let Some(raw) = state else {
                return Ok(CancelOutcome::Unknown);
            };
            let current: TaskState = raw.parse()?;
            if !current.can_transition_to(TaskState::Cancelled) {
                return Ok(CancelOutcome::AlreadyTerminal(current));
            }
            tx.execute(
                "UPDATE tasks SET state = 'cancelled' WHERE id = ?1",
                params![id.0.as_bytes()],
            )?;
            tx.commit()?;
            Ok(CancelOutcome::Cancelled)
        })
    }
}
