//! Leases — TTL-bounded claim records (PRD §14.5). Atomic state transitions
//! via WHERE-filtered UPDATE so concurrent claim attempts can't race.

use harness_core::{NodeId, TaskId};
use rusqlite::{params, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

use crate::error::StoreError;
use crate::open::Store;

// Since 3.3-fanout the id type lives in `harness-core` (it rides the
// `TaskAssign`/`TaskClaim`/`TaskResultMsg` wire envelopes); re-exported
// here so existing `harness_store::LeaseId` callers are unaffected.
pub use harness_core::LeaseId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum LeaseState {
    Pending,
    Claimed,
    Expired,
    Released,
    Completed,
}

impl LeaseState {
    pub fn as_str(&self) -> &'static str {
        match self {
            LeaseState::Pending => "pending",
            LeaseState::Claimed => "claimed",
            LeaseState::Expired => "expired",
            LeaseState::Released => "released",
            LeaseState::Completed => "completed",
        }
    }
}

impl FromStr for LeaseState {
    type Err = StoreError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "pending" => LeaseState::Pending,
            "claimed" => LeaseState::Claimed,
            "expired" => LeaseState::Expired,
            "released" => LeaseState::Released,
            "completed" => LeaseState::Completed,
            other => {
                return Err(StoreError::InvalidTransition {
                    from: other.into(),
                    to: "<invalid>".into(),
                })
            }
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lease {
    pub lease_id: LeaseId,
    pub task_id: TaskId,
    pub worker_id: Option<NodeId>,
    pub state: LeaseState,
    pub issued_at: u64,
    pub expires_at: u64,
    pub attempt: u32,
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

impl Store {
    /// Insert a fresh `pending` lease. Returns the [`Lease`] so the caller
    /// can embed `lease_id` in the assignment envelope.
    ///
    /// # Errors
    /// `StoreError::Sqlite` for FK violations (task does not exist).
    pub fn create_lease(
        &self,
        task_id: TaskId,
        worker_id: NodeId,
        ttl_ms: u32,
        attempt: u32,
    ) -> Result<Lease, StoreError> {
        let lease_id = LeaseId::new_v7();
        let issued_at = now_ms();
        let expires_at = issued_at.saturating_add(u64::from(ttl_ms));
        self.with_conn(|c| {
            c.execute(
                "INSERT INTO leases (
                    lease_id, task_id, worker_id, state,
                    issued_at, expires_at, attempt
                ) VALUES (?, ?, ?, 'pending', ?, ?, ?)",
                params![
                    lease_id.0.as_bytes(),
                    task_id.0.as_bytes(),
                    worker_id.as_bytes(),
                    i64::try_from(issued_at).unwrap_or(i64::MAX),
                    i64::try_from(expires_at).unwrap_or(i64::MAX),
                    attempt,
                ],
            )?;
            Ok(())
        })?;
        Ok(Lease {
            lease_id,
            task_id,
            worker_id: Some(worker_id),
            state: LeaseState::Pending,
            issued_at,
            expires_at,
            attempt,
        })
    }

    /// Atomically transition `pending → claimed` for `lease_id`, only if
    /// the `worker_id` matches and the lease has not yet expired in wall
    /// clock. Returns `Ok(true)` on success, `Ok(false)` if the row was
    /// already claimed / expired / belonged to a different worker.
    pub fn try_claim(&self, lease_id: LeaseId, claimer: NodeId) -> Result<bool, StoreError> {
        let now = now_ms();
        self.with_conn(|c| {
            let n = c.execute(
                "UPDATE leases
                    SET state = 'claimed'
                  WHERE lease_id = ?
                    AND state = 'pending'
                    AND worker_id = ?
                    AND expires_at >= ?",
                params![
                    lease_id.0.as_bytes(),
                    claimer.as_bytes(),
                    i64::try_from(now).unwrap_or(i64::MAX),
                ],
            )?;
            Ok(n == 1)
        })
    }

    /// Atomically transition `claimed → completed`. Returns `Ok(true)` on
    /// success, `Ok(false)` if the lease is no longer in `claimed`.
    pub fn try_complete(&self, lease_id: LeaseId) -> Result<bool, StoreError> {
        self.with_conn(|c| {
            let n = c.execute(
                "UPDATE leases SET state = 'completed'
                  WHERE lease_id = ? AND state = 'claimed'",
                params![lease_id.0.as_bytes()],
            )?;
            Ok(n == 1)
        })
    }

    /// Refresh `expires_at` for an in-flight `claimed` lease.
    pub fn extend(&self, lease_id: LeaseId, new_expires_at: u64) -> Result<bool, StoreError> {
        self.with_conn(|c| {
            let n = c.execute(
                "UPDATE leases SET expires_at = ?
                  WHERE lease_id = ? AND state = 'claimed'",
                params![
                    i64::try_from(new_expires_at).unwrap_or(i64::MAX),
                    lease_id.0.as_bytes(),
                ],
            )?;
            Ok(n == 1)
        })
    }

    /// Find leases whose state is `pending`/`claimed` and whose
    /// `expires_at < now_ms`. Bounded by `limit`.
    pub fn find_expired(&self, now_ms: u64, limit: u32) -> Result<Vec<Lease>, StoreError> {
        self.with_conn(|c| {
            let mut stmt = c.prepare(
                "SELECT lease_id, task_id, worker_id, state,
                        issued_at, expires_at, attempt
                   FROM leases
                  WHERE state IN ('pending', 'claimed')
                    AND expires_at < ?
                  ORDER BY expires_at ASC
                  LIMIT ?",
            )?;
            let rows: Vec<Lease> = stmt
                .query_map(
                    params![i64::try_from(now_ms).unwrap_or(i64::MAX), limit],
                    row_to_lease,
                )?
                .collect::<Result<_, _>>()?;
            Ok(rows)
        })
    }

    /// Atomically: mark the lease `expired` AND transition the parent task
    /// back to `submitted` for re-dispatch. Returns `Ok(true)` if both
    /// transitions applied.
    pub fn expire_and_reset_task(&self, lease_id: LeaseId) -> Result<bool, StoreError> {
        self.with_conn(|c| {
            let tx = c.unchecked_transaction()?;
            let task_blob: Option<Vec<u8>> = tx
                .query_row(
                    "SELECT task_id FROM leases
                      WHERE lease_id = ? AND state IN ('pending', 'claimed')",
                    params![lease_id.0.as_bytes()],
                    |r| r.get(0),
                )
                .optional()?;
            let Some(task_blob) = task_blob else {
                return Ok(false);
            };
            tx.execute(
                "UPDATE leases SET state = 'expired'
                  WHERE lease_id = ? AND state IN ('pending', 'claimed')",
                params![lease_id.0.as_bytes()],
            )?;
            tx.execute(
                "UPDATE tasks SET state = 'submitted', assigned_node = NULL
                  WHERE id = ? AND state IN ('dispatched', 'claimed', 'running')",
                params![task_blob],
            )?;
            tx.commit()?;
            Ok(true)
        })
    }

    pub fn fetch_lease(&self, lease_id: LeaseId) -> Result<Option<Lease>, StoreError> {
        self.with_conn(|c| {
            let row = c
                .query_row(
                    "SELECT lease_id, task_id, worker_id, state,
                            issued_at, expires_at, attempt
                       FROM leases WHERE lease_id = ?",
                    params![lease_id.0.as_bytes()],
                    row_to_lease,
                )
                .optional()?;
            Ok(row)
        })
    }

    pub fn list_leases_for_task(&self, task_id: TaskId) -> Result<Vec<Lease>, StoreError> {
        self.with_conn(|c| {
            let mut stmt = c.prepare(
                "SELECT lease_id, task_id, worker_id, state,
                        issued_at, expires_at, attempt
                   FROM leases WHERE task_id = ?
                ORDER BY issued_at ASC",
            )?;
            let rows: Vec<Lease> = stmt
                .query_map(params![task_id.0.as_bytes()], row_to_lease)?
                .collect::<Result<_, _>>()?;
            Ok(rows)
        })
    }

    // ---------- dispatcher_cursors ----------

    /// Read the round-robin cursor for `capability`. `None` means no
    /// dispatch has occurred yet (or the daemon was just installed).
    pub fn last_dispatched(&self, capability: &str) -> Result<Option<NodeId>, StoreError> {
        self.with_conn(|c| {
            let blob: Option<Vec<u8>> = c
                .query_row(
                    "SELECT last_node FROM dispatcher_cursors WHERE capability = ?",
                    params![capability],
                    |r| r.get(0),
                )
                .optional()?
                .flatten();
            blob.map(|b| {
                <[u8; 16]>::try_from(b.as_slice())
                    .map(NodeId::from_bytes)
                    .map_err(|_| StoreError::Cbor("last_node must be 16 bytes".into()))
            })
            .transpose()
        })
    }

    /// Persist the round-robin cursor.
    pub fn set_last_dispatched(&self, capability: &str, node: NodeId) -> Result<(), StoreError> {
        let now = now_ms();
        self.with_conn(|c| {
            c.execute(
                "INSERT INTO dispatcher_cursors (capability, last_node, updated_at)
                 VALUES (?, ?, ?)
                 ON CONFLICT(capability) DO UPDATE SET
                    last_node  = excluded.last_node,
                    updated_at = excluded.updated_at",
                params![
                    capability,
                    node.as_bytes(),
                    i64::try_from(now).unwrap_or(i64::MAX),
                ],
            )?;
            Ok(())
        })
    }
}

fn row_to_lease(r: &rusqlite::Row<'_>) -> rusqlite::Result<Lease> {
    let lease_blob: Vec<u8> = r.get(0)?;
    let task_blob: Vec<u8> = r.get(1)?;
    let worker_blob: Option<Vec<u8>> = r.get(2)?;
    let state_str: String = r.get(3)?;
    let issued_at: i64 = r.get(4)?;
    let expires_at: i64 = r.get(5)?;
    let attempt: u32 = r.get(6)?;

    let lease_arr: [u8; 16] = lease_blob.as_slice().try_into().map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Blob,
            "lease_id must be 16 bytes".into(),
        )
    })?;
    let task_arr: [u8; 16] = task_blob.as_slice().try_into().map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            1,
            rusqlite::types::Type::Blob,
            "task_id must be 16 bytes".into(),
        )
    })?;
    let worker_id = worker_blob
        .map(|b| {
            <[u8; 16]>::try_from(b.as_slice())
                .map(NodeId::from_bytes)
                .map_err(|_| {
                    rusqlite::Error::FromSqlConversionFailure(
                        2,
                        rusqlite::types::Type::Blob,
                        "worker_id must be 16 bytes".into(),
                    )
                })
        })
        .transpose()?;
    let state: LeaseState = state_str.parse().map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            3,
            rusqlite::types::Type::Text,
            "invalid lease state".into(),
        )
    })?;
    Ok(Lease {
        lease_id: LeaseId(uuid::Uuid::from_bytes(lease_arr)),
        task_id: TaskId(uuid::Uuid::from_bytes(task_arr)),
        worker_id,
        state,
        issued_at: u64::try_from(issued_at).unwrap_or(0),
        expires_at: u64::try_from(expires_at).unwrap_or(0),
        attempt,
    })
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use harness_core::{
        Constraints, ExecutionPolicy, Identity, ResourceHints, RetryPolicy, Signable, Signature,
        Task, TaskId, TraceContext,
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
            input: serde_json::json!({}),
            constraints: Constraints::default(),
            retry: RetryPolicy::default(),
            execution: ExecutionPolicy::default(),
            resource_hints: empty_hints(),
            trace_ctx: TraceContext::default(),
            issued_by: id.public_key().node_id(),
            issued_at: 1_700_000_000_000,
            tags: Vec::new(),
            sig: Signature::from_bytes([0; 64]),
        };
        t.sign(id).expect("sign");
        t
    }

    #[test]
    fn create_then_fetch_round_trips() {
        let s = Store::open_memory().expect("open");
        let id = Identity::generate();
        let task = signed_task(&id);
        s.insert_task(&task).expect("task");
        let lease = s
            .create_lease(task.id, NodeId::from_bytes([2; 16]), 5_000, 1)
            .expect("create");
        let fetched = s
            .fetch_lease(lease.lease_id)
            .expect("fetch")
            .expect("present");
        assert_eq!(fetched, lease);
        assert_eq!(fetched.state, LeaseState::Pending);
    }

    #[test]
    fn try_claim_succeeds_once() {
        let s = Store::open_memory().expect("open");
        let id = Identity::generate();
        let task = signed_task(&id);
        s.insert_task(&task).expect("task");
        let worker = NodeId::from_bytes([2; 16]);
        let lease = s.create_lease(task.id, worker, 5_000, 1).expect("create");
        assert!(s.try_claim(lease.lease_id, worker).expect("claim 1"));
        assert!(!s.try_claim(lease.lease_id, worker).expect("claim 2"));
    }

    #[test]
    fn try_claim_wrong_worker_fails() {
        let s = Store::open_memory().expect("open");
        let id = Identity::generate();
        let task = signed_task(&id);
        s.insert_task(&task).expect("task");
        let worker = NodeId::from_bytes([2; 16]);
        let other = NodeId::from_bytes([3; 16]);
        let lease = s.create_lease(task.id, worker, 5_000, 1).expect("create");
        assert!(!s.try_claim(lease.lease_id, other).expect("claim"));
    }

    #[test]
    fn try_complete_only_after_claim() {
        let s = Store::open_memory().expect("open");
        let id = Identity::generate();
        let task = signed_task(&id);
        s.insert_task(&task).expect("task");
        let worker = NodeId::from_bytes([2; 16]);
        let lease = s.create_lease(task.id, worker, 5_000, 1).expect("create");
        assert!(!s.try_complete(lease.lease_id).expect("complete pre-claim"));
        s.try_claim(lease.lease_id, worker).expect("claim");
        assert!(s.try_complete(lease.lease_id).expect("complete"));
        assert!(!s.try_complete(lease.lease_id).expect("complete twice"));
    }

    #[test]
    fn extend_only_when_claimed() {
        let s = Store::open_memory().expect("open");
        let id = Identity::generate();
        let task = signed_task(&id);
        s.insert_task(&task).expect("task");
        let worker = NodeId::from_bytes([2; 16]);
        let lease = s.create_lease(task.id, worker, 5_000, 1).expect("create");
        // pending → extend should fail (not claimed yet)
        assert!(!s
            .extend(lease.lease_id, lease.expires_at + 1000)
            .expect("extend"));
        s.try_claim(lease.lease_id, worker).expect("claim");
        assert!(s
            .extend(lease.lease_id, lease.expires_at + 1000)
            .expect("extend"));
    }

    #[test]
    fn expire_and_reset_resubmits_task() {
        let s = Store::open_memory().expect("open");
        let id = Identity::generate();
        let task = signed_task(&id);
        s.insert_task(&task).expect("task");
        s.transition_task(task.id, crate::TaskState::Dispatched)
            .expect("dispatch");
        let worker = NodeId::from_bytes([2; 16]);
        let lease = s.create_lease(task.id, worker, 5_000, 1).expect("create");
        assert!(s.expire_and_reset_task(lease.lease_id).expect("expire"));
        assert_eq!(
            s.task_state(task.id).unwrap(),
            Some(crate::TaskState::Submitted)
        );
        // Second call is a no-op (lease already expired).
        assert!(!s.expire_and_reset_task(lease.lease_id).expect("expire 2"));
    }

    #[test]
    fn find_expired_returns_only_old_pending_or_claimed() {
        let s = Store::open_memory().expect("open");
        let id = Identity::generate();
        let task = signed_task(&id);
        s.insert_task(&task).expect("task");
        let worker = NodeId::from_bytes([2; 16]);
        let lease = s.create_lease(task.id, worker, 1, 1).expect("create");
        // Sleep just enough to push wall-clock past the 1ms TTL.
        std::thread::sleep(std::time::Duration::from_millis(10));
        let now = lease.expires_at + 100;
        let expired = s.find_expired(now, 10).expect("find");
        assert!(expired.iter().any(|l| l.lease_id == lease.lease_id));

        // After expiring + completing, the lease is no longer in the
        // "find_expired" set.
        s.expire_and_reset_task(lease.lease_id).expect("expire");
        let expired2 = s.find_expired(now + 1_000, 10).expect("find");
        assert!(expired2.iter().all(|l| l.lease_id != lease.lease_id));
    }

    #[test]
    fn last_dispatched_round_trips() {
        let s = Store::open_memory().expect("open");
        assert_eq!(s.last_dispatched("echo").unwrap(), None);
        s.set_last_dispatched("echo", NodeId::from_bytes([7; 16]))
            .expect("set");
        assert_eq!(
            s.last_dispatched("echo").unwrap(),
            Some(NodeId::from_bytes([7; 16]))
        );
        s.set_last_dispatched("echo", NodeId::from_bytes([9; 16]))
            .expect("set 2");
        assert_eq!(
            s.last_dispatched("echo").unwrap(),
            Some(NodeId::from_bytes([9; 16]))
        );
    }

    #[test]
    fn task_cascade_drops_leases() {
        let s = Store::open_memory().expect("open");
        let id = Identity::generate();
        let task = signed_task(&id);
        s.insert_task(&task).expect("task");
        let _ = s
            .create_lease(task.id, NodeId::from_bytes([2; 16]), 5_000, 1)
            .expect("create");
        // Deleting the task via FK CASCADE should clear leases. We don't
        // currently have a delete_task helper; verify list_leases_for_task
        // first finds the lease, then cleanup via raw SQL (in tests only).
        assert_eq!(s.list_leases_for_task(task.id).unwrap().len(), 1);
        s.with_conn(|c| {
            c.execute(
                "DELETE FROM tasks WHERE id = ?",
                rusqlite::params![task.id.0.as_bytes()],
            )?;
            Ok(())
        })
        .unwrap();
        assert_eq!(s.list_leases_for_task(task.id).unwrap().len(), 0);
    }
}
