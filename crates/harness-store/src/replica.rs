//! Replicated task state — persistence layer for the LWW map. Implements
//! [`harness_core::ReplicaApplier`] so `harness-mesh::gossip` can deliver
//! incoming changes without depending on this crate.
//!
//! ADR-0006 documents why we ship a custom LWW map rather than `automerge`.
//!
//! Wire-level gossip transport lands in 2.6 (alongside HTTP submit + auth).
//! 2.5 ships:
//!
//! - V0003 `task_replica_state` table.
//! - `ReplicatedTaskStore` with `apply_local`, `apply_remote_envelope`,
//!   `view_task`, `snapshot_all`, and `head` (blake3 of canonical bytes).
//! - LWW merge logic delegated to `harness_core::ReplicatedTaskState::supersedes`.

use harness_core::{
    NodeId, ReplicaApplier, ReplicaError, ReplicaSyncEnvelope, ReplicatedState,
    ReplicatedTaskState, TaskId,
};
use rusqlite::{params, OptionalExtension};

use crate::error::StoreError;
use crate::open::Store;

impl Store {
    /// Apply a locally-generated transition. Used by the dispatcher and
    /// worker glue (wired in 2.6) to mirror SQL state into the replica map.
    /// Returns `true` if the entry was inserted/updated, `false` if an
    /// existing entry already supersedes the incoming.
    pub fn replica_apply_local(&self, entry: &ReplicatedTaskState) -> Result<bool, StoreError> {
        self.with_conn(|c| upsert_if_supersedes(c, entry))
    }

    /// Read the latest replicated state for `task_id`, if any.
    pub fn replica_view_task(
        &self,
        task_id: TaskId,
    ) -> Result<Option<ReplicatedTaskState>, StoreError> {
        self.with_conn(|c| {
            let row = c
                .query_row(
                    "SELECT task_id, state, at_ms, source, output_preview
                       FROM task_replica_state WHERE task_id = ?",
                    params![task_id.0.as_bytes()],
                    row_to_replicated_state,
                )
                .optional()?;
            Ok(row)
        })
    }

    /// Snapshot the entire replica map, sorted by `task_id` so the
    /// `replica_head` blake3 is deterministic across nodes that converged.
    pub fn replica_snapshot(&self) -> Result<Vec<ReplicatedTaskState>, StoreError> {
        self.with_conn(|c| {
            let mut stmt = c.prepare(
                "SELECT task_id, state, at_ms, source, output_preview
                   FROM task_replica_state ORDER BY task_id",
            )?;
            let rows: Vec<ReplicatedTaskState> = stmt
                .query_map([], row_to_replicated_state)?
                .collect::<Result<_, _>>()?;
            Ok(rows)
        })
    }

    /// `blake3(ciborium-canonical-encoding(replica_snapshot))[..32]` —
    /// the value heartbeats piggy-back for anti-entropy. Two nodes that
    /// have converged on the same set of `(task_id, state, at_ms, source)`
    /// tuples produce the same head.
    pub fn replica_head(&self) -> Result<[u8; 32], StoreError> {
        let snap = self.replica_snapshot()?;
        let mut buf = Vec::with_capacity(snap.len() * 64);
        ciborium::ser::into_writer(&snap, &mut buf).map_err(|e| StoreError::Cbor(e.to_string()))?;
        Ok(*blake3::hash(&buf).as_bytes())
    }

    /// Apply a remote envelope after the gossip layer has verified its
    /// signature. Returns the count of entries that actually changed
    /// local state.
    pub fn replica_apply_envelope(
        &self,
        envelope: &ReplicaSyncEnvelope,
    ) -> Result<usize, StoreError> {
        let mut applied = 0;
        self.with_conn(|c| {
            let tx = c.unchecked_transaction()?;
            for entry in &envelope.entries {
                if upsert_if_supersedes_in_tx(&tx, entry)? {
                    applied += 1;
                }
            }
            tx.commit()?;
            Ok(())
        })?;
        Ok(applied)
    }
}

fn upsert_if_supersedes(
    c: &rusqlite::Connection,
    entry: &ReplicatedTaskState,
) -> Result<bool, StoreError> {
    let tx = c.unchecked_transaction()?;
    let result = upsert_if_supersedes_in_tx(&tx, entry)?;
    tx.commit()?;
    Ok(result)
}

fn upsert_if_supersedes_in_tx(
    tx: &rusqlite::Transaction<'_>,
    entry: &ReplicatedTaskState,
) -> Result<bool, StoreError> {
    let current: Option<ReplicatedTaskState> = tx
        .query_row(
            "SELECT task_id, state, at_ms, source, output_preview
               FROM task_replica_state WHERE task_id = ?",
            params![entry.task_id.0.as_bytes()],
            row_to_replicated_state,
        )
        .optional()?;

    if let Some(c) = &current {
        if !c.supersedes(entry) {
            return Ok(false);
        }
    }

    let preview_bound = entry
        .output_preview
        .as_ref()
        .map(|v| v.iter().take(256).copied().collect::<Vec<u8>>());
    tx.execute(
        "INSERT INTO task_replica_state
            (task_id, state, at_ms, source, output_preview)
         VALUES (?, ?, ?, ?, ?)
         ON CONFLICT(task_id) DO UPDATE SET
            state          = excluded.state,
            at_ms          = excluded.at_ms,
            source         = excluded.source,
            output_preview = excluded.output_preview",
        params![
            entry.task_id.0.as_bytes(),
            entry.state as u8,
            i64::try_from(entry.at_ms).unwrap_or(i64::MAX),
            entry.source.as_bytes(),
            preview_bound,
        ],
    )?;
    Ok(true)
}

fn row_to_replicated_state(r: &rusqlite::Row<'_>) -> rusqlite::Result<ReplicatedTaskState> {
    let task_blob: Vec<u8> = r.get(0)?;
    let task_arr: [u8; 16] = task_blob.as_slice().try_into().map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Blob,
            "task_id must be 16 bytes".into(),
        )
    })?;
    let state_byte: u8 = r.get::<_, i64>(1)?.try_into().unwrap_or(0);
    let state = match state_byte {
        0 => ReplicatedState::Submitted,
        1 => ReplicatedState::Planned,
        2 => ReplicatedState::Dispatched,
        3 => ReplicatedState::Claimed,
        4 => ReplicatedState::Running,
        5 => ReplicatedState::Expired,
        6 => ReplicatedState::Cancelled,
        7 => ReplicatedState::Failed,
        8 => ReplicatedState::Done,
        _ => {
            return Err(rusqlite::Error::FromSqlConversionFailure(
                1,
                rusqlite::types::Type::Integer,
                "state byte out of range".into(),
            ))
        }
    };
    let at_ms = u64::try_from(r.get::<_, i64>(2)?).unwrap_or(0);
    let source_blob: Vec<u8> = r.get(3)?;
    let source_arr: [u8; 16] = source_blob.as_slice().try_into().map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            3,
            rusqlite::types::Type::Blob,
            "source must be 16 bytes".into(),
        )
    })?;
    let output_preview: Option<Vec<u8>> = r.get(4)?;
    Ok(ReplicatedTaskState {
        task_id: TaskId(uuid::Uuid::from_bytes(task_arr)),
        state,
        at_ms,
        source: NodeId::from_bytes(source_arr),
        output_preview,
    })
}

/// Adapter so the gossip transport can call into `Store` via the trait
/// without naming the concrete type. The mesh layer holds an
/// `Arc<dyn ReplicaApplier>` so 2.6 can wire either a real Store or a
/// no-op for hosts running with `--ephemeral`.
#[derive(Debug)]
pub struct StoreReplicaApplier(pub Store);

impl ReplicaApplier for StoreReplicaApplier {
    fn apply(&self, envelope: &ReplicaSyncEnvelope) -> Result<usize, ReplicaError> {
        self.0
            .replica_apply_envelope(envelope)
            .map_err(|e| ReplicaError::Storage(e.to_string()))
    }

    fn head(&self) -> [u8; 32] {
        self.0.replica_head().unwrap_or_default()
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use harness_core::{Identity, Signable};

    fn entry(
        state: ReplicatedState,
        at_ms: u64,
        source: u8,
        task_id: TaskId,
    ) -> ReplicatedTaskState {
        ReplicatedTaskState {
            task_id,
            state,
            at_ms,
            source: NodeId::from_bytes([source; 16]),
            output_preview: None,
        }
    }

    #[test]
    fn apply_local_inserts_first_entry() {
        let s = Store::open_memory().expect("open");
        let id = TaskId::new_v7();
        let e = entry(ReplicatedState::Submitted, 100, 1, id);
        assert!(s.replica_apply_local(&e).expect("apply"));
        let view = s.replica_view_task(id).expect("view").expect("present");
        assert_eq!(view, e);
    }

    #[test]
    fn lww_higher_state_supersedes() {
        let s = Store::open_memory().expect("open");
        let id = TaskId::new_v7();
        s.replica_apply_local(&entry(ReplicatedState::Running, 1000, 1, id))
            .expect("running");
        // Earlier wall-clock but more advanced state — supersedes.
        assert!(s
            .replica_apply_local(&entry(ReplicatedState::Done, 500, 2, id))
            .expect("done"));
        let view = s.replica_view_task(id).expect("view").expect("present");
        assert_eq!(view.state, ReplicatedState::Done);
    }

    #[test]
    fn lww_earlier_state_does_not_supersede() {
        let s = Store::open_memory().expect("open");
        let id = TaskId::new_v7();
        s.replica_apply_local(&entry(ReplicatedState::Done, 500, 1, id))
            .expect("done");
        // Later wall-clock but more rewinding state — does NOT supersede.
        assert!(!s
            .replica_apply_local(&entry(ReplicatedState::Running, 10_000, 2, id))
            .expect("running"));
        let view = s.replica_view_task(id).expect("view").expect("present");
        assert_eq!(view.state, ReplicatedState::Done);
    }

    #[test]
    fn at_ms_breaks_tie_when_state_equal() {
        let s = Store::open_memory().expect("open");
        let id = TaskId::new_v7();
        s.replica_apply_local(&entry(ReplicatedState::Done, 500, 1, id))
            .expect("v1");
        // Same state, later at_ms wins.
        assert!(s
            .replica_apply_local(&entry(ReplicatedState::Done, 1000, 1, id))
            .expect("v2"));
        let view = s.replica_view_task(id).expect("view").expect("present");
        assert_eq!(view.at_ms, 1000);
    }

    #[test]
    fn output_preview_truncated_to_256_bytes() {
        let s = Store::open_memory().expect("open");
        let id = TaskId::new_v7();
        let big_preview = vec![0xAB_u8; 1024];
        let e = ReplicatedTaskState {
            task_id: id,
            state: ReplicatedState::Done,
            at_ms: 100,
            source: NodeId::from_bytes([1; 16]),
            output_preview: Some(big_preview),
        };
        s.replica_apply_local(&e).expect("apply");
        let view = s.replica_view_task(id).expect("view").expect("present");
        assert_eq!(view.output_preview.expect("preview").len(), 256);
    }

    #[test]
    fn head_changes_on_apply_and_is_deterministic() {
        let s1 = Store::open_memory().expect("open");
        let s2 = Store::open_memory().expect("open");
        let h0 = s1.replica_head().expect("head");
        assert_eq!(h0, s2.replica_head().expect("head"));

        let id = TaskId::new_v7();
        let e = entry(ReplicatedState::Done, 100, 1, id);
        s1.replica_apply_local(&e).expect("apply 1");
        s2.replica_apply_local(&e).expect("apply 2");
        let h1 = s1.replica_head().expect("head 1");
        assert_ne!(h0, h1, "head must change on apply");
        assert_eq!(h1, s2.replica_head().expect("head 2"));
    }

    #[test]
    fn snapshot_is_sorted_by_task_id() {
        let s = Store::open_memory().expect("open");
        let mut ids: Vec<TaskId> = (0..4).map(|_| TaskId::new_v7()).collect();
        for &id in &ids {
            s.replica_apply_local(&entry(ReplicatedState::Submitted, 0, 1, id))
                .expect("apply");
        }
        ids.sort();
        let snap = s.replica_snapshot().expect("snap");
        let snap_ids: Vec<TaskId> = snap.iter().map(|e| e.task_id).collect();
        assert_eq!(snap_ids, ids);
    }

    #[test]
    fn apply_envelope_returns_changed_count() {
        let s = Store::open_memory().expect("open");
        let id_a = TaskId::new_v7();
        let id_b = TaskId::new_v7();
        // Pre-load id_a with a Done entry; envelope's Running entry will lose.
        s.replica_apply_local(&entry(ReplicatedState::Done, 200, 1, id_a))
            .expect("preload");

        let identity = Identity::generate();
        let mut env = ReplicaSyncEnvelope {
            source: identity.public_key().node_id(),
            assembled_at: 1,
            entries: vec![
                entry(ReplicatedState::Running, 100, 5, id_a), // loses to existing Done
                entry(ReplicatedState::Done, 100, 5, id_b),    // new
            ],
            sig: harness_core::Signature::from_bytes([0; 64]),
        };
        env.sign(&identity).expect("sign");
        let n = s.replica_apply_envelope(&env).expect("apply env");
        assert_eq!(n, 1, "only the new task changed local state");
    }

    #[test]
    fn store_replica_applier_trait_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<StoreReplicaApplier>();
    }
}
