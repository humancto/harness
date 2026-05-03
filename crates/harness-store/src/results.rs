//! Task results — full local output / error for a terminal task.
//!
//! The LWW replica's `output_preview` (256 bytes, often non-parseable)
//! is for cross-node gossip. This table holds the full output for the
//! UI / CLI to render.
//!
//! Writes use `INSERT ... ON CONFLICT(task_id) DO UPDATE SET ...` so a
//! retry doesn't error on the unique key.

use harness_core::{NodeId, TaskId};
use rusqlite::{params, OptionalExtension};
use serde_json::Value as JsonValue;

use crate::error::StoreError;
use crate::open::Store;

#[derive(Debug, Clone)]
pub struct TaskResult {
    pub task_id: TaskId,
    pub output: Option<JsonValue>,
    pub error: Option<String>,
    pub completed_at_ms: u64,
    pub completed_by: NodeId,
}

impl Store {
    /// Persist a successful capability execution.
    pub fn write_task_result_done(
        &self,
        task_id: TaskId,
        output: &JsonValue,
        completed_at_ms: u64,
        completed_by: NodeId,
    ) -> Result<(), StoreError> {
        let output_json = serde_json::to_string(output)
            .map_err(|e| StoreError::Cbor(format!("encode output: {e}")))?;
        self.with_conn(|c| {
            c.execute(
                "INSERT INTO task_results (task_id, output, error, completed_at_ms, completed_by)
                 VALUES (?1, ?2, NULL, ?3, ?4)
                 ON CONFLICT(task_id) DO UPDATE SET
                    output           = excluded.output,
                    error            = NULL,
                    completed_at_ms  = excluded.completed_at_ms,
                    completed_by     = excluded.completed_by",
                params![
                    task_id.0.as_bytes(),
                    output_json,
                    completed_at_ms,
                    completed_by.as_bytes(),
                ],
            )?;
            Ok(())
        })
    }

    /// Persist a failed capability execution.
    pub fn write_task_result_failed(
        &self,
        task_id: TaskId,
        error: &str,
        completed_at_ms: u64,
        completed_by: NodeId,
    ) -> Result<(), StoreError> {
        self.with_conn(|c| {
            c.execute(
                "INSERT INTO task_results (task_id, output, error, completed_at_ms, completed_by)
                 VALUES (?1, NULL, ?2, ?3, ?4)
                 ON CONFLICT(task_id) DO UPDATE SET
                    output           = NULL,
                    error            = excluded.error,
                    completed_at_ms  = excluded.completed_at_ms,
                    completed_by     = excluded.completed_by",
                params![
                    task_id.0.as_bytes(),
                    error,
                    completed_at_ms,
                    completed_by.as_bytes(),
                ],
            )?;
            Ok(())
        })
    }

    /// Fetch the full result for a task (None if not yet terminal or unknown).
    pub fn load_task_result(&self, task_id: TaskId) -> Result<Option<TaskResult>, StoreError> {
        self.with_conn(|c| {
            let row = c
                .query_row(
                    "SELECT output, error, completed_at_ms, completed_by
                       FROM task_results WHERE task_id = ?1",
                    params![task_id.0.as_bytes()],
                    |r| {
                        let output: Option<String> = r.get(0)?;
                        let error: Option<String> = r.get(1)?;
                        let completed_at_ms: u64 = r.get(2)?;
                        let by_blob: Vec<u8> = r.get(3)?;
                        Ok((output, error, completed_at_ms, by_blob))
                    },
                )
                .optional()?;
            let Some((output, error, completed_at_ms, by_blob)) = row else {
                return Ok(None);
            };
            let by_arr: [u8; 16] = by_blob
                .as_slice()
                .try_into()
                .map_err(|_| StoreError::Cbor("completed_by must be 16 bytes".into()))?;
            let parsed_output = match output {
                Some(s) => Some(
                    serde_json::from_str::<JsonValue>(&s)
                        .map_err(|e| StoreError::Cbor(format!("decode output: {e}")))?,
                ),
                None => None,
            };
            Ok(Some(TaskResult {
                task_id,
                output: parsed_output,
                error,
                completed_at_ms,
                completed_by: NodeId::from_bytes(by_arr),
            }))
        })
    }
}
