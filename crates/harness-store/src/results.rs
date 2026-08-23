//! Task results — full local output / error for a terminal task.
//!
//! The LWW replica's `output_preview` (256 bytes, often non-parseable)
//! is for cross-node gossip. This table holds the full output for the
//! UI / CLI to render.
//!
//! Writes use `INSERT ... ON CONFLICT(task_id) DO UPDATE SET ...` so a
//! retry doesn't error on the unique key.
//!
//! 4.5 (ADR-0027): the `provenance` column stores a JSON
//! `Vec<NodeContribution>` for Federated parents; `NULL` for the
//! Anyone/Owner single-node case.

use harness_core::{NodeContribution, NodeId, TaskId};
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
    /// Per-node contributions for a Federated result; `None` otherwise.
    pub provenance: Option<Vec<NodeContribution>>,
    /// Actual dollars (5.9, ADR-0037) — written only by the gated
    /// sites for `CloudPaid` capabilities; `None` elsewhere.
    pub cost_usd: Option<f64>,
}

fn encode_provenance(provenance: &[NodeContribution]) -> Result<String, StoreError> {
    serde_json::to_string(provenance)
        .map_err(|e| StoreError::Cbor(format!("encode provenance: {e}")))
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
        self.write_done_inner(task_id, output, completed_at_ms, completed_by, None)
    }

    /// Persist a successful *federated* execution with its per-node
    /// provenance (4.5, ADR-0027).
    pub fn write_task_result_done_with_provenance(
        &self,
        task_id: TaskId,
        output: &JsonValue,
        completed_at_ms: u64,
        completed_by: NodeId,
        provenance: &[NodeContribution],
    ) -> Result<(), StoreError> {
        let json = encode_provenance(provenance)?;
        self.write_done_inner(task_id, output, completed_at_ms, completed_by, Some(&json))
    }

    fn write_done_inner(
        &self,
        task_id: TaskId,
        output: &JsonValue,
        completed_at_ms: u64,
        completed_by: NodeId,
        provenance_json: Option<&str>,
    ) -> Result<(), StoreError> {
        let output_json = serde_json::to_string(output)
            .map_err(|e| StoreError::Cbor(format!("encode output: {e}")))?;
        self.with_conn(|c| {
            c.execute(
                "INSERT INTO task_results (task_id, output, error, completed_at_ms, completed_by, provenance)
                 VALUES (?1, ?2, NULL, ?3, ?4, ?5)
                 ON CONFLICT(task_id) DO UPDATE SET
                    output           = excluded.output,
                    error            = NULL,
                    completed_at_ms  = excluded.completed_at_ms,
                    completed_by     = excluded.completed_by,
                    provenance       = excluded.provenance",
                params![
                    task_id.0.as_bytes(),
                    output_json,
                    completed_at_ms,
                    completed_by.as_bytes(),
                    provenance_json,
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
        self.write_failed_inner(task_id, error, completed_at_ms, completed_by, None)
    }

    /// Persist a failed *federated* execution, keeping the per-node
    /// provenance so the UI can show which nodes did answer (4.5).
    pub fn write_task_result_failed_with_provenance(
        &self,
        task_id: TaskId,
        error: &str,
        completed_at_ms: u64,
        completed_by: NodeId,
        provenance: &[NodeContribution],
    ) -> Result<(), StoreError> {
        let json = encode_provenance(provenance)?;
        self.write_failed_inner(task_id, error, completed_at_ms, completed_by, Some(&json))
    }

    fn write_failed_inner(
        &self,
        task_id: TaskId,
        error: &str,
        completed_at_ms: u64,
        completed_by: NodeId,
        provenance_json: Option<&str>,
    ) -> Result<(), StoreError> {
        self.with_conn(|c| {
            c.execute(
                "INSERT INTO task_results (task_id, output, error, completed_at_ms, completed_by, provenance)
                 VALUES (?1, NULL, ?2, ?3, ?4, ?5)
                 ON CONFLICT(task_id) DO UPDATE SET
                    output           = NULL,
                    error            = excluded.error,
                    completed_at_ms  = excluded.completed_at_ms,
                    completed_by     = excluded.completed_by,
                    provenance       = excluded.provenance",
                params![
                    task_id.0.as_bytes(),
                    error,
                    completed_at_ms,
                    completed_by.as_bytes(),
                    provenance_json,
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
                    "SELECT output, error, completed_at_ms, completed_by, provenance, cost_usd
                       FROM task_results WHERE task_id = ?1",
                    params![task_id.0.as_bytes()],
                    |r| {
                        let output: Option<String> = r.get(0)?;
                        let error: Option<String> = r.get(1)?;
                        let completed_at_ms: u64 = r.get(2)?;
                        let by_blob: Vec<u8> = r.get(3)?;
                        let provenance: Option<String> = r.get(4)?;
                        let cost_usd: Option<f64> = r.get(5)?;
                        Ok((
                            output,
                            error,
                            completed_at_ms,
                            by_blob,
                            provenance,
                            cost_usd,
                        ))
                    },
                )
                .optional()?;
            let Some((output, error, completed_at_ms, by_blob, provenance, cost_usd)) = row else {
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
            let parsed_provenance = match provenance {
                Some(s) => Some(
                    serde_json::from_str::<Vec<NodeContribution>>(&s)
                        .map_err(|e| StoreError::Cbor(format!("decode provenance: {e}")))?,
                ),
                None => None,
            };
            Ok(Some(TaskResult {
                task_id,
                output: parsed_output,
                error,
                completed_at_ms,
                completed_by: NodeId::from_bytes(by_arr),
                provenance: parsed_provenance,
                cost_usd,
            }))
        })
    }

    /// 5.9 (ADR-0037): record the actual dollars for an existing
    /// result row. Called by the GATED sites only (local executor /
    /// issuer-side ingest, when the LOCAL manifest is `CloudPaid`).
    /// A retry that rewrites the row (ON CONFLICT) is followed by a
    /// fresh call, so the billed-then-retried call counts once.
    pub fn write_result_cost(&self, task_id: TaskId, cost_usd: f64) -> Result<(), StoreError> {
        self.with_conn(|c| {
            c.execute(
                "UPDATE task_results SET cost_usd = ?2 WHERE task_id = ?1",
                params![task_id.0.as_bytes(), cost_usd],
            )?;
            Ok(())
        })
    }

    /// 5.9 ledger feed: COST-BEARING completed rows joined to their
    /// task rows, newest-first, bounded by BOTH a time window and a
    /// row cap. NULL/zero-cost rows are excluded IN SQL, before the
    /// cap (Codex P1 on #60: a burst of free local completions must
    /// not evict paid rows from the window). Returns
    /// `(capability, plan_id?, issued_by, completed_at_ms, cost_usd?)`.
    #[allow(clippy::type_complexity)]
    pub fn recent_result_costs(
        &self,
        since_ms: u64,
        limit: usize,
    ) -> Result<
        Vec<(
            String,
            Option<harness_core::PlanId>,
            NodeId,
            u64,
            Option<f64>,
        )>,
        StoreError,
    > {
        self.with_conn(|c| {
            let mut stmt = c.prepare(
                "SELECT t.capability, t.plan_id, t.issued_by, r.completed_at_ms, r.cost_usd
                   FROM task_results r JOIN tasks t ON t.id = r.task_id
                  WHERE r.completed_at_ms >= ?1
                        AND r.cost_usd IS NOT NULL AND r.cost_usd > 0
                  ORDER BY r.completed_at_ms DESC LIMIT ?2",
            )?;
            let rows = stmt.query_map(
                params![since_ms, i64::try_from(limit).unwrap_or(i64::MAX)],
                |r| {
                    let capability: String = r.get(0)?;
                    let plan_blob: Option<Vec<u8>> = r.get(1)?;
                    let issued_blob: Vec<u8> = r.get(2)?;
                    let at: u64 = r.get(3)?;
                    let cost: Option<f64> = r.get(4)?;
                    Ok((capability, plan_blob, issued_blob, at, cost))
                },
            )?;
            let mut out = Vec::new();
            for row in rows {
                let (capability, plan_blob, issued_blob, at, cost) = row?;
                let plan_id = match plan_blob {
                    Some(b) => Some(harness_core::PlanId(uuid::Uuid::from_bytes(
                        b.as_slice()
                            .try_into()
                            .map_err(|_| StoreError::Cbor("plan_id must be 16 bytes".into()))?,
                    ))),
                    None => None,
                };
                let issued: [u8; 16] = issued_blob
                    .as_slice()
                    .try_into()
                    .map_err(|_| StoreError::Cbor("issued_by must be 16 bytes".into()))?;
                out.push((capability, plan_id, NodeId::from_bytes(issued), at, cost));
            }
            Ok(out)
        })
    }

    /// 5.9 ledger feed: recent OUTPUTS for one capability (bounded) —
    /// used to parse `plan.execute` aggregates for per-plan context.
    pub fn recent_outputs_for_capability(
        &self,
        capability: &str,
        since_ms: u64,
        limit: usize,
    ) -> Result<Vec<JsonValue>, StoreError> {
        self.with_conn(|c| {
            let mut stmt = c.prepare(
                "SELECT r.output
                   FROM task_results r JOIN tasks t ON t.id = r.task_id
                  WHERE t.capability = ?1 AND r.completed_at_ms >= ?2
                        AND r.output IS NOT NULL
                  ORDER BY r.completed_at_ms DESC LIMIT ?3",
            )?;
            let rows = stmt.query_map(
                params![
                    capability,
                    since_ms,
                    i64::try_from(limit).unwrap_or(i64::MAX)
                ],
                |r| {
                    let s: String = r.get(0)?;
                    Ok(s)
                },
            )?;
            let mut out = Vec::new();
            for row in rows {
                if let Ok(v) = serde_json::from_str::<JsonValue>(&row?) {
                    out.push(v);
                }
            }
            Ok(out)
        })
    }
}
