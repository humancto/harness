-- Phase 3.3a — full task output / error storage. Replica state's
-- output_preview stays as a 256-byte cross-node gossip summary; this
-- table holds the full local result.

CREATE TABLE IF NOT EXISTS task_results (
    task_id          BLOB PRIMARY KEY,
    output           TEXT,                    -- JSON, set on Done
    error            TEXT,                    -- set on Failed
    completed_at_ms  INTEGER NOT NULL,
    completed_by     BLOB    NOT NULL,
    FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE
) STRICT;

CREATE INDEX IF NOT EXISTS idx_task_results_completed_at
    ON task_results(completed_at_ms);
