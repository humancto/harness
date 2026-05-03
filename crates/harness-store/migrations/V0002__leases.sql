-- Phase 2.4 — leases + dispatcher cursors. See
-- `.planning/phase-2.4-round-robin-dispatcher-leases.plan.md`.

INSERT OR REPLACE INTO harness_meta(key, value) VALUES ('schema_version', '2');

-- One row per (task, attempt). Tracks claims with a wall-clock TTL.
CREATE TABLE IF NOT EXISTS leases (
    lease_id      BLOB PRIMARY KEY NOT NULL,
    task_id       BLOB NOT NULL,
    worker_id     BLOB,
    state         TEXT NOT NULL,
    issued_at     INTEGER NOT NULL,
    expires_at    INTEGER NOT NULL,
    attempt       INTEGER NOT NULL,
    FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE,
    CHECK (length(lease_id) = 16),
    CHECK (length(task_id) = 16),
    CHECK (worker_id IS NULL OR length(worker_id) = 16),
    CHECK (state IN ('pending', 'claimed', 'expired', 'released', 'completed')),
    CHECK (attempt >= 1)
);

CREATE INDEX IF NOT EXISTS idx_leases_by_task         ON leases(task_id);
CREATE INDEX IF NOT EXISTS idx_leases_by_state        ON leases(state);
CREATE INDEX IF NOT EXISTS idx_leases_by_expires_at   ON leases(expires_at);
CREATE INDEX IF NOT EXISTS idx_leases_by_worker_state ON leases(worker_id, state);

-- Round-robin pointer per capability. Persisted so daemon restart doesn't
-- bias toward the lowest NodeId for the first few dispatches.
CREATE TABLE IF NOT EXISTS dispatcher_cursors (
    capability TEXT NOT NULL,
    last_node  BLOB,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (capability),
    CHECK (last_node IS NULL OR length(last_node) = 16)
) WITHOUT ROWID;
