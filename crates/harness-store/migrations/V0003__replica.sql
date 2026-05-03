-- Phase 2.5 — replicated task state map. ADR-0006 explains why we ship
-- a custom LWW map rather than `automerge` here.

INSERT OR REPLACE INTO harness_meta(key, value) VALUES ('schema_version', '3');

-- One row per task. `(state, at_ms, source)` is the LWW merge key,
-- enforced in application code (the replica module). The DB just stores
-- the latest-wins entry per task_id.
CREATE TABLE IF NOT EXISTS task_replica_state (
    task_id        BLOB PRIMARY KEY NOT NULL,
    state          INTEGER NOT NULL,    -- ReplicatedState as u8
    at_ms          INTEGER NOT NULL,    -- unix milliseconds
    source         BLOB NOT NULL,       -- emitting NodeId
    output_preview BLOB,                -- nullable; up to 256 bytes when terminal
    CHECK (length(task_id) = 16),
    CHECK (length(source) = 16),
    CHECK (state >= 0 AND state <= 8),
    CHECK (output_preview IS NULL OR length(output_preview) <= 256)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_replica_by_state ON task_replica_state(state);
CREATE INDEX IF NOT EXISTS idx_replica_by_at_ms ON task_replica_state(at_ms);
