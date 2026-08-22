-- Phase 3.3-fanout — which node a dispatched task is assigned to.
-- NULL until the DispatchService routes the task (state 'submitted').
-- The local executor claims only rows assigned to the local node; remote
-- assignments live in the worker's store with assigned_node = self.
-- See `.planning/phase-3.3-fanout.plan.md` and ADR-0017.

INSERT OR REPLACE INTO harness_meta(key, value) VALUES ('schema_version', '5');

ALTER TABLE tasks ADD COLUMN assigned_node BLOB
    CHECK (assigned_node IS NULL OR length(assigned_node) = 16);

CREATE INDEX IF NOT EXISTS idx_tasks_state_assigned ON tasks(state, assigned_node);
