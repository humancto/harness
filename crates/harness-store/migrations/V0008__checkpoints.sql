-- 5.11 (ADR-0039) — checkpoint store for plan.execute DAG steps.
--
-- One row per (plan, plan-NODE), holding the output that node produced
-- and the hash of the resolved input it produced it FOR. On resume the
-- loop recomputes each ready node's input hash; a row whose stored hash
-- matches settles the step instead of dispatching it.
--
-- The key is (plan_id, node_id), NOT (plan_id, input_hash): plan review
-- BLOCKER-1 — hashing input alone collapses legitimately distinct steps
-- that happen to share an input (`notify.send {"msg":"done"}` twice in
-- one plan) and collides ACROSS capabilities (`fs.read` vs `fs.delete`
-- on the same path), skipping a side effect while reporting ok. Node
-- ids live in the signed Plan, so they are stable across resubmission;
-- input_hash stays as the VALIDITY check (a re-planned node with a
-- different input re-runs).
--
-- A rowid table, not WITHOUT ROWID (plan review MAJOR-4): `output` is
-- up to 256 KiB and WITHOUT ROWID would store it inside the index
-- B-tree, degrading the very lookup this table exists for.
--
-- LOCAL DERIVED DATA. Never gossiped (the replica stream carries
-- ReplicatedTaskState only). Rows are dropped when the plan completes
-- fully — a checkpoint survives interruption, it is not a result cache.

INSERT OR REPLACE INTO harness_meta(key, value) VALUES ('schema_version', '8');

CREATE TABLE IF NOT EXISTS checkpoints (
    id         INTEGER PRIMARY KEY,
    plan_id    BLOB    NOT NULL,   -- PlanId bytes (16)
    node_id    BLOB    NOT NULL,   -- plan node id (16) — the step's identity
    input_hash BLOB    NOT NULL,   -- blake3 of the canonical resolved input (32)
    task_id    BLOB    NOT NULL,   -- step row that produced the output
    output     TEXT    NOT NULL,   -- full JSON; dependents resolve against it
    created_at INTEGER NOT NULL,   -- unix ms — the boot age sweep reads this
    UNIQUE(plan_id, node_id)
);

-- Both sweeps (durably-complete plans, and the age cutoff) run on the
-- periodic maintenance tick, not just at boot.
CREATE INDEX IF NOT EXISTS idx_checkpoints_created_at ON checkpoints(created_at);
