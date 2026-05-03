-- Phase 2.3 initial schema. See `.planning/phase-2.3-sqlite-schema.plan.md`.

CREATE TABLE IF NOT EXISTS harness_meta (
    key   TEXT PRIMARY KEY NOT NULL,
    value TEXT             NOT NULL
) WITHOUT ROWID;

INSERT OR REPLACE INTO harness_meta(key, value) VALUES ('schema_version', '1');

-- Most-recent verified manifest per peer. Re-populated on startup by
-- the gossip layer; persisted so the dispatcher doesn't have to wait
-- for every peer to re-announce before it can route.
CREATE TABLE IF NOT EXISTS node_manifests (
    node_id        BLOB PRIMARY KEY NOT NULL,
    pubkey_hex     TEXT             NOT NULL,
    hostname       TEXT             NOT NULL,
    online_since   INTEGER          NOT NULL,
    canonical_cbor BLOB             NOT NULL,
    signature      BLOB             NOT NULL,
    received_at    INTEGER          NOT NULL,
    CHECK (length(node_id) = 16),
    CHECK (length(signature) = 64)
);

CREATE INDEX IF NOT EXISTS idx_node_manifests_pubkey ON node_manifests(pubkey_hex);

-- Capability index — denormalized "(capability_id, major) -> node_id".
-- Rebuildable from node_manifests; persisted for fast dispatcher startup.
CREATE TABLE IF NOT EXISTS capability_index (
    capability_id  TEXT NOT NULL,
    version_major  INTEGER NOT NULL,
    version_minor  INTEGER NOT NULL,
    version_patch  INTEGER NOT NULL,
    node_id        BLOB NOT NULL,
    cardinality    INTEGER NOT NULL,
    cost_hint      TEXT NOT NULL,
    PRIMARY KEY (capability_id, node_id),
    FOREIGN KEY (node_id) REFERENCES node_manifests(node_id) ON DELETE CASCADE,
    CHECK (length(node_id) = 16),
    CHECK (cardinality IN (0, 1, 2)),
    CHECK (cost_hint IN ('local_fast', 'local_slow', 'gpu', 'cloud_paid'))
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_cap_index_by_capability
    ON capability_index(capability_id, version_major);

-- Scope index — "scope_id -> node_id" for Owner cardinality routing.
-- Multi-owner is legal (e.g., shared NAS); the dispatcher tiebreaks.
CREATE TABLE IF NOT EXISTS scope_index (
    scope_id       TEXT NOT NULL,
    scope_kind     TEXT NOT NULL,
    label          TEXT NOT NULL,
    node_id        BLOB NOT NULL,
    indexed        INTEGER NOT NULL,
    last_indexed   INTEGER,
    PRIMARY KEY (scope_id, node_id),
    FOREIGN KEY (node_id) REFERENCES node_manifests(node_id) ON DELETE CASCADE,
    CHECK (length(node_id) = 16),
    CHECK (indexed IN (0, 1))
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_scope_index_by_scope_id ON scope_index(scope_id);

-- Tasks — persistent task record per the §13.3 envelope. We store the
-- canonical CBOR + sig so the row is the source of truth for replay /
-- reconstruction; structured columns provide indexed lookups.
CREATE TABLE IF NOT EXISTS tasks (
    id             BLOB PRIMARY KEY NOT NULL,
    parent_id      BLOB,
    plan_id        BLOB,
    capability     TEXT NOT NULL,
    state          TEXT NOT NULL,
    issued_by      BLOB NOT NULL,
    issued_at      INTEGER NOT NULL,
    canonical_cbor BLOB NOT NULL,
    signature      BLOB NOT NULL,
    result_cbor    BLOB,
    result_sig     BLOB,
    completed_by   BLOB,
    started_at     INTEGER,
    finished_at    INTEGER,
    cost_usd       REAL,
    cost_tokens_in  INTEGER,
    cost_tokens_out INTEGER,
    CHECK (length(id) = 16),
    CHECK (length(issued_by) = 16),
    CHECK (length(signature) = 64),
    CHECK (parent_id IS NULL OR length(parent_id) = 16),
    CHECK (plan_id IS NULL OR length(plan_id) = 16),
    CHECK (state IN (
        'submitted', 'planned', 'dispatched', 'claimed',
        'running', 'done', 'failed', 'expired', 'cancelled'
    )),
    CHECK (result_sig IS NULL OR length(result_sig) = 64)
);

CREATE INDEX IF NOT EXISTS idx_tasks_by_state          ON tasks(state);
CREATE INDEX IF NOT EXISTS idx_tasks_by_capability     ON tasks(capability);
CREATE INDEX IF NOT EXISTS idx_tasks_by_plan_id        ON tasks(plan_id) WHERE plan_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_tasks_by_completed_by   ON tasks(completed_by) WHERE completed_by IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_tasks_by_issued_at      ON tasks(issued_at);
