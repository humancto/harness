-- 5.13a (ADR-0041) — the append-only, hash-chained audit log (PRD
-- §10.6). One chain PER NODE: a single mesh-wide chain would need
-- consensus on the next position, and "no broker, no consensus" is
-- load-bearing in this design. Each node appends only to its own
-- chain; 5.13c replicates the others'.
--
-- `entry_hash` is blake3 over a JSON OBJECT of the entry's fields
-- INCLUDING node_id, seq and prev_hash (never a concatenation — see
-- harness_core::audit_entry_hash). Retention prunes through a
-- `audit.truncated` marker entry that carries the anchor hash, so the
-- chain still verifies across the gap.
--
-- Rowid table, not WITHOUT ROWID: `detail` is variable-length TEXT.

INSERT OR REPLACE INTO harness_meta(key, value) VALUES ('schema_version', '9');

CREATE TABLE IF NOT EXISTS audit_log (
    id         INTEGER PRIMARY KEY,
    node_id    BLOB    NOT NULL CHECK (length(node_id) = 16),
    seq        INTEGER NOT NULL CHECK (seq > 0),
    at_ms      INTEGER NOT NULL,
    action     TEXT    NOT NULL,
    subject    TEXT,
    detail     TEXT    CHECK (detail IS NULL OR length(detail) <= 4096),
    actor      TEXT    NOT NULL,
    prev_hash  BLOB    NOT NULL CHECK (length(prev_hash) = 32),
    entry_hash BLOB    NOT NULL CHECK (length(entry_hash) = 32),
    UNIQUE(node_id, seq)
);

-- The listing is time-ordered and merged across nodes; the filters
-- are by action and by node.
CREATE INDEX IF NOT EXISTS idx_audit_at_ms ON audit_log(at_ms DESC);
CREATE INDEX IF NOT EXISTS idx_audit_action_at ON audit_log(action, at_ms DESC);
