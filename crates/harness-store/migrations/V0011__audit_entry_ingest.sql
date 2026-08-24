-- 5.13c-2 (ADR-0041) — ingested peer entries, and the bookkeeping that
-- keeps a half-finished pull from being mistaken for corroboration.
--
-- Peer rows live in `audit_log` itself: it is already keyed
-- `(node_id, seq)` and `audit_verify_chain` is already per-node, which
-- is exactly why 5.13a chose one chain per node. What differs is the
-- WRITE PATH — `audit_ingest_range` never allocates a local seq and
-- refuses rows claiming this node's own chain.

INSERT OR REPLACE INTO harness_meta(key, value) VALUES ('schema_version', '11');

-- When WE received the row. NULL for rows this node appended itself.
--
-- A peer's `at_ms` is inside its own hash preimage and therefore
-- chosen by the peer; `audit_recent` orders by it, so one ingested row
-- stamped `u64::MAX` would occupy page 1 of every node's History
-- forever. The merged feed orders on COALESCE(received_at_ms, at_ms)
-- instead — our clock for their rows, theirs only for ours.
--
-- This is the same rule 5.13c-1's re-review established for relay
-- ordering: no ordering decision may key on a field the subject of
-- that ordering controls.
ALTER TABLE audit_log ADD COLUMN received_at_ms INTEGER;

CREATE INDEX IF NOT EXISTS idx_audit_feed
    ON audit_log(COALESCE(received_at_ms, at_ms) DESC);

-- An in-progress walk from some anchor toward a pin.
--
-- A pull of 40k entries cannot be one transaction (it would hold the
-- single process-wide connection mutex for the duration), and it
-- cannot be all-or-nothing per batch either — with a row cap, every
-- batch but the last ends on no pin and would be rejected, so the last
-- is unreachable. Instead a batch commits when it LINKS to the last
-- committed row, and this table records that the run is not yet
-- corroborating. Only `complete = 1` with `through_seq = target_seq`
-- and a matching hash upgrades the pin.
CREATE TABLE IF NOT EXISTS audit_ingest_runs (
    node_id       BLOB    NOT NULL CHECK (length(node_id) = 16),
    -- The pin this run is walking toward.
    target_seq    INTEGER NOT NULL CHECK (target_seq > 0),
    -- The anchor the run started from, and how far it has committed.
    from_seq      INTEGER NOT NULL CHECK (from_seq > 0),
    through_seq   INTEGER NOT NULL,
    complete      INTEGER NOT NULL DEFAULT 0 CHECK (complete IN (0, 1)),
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (node_id, target_seq)
);

CREATE INDEX IF NOT EXISTS idx_ingest_runs_stale
    ON audit_ingest_runs(complete, updated_at_ms);
