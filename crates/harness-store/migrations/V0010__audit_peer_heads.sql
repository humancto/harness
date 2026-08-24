-- 5.13c-1 (ADR-0041) — peer head pins. What makes 5.13a's chain
-- EVIDENCE rather than a local integrity check.
--
-- A node holds its own DB and its own key, so it can rebuild its chain
-- end to end and it will verify. What it cannot do is un-tell a peer
-- that already pinned `(seq, entry_hash)` — and it cannot rewrite the
-- entries BETWEEN two pins it cannot un-tell, because reproducing the
-- later pinned hash over altered entries is a blake3 collision
-- problem. That second clause is the mechanism; 5.13c-2 supplies the
-- entries it needs.
--
-- APPEND-ONLY, keyed `(node_id, seq)`. The obvious design — one row
-- per node, "higher seq replaces" — deletes the very pin the
-- corroboration check needs: a node that truncates and regrows past
-- the pin then reads as ordinary growth, and the ingest of the lie
-- erases the only evidence of it. Supersession is a verification
-- OBLIGATION, never a delete.
--
-- The PK is also the fork detector. `(node_id, seq, entry_hash)` would
-- store two conflicting histories as two ordinary pins and detect
-- nothing; colliding on `(node_id, seq)` is what surfaces the fork.

INSERT OR REPLACE INTO harness_meta(key, value) VALUES ('schema_version', '10');

CREATE TABLE IF NOT EXISTS audit_peer_heads (
    node_id        BLOB    NOT NULL CHECK (length(node_id) = 16),
    seq            INTEGER NOT NULL CHECK (seq > 0),
    entry_hash     BLOB    NOT NULL CHECK (length(entry_hash) = 32),
    -- The head's own timestamp, chosen by its signer.
    at_ms          INTEGER NOT NULL,
    -- The signature over the AuditHead. This is the evidence: it is
    -- what lets us show a third party that the node said this, so it
    -- is stored, not just checked and discarded.
    sig            BLOB    NOT NULL CHECK (length(sig) = 64),
    -- When WE first saw it and last saw it. Thinning keys on
    -- `first_seen_ms`, never on `seq`: a node that floods 100k entries
    -- must not be able to push honest historical pins into the tail.
    first_seen_ms  INTEGER NOT NULL,
    observed_at_ms INTEGER NOT NULL,
    -- unchecked | corroborated | contradicted | unverifiable.
    -- Thinning is status-aware: a pin that is not unchecked or
    -- corroborated is never evicted, or the eviction sweep becomes a
    -- second route to the deletion this table exists to prevent.
    status         TEXT    NOT NULL DEFAULT 'unchecked',
    PRIMARY KEY (node_id, seq)
);

CREATE INDEX IF NOT EXISTS idx_peer_heads_status
    ON audit_peer_heads(status, first_seen_ms);

-- Two heads at the same position, both validly signed by the same
-- node's key. Both sides are kept in full — the signatures ARE the
-- evidence, and a boolean would throw away the only thing that proves
-- the accusation to anyone else.
CREATE TABLE IF NOT EXISTS audit_head_conflicts (
    id             INTEGER PRIMARY KEY,
    node_id        BLOB    NOT NULL CHECK (length(node_id) = 16),
    seq            INTEGER NOT NULL CHECK (seq > 0),
    held_hash      BLOB    NOT NULL CHECK (length(held_hash) = 32),
    held_at_ms     INTEGER NOT NULL,
    held_sig       BLOB    NOT NULL CHECK (length(held_sig) = 64),
    other_hash     BLOB    NOT NULL CHECK (length(other_hash) = 32),
    other_at_ms    INTEGER NOT NULL,
    other_sig      BLOB    NOT NULL CHECK (length(other_sig) = 64),
    -- Which peer relayed the conflicting head to us. Not the accused:
    -- a head is verified against ITS OWN node's key, so the relayer is
    -- provenance, not blame.
    reported_by    BLOB    NOT NULL CHECK (length(reported_by) = 16),
    detected_at_ms INTEGER NOT NULL,
    UNIQUE(node_id, seq, held_hash, other_hash)
);

CREATE INDEX IF NOT EXISTS idx_head_conflicts_node
    ON audit_head_conflicts(node_id, detected_at_ms DESC);
