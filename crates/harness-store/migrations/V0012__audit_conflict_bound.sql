-- 5.13c-1b-i: bound `audit_head_conflicts` per position.
--
-- That table's UNIQUE key includes `other_hash`, which the SUBJECT
-- mints freely, so one envelope could write a conflict row per head --
-- each costing an ed25519 verify and a write transaction -- and the
-- AUDIT channel has no message rate limit. Conflicts beyond
-- MAX_CONFLICTS_PER_POSITION are REFUSED, never deleted: deleting a
-- conflict row would be ADR-0041 Decision 12 ("a contradicted pin is
-- the evidence") by the back door, and the first rows kept hold both
-- signatures in full, which is the proof.
--
-- The refused ones are COUNTED, per position, so an operator is told
-- that more branches exist rather than being shown a silently partial
-- picture -- and so 5.13c-3's acknowledgement can state that an ack
-- cannot clear an un-enumerable remainder.
CREATE TABLE IF NOT EXISTS audit_conflict_overflow (
    node_id    BLOB    NOT NULL CHECK (length(node_id) = 16),
    seq        INTEGER NOT NULL CHECK (seq > 0),
    -- Distinct `other_hash` values refused at this position.
    suppressed INTEGER NOT NULL DEFAULT 0 CHECK (suppressed >= 0),
    -- Our clock. First and most recent refusal at this position.
    first_ms   INTEGER NOT NULL,
    last_ms    INTEGER NOT NULL,
    PRIMARY KEY (node_id, seq)
);
