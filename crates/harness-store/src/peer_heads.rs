//! 5.13c-1 (ADR-0041) — peer head pins and fork detection.
//!
//! 5.13a's chain detects an edit made by something that is not the
//! daemon. It does not detect the node itself lying: a node holds its
//! own DB and its own key, so it can rebuild its chain end to end and
//! it will verify. What it cannot do is un-tell a peer that already
//! pinned `(seq, entry_hash)`.
//!
//! Everything here exists to create those pins and keep them.

use harness_core::{AuditHead, NodeId, Signature};
use rusqlite::{params, OptionalExtension};

use crate::error::StoreError;
use crate::open::Store;

/// How a pin stands against the entries we have actually seen.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PinStatus {
    /// Recorded, not yet checked against entries. 5.13c-2 resolves
    /// these; until then this is the honest state, and the UI must not
    /// render it as corroboration.
    Unchecked,
    /// Entries were pulled and hash up to this pin.
    Corroborated,
    /// Entries contradict this pin, or a second validly-signed head
    /// exists at this position.
    Contradicted,
    /// The owner pruned through this position before we could check
    /// it. Amber, never green and never red: an honest node that hit
    /// retention looks exactly like this.
    Unverifiable,
}

impl PinStatus {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            PinStatus::Unchecked => "unchecked",
            PinStatus::Corroborated => "corroborated",
            PinStatus::Contradicted => "contradicted",
            PinStatus::Unverifiable => "unverifiable",
        }
    }

    fn from_str(raw: &str) -> Option<Self> {
        match raw {
            "unchecked" => Some(PinStatus::Unchecked),
            "corroborated" => Some(PinStatus::Corroborated),
            "contradicted" => Some(PinStatus::Contradicted),
            "unverifiable" => Some(PinStatus::Unverifiable),
            _ => None,
        }
    }

    /// Thinning may only ever evict these. A `Contradicted` or
    /// `Unverifiable` pin is the record of something worth keeping,
    /// and an age sweep that dropped it would be a second route to the
    /// deletion this table exists to prevent.
    #[cfg_attr(not(test), allow(dead_code))] // pinned against the SQL by p06b
    fn evictable(self) -> bool {
        matches!(self, PinStatus::Unchecked | PinStatus::Corroborated)
    }
}

/// Raw pin columns as read back: `seq`, `entry_hash`, `at_ms`, `sig`,
/// `observed_at_ms`.
type PinRow = (i64, Vec<u8>, i64, Vec<u8>, i64);

/// One stored pin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerHeadPin {
    pub node_id: NodeId,
    pub seq: u64,
    pub entry_hash: [u8; 32],
    pub at_ms: u64,
    pub first_seen_ms: u64,
    pub observed_at_ms: u64,
    pub status: PinStatus,
}

/// What happened when a head was offered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PinOutcome {
    /// A position we had not pinned.
    Pinned,
    /// Same position, same hash — the pin was refreshed.
    Refreshed,
    /// Same position, DIFFERENT hash. Two histories, both signed by
    /// that node's key. Recorded in `audit_head_conflicts` with both
    /// signatures.
    Fork { conflict_id: i64 },
    /// Pinned, but at a position BELOW the newest we hold — so it is
    /// not a regression claim.
    ///
    /// The head is still stored (diff review BLOCKER-1 on #66). An
    /// earlier version returned this WITHOUT inserting, which let one
    /// validly-signed head at `u64::MAX` permanently immunize its
    /// sender: every genuine head afterwards compared below the
    /// poisoned maximum, was discarded, and no peer ever pinned that
    /// node again. Not treating a low head as EVIDENCE (which is
    /// correct — the envelope is not replay-protected, so any peer can
    /// rebroadcast a genuine old head forever) is a different thing
    /// from not STORING it. Storing costs one row and buys a position
    /// a later contradiction can fork against.
    StalePinned,
    /// `seq` exceeds what the store can represent (`i64::MAX`).
    ///
    /// Rejected rather than clamped (diff review MAJOR-3): clamping
    /// maps every oversized seq onto one row, so two distinct signed
    /// heads collide and are recorded as a FORK manufactured by a
    /// lossy cast — and the relayed rebuild carries the clamped seq,
    /// which is not what the signer signed, so it fails verification
    /// at every receiver.
    RejectedSeq,
}

fn to_hash(raw: &[u8]) -> [u8; 32] {
    <[u8; 32]>::try_from(raw).unwrap_or([0u8; 32])
}

fn to_sig(raw: &[u8]) -> Signature {
    Signature::from_bytes(<[u8; 64]>::try_from(raw).unwrap_or([0u8; 64]))
}

impl Store {
    /// Record a peer's signed head.
    ///
    /// The caller MUST have verified `head`'s signature against
    /// `head.node_id`'s own public key — not the relaying peer's.
    /// `reported_by` is provenance for a fork record, never blame.
    ///
    /// Append-only: a higher seq never replaces a lower one. The
    /// obvious "one row per node, higher seq wins" design deletes the
    /// pin the corroboration check needs, so a node that truncates and
    /// regrows past the pin reads as ordinary growth.
    pub fn pin_peer_head(
        &self,
        head: &AuditHead,
        reported_by: NodeId,
        now_ms: u64,
    ) -> Result<PinOutcome, StoreError> {
        // Never clamp a value a signature covers.
        let Ok(seq) = i64::try_from(head.seq) else {
            return Ok(PinOutcome::RejectedSeq);
        };
        self.with_conn(|c| {
            let tx = c.unchecked_transaction()?;
            let existing: Option<(Vec<u8>, i64, Vec<u8>)> = tx
                .query_row(
                    "SELECT entry_hash, at_ms, sig FROM audit_peer_heads
                      WHERE node_id = ?1 AND seq = ?2",
                    params![head.node_id.as_bytes(), seq],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .optional()?;

            let outcome = match existing {
                Some((held_hash, held_at, held_sig)) if to_hash(&held_hash) == head.entry_hash => {
                    tx.execute(
                        "UPDATE audit_peer_heads SET observed_at_ms = ?3
                          WHERE node_id = ?1 AND seq = ?2",
                        params![head.node_id.as_bytes(), seq, i64_ms(now_ms)],
                    )?;
                    let _ = (held_at, held_sig);
                    PinOutcome::Refreshed
                }
                Some((held_hash, held_at, held_sig)) => {
                    // The PK collision IS the fork detector. Keying the
                    // table on the hash too would store both as
                    // ordinary pins and detect nothing.
                    tx.execute(
                        "INSERT OR IGNORE INTO audit_head_conflicts(
                             node_id, seq, held_hash, held_at_ms, held_sig,
                             other_hash, other_at_ms, other_sig,
                             reported_by, detected_at_ms)
                         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
                        params![
                            head.node_id.as_bytes(),
                            seq,
                            held_hash,
                            held_at,
                            held_sig,
                            head.entry_hash.as_slice(),
                            i64_ms(head.at_ms),
                            head.sig.to_bytes().as_slice(),
                            reported_by.as_bytes(),
                            i64_ms(now_ms),
                        ],
                    )?;
                    let conflict_id: i64 = tx.query_row(
                        "SELECT id FROM audit_head_conflicts
                          WHERE node_id = ?1 AND seq = ?2
                            AND held_hash = ?3 AND other_hash = ?4",
                        params![
                            head.node_id.as_bytes(),
                            seq,
                            held_hash,
                            head.entry_hash.as_slice()
                        ],
                        |r| r.get(0),
                    )?;
                    // The held pin is now contradicted, and a
                    // contradicted pin is never evicted by thinning.
                    tx.execute(
                        "UPDATE audit_peer_heads SET status = 'contradicted'
                          WHERE node_id = ?1 AND seq = ?2",
                        params![head.node_id.as_bytes(), seq],
                    )?;
                    PinOutcome::Fork { conflict_id }
                }
                None => {
                    let highest: Option<i64> = tx
                        .query_row(
                            "SELECT MAX(seq) FROM audit_peer_heads WHERE node_id = ?1",
                            params![head.node_id.as_bytes()],
                            |r| r.get(0),
                        )
                        .optional()?
                        .flatten();
                    // ALWAYS insert. Whether this position is below the
                    // newest we hold changes only the CLASSIFICATION
                    // (it is not a regression claim), never whether we
                    // keep the pin — see `PinOutcome::StalePinned`.
                    tx.execute(
                        "INSERT INTO audit_peer_heads(
                             node_id, seq, entry_hash, at_ms, sig,
                             first_seen_ms, observed_at_ms, status)
                         VALUES (?1,?2,?3,?4,?5,?6,?6,'unchecked')",
                        params![
                            head.node_id.as_bytes(),
                            seq,
                            head.entry_hash.as_slice(),
                            i64_ms(head.at_ms),
                            head.sig.to_bytes().as_slice(),
                            i64_ms(now_ms),
                        ],
                    )?;
                    if highest.is_some_and(|h| seq < h) {
                        PinOutcome::StalePinned
                    } else {
                        PinOutcome::Pinned
                    }
                }
            };
            tx.commit()?;
            Ok(outcome)
        })
    }

    /// Every pin we hold for one node, oldest first.
    pub fn peer_head_pins(&self, node: NodeId) -> Result<Vec<PeerHeadPin>, StoreError> {
        self.with_conn(|c| {
            let mut stmt = c.prepare(
                "SELECT node_id, seq, entry_hash, at_ms, first_seen_ms, observed_at_ms, status
                   FROM audit_peer_heads WHERE node_id = ?1 ORDER BY seq ASC",
            )?;
            let rows = stmt
                .query_map(params![node.as_bytes()], |r| {
                    let node_raw: Vec<u8> = r.get(0)?;
                    let hash: Vec<u8> = r.get(2)?;
                    let status: String = r.get(6)?;
                    Ok(PeerHeadPin {
                        node_id: NodeId::from_bytes(
                            <[u8; 16]>::try_from(node_raw.as_slice()).unwrap_or([0u8; 16]),
                        ),
                        seq: u64::try_from(r.get::<_, i64>(1)?).unwrap_or(0),
                        entry_hash: to_hash(&hash),
                        at_ms: u64::try_from(r.get::<_, i64>(3)?).unwrap_or(0),
                        first_seen_ms: u64::try_from(r.get::<_, i64>(4)?).unwrap_or(0),
                        observed_at_ms: u64::try_from(r.get::<_, i64>(5)?).unwrap_or(0),
                        status: PinStatus::from_str(&status).unwrap_or(PinStatus::Unchecked),
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        })
    }

    /// Recorded forks, newest first.
    pub fn head_conflicts(&self, limit: usize) -> Result<Vec<HeadConflict>, StoreError> {
        self.with_conn(|c| {
            let mut stmt = c.prepare(
                "SELECT node_id, seq, held_hash, held_sig, other_hash, other_sig,
                        reported_by, detected_at_ms
                   FROM audit_head_conflicts ORDER BY detected_at_ms DESC LIMIT ?1",
            )?;
            let rows = stmt
                .query_map(params![i64::try_from(limit).unwrap_or(i64::MAX)], |r| {
                    let node_raw: Vec<u8> = r.get(0)?;
                    let by_raw: Vec<u8> = r.get(6)?;
                    let held_hash: Vec<u8> = r.get(2)?;
                    let held_sig: Vec<u8> = r.get(3)?;
                    let other_hash: Vec<u8> = r.get(4)?;
                    let other_sig: Vec<u8> = r.get(5)?;
                    Ok(HeadConflict {
                        node_id: NodeId::from_bytes(
                            <[u8; 16]>::try_from(node_raw.as_slice()).unwrap_or([0u8; 16]),
                        ),
                        seq: u64::try_from(r.get::<_, i64>(1)?).unwrap_or(0),
                        held_hash: to_hash(&held_hash),
                        held_sig: to_sig(&held_sig),
                        other_hash: to_hash(&other_hash),
                        other_sig: to_sig(&other_sig),
                        reported_by: NodeId::from_bytes(
                            <[u8; 16]>::try_from(by_raw.as_slice()).unwrap_or([0u8; 16]),
                        ),
                        detected_at_ms: u64::try_from(r.get::<_, i64>(7)?).unwrap_or(0),
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        })
    }

    /// Bound the pin table without losing the pins that matter.
    ///
    /// Keeps the newest `keep_recent` evictable pins per node and
    /// thins the rest to one per `ladder_ms` bucket. Two rules make
    /// this safe:
    ///
    /// - Only `Unchecked`/`Corroborated` pins are ever evicted. A
    ///   `Contradicted` pin is the evidence; an age sweep that dropped
    ///   it would reintroduce, by the back door, the deletion the
    ///   append-only design exists to prevent.
    /// - Thinning keys on `first_seen_ms`, never on `seq`. A node that
    ///   floods 100k entries would otherwise push every honest
    ///   historical pin into the thinned tail.
    /// - Ties on `first_seen_ms` break by `rowid` (Codex P1 on #66).
    ///   Every head in one envelope is pinned at the same receiver-side
    ///   instant, so without a tie-breaker the "newest N" count sees
    ///   zero newer rows for ALL of them and exempts every one — a
    ///   peer sending 64 signed heads per envelope would defeat the
    ///   bound outright.
    ///
    /// The OLDEST pin per node is always kept — it anchors the
    /// furthest back we can prove anything about.
    pub fn thin_peer_head_pins(
        &self,
        keep_recent: usize,
        ladder_ms: u64,
    ) -> Result<usize, StoreError> {
        let keep = i64::try_from(keep_recent).unwrap_or(i64::MAX);
        let bucket = i64::try_from(ladder_ms.max(1)).unwrap_or(i64::MAX);
        self.with_conn(|c| {
            let tx = c.unchecked_transaction()?;
            let removed = tx.execute(
                "DELETE FROM audit_peer_heads
                  WHERE status IN ('unchecked','corroborated')
                    AND rowid NOT IN (
                        -- newest N EVICTABLE pins per node. Filtered on
                        -- status so a burst of manufactured forks
                        -- cannot consume the recent window and
                        -- accelerate eviction of honest pins (diff
                        -- review, thinning wrinkle).
                        SELECT rowid FROM audit_peer_heads a
                         WHERE a.status IN ('unchecked','corroborated')
                           AND (SELECT COUNT(*) FROM audit_peer_heads b
                                 WHERE b.node_id = a.node_id
                                   AND b.status IN ('unchecked','corroborated')
                                   AND (b.first_seen_ms > a.first_seen_ms
                                        OR (b.first_seen_ms = a.first_seen_ms
                                            AND b.rowid > a.rowid))) < ?1
                    )
                    AND rowid NOT IN (
                        -- one survivor per time bucket per node
                        SELECT MIN(rowid) FROM audit_peer_heads
                         GROUP BY node_id, first_seen_ms / ?2
                    )
                    AND rowid NOT IN (
                        -- the oldest pin per node, always
                        SELECT MIN(rowid) FROM audit_peer_heads GROUP BY node_id
                    )",
                params![keep, bucket],
            )?;
            tx.commit()?;
            Ok(removed)
        })
    }

    /// The newest pin we hold for a node, rebuilt as a signed
    /// [`AuditHead`] so it can be RELAYED.
    ///
    /// Relaying is what lets a pin outlive its subject: the head
    /// carries its own signature, so node C can learn A's head from B
    /// and still verify it against A's key. We are not re-signing
    /// anything — the stored signature is A's, returned verbatim.
    pub fn newest_pin_as_head(&self, node: NodeId) -> Result<Option<(AuditHead, u64)>, StoreError> {
        self.with_conn(|c| {
            let row: Option<PinRow> = c
                .query_row(
                    "SELECT seq, entry_hash, at_ms, sig, observed_at_ms
                       FROM audit_peer_heads
                      WHERE node_id = ?1 ORDER BY seq DESC LIMIT 1",
                    params![node.as_bytes()],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
                )
                .optional()?;
            Ok(row.map(|(seq, hash, at_ms, sig, observed)| {
                (
                    AuditHead {
                        node_id: node,
                        seq: u64::try_from(seq).unwrap_or(0),
                        entry_hash: to_hash(&hash),
                        at_ms: u64::try_from(at_ms).unwrap_or(0),
                        sig: to_sig(&sig),
                    },
                    // OUR clock, for relay ordering. `at_ms` is chosen
                    // by the head's signer, so ranking relays by it
                    // lets a node stamp `u64::MAX` and hold a relay
                    // slot forever (diff review MAJOR-6).
                    u64::try_from(observed).unwrap_or(0),
                )
            }))
        })
    }

    /// Mark a pin's status (5.13c-2 drives this from entry pulls).
    pub fn set_pin_status(
        &self,
        node: NodeId,
        seq: u64,
        status: PinStatus,
    ) -> Result<bool, StoreError> {
        let seq = i64::try_from(seq).unwrap_or(i64::MAX);
        self.with_conn(|c| {
            let n = c.execute(
                "UPDATE audit_peer_heads SET status = ?3 WHERE node_id = ?1 AND seq = ?2",
                params![node.as_bytes(), seq, status.as_str()],
            )?;
            Ok(n > 0)
        })
    }
}

/// A recorded fork, both sides retained.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadConflict {
    pub node_id: NodeId,
    pub seq: u64,
    pub held_hash: [u8; 32],
    pub held_sig: Signature,
    pub other_hash: [u8; 32],
    pub other_sig: Signature,
    pub reported_by: NodeId,
    pub detected_at_ms: u64,
}

fn i64_ms(ms: u64) -> i64 {
    i64::try_from(ms).unwrap_or(i64::MAX)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use harness_core::Signable;

    fn node(n: u8) -> NodeId {
        NodeId::from_bytes([n; 16])
    }

    fn head(n: u8, seq: u64, hash: u8, at_ms: u64) -> AuditHead {
        AuditHead {
            node_id: node(n),
            seq,
            entry_hash: [hash; 32],
            at_ms,
            sig: Signature::from_bytes([hash; 64]),
        }
    }

    #[test]
    fn p01_a_new_position_pins_and_a_repeat_refreshes() {
        let s = Store::open_memory().expect("store");
        assert_eq!(
            s.pin_peer_head(&head(1, 10, 0xAA, 100), node(2), 1_000)
                .expect("pin"),
            PinOutcome::Pinned
        );
        assert_eq!(
            s.pin_peer_head(&head(1, 10, 0xAA, 100), node(2), 2_000)
                .expect("pin"),
            PinOutcome::Refreshed
        );
        let pins = s.peer_head_pins(node(1)).expect("pins");
        assert_eq!(pins.len(), 1, "a refresh is not a second pin");
        assert_eq!(pins[0].first_seen_ms, 1_000);
        assert_eq!(pins[0].observed_at_ms, 2_000, "refresh moved last-seen");
        assert_eq!(pins[0].status, PinStatus::Unchecked);
    }

    #[test]
    fn p02_growth_is_append_only_so_earlier_pins_survive() {
        // The whole mechanism. "Higher seq replaces" would leave one
        // pin — the current one — which corroborates nothing, and a
        // truncate-and-regrow would erase the evidence of itself.
        let s = Store::open_memory().expect("store");
        for (seq, hash) in [(10u64, 0xAAu8), (20, 0xBB), (30, 0xCC)] {
            s.pin_peer_head(&head(1, seq, hash, seq * 10), node(2), seq * 100)
                .expect("pin");
        }
        let pins = s.peer_head_pins(node(1)).expect("pins");
        assert_eq!(pins.len(), 3);
        assert_eq!(pins[0].seq, 10, "the oldest pin is still there");
        assert_eq!(pins[0].entry_hash, [0xAA; 32]);
    }

    #[test]
    fn p03_a_same_seq_fork_is_caught_and_both_signatures_kept() {
        let s = Store::open_memory().expect("store");
        s.pin_peer_head(&head(1, 10, 0xAA, 100), node(2), 1_000)
            .expect("pin");
        // A second, differently-signed history at the same position.
        let outcome = s
            .pin_peer_head(&head(1, 10, 0xDD, 900), node(3), 2_000)
            .expect("pin");
        assert!(matches!(outcome, PinOutcome::Fork { .. }));

        let conflicts = s.head_conflicts(10).expect("conflicts");
        assert_eq!(conflicts.len(), 1);
        let c = &conflicts[0];
        assert_eq!(c.node_id, node(1));
        assert_eq!(c.seq, 10);
        assert_eq!(c.held_hash, [0xAA; 32]);
        assert_eq!(c.other_hash, [0xDD; 32]);
        assert_eq!(
            c.held_sig.to_bytes(),
            [0xAA; 64],
            "both signatures retained — they ARE the evidence"
        );
        assert_eq!(c.other_sig.to_bytes(), [0xDD; 64]);
        assert_eq!(c.reported_by, node(3), "provenance, not blame");

        let pins = s.peer_head_pins(node(1)).expect("pins");
        assert_eq!(pins[0].status, PinStatus::Contradicted);
    }

    #[test]
    fn p03b_a_repeated_fork_report_does_not_pile_up() {
        let s = Store::open_memory().expect("store");
        s.pin_peer_head(&head(1, 10, 0xAA, 100), node(2), 1_000)
            .expect("pin");
        for t in 0..5 {
            s.pin_peer_head(&head(1, 10, 0xDD, 900), node(3), 2_000 + t)
                .expect("pin");
        }
        assert_eq!(s.head_conflicts(10).expect("conflicts").len(), 1);
    }

    #[test]
    fn p04_a_replayed_older_head_is_ignored_not_treated_as_regression() {
        // The envelope carrying heads is not replay-protected, so any
        // peer can rebroadcast a genuine old head forever. If "lower
        // seq than a pin we hold" counted as evidence, that would be a
        // one-packet permanent defamation of an honest node — and a
        // node returning from a partition would trip it by accident.
        let s = Store::open_memory().expect("store");
        s.pin_peer_head(&head(1, 30, 0xCC, 300), node(2), 3_000)
            .expect("pin");
        let outcome = s
            .pin_peer_head(&head(1, 10, 0xAA, 100), node(9), 4_000)
            .expect("pin");
        assert_eq!(outcome, PinOutcome::StalePinned);
        assert!(
            s.head_conflicts(10).expect("conflicts").is_empty(),
            "a stale relay is not an accusation"
        );
        // It IS stored, though: classification and storage are
        // separate decisions (BLOCKER-1). Discarding it let one huge
        // signed seq immunize a node forever.
        let pins = s.peer_head_pins(node(1)).expect("pins");
        assert_eq!(pins.len(), 2);
        assert!(pins.iter().all(|p| p.status == PinStatus::Unchecked));
    }

    #[test]
    fn p04b_but_a_contradiction_at_a_held_seq_still_counts() {
        // Lower-than-newest is not automatically ignorable: if it
        // contradicts a position we actually hold, that is a fork.
        let s = Store::open_memory().expect("store");
        s.pin_peer_head(&head(1, 10, 0xAA, 100), node(2), 1_000)
            .expect("pin");
        s.pin_peer_head(&head(1, 30, 0xCC, 300), node(2), 3_000)
            .expect("pin");
        let outcome = s
            .pin_peer_head(&head(1, 10, 0xEE, 100), node(3), 4_000)
            .expect("pin");
        assert!(
            matches!(outcome, PinOutcome::Fork { .. }),
            "an old position we hold, with a different hash, is evidence"
        );
    }

    #[test]
    fn p05_chains_are_per_node() {
        let s = Store::open_memory().expect("store");
        s.pin_peer_head(&head(1, 10, 0xAA, 100), node(9), 1_000)
            .expect("pin");
        s.pin_peer_head(&head(2, 10, 0xBB, 100), node(9), 1_000)
            .expect("pin");
        assert_eq!(s.peer_head_pins(node(1)).expect("pins").len(), 1);
        assert_eq!(s.peer_head_pins(node(2)).expect("pins").len(), 1);
        assert!(s.head_conflicts(10).expect("conflicts").is_empty());
    }

    #[test]
    fn p06_thinning_never_evicts_evidence_and_keeps_the_oldest() {
        let s = Store::open_memory().expect("store");
        // 40 pins one hour apart, so every one lands in its own bucket
        // and only `keep_recent` + bucket survivors would otherwise
        // remain.
        for i in 1..=40u64 {
            s.pin_peer_head(
                &head(1, i, u8::try_from(i).unwrap_or(0), i),
                node(2),
                i * 3_600_000,
            )
            .expect("pin");
        }
        // One contradicted and one unverifiable, deep in the tail.
        s.set_pin_status(node(1), 5, PinStatus::Contradicted)
            .expect("status");
        s.set_pin_status(node(1), 7, PinStatus::Unverifiable)
            .expect("status");

        s.thin_peer_head_pins(5, 24 * 3_600_000).expect("thin");
        let pins = s.peer_head_pins(node(1)).expect("pins");
        let seqs: Vec<u64> = pins.iter().map(|p| p.seq).collect();

        assert!(seqs.len() < 40, "thinning did something");
        assert!(seqs.contains(&5), "a contradicted pin is never evicted");
        assert!(seqs.contains(&7), "an unverifiable pin is never evicted");
        assert!(seqs.contains(&1), "the oldest pin anchors the range");
        for recent in 36..=40 {
            assert!(seqs.contains(&recent), "recent pin {recent} kept");
        }
    }

    #[test]
    fn p06b_evictability_matches_what_thinning_actually_does() {
        // The SQL hardcodes the evictable statuses; `PinStatus::
        // evictable` states them in Rust. This test is what keeps the
        // two from drifting apart.
        for status in [
            PinStatus::Unchecked,
            PinStatus::Corroborated,
            PinStatus::Contradicted,
            PinStatus::Unverifiable,
        ] {
            let s = Store::open_memory().expect("store");
            // Three pins in one bucket: the oldest is always kept and
            // the newest is within keep_recent, so the middle one is
            // the only eviction candidate.
            for i in 1..=3u64 {
                s.pin_peer_head(&head(1, i, u8::try_from(i).unwrap_or(0), i), node(2), i)
                    .expect("pin");
            }
            s.set_pin_status(node(1), 2, status).expect("status");
            s.thin_peer_head_pins(1, 1_000_000).expect("thin");
            let survived = s
                .peer_head_pins(node(1))
                .expect("pins")
                .iter()
                .any(|p| p.seq == 2);
            assert_eq!(
                survived,
                !status.evictable(),
                "{} evictability disagrees with the thinning SQL",
                status.as_str()
            );
        }
    }

    #[test]
    fn p07_a_signed_head_round_trips_through_a_pin() {
        // End to end against a real identity: sign, verify, pin, and
        // confirm what came back out is what was signed.
        let tmp = tempfile::tempdir().expect("tmp");
        let identity = harness_core::Identity::generate();
        let store = Store::open_memory().expect("store");
        let _ = tmp;

        let mut h = AuditHead {
            node_id: identity.node_id(),
            seq: 42,
            entry_hash: [0x11; 32],
            at_ms: 5_000,
            sig: Signature::from_bytes([0u8; 64]),
        };
        h.sign(&identity).expect("sign");
        assert!(
            h.verify_signature(identity.public_key()).is_ok(),
            "self-verifies"
        );

        store.pin_peer_head(&h, node(2), 6_000).expect("pin");
        let pins = store.peer_head_pins(identity.node_id()).expect("pins");
        assert_eq!(pins.len(), 1);
        assert_eq!(pins[0].seq, 42);
        assert_eq!(pins[0].entry_hash, [0x11; 32]);

        // A mutated head at the same position is a fork, and the
        // stored signature is still the original.
        let mut forged = h.clone();
        forged.entry_hash = [0x22; 32];
        forged.sign(&identity).expect("sign");
        assert!(matches!(
            store.pin_peer_head(&forged, node(3), 7_000).expect("pin"),
            PinOutcome::Fork { .. }
        ));
        let c = &store.head_conflicts(1).expect("conflicts")[0];
        assert_eq!(c.held_sig.to_bytes(), h.sig.to_bytes());
        assert_eq!(c.other_sig.to_bytes(), forged.sig.to_bytes());
    }

    #[test]
    fn p06c_pins_sharing_one_instant_are_still_thinnable() {
        // Codex P1 on #66: every head in one envelope is pinned at the
        // same receiver-side `now_ms`. Without a tie-breaker the
        // "newest N" correlated count sees zero newer rows for all of
        // them, exempts every one, and a peer sending 64 signed heads
        // per envelope defeats the bound outright.
        let s = Store::open_memory().expect("store");
        for seq in 1..=40u64 {
            s.pin_peer_head(
                &head(1, seq, u8::try_from(seq).unwrap_or(0), seq),
                node(2),
                // One instant for all of them.
                5_000,
            )
            .expect("pin");
        }
        assert_eq!(s.peer_head_pins(node(1)).expect("pins").len(), 40);

        s.thin_peer_head_pins(5, 60_000).expect("thin");
        let after = s.peer_head_pins(node(1)).expect("pins");
        assert!(
            after.len() < 40,
            "tied pins must still be thinnable, kept {}",
            after.len()
        );
        // The bound is honored: newest 5 + one bucket survivor + the
        // oldest, with heavy overlap between those sets.
        assert!(after.len() <= 7, "kept {} pins", after.len());
        assert_eq!(after[0].seq, 1, "the oldest pin still anchors");
    }
}
