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

    /// Thinning may only ever evict `Unchecked`. Every other status is
    /// a RESULT — something we spent a chain walk to establish — and an
    /// age sweep that dropped it would be a second route to the
    /// deletion this table exists to prevent.
    ///
    /// `Corroborated` used to be evictable, which round 7 showed was
    /// exploitable: `MAX_HEADS_PER_ENVELOPE` is 64 and
    /// `PIN_KEEP_RECENT` is also 64, so ONE 64-head envelope fills a
    /// node's entire recent-pin window, and the next hourly sweep then
    /// deleted its honest corroborated pins — keeping only the
    /// `MIN(rowid)` row per node. The verdicts were unrecoverable:
    /// `audit_run_corroborates` reads the pin, so deleting the pin
    /// destroys the corroboration, and only the subject can mint a
    /// replacement. It also defeated both of Decision 25's
    /// mitigations at once, since `?seq=` cannot name a row that has
    /// been deleted.
    ///
    /// Cost of keeping them: corroborated pins now accumulate. That is
    /// bounded in practice because a corroborated pin requires a
    /// successful walk over entries the subject actually served — it
    /// is expensive for an attacker to create and cheap for an honest
    /// peer — but a cap on corroborated pins belongs in 5.13c-3's
    /// hygiene pass alongside `audit_ingest_runs` thinning.
    #[cfg_attr(not(test), allow(dead_code))] // pinned against the SQL by p06b
    fn evictable(self) -> bool {
        matches!(self, PinStatus::Unchecked)
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
    /// `seq` is outside what the store can represent: 0, which
    /// `CHECK (seq > 0)` refuses, or above `i64::MAX`.
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
    /// Record a batch of signed heads in ONE transaction.
    ///
    /// The caller MUST have verified every `head`'s signature against
    /// `head.node_id`'s own public key — not the relaying peer's.
    /// `reported_by` is provenance for a fork record, never blame.
    ///
    /// Append-only: a higher seq never replaces a lower one. The
    /// obvious "one row per node, higher seq wins" design deletes the
    /// pin the corroboration check needs, so a node that truncates and
    /// regrows past the pin reads as ordinary growth.
    ///
    /// One lock acquisition and one transaction for the whole
    /// envelope, where the per-head path cost up to
    /// `MAX_HEADS_PER_ENVELOPE` of each.
    ///
    /// No per-peer allowance is applied here, and that is deliberate
    /// rather than pending: two spend orders were implemented and
    /// withdrawn because each one let the sender pick who got refused.
    /// ADR-0041 Decision 28 records both and why an allowance without
    /// a sender-independent order is worse than none. 5.13c-1b-ii
    /// carries the round-robin order that replaces them.
    ///
    /// Outcomes are returned per input head, in input order.
    ///
    /// # Errors
    /// Underlying sqlite errors.
    pub fn pin_peer_heads(
        &self,
        heads: &[AuditHead],
        reported_by: NodeId,
        now_ms: u64,
    ) -> Result<Vec<PinOutcome>, StoreError> {
        self.with_conn(|c| {
            let tx = c.unchecked_transaction()?;
            let mut out = Vec::with_capacity(heads.len());
            for head in heads {
                // An earlier head in this same batch may have
                // inserted the position this one names, in which case
                // the re-read inside `pin_one` sees it and classifies
                // this one as the same-envelope fork it is.
                out.push(pin_one(&tx, head, reported_by, now_ms)?);
            }
            tx.commit()?;
            Ok(out)
        })
    }

    /// Record one signed head. [`Self::pin_peer_heads`] with a
    /// single-element slice, kept because most callers and tests have
    /// exactly one head in hand.
    ///
    /// # Errors
    /// Underlying sqlite errors.
    pub fn pin_peer_head(
        &self,
        head: &AuditHead,
        reported_by: NodeId,
        now_ms: u64,
    ) -> Result<PinOutcome, StoreError> {
        self.with_conn(|c| {
            let tx = c.unchecked_transaction()?;
            let outcome = pin_one(&tx, head, reported_by, now_ms)?;
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
    /// - Only `Unchecked` pins are ever evicted. Every other status
    ///   is a RESULT — something a chain walk established — and an
    ///   age sweep that dropped one would reintroduce, by the back
    ///   door, the deletion the append-only design exists to prevent.
    ///   `Corroborated` was evictable until round 8 of #67 showed a
    ///   head flood plus the ordinary sweep deleting it.
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
                  WHERE status = 'unchecked'
                    AND rowid NOT IN (
                        -- newest N EVICTABLE pins per node. Filtered on
                        -- status so a burst of manufactured forks
                        -- cannot consume the recent window and
                        -- accelerate eviction of honest pins (diff
                        -- review, thinning wrinkle).
                        --
                        -- `b.status` is the load-bearing half: it
                        -- counts how many newer pins sit ABOVE this
                        -- one. Count non-evictable rows there and,
                        -- once a node has `keep_recent` permanent
                        -- pins — which `corroborated` now is — every
                        -- `unchecked` pin scores outside the window
                        -- and becomes evictable at once. `p06e` pins
                        -- it (round 8 M7, which survived with the
                        -- whole suite green).
                        --
                        -- `a.status` is a redundant narrowing, kept
                        -- for symmetry and cheapness: the outer DELETE
                        -- already touches only `unchecked`, so adding
                        -- permanent rowids to this exemption list
                        -- cannot change any outcome. It has no test
                        -- because widening it is a genuine no-op, not
                        -- because nobody looked (round 8 M6).
                        SELECT rowid FROM audit_peer_heads a
                         WHERE a.status = 'unchecked'
                           AND (SELECT COUNT(*) FROM audit_peer_heads b
                                 WHERE b.node_id = a.node_id
                                   AND b.status = 'unchecked'
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

    /// The pin to verify next for one node: the one we observed most
    /// recently, by OUR clock.
    ///
    /// Never `MAX(seq)`. `seq` is signed by the subject and
    /// [`PinOutcome::RejectedSeq`] only refuses values above
    /// `i64::MAX`, so a head signed at exactly `i64::MAX` is pinned
    /// and would be the highest-seq pin for that node forever —
    /// permanently aiming the verify endpoint at a position the honest
    /// chain will never reach, so `reached_pin` never fires and no
    /// honest pin for that node is ever corroborated. One validly
    /// signed garbage head would let a subject decide whether it can
    /// be audited at all. `newest_pin_as_head` already orders this way
    /// for the relay path; this is the same rule for the verify path
    /// (diff review round 4 BLOCKER-2).
    ///
    /// Order on `first_seen_ms DESC, seq ASC` — when we FIRST saw
    /// that position, then the lower claim.
    ///
    /// Three attempts at this got it wrong, each time by keying on
    /// something the subject controls:
    ///
    /// - Round 4: `MAX(seq)`. A head signed at `i64::MAX` owned the
    ///   slot forever.
    /// - Round 5: `observed_at_ms DESC, seq DESC`. Every head in one
    ///   envelope is pinned with the same `now_ms`, so the subject
    ///   manufactured a tie and `seq DESC` resolved it in its favour.
    /// - Round 5 again, `seq ASC` plus an unchecked-first clause. The
    ///   envelope is deliberately NOT replay-protected, so the subject
    ///   re-pushes a genuine OLD head and `Refreshed` re-stamps
    ///   `observed_at_ms` on that row: our clock stamps it, but the
    ///   subject chooses WHAT gets stamped. And `Contradicted` is also
    ///   `<> 'unchecked'`, so a subject that self-forked its own
    ///   newest position handed the slot back to an older, lower pin —
    ///   the clause was a regression, so it is gone.
    ///
    /// `first_seen_ms` is stamped once on INSERT and never touched by
    /// the `Refreshed` branch, so a replayed old head keeps its
    /// original value and cannot re-win.
    ///
    /// The tie breaks on `seq ASC`, not `rowid`. `rowid` looks like
    /// our own value, but within ONE envelope it is the sender's `Vec`
    /// order — so `rowid DESC` just hands the slot to whichever head
    /// the sender listed last, which `p09` demonstrates. `seq` is the
    /// subject's field either way, but ASC is the direction that
    /// cannot be inflated: to win a tie you must claim a LOWER,
    /// therefore reachable, position.
    ///
    /// # What this still does NOT give you
    ///
    /// A subject can mint a fresh signed position whenever it likes,
    /// and any "newest" rule selects it. So a subject determined to
    /// keep its honest head out of this one slot can do so by pushing
    /// a new inflated head every tick. That is not a tie-break bug —
    /// it is inherent to choosing ONE target from candidates the
    /// subject generates, and no ordering fixes it.
    ///
    /// Two things address it instead: an operator can name the target
    /// explicitly (`?seq=` on the verify endpoint), and 5.13c-3a's
    /// eager sweep walks several unchecked pins per tick rather than
    /// one. This function is the default for the manual path, not a
    /// defence.
    pub fn newest_pin_by_observation(
        &self,
        node: NodeId,
    ) -> Result<Option<PeerHeadPin>, StoreError> {
        self.with_conn(|c| {
            let row: Option<(i64, Vec<u8>, i64, i64, i64, String)> = c
                .query_row(
                    "SELECT seq, entry_hash, at_ms, first_seen_ms, observed_at_ms, status
                       FROM audit_peer_heads
                      WHERE node_id = ?1
                      ORDER BY first_seen_ms DESC, seq ASC LIMIT 1",
                    params![node.as_bytes()],
                    |r| {
                        Ok((
                            r.get(0)?,
                            r.get(1)?,
                            r.get(2)?,
                            r.get(3)?,
                            r.get(4)?,
                            r.get(5)?,
                        ))
                    },
                )
                .optional()?;
            Ok(row.map(
                |(seq, hash, at_ms, first_seen, observed, status)| PeerHeadPin {
                    node_id: node,
                    seq: u64::try_from(seq).unwrap_or(0),
                    entry_hash: to_hash(&hash),
                    at_ms: u64::try_from(at_ms).unwrap_or(0),
                    first_seen_ms: u64::try_from(first_seen).unwrap_or(0),
                    observed_at_ms: u64::try_from(observed).unwrap_or(0),
                    status: PinStatus::from_str(&status).unwrap_or(PinStatus::Unchecked),
                },
            ))
        })
    }

    /// The newest pin we hold for a node, rebuilt as a signed
    /// [`AuditHead`] so it can be RELAYED.
    ///
    /// Relaying is what lets a pin outlive its subject: the head
    /// carries its own signature, so node C can learn A's head from B
    /// and still verify it against A's key. We are not re-signing
    /// anything — the stored signature is A's, returned verbatim.
    ///
    /// Ordered `first_seen_ms DESC, seq DESC`, which is NOT quite the
    /// verify selector's rule and the difference is deliberate.
    ///
    /// `first_seen_ms` for the same reason: it is written once on
    /// INSERT and never touched by the `Refreshed` branch, so a peer
    /// re-pushing a head we already hold cannot re-take the slot.
    /// `main` ordered on `observed_at_ms`, which that branch DOES
    /// re-stamp, so this is the one improvement kept from the
    /// relay-selection work.
    ///
    /// But `seq DESC`, not the verify path's `seq ASC`: for a verify
    /// TARGET a lower claim is at least walkable, so preferring it is
    /// safe. For RELAY a lower position is simply worse for everyone —
    /// round 7 measured a trusted peer forwarding a genuine OLD head
    /// of a third party and owning the position the mesh learned for
    /// it, which `seq ASC` actively helped.
    ///
    /// This is still ONE slot, and one slot is still capturable by a
    /// peer that introduces a previously-unseen position. Relaying a
    /// bounded SET is the fix, and it is ROADMAP 5.13c-1b rather than
    /// this item: the first attempt at it selected the set by
    /// `first_seen_ms` too, so three unseen old heads took all three
    /// slots and the honest head was evicted — a worse outcome than
    /// one slot, shipped as a fix for it. `m25` pins this ordering;
    /// `p09` covers the verify selector.
    pub fn newest_pin_as_head(&self, node: NodeId) -> Result<Option<(AuditHead, u64)>, StoreError> {
        self.with_conn(|c| {
            let row: Option<PinRow> = c
                .query_row(
                    // Ordered by OUR clock first (diff review
                    // re-review): `seq` is as attacker-chosen as
                    // `at_ms` was, and `RejectedSeq` only stops values
                    // above i64::MAX — so a head signed at exactly
                    // i64::MAX would otherwise be selected forever and
                    // every honest peer would relay that garbage for
                    // the subject permanently.
                    "SELECT seq, entry_hash, at_ms, sig, observed_at_ms
                       FROM audit_peer_heads
                      WHERE node_id = ?1
                      ORDER BY first_seen_ms DESC, seq DESC LIMIT 1",
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
            // A CONTRADICTED pin never moves back (diff review
            // BLOCKER on #67). Two validly-signed heads at one
            // position is the strongest evidence this feature
            // produces, and an unconditional UPDATE let a later walk
            // of whichever side we happen to hold repaint it green.
            //
            // Not merely a display bug: thinning evicts `unchecked`
            // pins, so a repainted pin becomes sweepable — Decision
            // 12's rule ("a contradicted pin is the evidence")
            // defeated through the back door. (`settle_pin_from_run`
            // can still move a pin `Corroborated -> Unchecked` and so
            // make an evidence-bearing row evictable again; that is
            // reachable only by local tampering, never by the subject,
            // and is recorded in the ADR's limits.)
            //
            // `Unverifiable` is deliberately NOT sticky (diff review
            // MAJOR-5): ingest validates rows against the SUBJECT's
            // hashes and cares only that they are not our own chain,
            // so a third node that retained a range the owner pruned
            // can serve it and legitimately corroborate. That is what
            // relay is for. Only `contradicted` is terminal.
            let n = c.execute(
                "UPDATE audit_peer_heads SET status = ?3
                  WHERE node_id = ?1 AND seq = ?2
                    AND status <> 'contradicted'",
                params![node.as_bytes(), seq, status.as_str()],
            )?;
            Ok(n > 0)
        })
    }

    /// The stored status of one pin, if we hold it.
    ///
    /// # Errors
    /// Underlying sqlite errors.
    pub fn pin_status(&self, node: NodeId, seq: u64) -> Result<Option<PinStatus>, StoreError> {
        let seq = i64::try_from(seq).unwrap_or(i64::MAX);
        self.with_conn(|c| {
            let raw: Option<String> = c
                .query_row(
                    "SELECT status FROM audit_peer_heads WHERE node_id = ?1 AND seq = ?2",
                    params![node.as_bytes(), seq],
                    |r| r.get(0),
                )
                .optional()?;
            Ok(raw.map(|s| PinStatus::from_str(&s).unwrap_or(PinStatus::Unchecked)))
        })
    }

    /// Force a pin's status, including out of `contradicted`.
    ///
    /// The ONLY intended caller is 5.13c-3's operator
    /// acknowledgement — the one act that may legitimately retire a
    /// fork verdict, and which records itself in the local chain so
    /// the retirement is itself evidence.
    ///
    /// Fork detection does NOT use this and cannot (diff review
    /// MAJOR-3): its `status = 'contradicted'` UPDATE runs inside
    /// `pin_peer_head`'s own `with_conn` closure, and `with_conn`
    /// takes the single process-wide mutex non-reentrantly, so
    /// calling this from there would deadlock. An earlier version of
    /// this doc named it as a caller, which was wrong.
    ///
    /// Ordinary settle paths use [`Self::set_pin_status`], which
    /// refuses to leave `contradicted`.
    ///
    /// # Errors
    /// Underlying sqlite errors.
    pub fn force_pin_status(
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

/// One head, inside a transaction the caller owns.
///
/// Takes `&Transaction` rather than `&Store` so a whole envelope can
/// run in one `with_conn`. It must never reach back into
/// `Store::with_conn`: that holds a single process-wide, NON-reentrant
/// `parking_lot::Mutex`, which blocks forever rather than panicking.
/// ADR-0041 Decision 23 records the same hazard for
/// `force_pin_status`.
fn pin_one(
    tx: &rusqlite::Transaction<'_>,
    head: &AuditHead,
    reported_by: NodeId,
    now_ms: u64,
) -> Result<PinOutcome, StoreError> {
    // Never clamp a value a signature covers.
    let Ok(seq) = i64::try_from(head.seq) else {
        return Ok(PinOutcome::RejectedSeq);
    };
    // `audit_peer_heads` and `audit_head_conflicts` have both carried
    // CHECK (seq > 0) since V0010, and a whole envelope now shares ONE
    // transaction. Letting seq 0 reach the INSERT raises a constraint
    // violation, which abandons that transaction and rolls back every
    // honest head beside it — so a single self-signed seq-0 head,
    // which any paired peer can mint for its own node_id, voids the
    // entire envelope including heads for subjects it is the only path
    // to. On the per-head path that cost one head; batched it costs
    // all MAX_HEADS_PER_ENVELOPE of them, in every window, forever.
    //
    // Rejected at both ends of the range for the same reason the upper
    // end is: the store cannot hold it, and clamping would forge a seq
    // its signer never signed.
    if seq == 0 {
        return Ok(PinOutcome::RejectedSeq);
    }
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
    Ok(outcome)
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
    #[test]
    fn p09_the_verify_target_does_not_break_a_tie_on_an_inflated_seq() {
        // Round 6 MINOR-9 / BLOCKER-1, re-tested in round 7. Every
        // head in one envelope is
        // pinned at the same instant, so the tie-break decides, and
        // `seq` is the SUBJECT's field — a `seq` tie-break let a
        // subject choose both its own verify target and the head every
        // honest peer relays for it. Both selectors key on our own
        // insert order instead.
        let s = Store::open_memory().expect("store");
        let absurd = u64::try_from(i64::MAX).expect("fits");
        // Honest first, inflated second, SAME instant — so insert
        // order favours the attacker and only the key can save us.
        s.pin_peer_head(&head(1, 9, 0x09, 9), node(2), 1_000)
            .expect("pin honest");
        s.pin_peer_head(&head(1, absurd, 0xFF, absurd), node(2), 1_000)
            .expect("pin absurd");

        // Both stored: the fix is in the ordering, never in discarding
        // evidence.
        let seqs: Vec<u64> = s
            .peer_head_pins(node(1))
            .expect("pins")
            .into_iter()
            .map(|p| p.seq)
            .collect();
        assert_eq!(seqs.len(), 2, "both positions are stored, have {seqs:?}");

        // Verify target: newest position we FIRST saw, tie on the
        // LOWER seq — never `MAX(seq)`.
        let target = s
            .newest_pin_by_observation(node(1))
            .expect("target")
            .expect("some pin");
        assert_ne!(
            target.seq, absurd,
            "an inflated claim must not win the verify slot"
        );

        // The RELAY selector deliberately differs on the tie (`seq
        // DESC`, see its doc comment), so it is not asserted here: for
        // relay a lower position is simply worse, and with one slot
        // the two capture directions are mutually exclusive. What both
        // selectors DO share is `first_seen_ms`, so a re-push cannot
        // re-take either slot.
        s.pin_peer_head(&head(1, 9, 0x09, 9), node(2), 5_000)
            .expect("re-push");
        assert_ne!(
            s.newest_pin_by_observation(node(1))
                .expect("target")
                .expect("some pin")
                .seq,
            absurd,
            "a re-push must not move the verify target either"
        );
    }
    #[test]
    fn p06d_a_corroborated_pin_survives_being_pushed_out_of_the_recent_window() {
        // Round 7 BLOCKER-1. `Corroborated` used to be evictable, so a
        // flood that pushed an honest corroborated pin out of
        // `PIN_KEEP_RECENT` made the hourly sweep delete it — and the
        // verdict was unrecoverable, because `audit_run_corroborates`
        // reads the pin, so losing the row loses the corroboration and
        // only the subject can mint a replacement.
        //
        // p06b only checks that `evictable()` AGREES with the SQL, so
        // flipping both together passes it. This asserts the policy.
        let s = Store::open_memory().expect("store");
        // An UNCHECKED pin first, so it takes the `MIN(rowid)`
        // always-kept slot. Otherwise the corroborated pin would
        // survive for being the oldest row rather than for its status,
        // and the test would pass against the defect.
        s.pin_peer_head(&head(1, 1, 0x01, 1), node(2), 1_000)
            .expect("pin");
        s.pin_peer_head(&head(1, 2, 0x02, 2), node(2), 1_001)
            .expect("pin");
        s.set_pin_status(node(1), 2, PinStatus::Corroborated)
            .expect("corroborate");
        // Newer pins, all in ONE age bucket (coarse ladder) so the
        // bucket rule has something to thin, and enough of them that
        // the corroborated pin at seq 2 is well outside keep_recent.
        for i in 3..=40u64 {
            s.pin_peer_head(
                &head(1, i, u8::try_from(i).unwrap_or(0), i),
                node(2),
                1_000 + i,
            )
            .expect("pin");
        }

        s.thin_peer_head_pins(2, 1_000_000).expect("thin");

        let survivors: Vec<(u64, PinStatus)> = s
            .peer_head_pins(node(1))
            .expect("pins")
            .into_iter()
            .map(|p| (p.seq, p.status))
            .collect();
        assert!(
            survivors.contains(&(2, PinStatus::Corroborated)),
            "a corroborated pin is EVIDENCE and must outlive the sweep; have {survivors:?}"
        );
        assert!(
            survivors.len() < 40,
            "and the sweep still thinned the unchecked ones; have {} rows",
            survivors.len()
        );
    }
    #[test]
    fn p06e_permanent_pins_do_not_consume_the_recent_window() {
        // Round 8 M6/M7: both `status` filters in the keep-recent
        // subquery could be widened back to
        // `IN ('unchecked','corroborated')` with the ENTIRE workspace
        // suite green. They are not equivalent and the `b` one is the
        // dangerous half: it counts the pins sitting ABOVE a candidate,
        // so counting permanent rows there means that once a node has
        // `keep_recent` corroborated pins — which never expire now —
        // every `unchecked` pin scores outside the window and becomes
        // evictable in one sweep.
        let s = Store::open_memory().expect("store");
        let keep = 4usize;

        // UNCHECKED pins first (oldest), then `keep` corroborated ones
        // that are NEWER. Order matters: the `b` subquery counts rows
        // ABOVE a candidate, so the permanent rows have to sit above
        // the candidate to discriminate at all. An earlier version of
        // this test put them below and passed against both mutants.
        for i in 1..=keep as u64 {
            s.pin_peer_head(
                &head(1, i, u8::try_from(i).unwrap_or(0), i),
                node(2),
                1_000 + i,
            )
            .expect("pin");
        }
        for i in (keep as u64 + 1)..=(keep as u64 * 2) {
            s.pin_peer_head(
                &head(1, i, u8::try_from(i).unwrap_or(0), i),
                node(2),
                2_000 + i,
            )
            .expect("pin");
            s.set_pin_status(node(1), i, PinStatus::Corroborated)
                .expect("corroborate");
        }

        s.thin_peer_head_pins(keep, 1_000_000).expect("thin");

        let survivors: Vec<u64> = s
            .peer_head_pins(node(1))
            .expect("pins")
            .into_iter()
            .map(|p| p.seq)
            .collect();

        // Every unchecked pin has fewer than `keep` UNCHECKED rows
        // above it, so all of them are inside the window and must
        // live. Count the permanent rows there and they are all pushed
        // out at once.
        for i in 1..=keep as u64 {
            assert!(
                survivors.contains(&i),
                "unchecked pin {i} is inside keep_recent and must survive; have {survivors:?}"
            );
        }
        // And the permanent ones survive regardless of the window.
        for i in (keep as u64 + 1)..=(keep as u64 * 2) {
            assert!(
                survivors.contains(&i),
                "corroborated pin {i} must survive; have {survivors:?}"
            );
        }
    }

    #[test]
    fn p10_one_unstorable_seq_does_not_void_the_rest_of_the_envelope() {
        // `audit_peer_heads` has carried CHECK (seq > 0) since V0010,
        // so a head at seq 0 cannot be stored. Batching the envelope
        // into ONE transaction turned that from a per-head loss into a
        // whole-envelope loss: the INSERT raised a constraint
        // violation, the transaction was abandoned, and every honest
        // head beside it was rolled back. Any paired peer can mint a
        // self-signed seq-0 head for its own node_id, so this was one
        // head per envelope away from denying pin density outright.
        let s = Store::open_memory().expect("store");
        let heads = vec![
            head(1, 5, 0xAA, 5),
            head(1, 0, 0xBB, 1),
            head(1, 6, 0xCC, 6),
        ];
        let outcomes = s
            .pin_peer_heads(&heads, node(2), 1_000)
            .expect("the batch must COMMIT, not fail the envelope");
        assert!(
            matches!(outcomes[1], PinOutcome::RejectedSeq),
            "seq 0 is not storable, got {:?}",
            outcomes[1]
        );
        assert!(
            matches!(outcomes[0], PinOutcome::Pinned) && matches!(outcomes[2], PinOutcome::Pinned),
            "the honest heads beside it must survive, got {outcomes:?}"
        );
        let kept: Vec<u64> = s
            .peer_head_pins(node(1))
            .expect("pins")
            .into_iter()
            .map(|p| p.seq)
            .collect();
        assert_eq!(kept, vec![5, 6], "and must be durably pinned");
    }

    #[test]
    fn p11_a_fork_between_two_new_branches_in_one_envelope_is_recorded() {
        // p03 covers a fork against a position we ALREADY hold. This
        // is the other half: both branches arrive in one envelope at a
        // position we have never pinned. The first insert makes the
        // position exist, so `pin_one`'s re-read inside the shared
        // transaction sees it and classifies the second as the fork it
        // is — rather than storing two unrelated pins and detecting
        // nothing.
        let s = Store::open_memory().expect("store");
        let heads = vec![head(1, 7, 0xAA, 7), head(1, 7, 0xBB, 7)];
        let outcomes = s.pin_peer_heads(&heads, node(2), 1_000).expect("pin batch");
        assert!(
            matches!(outcomes[0], PinOutcome::Pinned),
            "the first branch pins, got {:?}",
            outcomes[0]
        );
        assert!(
            matches!(outcomes[1], PinOutcome::Fork { .. }),
            "and the second is a fork, got {:?}",
            outcomes[1]
        );
        let conflicts = s.head_conflicts(10).expect("conflicts");
        assert_eq!(conflicts.len(), 1, "one conflict row for the pair");
        // Both signatures are kept: that is what shows a third party
        // the node signed two histories at one position.
        assert_eq!(conflicts[0].held_hash, [0xAA; 32]);
        assert_eq!(conflicts[0].other_hash, [0xBB; 32]);
        let pins = s.peer_head_pins(node(1)).expect("pins");
        assert_eq!(pins.len(), 1, "the fork adds evidence, not a second pin");
        assert_eq!(
            pins[0].status,
            PinStatus::Contradicted,
            "and the held pin is marked contradicted"
        );
    }
}
