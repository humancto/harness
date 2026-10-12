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
    /// A NEW position refused because the reporting peer had spent its
    /// allowance for this window (5.13c-1b-i).
    ///
    /// Only ever returned for a position we do not already hold.
    /// `Fork`, `Refreshed` and `RejectedSeq` are classifications, not
    /// writes of new rows, and are never refused — an allowance that
    /// suppressed `Fork` would let a node hide a rewrite of its own
    /// chain behind junk heads, which is how the first attempt at this
    /// failed.
    ///
    /// A refusal is a permanent loss of pin density, which the sync
    /// module calls the security parameter of the whole design, so the
    /// allowance errs high.
    BudgetRefused,
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
    /// Pin a whole envelope's heads under one allowance, in ONE
    /// transaction.
    ///
    /// # Why batched, and why the allowance is an integer
    ///
    /// Two earlier attempts at this allowance failed, both by letting
    /// something the adversary controls decide the outcome:
    ///
    /// 1. Checked per head in `ingest` BEFORE `pin_peer_head`, with a
    ///    `continue` past it. A spent allowance then dropped every
    ///    head from that peer — `Fork` included — so a node could hide
    ///    a rewrite of its own chain behind junk heads: no conflict
    ///    row, no log line.
    /// 2. Moved inside `pin_peer_head`, per head. `ingest` iterates
    ///    the SENDER's `Vec` order, so that is an attacker-ordered
    ///    allowance: a subject lists 64 junk positions first and its
    ///    genuine head last, and the honest position is refused in
    ///    every window, in every peer, forever. The subject gets to
    ///    collapse its own pin density.
    ///
    /// So the allowance arrives as a plain `u32` and the spend order
    /// is OURS: the highest NEW position for each distinct subject
    /// first, then the remainder. A subject cannot crowd out its own
    /// honest head and a relayer cannot crowd out a third party's.
    ///
    /// An integer rather than a callback or trait handle, because a
    /// budget handle invoked from inside this `with_conn` closure that
    /// touched the store would deadlock the daemon silently —
    /// `with_conn` holds one process-wide non-reentrant
    /// `parking_lot::Mutex`, and Decision 23 already records that
    /// hazard for `force_pin_status`. One lock acquisition and one
    /// transaction for the whole envelope also replaces the 64 of each
    /// that the per-head path cost.
    ///
    /// Returns an outcome per input head, in input order, plus how
    /// much of the allowance was spent.
    ///
    /// # Errors
    /// Underlying sqlite errors.
    pub fn pin_peer_heads(
        &self,
        heads: &[AuditHead],
        reported_by: NodeId,
        allowance: u32,
        now_ms: u64,
    ) -> Result<(Vec<PinOutcome>, u32), StoreError> {
        self.with_conn(|c| {
            let tx = c.unchecked_transaction()?;

            // Pass 1, read-only: which heads are NEW positions?
            // `RejectedSeq` is decided here too, since it needs no
            // read and must never consume allowance.
            let mut is_new = vec![false; heads.len()];
            for (i, head) in heads.iter().enumerate() {
                let Ok(seq) = i64::try_from(head.seq) else {
                    continue;
                };
                let held: Option<i64> = tx
                    .query_row(
                        "SELECT 1 FROM audit_peer_heads WHERE node_id = ?1 AND seq = ?2",
                        params![head.node_id.as_bytes(), seq],
                        |r| r.get(0),
                    )
                    .optional()?;
                is_new[i] = held.is_none();
            }

            // Spend the allowance in OUR order: highest new seq per
            // distinct subject first, then everything else by
            // descending seq. Ties break on input index only after
            // both keys, so the sender's ordering is the last word and
            // never the first.
            let mut candidates: Vec<(usize, NodeId, u64)> = heads
                .iter()
                .enumerate()
                .filter(|(i, _)| is_new[*i])
                .map(|(i, h)| (i, h.node_id, h.seq))
                .collect();
            let mut best_per_subject: std::collections::HashMap<NodeId, u64> =
                std::collections::HashMap::new();
            for (_, node, seq) in &candidates {
                let e = best_per_subject.entry(*node).or_insert(*seq);
                if *seq > *e {
                    *e = *seq;
                }
            }
            candidates.sort_by_key(|(i, node, seq)| {
                let first_tier = u8::from(best_per_subject.get(node) != Some(seq));
                (first_tier, std::cmp::Reverse(*seq), *i)
            });
            let selected: std::collections::HashSet<usize> = candidates
                .into_iter()
                .take(allowance as usize)
                .map(|(i, _, _)| i)
                .collect();

            // Pass 2, in INPUT order, applying writes. The existing
            // row is re-read because an earlier head in this same
            // batch may have inserted the position — in which case
            // this one is a genuine same-envelope fork and must be
            // recorded, not refused.
            let mut out = Vec::with_capacity(heads.len());
            let mut spent = 0u32;
            for (i, head) in heads.iter().enumerate() {
                let outcome = pin_one(&tx, head, reported_by, now_ms, selected.contains(&i))?;
                if matches!(outcome, PinOutcome::Pinned | PinOutcome::StalePinned) {
                    spent = spent.saturating_add(1);
                }
                out.push(outcome);
            }
            tx.commit()?;
            Ok((out, spent))
        })
    }

    /// Pin one head. Equivalent to [`Self::pin_peer_heads`] with an
    /// allowance of 1 and a single-element slice, kept because most
    /// callers and tests have exactly one head in hand.
    pub fn pin_peer_head(
        &self,
        head: &AuditHead,
        reported_by: NodeId,
        now_ms: u64,
    ) -> Result<PinOutcome, StoreError> {
        self.with_conn(|c| {
            let tx = c.unchecked_transaction()?;
            let outcome = pin_one(&tx, head, reported_by, now_ms, true)?;
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
    /// How many conflict branches at this position were REFUSED by
    /// the per-position cap.
    ///
    /// Surfaced so an operator is told more branches exist rather
    /// than shown a silently partial picture — and so 5.13c-3's
    /// acknowledgement can state that an ack binds the kept pair and
    /// cannot clear an un-enumerable remainder.
    ///
    /// # Errors
    /// Underlying sqlite errors.
    pub fn conflict_overflow(&self, node: NodeId, seq: u64) -> Result<usize, StoreError> {
        self.with_conn(|c| {
            let n: Option<i64> = c
                .query_row(
                    "SELECT suppressed FROM audit_conflict_overflow
                      WHERE node_id = ?1 AND seq = ?2",
                    params![node.as_bytes(), i64_ms(seq)],
                    |r| r.get(0),
                )
                .optional()?;
            Ok(usize::try_from(n.unwrap_or(0)).unwrap_or(0))
        })
    }

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

/// Conflict rows kept per `(node_id, seq)` before refusals start.
///
/// `audit_head_conflicts` is keyed on `other_hash`, which the SUBJECT
/// mints freely, so without this one envelope writes a row per head.
/// Beyond this the refusals are COUNTED, never deleted: the kept rows
/// hold both signatures in full and are the proof, and deleting a
/// conflict row would be Decision 12 by the back door.
pub const MAX_CONFLICTS_PER_POSITION: i64 = 8;

/// Pin one head inside an existing transaction.
///
/// `allow_new` is the allowance decision, already made by the caller
/// in an order the sender does not control. It gates ONLY the insert
/// of a position we do not hold. `Fork`, `Refreshed` and
/// `RejectedSeq` are classifications and always proceed — an
/// allowance that suppressed `Fork` is how the first attempt at this
/// let a node hide a rewrite of its own chain.
fn pin_one(
    tx: &rusqlite::Transaction<'_>,
    head: &AuditHead,
    reported_by: NodeId,
    now_ms: u64,
    allow_new: bool,
) -> Result<PinOutcome, StoreError> {
    // Never clamp a value a signature covers.
    let Ok(seq) = i64::try_from(head.seq) else {
        return Ok(PinOutcome::RejectedSeq);
    };
    let existing: Option<(Vec<u8>, i64, Vec<u8>)> = tx
        .query_row(
            "SELECT entry_hash, at_ms, sig FROM audit_peer_heads
              WHERE node_id = ?1 AND seq = ?2",
            params![head.node_id.as_bytes(), seq],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?;

    match existing {
        Some((held_hash, _held_at, _held_sig)) if to_hash(&held_hash) == head.entry_hash => {
            tx.execute(
                "UPDATE audit_peer_heads SET observed_at_ms = ?3
                  WHERE node_id = ?1 AND seq = ?2",
                params![head.node_id.as_bytes(), seq, i64_ms(now_ms)],
            )?;
            Ok(PinOutcome::Refreshed)
        }
        Some((held_hash, held_at, held_sig)) => record_fork(
            tx,
            head,
            seq,
            &held_hash,
            held_at,
            &held_sig,
            reported_by,
            now_ms,
        ),
        None => {
            if !allow_new {
                return Ok(PinOutcome::BudgetRefused);
            }
            let highest: Option<i64> = tx
                .query_row(
                    "SELECT MAX(seq) FROM audit_peer_heads WHERE node_id = ?1",
                    params![head.node_id.as_bytes()],
                    |r| r.get(0),
                )
                .optional()?
                .flatten();
            // ALWAYS insert when allowed. Whether this position is
            // below the newest we hold changes only the
            // CLASSIFICATION (it is not a regression claim), never
            // whether we keep the pin — see `PinOutcome::StalePinned`.
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
                Ok(PinOutcome::StalePinned)
            } else {
                Ok(PinOutcome::Pinned)
            }
        }
    }
}

/// Record a same-seq fork, bounded per position.
///
/// Beyond [`MAX_CONFLICTS_PER_POSITION`] the branch is COUNTED rather
/// than stored — never deleted, which would be Decision 12 by the
/// back door — and the outcome still carries a stable `conflict_id`
/// so 5.13c-3's acknowledgement has something to bind.
#[allow(clippy::too_many_arguments)] // all of it is one row's worth of columns
fn record_fork(
    tx: &rusqlite::Transaction<'_>,
    head: &AuditHead,
    seq: i64,
    held_hash: &[u8],
    held_at: i64,
    held_sig: &[u8],
    reported_by: NodeId,
    now_ms: u64,
) -> Result<PinOutcome, StoreError> {
    {
        // The PK collision IS the fork detector. Keying the table
        // on the hash too would store both as ordinary pins and
        // detect nothing.
        //
        // Bounded per position: beyond MAX_CONFLICTS_PER_POSITION
        // the branch is counted rather than stored, and the
        // outcome still reports the FIRST conflict's id so the
        // caller has a stable handle (5.13c-3's acknowledgement
        // binds by it).
        let held_count: i64 = tx.query_row(
            "SELECT COUNT(*) FROM audit_head_conflicts WHERE node_id = ?1 AND seq = ?2",
            params![head.node_id.as_bytes(), seq],
            |r| r.get(0),
        )?;
        let already: Option<i64> = tx
            .query_row(
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
            )
            .optional()?;
        if already.is_none() && held_count >= MAX_CONFLICTS_PER_POSITION {
            tx.execute(
                "INSERT INTO audit_conflict_overflow(node_id, seq, suppressed, first_ms, last_ms)
                     VALUES (?1,?2,1,?3,?3)
                     ON CONFLICT(node_id, seq) DO UPDATE
                       SET suppressed = suppressed + 1, last_ms = ?3",
                params![head.node_id.as_bytes(), seq, i64_ms(now_ms)],
            )?;
        } else if already.is_none() {
            tx.execute(
                "INSERT INTO audit_head_conflicts(
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
        }
        // Whichever branch ran, the caller needs a stable id: the
        // row for THIS pair if we kept it, else the oldest at this
        // position.
        let conflict_id: i64 = tx.query_row(
            "SELECT id FROM audit_head_conflicts
                  WHERE node_id = ?1 AND seq = ?2
                  ORDER BY (held_hash = ?3 AND other_hash = ?4) DESC, id ASC
                  LIMIT 1",
            params![
                head.node_id.as_bytes(),
                seq,
                held_hash,
                head.entry_hash.as_slice()
            ],
            |r| r.get(0),
        )?;
        // The held pin is now contradicted, and a contradicted pin
        // is never evicted by thinning.
        tx.execute(
            "UPDATE audit_peer_heads SET status = 'contradicted'
                  WHERE node_id = ?1 AND seq = ?2",
            params![head.node_id.as_bytes(), seq],
        )?;
        Ok(PinOutcome::Fork { conflict_id })
    }
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
    fn p10_junk_listed_first_cannot_crowd_out_the_honest_head() {
        // 5.13c-1b-i, and the probe that kills the shape the first
        // draft of this item proposed. `ingest` iterates the SENDER's
        // `Vec` order, so a per-head allowance is an attacker-ordered
        // allowance: a subject lists its junk positions FIRST and its
        // genuine head last, and the honest position is refused in
        // every window, in every peer, forever. The subject would get
        // to collapse its own pin density, which the sync module calls
        // the security parameter of the whole design.
        //
        // The allowance is spent in OUR order instead: highest new
        // position per subject first.
        let s = Store::open_memory().expect("store");
        let allowance = 4u32;
        let mut heads: Vec<AuditHead> = (1..=20u64)
            .map(|i| head(1, i, u8::try_from(i).unwrap_or(0), i))
            .collect();
        // The honest head is the highest, listed LAST.
        heads.push(head(1, 9_000, 0x90, 9_000));

        let (outcomes, spent) = s
            .pin_peer_heads(&heads, node(2), allowance, 1_000)
            .expect("pin batch");
        assert_eq!(spent, allowance, "the whole allowance is spent");
        assert_eq!(
            outcomes.last(),
            Some(&PinOutcome::Pinned),
            "the honest head is pinned despite being listed last, got {:?}",
            outcomes.last()
        );
        let seqs: Vec<u64> = s
            .peer_head_pins(node(1))
            .expect("pins")
            .into_iter()
            .map(|p| p.seq)
            .collect();
        assert!(seqs.contains(&9_000), "have {seqs:?}");
        assert_eq!(
            seqs.len(),
            allowance as usize,
            "and the allowance bounded the rest; have {seqs:?}"
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|o| **o == PinOutcome::BudgetRefused)
                .count(),
            heads.len() - allowance as usize,
            "the rest are refused, visibly"
        );
    }

    #[test]
    fn p11_a_fork_is_recorded_even_when_the_allowance_is_spent() {
        // The first attempt at this allowance checked it per head
        // BEFORE `pin_peer_head` and `continue`d past it, so a spent
        // allowance dropped `Fork` outcomes too — a node could hide a
        // rewrite of its own chain behind junk heads: no conflict row,
        // no log line. Classification is never refused.
        let s = Store::open_memory().expect("store");
        s.pin_peer_head(&head(1, 10, 0xAA, 10), node(2), 1_000)
            .expect("pin");

        // One envelope: junk first to exhaust the allowance, then the
        // conflicting head at the position we already hold.
        let mut heads: Vec<AuditHead> = (100..=120u64)
            .map(|i| head(1, i, u8::try_from(i % 251).unwrap_or(0), i))
            .collect();
        heads.push(head(1, 10, 0xBB, 10));

        let (outcomes, _) = s
            .pin_peer_heads(&heads, node(2), 2, 2_000)
            .expect("pin batch");
        assert!(
            matches!(outcomes.last(), Some(PinOutcome::Fork { .. })),
            "the fork must be recorded with the allowance spent, got {:?}",
            outcomes.last()
        );
        assert_eq!(
            s.head_conflicts(10).expect("conflicts").len(),
            1,
            "and a conflict row written"
        );
        assert_eq!(status_of_pin(&s, 10), PinStatus::Contradicted);
    }

    #[test]
    fn p12_conflicts_are_bounded_per_position_and_counted_not_deleted() {
        // `audit_head_conflicts` is keyed on `other_hash`, which the
        // subject mints freely, so one envelope could write a row per
        // head. Beyond the cap they are REFUSED and counted — never
        // deleted, which would be Decision 12 by the back door.
        let s = Store::open_memory().expect("store");
        s.pin_peer_head(&head(1, 7, 0x01, 7), node(2), 1_000)
            .expect("pin");
        let branches = usize::try_from(MAX_CONFLICTS_PER_POSITION).expect("fits") * 3;
        for i in 0..branches {
            let hash = u8::try_from(100 + i).unwrap_or(0);
            s.pin_peer_head(&head(1, 7, hash, 7), node(2), 2_000 + i as u64)
                .expect("pin");
        }

        let kept = s.head_conflicts(100).expect("conflicts");
        assert_eq!(
            i64::try_from(kept.len()).expect("fits"),
            MAX_CONFLICTS_PER_POSITION,
            "bounded at the cap; kept {}",
            kept.len()
        );
        // The first row still carries both signatures in full — that
        // is the proof the cap must not cost.
        assert!(kept.iter().all(|c| c.seq == 7));
        let suppressed = s.conflict_overflow(node(1), 7).expect("overflow");
        assert_eq!(
            suppressed,
            branches - usize::try_from(MAX_CONFLICTS_PER_POSITION).expect("fits"),
            "and the refusals are counted"
        );
        assert_eq!(status_of_pin(&s, 7), PinStatus::Contradicted);
    }

    fn status_of_pin(s: &Store, seq: u64) -> PinStatus {
        s.peer_head_pins(node(1))
            .expect("pins")
            .into_iter()
            .find(|p| p.seq == seq)
            .expect("pin")
            .status
    }
}
