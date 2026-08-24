//! 5.13c-2 (ADR-0041) — pulling a peer's entries and turning a pin
//! into evidence.
//!
//! 5.13c-1 records that a peer said `(seq, entry_hash)` at some point.
//! That catches a fork at one position. It does NOT catch the dominant
//! rewrite shape — truncate, re-append, grow past the pin — because
//! linking pin `(100, h100)` to pin `(200, h200)` requires walking
//! entries 101..200 and checking that each one hashes to the next
//! one's `prev_hash`. Reproducing `h200` over altered entries is then
//! a blake3 collision problem, which is the actual guarantee.
//!
//! This module does that walk.

use harness_core::{audit_entry_hash, AuditEntryWire, NodeId};
use rusqlite::{params, OptionalExtension};

use crate::audit::action_from_str;
use crate::error::StoreError;
use crate::open::Store;
use crate::peer_heads::PinStatus;

/// Most rows accepted from one `Entries` message.
///
/// A peer streaming its whole chain must not hold the single
/// process-wide connection mutex for the duration — the same concern
/// that bounded `verify_page` in 5.13a.
pub const MAX_INGEST_ROWS_PER_PULL: usize = 5_000;

/// Encoded-size budget for one served batch.
///
/// The row cap alone is not a size bound: `detail` is capped at 4 KiB
/// per row, so 5000 rows could be 20 MB — far past any sane frame.
/// Serving stops at whichever limit is reached first and reports
/// `truncated`, and the requester asks again from where it stopped.
pub const MAX_SERVE_BYTES: usize = 768 * 1024;

/// How far ahead of our clock an ingested row's `at_ms` may be before
/// we refuse it. Clock skew across a LAN is seconds; a row claiming
/// next century is an attempt to own the top of every History page.
pub const MAX_INGEST_SKEW_MS: u64 = 24 * 60 * 60 * 1000;

/// Why a range was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngestRefusal {
    /// Rows claimed to belong to this node's own chain. A peer must
    /// never write our chain — that is the one thing local
    /// verification is supposed to guarantee.
    OwnChain,
    /// An entry's stored fields do not reproduce its `entry_hash`.
    HashMismatch { seq: u64 },
    /// Row N+1's `prev_hash` is not row N's `entry_hash`.
    BrokenLink { seq: u64 },
    /// A hole in the supplied range.
    Gap { expected: u64, got: u64 },
    /// An action string this build does not know. Same rule as
    /// verification: an unknown action must not be silently accepted,
    /// or a relabelled row rehashes under a name we cannot check.
    UnknownAction { seq: u64 },
    /// `at_ms` is implausibly far ahead of our clock.
    Skew { seq: u64 },
    /// The batch does not continue from what we already committed.
    NotContiguous { have_through: u64, offered: u64 },
    /// More rows than [`MAX_INGEST_ROWS_PER_PULL`].
    TooMany { got: usize },
    /// A continuing batch does not hash-link to the row we already
    /// committed at `through_seq` — only its sequence number lined up.
    BrokenRunLink { at_seq: u64 },
    /// A run starting above seq 1 with nothing to anchor it: no stored
    /// predecessor and no pin at `seq - 1` matching its `prev_hash`.
    UnanchoredStart { seq: u64 },
    /// A row we already hold at this position has a DIFFERENT hash.
    /// Two entries at one `(node_id, seq)` is the fork 5.13c-1 exists
    /// to detect, so it is refused loudly rather than swallowed.
    Conflict { seq: u64 },
    /// `detail` exceeds what a locally appended row may carry.
    DetailTooLarge { seq: u64 },
    /// Rows past the pin this run is walking toward.
    PastTarget { seq: u64 },
}

/// What one accepted batch did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestProgress {
    /// Rows newly committed.
    pub committed: usize,
    /// Highest seq now held for this run.
    pub through_seq: u64,
    /// The run reached the pin it was walking toward AND the pinned
    /// hash matches. Only then is the pin corroborated.
    pub reached_pin: bool,
}

impl Store {
    /// Ingest one contiguous batch of a peer's entries.
    ///
    /// `local` is passed explicitly because `Store` has no notion of
    /// which node it belongs to, and the "never write our own chain"
    /// guard must not be skippable.
    ///
    /// A batch commits when it links to what we already committed for
    /// this run. It is NOT all-or-nothing across the whole pull:
    /// with a row cap, every batch but the last ends short of the pin,
    /// so an all-or-nothing rule would make the last batch
    /// unreachable. `audit_ingest_runs` records that the walk is
    /// incomplete, and only a run that reaches its target pin with a
    /// matching hash upgrades that pin to `Corroborated`.
    ///
    /// # Errors
    /// Underlying sqlite errors.
    pub fn audit_ingest_range(
        &self,
        local: NodeId,
        node: NodeId,
        target_seq: u64,
        entries: &[AuditEntryWire],
        now_ms: u64,
    ) -> Result<Result<IngestProgress, IngestRefusal>, StoreError> {
        if node == local {
            return Ok(Err(IngestRefusal::OwnChain));
        }
        if entries.len() > MAX_INGEST_ROWS_PER_PULL {
            return Ok(Err(IngestRefusal::TooMany { got: entries.len() }));
        }
        let Some(first) = entries.first() else {
            return Ok(Ok(IngestProgress {
                committed: 0,
                through_seq: 0,
                reached_pin: false,
            }));
        };

        if let Err(refusal) = validate_batch(node, entries, target_seq, now_ms) {
            return Ok(Err(refusal));
        }
        let last = entries.last().unwrap_or(first);

        let pin_hash = self.pinned_hash_at(node, target_seq)?;
        self.with_conn(|c| {
            let tx = c.unchecked_transaction()?;
            let run: Option<(i64, i64)> = tx
                .query_row(
                    "SELECT from_seq, through_seq FROM audit_ingest_runs
                      WHERE node_id = ?1 AND target_seq = ?2",
                    params![node.as_bytes(), i64c(target_seq)],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;

            if let Err(refusal) = check_run_anchor(&tx, node, run, first)? {
                tx.rollback()?;
                return Ok(Err(refusal));
            }

            let mut committed = 0usize;
            for e in entries {
                // A row already here with a DIFFERENT hash is not a
                // duplicate to skip — it is two entries at one
                // position, the fork 5.13c-1 exists to detect.
                // `INSERT OR IGNORE` swallowed it and then reported
                // progress it had not made (diff review MAJOR-5).
                let existing: Option<Vec<u8>> = tx
                    .query_row(
                        "SELECT entry_hash FROM audit_log WHERE node_id = ?1 AND seq = ?2",
                        params![node.as_bytes(), i64c(e.seq)],
                        |r| r.get(0),
                    )
                    .optional()?;
                if let Some(raw) = existing {
                    if <[u8; 32]>::try_from(raw.as_slice()).unwrap_or([0u8; 32]) != e.entry_hash {
                        tx.rollback()?;
                        return Ok(Err(IngestRefusal::Conflict { seq: e.seq }));
                    }
                    continue;
                }
                committed += tx.execute(
                    "INSERT INTO audit_log(
                         node_id, seq, at_ms, action, subject, detail,
                         actor, prev_hash, entry_hash, received_at_ms)
                     VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
                    params![
                        node.as_bytes(),
                        i64c(e.seq),
                        i64c(e.at_ms),
                        e.action,
                        e.subject,
                        e.detail,
                        e.actor,
                        e.prev_hash.as_slice(),
                        e.entry_hash.as_slice(),
                        i64c(now_ms),
                    ],
                )?;
            }

            let reached = last.seq == target_seq && pin_hash == Some(last.entry_hash);
            let from_seq = run.map_or(first.seq, |(f, _)| u64::try_from(f).unwrap_or(first.seq));
            tx.execute(
                "INSERT INTO audit_ingest_runs(
                     node_id, target_seq, from_seq, through_seq, complete, updated_at_ms)
                 VALUES (?1,?2,?3,?4,?5,?6)
                 ON CONFLICT(node_id, target_seq) DO UPDATE SET
                     through_seq = excluded.through_seq,
                     complete = excluded.complete,
                     updated_at_ms = excluded.updated_at_ms",
                params![
                    node.as_bytes(),
                    i64c(target_seq),
                    i64c(from_seq),
                    i64c(last.seq),
                    i32::from(reached),
                    i64c(now_ms),
                ],
            )?;
            tx.commit()?;
            Ok(Ok(IngestProgress {
                // Rows actually WRITTEN, not rows offered.
                committed,
                through_seq: last.seq,
                reached_pin: reached,
            }))
        })
    }

    /// Verify a run of an INGESTED peer chain.
    ///
    /// `audit_verify_chain` cannot do this. Its `anchor_holds` accepts
    /// only three anchors — seq 1 with a zero `prev_hash`, a locally
    /// stored predecessor, or a truncation marker in that node's own
    /// chain — so a legitimate partial pull of 500..600 reports
    /// `Broken { at_seq: 500 }`, flagging honest replication as
    /// tampering.
    ///
    /// The fourth legitimate anchor is a pin WE took at
    /// `first.seq - 1` whose hash matches `first.prev_hash`. It must
    /// be a pin we already held — never a head carried in the same
    /// message that delivered these rows, which would let the sender
    /// supply both the claim and its own corroboration.
    ///
    /// # Errors
    /// Underlying sqlite errors.
    pub fn audit_verify_range(
        &self,
        node: NodeId,
        from_seq: u64,
        max_rows: usize,
    ) -> Result<crate::audit::ChainStatus, StoreError> {
        // Try the ordinary anchors first: an ingested run that happens
        // to start at seq 1, or to sit directly on rows we already
        // hold, is verifiable the normal way.
        match self.audit_verify_chain(node, from_seq, max_rows)? {
            crate::audit::ChainStatus::Broken { at_seq } if at_seq == from_seq => {}
            other => return Ok(other),
        }
        // Otherwise the run is anchored on a pin, if we hold one that
        // matches the first row's predecessor.
        // Guard BEFORE the lookup: no row has seq 0, so asking for the
        // predecessor of 0 would return Empty and make this
        // unreachable (diff review MINOR).
        if from_seq == 0 {
            return Ok(crate::audit::ChainStatus::Broken { at_seq: from_seq });
        }
        let Some(first_prev) = self.entry_prev_hash_at(node, from_seq)? else {
            return Ok(crate::audit::ChainStatus::Empty);
        };
        match self.pinned_hash_at(node, from_seq - 1)? {
            Some(pinned) if pinned == first_prev => {
                self.audit_verify_from_anchor(node, from_seq, max_rows)
            }
            _ => Ok(crate::audit::ChainStatus::Broken { at_seq: from_seq }),
        }
    }

    /// The hash we pinned at `seq`, if any.
    fn pinned_hash_at(&self, node: NodeId, seq: u64) -> Result<Option<[u8; 32]>, StoreError> {
        self.with_conn(|c| {
            let raw: Option<Vec<u8>> = c
                .query_row(
                    "SELECT entry_hash FROM audit_peer_heads
                      WHERE node_id = ?1 AND seq = ?2",
                    params![node.as_bytes(), i64c(seq)],
                    |r| r.get(0),
                )
                .optional()?;
            Ok(raw.map(|b| <[u8; 32]>::try_from(b.as_slice()).unwrap_or([0u8; 32])))
        })
    }

    /// Is the run toward `target_seq` complete and corroborating?
    ///
    /// A run that stopped short is NOT corroboration, and this is the
    /// predicate that keeps a half-finished pull from being mistaken
    /// for one.
    ///
    /// # Errors
    /// Underlying sqlite errors.
    pub fn audit_run_corroborates(
        &self,
        node: NodeId,
        target_seq: u64,
    ) -> Result<bool, StoreError> {
        let pin = self.pinned_hash_at(node, target_seq)?;
        let Some(pin) = pin else { return Ok(false) };
        let row: Option<(i64, i64, i64)> = self.with_conn(|c| {
            Ok(c.query_row(
                "SELECT through_seq, complete, from_seq FROM audit_ingest_runs
                  WHERE node_id = ?1 AND target_seq = ?2",
                params![node.as_bytes(), i64c(target_seq)],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?)
        })?;
        let Some((through, complete, from)) = row else {
            return Ok(false);
        };
        if complete == 0 || u64::try_from(through).unwrap_or(0) != target_seq {
            return Ok(false);
        }
        if self.audit_entry_hash_at(node, target_seq)? != Some(pin) {
            return Ok(false);
        }
        // The run bookkeeping says the walk finished; the VERIFIER says
        // whether what it walked is actually a chain (diff review
        // BLOCKER-3). Without this the two disagreed and nothing
        // reconciled them: a run stitched from two different histories
        // reported `corroborates = true` while `audit_verify_range`
        // called the same rows Broken.
        let from_seq = u64::try_from(from).unwrap_or(1).max(1);
        let span = usize::try_from(target_seq.saturating_sub(from_seq).saturating_add(1))
            .unwrap_or(usize::MAX);
        Ok(matches!(
            self.audit_verify_range(node, from_seq, span)?,
            crate::audit::ChainStatus::Verified { .. }
        ))
    }

    /// Mark a pin from the run that reached it.
    ///
    /// # Errors
    /// Underlying sqlite errors.
    pub fn settle_pin_from_run(
        &self,
        node: NodeId,
        target_seq: u64,
    ) -> Result<PinStatus, StoreError> {
        let status = if self.audit_run_corroborates(node, target_seq)? {
            PinStatus::Corroborated
        } else {
            PinStatus::Unchecked
        };
        self.set_pin_status(node, target_seq, status)?;
        Ok(status)
    }

    /// Serve a contiguous run of one node's chain.
    ///
    /// # Errors
    /// Underlying sqlite errors.
    pub fn audit_entries_for_range(
        &self,
        node: NodeId,
        from_seq: u64,
        to_seq: u64,
    ) -> Result<(Vec<AuditEntryWire>, bool), StoreError> {
        let cap = MAX_INGEST_ROWS_PER_PULL;
        let rows = self.with_conn(|c| {
            let mut stmt = c.prepare(
                "SELECT seq, at_ms, action, subject, detail, actor, prev_hash, entry_hash
                   FROM audit_log
                  WHERE node_id = ?1 AND seq >= ?2 AND seq <= ?3
                  ORDER BY seq ASC LIMIT ?4",
            )?;
            let out = stmt
                .query_map(
                    params![
                        node.as_bytes(),
                        i64c(from_seq),
                        i64c(to_seq),
                        i64c(cap as u64 + 1)
                    ],
                    |r| {
                        let prev: Vec<u8> = r.get(6)?;
                        let hash: Vec<u8> = r.get(7)?;
                        Ok(AuditEntryWire {
                            seq: u64::try_from(r.get::<_, i64>(0)?).unwrap_or(0),
                            at_ms: u64::try_from(r.get::<_, i64>(1)?).unwrap_or(0),
                            action: r.get(2)?,
                            subject: r.get(3)?,
                            detail: r.get(4)?,
                            actor: r.get(5)?,
                            prev_hash: <[u8; 32]>::try_from(prev.as_slice()).unwrap_or([0u8; 32]),
                            entry_hash: <[u8; 32]>::try_from(hash.as_slice()).unwrap_or([0u8; 32]),
                        })
                    },
                )?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(out)
        })?;
        let over_rows = rows.len() > cap;
        let mut budget = MAX_SERVE_BYTES;
        let mut out = Vec::new();
        let mut over_bytes = false;
        for row in rows.into_iter().take(cap) {
            // Cheap upper bound on the encoded row: the variable parts
            // plus fixed framing. Exactness does not matter — this is
            // a bound, not an accounting.
            let cost = row.action.len()
                + row.subject.as_ref().map_or(0, String::len)
                + row.detail.as_ref().map_or(0, String::len)
                + row.actor.len()
                + 160;
            if !out.is_empty() && cost > budget {
                over_bytes = true;
                break;
            }
            budget = budget.saturating_sub(cost);
            out.push(row);
        }
        Ok((out, over_rows || over_bytes))
    }
}

/// Every row's hash recomputed from its OWN fields, plus the links
/// between them and the plausibility of its clock. Runs before the
/// connection is touched — a sender's `entry_hash` is never taken on
/// trust, so this is the whole reason ingest is safe.
fn validate_batch(
    node: NodeId,
    entries: &[AuditEntryWire],
    target_seq: u64,
    now_ms: u64,
) -> Result<(), IngestRefusal> {
    let Some(first) = entries.first() else {
        return Ok(());
    };
    let mut expect_seq = first.seq;
    for (i, e) in entries.iter().enumerate() {
        if e.seq != expect_seq {
            return Err(IngestRefusal::Gap {
                expected: expect_seq,
                got: e.seq,
            });
        }
        if e.at_ms > now_ms.saturating_add(MAX_INGEST_SKEW_MS) {
            return Err(IngestRefusal::Skew { seq: e.seq });
        }
        // Locally appended rows are capped; ingest checked nothing, so
        // a peer could plant ~1 MiB per row and it would render into
        // the History feed (diff review MAJOR-6).
        if e.detail
            .as_ref()
            .is_some_and(|d| d.len() > crate::audit::MAX_AUDIT_DETAIL_BYTES)
        {
            return Err(IngestRefusal::DetailTooLarge { seq: e.seq });
        }
        if e.seq > target_seq {
            return Err(IngestRefusal::PastTarget { seq: e.seq });
        }
        let Some(action) = action_from_str(&e.action) else {
            return Err(IngestRefusal::UnknownAction { seq: e.seq });
        };
        let recomputed = audit_entry_hash(
            node,
            e.seq,
            e.at_ms,
            action,
            e.subject.as_deref(),
            e.detail.as_deref(),
            &e.actor,
            &e.prev_hash,
        );
        // An encoding failure is a mismatch, not a pass.
        if recomputed.ok() != Some(e.entry_hash) {
            return Err(IngestRefusal::HashMismatch { seq: e.seq });
        }
        if i > 0 && entries[i - 1].entry_hash != e.prev_hash {
            return Err(IngestRefusal::BrokenLink { seq: e.seq });
        }
        expect_seq = expect_seq.saturating_add(1);
    }
    Ok(())
}

/// A batch must continue the run it claims to continue — by SEQUENCE
/// and by HASH — or, for a fresh run above seq 1, be anchored on
/// something we already hold.
///
/// Checking only the sequence number (Codex P1 on #67) lets a peer
/// submit individually valid but disconnected batches and finish on
/// the pinned hash. Accepting an unanchored fresh start lets a peer
/// answer "give me 1..target" with only the entry at `target`, so the
/// pin is corroborated without a single preceding entry being walked.
fn check_run_anchor(
    tx: &rusqlite::Transaction<'_>,
    node: NodeId,
    run: Option<(i64, i64)>,
    first: &AuditEntryWire,
) -> Result<Result<(), IngestRefusal>, StoreError> {
    let stored_hash = |seq: u64| -> Result<Option<[u8; 32]>, StoreError> {
        let raw: Option<Vec<u8>> = tx
            .query_row(
                "SELECT entry_hash FROM audit_log WHERE node_id = ?1 AND seq = ?2",
                params![node.as_bytes(), i64c(seq)],
                |r| r.get(0),
            )
            .optional()?;
        Ok(raw.map(|b| <[u8; 32]>::try_from(b.as_slice()).unwrap_or([0u8; 32])))
    };

    if let Some((_, through)) = run {
        let have_through = u64::try_from(through).unwrap_or(0);
        if first.seq != have_through.saturating_add(1) {
            return Ok(Err(IngestRefusal::NotContiguous {
                have_through,
                offered: first.seq,
            }));
        }
        if stored_hash(have_through)? != Some(first.prev_hash) {
            return Ok(Err(IngestRefusal::BrokenRunLink { at_seq: first.seq }));
        }
        return Ok(Ok(()));
    }

    if first.seq > 1 {
        let pinned: Option<Vec<u8>> = tx
            .query_row(
                "SELECT entry_hash FROM audit_peer_heads WHERE node_id = ?1 AND seq = ?2",
                params![node.as_bytes(), i64c(first.seq - 1)],
                |r| r.get(0),
            )
            .optional()?;
        let pinned = pinned.map(|b| <[u8; 32]>::try_from(b.as_slice()).unwrap_or([0u8; 32]));
        let anchored =
            stored_hash(first.seq - 1)? == Some(first.prev_hash) || pinned == Some(first.prev_hash);
        if !anchored {
            return Ok(Err(IngestRefusal::UnanchoredStart { seq: first.seq }));
        }
    }
    Ok(Ok(()))
}

fn i64c(v: u64) -> i64 {
    i64::try_from(v).unwrap_or(i64::MAX)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use harness_core::{AuditAction, AuditActor, AuditHead, AuditRecord, AuditSink, Signature};

    fn node(n: u8) -> NodeId {
        NodeId::from_bytes([n; 16])
    }

    /// The sink stamps real wall-clock times, so the receive clock in
    /// these tests has to be real too — otherwise every row looks like
    /// it came from the far future and trips the skew guard.
    fn now() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
            .unwrap_or(0)
    }

    /// Build a real chain in `src`, then read it back as wire entries —
    /// the same shape a peer would send.
    fn chain_of(len: u32) -> (Store, Vec<AuditEntryWire>) {
        let src = Store::open_memory().expect("store");
        let sink = crate::audit::StoreAuditSink::new(src.clone(), node(1));
        for i in 0..len {
            sink.record(
                AuditRecord::new(AuditAction::TaskDispatched, AuditActor::System)
                    .with_subject(format!("task-{i}")),
            );
        }
        let (entries, _) = src
            .audit_entries_for_range(node(1), 1, u64::from(len))
            .expect("serve");
        (src, entries)
    }

    fn pin(dst: &Store, seq: u64, hash: [u8; 32], now: u64) {
        dst.pin_peer_head(
            &AuditHead {
                node_id: node(1),
                seq,
                entry_hash: hash,
                at_ms: now,
                sig: Signature::from_bytes([9u8; 64]),
            },
            node(2),
            now,
        )
        .expect("pin");
    }

    #[test]
    fn i01_a_full_walk_to_the_pin_corroborates_it() {
        let (_src, entries) = chain_of(20);
        let dst = Store::open_memory().expect("dst");
        let last = entries.last().expect("entries");
        pin(&dst, last.seq, last.entry_hash, now());

        let out = dst
            .audit_ingest_range(node(9), node(1), last.seq, &entries, now())
            .expect("ingest")
            .expect("accepted");
        assert_eq!(out.committed, 20);
        assert!(out.reached_pin);
        assert!(dst
            .audit_run_corroborates(node(1), last.seq)
            .expect("check"));
        assert_eq!(
            dst.settle_pin_from_run(node(1), last.seq).expect("settle"),
            PinStatus::Corroborated
        );
    }

    #[test]
    fn i02_a_rewritten_chain_cannot_corroborate_the_pin_it_replaced() {
        // THE property of 5.13c-2, asserted for the right reason.
        //
        // The first version of this test queried
        // `audit_run_corroborates(node, 5)` when the run targeted 12,
        // so it returned false at the "no run row" early-out before
        // any hash was compared — it would have passed against a
        // gutted implementation (diff review BLOCKER-4).
        let (_src, entries) = chain_of(10);
        let dst = Store::open_memory().expect("dst");
        let pinned = &entries[4]; // we pinned the honest hash at seq 5
        pin(&dst, pinned.seq, pinned.entry_hash, now());

        // The node rewrites from the start and regrows. Its own chain
        // is internally consistent and verifies locally.
        let (_src2, rewritten) = {
            let src = Store::open_memory().expect("store");
            let sink = crate::audit::StoreAuditSink::new(src.clone(), node(1));
            for i in 0..12 {
                sink.record(
                    AuditRecord::new(AuditAction::TaskDispatched, AuditActor::System)
                        .with_subject(format!("REWRITTEN-{i}")),
                );
            }
            let (e, _) = src.audit_entries_for_range(node(1), 1, 12).expect("serve");
            (src, e)
        };
        assert_ne!(
            rewritten[4].entry_hash, pinned.entry_hash,
            "the rewrite really changed seq 5"
        );

        // Walking the rewritten chain toward the pin we hold at seq 5
        // hits the position we already pinned and the hashes disagree.
        let refusal = dst
            .audit_ingest_range(node(9), node(1), pinned.seq, &rewritten[..5], now())
            .expect("ingest");
        assert!(
            !dst.audit_run_corroborates(node(1), pinned.seq)
                .expect("check"),
            "a chain that does not contain our pin cannot corroborate it (ingest said {refusal:?})"
        );
    }

    #[test]
    fn i02b_a_walk_that_lands_on_the_wrong_hash_does_not_corroborate() {
        // Pins the comparison itself: the run completes, reaches
        // `target_seq`, and the stored hash there is NOT what we
        // pinned. Deleting either pin-hash check makes this pass
        // (diff review BLOCKER-4, mutants M1 and M2).
        let (_src, entries) = chain_of(8);
        let dst = Store::open_memory().expect("dst");
        let last = entries.last().expect("entries");

        // A pin at the right position carrying the WRONG hash.
        pin(&dst, last.seq, [0x5A; 32], now());

        let out = dst
            .audit_ingest_range(node(9), node(1), last.seq, &entries, now())
            .expect("ingest")
            .expect("the chain itself is sound");
        assert_eq!(out.through_seq, last.seq, "the walk did reach the target");
        assert!(
            !out.reached_pin,
            "but it did not land on the hash we pinned"
        );
        assert!(
            !dst.audit_run_corroborates(node(1), last.seq)
                .expect("check"),
            "and corroboration must say so"
        );
        assert_eq!(
            dst.settle_pin_from_run(node(1), last.seq).expect("settle"),
            PinStatus::Unchecked
        );
    }

    #[test]
    fn i02c_an_anchor_pin_with_the_wrong_hash_does_not_verify() {
        // The third surviving mutant (M3): `audit_verify_range`
        // accepting any pin at `from_seq - 1` rather than one whose
        // hash matches the run's first `prev_hash`.
        let (_src, entries) = chain_of(20);
        let dst = Store::open_memory().expect("dst");
        let last = entries.last().expect("entries");
        pin(&dst, last.seq, last.entry_hash, now());
        // Anchor pin at seq 9, wrong hash.
        pin(&dst, 9, [0x77; 32], now());

        assert_eq!(
            dst.audit_ingest_range(node(9), node(1), last.seq, &entries[9..], now())
                .expect("ingest")
                .expect_err("refuse"),
            IngestRefusal::UnanchoredStart { seq: 10 },
            "a mismatched anchor is not an anchor"
        );

        // Force the rows in and check the verifier independently.
        for e in &entries[9..] {
            dst.with_conn(|c| {
                c.execute(
                    "INSERT INTO audit_log(node_id, seq, at_ms, action, subject,
                                           detail, actor, prev_hash, entry_hash,
                                           received_at_ms)
                     VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
                    params![
                        node(1).as_bytes(),
                        i64c(e.seq),
                        i64c(e.at_ms),
                        e.action,
                        e.subject,
                        e.detail,
                        e.actor,
                        e.prev_hash.as_slice(),
                        e.entry_hash.as_slice(),
                        i64c(now()),
                    ],
                )?;
                Ok(())
            })
            .expect("force insert");
        }
        assert_eq!(
            dst.audit_verify_range(node(1), 10, 1_000).expect("verify"),
            crate::audit::ChainStatus::Broken { at_seq: 10 },
            "the pin at seq 9 does not match the run's prev_hash"
        );
    }

    #[test]
    fn i02d_tampering_after_a_walk_revokes_corroboration() {
        // Corroboration is re-derived on every read, not cached. A run
        // that once completed must stop corroborating the moment the
        // rows it walked stop hashing up to the pin.
        let (_src, entries) = chain_of(8);
        let dst = Store::open_memory().expect("dst");
        let last = entries.last().expect("entries");
        pin(&dst, last.seq, last.entry_hash, now());

        dst.audit_ingest_range(node(9), node(1), last.seq, &entries, now())
            .expect("ingest")
            .expect("accepted");
        assert!(dst
            .audit_run_corroborates(node(1), last.seq)
            .expect("check"));

        // Edit a walked row out from under the completed run.
        dst.with_conn(|c| {
            c.execute(
                "UPDATE audit_log SET subject = 'tampered'
                  WHERE node_id = ?1 AND seq = 4",
                params![node(1).as_bytes()],
            )?;
            Ok(())
        })
        .expect("tamper");

        assert!(
            !dst.audit_run_corroborates(node(1), last.seq)
                .expect("check"),
            "the run marker still says complete; the rows no longer verify"
        );
        assert_eq!(
            dst.settle_pin_from_run(node(1), last.seq).expect("settle"),
            PinStatus::Unchecked
        );
    }

    #[test]
    fn i03_a_peer_cannot_write_our_own_chain() {
        let (_src, entries) = chain_of(3);
        let dst = Store::open_memory().expect("dst");
        let refusal = dst
            .audit_ingest_range(node(1), node(1), 3, &entries, now())
            .expect("ingest")
            .expect_err("must refuse");
        assert_eq!(refusal, IngestRefusal::OwnChain);
    }

    #[test]
    fn i04_a_tampered_entry_is_refused() {
        let (_src, mut entries) = chain_of(5);
        entries[2].subject = Some("tampered".into());
        let dst = Store::open_memory().expect("dst");
        let refusal = dst
            .audit_ingest_range(node(9), node(1), 5, &entries, now())
            .expect("ingest")
            .expect_err("must refuse");
        assert_eq!(refusal, IngestRefusal::HashMismatch { seq: 3 });
    }

    #[test]
    fn i05_a_hole_and_a_broken_link_are_refused() {
        let (_src, entries) = chain_of(6);
        let dst = Store::open_memory().expect("dst");

        let mut holed = entries.clone();
        holed.remove(2);
        assert_eq!(
            dst.audit_ingest_range(node(9), node(1), 6, &holed, now())
                .expect("ingest")
                .expect_err("refuse"),
            IngestRefusal::Gap {
                expected: 3,
                got: 4
            }
        );

        // A row whose hash is self-consistent but whose prev_hash does
        // not match its predecessor.
        let mut relinked = entries.clone();
        relinked[3].prev_hash = [0x77; 32];
        let refusal = dst
            .audit_ingest_range(node(9), node(1), 6, &relinked, now())
            .expect("ingest")
            .expect_err("refuse");
        assert_eq!(
            refusal,
            IngestRefusal::HashMismatch { seq: 4 },
            "changing prev_hash changes the entry hash, caught before the link check"
        );
    }

    #[test]
    fn i06_an_unknown_action_is_refused() {
        let (_src, mut entries) = chain_of(3);
        entries[1].action = "future.action".into();
        let dst = Store::open_memory().expect("dst");
        assert_eq!(
            dst.audit_ingest_range(node(9), node(1), 3, &entries, now())
                .expect("ingest")
                .expect_err("refuse"),
            IngestRefusal::UnknownAction { seq: 2 }
        );
    }

    #[test]
    fn i07_a_row_from_the_far_future_is_refused() {
        // `at_ms` is inside the peer's own hash preimage, so it is
        // attacker-chosen, and the merged feed orders by it.
        let (_src, mut entries) = chain_of(2);
        entries[1].at_ms = u64::MAX;
        let dst = Store::open_memory().expect("dst");
        let refusal = dst
            .audit_ingest_range(node(9), node(1), 2, &entries, now())
            .expect("ingest")
            .expect_err("refuse");
        assert_eq!(refusal, IngestRefusal::Skew { seq: 2 });
    }

    #[test]
    fn i08_a_partial_run_does_not_corroborate_until_it_reaches_the_pin() {
        // All-or-nothing per batch plus a row cap would make the final
        // batch unreachable, so batches commit as they link — and the
        // run marker is what stops a half-finished walk from reading
        // as corroboration.
        let (_src, entries) = chain_of(20);
        let dst = Store::open_memory().expect("dst");
        let last = entries.last().expect("entries");
        pin(&dst, last.seq, last.entry_hash, now());

        let first_half = &entries[..12];
        let out = dst
            .audit_ingest_range(node(9), node(1), last.seq, first_half, now())
            .expect("ingest")
            .expect("accepted");
        assert!(!out.reached_pin);
        assert!(
            !dst.audit_run_corroborates(node(1), last.seq)
                .expect("check"),
            "a partial walk is not corroboration"
        );
        assert_eq!(
            dst.settle_pin_from_run(node(1), last.seq).expect("settle"),
            PinStatus::Unchecked
        );

        // A batch that does not continue the run is refused.
        assert_eq!(
            dst.audit_ingest_range(node(9), node(1), last.seq, &entries[15..], now())
                .expect("ingest")
                .expect_err("refuse"),
            IngestRefusal::NotContiguous {
                have_through: 12,
                offered: 16
            }
        );

        // Continuing properly finishes the walk.
        let out = dst
            .audit_ingest_range(node(9), node(1), last.seq, &entries[12..], now())
            .expect("ingest")
            .expect("accepted");
        assert!(out.reached_pin);
        assert!(dst
            .audit_run_corroborates(node(1), last.seq)
            .expect("check"));
    }

    #[test]
    fn i09_serving_is_capped_and_reports_truncation() {
        let (src, _) = chain_of(20);
        let (rows, truncated) = src.audit_entries_for_range(node(1), 1, 20).expect("serve");
        assert_eq!(rows.len(), 20);
        assert!(!truncated);
        assert!(rows.len() <= MAX_INGEST_ROWS_PER_PULL);

        let (rows, _) = src.audit_entries_for_range(node(1), 5, 9).expect("serve");
        assert_eq!(rows.len(), 5);
        assert_eq!(rows[0].seq, 5);
    }

    #[test]
    fn i10_ingested_rows_are_stamped_with_our_clock() {
        // The merged feed must order on our clock for their rows, or
        // one row stamped far in the future owns page 1 forever.
        let (_src, entries) = chain_of(3);
        let dst = Store::open_memory().expect("dst");
        let received = now();
        dst.audit_ingest_range(node(9), node(1), 3, &entries, received)
            .expect("ingest")
            .expect("accepted");
        let stamped: Vec<Option<i64>> = dst
            .with_conn(|c| {
                let mut stmt =
                    c.prepare("SELECT received_at_ms FROM audit_log WHERE node_id = ?1")?;
                let v = stmt
                    .query_map(params![node(1).as_bytes()], |r| r.get(0))?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(v)
            })
            .expect("read");
        assert!(
            stamped
                .iter()
                .all(|v| *v == Some(i64::try_from(received).unwrap_or(i64::MAX))),
            "every ingested row carries OUR receive time, got {stamped:?}"
        );
    }

    #[test]
    fn i11_a_partial_pull_verifies_when_anchored_on_a_pin() {
        // `audit_verify_chain` reports Broken for a run that does not
        // start at seq 1 and has no stored predecessor — which is
        // every legitimate partial pull, so honest replication would
        // read as tampering. `audit_verify_range` accepts a pin WE
        // took as the fourth anchor.
        let (_src, entries) = chain_of(20);
        let dst = Store::open_memory().expect("dst");

        // We hold a pin at seq 9 — the predecessor of the run.
        let anchor = &entries[8];
        pin(&dst, anchor.seq, anchor.entry_hash, now());
        let last = entries.last().expect("entries");
        pin(&dst, last.seq, last.entry_hash, now());

        // Ingest only 10..20.
        dst.audit_ingest_range(node(9), node(1), last.seq, &entries[9..], now())
            .expect("ingest")
            .expect("accepted");

        assert_eq!(
            dst.audit_verify_chain(node(1), 10, 1_000).expect("verify"),
            crate::audit::ChainStatus::Broken { at_seq: 10 },
            "the old function cannot anchor a partial pull"
        );
        assert_eq!(
            dst.audit_verify_range(node(1), 10, 1_000).expect("verify"),
            crate::audit::ChainStatus::Verified { through_seq: 20 },
            "anchored on the pin we hold at seq 9"
        );
    }

    #[test]
    fn i12_an_unanchored_run_is_refused_and_would_not_verify_anyway() {
        // Two layers, both required. Ingest refuses a fresh run above
        // seq 1 with nothing anchoring it (i15) — and if rows reach
        // the table by some other route, `audit_verify_range` still
        // refuses to verify them, because the pin that would anchor
        // the walk is not there.
        let (_src, entries) = chain_of(20);
        let dst = Store::open_memory().expect("dst");
        let last = entries.last().expect("entries");
        pin(&dst, last.seq, last.entry_hash, now());

        assert_eq!(
            dst.audit_ingest_range(node(9), node(1), last.seq, &entries[9..], now())
                .expect("ingest")
                .expect_err("refuse"),
            IngestRefusal::UnanchoredStart { seq: 10 },
            "no pin or row at seq 9 to anchor the run"
        );

        // Force the rows in behind ingest's back, then check the
        // verifier independently.
        for e in &entries[9..] {
            dst.with_conn(|c| {
                c.execute(
                    "INSERT INTO audit_log(node_id, seq, at_ms, action, subject,
                                           detail, actor, prev_hash, entry_hash,
                                           received_at_ms)
                     VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
                    params![
                        node(1).as_bytes(),
                        i64c(e.seq),
                        i64c(e.at_ms),
                        e.action,
                        e.subject,
                        e.detail,
                        e.actor,
                        e.prev_hash.as_slice(),
                        e.entry_hash.as_slice(),
                        i64c(now()),
                    ],
                )?;
                Ok(())
            })
            .expect("force insert");
        }
        assert_eq!(
            dst.audit_verify_range(node(1), 10, 1_000).expect("verify"),
            crate::audit::ChainStatus::Broken { at_seq: 10 },
            "nothing anchors the run, so it cannot verify"
        );
    }

    #[test]
    fn i13_a_tampered_ingested_row_breaks_the_anchored_walk() {
        let (_src, entries) = chain_of(20);
        let dst = Store::open_memory().expect("dst");
        let anchor = &entries[8];
        pin(&dst, anchor.seq, anchor.entry_hash, now());
        let last = entries.last().expect("entries");
        pin(&dst, last.seq, last.entry_hash, now());
        dst.audit_ingest_range(node(9), node(1), last.seq, &entries[9..], now())
            .expect("ingest")
            .expect("accepted");

        // Edit a stored peer row out from under us.
        dst.with_conn(|c| {
            c.execute(
                "UPDATE audit_log SET subject = 'tampered'
                  WHERE node_id = ?1 AND seq = 15",
                params![node(1).as_bytes()],
            )?;
            Ok(())
        })
        .expect("tamper");

        assert_eq!(
            dst.audit_verify_range(node(1), 10, 1_000).expect("verify"),
            crate::audit::ChainStatus::Broken { at_seq: 15 }
        );
    }

    #[test]
    fn i14_a_batch_must_link_by_hash_not_just_by_number() {
        // Codex P1 on #67: checking only `seq == through + 1` lets a
        // peer submit individually valid but DISCONNECTED batches and
        // finish on the pinned hash. Each batch verifies internally,
        // the numbers line up, and the run reports corroboration for a
        // chain that is broken across the seams.
        let (_src, entries) = chain_of(20);
        let dst = Store::open_memory().expect("dst");
        let last = entries.last().expect("entries");
        pin(&dst, last.seq, last.entry_hash, now());

        dst.audit_ingest_range(node(9), node(1), last.seq, &entries[..10], now())
            .expect("ingest")
            .expect("accepted");

        // A second chain, valid on its own, whose seq 11 does not
        // follow the seq 10 we committed.
        let (_other, foreign) = chain_of(20);
        let refusal = dst
            .audit_ingest_range(node(9), node(1), last.seq, &foreign[10..], now())
            .expect("ingest")
            .expect_err("must refuse");
        assert_eq!(refusal, IngestRefusal::BrokenRunLink { at_seq: 11 });
        assert!(
            !dst.audit_run_corroborates(node(1), last.seq)
                .expect("check"),
            "the stitched run must not corroborate"
        );
    }

    #[test]
    fn i15_a_fresh_run_above_seq_one_needs_an_anchor() {
        // The other half of the same attack (Codex P1 on #67): a
        // malicious subject answers a request for 1..target with ONLY
        // the entry at target. With no anchor requirement the final
        // hash matches the pin and the pin is corroborated without a
        // single preceding entry being walked.
        let (_src, entries) = chain_of(20);
        let dst = Store::open_memory().expect("dst");
        let last = entries.last().expect("entries");
        pin(&dst, last.seq, last.entry_hash, now());

        let refusal = dst
            .audit_ingest_range(node(9), node(1), last.seq, &entries[19..], now())
            .expect("ingest")
            .expect_err("must refuse");
        assert_eq!(refusal, IngestRefusal::UnanchoredStart { seq: 20 });
        assert!(!dst
            .audit_run_corroborates(node(1), last.seq)
            .expect("check"));

        // With a pin at the predecessor, the same batch anchors.
        pin(&dst, entries[18].seq, entries[18].entry_hash, now());
        let out = dst
            .audit_ingest_range(node(9), node(1), last.seq, &entries[19..], now())
            .expect("ingest")
            .expect("anchored on the pin at seq 19");
        assert!(out.reached_pin);
    }

    #[test]
    fn i16_the_feed_orders_ingested_rows_by_our_receive_time() {
        // Codex P2 on #67: `received_at_ms` and its index existed but
        // `audit_recent` still ordered on `at_ms`, so an ingested row
        // could choose its own position in every node's History.
        let dst = Store::open_memory().expect("dst");

        // A peer row whose own at_ms is nearly a day ahead — inside
        // the skew allowance, so it is accepted — but received now.
        let (_src, mut entries) = chain_of(1);
        entries[0].at_ms = now() + MAX_INGEST_SKEW_MS - 60_000;
        // Re-hash so the doctored row is internally valid.
        let action = crate::audit::action_from_str(&entries[0].action).expect("action");
        entries[0].entry_hash = harness_core::audit_entry_hash(
            node(1),
            entries[0].seq,
            entries[0].at_ms,
            action,
            entries[0].subject.as_deref(),
            entries[0].detail.as_deref(),
            &entries[0].actor,
            &entries[0].prev_hash,
        )
        .expect("hash");

        dst.audit_ingest_range(node(9), node(1), 1, &entries, now())
            .expect("ingest")
            .expect("accepted");

        // Recorded AFTER the peer row was received, so on our clock it
        // is genuinely newer — while the peer's own stamp is a day
        // ahead of both.
        let sink = crate::audit::StoreAuditSink::new(dst.clone(), node(5));
        sink.record(
            AuditRecord::new(AuditAction::TaskDispatched, AuditActor::System)
                .with_subject("local-now"),
        );

        let rows = dst.audit_recent(None, None, None, 10).expect("recent");
        assert_eq!(
            rows[0].node_id,
            node(5),
            "the local row stays on top; the peer's future at_ms buys nothing"
        );
        let peer_row = rows
            .iter()
            .find(|r| r.node_id == node(1))
            .expect("peer row");
        assert!(
            peer_row.feed_ms < peer_row.at_ms,
            "the peer row is ordered by our receive time, not its own stamp"
        );
    }
}
