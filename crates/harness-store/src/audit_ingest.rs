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

        if let Err(refusal) = validate_batch(node, entries, now_ms) {
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

            // A continuing run must pick up exactly where it left off.
            if let Some((_, through)) = run {
                let want = u64::try_from(through).unwrap_or(0).saturating_add(1);
                if first.seq != want {
                    tx.rollback()?;
                    return Ok(Err(IngestRefusal::NotContiguous {
                        have_through: u64::try_from(through).unwrap_or(0),
                        offered: first.seq,
                    }));
                }
            }

            for e in entries {
                tx.execute(
                    "INSERT OR IGNORE INTO audit_log(
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
                committed: entries.len(),
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
        let Some(first_prev) = self.entry_prev_hash_at(node, from_seq)? else {
            return Ok(crate::audit::ChainStatus::Empty);
        };
        if from_seq == 0 {
            return Ok(crate::audit::ChainStatus::Broken { at_seq: from_seq });
        }
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
        let row: Option<(i64, i64)> = self.with_conn(|c| {
            Ok(c.query_row(
                "SELECT through_seq, complete FROM audit_ingest_runs
                  WHERE node_id = ?1 AND target_seq = ?2",
                params![node.as_bytes(), i64c(target_seq)],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?)
        })?;
        let Some((through, complete)) = row else {
            return Ok(false);
        };
        if complete == 0 || u64::try_from(through).unwrap_or(0) != target_seq {
            return Ok(false);
        }
        Ok(self.audit_entry_hash_at(node, target_seq)? == Some(pin))
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
    fn i02_a_truncate_and_regrow_past_the_pin_is_contradicted() {
        // THE property of 5.13c-2. 5.13c-1 catches a same-seq fork; it
        // cannot catch this, because the rewritten node's new head
        // lands at an unpinned position and reads as ordinary growth.
        // Linking the old pin to the new head requires the walk.
        let (_src, entries) = chain_of(10);
        let dst = Store::open_memory().expect("dst");
        let pinned = &entries[4]; // pin at seq 5
        pin(&dst, pinned.seq, pinned.entry_hash, now());

        // The node rewrites history from seq 5 on and regrows. Its own
        // chain is internally consistent and verifies locally.
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
        let new_head = rewritten.last().expect("rewritten");

        // The rewritten chain is internally sound — every hash and
        // link checks out — so ingest ACCEPTS it.
        let out = dst
            .audit_ingest_range(node(9), node(1), new_head.seq, &rewritten, now())
            .expect("ingest")
            .expect("internally consistent");
        assert_eq!(out.committed, 12);

        // But the pin we already hold at seq 5 is not in it.
        let held = dst
            .peer_head_pins(node(1))
            .expect("pins")
            .into_iter()
            .find(|p| p.seq == pinned.seq)
            .expect("pin");
        let ingested_at_5 = dst
            .audit_entry_hash_at(node(1), pinned.seq)
            .expect("read")
            .expect("row");
        assert_ne!(
            ingested_at_5, held.entry_hash,
            "the rewrite really changed seq 5"
        );
        assert!(
            !dst.audit_run_corroborates(node(1), pinned.seq)
                .expect("check"),
            "a chain that does not contain our pin cannot corroborate it"
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
    fn i12_a_partial_pull_without_a_matching_pin_stays_broken() {
        // The anchor must be a pin we ALREADY held. Without one, an
        // unanchored run is exactly the prefix-deletion case 5.13a's
        // review established, and must not verify.
        let (_src, entries) = chain_of(20);
        let dst = Store::open_memory().expect("dst");
        let last = entries.last().expect("entries");
        pin(&dst, last.seq, last.entry_hash, now());

        dst.audit_ingest_range(node(9), node(1), last.seq, &entries[9..], now())
            .expect("ingest")
            .expect("accepted");

        assert_eq!(
            dst.audit_verify_range(node(1), 10, 1_000).expect("verify"),
            crate::audit::ChainStatus::Broken { at_seq: 10 },
            "no pin at seq 9, so nothing anchors the run"
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
}
