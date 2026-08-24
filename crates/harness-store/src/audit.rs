//! 5.13a (ADR-0041): the per-node hash chain behind PRD §10.6.
//!
//! Every privileged action appends one row to THIS node's chain.
//! `entry_hash` covers the entry's fields plus its position
//! (`node_id`, `seq`) and its predecessor, so any edit to a stored row
//! breaks verification from that point forward.
//!
//! What this proves, precisely: the chain is tamper-evident against
//! edits made OUTSIDE the daemon — `sqlite3` at the shell, bit rot, a
//! restore from a bad backup. It is NOT evidence against the node
//! itself, which holds both the database and the signing key and can
//! rebuild its own chain end to end. What makes it evidence is a peer
//! having pinned `(seq, entry_hash)` earlier — hence
//! [`Store::signed_audit_head`] now and replication in 5.13c.

use harness_core::{
    audit_entry_hash, AuditAction, AuditActor, AuditHead, AuditRecord, AuditSink, Identity, NodeId,
    Signable, Signature,
};
use rusqlite::{params, OptionalExtension};

use crate::error::StoreError;
use crate::open::Store;

/// Cap on a stored `detail` (mirrored by the column CHECK). Details
/// are hashes and identifiers, never payloads.
pub const MAX_AUDIT_DETAIL_BYTES: usize = 4096;

/// 5.13b (ADR-0041 follow-up): how long an identical repeat of a
/// FLOODABLE action is rate-limited per actor.
///
/// `shell.denied` is appended once per attempt and is
/// attacker-triggerable at submit rate — `rate_limit` is declared in
/// capability manifests but enforced nowhere — so an unbounded
/// append lets a peer push genuine entries out of the retention
/// window one denial at a time.
///
/// The rate limit is keyed `(action, actor)` and NOT by subject
/// (diff review BLOCKER-1 on #65): `subject` for a denial is the
/// submitted command, which the adversary chooses. Keying on it means
/// `/bin/x1`, `/bin/x2`, … mints a fresh key per attempt — the flood
/// passes untouched while the only records dropped are a real
/// operator's repeated denials of ONE command. That trade is exactly
/// backwards.
///
/// Within a window the first [`BURST_ALLOWANCE`] records append IN
/// FULL, so distinct denials — different argv, different task — are
/// still recorded individually at any rate a human produces. Past the
/// allowance the record is dropped and counted, with a bounded sample
/// of what was dropped, and when the window CLOSES a summary entry is
/// appended carrying the counts, the sample and the window's own
/// bounds. Closing runs on the housekeeping tick as well as inline,
/// because a burst that simply STOPS must still be recorded (Codex P1
/// on #65) — the count cannot wait for a later attempt that may never
/// come.
pub const SUPPRESSION_WINDOW_MS: u64 = 60_000;

/// Full appends allowed per `(action, actor)` per window before
/// coalescing starts. Ten denials a minute from one actor is already
/// far past anything a person does by hand; a flooder is bounded to
/// this many rows per minute instead of one per attempt.
pub const BURST_ALLOWANCE: u64 = 10;

/// Distinct subjects sampled into a summary entry. The subjects are
/// already in the log's own vocabulary (a denial's subject is a
/// command name), so sampling discloses nothing new — it just keeps
/// the summary from becoming an unbounded row.
const SAMPLE_SUBJECTS: usize = 8;

/// Distinct subjects counted exactly before the summary reports a
/// floor instead ("at least N"). Bounds the per-window set.
const DISTINCT_CAP: usize = 64;

/// Truncation for a sampled subject. A submitted command is bounded
/// only by the 64 KiB frame cap; the key holds no subject at all now,
/// but the sample must not become the leak the key used to be
/// (diff review MINOR-10).
const SAMPLE_SUBJECT_BYTES: usize = 128;

/// Actions that are cheap for an adversary to trigger.
fn is_floodable(action: AuditAction) -> bool {
    matches!(action, AuditAction::ShellDenied)
}

/// One row of the log, as read back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditRow {
    pub node_id: NodeId,
    pub seq: u64,
    pub at_ms: u64,
    pub action: String,
    pub subject: Option<String>,
    pub detail: Option<String>,
    pub actor: String,
    pub prev_hash: [u8; 32],
    pub entry_hash: [u8; 32],
}

/// Keyset cursor for [`Store::audit_recent`] — the full ordering key,
/// so a page boundary inside a same-millisecond burst does not skip
/// rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuditCursor {
    pub at_ms: u64,
    pub node: NodeId,
    pub seq: u64,
}

/// Outcome of walking a chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChainStatus {
    /// Every entry checked links to its predecessor.
    Verified { through_seq: u64 },
    /// The chain breaks at this seq — the row was edited, or an entry
    /// is missing with no truncation marker to explain it.
    Broken { at_seq: u64 },
    /// No entries yet.
    Empty,
}

fn to_hash(raw: &[u8]) -> [u8; 32] {
    <[u8; 32]>::try_from(raw).unwrap_or([0u8; 32])
}

fn row_to_audit(r: &rusqlite::Row<'_>) -> rusqlite::Result<AuditRow> {
    let node_raw: Vec<u8> = r.get(0)?;
    let prev: Vec<u8> = r.get(7)?;
    let entry: Vec<u8> = r.get(8)?;
    Ok(AuditRow {
        node_id: NodeId::from_bytes(<[u8; 16]>::try_from(node_raw.as_slice()).unwrap_or([0u8; 16])),
        seq: u64::try_from(r.get::<_, i64>(1)?).unwrap_or(0),
        at_ms: u64::try_from(r.get::<_, i64>(2)?).unwrap_or(0),
        action: r.get(3)?,
        subject: r.get(4)?,
        detail: r.get(5)?,
        actor: r.get(6)?,
        prev_hash: to_hash(&prev),
        entry_hash: to_hash(&entry),
    })
}

impl Store {
    /// Append one entry to `node_id`'s chain.
    ///
    /// Head read and insert share ONE transaction (plan review
    /// MINOR-5): the chain's no-fork property must be structural, not
    /// a convention every future caller has to remember. Returns the
    /// new entry's seq.
    ///
    /// # Errors
    /// Underlying sqlite errors, or an unencodable entry.
    pub fn audit_append(
        &self,
        node_id: NodeId,
        record: &AuditRecord,
        at_ms: u64,
    ) -> Result<u64, StoreError> {
        let actor = record.actor.as_string();
        let detail = record.detail.as_ref().and_then(|d| {
            if d.len() > MAX_AUDIT_DETAIL_BYTES {
                tracing::warn!(
                    target: "harness.store.audit",
                    bytes = d.len(),
                    "audit detail over cap; dropped"
                );
                None
            } else {
                Some(d.clone())
            }
        });
        let action = record.action;
        let subject = record.subject.clone();
        self.with_conn(move |c| {
            let tx = c.unchecked_transaction()?;
            let head: Option<(i64, Vec<u8>)> = tx
                .query_row(
                    "SELECT seq, entry_hash FROM audit_log
                      WHERE node_id = ?1 ORDER BY seq DESC LIMIT 1",
                    params![node_id.as_bytes()],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;
            let (seq, prev_hash) = match head {
                Some((s, h)) => (u64::try_from(s).unwrap_or(0) + 1, to_hash(&h)),
                None => (1, [0u8; 32]),
            };
            let entry_hash = audit_entry_hash(
                node_id,
                seq,
                at_ms,
                action,
                subject.as_deref(),
                detail.as_deref(),
                &actor,
                &prev_hash,
            )
            .map_err(|e| StoreError::Cbor(format!("audit hash: {e}")))?;
            tx.execute(
                "INSERT INTO audit_log
                    (node_id, seq, at_ms, action, subject, detail, actor, prev_hash, entry_hash)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    node_id.as_bytes(),
                    i64::try_from(seq).unwrap_or(i64::MAX),
                    i64::try_from(at_ms).unwrap_or(i64::MAX),
                    action.as_str(),
                    subject,
                    detail,
                    actor,
                    &prev_hash[..],
                    &entry_hash[..],
                ],
            )?;
            tx.commit()?;
            Ok(seq)
        })
    }

    /// Walk `node_id`'s chain from `from_seq` (inclusive) and confirm
    /// every link. A gap is only a break when no `audit.truncated`
    /// marker explains it (plan review BLOCKER-2): retention prunes
    /// through a marker that carries the anchor hash, so a pruned
    /// chain must still verify — otherwise every node that ever hit
    /// the retention bound would show a permanent BROKEN banner and
    /// operators would learn to ignore the one signal that matters.
    ///
    /// # Errors
    /// Underlying sqlite errors.
    pub fn audit_verify_chain(
        &self,
        node_id: NodeId,
        from_seq: u64,
        max_rows: usize,
    ) -> Result<ChainStatus, StoreError> {
        let rows = self.with_conn(|c| {
            let mut stmt = c.prepare(
                "SELECT node_id, seq, at_ms, action, subject, detail, actor,
                        prev_hash, entry_hash
                   FROM audit_log
                  WHERE node_id = ?1 AND seq >= ?2
               ORDER BY seq ASC
                  LIMIT ?3",
            )?;
            let rows = stmt
                .query_map(
                    params![
                        node_id.as_bytes(),
                        i64::try_from(from_seq).unwrap_or(i64::MAX),
                        i64::try_from(max_rows).unwrap_or(i64::MAX),
                    ],
                    row_to_audit,
                )?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(rows)
        })?;
        let Some(first) = rows.first() else {
            return Ok(ChainStatus::Empty);
        };
        // Anchor the run before walking it (Codex P1 on #64). Without
        // this, deleting the chain's PREFIX verifies clean: the oldest
        // surviving row supplies both its own `prev_hash` and the
        // expected seq, so the walk starts wherever the deletion left
        // off — exactly the case the truncation marker exists to
        // distinguish from legitimate pruning.
        if !self.anchor_holds(node_id, first)? {
            return Ok(ChainStatus::Broken { at_seq: first.seq });
        }
        Ok(verify_rows(node_id, &rows))
    }

    /// Is this row a legitimate start for a verification run?
    ///
    /// Three ways it can be: it is the genesis entry (`seq` 1 linked to
    /// the zero hash); its immediate predecessor is still stored and
    /// its hash matches (the caller simply started mid-chain); or a
    /// truncation marker states that the chain was pruned through
    /// exactly this predecessor. Anything else means rows are missing
    /// with nothing accounting for them.
    ///
    /// A locally-forged marker satisfies this, of course — that is the
    /// limit ADR-0041 states: the chain is evidence only once peers
    /// have pinned a head.
    fn anchor_holds(&self, node_id: NodeId, first: &AuditRow) -> Result<bool, StoreError> {
        if first.seq == 1 {
            return Ok(first.prev_hash == [0u8; 32]);
        }
        if let Some(prev) = self.audit_entry_hash_at(node_id, first.seq - 1)? {
            return Ok(prev == first.prev_hash);
        }
        let want_hex = harness_core::hash_hex(&first.prev_hash);
        let markers = self.with_conn(|c| {
            let mut stmt = c.prepare(
                "SELECT detail FROM audit_log
                  WHERE node_id = ?1 AND action = 'audit.truncated'",
            )?;
            let rows = stmt
                .query_map(params![node_id.as_bytes()], |r| {
                    r.get::<_, Option<String>>(0)
                })?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(rows)
        })?;
        Ok(markers.into_iter().flatten().any(|detail| {
            serde_json::from_str::<serde_json::Value>(&detail).is_ok_and(|d| {
                d.get("through_seq").and_then(serde_json::Value::as_u64) == Some(first.seq - 1)
                    && d.get("through_hash").and_then(serde_json::Value::as_str)
                        == Some(want_hex.as_str())
            })
        }))
    }

    /// The signed head of this node's chain — what a peer pins so a
    /// later rewrite is detectable (5.13c gossips it).
    ///
    /// # Errors
    /// Underlying sqlite errors, or a signing failure.
    pub fn signed_audit_head(
        &self,
        identity: &Identity,
        at_ms: u64,
    ) -> Result<Option<AuditHead>, StoreError> {
        let node_id = identity.node_id();
        let head: Option<(i64, Vec<u8>)> = self.with_conn(|c| {
            Ok(c.query_row(
                "SELECT seq, entry_hash FROM audit_log
                  WHERE node_id = ?1 ORDER BY seq DESC LIMIT 1",
                params![node_id.as_bytes()],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?)
        })?;
        let Some((seq, hash)) = head else {
            return Ok(None);
        };
        let mut out = AuditHead {
            node_id,
            seq: u64::try_from(seq).unwrap_or(0),
            entry_hash: to_hash(&hash),
            at_ms,
            sig: Signature::from_bytes([0u8; 64]),
        };
        out.sign(identity)
            .map_err(|e| StoreError::Cbor(format!("sign audit head: {e}")))?;
        Ok(Some(out))
    }

    /// Prune this node's chain to at most `keep` entries.
    ///
    /// Order matters (plan review BLOCKER-2): the `audit.truncated`
    /// marker naming the anchor is appended FIRST, so it is itself
    /// covered by the chain, and only then are the old rows deleted.
    /// Returns the number of rows removed.
    ///
    /// # Errors
    /// Underlying sqlite errors.
    pub fn audit_prune(&self, node_id: NodeId, keep: u64, at_ms: u64) -> Result<usize, StoreError> {
        let Some((head_seq, _)) = self.audit_head_raw(node_id)? else {
            return Ok(0);
        };
        if head_seq <= keep {
            return Ok(0);
        }
        // The marker is entry head+1, so everything through `cutoff`
        // can go once it is written.
        let cutoff = head_seq - keep;
        let Some(anchor) = self.audit_entry_hash_at(node_id, cutoff)? else {
            return Ok(0);
        };
        let record = AuditRecord::new(
            AuditAction::AuditTruncated,
            harness_core::AuditActor::System,
        )
        .with_detail(&serde_json::json!({
            "through_seq": cutoff,
            "through_hash": hex::encode(anchor),
        }));
        self.audit_append(node_id, &record, at_ms)?;
        self.with_conn(|c| {
            let n = c.execute(
                "DELETE FROM audit_log WHERE node_id = ?1 AND seq <= ?2",
                params![
                    node_id.as_bytes(),
                    i64::try_from(cutoff).unwrap_or(i64::MAX)
                ],
            )?;
            Ok(n)
        })
    }

    fn audit_head_raw(&self, node_id: NodeId) -> Result<Option<(u64, [u8; 32])>, StoreError> {
        self.with_conn(|c| {
            let row: Option<(i64, Vec<u8>)> = c
                .query_row(
                    "SELECT seq, entry_hash FROM audit_log
                      WHERE node_id = ?1 ORDER BY seq DESC LIMIT 1",
                    params![node_id.as_bytes()],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;
            Ok(row.map(|(s, h)| (u64::try_from(s).unwrap_or(0), to_hash(&h))))
        })
    }

    fn audit_entry_hash_at(
        &self,
        node_id: NodeId,
        seq: u64,
    ) -> Result<Option<[u8; 32]>, StoreError> {
        self.with_conn(|c| {
            let row: Option<Vec<u8>> = c
                .query_row(
                    "SELECT entry_hash FROM audit_log WHERE node_id = ?1 AND seq = ?2",
                    params![node_id.as_bytes(), i64::try_from(seq).unwrap_or(i64::MAX)],
                    |r| r.get(0),
                )
                .optional()?;
            Ok(row.map(|h| to_hash(&h)))
        })
    }

    /// Recent entries across ALL nodes, newest first — the History
    /// listing. Keyset paging on `(at_ms, node_id, seq)`, because
    /// `seq` alone is meaningless once two chains interleave.
    ///
    /// # Errors
    /// Underlying sqlite errors.
    pub fn audit_recent(
        &self,
        cursor: Option<AuditCursor>,
        action: Option<&str>,
        node: Option<NodeId>,
        limit: usize,
    ) -> Result<Vec<AuditRow>, StoreError> {
        // The cursor carries the WHOLE ordering key (Codex P2 on #64).
        // `at_ms` alone silently drops every row sharing the last
        // row's millisecond once a burst exceeds one page — and
        // millisecond precision makes such bursts ordinary.
        let (cur_ms, cur_node, cur_seq) = match cursor {
            Some(c) => (
                Some(i64::try_from(c.at_ms).unwrap_or(i64::MAX)),
                Some(c.node.as_bytes().to_vec()),
                Some(i64::try_from(c.seq).unwrap_or(i64::MAX)),
            ),
            None => (None, None, None),
        };
        self.with_conn(|c| {
            let mut stmt = c.prepare(
                "SELECT node_id, seq, at_ms, action, subject, detail, actor,
                        prev_hash, entry_hash
                   FROM audit_log
                  WHERE (?1 IS NULL
                         OR at_ms < ?1
                         OR (at_ms = ?1
                             AND (node_id < ?2
                                  OR (node_id = ?2 AND seq < ?3))))
                    AND (?4 IS NULL OR action = ?4)
                    AND (?5 IS NULL OR node_id = ?5)
               ORDER BY at_ms DESC, node_id DESC, seq DESC
                  LIMIT ?6",
            )?;
            let rows = stmt
                .query_map(
                    params![
                        cur_ms,
                        cur_node,
                        cur_seq,
                        action,
                        node.map(|n| n.as_bytes().to_vec()),
                        i64::try_from(limit).unwrap_or(i64::MAX),
                    ],
                    row_to_audit,
                )?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(rows)
        })
    }
}

/// Walk an ascending run of one node's entries.
fn verify_rows(node_id: NodeId, rows: &[AuditRow]) -> ChainStatus {
    let Some(first) = rows.first() else {
        return ChainStatus::Empty;
    };
    let mut expected_prev = first.prev_hash;
    let mut expected_seq = first.seq;
    for row in rows {
        if row.seq != expected_seq {
            return ChainStatus::Broken { at_seq: row.seq };
        }
        if row.prev_hash != expected_prev {
            return ChainStatus::Broken { at_seq: row.seq };
        }
        let Some(action) = action_from_str(&row.action) else {
            return ChainStatus::Broken { at_seq: row.seq };
        };
        let Ok(recomputed) = audit_entry_hash(
            node_id,
            row.seq,
            row.at_ms,
            action,
            row.subject.as_deref(),
            row.detail.as_deref(),
            &row.actor,
            &row.prev_hash,
        ) else {
            return ChainStatus::Broken { at_seq: row.seq };
        };
        if recomputed != row.entry_hash {
            return ChainStatus::Broken { at_seq: row.seq };
        }
        expected_prev = row.entry_hash;
        expected_seq = row.seq + 1;
    }
    ChainStatus::Verified {
        through_seq: expected_seq - 1,
    }
}

/// Stored action text → the typed action, or `None` for a string this
/// build never writes.
///
/// `None` must break verification (Codex P1 on #64): falling back to a
/// valid variant would rehash an edited row under the ORIGINAL action
/// and reproduce its hash — so rewriting a `task.dispatched` row's
/// action to anything unrecognized would display the forged action
/// while the chain reported "verified".
fn action_from_str(s: &str) -> Option<AuditAction> {
    Some(match s {
        "task.dispatched" => AuditAction::TaskDispatched,
        "task.cancelled" => AuditAction::TaskCancelled,
        "plan.resumed" => AuditAction::PlanResumed,
        "shell.allowed" => AuditAction::ShellAllowed,
        "shell.denied" => AuditAction::ShellDenied,
        "secret.accessed" => AuditAction::SecretAccessed,
        "peer.approved" => AuditAction::PeerApproved,
        "policy.loaded" => AuditAction::PolicyLoaded,
        "cloud.escalated" => AuditAction::CloudEscalated,
        "audit.truncated" => AuditAction::AuditTruncated,
        _ => return None,
    })
}

/// One open rate-limit window for a `(action, actor)` pair.
#[derive(Debug, Default)]
struct Window {
    start_ms: u64,
    /// Records appended in full so far this window.
    appended: u64,
    /// Records dropped past [`BURST_ALLOWANCE`].
    suppressed: u64,
    /// Distinct subjects seen among the DROPPED records, capped at
    /// [`DISTINCT_CAP`]; `distinct_overflow` marks that the real
    /// number is higher.
    ///
    /// Keyed on a hash of the UNTRUNCATED subject (re-review MINOR-4
    /// on #65): keying on the truncated form collapses 1000 commands
    /// sharing a 128-byte prefix into `distinct_subjects: 1` with
    /// `distinct_subjects_capped: false` — a flag that says "exact"
    /// when it is not, which is the same species of overstatement
    /// this item just removed from the History banner.
    distinct: std::collections::HashSet<[u8; 32]>,
    distinct_overflow: bool,
    /// Up to [`SAMPLE_SUBJECTS`] of those subjects, kept in the order
    /// first seen.
    sample: Vec<String>,
}

/// What makes two floodable records "the same" for rate limiting.
/// Deliberately subject-free: see [`SUPPRESSION_WINDOW_MS`].
type BurstKey = (AuditAction, AuditActor);

/// Ceiling on simultaneously open windows. With a subject-free key
/// the live count is bounded by (floodable actions × actors) anyway;
/// this is the backstop. Hitting it CLOSES every window (emitting
/// their summaries) rather than discarding counts.
const MAX_OPEN_BURSTS: usize = 1024;

/// Budget for a summary's `sample_subjects`, leaving room for the
/// counts. The counts are the part that must never be lost.
const SAMPLE_BUDGET_BYTES: usize = MAX_AUDIT_DETAIL_BYTES / 2;

/// Sample form of a subject: control characters folded out, then
/// truncated.
///
/// Folding is not cosmetic. `audit_append` drops a detail WHOLE once
/// its stored JSON passes [`MAX_AUDIT_DETAIL_BYTES`], and a control
/// character escapes to `\uXXXX` — six bytes for one. A flooder
/// choosing control-character command names could otherwise blow the
/// cap with eight samples and erase the very count that records its
/// attempts. The encoded budget in `into_summary` is the actual
/// guarantee; this keeps samples readable and the common case
/// nowhere near it.
fn sampled(subject: &str) -> String {
    let folded: String = subject
        .chars()
        .map(|c| if c.is_control() { '·' } else { c })
        .collect();
    if folded.len() <= SAMPLE_SUBJECT_BYTES {
        return folded;
    }
    let mut end = SAMPLE_SUBJECT_BYTES;
    while end > 0 && !folded.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &folded[..end])
}

impl Window {
    /// Record one dropped entry's distinguishing subject.
    fn note_dropped(&mut self, subject: Option<&str>) {
        self.suppressed += 1;
        let Some(subject) = subject else { return };
        let id = *blake3::hash(subject.as_bytes()).as_bytes();
        if self.distinct.len() < DISTINCT_CAP {
            if self.distinct.insert(id) && self.sample.len() < SAMPLE_SUBJECTS {
                self.sample.push(sampled(subject));
            }
        } else if !self.distinct.contains(&id) {
            self.distinct_overflow = true;
        }
    }

    /// The entry a closed window leaves behind, if it swallowed
    /// anything. Same action and actor as the records it stands for,
    /// so the History page shows it under the action it suppressed;
    /// no `subject`, because it stands for several.
    fn into_summary(self, key: BurstKey) -> Option<AuditRecord> {
        if self.suppressed == 0 {
            return None;
        }
        let (action, actor) = key;
        // Samples are admitted one at a time against an ENCODED
        // budget, so what an adversary put in a command name can
        // never push the COUNTS past the detail cap — over the cap,
        // `audit_append` drops the detail whole, which would erase
        // exactly the record of the attempts.
        let mut sample: Vec<String> = Vec::new();
        let mut encoded = 2usize; // the `[]`
        let mut sample_dropped = 0usize;
        for subject in self.sample {
            let cost = serde_json::to_string(&subject).map_or(usize::MAX, |s| s.len() + 1);
            if encoded.saturating_add(cost) > SAMPLE_BUDGET_BYTES {
                sample_dropped += 1;
                continue;
            }
            encoded += cost;
            sample.push(subject);
        }
        Some(
            AuditRecord::new(action, actor).with_detail(&serde_json::json!({
                "suppressed_repeats": self.suppressed,
                "distinct_subjects": self.distinct.len(),
                "distinct_subjects_capped": self.distinct_overflow,
                "sample_subjects": sample,
                "sample_dropped": sample_dropped,
                "appended_before_suppressing": self.appended,
                "window_start_ms": self.start_ms,
                "window_ms": SUPPRESSION_WINDOW_MS,
            })),
        )
    }
}

/// The store-backed [`AuditSink`] handed to capabilities and the mesh.
///
/// Recording never fails the audited action: a store error is logged
/// and dropped. An action that happened but could not be recorded is
/// a gap in the log, not a refused operation.
pub struct StoreAuditSink {
    store: Store,
    node_id: NodeId,
    /// 5.13b: open rate-limit windows for floodable actions. In
    /// memory deliberately — a restart resets them, which errs toward
    /// RECORDING the next attempt rather than silently dropping it.
    bursts: parking_lot::Mutex<std::collections::HashMap<BurstKey, Window>>,
}

impl StoreAuditSink {
    #[must_use]
    pub fn new(store: Store, node_id: NodeId) -> Self {
        Self {
            store,
            node_id,
            bursts: parking_lot::Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Append every summary owed by a window that has since closed.
    ///
    /// Called on the housekeeping tick as well as inline, because a
    /// burst that STOPS must still be recorded (Codex P1 on #65): the
    /// count cannot wait for a later attempt that may never come, or
    /// a completed flood would show its first few rows and no trace
    /// of the rest.
    pub fn flush_suppressed(&self) {
        self.flush_at(now_ms());
    }

    /// Close EVERY open window, elapsed or not, and append what they
    /// owe. Called on daemon shutdown: [`Self::close_expired`] closes
    /// only windows whose interval has passed, so a burst that was
    /// still in progress would otherwise take its count to the grave
    /// (re-review MAJOR-2 on #65 — an operator restarting the daemon
    /// 30s into a flood is the expected reaction to a flood).
    ///
    /// An UNGRACEFUL exit still loses the open window's count. That
    /// residual is irreducible for an in-memory map and is stated in
    /// ADR-0041 rather than implied away.
    pub fn close_all_windows(&self) {
        let now = now_ms();
        let drained: Vec<(BurstKey, Window)> = self.bursts.lock().drain().collect();
        for (key, window) in drained {
            if let Some(summary) = window.into_summary(key) {
                self.append(&summary, now);
            }
        }
    }

    fn flush_at(&self, now_ms: u64) {
        for summary in self.close_expired(now_ms) {
            self.append(&summary, now_ms);
        }
    }

    /// Remove every window whose interval has elapsed, returning the
    /// summaries owed. Also enforces [`MAX_OPEN_BURSTS`] — closing
    /// windows early rather than dropping their counts (diff review
    /// MAJOR-3: the old eviction discarded exactly the pending
    /// counts, which let a flooder erase the record of its own
    /// attempts).
    fn close_expired(&self, now_ms: u64) -> Vec<AuditRecord> {
        let mut bursts = self.bursts.lock();
        let expired: Vec<BurstKey> = bursts
            .iter()
            .filter(|(_, w)| now_ms.saturating_sub(w.start_ms) >= SUPPRESSION_WINDOW_MS)
            .map(|(k, _)| k.clone())
            .collect();
        let mut summaries = Vec::new();
        for key in expired {
            if let Some(window) = bursts.remove(&key) {
                summaries.extend(window.into_summary(key));
            }
        }
        if bursts.len() >= MAX_OPEN_BURSTS {
            let drained: Vec<(BurstKey, Window)> = bursts.drain().collect();
            for (key, window) in drained {
                summaries.extend(window.into_summary(key));
            }
        }
        summaries
    }

    /// Should this record append?
    ///
    /// Only floodable actions are ever dropped, and only past
    /// [`BURST_ALLOWANCE`] within one window; everything else always
    /// appends. A dropped record bumps its window's counters, which
    /// [`Self::close_expired`] later turns into a summary entry.
    fn admit(&self, record: &AuditRecord, now_ms: u64) -> bool {
        if !is_floodable(record.action) {
            return true;
        }
        let key = (record.action, record.actor.clone());
        let mut bursts = self.bursts.lock();
        let window = bursts.entry(key).or_insert_with(|| Window {
            start_ms: now_ms,
            ..Window::default()
        });
        if window.appended < BURST_ALLOWANCE {
            window.appended += 1;
            return true;
        }
        window.note_dropped(record.subject.as_deref());
        false
    }

    fn append(&self, record: &AuditRecord, now_ms: u64) {
        if let Err(e) = self.store.audit_append(self.node_id, record, now_ms) {
            tracing::warn!(
                target: "harness.store.audit",
                ?e,
                action = record.action.as_str(),
                "audit append failed; the action proceeded unrecorded"
            );
        }
    }
}

impl std::fmt::Debug for StoreAuditSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreAuditSink")
            .field("node_id", &self.node_id)
            .finish_non_exhaustive()
    }
}

impl AuditSink for StoreAuditSink {
    fn record(&self, record: AuditRecord) {
        let now = now_ms();
        // 5.13b: fold a burst of identical floodable entries into one
        // row plus a summary, so an adversary cannot push genuine
        // entries out of the retention window one denial at a time.
        // Closing happens first, so a summary lands BEFORE the entry
        // that reopened the window.
        self.flush_at(now);
        if !self.admit(&record, now) {
            return;
        }
        self.append(&record, now);
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    fn node() -> NodeId {
        NodeId::from_bytes([3u8; 16])
    }

    fn rec(n: u32) -> AuditRecord {
        AuditRecord::new(AuditAction::TaskDispatched, AuditActor::System)
            .with_subject(format!("task-{n}"))
    }

    #[test]
    fn a01_appends_form_a_gapless_verified_chain() {
        let s = Store::open_memory().expect("store");
        assert_eq!(
            s.audit_verify_chain(node(), 1, 1_000).expect("verify"),
            ChainStatus::Empty
        );
        for n in 1..=5u32 {
            assert_eq!(
                s.audit_append(node(), &rec(n), u64::from(n))
                    .expect("append"),
                u64::from(n)
            );
        }
        assert_eq!(
            s.audit_verify_chain(node(), 1, 1_000).expect("verify"),
            ChainStatus::Verified { through_seq: 5 }
        );
    }

    #[test]
    fn a02_an_edited_row_breaks_the_chain_at_that_seq() {
        // The point of the whole feature: an out-of-daemon edit is
        // detectable, and detectable AT the row that changed.
        let s = Store::open_memory().expect("store");
        for n in 1..=4u32 {
            s.audit_append(node(), &rec(n), u64::from(n))
                .expect("append");
        }
        s.with_conn(|c| {
            c.execute(
                "UPDATE audit_log SET subject = 'tampered' WHERE node_id = ?1 AND seq = 3",
                params![node().as_bytes()],
            )?;
            Ok(())
        })
        .expect("tamper");
        assert_eq!(
            s.audit_verify_chain(node(), 1, 1_000).expect("verify"),
            ChainStatus::Broken { at_seq: 3 }
        );
    }

    #[test]
    fn a03_a_deleted_row_breaks_the_chain() {
        let s = Store::open_memory().expect("store");
        for n in 1..=4u32 {
            s.audit_append(node(), &rec(n), u64::from(n))
                .expect("append");
        }
        s.with_conn(|c| {
            c.execute(
                "DELETE FROM audit_log WHERE node_id = ?1 AND seq = 2",
                params![node().as_bytes()],
            )?;
            Ok(())
        })
        .expect("delete");
        assert_eq!(
            s.audit_verify_chain(node(), 1, 1_000).expect("verify"),
            ChainStatus::Broken { at_seq: 3 },
            "seq 3 no longer follows seq 1"
        );
    }

    #[test]
    fn a03b_a_deleted_prefix_does_not_pass_as_verified() {
        // Codex P1 on #64: the oldest surviving row would otherwise
        // supply both its own prev_hash and the expected seq, so the
        // walk would start wherever the deletion left off and report
        // clean — precisely the case the truncation marker exists to
        // tell apart from legitimate pruning.
        let s = Store::open_memory().expect("store");
        for n in 1..=4u32 {
            s.audit_append(node(), &rec(n), u64::from(n))
                .expect("append");
        }
        s.with_conn(|c| {
            c.execute(
                "DELETE FROM audit_log WHERE node_id = ?1 AND seq = 1",
                params![node().as_bytes()],
            )?;
            Ok(())
        })
        .expect("delete prefix");
        assert_eq!(
            s.audit_verify_chain(node(), 1, 1_000).expect("verify"),
            ChainStatus::Broken { at_seq: 2 },
            "nothing accounts for the missing genesis entry"
        );

        // A rewritten genesis is caught the same way: seq 1 must link
        // to the zero hash.
        let s2 = Store::open_memory().expect("store");
        s2.audit_append(node(), &rec(1), 1).expect("append");
        s2.with_conn(|c| {
            c.execute(
                "UPDATE audit_log SET prev_hash = ?2 WHERE node_id = ?1 AND seq = 1",
                params![node().as_bytes(), &[7u8; 32][..]],
            )?;
            Ok(())
        })
        .expect("tamper");
        assert_eq!(
            s2.audit_verify_chain(node(), 1, 1_000).expect("verify"),
            ChainStatus::Broken { at_seq: 1 }
        );
    }

    #[test]
    fn a03c_an_unknown_action_breaks_verification() {
        // Codex P1 on #64: mapping an unrecognized action back to a
        // valid variant rehashes the row under its ORIGINAL action and
        // reproduces the hash — so a forged action would display while
        // the chain reported verified.
        let s = Store::open_memory().expect("store");
        s.audit_append(node(), &rec(1), 1).expect("append");
        s.with_conn(|c| {
            c.execute(
                "UPDATE audit_log SET action = 'totally.made.up' WHERE node_id = ?1 AND seq = 1",
                params![node().as_bytes()],
            )?;
            Ok(())
        })
        .expect("tamper");
        assert_eq!(
            s.audit_verify_chain(node(), 1, 1_000).expect("verify"),
            ChainStatus::Broken { at_seq: 1 }
        );
    }

    #[test]
    fn a04_pruning_keeps_the_chain_verifiable() {
        // Plan review BLOCKER-2: retention must not manufacture a
        // permanent BROKEN banner. The marker is appended BEFORE the
        // delete and carries the anchor, so what survives verifies.
        let s = Store::open_memory().expect("store");
        for n in 1..=10u32 {
            s.audit_append(node(), &rec(n), u64::from(n))
                .expect("append");
        }
        // Keep 4 → marker becomes seq 11, rows 1..=6 go.
        let removed = s.audit_prune(node(), 4, 100).expect("prune");
        assert_eq!(removed, 6);

        let rows = s
            .audit_recent(None, None, Some(node()), 100)
            .expect("recent");
        assert_eq!(rows.len(), 5, "4 kept + the marker");
        let marker = rows
            .iter()
            .find(|r| r.action == "audit.truncated")
            .expect("marker present");
        let detail: serde_json::Value =
            serde_json::from_str(marker.detail.as_deref().expect("detail")).expect("json");
        assert_eq!(detail["through_seq"], 6);

        // What remains still verifies from the surviving head.
        assert_eq!(
            s.audit_verify_chain(node(), 7, 1_000).expect("verify"),
            ChainStatus::Verified { through_seq: 11 }
        );
    }

    #[test]
    fn a05_chains_are_per_node_and_independent() {
        let s = Store::open_memory().expect("store");
        let other = NodeId::from_bytes([9u8; 16]);
        s.audit_append(node(), &rec(1), 1).expect("append");
        assert_eq!(
            s.audit_append(other, &rec(1), 1).expect("append"),
            1,
            "the other node's chain starts at 1 too"
        );
        assert_eq!(s.audit_append(node(), &rec(2), 2).expect("append"), 2);
        assert_eq!(
            s.audit_verify_chain(other, 1, 1_000).expect("verify"),
            ChainStatus::Verified { through_seq: 1 }
        );
        assert_eq!(
            s.audit_recent(None, None, Some(other), 10)
                .expect("recent")
                .len(),
            1
        );
    }

    #[test]
    fn a06_oversized_detail_is_dropped_not_stored() {
        let s = Store::open_memory().expect("store");
        let big = AuditRecord::new(AuditAction::ShellDenied, AuditActor::System)
            .with_detail(&serde_json::json!({ "x": "y".repeat(MAX_AUDIT_DETAIL_BYTES) }));
        s.audit_append(node(), &big, 1).expect("append");
        let rows = s
            .audit_recent(None, None, Some(node()), 10)
            .expect("recent");
        assert_eq!(rows.len(), 1, "the entry is still recorded");
        assert!(rows[0].detail.is_none(), "the payload is not");
        assert_eq!(
            s.audit_verify_chain(node(), 1, 1_000).expect("verify"),
            ChainStatus::Verified { through_seq: 1 }
        );
    }

    #[test]
    fn a07_signed_head_tracks_the_chain() {
        let s = Store::open_memory().expect("store");
        let id = Identity::generate();
        assert!(s.signed_audit_head(&id, 1).expect("head").is_none());
        s.audit_append(id.node_id(), &rec(1), 1).expect("append");
        let head = s.signed_audit_head(&id, 5).expect("head").expect("some");
        assert_eq!(head.seq, 1);
        assert!(head.verify_signature(id.public_key()).is_ok());

        s.audit_append(id.node_id(), &rec(2), 6).expect("append");
        let head2 = s.signed_audit_head(&id, 7).expect("head").expect("some");
        assert_eq!(head2.seq, 2);
        assert_ne!(head2.entry_hash, head.entry_hash);
    }

    #[test]
    fn a08_recent_filters_and_pages_by_time() {
        let s = Store::open_memory().expect("store");
        for n in 1..=3u32 {
            s.audit_append(node(), &rec(n), u64::from(n) * 10)
                .expect("append");
        }
        s.audit_append(
            node(),
            &AuditRecord::new(AuditAction::ShellDenied, AuditActor::System),
            40,
        )
        .expect("append");

        let all = s.audit_recent(None, None, None, 100).expect("recent");
        assert_eq!(all.len(), 4);
        assert!(all[0].at_ms >= all[1].at_ms, "newest first");

        let denied = s
            .audit_recent(None, Some("shell.denied"), None, 100)
            .expect("recent");
        assert_eq!(denied.len(), 1);

        // Keyset page: everything strictly older than 30.
        let older = s
            .audit_recent(
                Some(AuditCursor {
                    at_ms: 30,
                    node: node(),
                    seq: 0,
                }),
                None,
                None,
                100,
            )
            .expect("recent");
        assert_eq!(older.len(), 2);
        assert!(older.iter().all(|r| r.at_ms < 30));
    }

    /// `BURST_ALLOWANCE` as a length, without a lossy cast.
    fn allowance() -> usize {
        usize::try_from(BURST_ALLOWANCE).expect("allowance fits usize")
    }

    fn denial(subject: &str) -> AuditRecord {
        AuditRecord::new(AuditAction::ShellDenied, AuditActor::System)
            .with_subject(subject)
            .with_detail(&serde_json::json!({ "argv_hash": subject, "reason": "policy" }))
    }

    fn detail_of(row: &AuditRow) -> serde_json::Value {
        serde_json::from_str(row.detail.as_deref().expect("detail")).expect("json")
    }

    #[test]
    fn a09_a_flood_is_bounded_even_when_the_attacker_varies_the_command() {
        // Diff review BLOCKER-1 on #65: `subject` for a denial IS the
        // submitted command, which the adversary chooses. Keying the
        // rate limit on it let `/bin/x1`, `/bin/x2`, … mint a fresh
        // key per attempt — the flood passed untouched while the only
        // records dropped were an operator's repeated denials of one
        // command. The key is `(action, actor)`; the varying command
        // buys nothing.
        let s = Store::open_memory().expect("store");
        let sink = StoreAuditSink::new(s.clone(), node());
        for i in 0..1_000 {
            sink.record(denial(&format!("/bin/x{i}")));
        }
        let rows = s
            .audit_recent(None, Some("shell.denied"), None, 5_000)
            .expect("recent");
        assert_eq!(
            rows.len(),
            allowance(),
            "1000 distinct commands, {BURST_ALLOWANCE} rows"
        );

        // And the rest are accounted for once the window closes.
        sink.flush_at(now_ms() + SUPPRESSION_WINDOW_MS + 1);
        let rows = s
            .audit_recent(None, Some("shell.denied"), None, 5_000)
            .expect("recent");
        assert_eq!(rows.len(), allowance() + 1);
        let summary = detail_of(&rows[0]);
        assert_eq!(
            summary["suppressed_repeats"],
            serde_json::json!(1_000 - BURST_ALLOWANCE)
        );
        assert_eq!(
            summary["distinct_subjects"],
            serde_json::json!(DISTINCT_CAP)
        );
        assert_eq!(summary["distinct_subjects_capped"], serde_json::json!(true));
        assert_eq!(
            summary["sample_subjects"].as_array().expect("sample").len(),
            SAMPLE_SUBJECTS,
            "a bounded sample of what was dropped survives"
        );
        assert!(summary["window_start_ms"].is_u64(), "the summary is dated");
        assert_eq!(
            s.audit_verify_chain(node(), 1, 1_000).expect("verify"),
            ChainStatus::Verified {
                through_seq: BURST_ALLOWANCE + 1
            }
        );
    }

    #[test]
    fn a09b_distinct_denials_below_the_allowance_are_recorded_in_full() {
        // Diff review MAJOR-2: coalescing must not silently discard
        // different attempts. Under the allowance every denial keeps
        // its own argv, reason and subject.
        let s = Store::open_memory().expect("store");
        let sink = StoreAuditSink::new(s.clone(), node());
        for i in 0..5 {
            sink.record(denial(&format!("curl-{i}")));
        }
        let rows = s
            .audit_recent(None, Some("shell.denied"), None, 100)
            .expect("recent");
        assert_eq!(rows.len(), 5);
        for (n, row) in rows.iter().enumerate() {
            let expected = format!("curl-{}", 4 - n);
            assert_eq!(row.subject.as_deref(), Some(expected.as_str()));
            assert_eq!(detail_of(row)["argv_hash"], serde_json::json!(expected));
        }
    }

    #[test]
    fn a09c_a_burst_that_stops_is_still_recorded() {
        // Codex P1 on #65: the count cannot wait for a later matching
        // attempt that may never come — a completed flood would show
        // its first rows and no trace of the rest.
        let s = Store::open_memory().expect("store");
        let sink = StoreAuditSink::new(s.clone(), node());
        for _ in 0..BURST_ALLOWANCE + 3 {
            sink.record(denial("rm -rf /"));
        }
        assert_eq!(
            s.audit_recent(None, None, None, 100).expect("recent").len(),
            allowance(),
            "three swallowed, nothing else appended yet"
        );

        // Nothing more arrives; the window simply expires and the
        // housekeeping flush closes it.
        sink.flush_at(now_ms() + SUPPRESSION_WINDOW_MS + 1);
        let rows = s.audit_recent(None, None, None, 100).expect("recent");
        assert_eq!(rows.len(), allowance() + 1);
        let summary = detail_of(&rows[0]);
        assert_eq!(summary["suppressed_repeats"], serde_json::json!(3));
        assert_eq!(summary["distinct_subjects"], serde_json::json!(1));
        assert_eq!(
            summary["appended_before_suppressing"],
            serde_json::json!(BURST_ALLOWANCE)
        );
        assert_eq!(rows[0].action, "shell.denied");
        assert_eq!(rows[0].actor, "system");
        assert!(
            rows[0].subject.is_none(),
            "a summary stands for several subjects, so it claims none"
        );

        // Consumed, not re-reported.
        sink.flush_at(now_ms() + SUPPRESSION_WINDOW_MS * 3);
        assert_eq!(
            s.audit_recent(None, None, None, 100).expect("recent").len(),
            allowance() + 1
        );
    }

    #[test]
    fn a09d_one_actors_flood_does_not_mask_another() {
        // The limit is per actor: a flooding peer must not spend the
        // allowance that would have recorded someone else's denial.
        let s = Store::open_memory().expect("store");
        let sink = StoreAuditSink::new(s.clone(), node());
        for i in 0..100 {
            sink.record(denial(&format!("flood-{i}")));
        }
        let peer = AuditRecord::new(AuditAction::ShellDenied, AuditActor::Peer { node: node() })
            .with_subject("legitimate");
        sink.record(peer);
        let rows = s
            .audit_recent(None, Some("shell.denied"), None, 500)
            .expect("recent");
        assert_eq!(rows[0].subject.as_deref(), Some("legitimate"));
        assert!(rows[0].actor.starts_with("peer:"));
    }

    #[test]
    fn a09e_non_floodable_actions_are_never_dropped() {
        // Suppression exists for the flood surface, not for the log.
        let s = Store::open_memory().expect("store");
        let sink = StoreAuditSink::new(s.clone(), node());
        for _ in 0..BURST_ALLOWANCE * 3 {
            sink.record(rec(1));
        }
        let rows = s
            .audit_recent(None, Some("task.dispatched"), None, 500)
            .expect("recent");
        assert_eq!(rows.len(), allowance() * 3);
    }

    #[test]
    fn a09f_the_open_window_ceiling_closes_windows_instead_of_losing_counts() {
        // Diff review MAJOR-3: the old eviction discarded exactly the
        // pending counts, which let a flooder erase the record of its
        // own attempts. Closing a window emits its summary first.
        let s = Store::open_memory().expect("store");
        let sink = StoreAuditSink::new(s.clone(), node());
        let per_actor = BURST_ALLOWANCE + 2;
        let actors = u64::try_from(MAX_OPEN_BURSTS + 4).expect("fits");
        for i in 0..actors {
            let actor = AuditActor::Webhook {
                channel: format!("chan-{i}"),
            };
            for n in 0..per_actor {
                sink.record(
                    AuditRecord::new(AuditAction::ShellDenied, actor.clone())
                        .with_subject(format!("cmd-{n}")),
                );
            }
        }
        assert!(
            sink.bursts.lock().len() < MAX_OPEN_BURSTS,
            "the ceiling bounds open windows"
        );

        let rows = s
            .audit_recent(None, Some("shell.denied"), None, 100_000)
            .expect("recent");
        let mut appended = 0u64;
        let mut summarized = 0u64;
        for row in &rows {
            match row
                .detail
                .as_deref()
                .and_then(|d| serde_json::from_str::<serde_json::Value>(d).ok())
                .and_then(|d| d["suppressed_repeats"].as_u64())
            {
                Some(n) => summarized += n,
                None => appended += 1,
            }
        }
        let still_open: u64 = sink.bursts.lock().values().map(|w| w.suppressed).sum();
        // Conservation: every record fed to the sink is either a row
        // in the log or a counted repeat. The ceiling may turn a
        // would-be repeat into a full row (its window was drained
        // first) — what it must never do is make one vanish.
        assert_eq!(
            appended + summarized + still_open,
            actors * per_actor,
            "every denial is either appended or counted"
        );
        assert!(matches!(
            s.audit_verify_chain(node(), 1, 1_000_000).expect("verify"),
            ChainStatus::Verified { .. }
        ));
    }

    #[test]
    fn a09g_a_sampled_subject_is_truncated() {
        // Diff review MINOR-10: a submitted command is bounded only
        // by the frame cap, so nothing derived from it may be stored
        // unbounded.
        let s = Store::open_memory().expect("store");
        let sink = StoreAuditSink::new(s.clone(), node());
        let long = "A".repeat(50_000);
        for _ in 0..=BURST_ALLOWANCE {
            sink.record(denial(&long));
        }
        sink.flush_at(now_ms() + SUPPRESSION_WINDOW_MS + 1);
        let rows = s
            .audit_recent(None, Some("shell.denied"), None, 100)
            .expect("recent");
        let sample = detail_of(&rows[0])["sample_subjects"][0]
            .as_str()
            .expect("sample")
            .to_string();
        assert!(
            sample.len() <= SAMPLE_SUBJECT_BYTES + 4,
            "sampled subject truncated, got {} bytes",
            sample.len()
        );
    }

    #[test]
    fn a09h_a_hostile_command_name_cannot_erase_the_count() {
        // The subjects sampled into a summary come from commands the
        // ADVERSARY chose. `audit_append` drops a detail WHOLE once
        // its stored JSON passes MAX_AUDIT_DETAIL_BYTES, and a
        // control character escapes to six bytes — so eight samples
        // of control characters could blow the cap and take the
        // suppressed count with them, erasing the record of exactly
        // the attempts the summary exists to record.
        let s = Store::open_memory().expect("store");
        let sink = StoreAuditSink::new(s.clone(), node());
        for _ in 0..BURST_ALLOWANCE {
            sink.record(denial("warmup"));
        }
        for i in 0..SAMPLE_SUBJECTS {
            // Distinct (so each is sampled) and maximally expensive
            // to encode.
            let cmd = format!("{}{}", "\u{1}".repeat(300), i);
            sink.record(
                AuditRecord::new(AuditAction::ShellDenied, AuditActor::System).with_subject(cmd),
            );
        }
        sink.flush_at(now_ms() + SUPPRESSION_WINDOW_MS + 1);

        let rows = s
            .audit_recent(None, Some("shell.denied"), None, 100)
            .expect("recent");
        assert_eq!(rows.len(), allowance() + 1);
        let stored = rows[0]
            .detail
            .as_deref()
            .expect("the summary kept its detail");
        assert!(
            stored.len() <= MAX_AUDIT_DETAIL_BYTES,
            "summary detail must fit the cap, got {} bytes",
            stored.len()
        );
        let summary: serde_json::Value = serde_json::from_str(stored).expect("json");
        assert_eq!(
            summary["suppressed_repeats"],
            serde_json::json!(SAMPLE_SUBJECTS),
            "the count survives whatever the attacker named the command"
        );
        assert!(
            !stored.contains("\\u0001"),
            "control characters are folded out before sampling"
        );
        assert_eq!(
            s.audit_verify_chain(node(), 1, 1_000).expect("verify"),
            ChainStatus::Verified {
                through_seq: BURST_ALLOWANCE + 1
            }
        );
    }

    #[test]
    fn a09i_shutdown_closes_an_in_progress_window() {
        // Re-review MAJOR-2 on #65: restarting the daemon mid-flood
        // is the natural reaction to a flood, and an unexpired window
        // is exactly the one holding the count.
        let s = Store::open_memory().expect("store");
        let sink = StoreAuditSink::new(s.clone(), node());
        for _ in 0..BURST_ALLOWANCE + 7 {
            sink.record(denial("rm -rf /"));
        }
        // The window is seconds old: the expiry-based flush is a no-op.
        sink.flush_suppressed();
        assert_eq!(
            s.audit_recent(None, None, None, 100).expect("recent").len(),
            allowance(),
            "an unexpired window is not closed by the periodic flush"
        );

        sink.close_all_windows();
        let rows = s.audit_recent(None, None, None, 100).expect("recent");
        assert_eq!(rows.len(), allowance() + 1);
        assert_eq!(
            detail_of(&rows[0])["suppressed_repeats"],
            serde_json::json!(7)
        );
        assert!(sink.bursts.lock().is_empty());
    }

    #[test]
    fn a09j_distinct_subjects_is_not_collapsed_by_truncation() {
        // Re-review MINOR-4 on #65: keying the distinct set on the
        // TRUNCATED subject collapsed commands sharing a long prefix
        // into `distinct_subjects: 1` with `capped: false` — a flag
        // claiming exactness it does not have.
        let s = Store::open_memory().expect("store");
        let sink = StoreAuditSink::new(s.clone(), node());
        for _ in 0..BURST_ALLOWANCE {
            sink.record(denial("warmup"));
        }
        let prefix = "x".repeat(SAMPLE_SUBJECT_BYTES * 2);
        for i in 0..5 {
            sink.record(denial(&format!("{prefix}{i}")));
        }
        sink.flush_at(now_ms() + SUPPRESSION_WINDOW_MS + 1);
        let rows = s
            .audit_recent(None, Some("shell.denied"), None, 100)
            .expect("recent");
        let summary = detail_of(&rows[0]);
        assert_eq!(
            summary["distinct_subjects"],
            serde_json::json!(5),
            "five commands differing past the truncation point are five"
        );
        assert_eq!(
            summary["distinct_subjects_capped"],
            serde_json::json!(false)
        );
    }

    #[test]
    fn a09k_a_summary_survives_with_no_samples_at_all() {
        // The counts must be unlosable: there must be no input for
        // which the numeric-only detail exceeds the cap.
        let s = Store::open_memory().expect("store");
        let sink = StoreAuditSink::new(s.clone(), node());
        for _ in 0..=BURST_ALLOWANCE {
            // No subject at all — nothing to sample.
            sink.record(AuditRecord::new(
                AuditAction::ShellDenied,
                AuditActor::System,
            ));
        }
        sink.close_all_windows();
        let rows = s
            .audit_recent(None, Some("shell.denied"), None, 100)
            .expect("recent");
        let summary = detail_of(&rows[0]);
        assert_eq!(summary["suppressed_repeats"], serde_json::json!(1));
        assert_eq!(summary["sample_subjects"], serde_json::json!([]));
        assert!(
            rows[0].detail.as_deref().expect("detail").len() < MAX_AUDIT_DETAIL_BYTES / 4,
            "the numeric-only detail is nowhere near the cap"
        );
    }
}
