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
    audit_entry_hash, AuditAction, AuditHead, AuditRecord, AuditSink, Identity, NodeId, Signable,
    Signature,
};
use rusqlite::{params, OptionalExtension};

use crate::error::StoreError;
use crate::open::Store;

/// Cap on a stored `detail` (mirrored by the column CHECK). Details
/// are hashes and identifiers, never payloads.
pub const MAX_AUDIT_DETAIL_BYTES: usize = 4096;

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
    ) -> Result<ChainStatus, StoreError> {
        let rows = self.with_conn(|c| {
            let mut stmt = c.prepare(
                "SELECT node_id, seq, at_ms, action, subject, detail, actor,
                        prev_hash, entry_hash
                   FROM audit_log
                  WHERE node_id = ?1 AND seq >= ?2
               ORDER BY seq ASC",
            )?;
            let rows = stmt
                .query_map(
                    params![
                        node_id.as_bytes(),
                        i64::try_from(from_seq).unwrap_or(i64::MAX)
                    ],
                    row_to_audit,
                )?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(rows)
        })?;
        Ok(verify_rows(node_id, &rows))
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
        before_at_ms: Option<u64>,
        action: Option<&str>,
        node: Option<NodeId>,
        limit: usize,
    ) -> Result<Vec<AuditRow>, StoreError> {
        self.with_conn(|c| {
            let mut stmt = c.prepare(
                "SELECT node_id, seq, at_ms, action, subject, detail, actor,
                        prev_hash, entry_hash
                   FROM audit_log
                  WHERE (?1 IS NULL OR at_ms < ?1)
                    AND (?2 IS NULL OR action = ?2)
                    AND (?3 IS NULL OR node_id = ?3)
               ORDER BY at_ms DESC, node_id DESC, seq DESC
                  LIMIT ?4",
            )?;
            let rows = stmt
                .query_map(
                    params![
                        before_at_ms.map(|v| i64::try_from(v).unwrap_or(i64::MAX)),
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
        let Ok(recomputed) = audit_entry_hash(
            node_id,
            row.seq,
            row.at_ms,
            action_from_str(&row.action),
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

/// Stored action text → the typed action. An unknown string cannot be
/// rehashed identically, so it verifies as broken — which is the
/// honest outcome for a row nobody in this build wrote.
fn action_from_str(s: &str) -> AuditAction {
    match s {
        "task.cancelled" => AuditAction::TaskCancelled,
        "plan.resumed" => AuditAction::PlanResumed,
        "shell.allowed" => AuditAction::ShellAllowed,
        "shell.denied" => AuditAction::ShellDenied,
        "secret.accessed" => AuditAction::SecretAccessed,
        "peer.approved" => AuditAction::PeerApproved,
        "policy.loaded" => AuditAction::PolicyLoaded,
        "cloud.escalated" => AuditAction::CloudEscalated,
        "audit.truncated" => AuditAction::AuditTruncated,
        _ => AuditAction::TaskDispatched,
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
}

impl StoreAuditSink {
    #[must_use]
    pub fn new(store: Store, node_id: NodeId) -> Self {
        Self { store, node_id }
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
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
            .unwrap_or(0);
        if let Err(e) = self.store.audit_append(self.node_id, &record, now) {
            tracing::warn!(
                target: "harness.store.audit",
                ?e,
                action = record.action.as_str(),
                "audit append failed; the action proceeded unrecorded"
            );
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use harness_core::AuditActor;

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
            s.audit_verify_chain(node(), 1).expect("verify"),
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
            s.audit_verify_chain(node(), 1).expect("verify"),
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
            s.audit_verify_chain(node(), 1).expect("verify"),
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
            s.audit_verify_chain(node(), 1).expect("verify"),
            ChainStatus::Broken { at_seq: 3 },
            "seq 3 no longer follows seq 1"
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
            s.audit_verify_chain(node(), 7).expect("verify"),
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
            s.audit_verify_chain(other, 1).expect("verify"),
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
            s.audit_verify_chain(node(), 1).expect("verify"),
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
        let older = s.audit_recent(Some(30), None, None, 100).expect("recent");
        assert_eq!(older.len(), 2);
        assert!(older.iter().all(|r| r.at_ms < 30));
    }
}
