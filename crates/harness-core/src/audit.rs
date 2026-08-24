//! 5.13a (ADR-0041): the audit log's wire-and-boundary types.
//!
//! PRD §10.6 — every privileged action lands in an append-only,
//! hash-chained log. The chain itself lives in `harness-store`; this
//! module holds what BOTH sides need: the action vocabulary, the
//! closed actor enum, the entry-hash preimage, and the [`AuditSink`]
//! trait that lets `harness-capabilities` and `harness-mesh` record
//! without depending on the store (the [`crate::ReplicaApplier`]
//! precedent — those crates are core-only by design).

use serde::{Deserialize, Serialize};

use crate::error::ProtocolError;
use crate::identity::{NodeId, Signature};
use crate::protocol::Signable;

/// The privileged actions PRD §10.6 enumerates, plus the log's own
/// housekeeping entry. Serialized `snake_case` with a dot path, so a
/// reader can filter by prefix (`task.`, `secret.`, …).
/// NOTE: the serde form and [`AuditAction::as_str`] MUST agree — the
/// `as_str` form is what gets stored AND hashed, so if 5.13c gossips
/// entries through serde, a divergent rename would make replicated
/// rows re-verify against a different preimage than the origin's
/// (diff review MINOR-6). A test asserts they match.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum AuditAction {
    /// A task was routed to a node.
    #[serde(rename = "task.dispatched")]
    TaskDispatched,
    /// An operator stopped a task (5.10).
    #[serde(rename = "task.cancelled")]
    TaskCancelled,
    /// A stopped plan was resumed (5.12).
    #[serde(rename = "plan.resumed")]
    PlanResumed,
    /// `shell.exec` passed its policy gate on the executing node.
    #[serde(rename = "shell.allowed")]
    ShellAllowed,
    /// `shell.exec` was refused by policy. Attacker-triggerable at
    /// submit rate: retention prunes the chain, but per-attempt
    /// coalescing is NOT implemented (5.13b follow-up) — this is not
    /// a rate limit, and the ADR says so.
    #[serde(rename = "shell.denied")]
    ShellDenied,
    /// A secret was read by TAG on the executing node.
    #[serde(rename = "secret.accessed")]
    SecretAccessed,
    /// A peer was approved into the trust store.
    #[serde(rename = "peer.approved")]
    PeerApproved,
    /// `policy.toml` was loaded or reloaded.
    #[serde(rename = "policy.loaded")]
    PolicyLoaded,
    /// Planning escalated to a cloud backend (§15, 5.2/5.3).
    #[serde(rename = "cloud.escalated")]
    CloudEscalated,
    /// Retention pruned entries `<= through_seq`; carries the anchor
    /// hash so the chain still verifies across the gap.
    #[serde(rename = "audit.truncated")]
    AuditTruncated,
}

impl AuditAction {
    /// Stable string form — the stored column and the API's filter.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            AuditAction::TaskDispatched => "task.dispatched",
            AuditAction::TaskCancelled => "task.cancelled",
            AuditAction::PlanResumed => "plan.resumed",
            AuditAction::ShellAllowed => "shell.allowed",
            AuditAction::ShellDenied => "shell.denied",
            AuditAction::SecretAccessed => "secret.accessed",
            AuditAction::PeerApproved => "peer.approved",
            AuditAction::PolicyLoaded => "policy.loaded",
            AuditAction::CloudEscalated => "cloud.escalated",
            AuditAction::AuditTruncated => "audit.truncated",
        }
    }
}

/// WHO caused the action — a CLOSED set (plan review MAJOR-2).
///
/// Deliberately not free text. There is no user identity in this
/// system: sessions are anonymous bearer tokens behind one admin
/// password, so a "session" actor would put token material in a
/// persisted, soon-to-be-replicated table. And a webhook actor
/// carrying the sender's address would replicate the user's phone
/// number across the LAN — the same defect 5.11 refused when it kept
/// `reply_to` out of task tags. Only the CHANNEL is recorded.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
#[non_exhaustive]
pub enum AuditActor {
    /// An authenticated local session (the single admin identity).
    LocalAdmin,
    /// An inbound webhook, named by channel only — never by sender.
    Webhook { channel: String },
    /// Another mesh node, by id.
    Peer { node: NodeId },
    /// The daemon itself (scheduled sweeps, boot, policy load).
    System,
}

impl AuditActor {
    /// Stable string form for the stored column.
    #[must_use]
    pub fn as_string(&self) -> String {
        match self {
            AuditActor::LocalAdmin => "local_admin".to_string(),
            // The channel is a compile-time-known adapter name
            // (whatsapp / sms / shortcuts), never caller input.
            AuditActor::Webhook { channel } => format!("webhook:{channel}"),
            AuditActor::Peer { node } => format!("peer:{node}"),
            AuditActor::System => "system".to_string(),
        }
    }
}

/// One record to append. `subject` names what was acted on (a task id,
/// a capability, a secret TAG, a peer id); `detail` is bounded JSON
/// TEXT that must never carry a payload — hash inputs and argv rather
/// than storing them (plan review MAJOR-2).
#[derive(Debug, Clone)]
pub struct AuditRecord {
    pub action: AuditAction,
    pub subject: Option<String>,
    pub detail: Option<String>,
    pub actor: AuditActor,
}

impl AuditRecord {
    /// A record with no subject or detail.
    #[must_use]
    pub fn new(action: AuditAction, actor: AuditActor) -> Self {
        Self {
            action,
            subject: None,
            detail: None,
            actor,
        }
    }

    #[must_use]
    pub fn with_subject(mut self, subject: impl Into<String>) -> Self {
        self.subject = Some(subject.into());
        self
    }

    /// Attach bounded JSON detail. The caller is responsible for
    /// keeping payloads out; `harness-store` caps the length.
    #[must_use]
    pub fn with_detail(mut self, detail: &serde_json::Value) -> Self {
        self.detail = serde_json::to_string(detail).ok();
        self
    }
}

/// The chain link for one stored entry.
///
/// Hashed as a JSON OBJECT, never a concatenation (plan review
/// BLOCKER-3): `subject`, `detail` and `actor` are free-form enough
/// that `a ‖ b` is a live collision surface, and this repo already
/// rejected concatenation in `step_hash` for the same reason.
/// `node_id` and `seq` are IN the preimage, so an entry cannot be
/// lifted to another position or another node's chain once
/// replication lands. `detail` is hashed exactly as stored — never
/// re-serialized, which would invite float/escape drift across
/// versions. Key order is stable because `serde_json`'s map is a
/// `BTreeMap` in this workspace (`preserve_order` off, pinned by
/// test).
///
/// # Errors
/// [`ProtocolError::JsonEncode`] if the preimage cannot be encoded —
/// practically unreachable.
#[allow(clippy::too_many_arguments)]
pub fn audit_entry_hash(
    node_id: NodeId,
    seq: u64,
    at_ms: u64,
    action: AuditAction,
    subject: Option<&str>,
    detail: Option<&str>,
    actor: &str,
    prev_hash: &[u8; 32],
) -> Result<[u8; 32], ProtocolError> {
    let preimage = serde_json::json!({
        "node_id": hex::encode(node_id.as_bytes()),
        "seq": seq,
        "at_ms": at_ms,
        "action": action.as_str(),
        "subject": subject,
        "detail": detail,
        "actor": actor,
        "prev_hash": hex::encode(prev_hash),
    });
    let bytes = serde_json::to_vec(&preimage)
        .map_err(|e| ProtocolError::JsonEncode(format!("audit entry: {e}")))?;
    Ok(*blake3::hash(&bytes).as_bytes())
}

/// Lowercase hex of a 32-byte hash — the form the API and details use.
#[must_use]
pub fn hash_hex(hash: &[u8; 32]) -> String {
    use std::fmt::Write as _;
    hash.iter().fold(String::with_capacity(64), |mut acc, b| {
        let _ = write!(acc, "{b:02x}");
        acc
    })
}

/// A node's signed chain head (5.13a).
///
/// This is what makes the chain *evidence* rather than a local
/// integrity check: a node holds its own database and key, so it can
/// rewrite its own chain wholesale — what it cannot do is un-tell a
/// peer that already pinned `(seq, entry_hash)` at an earlier time.
/// 5.13c gossips these; shipping the type now keeps that a pure
/// transport change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditHead {
    pub node_id: NodeId,
    pub seq: u64,
    pub entry_hash: [u8; 32],
    pub at_ms: u64,
    pub sig: Signature,
}

impl Signable for AuditHead {
    fn sig_field_mut(&mut self) -> &mut Signature {
        &mut self.sig
    }
    fn sig_field(&self) -> &Signature {
        &self.sig
    }
}

/// Recording side of the audit log, implemented by `harness-store`.
///
/// Exists so `harness-capabilities` and `harness-mesh` — which are
/// core-only by design — can record privileged actions without a
/// store dependency (the [`crate::ReplicaApplier`] precedent).
/// Recording is best-effort from the caller's perspective: an audit
/// failure is logged by the implementation and never fails the action
/// being audited.
pub trait AuditSink: Send + Sync {
    fn record(&self, record: AuditRecord);
}

/// A sink that drops everything — the default for contexts with no
/// store (validation-only API state, bare test fixtures).
#[derive(Debug, Clone, Copy)]
pub struct NullAuditSink;

impl AuditSink for NullAuditSink {
    fn record(&self, _record: AuditRecord) {}
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    fn node() -> NodeId {
        NodeId::from_bytes([7u8; 16])
    }

    #[test]
    fn entry_hash_covers_position_and_chain() {
        let base = |seq, prev: [u8; 32]| {
            audit_entry_hash(
                node(),
                seq,
                1,
                AuditAction::TaskDispatched,
                Some("task-1"),
                None,
                "system",
                &prev,
            )
            .expect("hash")
        };
        let zero = [0u8; 32];
        // seq and prev_hash both bind: an entry cannot be lifted to
        // another position or re-linked to another predecessor.
        assert_ne!(base(1, zero), base(2, zero));
        assert_ne!(base(1, zero), base(1, [1u8; 32]));
        // …and neither can it be lifted to another node's chain.
        let other = audit_entry_hash(
            NodeId::from_bytes([9u8; 16]),
            1,
            1,
            AuditAction::TaskDispatched,
            Some("task-1"),
            None,
            "system",
            &zero,
        )
        .expect("hash");
        assert_ne!(base(1, zero), other);
    }

    #[test]
    fn entry_hash_separates_fields_that_concatenation_would_blur() {
        // The reason the preimage is an object: with `a ‖ b` these two
        // records hash identically.
        let a = audit_entry_hash(
            node(),
            1,
            1,
            AuditAction::ShellDenied,
            Some("cap"),
            Some("detail"),
            "system",
            &[0u8; 32],
        )
        .expect("hash");
        let b = audit_entry_hash(
            node(),
            1,
            1,
            AuditAction::ShellDenied,
            Some("capdetail"),
            None,
            "system",
            &[0u8; 32],
        )
        .expect("hash");
        assert_ne!(a, b);
    }

    #[test]
    fn null_and_empty_detail_are_distinct() {
        let with_null = audit_entry_hash(
            node(),
            1,
            1,
            AuditAction::PolicyLoaded,
            None,
            None,
            "system",
            &[0u8; 32],
        )
        .expect("hash");
        let with_empty = audit_entry_hash(
            node(),
            1,
            1,
            AuditAction::PolicyLoaded,
            Some(""),
            Some(""),
            "system",
            &[0u8; 32],
        )
        .expect("hash");
        assert_ne!(with_null, with_empty, "NULL is not the empty string");
    }

    #[test]
    fn actor_never_carries_caller_text() {
        // Plan review MAJOR-2: the closed set is the guarantee. A
        // webhook actor names the CHANNEL, never the sender.
        assert_eq!(AuditActor::LocalAdmin.as_string(), "local_admin");
        assert_eq!(AuditActor::System.as_string(), "system");
        assert_eq!(
            AuditActor::Webhook {
                channel: "sms".into()
            }
            .as_string(),
            "webhook:sms"
        );
        let peer = AuditActor::Peer { node: node() }.as_string();
        assert!(peer.starts_with("peer:"));
        assert!(
            !peer.contains('+') && !peer.contains('@'),
            "no address material can reach the actor column"
        );
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod vector_tests {
    use super::*;

    /// A known-answer vector (diff review MAJOR-6).
    ///
    /// The differential tests above would pass under ANY key ordering.
    /// This one pins the actual bytes: `serde_json`'s map is a
    /// `BTreeMap` in this workspace, but feature unification is
    /// workspace-wide, so a future dependency enabling
    /// `serde_json/preserve_order` would silently reorder the preimage
    /// and every stored chain would stop verifying on upgrade. This
    /// test fails in CI instead.
    #[test]
    fn entry_hash_is_stable_across_versions() {
        let hash = audit_entry_hash(
            NodeId::from_bytes([0x11; 16]),
            42,
            1_700_000_000_000,
            AuditAction::ShellDenied,
            Some("rm"),
            Some(r#"{"reason":"denied"}"#),
            "local_admin",
            &[0x22; 32],
        )
        .expect("hash");
        assert_eq!(
            hash_hex(&hash),
            "73f9ee069b2976154b27de77785cc188add68c6947bd2220f531505e130a7c39",
            "the entry preimage changed — every stored chain stops verifying"
        );
    }

    /// The stored/hashed form and the serde form must not diverge.
    #[test]
    fn action_wire_form_matches_the_hashed_form() {
        for action in [
            AuditAction::TaskDispatched,
            AuditAction::TaskCancelled,
            AuditAction::PlanResumed,
            AuditAction::ShellAllowed,
            AuditAction::ShellDenied,
            AuditAction::SecretAccessed,
            AuditAction::PeerApproved,
            AuditAction::PolicyLoaded,
            AuditAction::CloudEscalated,
            AuditAction::AuditTruncated,
        ] {
            let serde_form = serde_json::to_string(&action).expect("serialize");
            assert_eq!(
                serde_form.trim_matches('"'),
                action.as_str(),
                "serde and as_str must agree or replicated rows re-verify wrong"
            );
        }
    }
}
