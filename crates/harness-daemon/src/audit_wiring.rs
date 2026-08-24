//! 5.13a (ADR-0041): the daemon-side glue that turns privileged
//! actions into audit-chain entries.
//!
//! Two of the §10.6 sites cannot call the store directly — the crates
//! they live in are core-only by design — and two more are best
//! observed from outside the code that performs them:
//!
//! - **Secret access** happens in `SecretsStore::get`, on the
//!   EXECUTING node. Wrapping the store in [`AuditingSecrets`] audits
//!   every present and future consumer for free. (The routing-side
//!   `SecretAwareLiveSet` is NOT the access site — its own docs say it
//!   is not a security boundary; it only decides eligibility.)
//! - **Peer approval** already broadcasts `TrustEvent`; subscribing is
//!   cheaper and harder to forget than editing every approval path.

use std::sync::Arc;

use harness_core::{AuditAction, AuditActor, AuditRecord, AuditSink, NodeId};
use harness_vault::{SecretValue, SecretsStore};

/// Wraps a [`SecretsStore`] and records every successful lookup BY
/// TAG (5.13a).
///
/// The value never reaches the record — `SecretValue`'s whole purpose
/// is that it does not leave the redaction wall, and this row is
/// destined for replication in 5.13c. A miss is not recorded: asking
/// for a tag this node does not hold is routing noise, not a
/// privileged action.
pub(crate) struct AuditingSecrets {
    inner: Arc<dyn SecretsStore>,
    sink: Arc<dyn AuditSink>,
}

impl AuditingSecrets {
    pub(crate) fn new(inner: Arc<dyn SecretsStore>, sink: Arc<dyn AuditSink>) -> Arc<Self> {
        Arc::new(Self { inner, sink })
    }
}

impl std::fmt::Debug for AuditingSecrets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuditingSecrets").finish_non_exhaustive()
    }
}

impl SecretsStore for AuditingSecrets {
    fn get(&self, tag: &str) -> Option<SecretValue> {
        let found = self.inner.get(tag);
        if found.is_some() {
            self.sink.record(
                AuditRecord::new(AuditAction::SecretAccessed, AuditActor::System)
                    // The TAG, never the value.
                    .with_subject(tag.to_string()),
            );
        }
        found
    }

    fn tags(&self) -> Vec<String> {
        self.inner.tags()
    }
}

/// Record peer approvals by subscribing to the trust store's existing
/// broadcast (5.13a). Runs until the daemon shuts down.
pub(crate) async fn run_trust_auditor(
    mut events: tokio::sync::broadcast::Receiver<harness_mesh::trust::TrustEvent>,
    sink: Arc<dyn AuditSink>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            event = events.recv() => match event {
                Ok(harness_mesh::trust::TrustEvent::Added(peer)) => {
                    sink.record(
                        AuditRecord::new(AuditAction::PeerApproved, AuditActor::System)
                            .with_subject(peer.node_id.to_string())
                            .with_detail(&serde_json::json!({
                                "tier": format!("{:?}", peer.tier),
                            })),
                    );
                }
                // Removals and tier changes are privileged too, but
                // §10.6 names approval; recording the rest is 5.13b's
                // call once the History page exists to show them.
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!(
                        target: "harness.audit",
                        missed = n,
                        "trust event lag; approvals in the gap are unrecorded"
                    );
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
            },
            _ = shutdown.changed() => return,
        }
    }
}

/// Record that policy was (re)loaded — the §10.6 "policy change" row.
pub(crate) fn audit_policy_loaded(sink: &Arc<dyn AuditSink>, source: &str) {
    sink.record(
        AuditRecord::new(AuditAction::PolicyLoaded, AuditActor::System)
            .with_subject(source.to_string()),
    );
}

/// Record a dispatch — the §10.6 "dispatch" row.
///
/// `detail` carries the task's shape, never its input: a
/// webhook-minted plan's input IS the user's message text (plan
/// review MAJOR-2).
pub(crate) fn audit_dispatch(
    sink: &Arc<dyn AuditSink>,
    task_id: harness_core::TaskId,
    capability: &str,
    to_node: NodeId,
    issued_by: NodeId,
    local: NodeId,
) {
    let actor = if issued_by == local {
        AuditActor::System
    } else {
        AuditActor::Peer { node: issued_by }
    };
    sink.record(
        AuditRecord::new(AuditAction::TaskDispatched, actor)
            .with_subject(task_id.0.to_string())
            .with_detail(&serde_json::json!({
                "capability": capability,
                "to": to_node.to_string(),
            })),
    );
}
