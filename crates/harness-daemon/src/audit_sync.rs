//! 5.13c-1 (ADR-0041) — audit head exchange over `harness.audit`.
//!
//! Pushes this node's signed [`AuditHead`] to every live peer, and
//! relays the third-party heads it holds so a pin survives its subject
//! going offline. Incoming heads are verified and pinned; a second
//! validly-signed head at a position we already hold is a fork.
//!
//! Pin density is the security parameter of the whole design — a
//! rewrite is only detectable between positions someone pinned — so
//! the push runs on a timer rather than only on change.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;

use harness_core::{AuditHead, AuditSyncEnvelope, Identity, NodeId, PublicKey, Signable};
use harness_mesh::trust::TrustStore;
use harness_store::{PinOutcome, Store};
use parking_lot::Mutex as ParkingMutex;
use tokio::time::MissedTickBehavior;

use crate::peer_net::{OutboundMsg, PeerNet};

/// How often we push our head. Production 30s: a pin every half minute
/// bounds how much history a node could rewrite unobserved, without
/// making the channel chatty.
#[cfg(not(test))]
pub(crate) const HEAD_PUSH_INTERVAL: Duration = Duration::from_secs(30);
#[cfg(test)]
pub(crate) const HEAD_PUSH_INTERVAL: Duration = Duration::from_millis(200);

/// Third-party heads relayed alongside our own, newest first.
const RELAY_HEADS: usize = 16;

/// Consecutive send failures to one peer before we back off. A
/// pre-5.13c peer resets the unknown channel every time, so without
/// this the loop opens and resets a stream against every old node on
/// every tick and logs it.
const BACKOFF_AFTER_FAILURES: u32 = 3;
/// Ticks skipped once a peer is in backoff.
const BACKOFF_TICKS: u32 = 10;

#[derive(Default)]
struct PeerSyncState {
    consecutive_failures: u32,
    skip_ticks: u32,
}

pub(crate) struct AuditSyncService {
    store: Store,
    identity: Arc<Identity>,
    local_id: NodeId,
    trust: TrustStore,
    net: OnceLock<Weak<PeerNet>>,
    peers: ParkingMutex<HashMap<NodeId, PeerSyncState>>,
}

impl AuditSyncService {
    pub(crate) fn new(store: Store, identity: Arc<Identity>, trust: TrustStore) -> Arc<Self> {
        let local_id = identity.node_id();
        Arc::new(Self {
            store,
            identity,
            local_id,
            trust,
            net: OnceLock::new(),
            peers: ParkingMutex::new(HashMap::new()),
        })
    }

    pub(crate) fn attach_net(&self, net: &Arc<PeerNet>) {
        let _ = self.net.set(Arc::downgrade(net));
    }

    /// Public key for a node, from our OWN trust store only.
    ///
    /// Never from the relaying envelope: a relayer that could supply
    /// the key would mint a keypair and forge the whole history it
    /// claims to be reporting. A head for a node we cannot
    /// independently key is dropped, not stored — an unverifiable
    /// accusation is worse than none.
    fn pubkey_for(&self, node: NodeId) -> Option<PublicKey> {
        self.trust
            .all_peers()
            .into_iter()
            .find(|p| p.node_id == node)
            .map(|p| p.pubkey)
    }

    /// Verify and pin every head in an envelope.
    ///
    /// Returns the outcomes, for tests and logging. Called from the
    /// `harness.audit` recv arm, which has already checked the
    /// envelope signature against the connection peer.
    pub(crate) fn ingest(
        &self,
        from: NodeId,
        env: &AuditSyncEnvelope,
        now_ms: u64,
    ) -> Vec<(NodeId, PinOutcome)> {
        let mut out = Vec::new();
        for head in env.heads.iter().take(harness_core::MAX_HEADS_PER_ENVELOPE) {
            // Our own chain is authoritative locally; a peer telling
            // us about ourselves proves nothing and a forged one would
            // be an accusation we cannot check.
            if head.node_id == self.local_id {
                continue;
            }
            let Some(pubkey) = self.pubkey_for(head.node_id) else {
                tracing::debug!(
                    target: "harness.audit.sync",
                    relayer = %from,
                    subject = %head.node_id,
                    "head for a node we cannot independently key; dropped"
                );
                continue;
            };
            if head.verify_signature(&pubkey).is_err() {
                tracing::warn!(
                    target: "harness.audit.sync",
                    relayer = %from,
                    subject = %head.node_id,
                    "audit head failed signature verification; dropped"
                );
                continue;
            }
            match self.store.pin_peer_head(head, from, now_ms) {
                Ok(outcome) => {
                    if let PinOutcome::Fork { conflict_id } = outcome {
                        tracing::error!(
                            target: "harness.audit.sync",
                            subject = %head.node_id,
                            seq = head.seq,
                            conflict_id,
                            reported_by = %from,
                            "AUDIT CHAIN FORK: two signed heads at one position"
                        );
                    }
                    out.push((head.node_id, outcome));
                }
                Err(e) => tracing::warn!(
                    target: "harness.audit.sync",
                    ?e,
                    subject = %head.node_id,
                    "pinning peer head failed"
                ),
            }
        }
        out
    }

    /// Our own signed head plus the newest third-party heads we hold.
    fn outgoing_heads(&self, now_ms: u64) -> Vec<AuditHead> {
        let mut heads = Vec::new();
        match self.store.signed_audit_head(&self.identity, now_ms) {
            Ok(Some(head)) => heads.push(head),
            Ok(None) => {}
            Err(e) => tracing::warn!(target: "harness.audit.sync", ?e, "signing local head"),
        }
        heads.extend(self.relayable_heads());
        heads
    }

    /// Third-party heads worth relaying. Relaying is what lets a pin
    /// outlive its subject: node C can learn A's head from B and it
    /// still verifies against A's key.
    fn relayable_heads(&self) -> Vec<AuditHead> {
        let mut relayed = Vec::new();
        for peer in self.trust.all_peers() {
            if peer.node_id == self.local_id {
                continue;
            }
            match self.store.newest_pin_as_head(peer.node_id) {
                Ok(Some(head)) => relayed.push(head),
                Ok(None) => {}
                Err(e) => {
                    tracing::debug!(target: "harness.audit.sync", ?e, "reading pin for relay");
                }
            }
        }
        relayed.sort_by(|a, b| b.at_ms.cmp(&a.at_ms));
        relayed.truncate(RELAY_HEADS);
        relayed
    }

    /// One push round.
    pub(crate) fn push_round(&self, now_ms: u64) {
        let Some(net) = self.net.get().and_then(Weak::upgrade) else {
            return;
        };
        let heads = self.outgoing_heads(now_ms);
        if heads.is_empty() {
            return;
        }
        let mut env = AuditSyncEnvelope {
            source: self.local_id,
            assembled_at: now_ms,
            heads,
            sig: harness_core::Signature::from_bytes([0u8; 64]),
        };
        if let Err(e) = env.sign(&self.identity) {
            tracing::warn!(target: "harness.audit.sync", ?e, "signing audit envelope");
            return;
        }

        for peer in net.live_peers() {
            if peer == self.local_id {
                continue;
            }
            {
                let mut states = self.peers.lock();
                let state = states.entry(peer).or_default();
                if state.skip_ticks > 0 {
                    state.skip_ticks -= 1;
                    continue;
                }
            }
            let sent = net.send_to(peer, OutboundMsg::AuditHeads(env.clone()));
            let mut states = self.peers.lock();
            let state = states.entry(peer).or_default();
            if sent.is_ok() {
                state.consecutive_failures = 0;
            } else {
                state.consecutive_failures += 1;
                if state.consecutive_failures >= BACKOFF_AFTER_FAILURES {
                    // Most likely a pre-5.13c peer resetting the
                    // unknown channel. Back off rather than opening
                    // and resetting a stream every tick.
                    state.skip_ticks = BACKOFF_TICKS;
                    state.consecutive_failures = 0;
                    tracing::debug!(
                        target: "harness.audit.sync",
                        peer = %peer,
                        "backing off audit head pushes (peer may predate 5.13c)"
                    );
                }
            }
        }
    }

    pub(crate) async fn run(self: Arc<Self>, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        let mut tick = tokio::time::interval(HEAD_PUSH_INTERVAL);
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = tick.tick() => self.push_round(now_unix_ms()),
                _ = shutdown.changed() => return,
            }
        }
    }
}

fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}
