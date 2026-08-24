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

/// Outstanding entry requests we will track at once.
///
/// This mesh has no request/response primitive — `OutboundMsg` is
/// fire-and-forget — so a response is matched by an explicit `req_id`
/// against this table. Bounded because the table is fed by our own
/// requests but *keyed* against replies a peer controls the timing of.
const MAX_INFLIGHT_REQUESTS: usize = 32;

/// How long an unanswered request stays matchable. A reply arriving
/// after this is dropped: its `req_id` is gone, so it cannot be
/// mistaken for the answer to a later question.
const REQUEST_TIMEOUT_MS: u64 = 30_000;

/// Entry batches one peer may ask us for per minute. Serving reads
/// rows under the single process-wide connection mutex, and a
/// `RangeReq` is replayable, so the only thing between a trusted peer
/// and unbounded read work is this.
const SERVE_BATCHES_PER_MINUTE: u32 = 20;
const SERVE_WINDOW_MS: u64 = 60_000;

#[derive(Default)]
struct PeerSyncState {
    consecutive_failures: u32,
    skip_ticks: u32,
    /// Serve-side rate limiting: window start and batches served in it.
    serve_window_start_ms: u64,
    served_in_window: u32,
}

/// A request we sent and are still waiting on.
#[derive(Debug, Clone, Copy)]
struct InFlight {
    peer: NodeId,
    subject: NodeId,
    target_seq: u64,
    from_seq: u64,
    sent_at_ms: u64,
}

pub(crate) struct AuditSyncService {
    store: Store,
    identity: Arc<Identity>,
    local_id: NodeId,
    trust: TrustStore,
    net: OnceLock<Weak<PeerNet>>,
    peers: ParkingMutex<HashMap<NodeId, PeerSyncState>>,
    inflight: ParkingMutex<HashMap<[u8; 16], InFlight>>,
    /// Monotonic counter feeding request ids. Ids need to be unique,
    /// not unpredictable — they never authorize anything, and the
    /// reply is validated on content regardless.
    next_req: std::sync::atomic::AtomicU64,
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
            inflight: ParkingMutex::new(HashMap::new()),
            next_req: std::sync::atomic::AtomicU64::new(1),
        })
    }

    pub(crate) fn attach_net(&self, net: &Arc<PeerNet>) {
        let _ = self.net.set(Arc::downgrade(net));
    }

    /// Public keys from our OWN trust store only.
    ///
    /// Never from the relaying envelope: a relayer that could supply
    /// the key would mint a keypair and forge the whole history it
    /// claims to be reporting. A head for a node we cannot
    /// independently key is dropped, not stored — an unverifiable
    /// accusation is worse than none.
    ///
    /// Built once per envelope, not once per head: `all_peers()`
    /// clones the whole peer list, and a 64-head envelope arriving at
    /// the sender's chosen rate would otherwise be 64 full clones
    /// (diff review MAJOR-5).
    fn pubkey_map(&self) -> HashMap<NodeId, PublicKey> {
        self.trust
            .all_peers()
            .into_iter()
            .map(|p| (p.node_id, p.pubkey))
            .collect()
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
        if let Some(req) = &env.range_req {
            self.serve_range(from, req, now_ms);
        }
        if let Some(resp) = &env.range_resp {
            self.consume_range(from, resp, now_ms);
        }
        let mut out = Vec::new();
        let keys = self.pubkey_map();
        for head in env.heads.iter().take(harness_core::MAX_HEADS_PER_ENVELOPE) {
            // Our own chain is authoritative locally; a peer telling
            // us about ourselves proves nothing and a forged one would
            // be an accusation we cannot check.
            if head.node_id == self.local_id {
                continue;
            }
            let Some(pubkey) = keys.get(&head.node_id).copied() else {
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
                    match outcome {
                        PinOutcome::Fork { conflict_id } => tracing::error!(
                            target: "harness.audit.sync",
                            subject = %head.node_id,
                            seq = head.seq,
                            conflict_id,
                            reported_by = %from,
                            "AUDIT CHAIN FORK: two signed heads at one position"
                        ),
                        // A node offering a position below one we
                        // already hold is the loudest signal available
                        // before entries exist — a wiped or restored
                        // DB looks exactly like this — so it is not
                        // silent, even though it is not evidence
                        // (diff review 1(c)/1(d)).
                        // FIRST-PARTY only: a node offering a
                        // position below one we hold for it is the
                        // loudest signal available before entries
                        // exist (a wiped or restored DB looks exactly
                        // like this). A RELAYED head lagging ours is
                        // the normal steady state in any mesh with
                        // relays, and warning on it would drown the
                        // signal the warn exists for.
                        PinOutcome::StalePinned if from == head.node_id => tracing::warn!(
                            target: "harness.audit.sync",
                            subject = %head.node_id,
                            offered_seq = head.seq,
                            "node advertised a head below one we already hold for it"
                        ),
                        PinOutcome::StalePinned => tracing::debug!(
                            target: "harness.audit.sync",
                            subject = %head.node_id,
                            offered_seq = head.seq,
                            reported_by = %from,
                            "relayed head lags the pin we hold"
                        ),
                        PinOutcome::RejectedSeq => tracing::warn!(
                            target: "harness.audit.sync",
                            subject = %head.node_id,
                            reported_by = %from,
                            "audit head seq exceeds the representable range; rejected"
                        ),
                        PinOutcome::Pinned | PinOutcome::Refreshed => {}
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
        let mut relayed: Vec<(AuditHead, u64)> = Vec::new();
        for peer in self.trust.all_peers() {
            if peer.node_id == self.local_id {
                continue;
            }
            match self.store.newest_pin_as_head(peer.node_id) {
                Ok(Some(pair)) => relayed.push(pair),
                Ok(None) => {}
                Err(e) => {
                    tracing::debug!(target: "harness.audit.sync", ?e, "reading pin for relay");
                }
            }
        }
        // Rank by when WE observed the pin, never by the head's own
        // `at_ms` — that field is chosen by the head's signer, so
        // ranking on it lets a node stamp `u64::MAX` and hold a relay
        // slot forever, pushing honest heads out of the window on any
        // mesh bigger than RELAY_HEADS (diff review MAJOR-6).
        relayed.sort_by(|(_, a_seen), (_, b_seen)| b_seen.cmp(a_seen));
        relayed.truncate(RELAY_HEADS);
        relayed.into_iter().map(|(head, _)| head).collect()
    }

    #[cfg(test)]
    pub(crate) fn relayable_heads_for_test(&self) -> Vec<AuditHead> {
        self.relayable_heads()
    }

    /// One push round.
    pub(crate) fn push_round(&self, now_ms: u64) {
        let Some(net) = self.net.get().and_then(Weak::upgrade) else {
            return;
        };
        let mut heads = self.outgoing_heads(now_ms);
        if heads.is_empty() {
            return;
        }
        // Structurally bounded at 1 + RELAY_HEADS, but pin the
        // contract on the send side too so a future change cannot
        // drift past what receivers accept.
        heads.truncate(harness_core::MAX_HEADS_PER_ENVELOPE);
        let mut env = AuditSyncEnvelope {
            source: self.local_id,
            assembled_at: now_ms,
            heads,
            range_req: None,
            range_resp: None,
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
            // A local-queue failure counts too, but the one that
            // matters arrives later through `note_send_failed`: the
            // wire send is asynchronous, so `send_to` returning Ok
            // means only that the message was queued.
            if net
                .send_to(peer, OutboundMsg::AuditHeads(env.clone()))
                .is_err()
            {
                self.note_send_failed(peer);
            }
        }
    }

    /// Ask `peer` for a run of `subject`'s chain, walking toward the
    /// pin at `target_seq`.
    ///
    /// Returns false if the request could not be sent or the in-flight
    /// table is full — pulls are best-effort and retried on a later
    /// tick, because a pin that stays `unchecked` is honest.
    pub(crate) fn request_range(
        &self,
        peer: NodeId,
        subject: NodeId,
        from_seq: u64,
        target_seq: u64,
        now_ms: u64,
    ) -> bool {
        let Some(net) = self.net.get().and_then(Weak::upgrade) else {
            return false;
        };
        let req_id = self.mint_req_id();
        {
            let mut inflight = self.inflight.lock();
            inflight.retain(|_, f| now_ms.saturating_sub(f.sent_at_ms) < REQUEST_TIMEOUT_MS);
            if inflight.len() >= MAX_INFLIGHT_REQUESTS {
                return false;
            }
            inflight.insert(
                req_id,
                InFlight {
                    peer,
                    subject,
                    target_seq,
                    from_seq,
                    sent_at_ms: now_ms,
                },
            );
        }
        let to_seq = target_seq;
        let mut env = AuditSyncEnvelope {
            source: self.local_id,
            assembled_at: now_ms,
            heads: Vec::new(),
            range_req: Some(harness_core::AuditRangeReq {
                req_id,
                node_id: subject,
                from_seq,
                to_seq,
            }),
            range_resp: None,
            sig: harness_core::Signature::from_bytes([0u8; 64]),
        };
        if env.sign(&self.identity).is_err() {
            self.inflight.lock().remove(&req_id);
            return false;
        }
        if net.send_to(peer, OutboundMsg::AuditHeads(env)).is_err() {
            self.inflight.lock().remove(&req_id);
            return false;
        }
        true
    }

    fn mint_req_id(&self) -> [u8; 16] {
        let n = self
            .next_req
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut id = [0u8; 16];
        id[..8].copy_from_slice(&self.local_id.as_bytes()[..8]);
        id[8..].copy_from_slice(&n.to_be_bytes());
        id
    }

    /// Serve a peer's request for entries, rate-limited per peer.
    ///
    /// Reading rows holds the single process-wide connection mutex and
    /// a `RangeReq` is replayable, so without this a trusted peer can
    /// stall dispatch by asking repeatedly.
    fn serve_range(&self, peer: NodeId, req: &harness_core::AuditRangeReq, now_ms: u64) {
        if !self.may_serve(peer, now_ms) {
            tracing::debug!(
                target: "harness.audit.sync",
                peer = %peer,
                "audit range request rate-limited"
            );
            return;
        }
        // `peers.toml` membership IS the trust boundary in this build:
        // nothing constructs `TrustTier::Trusted` outside tests and
        // pairing's QUIC leg is stubbed, so gating on Trusted would
        // ship this path inert. Guests are refused.
        if self.trust.tier(&peer) == harness_mesh::trust::TrustTier::Guest {
            return;
        }
        let Some(net) = self.net.get().and_then(Weak::upgrade) else {
            return;
        };
        let (entries, truncated) =
            match self
                .store
                .audit_entries_for_range(req.node_id, req.from_seq, req.to_seq)
            {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(target: "harness.audit.sync", ?e, "serving audit range");
                    return;
                }
            };
        if entries.is_empty() {
            return;
        }
        let mut env = AuditSyncEnvelope {
            source: self.local_id,
            assembled_at: now_ms,
            heads: Vec::new(),
            range_req: None,
            range_resp: Some(harness_core::AuditRangeResp {
                req_id: req.req_id,
                node_id: req.node_id,
                entries,
                truncated,
            }),
            sig: harness_core::Signature::from_bytes([0u8; 64]),
        };
        if env.sign(&self.identity).is_err() {
            return;
        }
        let _ = net.send_to(peer, OutboundMsg::AuditHeads(env));
    }

    fn may_serve(&self, peer: NodeId, now_ms: u64) -> bool {
        let mut states = self.peers.lock();
        let state = states.entry(peer).or_default();
        if now_ms.saturating_sub(state.serve_window_start_ms) >= SERVE_WINDOW_MS {
            state.serve_window_start_ms = now_ms;
            state.served_in_window = 0;
        }
        if state.served_in_window >= SERVE_BATCHES_PER_MINUTE {
            return false;
        }
        state.served_in_window += 1;
        true
    }

    /// Consume a batch of entries answering one of our requests.
    ///
    /// Returns the ingest outcome for tests and logging.
    fn consume_range(
        &self,
        peer: NodeId,
        resp: &harness_core::AuditRangeResp,
        now_ms: u64,
    ) -> Option<Result<harness_store::IngestProgress, harness_store::IngestRefusal>> {
        // An unmatched or expired id is dropped: it cannot be mistaken
        // for the answer to a later question.
        let flight = {
            let mut inflight = self.inflight.lock();
            inflight.retain(|_, f| now_ms.saturating_sub(f.sent_at_ms) < REQUEST_TIMEOUT_MS);
            inflight.remove(&resp.req_id)?
        };
        if flight.peer != peer || flight.subject != resp.node_id {
            tracing::warn!(
                target: "harness.audit.sync",
                peer = %peer,
                "audit range response does not match its request; dropped"
            );
            return None;
        }
        let outcome = match self.store.audit_ingest_range(
            self.local_id,
            resp.node_id,
            flight.target_seq,
            &resp.entries,
            now_ms,
        ) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(target: "harness.audit.sync", ?e, "ingesting audit range");
                return None;
            }
        };
        match &outcome {
            Ok(progress) => {
                if progress.reached_pin {
                    match self
                        .store
                        .settle_pin_from_run(resp.node_id, flight.target_seq)
                    {
                        Ok(status) => tracing::info!(
                            target: "harness.audit.sync",
                            subject = %resp.node_id,
                            seq = flight.target_seq,
                            status = status.as_str(),
                            "audit pin settled from an entry walk"
                        ),
                        Err(e) => {
                            tracing::warn!(target: "harness.audit.sync", ?e, "settling pin");
                        }
                    }
                } else if resp.truncated {
                    // More to come: ask for the next slice.
                    self.request_range(
                        peer,
                        resp.node_id,
                        progress.through_seq.saturating_add(1),
                        flight.target_seq,
                        now_ms,
                    );
                }
            }
            Err(refusal) => tracing::warn!(
                target: "harness.audit.sync",
                peer = %peer,
                subject = %resp.node_id,
                ?refusal,
                "audit range refused"
            ),
        }
        let _ = flight.from_seq;
        Some(outcome)
    }

    /// A push to `peer` failed on the wire.
    ///
    /// Called from `PeerNet::sender_task` after its retries, because
    /// that is where the failure actually surfaces (Codex P2 on #66):
    /// `send_to` reports only that the message reached the local
    /// queue, so a backoff driven from the push loop would never fire
    /// and a pre-5.13c peer would get its unknown channel opened and
    /// reset on every tick, forever.
    pub(crate) fn note_send_failed(&self, peer: NodeId) {
        let mut states = self.peers.lock();
        let state = states.entry(peer).or_default();
        state.consecutive_failures += 1;
        if state.consecutive_failures >= BACKOFF_AFTER_FAILURES {
            state.skip_ticks = BACKOFF_TICKS;
            state.consecutive_failures = 0;
            tracing::debug!(
                target: "harness.audit.sync",
                peer = %peer,
                "backing off audit head pushes (peer may predate 5.13c)"
            );
        }
    }

    /// A push to `peer` reached the wire. Clears the failure streak.
    pub(crate) fn note_send_ok(&self, peer: NodeId) {
        let mut states = self.peers.lock();
        states.entry(peer).or_default().consecutive_failures = 0;
    }

    #[cfg(test)]
    pub(crate) fn is_backing_off(&self, peer: NodeId) -> bool {
        self.peers
            .lock()
            .get(&peer)
            .is_some_and(|s| s.skip_ticks > 0)
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
