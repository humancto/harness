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

use harness_core::{
    AuditEntriesEnvelope, AuditHead, AuditSyncEnvelope, Identity, NodeId, PublicKey, Signable,
};
use harness_mesh::trust::TrustStore;
use harness_store::{PinOutcome, Store};
use parking_lot::Mutex as ParkingMutex;
use tokio::time::MissedTickBehavior;

use crate::peer_net::{OutboundMsg, PeerNet};

/// How often we push our head. Production 30s: a pin every half minute
/// bounds how much history a node could rewrite unobserved, without
/// making the channel chatty.
///
/// Any per-window budget derived from an interval MUST use
/// [`HEAD_PUSH_INTERVAL_PROD`] and never this constant: under
/// `cfg(test)` this is 200ms, so `WINDOW / INTERVAL` becomes 300 and
/// the derived budget is one no test can ever reach — the suite then
/// passes vacuously, which is the failure mode ADR-0041 keeps
/// recording. Only the timer in `run()` takes the `cfg`-gated value.
#[cfg(not(test))]
pub(crate) const HEAD_PUSH_INTERVAL: Duration = HEAD_PUSH_INTERVAL_PROD;
#[cfg(test)]
pub(crate) const HEAD_PUSH_INTERVAL: Duration = Duration::from_millis(200);

/// The production push interval, whatever the build profile.
pub(crate) const HEAD_PUSH_INTERVAL_PROD: Duration = Duration::from_secs(30);

/// Third-party heads relayed alongside our own, newest first.
const RELAY_HEADS: usize = 16;

/// Per-peer window for the log lines a PEER can provoke.
pub(crate) const LOG_WINDOW_MS: u64 = 60_000;
/// `AUDIT CHAIN FORK` lines allowed per peer per window. A fork is the
/// loudest event this subsystem has, so the limit is generous; it
/// exists only because the sender picks the rate.
pub(crate) const FORK_LOGS_PER_WINDOW: u32 = 8;
/// Oversized-envelope lines per peer per window. One is enough: the
/// condition is static for a given sender, so repeats carry nothing.
const OVERSIZE_LOGS_PER_WINDOW: u32 = 1;

/// Which peer-provoked log line is being rated.
#[derive(Clone, Copy)]
enum PeerLog {
    Fork,
    OversizedEnvelope,
}

// An honest envelope carries our own head plus the relay set, so the
// cap in `harness-core` must leave room for all of them. If it ever
// does not, `ingest`'s `take()` silently discards the tail of an
// HONEST sender's envelope — which looks exactly like a quiet peer.
// `harness-core` cannot assert this itself: `RELAY_HEADS` lives here.
// Spelled `> RELAY_HEADS` rather than `>= 1 + RELAY_HEADS` only
// because clippy rejects the latter; it is the same bound, and the
// `1` is our own head.
const _: () = assert!(harness_core::MAX_HEADS_PER_ENVELOPE > RELAY_HEADS);

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
pub(crate) const SERVE_BATCHES_PER_MINUTE: u32 = 20;

pub(crate) const SERVE_WINDOW_MS: u64 = 60_000;

/// Follow-up rounds allowed per pull.
///
/// Derived from the constraint that actually binds, which took three
/// goes to identify (round 6 MAJOR-3):
///
/// - 64, justified from `MAX_INGEST_ROWS_PER_PULL` (5000 rows). The
///   row cap never fires first.
/// - 1024, justified from `MAX_SERVE_BYTES` (768 KiB). Closer, but
///   still not the binding limit.
/// - The real one is [`SERVE_BATCHES_PER_MINUTE`]: rounds are strictly
///   sequential (`consume_range` -> `request_range_round`), and an
///   honest peer stops serving us after 20 batches in its window. Past
///   that, `serve_range` returns without a reply, our in-flight entry
///   times out after `REQUEST_TIMEOUT_MS`, and the pull ends. So a
///   single pull could never exceed ~21 rounds whatever this constant
///   said, and raising it to 1024 bought nothing for honest chains
///   while giving a malicious peer that always sets `truncated` a 16x
///   larger amplification — 1024 round trips and write transactions
///   under the process-wide connection mutex, per request.
///
/// `SERVE_BATCHES_PER_MINUTE + 12` leaves slack for a window that
/// rolls over mid-pull without being a budget a hostile server can
/// spend. A long chain legitimately needs SEVERAL pulls across several
/// windows, which is what the resume path exists for and what
/// 5.13c-3a's eager sweep will drive automatically; the `const
/// assert!` pair below is what pins the relationship, so it cannot
/// drift from the serve budget again.
pub(crate) const MAX_FOLLOWUP_ROUNDS: u32 = SERVE_BATCHES_PER_MINUTE + SERVE_BATCHES_PER_MINUTE / 2;

// The relationship IS the justification, so it is enforced at compile
// time rather than by a test: round 6 found the value changed 64 ->
// 1024 with every test green, because the one test that referenced it
// did so symbolically and therefore tracked any value (round 6
// MAJOR-3, mutation M9). A build that breaks the relationship now
// fails to compile.
const _: () = assert!(
    MAX_FOLLOWUP_ROUNDS > SERVE_BATCHES_PER_MINUTE,
    "a single pull must be able to use a full serve window"
);
// The slack is a FRACTION of the serve budget, not a constant. The
// previous form was independent of it in name only: `SERVE +
// max(SERVE/2, 12)` equals `SERVE + 12` for every budget under 24, so
// tightening the serve budget still broke this assert with a message
// about hostile servers that did not describe the real breach (round 8
// MINOR-14).
const _: () = assert!(
    MAX_FOLLOWUP_ROUNDS <= SERVE_BATCHES_PER_MINUTE * 2,
    "the round budget must stay within twice the serve budget, or it becomes \
     a budget a hostile server can spend"
);

#[derive(Default)]
struct PeerSyncState {
    consecutive_failures: u32,
    skip_ticks: u32,
    /// Serve-side rate limiting: window start and batches served in it.
    serve_window_start_ms: u64,
    served_in_window: u32,
    /// Log rate limiting, with its OWN window start — never the serve
    /// or pin window's. 1b-i's withdrawn version rolled the pin
    /// window's start from the fork limiter while leaving its
    /// counters, which was unreachable only because of call order; if
    /// that order had ever changed the allowance would have frozen at
    /// whatever the counter held and never recovered. Exactly one
    /// place rolls this window, and it resets every counter under it.
    log_window_start_ms: u64,
    fork_logs_in_window: u32,
    oversize_logs_in_window: u32,
}

/// A request we sent and are still waiting on.
#[derive(Debug, Clone, Copy)]
struct InFlight {
    peer: NodeId,
    subject: NodeId,
    target_seq: u64,
    from_seq: u64,
    sent_at_ms: u64,
    /// Follow-ups already issued for this walk. A server that always
    /// sets `truncated` and returns one row per reply would otherwise
    /// keep us asking forever (diff review MAJOR-6).
    round: u32,
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
            // Seeded from the clock, not 1: a counter that resets on
            // every boot lets a peer replay an old reply against a
            // same-numbered new request (diff review MINOR).
            next_req: std::sync::atomic::AtomicU64::new(now_unix_ms()),
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
    /// clones the whole peer list, and a full envelope arriving at the
    /// sender's chosen rate would otherwise be one full clone per head
    /// — `MAX_HEADS_PER_ENVELOPE` of them (diff review MAJOR-5).
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
    ///
    /// Heads only. Entry pulls arrive on `harness.audit.entries` and
    /// land in [`Self::ingest_entries`] — see
    /// [`harness_core::AuditEntriesEnvelope`] for why they are not two
    /// more fields here.
    pub(crate) fn ingest(
        &self,
        from: NodeId,
        env: &AuditSyncEnvelope,
        now_ms: u64,
    ) -> Vec<(NodeId, PinOutcome)> {
        // Also checked in the `harness.audit` recv arm, which keeps
        // its copy because the `audit_sync() == None` fallback there
        // hands the envelope to `handlers.on_audit_heads` without
        // reaching this function. So for HEAD traffic this check is
        // belt-and-braces and the recv-arm one is what production
        // hits; it is here so `m19` can reach it at all (round 6
        // MINOR-10 — round 5's claim that both checks "moved" was
        // wrong for this one: it was duplicated).
        if env.source != from {
            tracing::warn!(
                target: "harness.audit.sync",
                peer = %from,
                claimed = %env.source,
                "audit envelope source does not match the sending peer; dropped"
            );
            return Vec::new();
        }
        let mut out = Vec::new();
        let keys = self.pubkey_map();
        // Dedupe EXACT duplicates only — never choose between two
        // heads on `seq` (round 6 BLOCKER-1).
        //
        // Round 5 deduped to one head per subject keeping the LOWEST
        // seq, reasoning that a duplicate subject is anomalous. That
        // was a regression, not a fix: a subject sending
        // `{seq 9 honest, seq 1 bogus}` had its honest head DROPPED —
        // never pinned at all, so not even `peer_head_pins` could see
        // it. Round 4 at least stored both rows. Pin density is named
        // at the top of this file as the security parameter of the
        // whole design, and that change let any subject collapse its
        // own to a position of its choosing, permanently, and poison
        // every honest peer's relay slot for it. The accompanying
        // comment claimed a subject's direct push was unaffected
        // "where it is the only head for itself", which is false: the
        // subject simply attaches a second head for itself.
        //
        // So every distinct position a sender offers gets pinned, and
        // the ordering problem is solved in the ORDERING
        // (`newest_pin_by_observation`), not by discarding evidence.
        // An exact repeat within one envelope is pure waste and is
        // collapsed; anything else is stored and classified.
        let mut seen: std::collections::HashSet<(NodeId, u64, [u8; 32])> =
            std::collections::HashSet::new();
        // The cap is the only bound on per-envelope work, so the one
        // case where narrowing it to MAX_HEADS_PER_ENVELOPE actually
        // discards something must not be silent.
        //
        // WARN, now that the per-peer log window exists to bound it.
        // It was `debug` only because the sender picks the rate and
        // `harness.audit` has no message rate limit, so an unrated
        // per-envelope WARN was a log-flood lever handed to the
        // sender. Rated at one line per peer per window it is safe to
        // be loud, and being loud matters for the one HONEST case:
        // the const assert is per-binary, and `harness-core`
        // deliberately leaves the cap headroom for `RELAY_HEADS` to
        // grow, so in a mixed-version mesh an upgraded sender whose
        // `1 + RELAY_HEADS` exceeds our cap is truncated here. That
        // must not look like a quiet peer (Decision 31).
        if env.heads.len() > harness_core::MAX_HEADS_PER_ENVELOPE
            && self.may_log(from, PeerLog::OversizedEnvelope, now_ms)
        {
            tracing::warn!(
                target: "harness.audit.sync",
                relayer = %from,
                offered = env.heads.len(),
                cap = harness_core::MAX_HEADS_PER_ENVELOPE,
                "audit envelope carried more heads than the cap; the tail was ignored"
            );
        }
        let mut accepted: Vec<harness_core::AuditHead> = Vec::new();
        for head in env.heads.iter().take(harness_core::MAX_HEADS_PER_ENVELOPE) {
            if !seen.insert((head.node_id, head.seq, head.entry_hash)) {
                continue;
            }
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
            accepted.push(head.clone());
        }
        if accepted.is_empty() {
            return out;
        }
        // ONE lock acquisition and ONE transaction for the whole
        // envelope, where the per-head path cost up to
        // MAX_HEADS_PER_ENVELOPE of each. Sharing the transaction is
        // also what lets `pin_one` see a position an earlier head in
        // this same envelope inserted, and so classify the second
        // branch as the fork it is.
        let outcomes = match self.store.pin_peer_heads(&accepted, from, now_ms) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(
                    target: "harness.audit.sync",
                    ?e,
                    relayer = %from,
                    "pinning peer heads failed"
                );
                return out;
            }
        };
        for (head, outcome) in outcomes {
            self.log_pin_outcome(from, head, &outcome, now_ms);
            out.push((head.node_id, outcome));
        }
        out
    }

    /// Handle an entry-pull envelope from `harness.audit.entries`.
    ///
    /// The recv arm has already verified the envelope signature
    /// against the connection peer and cross-checked `source`.
    pub(crate) fn ingest_entries(&self, from: NodeId, env: &AuditEntriesEnvelope, now_ms: u64) {
        // The cross-check lives HERE, not in the recv arm, so a unit
        // test can reach it. Round 5 found the recv-arm copy surviving
        // deletion with all 128 daemon tests green: a guard inside a
        // QUIC read loop has no cheap test, and a guard with no test is
        // a guard that disappears. Neither `serve_range` nor
        // `consume_range` reads `env.source` today, so this is hygiene
        // rather than a live hole — but the next change that does read
        // it would inherit an unguarded field.
        if env.source != from {
            tracing::warn!(
                target: "harness.audit.sync",
                peer = %from,
                claimed = %env.source,
                "audit entries source does not match the sending peer; dropped"
            );
            return;
        }
        if let Some(req) = &env.range_req {
            self.serve_range(from, req, now_ms);
        }
        if let Some(resp) = &env.range_resp {
            self.consume_range(from, resp, now_ms);
        }
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

    /// Drive one real request round, so a test can assert the
    /// per-pull round bound and the in-flight bookkeeping that goes
    /// with it.
    #[cfg(test)]
    pub(crate) fn request_range_round_for_test(
        &self,
        peer: NodeId,
        subject: NodeId,
        from_seq: u64,
        target_seq: u64,
        round: u32,
        now_ms: u64,
    ) -> bool {
        self.request_range_round(peer, subject, from_seq, target_seq, round, now_ms)
    }

    #[cfg(test)]
    pub(crate) fn inflight_len_for_test(&self) -> usize {
        self.inflight.lock().len()
    }

    /// Register an in-flight request without sending it, so a test
    /// can deliver a crafted response without a live peer racing it
    /// with the honest answer.
    #[cfg(test)]
    pub(crate) fn track_inflight_for_test(
        &self,
        peer: NodeId,
        subject: NodeId,
        from_seq: u64,
        target_seq: u64,
        now_ms: u64,
    ) -> [u8; 16] {
        let round = 0;
        let req_id = self.mint_req_id();
        self.inflight.lock().insert(
            req_id,
            InFlight {
                peer,
                subject,
                target_seq,
                from_seq,
                sent_at_ms: now_ms,
                round,
            },
        );
        req_id
    }

    #[cfg(test)]
    pub(crate) fn relayable_heads_for_test(&self) -> Vec<AuditHead> {
        self.relayable_heads()
    }

    /// One push round.
    /// Build and sign the head-push envelope.
    ///
    /// Extracted from [`Self::push_round`] so a test can assert on the
    /// envelope PRODUCTION actually sends (round 6 MAJOR-2). The
    /// cross-version guard used a hand-written fixture, and a fixture
    /// co-varies with the snapshot beside it: adding a field and
    /// populating it HERE while leaving the fixture at its default —
    /// which is what the compiler nudges an author to do — left every
    /// test green. A guard against a wire break has to read the wire.
    pub(crate) fn assemble_push_envelope(&self, now_ms: u64) -> Option<AuditSyncEnvelope> {
        let mut heads = self.outgoing_heads(now_ms);
        if heads.is_empty() {
            return None;
        }
        // Structurally bounded at 1 + RELAY_HEADS, but pin the
        // contract on the send side too so a future change cannot
        // drift past what receivers accept.
        heads.truncate(harness_core::MAX_HEADS_PER_ENVELOPE);
        let mut env = AuditSyncEnvelope {
            source: self.local_id,
            assembled_at: now_ms,
            heads,
            sig: harness_core::Signature::from_bytes([0u8; 64]),
        };
        if let Err(e) = env.sign(&self.identity) {
            tracing::warn!(target: "harness.audit.sync", ?e, "signing audit envelope");
            return None;
        }
        Some(env)
    }

    /// One push round: assemble the envelope and send it to every
    /// live peer that is not backed off.
    pub(crate) fn push_round(&self, now_ms: u64) {
        let Some(net) = self.net.get().and_then(Weak::upgrade) else {
            return;
        };
        let Some(env) = self.assemble_push_envelope(now_ms) else {
            return;
        };

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
        self.request_range_round(peer, subject, from_seq, target_seq, 0, now_ms)
    }

    fn request_range_round(
        &self,
        peer: NodeId,
        subject: NodeId,
        from_seq: u64,
        target_seq: u64,
        round: u32,
        now_ms: u64,
    ) -> bool {
        if round > MAX_FOLLOWUP_ROUNDS {
            tracing::warn!(
                target: "harness.audit.sync",
                subject = %subject,
                round,
                "audit walk exceeded its follow-up budget; abandoned"
            );
            return false;
        }
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
                    round,
                },
            );
        }
        let to_seq = target_seq;
        let mut env = AuditEntriesEnvelope {
            source: self.local_id,
            assembled_at: now_ms,
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
        if net.send_to(peer, OutboundMsg::AuditEntries(env)).is_err() {
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
        // Trust gate BEFORE the budget, so a refused Guest does not
        // burn the allowance of a peer we would have served (diff
        // review MINOR).
        //
        // `peers.toml` membership IS the trust boundary in this build:
        // nothing constructs `TrustTier::Trusted` outside tests and
        // pairing's QUIC leg is stubbed, so gating on Trusted would
        // ship this path inert.
        if self.trust.tier(&peer) == harness_mesh::trust::TrustTier::Guest {
            return;
        }
        if !self.may_serve(peer, now_ms) {
            // Warn, not debug: this silently ends the requester's walk
            // (no reply, in-flight slot times out, nothing retries
            // until the next pull), so an operator wondering why a
            // verify never settled needs to see it (round 6 MAJOR-3).
            tracing::warn!(
                target: "harness.audit.sync",
                peer = %peer,
                limit = SERVE_BATCHES_PER_MINUTE,
                "audit range request rate-limited; the peer's walk ends here until its next pull"
            );
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
        let mut env = AuditEntriesEnvelope {
            source: self.local_id,
            assembled_at: now_ms,
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
        let _ = net.send_to(peer, OutboundMsg::AuditEntries(env));
    }

    /// May this peer be served one more entry BATCH in this window?
    ///
    /// Decision 18's limiter, and only that: it gates `serve_range` on
    /// `harness.audit.entries`. It does NOT rate-limit head ingest on
    /// `harness.audit` — a different channel and a different path,
    /// which is why closing that gap needs its own budget (ROADMAP
    /// 5.13c-1b). The revert of a first attempt at that budget left
    /// its doc comment stranded here, describing behaviour this
    /// function never had (round 9 finding 5).
    /// Whether this peer may provoke one more log line of `kind`.
    ///
    /// Takes the `peers` lock and nothing else. The lock-order rule
    /// this file now depends on: the `peers` mutex is NEVER held
    /// across a `Store::with_conn` call, which holds the single
    /// process-wide non-reentrant connection mutex. Every caller here
    /// runs after the store work, not around it.
    fn may_log(&self, peer: NodeId, kind: PeerLog, now_ms: u64) -> bool {
        let mut states = self.peers.lock();
        let state = states.entry(peer).or_default();
        if now_ms.saturating_sub(state.log_window_start_ms) >= LOG_WINDOW_MS {
            state.log_window_start_ms = now_ms;
            state.fork_logs_in_window = 0;
            state.oversize_logs_in_window = 0;
        }
        let (count, limit) = match kind {
            PeerLog::Fork => (&mut state.fork_logs_in_window, FORK_LOGS_PER_WINDOW),
            PeerLog::OversizedEnvelope => {
                (&mut state.oversize_logs_in_window, OVERSIZE_LOGS_PER_WINDOW)
            }
        };
        if *count >= limit {
            return false;
        }
        *count += 1;
        true
    }

    #[cfg(test)]
    pub(crate) fn fork_logs_in_window_for_test(&self, peer: NodeId) -> u32 {
        self.peers
            .lock()
            .entry(peer)
            .or_default()
            .fork_logs_in_window
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

    /// How many batches this peer has been ALLOWED to be served in
    /// the current window.
    ///
    /// The observable for `m16`/`m17`. Both the tier gate and the
    /// budget return before `serve_range` looks up the net, so a test
    /// that watched for a sent frame could not tell a refusal from a
    /// peer with no connection — which is exactly how the round-bound
    /// test managed to pass with its guard deleted (BLOCKER-1).
    #[cfg(test)]
    pub(crate) fn served_in_window_for_test(&self, peer: NodeId) -> u32 {
        self.peers
            .lock()
            .get(&peer)
            .map_or(0, |s| s.served_in_window)
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
        // The batch must START where we asked (Codex P1 on #67).
        // Without this, a malicious subject answers a request for
        // 1..target with ONLY its valid entry at `target`: a fresh run
        // accepts that arbitrary starting point, the final hash equals
        // the pin, and the pin is marked corroborated without a single
        // preceding entry being walked — which is the entire guarantee.
        match resp.entries.first() {
            Some(first) if first.seq == flight.from_seq => {}
            Some(first) => {
                tracing::warn!(
                    target: "harness.audit.sync",
                    peer = %peer,
                    subject = %resp.node_id,
                    asked_from = flight.from_seq,
                    got_from = first.seq,
                    "audit range response does not start where we asked; dropped"
                );
                return None;
            }
            None => return None,
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
                    self.request_range_round(
                        peer,
                        resp.node_id,
                        progress.through_seq.saturating_add(1),
                        flight.target_seq,
                        flight.round.saturating_add(1),
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

impl AuditSyncService {
    /// Log one pin outcome. Split out of `ingest` only to keep it
    /// under clippy's line limit; the arms are the interesting part
    /// and belong read together.
    fn log_pin_outcome(
        &self,
        from: NodeId,
        head: &harness_core::AuditHead,
        outcome: &PinOutcome,
        now_ms: u64,
    ) {
        match outcome {
            // Rated per peer per window. A fork is the loudest event
            // this subsystem has and the row is written either way, so
            // the limit only bounds the LINES — otherwise a sender
            // that mints conflicts at its own position turns our own
            // alarm into its flood (24 per envelope, unrated channel).
            PinOutcome::Fork { conflict_id } => {
                if self.may_log(from, PeerLog::Fork, now_ms) {
                    tracing::error!(
                        target: "harness.audit.sync",
                        subject = %head.node_id,
                        seq = head.seq,
                        conflict_id,
                        reported_by = %from,
                        "AUDIT CHAIN FORK: two signed heads at one position"
                    );
                }
            }
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
                offered_seq = head.seq,
                reported_by = %from,
                "audit head seq is not storable; rejected"
            ),
            PinOutcome::Pinned | PinOutcome::Refreshed => {}
        }
    }
}

fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

/// 5.13c-2: the API's narrow view of entry pulling.
///
/// Kept here rather than in `harness-api` because the service is a
/// daemon concern; the API sees only the trait (the `PauseControl`
/// precedent). Without this impl the endpoint would answer
/// `mesh_not_configured` forever — a route that exists and never works
/// is the inertness defect this project has hit repeatedly.
impl harness_api::AuditPuller for AuditSyncService {
    fn request_range(&self, peer: NodeId, subject: NodeId, from_seq: u64, target_seq: u64) -> bool {
        AuditSyncService::request_range(self, peer, subject, from_seq, target_seq, now_unix_ms())
    }
}

impl std::fmt::Debug for AuditSyncService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuditSyncService")
            .field("local_id", &self.local_id)
            .finish_non_exhaustive()
    }
}
