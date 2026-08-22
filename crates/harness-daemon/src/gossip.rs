//! `GossipService` — LWW replica sync over the `harness.gossip.state`
//! QUIC channel (3.3-gossip, ADR-0019).
//!
//! Two convergence paths, both idempotent (the store's LWW merge makes
//! re-delivery harmless):
//!
//! 1. **Periodic delta push** — every [`GOSSIP_INTERVAL`] the service
//!    sends each live peer the replica entries whose `at_ms` is newer
//!    than the per-peer watermark (the max `at_ms` this service has
//!    successfully enqueued to that peer). A fresh peer starts at
//!    watermark 0, so its first push is naturally a full snapshot.
//! 2. **Head-triggered full sync** — heartbeats carry
//!    `Heartbeat::replica_head` (`Store::replica_head()`); when a peer's
//!    advertised head differs from ours, the whole snapshot is re-sent
//!    (rate-limited to one full sync per peer per
//!    [`FULL_SYNC_MIN_INTERVAL`]). This recovers divergence the delta
//!    path cannot see — e.g. an entry applied with an `at_ms` older
//!    than the watermark (third-node relays, clock skew), or increments
//!    lost to a dropped enqueue.
//!
//! Envelope bound: the gossip channel's frame cap is 256 KiB
//! (ADR-0017). Snapshots are therefore **chunked** into envelopes of at
//! most [`CHUNK_MAX_ENTRIES`] entries; `max_entry_wire_bytes_is_bounded`
//! below proves the worst-case entry encoding keeps every chunk under
//! the cap with generous headroom.
//!
//! Envelopes are signed at assembly time with the local identity (the
//! channel is not `Sequenced` — LWW sync is idempotent, replaying an
//! old envelope can never regress state). The recv side (`peer_net`)
//! verifies the signature against the connection pubkey and requires
//! `source == connection peer`.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, Weak};
use std::time::{Duration, Instant};

use harness_core::{Identity, NodeId, ReplicaSyncEnvelope, ReplicatedTaskState, Signable};
use harness_store::Store;
use parking_lot::Mutex as ParkingMutex;
use tokio::sync::watch;
use tokio::time::MissedTickBehavior;

use crate::peer_net::{OutboundMsg, PeerNet};

/// Delta-push cadence. Production ~5 s (PRD §13.6 gossip is a
/// background convergence channel, not a latency path); test builds
/// shrink it so convergence tests run in CI time.
#[cfg(not(test))]
pub(crate) const GOSSIP_INTERVAL: Duration = Duration::from_secs(5);
#[cfg(test)]
pub(crate) const GOSSIP_INTERVAL: Duration = Duration::from_millis(500);

/// At most one head-triggered full sync per peer per this interval.
/// Heads that stay diverged re-trigger on a later heartbeat.
#[cfg(not(test))]
const FULL_SYNC_MIN_INTERVAL: Duration = Duration::from_secs(10);
#[cfg(test)]
const FULL_SYNC_MIN_INTERVAL: Duration = Duration::from_secs(2);

/// Maximum replica entries per envelope. With the worst-case entry
/// encoding ≤ [`MAX_ENTRY_WIRE_BYTES`] this keeps every chunk well
/// under the 256 KiB gossip frame cap (see the proof tests below:
/// 350 × 640 B + overhead ≈ 219 KiB).
pub(crate) const CHUNK_MAX_ENTRIES: usize = 350;

/// Measured upper bound on one CBOR-encoded [`ReplicatedTaskState`]:
/// 16-byte task id + 16-byte source + 256-byte preview
/// (store-truncated) + the longest state string + `u64` `at_ms` +
/// field names/framing. The preview dominates — serde encodes
/// `Vec<u8>` as a CBOR *integer array* (2 bytes per byte ≥ 0x18), so
/// 256 preview bytes cost up to 512 on the wire; the measured worst
/// case is 628 bytes. Verified by `max_entry_wire_bytes_is_bounded`.
#[cfg(test)]
pub(crate) const MAX_ENTRY_WIRE_BYTES: usize = 640;

/// Envelope overhead outside `entries`: `source`, `assembled_at`,
/// `sig`, field names, map framing. Generous. (Consumed only by the
/// compile-time chunk-bound assertion in the test module, which
/// dead-code analysis does not count as a use.)
#[cfg(test)]
#[allow(dead_code)]
pub(crate) const ENVELOPE_OVERHEAD_BYTES: usize = 1024;

#[derive(Default)]
struct PeerGossipState {
    /// Max `at_ms` successfully enqueued to this peer. Delta pushes
    /// send only entries strictly newer than this.
    watermark_ms: u64,
    /// Last head-triggered full sync (rate limiting).
    last_full_sync: Option<Instant>,
}

/// See the module docs.
pub(crate) struct GossipService {
    store: Store,
    identity: Arc<Identity>,
    local_id: NodeId,
    /// Set once after `PeerNet::new` (the net holds us back via a
    /// `Weak` too — no cycle).
    net: OnceLock<Weak<PeerNet>>,
    peers: ParkingMutex<HashMap<NodeId, PeerGossipState>>,
}

impl GossipService {
    pub(crate) fn new(store: Store, identity: Arc<Identity>) -> Arc<Self> {
        let local_id = identity.node_id();
        Arc::new(Self {
            store,
            identity,
            local_id,
            net: OnceLock::new(),
            peers: ParkingMutex::new(HashMap::new()),
        })
    }

    /// Wire the back-reference after `PeerNet::new`.
    pub(crate) fn attach_net(&self, net: &Arc<PeerNet>) {
        let _ = self.net.set(Arc::downgrade(net));
    }

    fn net(&self) -> Option<Arc<PeerNet>> {
        self.net.get().and_then(Weak::upgrade)
    }

    /// The periodic delta-push loop.
    pub(crate) async fn run_gossip_loop(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) {
        let mut tick = tokio::time::interval(GOSSIP_INTERVAL);
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        // Skip the immediate fire — peers connect after boot anyway.
        tick.tick().await;
        loop {
            tokio::select! {
                _ = tick.tick() => self.push_deltas_once(),
                _ = shutdown.changed() => return,
            }
        }
    }

    /// One delta-push pass over every live peer.
    pub(crate) fn push_deltas_once(&self) {
        let Some(net) = self.net() else { return };
        let live = net.live_peers();
        // Sweep state for peers that are gone; a reconnecting peer
        // restarts at watermark 0 → full (correct, if wasteful) re-push.
        self.peers.lock().retain(|p, _| live.contains(p));
        if live.is_empty() {
            return;
        }
        let snapshot = match self.store.replica_snapshot() {
            Ok(s) => s,
            Err(err) => {
                tracing::warn!(target: "harness.gossip", ?err, "replica_snapshot");
                return;
            }
        };
        if snapshot.is_empty() {
            return;
        }
        for peer in live {
            let watermark = {
                let mut guard = self.peers.lock();
                guard.entry(peer).or_default().watermark_ms
            };
            let delta: Vec<ReplicatedTaskState> = snapshot
                .iter()
                .filter(|e| e.at_ms > watermark)
                .cloned()
                .collect();
            if delta.is_empty() {
                continue;
            }
            let max_at = delta.iter().map(|e| e.at_ms).max().unwrap_or(watermark);
            if self.send_chunked(&net, peer, &delta) {
                let mut guard = self.peers.lock();
                let state = guard.entry(peer).or_default();
                state.watermark_ms = state.watermark_ms.max(max_at);
            }
        }
    }

    /// Anti-entropy trigger: called from the heartbeat recv path with
    /// the peer's advertised replica head. All-zero = "not advertised"
    /// (pre-3.3-gossip node or no store) — ignored.
    pub(crate) fn on_peer_head(&self, peer: NodeId, head: [u8; 32]) {
        if head == [0u8; 32] {
            return;
        }
        let local = match self.store.replica_head() {
            Ok(h) => h,
            Err(err) => {
                tracing::warn!(target: "harness.gossip", ?err, "replica_head");
                return;
            }
        };
        if local == head {
            return;
        }
        {
            // Rate limit BEFORE the (potentially large) snapshot read.
            // The stamp is taken even if the send later fails — heads
            // still differ, so a later heartbeat re-triggers after the
            // interval; conservative beats a tight send-fail loop.
            let now = Instant::now();
            let mut guard = self.peers.lock();
            let state = guard.entry(peer).or_default();
            if let Some(last) = state.last_full_sync {
                if now.duration_since(last) < FULL_SYNC_MIN_INTERVAL {
                    return;
                }
            }
            state.last_full_sync = Some(now);
        }
        let Some(net) = self.net() else { return };
        let snapshot = match self.store.replica_snapshot() {
            Ok(s) => s,
            Err(err) => {
                tracing::warn!(target: "harness.gossip", ?err, "replica_snapshot for full sync");
                return;
            }
        };
        if snapshot.is_empty() {
            // Nothing to offer; convergence comes from the peer's side
            // pushing its (superset) state to us.
            return;
        }
        tracing::info!(
            target: "harness.gossip",
            %peer,
            entries = snapshot.len(),
            "replica head diverged; sending full sync"
        );
        let max_at = snapshot.iter().map(|e| e.at_ms).max().unwrap_or(0);
        if self.send_chunked(&net, peer, &snapshot) {
            let mut guard = self.peers.lock();
            let state = guard.entry(peer).or_default();
            state.watermark_ms = state.watermark_ms.max(max_at);
        }
    }

    /// Chunk + sign + enqueue. Returns `true` only if every chunk was
    /// enqueued (the caller advances the watermark only then; a partial
    /// send retries in full next tick — idempotent).
    fn send_chunked(
        &self,
        net: &Arc<PeerNet>,
        peer: NodeId,
        entries: &[ReplicatedTaskState],
    ) -> bool {
        let envelopes = match build_envelopes(&self.identity, self.local_id, entries) {
            Ok(e) => e,
            Err(err) => {
                tracing::error!(target: "harness.gossip", ?err, "sign gossip envelope");
                return false;
            }
        };
        for env in envelopes {
            if let Err(err) = net.send_to(peer, OutboundMsg::Gossip(env)) {
                tracing::debug!(
                    target: "harness.gossip",
                    %peer,
                    %err,
                    "gossip enqueue failed; retrying next tick"
                );
                return false;
            }
        }
        true
    }
}

/// Split `entries` into signed envelopes of at most
/// [`CHUNK_MAX_ENTRIES`] entries each.
fn build_envelopes(
    identity: &Identity,
    local_id: NodeId,
    entries: &[ReplicatedTaskState],
) -> Result<Vec<ReplicaSyncEnvelope>, harness_core::ProtocolError> {
    let assembled_at = now_unix_ms();
    let mut out = Vec::with_capacity(entries.len().div_ceil(CHUNK_MAX_ENTRIES));
    for chunk in entries.chunks(CHUNK_MAX_ENTRIES) {
        let mut env = ReplicaSyncEnvelope {
            source: local_id,
            assembled_at,
            entries: chunk.to_vec(),
            sig: harness_core::Signature::from_bytes([0u8; 64]),
        };
        env.sign(identity)?;
        out.push(env);
    }
    Ok(out)
}

fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use harness_core::{ReplicatedState, TaskId};

    fn entry(at_ms: u64, preview: Option<Vec<u8>>) -> ReplicatedTaskState {
        ReplicatedTaskState {
            task_id: TaskId::new_v7(),
            state: ReplicatedState::Dispatched, // longest state string
            at_ms,
            source: NodeId::from_bytes([0x33; 16]),
            output_preview: preview,
        }
    }

    /// Compile-time arm of the chunk bound: a full chunk of
    /// worst-case entries plus envelope overhead must fit the 256 KiB
    /// gossip frame cap.
    const _: () =
        assert!(CHUNK_MAX_ENTRIES * MAX_ENTRY_WIRE_BYTES + ENVELOPE_OVERHEAD_BYTES < 256 * 1024);

    /// The measurement the chunk bound rests on: a worst-case entry
    /// (max-length state string, `u64::MAX` timestamp, full 256-byte
    /// preview) encodes under [`MAX_ENTRY_WIRE_BYTES`].
    #[test]
    fn max_entry_wire_bytes_is_bounded() {
        let worst = entry(u64::MAX, Some(vec![0xFF; 256]));
        let mut buf = Vec::new();
        ciborium::ser::into_writer(&worst, &mut buf).expect("encode");
        assert!(
            buf.len() <= MAX_ENTRY_WIRE_BYTES,
            "worst-case entry encodes to {} bytes > bound {}",
            buf.len(),
            MAX_ENTRY_WIRE_BYTES
        );
    }

    /// A full worst-case chunk really encodes + signs under the frame
    /// cap — end-to-end, not just by arithmetic.
    #[test]
    fn full_chunk_envelope_fits_frame_cap() {
        let identity = Identity::generate();
        let entries: Vec<_> = (0..CHUNK_MAX_ENTRIES)
            .map(|_| entry(u64::MAX, Some(vec![0xFF; 256])))
            .collect();
        let envs = build_envelopes(&identity, identity.node_id(), &entries).expect("build");
        assert_eq!(envs.len(), 1);
        let mut buf = Vec::new();
        ciborium::ser::into_writer(&envs[0], &mut buf).expect("encode");
        assert!(
            buf.len() < 256 * 1024,
            "full chunk envelope is {} bytes",
            buf.len()
        );
    }

    #[test]
    fn build_envelopes_chunks_and_signs() {
        let identity = Identity::generate();
        let n = CHUNK_MAX_ENTRIES * 2 + 101;
        let entries: Vec<_> = (0..n)
            .map(|i| entry(u64::try_from(i).unwrap(), None))
            .collect();
        let envs = build_envelopes(&identity, identity.node_id(), &entries).expect("build");
        assert_eq!(envs.len(), 3);
        assert_eq!(envs[0].entries.len(), CHUNK_MAX_ENTRIES);
        assert_eq!(envs[1].entries.len(), CHUNK_MAX_ENTRIES);
        assert_eq!(envs[2].entries.len(), 101);
        let total: usize = envs.iter().map(|e| e.entries.len()).sum();
        assert_eq!(total, n);
        for env in &envs {
            assert_eq!(env.source, identity.node_id());
            env.verify_signature(identity.public_key())
                .expect("each chunk envelope is independently signed");
        }
    }

    #[test]
    fn build_envelopes_empty_input_yields_no_envelopes() {
        let identity = Identity::generate();
        let envs = build_envelopes(&identity, identity.node_id(), &[]).expect("build");
        assert!(envs.is_empty());
    }
}
