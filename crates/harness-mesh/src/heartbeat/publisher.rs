//! Heartbeat publisher — builds a fresh [`Heartbeat`] every tick and
//! hands it to a caller-supplied broadcast sink.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use harness_core::{Heartbeat, Identity, NodeId, SemVer, Signable, Signature};

use super::HEARTBEAT_INTERVAL;

/// Snapshot of local-node liveness data the publisher injects into each
/// heartbeat. The daemon refreshes this every tick (or less often if
/// the underlying counters are read by other tasks).
///
/// Fields not represented here (`leader_belief`, `brain_score`, …) come
/// from the publisher's local state; the publisher takes a closure that
/// returns the fresh values per tick.
#[derive(Debug, Clone)]
pub struct HeartbeatPublisherConfig {
    pub queue_depth: u16,
    pub cpu_busy_pct: u8,
    pub cpu_pinned_count: u8,
    pub ram_used_mb: u32,
    pub ram_total_mb: u32,
    pub gpu_used_mb: u32,
    pub gpu_total_mb: u32,
    pub capabilities_hash: [u8; 16],
    pub on_battery: bool,
    pub paused: bool,
    pub version: SemVer,
}

impl Default for HeartbeatPublisherConfig {
    fn default() -> Self {
        Self {
            queue_depth: 0,
            cpu_busy_pct: 0,
            cpu_pinned_count: 0,
            ram_used_mb: 0,
            ram_total_mb: 0,
            gpu_used_mb: 0,
            gpu_total_mb: 0,
            capabilities_hash: [0u8; 16],
            on_battery: false,
            paused: false,
            version: SemVer::new(0, 0, 0),
        }
    }
}

/// Publisher: stateless heartbeat factory. The `next` method builds and
/// signs a fresh [`Heartbeat`] each call, with a monotonic `seq` counter
/// stored on the publisher so callers don't have to coordinate.
///
/// The publisher does NOT spawn a task or hold a connection — that's the
/// daemon's job. Caller pattern:
///
/// ```ignore
/// let mut pub_ = HeartbeatPublisher::new(identity.clone());
/// let mut tick = tokio::time::interval(HEARTBEAT_INTERVAL);
/// loop {
///     tick.tick().await;
///     let hb = pub_.next(&snapshot, leader_belief, brain_score);
///     for conn in &peers {
///         conn.send(&hb).await?;
///     }
/// }
/// ```
pub struct HeartbeatPublisher {
    identity: Arc<Identity>,
    seq: AtomicU64,
}

impl std::fmt::Debug for HeartbeatPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeartbeatPublisher")
            .field("node_id", &self.identity.node_id())
            .field("seq", &self.seq.load(Ordering::SeqCst))
            .finish_non_exhaustive()
    }
}

impl HeartbeatPublisher {
    pub fn new(identity: Arc<Identity>) -> Self {
        Self {
            identity,
            seq: AtomicU64::new(0),
        }
    }

    /// Local node's identity-derived id.
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.identity.node_id()
    }

    /// Build + sign the next heartbeat. Increments `seq` atomically so
    /// concurrent calls each get a distinct seq (broadcast loops typically
    /// run one publisher per node, but we don't enforce single-caller).
    ///
    /// The returned heartbeat is signed and ready to broadcast — but it
    /// is also pre-counted, so dropping it without sending wastes a
    /// `seq` slot. `#[must_use]` to catch the obvious mistake.
    #[must_use = "the heartbeat is signed and pre-counted; send it or you waste a seq"]
    pub fn next(
        &self,
        snapshot: &HeartbeatPublisherConfig,
        leader_belief: NodeId,
        brain_score: i32,
    ) -> Heartbeat {
        // Pre-increment so seq=1 is the first emitted. (ReplayTable
        // does accept seq=0 exactly once via its Option<u64> sentinel,
        // but we keep `0` as a useful "no heartbeat yet" marker for
        // PeerEntry-less callers and to match the wire conventions of
        // ed25519-dalek / TCP / RFC 8032.)
        let seq = self.seq.fetch_add(1, Ordering::SeqCst) + 1;
        let timestamp = unix_ms();
        let mut hb = Heartbeat {
            node_id: self.identity.node_id(),
            seq,
            timestamp,
            queue_depth: snapshot.queue_depth,
            cpu_busy_pct: snapshot.cpu_busy_pct,
            cpu_pinned_count: snapshot.cpu_pinned_count,
            ram_used_mb: snapshot.ram_used_mb,
            ram_total_mb: snapshot.ram_total_mb,
            gpu_used_mb: snapshot.gpu_used_mb,
            gpu_total_mb: snapshot.gpu_total_mb,
            capabilities_hash: snapshot.capabilities_hash,
            in_flight: Vec::new(), // populated by 2.x when tasks land
            leader_belief,
            brain_score,
            on_battery: snapshot.on_battery,
            paused: snapshot.paused,
            version: snapshot.version,
            sig: Signature::from_bytes([0u8; 64]),
        };
        // sign() can only fail on encode error; canonical_bytes for
        // Heartbeat is bounded and deterministic. expect() is justified.
        #[allow(clippy::expect_used)]
        hb.sign(&self.identity)
            .expect("heartbeat sign cannot fail on canonical CBOR encode");
        hb
    }

    /// Read the current seq counter without incrementing. Diagnostics.
    #[must_use]
    pub fn current_seq(&self) -> u64 {
        self.seq.load(Ordering::SeqCst)
    }

    /// Cheap clone of the underlying `Arc<Identity>`. Used by
    /// `HeartbeatService::spawn_broadcaster` to construct a sibling
    /// publisher in the spawned task.
    pub(super) fn identity_arc(&self) -> Arc<Identity> {
        self.identity.clone()
    }

    /// Sanity check that the publisher's interval matches the PRD value.
    /// Exists so a future tuning of [`HEARTBEAT_INTERVAL`] is visible.
    #[must_use]
    pub fn interval() -> Duration {
        HEARTBEAT_INTERVAL
    }
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use harness_core::verify;

    fn snapshot() -> HeartbeatPublisherConfig {
        HeartbeatPublisherConfig {
            queue_depth: 3,
            cpu_busy_pct: 27,
            cpu_pinned_count: 1,
            ram_used_mb: 4096,
            ram_total_mb: 32_768,
            gpu_used_mb: 0,
            gpu_total_mb: 0,
            capabilities_hash: [0xCD; 16],
            on_battery: false,
            paused: false,
            version: SemVer::new(0, 1, 0),
        }
    }

    #[test]
    fn first_heartbeat_has_seq_one() {
        let id = Arc::new(Identity::generate());
        let p = HeartbeatPublisher::new(id);
        let hb = p.next(&snapshot(), p.node_id(), 0);
        assert_eq!(hb.seq, 1);
        assert_eq!(p.current_seq(), 1);
    }

    #[test]
    fn seq_advances_monotonically() {
        let id = Arc::new(Identity::generate());
        let p = HeartbeatPublisher::new(id);
        let hb1 = p.next(&snapshot(), p.node_id(), 0);
        let hb2 = p.next(&snapshot(), p.node_id(), 0);
        let hb3 = p.next(&snapshot(), p.node_id(), 0);
        assert_eq!(hb1.seq, 1);
        assert_eq!(hb2.seq, 2);
        assert_eq!(hb3.seq, 3);
    }

    #[test]
    fn heartbeat_self_verifies() {
        let id = Arc::new(Identity::generate());
        let p = HeartbeatPublisher::new(id.clone());
        let hb = p.next(&snapshot(), p.node_id(), 100);
        // Verify both via the trait method and the underlying free fn.
        hb.verify_signature(id.public_key())
            .expect("self-signed heartbeat must verify");
        // sanity-check the sig is not all zeros
        assert_ne!(hb.sig.to_bytes(), [0u8; 64]);
    }

    #[test]
    fn heartbeat_carries_local_node_id() {
        let id = Arc::new(Identity::from_secret_bytes(&[0x42u8; 32]));
        let p = HeartbeatPublisher::new(id.clone());
        let hb = p.next(&snapshot(), p.node_id(), 0);
        assert_eq!(hb.node_id, id.node_id());
    }

    #[test]
    fn timestamp_is_recent_unix_ms() {
        let id = Arc::new(Identity::generate());
        let p = HeartbeatPublisher::new(id);
        let hb = p.next(&snapshot(), p.node_id(), 0);
        let now = unix_ms();
        // Within 5s window — generous to avoid flakes on slow runners.
        assert!(hb.timestamp <= now);
        assert!(now - hb.timestamp < 5_000);
    }

    #[test]
    fn leader_belief_and_brain_score_propagate() {
        let id = Arc::new(Identity::generate());
        let p = HeartbeatPublisher::new(id);
        let target = NodeId::from_bytes([0x99; 16]);
        let hb = p.next(&snapshot(), target, -42);
        assert_eq!(hb.leader_belief, target);
        assert_eq!(hb.brain_score, -42);
    }

    #[test]
    fn snapshot_fields_propagate() {
        let id = Arc::new(Identity::generate());
        let p = HeartbeatPublisher::new(id);
        let snap = HeartbeatPublisherConfig {
            queue_depth: 999,
            cpu_busy_pct: 88,
            ..snapshot()
        };
        let hb = p.next(&snap, p.node_id(), 0);
        assert_eq!(hb.queue_depth, 999);
        assert_eq!(hb.cpu_busy_pct, 88);
        assert_eq!(hb.capabilities_hash, [0xCD; 16]);
    }

    #[test]
    fn pubkey_independent_verify_helper_works() {
        // Sanity check the free-fn verify path that 1.4's transport uses.
        let id = Arc::new(Identity::generate());
        let p = HeartbeatPublisher::new(id.clone());
        let hb = p.next(&snapshot(), p.node_id(), 0);
        let bytes = hb.canonical_bytes().expect("encode");
        verify(id.public_key(), &bytes, &hb.sig).expect("verify");
    }
}
