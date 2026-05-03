//! Data-transfer objects exchanged between `harness-api` and the
//! `SvelteKit` UI in `ui/`.
//!
//! These are **the wire contract** between the daemon and the browser.
//! Every change must be reflected in `ui/src/lib/types.ts`. The
//! integration tests snapshot these shapes (via `insta`) so any drift
//! is caught at PR review.
//!
//! Field naming is `snake_case` because:
//! 1. The Rust types use `snake_case`.
//! 2. The `harness-core` types already serialize as `snake_case`.
//! 3. The UI declares matching `snake_case` interfaces in
//!    `ui/src/lib/types.ts`.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use harness_core::{Heartbeat, NodeId, PublicKey};
use harness_mesh::heartbeat::PeerEntry;
use serde::{Deserialize, Serialize};

/// Resources component — derived from a [`Heartbeat`]'s sampled fields.
/// Every field is optional because older heartbeats (or sleeping nodes)
/// may not report values.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResourcesDto {
    /// CPU load average over the last sample window. Range `0.0..=1.0`.
    pub cpu_load_avg: Option<f32>,
    pub cpu_count: Option<u32>,
    pub ram_total_bytes: Option<u64>,
    pub ram_avail_bytes: Option<u64>,
    /// Battery percent `0..=100`.
    pub battery_percent: Option<u8>,
    pub on_ac: Option<bool>,
    pub has_gpu: Option<bool>,
}

impl ResourcesDto {
    fn from_heartbeat(hb: &Heartbeat) -> Self {
        let ram_total_bytes = if hb.ram_total_mb == 0 {
            None
        } else {
            Some(u64::from(hb.ram_total_mb) * 1024 * 1024)
        };
        let ram_used_bytes = if hb.ram_used_mb == 0 {
            None
        } else {
            Some(u64::from(hb.ram_used_mb) * 1024 * 1024)
        };
        let ram_avail_bytes = match (ram_total_bytes, ram_used_bytes) {
            (Some(total), Some(used)) => Some(total.saturating_sub(used)),
            _ => None,
        };

        Self {
            cpu_load_avg: Some(f32::from(hb.cpu_busy_pct) / 100.0),
            cpu_count: None, // Heartbeat doesn't carry it directly; manifest does.
            ram_total_bytes,
            ram_avail_bytes,
            battery_percent: None,
            on_ac: Some(!hb.on_battery),
            has_gpu: Some(hb.gpu_total_mb > 0),
        }
    }
}

/// One peer in the mesh as seen from the local node.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PeerDto {
    /// Hex-encoded `NodeId`.
    pub node_id: String,
    /// Short fingerprint (`PublicKey::fingerprint_hex`) for human display.
    pub pubkey_fp: String,
    pub mesh_name: String,
    pub brain_score: i32,
    /// Hex-encoded `NodeId`, or `None` if pre-election.
    pub leader_belief: Option<String>,
    pub seq: u64,
    /// Milliseconds since this node last sent us a heartbeat.
    pub last_seen_ms_ago: u64,
    pub resources: Option<ResourcesDto>,
    pub capabilities_summary: Vec<String>,
}

impl PeerDto {
    /// Build from a verified peer entry. `mesh_name` comes from outside
    /// the heartbeat envelope (it's a daemon-static config value, not
    /// per-tick state).
    #[must_use]
    pub fn from_entry(entry: &PeerEntry, mesh_name: &str) -> Self {
        let last_seen_ms_ago =
            u64::try_from(entry.last_seen.elapsed().as_millis()).unwrap_or(u64::MAX);
        Self::from_heartbeat(&entry.heartbeat, mesh_name, last_seen_ms_ago)
    }

    fn from_heartbeat(hb: &Heartbeat, mesh_name: &str, last_seen_ms_ago: u64) -> Self {
        let leader_belief = if hb.leader_belief.as_bytes() == &[0u8; 16] {
            None
        } else {
            Some(format!("{}", hb.leader_belief))
        };

        // For peers we don't have the pubkey — `node_id` is already
        // `blake3(pubkey)[..16]`, and `PublicKey::fingerprint_hex` is
        // the first 8 bytes (16 hex chars) of `blake3(pubkey)`. Take
        // those same 8 bytes from the node id for a stable fingerprint.
        let node_id_hex = format!("{}", hb.node_id);
        Self {
            node_id: node_id_hex.clone(),
            pubkey_fp: node_id_hex[..16].to_owned(),
            mesh_name: mesh_name.to_owned(),
            brain_score: hb.brain_score,
            leader_belief,
            seq: hb.seq,
            last_seen_ms_ago,
            resources: Some(ResourcesDto::from_heartbeat(hb)),
            capabilities_summary: vec![],
        }
    }

    /// Synthesize a "self" peer for the local node — there is no
    /// `Heartbeat` for ourselves in the peer table, so we build one
    /// from current state.
    #[must_use]
    pub fn local(
        node_id: NodeId,
        pubkey: &PublicKey,
        mesh_name: &str,
        brain_score: i32,
        leader_belief: Option<NodeId>,
        seq: u64,
        capabilities: Vec<String>,
    ) -> Self {
        Self {
            node_id: format!("{node_id}"),
            pubkey_fp: pubkey.fingerprint_hex(),
            mesh_name: mesh_name.to_owned(),
            brain_score,
            leader_belief: leader_belief.map(|id| format!("{id}")),
            seq,
            last_seen_ms_ago: 0,
            resources: None,
            capabilities_summary: capabilities,
        }
    }
}

/// Local-node variant — adds a non-clobberable `is_local: true` flag
/// for the UI to discriminate against. Otherwise identical to [`PeerDto`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LocalDto {
    #[serde(flatten)]
    pub peer: PeerDto,
    pub is_local: bool,
}

impl LocalDto {
    #[must_use]
    pub fn new(peer: PeerDto) -> Self {
        Self {
            peer,
            is_local: true,
        }
    }
}

/// Top-level snapshot returned by `GET /api/v1/peers`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PeersSnapshot {
    pub local: LocalDto,
    pub peers: Vec<PeerDto>,
    pub leader_belief: Option<String>,
    /// Unix milliseconds when this snapshot was assembled.
    pub fetched_at_ms: u64,
}

impl PeersSnapshot {
    #[must_use]
    pub fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
            .unwrap_or(0)
    }
}

/// Local-node detail returned by `GET /api/v1/status`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StatusDto {
    pub node_id: String,
    pub pubkey_fp: String,
    pub mesh_name: String,
    pub brain_score: i32,
    pub leader_belief: Option<String>,
    pub started_at_ms: u64,
    pub ui_version: String,
}

/// Convert a `SystemTime` into Unix-epoch milliseconds. Returns `0` if
/// the input precedes the epoch (e.g. wildly miscomputed clock).
#[must_use]
pub fn system_time_to_ms(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| u64::try_from(d.as_millis()).ok())
        .unwrap_or(0)
}

/// Compute "ms since last seen" with saturating arithmetic so a future
/// `last` never underflows.
#[must_use]
pub fn last_seen_ms_ago(now: Instant, last: Instant) -> u64 {
    let elapsed: Duration = now.saturating_duration_since(last);
    u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use harness_core::{Identity, Signature};

    fn sample_hb(seq: u64) -> Heartbeat {
        Heartbeat {
            node_id: NodeId::from_bytes([0x11; 16]),
            seq,
            timestamp: 1_700_000_000_000,
            queue_depth: 0,
            cpu_busy_pct: 35,
            cpu_pinned_count: 0,
            ram_used_mb: 8_192,
            ram_total_mb: 16_384,
            gpu_used_mb: 0,
            gpu_total_mb: 0,
            capabilities_hash: [0; 16],
            in_flight: vec![],
            leader_belief: NodeId::from_bytes([0x22; 16]),
            brain_score: 200,
            on_battery: false,
            paused: false,
            version: harness_core::SemVer {
                major: 0,
                minor: 1,
                patch: 0,
            },
            sig: Signature::from_bytes([0u8; 64]),
        }
    }

    #[test]
    fn peer_dto_from_heartbeat_populates_expected_fields() {
        let hb = sample_hb(7);
        let dto = PeerDto::from_heartbeat(&hb, "lan-mesh", 1234);

        assert_eq!(dto.seq, 7);
        assert_eq!(dto.brain_score, 200);
        assert_eq!(dto.last_seen_ms_ago, 1234);
        assert_eq!(dto.mesh_name, "lan-mesh");
        assert!(dto.leader_belief.is_some());
        let res = dto.resources.expect("resources are derived");
        assert!(matches!(res.cpu_load_avg, Some(v) if (v - 0.35).abs() < 1e-3));
        assert_eq!(res.ram_total_bytes, Some(16_384u64 * 1024 * 1024));
    }

    #[test]
    fn local_dto_marks_is_local_true() {
        let id = Identity::generate();
        let dto = PeerDto::local(
            id.public_key().node_id(),
            id.public_key(),
            "lan-mesh",
            100,
            None,
            0,
            vec!["echo".to_owned()],
        );
        let local = LocalDto::new(dto);
        assert!(local.is_local);
        assert_eq!(local.peer.brain_score, 100);
    }

    #[test]
    fn empty_leader_belief_is_none() {
        let mut hb = sample_hb(1);
        hb.leader_belief = NodeId::from_bytes([0u8; 16]);
        let dto = PeerDto::from_heartbeat(&hb, "x", 0);
        assert_eq!(dto.leader_belief, None);
    }

    #[test]
    fn last_seen_ms_ago_clamps_underflow() {
        let now = Instant::now();
        let later = now + Duration::from_millis(50);
        // last after now → saturating_duration_since returns 0
        assert_eq!(last_seen_ms_ago(now, later), 0);
    }
}
