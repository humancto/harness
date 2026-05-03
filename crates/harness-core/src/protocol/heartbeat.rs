//! Heartbeat — broadcast every ~2s by every node, ~280 bytes typical.
//! PRD §13.1.

use serde::{Deserialize, Serialize};

use crate::identity::{NodeId, Signature};
use crate::ids::{SemVer, TaskId};
use crate::protocol::signable::Signable;

/// Periodic liveness packet broadcast on the mesh.
///
/// Carries enough state for the brain election (PRD §12) and for
/// scheduler decisions (PRD §14.3) without dragging the full manifest on
/// every tick. The manifest is gossiped only on change; the heartbeat
/// carries its 16-byte hash so peers know when to re-fetch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Heartbeat {
    pub node_id: NodeId,
    pub seq: u64,
    /// Unix milliseconds.
    pub timestamp: u64,
    pub queue_depth: u16,
    pub cpu_busy_pct: u8,
    pub cpu_pinned_count: u8,
    pub ram_used_mb: u32,
    pub ram_total_mb: u32,
    pub gpu_used_mb: u32,
    pub gpu_total_mb: u32,
    /// First 16 bytes of `blake3` over the canonical CBOR encoding of the
    /// node's current manifest. Peers re-fetch on mismatch.
    pub capabilities_hash: [u8; 16],
    /// Tasks currently running on this node. Recommended cap of 16
    /// entries; enforcement lives in the broadcast loop (item 1.5).
    pub in_flight: Vec<TaskId>,
    pub leader_belief: NodeId,
    pub brain_score: i32,
    pub on_battery: bool,
    /// Backpressure signal — set by the worker when its queue is full.
    pub paused: bool,
    /// Wire-protocol version of the heartbeat itself (distinct from
    /// `NodeManifest::version`, which advertises the daemon binary).
    pub version: SemVer,
    pub sig: Signature,
}

impl Signable for Heartbeat {
    fn sig_field_mut(&mut self) -> &mut Signature {
        &mut self.sig
    }
    fn sig_field(&self) -> &Signature {
        &self.sig
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    fn sample_heartbeat(sig: Signature) -> Heartbeat {
        Heartbeat {
            node_id: NodeId::from_bytes([0x11; 16]),
            seq: 42,
            timestamp: 1_700_000_000_000,
            queue_depth: 3,
            cpu_busy_pct: 27,
            cpu_pinned_count: 1,
            ram_used_mb: 8_192,
            ram_total_mb: 32_768,
            gpu_used_mb: 0,
            gpu_total_mb: 0,
            capabilities_hash: [0xAB; 16],
            in_flight: vec![
                TaskId(uuid::Uuid::from_u128(0)),
                TaskId(uuid::Uuid::from_u128(1)),
                TaskId(uuid::Uuid::from_u128(2)),
                TaskId(uuid::Uuid::from_u128(3)),
            ],
            leader_belief: NodeId::from_bytes([0x22; 16]),
            brain_score: 350,
            on_battery: false,
            paused: false,
            version: SemVer::new(0, 1, 0),
            sig,
        }
    }

    #[test]
    fn heartbeat_round_trip() {
        let hb = sample_heartbeat(Signature::from_bytes([0x77; 64]));
        let mut buf = Vec::new();
        ciborium::ser::into_writer(&hb, &mut buf).expect("serialize");
        let back: Heartbeat = ciborium::de::from_reader(buf.as_slice()).expect("deserialize");
        assert_eq!(hb, back);
    }

    #[test]
    fn heartbeat_sign_then_verify() {
        let id = crate::Identity::from_secret_bytes(&[0x42u8; 32]);
        let mut hb = sample_heartbeat(Signature::from_bytes([0u8; 64]));
        hb.sign(&id).expect("sign");
        hb.verify_signature(id.public_key())
            .expect("self-signature must verify");
    }

    /// The trait contract: `canonical_bytes` must zero the sig field
    /// before encoding, so two hearts with different sigs but otherwise
    /// identical produce the same bytes.
    #[test]
    fn canonical_bytes_does_not_depend_on_sig() {
        let mut hb = sample_heartbeat(Signature::from_bytes([0u8; 64]));
        let bytes_a = hb.canonical_bytes().expect("canonical_bytes");
        hb.sig = Signature::from_bytes([0xFF; 64]);
        let bytes_b = hb.canonical_bytes().expect("canonical_bytes");
        assert_eq!(bytes_a, bytes_b, "canonical_bytes must zero the sig field");
    }

    #[test]
    fn heartbeat_tampered_payload_fails_verify() {
        let id = crate::Identity::from_secret_bytes(&[0x55u8; 32]);
        let mut hb = sample_heartbeat(Signature::from_bytes([0u8; 64]));
        hb.sign(&id).expect("sign");
        hb.seq = hb.seq.wrapping_add(1);
        assert!(hb.verify_signature(id.public_key()).is_err());
    }

    #[test]
    fn heartbeat_wrong_pubkey_fails_verify() {
        let signer = crate::Identity::from_secret_bytes(&[0x66u8; 32]);
        let other = crate::Identity::from_secret_bytes(&[0x99u8; 32]);
        let mut hb = sample_heartbeat(Signature::from_bytes([0u8; 64]));
        hb.sign(&signer).expect("sign");
        assert!(hb.verify_signature(other.public_key()).is_err());
    }
}
