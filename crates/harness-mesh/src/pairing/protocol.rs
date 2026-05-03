//! Pairing-handshake protocol types + the host-side pending pairings
//! table.
//!
//! Pairing happens *outside* the normal `Heartbeat` / `NodeManifest`
//! gossip — when a joiner first dials, neither side trusts the other
//! yet, so the request is a one-shot frame on the same QUIC connection
//! the joiner already used to dial. The TLS handshake (1.4) proves the
//! joiner controls the secret half of their advertised pubkey; the
//! pairing code proves the operator authorized this specific pairing.
//!
//! Both sides verify the request/response signatures via the same
//! `Signable` trait the rest of the wire protocol uses.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use harness_core::{NodeId, PublicKey, Signable, Signature};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use super::code::PairingCode;

/// Errors from the pairing flow.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PairingError {
    #[error("pairing code did not match")]
    CodeMismatch,
    #[error("pairing code expired")]
    Expired,
    #[error("pairing code already consumed")]
    AlreadyConsumed,
    #[error("operator rejected the pairing")]
    Rejected,
    #[error("pairing claim is inconsistent: node_id != blake3(pubkey)[..16]")]
    Inconsistent,
}

/// What the joiner sends. The 8-character pairing code (sent as ASCII
/// bytes) plus their identity claim. Signed by the joiner's identity
/// so the host can prove the joiner holds the secret half of their
/// claimed `pubkey`.
///
/// The `code_bytes` field carries the 8 ASCII digits of the pairing
/// code. Sent in cleartext over an mTLS-protected channel — no
/// additional encryption needed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PairingRequest {
    pub joiner_node_id: NodeId,
    pub joiner_pubkey: PublicKey,
    pub joiner_hostname: String,
    /// 8 ASCII digits of the pairing code.
    pub code_bytes: [u8; 8],
    /// Unix milliseconds of issuance. Host validates this is within a
    /// reasonable clock-skew window.
    pub issued_at_ms: u64,
    pub sig: Signature,
}

impl harness_core::Signable for PairingRequest {
    fn sig_field_mut(&mut self) -> &mut Signature {
        &mut self.sig
    }
    fn sig_field(&self) -> &Signature {
        &self.sig
    }
}

/// Host's response. On success: the host's manifest (so the joiner can
/// add it to their trust store). On failure: the [`PairingStatus`]
/// reason.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PairingResponse {
    pub status: PairingStatus,
    pub host_node_id: NodeId,
    pub host_pubkey: PublicKey,
    pub host_hostname: String,
    pub sig: Signature,
}

impl harness_core::Signable for PairingResponse {
    fn sig_field_mut(&mut self) -> &mut Signature {
        &mut self.sig
    }
    fn sig_field(&self) -> &Signature {
        &self.sig
    }
}

/// Outcome reported by the host.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum PairingStatus {
    /// Code matched, operator approved (or no manual approval gate).
    Approved,
    /// Code didn't match.
    BadCode,
    /// Code expired (TTL elapsed).
    Expired,
    /// Operator manually rejected this joiner.
    Rejected,
}

/// Per-host registry of outstanding pairing codes. The host generates a
/// code, stashes it here with a TTL, and validates incoming
/// [`PairingRequest`]s against it.
///
/// Codes are single-use: the first matching request consumes the entry.
/// Cheap to clone (Arc-shared internally).
#[derive(Clone, Default)]
pub struct PendingPairings {
    inner: Arc<Mutex<HashMap<[u8; 8], Pending>>>,
}

#[derive(Debug, Clone, Copy)]
struct Pending {
    expires_at: Instant,
}

impl std::fmt::Debug for PendingPairings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingPairings")
            .field("entries", &self.inner.lock().len())
            .finish_non_exhaustive()
    }
}

impl PendingPairings {
    pub fn new() -> Self {
        Self::default()
    }

    /// Stash a fresh code with a TTL. Returns the code so the host can
    /// print it for the operator.
    pub fn issue(&self, ttl: Duration) -> PairingCode {
        let code = PairingCode::generate();
        let expires_at = Instant::now() + ttl;
        self.inner
            .lock()
            .insert(*code.as_bytes(), Pending { expires_at });
        code
    }

    /// Validate an incoming request's code against any outstanding
    /// pending entry. On success, the entry is consumed (single-use).
    /// Constant-time comparison is performed via [`PairingCode::ct_eq`]
    /// against every outstanding entry, but the comparison set is
    /// expected to be small (one or two codes at a time in practice).
    pub fn validate(&self, code_bytes: &[u8; 8]) -> Result<(), PairingError> {
        let mut g = self.inner.lock();
        // Find the matching entry via constant-time compare.
        let now = Instant::now();
        let mut matched_key: Option<[u8; 8]> = None;
        let mut expired_key: Option<[u8; 8]> = None;
        for (key, pending) in g.iter() {
            if PairingCode::from_raw(*key).ct_eq(&PairingCode::from_raw(*code_bytes)) {
                if now > pending.expires_at {
                    expired_key = Some(*key);
                } else {
                    matched_key = Some(*key);
                }
            }
        }
        if let Some(key) = matched_key {
            g.remove(&key);
            Ok(())
        } else if let Some(key) = expired_key {
            g.remove(&key);
            Err(PairingError::Expired)
        } else {
            Err(PairingError::CodeMismatch)
        }
    }

    /// Number of currently-outstanding codes (including expired-but-
    /// not-yet-pruned).
    pub fn len(&self) -> usize {
        self.inner.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.lock().is_empty()
    }

    /// Garbage-collect expired entries. Returns how many were removed.
    pub fn gc_expired(&self) -> usize {
        let mut g = self.inner.lock();
        let now = Instant::now();
        let before = g.len();
        g.retain(|_, p| p.expires_at >= now);
        before - g.len()
    }
}

/// Convenience: build a `PairingRequest` from the joiner's identity +
/// the typed pairing code, then sign.
#[allow(dead_code)]
pub(crate) fn build_request(
    identity: &harness_core::Identity,
    hostname: &str,
    code: PairingCode,
) -> PairingRequest {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(0))
        .unwrap_or(0);
    let mut req = PairingRequest {
        joiner_node_id: identity.node_id(),
        joiner_pubkey: *identity.public_key(),
        joiner_hostname: hostname.to_string(),
        code_bytes: *code.as_bytes(),
        issued_at_ms: now_ms,
        sig: Signature::from_bytes([0u8; 64]),
    };
    let _ = req.sign(identity);
    req
}

/// Convenience: build a `PairingResponse` from the host's identity +
/// the result, then sign.
#[allow(dead_code)]
pub(crate) fn build_response(
    identity: &harness_core::Identity,
    hostname: &str,
    status: PairingStatus,
) -> PairingResponse {
    let mut resp = PairingResponse {
        status,
        host_node_id: identity.node_id(),
        host_pubkey: *identity.public_key(),
        host_hostname: hostname.to_string(),
        sig: Signature::from_bytes([0u8; 64]),
    };
    let _ = resp.sign(identity);
    resp
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use harness_core::{Identity, Signable};

    #[test]
    fn issue_and_validate_round_trip() {
        let p = PendingPairings::new();
        let code = p.issue(Duration::from_secs(60));
        assert_eq!(p.len(), 1);
        p.validate(code.as_bytes()).expect("validate");
        assert!(p.is_empty(), "code should be consumed on validate");
    }

    #[test]
    fn validate_unknown_returns_code_mismatch() {
        let p = PendingPairings::new();
        let code = PairingCode::generate();
        let err = p.validate(code.as_bytes()).expect_err("not pending");
        assert!(matches!(err, PairingError::CodeMismatch));
    }

    #[test]
    fn expired_codes_return_expired() {
        let p = PendingPairings::new();
        let code = p.issue(Duration::from_millis(1));
        std::thread::sleep(Duration::from_millis(10));
        let err = p.validate(code.as_bytes()).expect_err("expired");
        assert!(matches!(err, PairingError::Expired));
    }

    #[test]
    fn validate_consumes_entry_single_use() {
        let p = PendingPairings::new();
        let code = p.issue(Duration::from_secs(60));
        p.validate(code.as_bytes()).expect("first");
        let err = p.validate(code.as_bytes()).expect_err("second use");
        assert!(matches!(err, PairingError::CodeMismatch));
    }

    #[test]
    fn gc_expired_drops_old_entries() {
        let p = PendingPairings::new();
        let _short = p.issue(Duration::from_millis(1));
        let _long = p.issue(Duration::from_secs(60));
        std::thread::sleep(Duration::from_millis(10));
        let gced = p.gc_expired();
        assert_eq!(gced, 1);
        assert_eq!(p.len(), 1);
    }

    #[test]
    fn pairing_request_self_verifies() {
        let id = Identity::generate();
        let code = PairingCode::generate();
        let req = build_request(&id, "joiner-host", code);
        req.verify_signature(id.public_key())
            .expect("self-signed request must verify");
        assert_eq!(req.joiner_node_id, id.node_id());
        assert_eq!(req.code_bytes, *code.as_bytes());
    }

    #[test]
    fn pairing_response_self_verifies() {
        let id = Identity::generate();
        let resp = build_response(&id, "host", PairingStatus::Approved);
        resp.verify_signature(id.public_key())
            .expect("self-signed response must verify");
        assert_eq!(resp.host_node_id, id.node_id());
        assert_eq!(resp.status, PairingStatus::Approved);
    }

    #[test]
    fn full_pairing_round_trip() {
        let host = Identity::generate();
        let joiner = Identity::generate();
        let pending = PendingPairings::new();

        // Host issues a code.
        let code = pending.issue(Duration::from_secs(60));

        // Joiner builds a request.
        let req = build_request(&joiner, "joiner-mac", code);
        // Host verifies the request signature against the claimed pubkey.
        req.verify_signature(&req.joiner_pubkey)
            .expect("joiner sig verifies");
        // Host validates the code.
        pending.validate(&req.code_bytes).expect("code validates");
        // Host responds with Approved.
        let resp = build_response(&host, "host-mac", PairingStatus::Approved);

        // Joiner verifies the host's response.
        resp.verify_signature(&resp.host_pubkey)
            .expect("host sig verifies");
        assert_eq!(resp.status, PairingStatus::Approved);
        assert_eq!(resp.host_pubkey, *host.public_key());
    }

    #[test]
    fn cross_pubkey_request_signature_fails() {
        let real = Identity::generate();
        let other = Identity::generate();
        let code = PairingCode::generate();
        let mut req = build_request(&real, "real", code);
        // Tamper: change the claimed pubkey but not the sig.
        req.joiner_pubkey = *other.public_key();
        // The sig was computed under `real` over canonical bytes that
        // included the original `real` pubkey field. Verify with the
        // tampered pubkey field included in canonical bytes — the sig
        // won't match either real (the bytes changed) or other (other
        // didn't sign anything).
        req.verify_signature(other.public_key())
            .expect_err("tampered request must fail");
    }
}
