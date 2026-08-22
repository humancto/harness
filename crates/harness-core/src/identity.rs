//! Identity primitives — Ed25519 keypair, `NodeId`, sign/verify.
//!
//! Public surface this module owns:
//! - [`NodeId`] — stable 128-bit node identifier (`blake3(pubkey)[..16]`).
//! - [`PublicKey`] — typed wrapper around an Ed25519 verifying key with
//!   wire-format-stable `serde` impls.
//! - [`Signature`] — typed 64-byte Ed25519 signature.
//! - [`Identity`] — in-memory secret signing key. Non-`Clone`, non-`Serialize`,
//!   zeroized on drop. `Debug` prints only the public half.
//! - [`verify`] — free function over `(pubkey, msg, sig)`. Uses `verify_strict`
//!   to reject malleable / non-canonical signatures (RFC 8032).
//!
//! On-disk format and `~/.harness/` filesystem layout live in `harness-mesh`.

use core::fmt;
use std::str::FromStr;

use ed25519_dalek::Signer;
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

// -----------------------------------------------------------------------------
// NodeId
// -----------------------------------------------------------------------------

/// Stable 128-bit node identifier. Derived as `blake3(pubkey)[..16]`.
///
/// Wire format: 16 raw bytes (CBOR byte string via `serde(transparent)`).
/// Display format: lowercase hex (32 chars), no `0x` prefix.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NodeId([u8; Self::LEN]);

impl NodeId {
    /// Length of a `NodeId` in bytes.
    pub const LEN: usize = 16;

    /// Construct from raw bytes. No validation — `NodeId` is opaque.
    #[must_use]
    pub const fn from_bytes(b: [u8; Self::LEN]) -> Self {
        Self(b)
    }

    /// Borrow the raw bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; Self::LEN] {
        &self.0
    }

    /// Derive from a public key. The only canonical mint path.
    #[must_use]
    pub fn from_pubkey(pk: &PublicKey) -> Self {
        let h = blake3::hash(pk.as_bytes());
        let mut out = [0u8; Self::LEN];
        out.copy_from_slice(&h.as_bytes()[..Self::LEN]);
        Self(out)
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for b in &self.0 {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NodeId({self})")
    }
}

impl FromStr for NodeId {
    type Err = ParseNodeIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() != Self::LEN * 2 {
            return Err(ParseNodeIdError::BadLength(s.len()));
        }
        let mut out = [0u8; Self::LEN];
        hex::decode_to_slice(s, &mut out).map_err(|_| ParseNodeIdError::NotHex)?;
        Ok(Self(out))
    }
}

// -----------------------------------------------------------------------------
// PublicKey
// -----------------------------------------------------------------------------

/// Ed25519 verifying key, wire-format-stable.
///
/// Serializes as 32 raw bytes (CBOR byte string), regardless of how
/// `ed25519-dalek` chooses to internally lay out a `VerifyingKey`. Refuses to
/// deserialize anything that isn't exactly 32 bytes or isn't a valid Ed25519
/// curve point.
#[derive(Clone, Copy, Eq, PartialEq, Hash)]
pub struct PublicKey(ed25519_dalek::VerifyingKey);

impl PublicKey {
    /// Length of a public key in bytes.
    pub const LEN: usize = 32;

    /// Borrow the raw 32-byte representation.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; Self::LEN] {
        self.0.as_bytes()
    }

    /// Construct from raw bytes. Errors if the bytes are not a valid Ed25519
    /// point (this is a curve-membership check, not a length check — the
    /// length is enforced by the `[u8; 32]` parameter).
    pub fn from_bytes(b: &[u8; Self::LEN]) -> Result<Self, KeyError> {
        ed25519_dalek::VerifyingKey::from_bytes(b)
            .map(Self)
            .map_err(|_| KeyError::InvalidPoint)
    }

    /// Derive the `NodeId` for this key.
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        NodeId::from_pubkey(self)
    }

    /// Short hex fingerprint for UI display: first 8 bytes of `blake3(pubkey)`.
    #[must_use]
    pub fn fingerprint_hex(&self) -> String {
        let h = blake3::hash(self.as_bytes());
        hex::encode(&h.as_bytes()[..8])
    }
}

impl fmt::Debug for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PublicKey({})", hex::encode(self.as_bytes()))
    }
}

impl Serialize for PublicKey {
    fn serialize<S: serde::Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        ser.serialize_bytes(self.as_bytes())
    }
}

impl<'de> Deserialize<'de> for PublicKey {
    fn deserialize<D: serde::Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        struct V;
        impl serde::de::Visitor<'_> for V {
            type Value = PublicKey;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("32 raw bytes encoding an Ed25519 public key")
            }
            fn visit_bytes<E: serde::de::Error>(self, b: &[u8]) -> Result<PublicKey, E> {
                let arr: [u8; PublicKey::LEN] = b
                    .try_into()
                    .map_err(|_| E::invalid_length(b.len(), &"exactly 32"))?;
                PublicKey::from_bytes(&arr).map_err(|e| E::custom(e.to_string()))
            }
        }
        de.deserialize_bytes(V)
    }
}

// -----------------------------------------------------------------------------
// Signature
// -----------------------------------------------------------------------------

/// Ed25519 signature, wire-format-stable as 64 raw bytes.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct Signature([u8; Self::LEN]);

impl Signature {
    /// Length of a signature in bytes.
    pub const LEN: usize = 64;

    /// Borrow the raw bytes.
    #[must_use]
    pub fn to_bytes(&self) -> [u8; Self::LEN] {
        self.0
    }

    /// Construct from raw bytes. No validation — Ed25519 signatures are
    /// validated at verify time, not construction time.
    #[must_use]
    pub fn from_bytes(b: [u8; Self::LEN]) -> Self {
        Self(b)
    }
}

impl fmt::Debug for Signature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Signature({})", hex::encode(self.0))
    }
}

impl Serialize for Signature {
    fn serialize<S: serde::Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        ser.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for Signature {
    fn deserialize<D: serde::Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = Signature;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("64 raw bytes encoding an Ed25519 signature")
            }
            fn visit_bytes<E: serde::de::Error>(self, b: &[u8]) -> Result<Signature, E> {
                let arr: [u8; Signature::LEN] = b
                    .try_into()
                    .map_err(|_| E::invalid_length(b.len(), &"exactly 64"))?;
                Ok(Signature(arr))
            }
            // Human-readable formats (JSON) render `serialize_bytes` as
            // a number sequence — accept it so envelopes that embed a
            // Signature (e.g. a `Plan` inside `plan.execute` input, 4.3)
            // round-trip through the JSON API surface. CBOR still takes
            // the byte-string fast path above.
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<Signature, A::Error> {
                let mut arr = [0u8; Signature::LEN];
                for (i, slot) in arr.iter_mut().enumerate() {
                    *slot = seq
                        .next_element::<u8>()?
                        .ok_or_else(|| serde::de::Error::invalid_length(i, &"exactly 64"))?;
                }
                if seq.next_element::<u8>()?.is_some() {
                    return Err(serde::de::Error::invalid_length(
                        Signature::LEN + 1,
                        &"exactly 64",
                    ));
                }
                Ok(Signature(arr))
            }
        }
        de.deserialize_bytes(V)
    }
}

// -----------------------------------------------------------------------------
// Identity
// -----------------------------------------------------------------------------

/// In-memory identity. Holds the secret signing key. Zeroized on drop via
/// `ed25519-dalek`'s `zeroize` feature.
///
/// Deliberately neither `Clone` nor `Serialize` — clones invite leaks; serde
/// derive on a struct with a secret seed is exactly how secrets end up in
/// JSON logs. Use [`Identity::to_secret_bytes`] for explicit, audited
/// extraction (returns a `Zeroizing<[u8; 32]>` so the caller can't ignore
/// cleanup).
pub struct Identity {
    signing: ed25519_dalek::SigningKey,
    public: PublicKey,
    node_id: NodeId,
}

impl Identity {
    /// Generate a fresh identity from `OsRng`.
    #[must_use]
    pub fn generate() -> Self {
        let signing = ed25519_dalek::SigningKey::generate(&mut OsRng);
        Self::from_signing_key(signing)
    }

    /// Construct from a 32-byte secret seed. This is the on-disk format read
    /// by `harness-mesh::identity::load`.
    #[must_use]
    pub fn from_secret_bytes(seed: &[u8; 32]) -> Self {
        let signing = ed25519_dalek::SigningKey::from_bytes(seed);
        Self::from_signing_key(signing)
    }

    fn from_signing_key(signing: ed25519_dalek::SigningKey) -> Self {
        let public = PublicKey(signing.verifying_key());
        let node_id = NodeId::from_pubkey(&public);
        Self {
            signing,
            public,
            node_id,
        }
    }

    /// Extract the secret seed for on-disk storage. Wrapped in `Zeroizing`
    /// so it's wiped on drop — caller is expected to write it directly to
    /// the key file and let the wrapper drop.
    #[must_use]
    pub fn to_secret_bytes(&self) -> Zeroizing<[u8; 32]> {
        Zeroizing::new(self.signing.to_bytes())
    }

    /// This identity's public key.
    #[must_use]
    pub fn public_key(&self) -> &PublicKey {
        &self.public
    }

    /// This identity's `NodeId`.
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Sign `msg`. Ed25519 signing is deterministic and infallible by spec.
    #[must_use]
    pub fn sign(&self, msg: &[u8]) -> Signature {
        Signature(self.signing.sign(msg).to_bytes())
    }
}

impl fmt::Debug for Identity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // NEVER touches `signing`. The `Debug` impl is the most common
        // accidental-leak vector; we keep it tight.
        f.debug_struct("Identity")
            .field("node_id", &self.node_id)
            .field("public_key", &self.public)
            .finish_non_exhaustive()
    }
}

// -----------------------------------------------------------------------------
// Verify (free function)
// -----------------------------------------------------------------------------

/// Verify that `sig` is `pubkey`'s signature over `msg`.
///
/// Uses `ed25519_dalek::VerifyingKey::verify_strict`, which rejects malleable
/// and non-canonical signatures per RFC 8032 — required for the wire-security
/// guarantees in PRD §10.3. Constant-time inside `dalek`.
///
/// The error is opaque (`VerifyError::BadSignature`) — we deliberately don't
/// surface the underlying `dalek` error message so timing of which check
/// failed isn't observable from the error path.
pub fn verify(pubkey: &PublicKey, msg: &[u8], sig: &Signature) -> Result<(), VerifyError> {
    let dalek_sig = ed25519_dalek::Signature::from_bytes(&sig.0);
    pubkey
        .0
        .verify_strict(msg, &dalek_sig)
        .map_err(|_| VerifyError::BadSignature)
}

// -----------------------------------------------------------------------------
// Errors
// -----------------------------------------------------------------------------

/// Errors constructing a `PublicKey`.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum KeyError {
    #[error("public key is not a valid Ed25519 point")]
    InvalidPoint,
}

/// Errors verifying a signature.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum VerifyError {
    #[error("signature did not verify against the provided public key")]
    BadSignature,
}

/// Errors parsing a `NodeId` from its hex string form.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ParseNodeIdError {
    #[error("expected 32 hex characters, got {0}")]
    BadLength(usize),
    #[error("non-hex character in node id")]
    NotHex,
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    /// RFC 8032 test vector #1: known seed → known public key.
    /// Source: <https://datatracker.ietf.org/doc/html/rfc8032#section-7.1>.
    const RFC_SEED: [u8; 32] = [
        0x9d, 0x61, 0xb1, 0x9d, 0xef, 0xfd, 0x5a, 0x60, 0xba, 0x84, 0x4a, 0xf4, 0x92, 0xec, 0x2c,
        0xc4, 0x44, 0x49, 0xc5, 0x69, 0x7b, 0x32, 0x69, 0x19, 0x70, 0x3b, 0xac, 0x03, 0x1c, 0xae,
        0x7f, 0x60,
    ];
    const RFC_PUBKEY: [u8; 32] = [
        0xd7, 0x5a, 0x98, 0x01, 0x82, 0xb1, 0x0a, 0xb7, 0xd5, 0x4b, 0xfe, 0xd3, 0xc9, 0x64, 0x07,
        0x3a, 0x0e, 0xe1, 0x72, 0xf3, 0xda, 0xa6, 0x23, 0x25, 0xaf, 0x02, 0x1a, 0x68, 0xf7, 0x07,
        0x51, 0x1a,
    ];

    #[test]
    fn node_id_is_first_16_bytes_of_blake3_pubkey() {
        let pk = PublicKey::from_bytes(&RFC_PUBKEY).expect("RFC pubkey is valid");
        let expected: [u8; 16] = blake3::hash(&RFC_PUBKEY).as_bytes()[..16]
            .try_into()
            .expect("blake3 produces 32 bytes");
        assert_eq!(pk.node_id(), NodeId::from_bytes(expected));
    }

    #[test]
    fn node_id_display_is_lowercase_hex_32_chars() {
        let id = NodeId::from_bytes([0xab; 16]);
        let s = id.to_string();
        assert_eq!(s.len(), 32);
        assert_eq!(s, "abababababababababababababababab");
    }

    #[test]
    fn node_id_display_round_trip() {
        for byte in 0u8..=255 {
            let id = NodeId::from_bytes([byte; 16]);
            let s = id.to_string();
            let parsed: NodeId = s.parse().expect("display output should parse");
            assert_eq!(parsed, id);
        }
    }

    #[test]
    fn parse_node_id_rejects_wrong_length() {
        assert!(matches!(
            "abc".parse::<NodeId>(),
            Err(ParseNodeIdError::BadLength(3))
        ));
    }

    #[test]
    fn parse_node_id_rejects_non_hex() {
        assert!(matches!(
            "zz".repeat(16).parse::<NodeId>(),
            Err(ParseNodeIdError::NotHex)
        ));
    }

    #[test]
    fn pubkey_serde_cbor_round_trip() {
        let pk = PublicKey::from_bytes(&RFC_PUBKEY).expect("RFC pubkey is valid");
        let mut buf = Vec::new();
        ciborium::ser::into_writer(&pk, &mut buf).expect("serialize");
        let back: PublicKey = ciborium::de::from_reader(buf.as_slice()).expect("deserialize");
        assert_eq!(pk, back);
    }

    #[test]
    fn pubkey_serde_rejects_wrong_length() {
        // CBOR for a 31-byte byte string (0x58 0x1f <31 bytes>).
        let mut bad = vec![0x58, 0x1f];
        bad.extend(std::iter::repeat(0u8).take(31));
        let res: Result<PublicKey, _> = ciborium::de::from_reader(bad.as_slice());
        assert!(res.is_err(), "31-byte pubkey must not deserialize");
    }

    #[test]
    fn signature_serde_cbor_round_trip() {
        let sig = Signature::from_bytes([0x42; 64]);
        let mut buf = Vec::new();
        ciborium::ser::into_writer(&sig, &mut buf).expect("serialize");
        let back: Signature = ciborium::de::from_reader(buf.as_slice()).expect("deserialize");
        assert_eq!(sig, back);
    }

    #[test]
    fn identity_sign_then_verify_succeeds() {
        let id = Identity::generate();
        let sig = id.sign(b"hello");
        verify(id.public_key(), b"hello", &sig).expect("self-signature must verify");
    }

    #[test]
    fn verify_rejects_tampered_message() {
        let id = Identity::generate();
        let sig = id.sign(b"hello");
        assert!(verify(id.public_key(), b"hellp", &sig).is_err());
    }

    #[test]
    fn verify_rejects_wrong_pubkey() {
        let a = Identity::generate();
        let b = Identity::generate();
        let sig = a.sign(b"hello");
        assert!(verify(b.public_key(), b"hello", &sig).is_err());
    }

    #[test]
    fn identity_debug_does_not_leak_secret() {
        let id = Identity::from_secret_bytes(&RFC_SEED);
        let dbg = format!("{id:?}");
        // The secret seed must not appear (in either direction's hex).
        let seed_hex = hex::encode(RFC_SEED);
        assert!(
            !dbg.contains(&seed_hex),
            "Debug leaked the secret seed: {dbg}"
        );
        // And every 4-byte window of the seed must also not appear (paranoid).
        for window in RFC_SEED.windows(4) {
            let chunk = hex::encode(window);
            assert!(
                !dbg.contains(&chunk),
                "Debug leaked a {}-byte window of the secret: {dbg}",
                window.len()
            );
        }
    }

    #[test]
    fn identity_from_secret_bytes_matches_rfc8032_vector() {
        let id = Identity::from_secret_bytes(&RFC_SEED);
        assert_eq!(id.public_key().as_bytes(), &RFC_PUBKEY);
    }

    #[test]
    fn node_id_is_deterministic_from_pubkey() {
        let id = Identity::from_secret_bytes(&RFC_SEED);
        assert_eq!(id.node_id(), id.public_key().node_id());
        // And derived a third time matches.
        let again = NodeId::from_pubkey(id.public_key());
        assert_eq!(id.node_id(), again);
    }

    #[test]
    fn identity_to_secret_bytes_round_trips() {
        let id = Identity::generate();
        let seed = id.to_secret_bytes();
        let same = Identity::from_secret_bytes(&seed);
        assert_eq!(id.public_key(), same.public_key());
        assert_eq!(id.node_id(), same.node_id());
    }
}
