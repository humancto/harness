//! Protocol-layer errors.
//!
//! Bridges to the real [`crate::identity::VerifyError`] that 1.1 chose to
//! expose. Deliberately does not surface `ed25519_dalek::SignatureError`
//! directly — the wire-protocol error surface is what `harness-core` itself
//! defines, and 1.2 honors that boundary.

use thiserror::Error;

use crate::identity::VerifyError;

/// Errors at the wire-protocol layer (encode, decode, signature).
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ProtocolError {
    #[error("cbor encode: {0}")]
    CborEncode(#[from] ciborium::ser::Error<std::io::Error>),
    #[error("cbor decode: {0}")]
    CborDecode(#[from] ciborium::de::Error<std::io::Error>),
    #[error("signature: {0}")]
    Signature(#[from] VerifyError),
    /// 5.11: a value could not be JSON-encoded for hashing. Practically
    /// unreachable for a `serde_json::Value` (its numbers are always
    /// finite and its map keys always strings) — surfaced rather than
    /// unwrapped so a future non-Value caller cannot hash silence.
    #[error("json encode: {0}")]
    JsonEncode(String),
}
