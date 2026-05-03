//! `Signable` — the canonical-encoding-with-sig-zeroed contract every
//! signed wire type implements.
//!
//! The rule: a signature commits to the bytes of `self` encoded in CBOR
//! **with the `sig` field replaced by 64 zero bytes**. This avoids defining
//! a parallel "unsigned" struct for every signed type, and means future
//! fields are automatically covered without touching this trait.
//!
//! Each implementer is a one-liner — they just hand back `&mut self.sig`;
//! the trait does the clone-zero-encode work generically.

use serde::Serialize;

use crate::error::ProtocolError;
use crate::identity::{verify, Identity, PublicKey, Signature};

/// Wire types that carry an inline `Signature` field.
///
/// Implementers provide [`sig_field`] / [`sig_field_mut`]; the trait
/// supplies [`canonical_bytes`], [`sign`], and [`verify_signature`] with
/// default implementations that should not be overridden.
pub trait Signable: Sized + Serialize + Clone {
    /// Mutable handle to the `Signature` slot in this message.
    fn sig_field_mut(&mut self) -> &mut Signature;

    /// Read-only handle to the `Signature` slot.
    fn sig_field(&self) -> &Signature;

    /// CBOR encoding of `self` with the `sig` field replaced by 64 zero
    /// bytes. The signature commits to exactly this byte sequence.
    fn canonical_bytes(&self) -> Result<Vec<u8>, ProtocolError> {
        let mut copy = self.clone();
        *copy.sig_field_mut() = Signature::from_bytes([0u8; Signature::LEN]);
        let mut buf = Vec::with_capacity(320);
        ciborium::ser::into_writer(&copy, &mut buf)?;
        Ok(buf)
    }

    /// Sign `self` in place using `identity`. Mutates the `sig` field.
    fn sign(&mut self, identity: &Identity) -> Result<(), ProtocolError> {
        let bytes = self.canonical_bytes()?;
        *self.sig_field_mut() = identity.sign(&bytes);
        Ok(())
    }

    /// Verify `self.sig` against `pubkey`.
    ///
    /// Routes through [`crate::identity::verify`], which uses
    /// `verify_strict` (RFC 8032 canonical-form check, rejecting malleable
    /// signatures).
    fn verify_signature(&self, pubkey: &PublicKey) -> Result<(), ProtocolError> {
        let bytes = self.canonical_bytes()?;
        verify(pubkey, &bytes, self.sig_field()).map_err(Into::into)
    }
}
