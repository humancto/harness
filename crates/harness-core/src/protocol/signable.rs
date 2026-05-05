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

use serde::{Deserialize, Serialize};

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

/// Discipline newtype around a [`Signable`] value that has not yet been
/// signed by an [`Identity`]. The `#[must_use]` annotation forces callers
/// to either sign or explicitly project out — accidentally treating an
/// unsigned plan as authentic becomes a compile-time warning.
///
/// The brain.plan capability (Phase 3.8) returns `Unsigned<Plan>` so the
/// dispatcher (Phase 3.10+) is statically required to call
/// [`Unsigned::sign`] before fan-out.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[must_use = "an Unsigned<T> must be signed before it is treated as authentic"]
pub struct Unsigned<T>(pub T);

impl<T: Signable> Unsigned<T> {
    /// Sign the inner value with `identity`, consuming the wrapper and
    /// returning the now-signed value. The next caller cannot accidentally
    /// re-sign or skip signing — the wrapper is gone.
    pub fn sign(mut self, identity: &Identity) -> Result<T, ProtocolError> {
        self.0.sign(identity)?;
        Ok(self.0)
    }
}

impl<T> Unsigned<T> {
    /// Project out without signing. For serialization-only paths: the
    /// brain.plan capability serializes the inner `Plan` to JSON
    /// (with a zeroed sig) and a downstream consumer decodes back into
    /// `Unsigned<Plan>` before signing.
    #[must_use]
    pub fn into_inner_unsigned(self) -> T {
        self.0
    }

    #[must_use]
    pub fn as_inner(&self) -> &T {
        &self.0
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::protocol::plan::{Plan, PlanNode};
    use crate::protocol::support::{CpuClass, DiskIoClass, NetworkClass};
    use crate::protocol::ResourceHints;
    use crate::{NodeId, PlanId, TaskId};
    use std::collections::HashMap;

    fn sample_unsigned_plan() -> Unsigned<Plan> {
        let task_id = TaskId::new_v7();
        let mut tasks = HashMap::new();
        tasks.insert(
            task_id,
            PlanNode {
                id: task_id,
                capability: "echo".into(),
                input: serde_json::json!({"msg": "hi"}),
                resource_hints: ResourceHints {
                    cpu_class: CpuClass::Light,
                    memory_mb: None,
                    gpu_required: false,
                    gpu_memory_mb: None,
                    network_class: NetworkClass::None,
                    disk_io_class: DiskIoClass::None,
                    estimated_duration_ms: None,
                },
                timeout_ms: None,
            },
        );
        let plan = Plan {
            id: PlanId::new_v7(),
            name: "minimal".into(),
            tasks,
            edges: vec![],
            budget: None,
            checkpoint: None,
            issued_by: NodeId::from_bytes([0x11; 16]),
            sig: Signature::from_bytes([0u8; Signature::LEN]),
        };
        Unsigned(plan)
    }

    #[test]
    fn unsigned_round_trip() {
        let u = sample_unsigned_plan();
        // Serialize, decode, compare. The inner sig stays zeroed.
        let mut buf = Vec::new();
        ciborium::ser::into_writer(&u, &mut buf).expect("ser");
        let back: Unsigned<Plan> = ciborium::de::from_reader(buf.as_slice()).expect("de");
        assert_eq!(u.as_inner().sig, back.as_inner().sig);
        assert_eq!(
            u.as_inner().sig,
            Signature::from_bytes([0u8; Signature::LEN])
        );
    }

    #[test]
    fn unsigned_sign_consumes_wrapper_returns_signed_plan() {
        let identity = Identity::generate();
        let mut u = sample_unsigned_plan();
        // Stamp issuer to match the signing identity so verify_signature
        // is meaningful.
        u.0.issued_by = identity.public_key().node_id();
        let signed = u.sign(&identity).expect("sign");
        // The newtype is gone — `signed` is `Plan`, not `Unsigned<Plan>`.
        assert_ne!(signed.sig, Signature::from_bytes([0u8; Signature::LEN]));
        signed
            .verify_signature(identity.public_key())
            .expect("self-signature must verify");
    }

    #[test]
    fn unsigned_into_inner_unsigned_does_not_sign() {
        let u = sample_unsigned_plan();
        let inner = u.into_inner_unsigned();
        assert_eq!(inner.sig, Signature::from_bytes([0u8; Signature::LEN]));
    }
}
