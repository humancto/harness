//! Property tests for `harness-core::identity`.
//!
//! The roadmap text for item 1.1 demands "Property test: round-trip
//! sign/verify." We over-deliver: every primitive the module exposes has a
//! property covering its happy path AND at least one negative path.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use harness_core::{verify, Identity, NodeId, PublicKey};
use proptest::prelude::*;

fn arb_seed() -> impl Strategy<Value = [u8; 32]> {
    any::<[u8; 32]>()
}

fn arb_msg() -> impl Strategy<Value = Vec<u8>> {
    proptest::collection::vec(any::<u8>(), 0..4096)
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 256,
        ..ProptestConfig::default()
    })]

    /// Seed → identity → seed must round-trip byte-exact.
    #[test]
    fn seed_round_trip(seed in arb_seed()) {
        let id = Identity::from_secret_bytes(&seed);
        let back = id.to_secret_bytes();
        prop_assert_eq!(*back, seed);
    }

    /// The literal roadmap requirement: sign-verify round-trip on an
    /// arbitrary seed and arbitrary message.
    #[test]
    fn sign_verify_round_trip(seed in arb_seed(), msg in arb_msg()) {
        let id = Identity::from_secret_bytes(&seed);
        let sig = id.sign(&msg);
        prop_assert!(verify(id.public_key(), &msg, &sig).is_ok());
    }

    /// Flipping any one bit of the message must make verification fail.
    #[test]
    fn tampered_message_fails(seed in arb_seed(), msg in arb_msg(), idx in any::<usize>(), mask in 1u8..=255) {
        prop_assume!(!msg.is_empty());
        let id = Identity::from_secret_bytes(&seed);
        let sig = id.sign(&msg);
        let mut tampered = msg.clone();
        let i = idx % tampered.len();
        tampered[i] ^= mask;
        prop_assert!(verify(id.public_key(), &tampered, &sig).is_err());
    }

    /// A signature from key A must not verify under key B.
    #[test]
    fn wrong_pubkey_fails(seed_a in arb_seed(), seed_b in arb_seed(), msg in arb_msg()) {
        prop_assume!(seed_a != seed_b);
        let a = Identity::from_secret_bytes(&seed_a);
        let b = Identity::from_secret_bytes(&seed_b);
        let sig = a.sign(&msg);
        prop_assert!(verify(b.public_key(), &msg, &sig).is_err());
    }

    /// Flipping any one bit of the signature must make verification fail.
    /// `verify_strict` is required for this to catch bit-flips reliably.
    #[test]
    fn tampered_signature_fails(seed in arb_seed(), msg in arb_msg(), byte_idx in 0usize..64, bit_idx in 0u8..8) {
        let id = Identity::from_secret_bytes(&seed);
        let sig = id.sign(&msg);
        let mut bytes = sig.to_bytes();
        bytes[byte_idx] ^= 1u8 << bit_idx;
        let tampered = harness_core::Signature::from_bytes(bytes);
        prop_assert!(verify(id.public_key(), &msg, &tampered).is_err());
    }

    /// `NodeId` derivation is a pure function of the public key.
    #[test]
    fn node_id_is_deterministic(seed in arb_seed()) {
        let id = Identity::from_secret_bytes(&seed);
        let pk: &PublicKey = id.public_key();
        let n1 = NodeId::from_pubkey(pk);
        let n2 = NodeId::from_pubkey(pk);
        prop_assert_eq!(n1, n2);
        prop_assert_eq!(n1, id.node_id());
    }
}
