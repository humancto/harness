//! Protocol types, traits, and shared glue for Harness.
//!
//! Phase 1 is filling this in incrementally. The first piece is the
//! [`identity`] module — Ed25519 keys, [`NodeId`], sign/verify. Subsequent
//! items in Phase 1 will add the wire types ([`Heartbeat`], [`NodeManifest`],
//! etc.) in a `protocol` module (item 1.2 — pending).
//!
//! [`Heartbeat`]: https://github.com/humancto/harness/blob/main/HARNESS_PRD_v2.md
//! [`NodeManifest`]: https://github.com/humancto/harness/blob/main/HARNESS_PRD_v2.md

pub mod identity;

pub use identity::{
    verify, Identity, KeyError, NodeId, ParseNodeIdError, PublicKey, Signature, VerifyError,
};
