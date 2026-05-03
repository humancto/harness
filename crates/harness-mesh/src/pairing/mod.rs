//! Pairing flow (item 1.7).
//!
//! The shape:
//!
//! 1. Host generates a [`PairingCode`] (8 digits, `NNNN-NNNN`) with a
//!    short TTL (default 10 minutes per PRD §9.2).
//! 2. Joiner dials the host (via mDNS discovery from item 1.3 +
//!    QUIC transport from item 1.4), authenticates with the pairing
//!    code in a [`PairingRequest`].
//! 3. Host validates the code (constant-time), checks expiry, and
//!    optionally requires operator approval before responding with
//!    a [`PairingResponse`] containing its own [`harness_core::NodeManifest`].
//! 4. Both sides add each other to their [`crate::TrustStore`]s.
//!
//! 1.7 ships the protocol types + the validator. The CLI and UI
//! plumbing (item 1.9 + 1.10) hand-wire this into `harness init` /
//! `harness join`. The QUIC transport handshake (1.4) already
//! authenticates that the joiner holds their claimed pubkey via
//! TLS pinning, so the pairing code is the only "shared secret"
//! the operator needs to type.

mod code;
mod protocol;

pub use code::{PairingCode, PairingCodeError};
pub use protocol::{PairingError, PairingRequest, PairingResponse, PairingStatus, PendingPairings};
