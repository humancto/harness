//! QUIC transport for harness mesh nodes (item 1.4).
//!
//! Public surface:
//!
//! - [`Transport`] — one per node, binds a UDP socket and serves both
//!   inbound and outbound connections.
//! - [`TrustStore`] — in-memory snapshot of trusted pubkeys (1.8 has the
//!   persistent version; this is for transport-layer dedupe).
//! - [`Connection`] — one peer-to-peer link with `send` / `recv` /
//!   `recv_sequenced` (cancel-safe recv via the [`envelope::RecvFramer`]
//!   state machine).
//! - [`IncomingConnection`] — pending inbound; caller decides whether to
//!   accept based on the peer pubkey.
//! - [`Sequenced`] — opt-in trait for replay-protected message types.
//!   `Heartbeat` implements it; `NodeManifest` does not.
//! - [`channels`] — `&'static str` constants for the PRD §13.6 logical
//!   channel names.
//! - [`TransportError`] — single enum for every fallible path.

mod cert;
mod channel;
mod connection;
mod envelope;
mod error;
mod quic;
mod verifier;

pub use channel::ChannelStream;
pub use connection::{Connection, IncomingConnection};
pub use envelope::{channels, Sequenced, MAX_FRAME_BYTES};
pub use error::TransportError;
pub use quic::{Transport, TrustStore};
