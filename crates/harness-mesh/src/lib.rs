//! Discovery, transport, gossip, and weighted brain election.
//!
//! Phase 1 is filling this in incrementally:
//!
//! - [`identity`] (1.1, shipped) — `~/.harness/identity.key` (mode 0600)
//!   load/save built on top of `harness_core::Identity`.
//! - [`trust`] (1.8, shipped) — `~/.harness/peers.toml` trust store.
//! - [`transport`] (1.4, shipped) — QUIC transport with deterministic
//!   self-signed mTLS, length-prefixed CBOR envelopes, cancel-safe recv.
//! - [`discovery`] (1.3, in progress) — mDNS service advertisement +
//!   browse, with a static-peer fallback merged into the same event
//!   stream.

pub mod discovery;
pub mod heartbeat;
pub mod identity;
pub mod transport;
pub mod trust;

mod fs_util;

pub use discovery::{
    DiscoveredPeer, Discovery, DiscoveryConfig, DiscoveryError, DiscoveryEvent, DiscoverySource,
};
pub use heartbeat::{
    HeartbeatPublisher, HeartbeatPublisherConfig, PeerEntry, PeerTable, HEARTBEAT_INTERVAL,
    PEER_TIMEOUT,
};
pub use identity::{default_root, init_or_load, load, save, IdentityError};
pub use trust::{AddedVia, Peer, TrustError, TrustEvent, TrustStore, TrustTier};
