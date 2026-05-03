//! Discovery, transport, gossip, and weighted brain election.
//!
//! Phase 1 is filling this in incrementally:
//!
//! - [`identity`] (1.1, shipped) — `~/.harness/identity.key` (mode 0600)
//!   load/save built on top of `harness_core::Identity`.
//! - [`trust`] (1.8, in progress) — `~/.harness/peers.toml` trust store.
//! - mDNS discovery, QUIC transport, gossip, and the weighted brain
//!   election follow in 1.3+.

pub mod identity;
pub mod trust;

mod fs_util;

pub use identity::{default_root, init_or_load, load, save, IdentityError};
pub use trust::{AddedVia, Peer, TrustError, TrustEvent, TrustStore, TrustTier};
