//! Discovery, transport, gossip, and weighted brain election.
//!
//! Phase 1 is filling this in incrementally. The first piece is the
//! [`identity`] module — `~/.harness/identity.key` (mode 0600) load/save
//! built on top of `harness_core::Identity`. Subsequent items in Phase 1
//! will add mDNS discovery, QUIC transport, gossip, and the weighted brain
//! election.

pub mod identity;

pub use identity::{default_root, init_or_load, load, save, IdentityError};
