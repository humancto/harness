//! Discovery, transport, gossip, and weighted brain election.
//!
//! Phase 1 is filling this in incrementally:
//!
//! - [`identity`] (1.1, shipped) — `~/.harness/identity.key` (mode 0600)
//!   load/save built on top of `harness_core::Identity`.
//! - mDNS discovery, QUIC transport, gossip, and the weighted brain
//!   election follow in 1.3+.

pub mod identity;

mod fs_util;

pub use identity::{default_root, init_or_load, load, save, IdentityError};
