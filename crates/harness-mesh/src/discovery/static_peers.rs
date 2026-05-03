//! Static-peer fallback. 1.3 is the trivial pass-through: take a
//! `Vec<SocketAddr>` from `DiscoveryConfig` and emit one
//! `DiscoveryEvent::StaticHint` per address at startup. Item 1.4's
//! `Transport::dial` validates each peer's pubkey via TLS pinning, so
//! we do NOT carry a synthetic `node_id` here — that would silently
//! confuse downstream callers.
//!
//! This module currently has no runtime state. Kept as a separate file
//! so 1.5+ can grow it without churning `mod.rs` (e.g., periodic
//! re-emission of static hints, retry/backoff for unreachable static
//! peers, etc.).
