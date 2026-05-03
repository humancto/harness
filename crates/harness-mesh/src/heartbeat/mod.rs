//! Heartbeat broadcast loop and peer-table maintenance (item 1.5).
//!
//! Every node runs:
//!
//! 1. A **publisher** that ticks every `HEARTBEAT_INTERVAL` (default 2s),
//!    builds a fresh [`harness_core::Heartbeat`] with a monotonically
//!    increasing `seq`, signs it with the local `Identity`, and broadcasts
//!    it to every currently-connected peer via
//!    [`crate::transport::Connection::send`].
//! 2. A **listener** per peer-connection that calls
//!    `Connection::recv_sequenced(channels::HEARTBEAT)` in a loop and
//!    feeds incoming heartbeats into a shared [`PeerTable`].
//!
//! The peer table is the local snapshot 1.6's election reads to compute
//! `brain_score` aggregates.

mod publisher;
mod table;

pub use publisher::{HeartbeatPublisher, HeartbeatPublisherConfig};
pub use table::{PeerEntry, PeerTable};

use std::time::Duration;

/// Default heartbeat interval — PRD §13.1 says 2s.
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(2);

/// How long without a heartbeat before we consider a peer offline.
/// PRD §12.1 says >6s missed for leader timeout; we use 6s for general
/// liveness too.
pub const PEER_TIMEOUT: Duration = Duration::from_secs(6);
