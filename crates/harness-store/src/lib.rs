//! `SQLite` persistence for harness — tasks, capability index, scopes,
//! manifests. WAL mode, single-file at `~/.harness/harness.db`.
//!
//! Phase 2.3 ships the schema + typed CRUD; Phase 2.4 adds lease tables
//! and dispatcher cursors; Phase 2.5 wires CRDT replication on top;
//! Phase 5.11 adds checkpoints; Phase 5.13 adds the audit log.

#![forbid(unsafe_code)]

pub mod capabilities;
pub mod error;
pub mod leases;
pub mod manifests;
mod migrations;
pub mod open;
pub mod replica;
pub mod scopes;
pub mod tasks;

pub use crate::capabilities::nodes_for_versioned_capability;
pub use crate::error::StoreError;
pub use crate::leases::{Lease, LeaseId, LeaseState};
pub use crate::open::{Store, StoreConfig};
pub use crate::replica::StoreReplicaApplier;
pub use crate::tasks::{TaskRow, TaskState};
