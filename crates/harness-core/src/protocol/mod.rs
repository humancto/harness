//! Wire-protocol types (PRD §13).
//!
//! Phase 1.2 lands the §13.1–§13.2 surface: `Heartbeat`, `NodeManifest`,
//! `Capability`, `Cardinality`, `Scope`, `ResourceHints`, `Resources`, plus
//! the [`Signable`] trait that defines the canonical-encoding-with-sig-zeroed
//! contract every signed wire type implements.
//!
//! `Task`, `Plan`, `TaskResult`, and friends (PRD §13.3–§13.5) belong to
//! item 2.1 and are not yet present.

pub mod cardinality;
pub mod heartbeat;
pub mod manifest;
pub mod signable;
pub mod support;

pub use cardinality::{AggregateOp, Cardinality, MergeStrategy, PartialPolicy};
pub use heartbeat::Heartbeat;
pub use manifest::{Capability, NodeManifest, ResourceHints, Resources, Scope};
pub use signable::Signable;
pub use support::{CostHint, CpuClass, DiskIoClass, GpuInfo, NetworkClass, RateLimit};
