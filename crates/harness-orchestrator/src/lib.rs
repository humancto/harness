//! Orchestrator — DAG executor, scheduler, fanout controller.
//!
//! Phase 2.2 ships pure routing: given a `Task`, a `Cardinality`, and a
//! mesh-state snapshot, decide which node(s) should execute. Phase 2.4
//! adds round-robin selection + lease-based claiming on top; Phase 4.x
//! adds streaming fanout, scoring, and federated execution lifecycles.

#![forbid(unsafe_code)]

pub mod dispatcher;
pub mod error;
pub mod index;

pub use dispatcher::{DispatchPlan, Dispatcher, LiveSet, RoundRobin, StaticLiveSet};
pub use error::DispatchError;
pub use index::{CapabilityIndex, ScopeIndex};
