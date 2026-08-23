//! Wire-protocol types (PRD §13).
//!
//! Phase 1.2 landed the §13.1–§13.2 surface: `Heartbeat`, `NodeManifest`,
//! `Capability`, `Cardinality`, `Scope`, `ResourceHints`, `Resources`,
//! plus the [`Signable`] trait that defines the canonical-encoding-with-
//! sig-zeroed contract every signed wire type implements.
//!
//! Phase 2.1 extends this with the §13.3–§13.5 envelopes: `Task`,
//! `TaskResult` (`Final` | `Partial`), `FinalResult`, `PartialResult`,
//! `Plan`, and supporting types (`Constraints`, `RetryPolicy`,
//! `ExecutionPolicy`, `TraceContext`, `Status`, `LogLine`, `Cost`,
//! `NodeContribution`, `Budget`, `CheckpointConfig`, ...).

pub mod cardinality;
pub mod dispatch;
pub mod heartbeat;
pub mod manifest;
pub mod plan;
pub mod result;
pub mod signable;
pub mod support;
pub mod task;

pub use cardinality::{AggregateOp, Cardinality, MergeStrategy, PartialPolicy};
pub use dispatch::{LeaseExtend, LeaseId, TaskAssign, TaskClaim, TaskResultMsg};
pub use heartbeat::Heartbeat;
pub use manifest::{Capability, CapabilityRef, NodeManifest, ResourceHints, Resources, Scope};
pub use plan::{
    find_output_refs, resolve_output_refs, step_hash, Budget, BudgetAction, CheckpointConfig,
    CheckpointStorage, HashFn, OutputRef, OutputRefError, Plan, PlanNode,
};
pub use result::{
    Cost, FinalResult, LogLevel, LogLine, NodeContribution, NodeStatus, PartialResult, Status,
    TaskResult,
};
pub use signable::{Signable, Unsigned};
pub use support::{CostHint, CpuClass, DiskIoClass, GpuInfo, NetworkClass, RateLimit};
pub use task::{Constraints, ExecutionPolicy, RetryPolicy, Task, TraceContext};
