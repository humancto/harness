//! Orchestrator — DAG executor, scheduler, fanout controller.
//!
//! Phase 2.2 ships pure routing: given a `Task`, a `Cardinality`, and a
//! mesh-state snapshot, decide which node(s) should execute. Phase 2.4
//! adds round-robin selection + lease-based claiming on top; Phase 3.1
//! adds the `executor::policy_check` shim that 3.2 (`shell.exec`) will
//! call before invoking a command. Phase 4.x adds streaming fanout,
//! scoring, and federated execution lifecycles.

#![forbid(unsafe_code)]

pub mod dag;
pub mod dispatcher;
pub mod error;
pub mod executor;
pub mod fanout;
pub mod index;
pub mod results;

pub use dag::{
    DagError, DagScheduler, DagSummary, Progress, StepOutcome, StepState, MAX_PLAN_STEPS,
};
pub use dispatcher::{DispatchPlan, Dispatcher, LiveSet, RoundRobin, StaticLiveSet};
pub use error::DispatchError;
pub use executor::{policy_check, ExecutorError};
pub use fanout::{
    EndReason, FanoutController, FanoutEvent, FanoutSpec, FanoutStream, FanoutSummary, ItemOutcome,
    WindowPolicy,
};
pub use index::{CapabilityIndex, ScopeIndex};
pub use results::{task_results, ResultCtx, ResultMapper, TaskResultStream};
