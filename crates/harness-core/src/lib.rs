//! Protocol types, traits, and shared glue for Harness.
//!
//! Phase 1 is filling this in incrementally:
//!
//! - **1.1** (shipped): [`identity`] — Ed25519 keys, [`NodeId`], sign/verify.
//! - **1.2** (in progress): [`protocol`] — wire types from PRD §13.
//! - **1.3+**: mDNS, QUIC, gossip, election (in `harness-mesh`).

pub mod audit;
pub mod error;
pub mod identity;
pub mod ids;
pub mod protocol;
pub mod replica;

pub use crate::error::ProtocolError;
pub use crate::ids::{PlanId, SemVer, TaskId};
pub use crate::replica::{
    ReplicaApplier, ReplicaError, ReplicaSyncEnvelope, ReplicatedState, ReplicatedTaskState,
};
pub use audit::{
    audit_entry_hash, hash_hex, AuditAction, AuditActor, AuditHead, AuditRecord, AuditSink,
    AuditSyncEnvelope, NullAuditSink, MAX_HEADS_PER_ENVELOPE,
};
pub use identity::{
    verify, Identity, KeyError, NodeId, ParseNodeIdError, PublicKey, Signature, VerifyError,
};
pub use protocol::{
    find_output_refs, resolve_output_refs, step_hash, Budget, BudgetAction, Capability,
    CapabilityRef, Cardinality, CheckpointConfig, CheckpointStorage, Constraints, Cost,
    ExecutionPolicy, FinalResult, HashFn, Heartbeat, LeaseExtend, LeaseId, LogLevel, LogLine,
    MergeStrategy, NodeContribution, NodeManifest, NodeStatus, OutputRef, OutputRefError,
    PartialPolicy, PartialResult, Plan, PlanNode, ResourceHints, Resources, RetryPolicy, Scope,
    Signable, Status, Task, TaskAssign, TaskClaim, TaskResult, TaskResultMsg, TraceContext,
    Unsigned,
};
