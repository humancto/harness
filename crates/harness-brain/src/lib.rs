//! Planner runtime and backend tiers (PRD §15).
//!
//! Phase 3.8 ships the [`Template`](template::TemplateBackend) tier —
//! pure pattern-matching, deterministic, no LLM. Phase 3.9 layers on
//! `LocalFast` (Ollama-backed) at tier 1 of the same trait.
//!
//! Operators consume this crate via the `brain.plan` capability in
//! `harness-capabilities`; this crate itself is policy-blind and has no
//! HTTP / I/O surface.

#![forbid(unsafe_code)]

pub mod backend;
pub mod error;
pub mod template;
pub mod validate;

pub use backend::{
    CapabilityRef, PlanConstraints, PlanOutcome, PlanRequest, PlanResponse, PlannerBackend,
    Unsigned,
};
pub use error::{PlanValidationError, PlannerError};
pub use template::{TemplateBackend, BACKEND_ID as TEMPLATE_BACKEND_ID, TEMPLATE_CONFIDENCE};
pub use validate::validate_plan_well_formed;
