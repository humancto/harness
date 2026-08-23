//! Planner runtime and backend tiers (PRD §15).
//!
//! Phase 3.8 ships the [`Template`](template::TemplateBackend) tier —
//! pure pattern-matching, deterministic, no LLM. Phase 3.9 adds
//! `LocalFast` (Ollama-backed) at tier 1 of the same trait, plus
//! schema-aware plan validation via [`schema::CapabilitySchemaIndex`]
//! and [`validate::validate_plan`].
//!
//! Operators consume this crate via the `brain.plan` capability in
//! `harness-capabilities`. The crate is policy-blind; the `LocalFast`
//! backend (HTTP to Ollama) is feature-gated behind `localfast` so a
//! Template-only build does not pull in `reqwest`.

#![forbid(unsafe_code)]

pub mod backend;
#[cfg(feature = "cloud")]
pub mod cloud;
pub mod error;
#[cfg(any(feature = "localfast", feature = "cloud"))]
mod llm_common;
#[cfg(feature = "localfast")]
pub mod local_fast;
pub mod schema;
pub mod template;
pub mod validate;

pub use backend::{
    CapabilityRef, PlanConstraints, PlanOutcome, PlanRequest, PlanResponse, PlannerBackend,
    Unsigned,
};
pub use error::{PlanValidationError, PlannerError};
pub use schema::CapabilitySchemaIndex;
pub use template::{TemplateBackend, BACKEND_ID as TEMPLATE_BACKEND_ID, TEMPLATE_CONFIDENCE};
#[allow(deprecated)]
pub use validate::{validate_plan, validate_plan_well_formed};

#[cfg(feature = "localfast")]
pub use local_fast::{LocalFastBackend, LocalStrongBackend};

#[cfg(feature = "cloud")]
pub use cloud::{CloudBackend, CloudKeyProvider};

#[cfg(any(feature = "localfast", feature = "cloud"))]
pub use llm_common::extract_json_object;
