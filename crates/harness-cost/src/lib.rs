//! Real-time cost tracking (5.9, ADR-0037, PRD §17.8).
//!
//! Division of labor (ADR-0036/0037): budget ENFORCEMENT lives in
//! `harness-orchestrator` (in the exec loop); this crate owns
//! PRICING (dollars from provider-reported token usage) and the
//! LEDGER (per-plan / per-issuer / per-day aggregation over the
//! local store).
//!
//! Pricing posture: built-ins are best-effort snapshots; the
//! `[cost.model_prices]` policy overrides are the operator contract;
//! an unknown model is UNPRICED (`None`) — never guessed.

#![forbid(unsafe_code)]

pub mod ledger;
pub mod pricing;

pub use ledger::{CostLedger, CostTotals, DayCost, IssuerCost, PlanCost};
pub use pricing::{install_pricing, price_usd, Pricing};
