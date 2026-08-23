//! Runtime `Budget` enforcement for plan execution (5.8, ADR-0036,
//! PRD §17.8). The `Budget { max_cost_usd, soft_limit_usd,
//! on_exceed }` wire type has ridden every `Plan` since Phase 2;
//! this module is the first thing that READS it.
//!
//! Division of labor (ADR-0036): ENFORCEMENT lives here in the
//! orchestrator (it must sit inside the exec loop); PRICING and
//! per-plan/user/day aggregation are 5.9's `harness-cost`.
//!
//! Attribution convention: a step's actual cost is the top-level
//! `cost_usd` number in its result JSON, else $0 (local execution is
//! free by the product thesis; cloud caps start emitting `cost_usd`
//! when 5.9 lands pricing). Enforcement acts on ACTUALS only —
//! estimates never abort work. Failed steps contribute $0 today
//! (their outcomes carry only an error string) — a systematic
//! undercount recorded in ADR-0036 with the 5.9 fix path.

use harness_core::protocol::{Budget, BudgetAction};

/// What one recorded step completion means for the budget.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BudgetVerdict {
    /// Under every limit (or no budget in effect, or a limit already
    /// fired — the action happens once).
    Ok,
    /// `spent` crossed `soft_limit_usd` for the FIRST time.
    SoftCrossed { spent_usd: f64, limit_usd: f64 },
    /// `spent` crossed `max_cost_usd` for the FIRST time.
    Exceeded {
        spent_usd: f64,
        cap_usd: f64,
        action: BudgetAction,
    },
}

/// Sync, lock-free spend tracker — lives inside the single exec loop.
#[derive(Debug)]
pub struct BudgetTracker {
    cap_usd: Option<f64>,
    soft_usd: Option<f64>,
    action: BudgetAction,
    /// Whether ANY budget (plan-carried, policy default, or ceiling)
    /// is in effect — controls the summary object's presence.
    active: bool,
    spent_usd: f64,
    soft_fired: bool,
    exceeded_fired: bool,
}

impl BudgetTracker {
    /// Resolve the EFFECTIVE budget (ADR-0036 §resolution):
    ///
    /// 1. The plan's own `Budget` wins over the per-mesh default —
    ///    carrying a Budget IS the "explicit approval" of §17.8
    ///    (planner backends always emit `budget: None`, pinned by
    ///    test, so only the submitter can approve).
    /// 2. No plan Budget → `default_cap_usd` (policy
    ///    `default_plan_budget_usd`, default $5) with
    ///    `on_exceed: Cancel`.
    /// 3. `ceiling_usd` (policy `plan_budget_ceiling_usd`, default
    ///    None) hard-caps EVERYTHING when set — including a
    ///    plan-carried `max_cost_usd: None` waiver. Effective cap =
    ///    min(resolved cap, ceiling); a waiver under a ceiling gets
    ///    cap = ceiling with the plan's own action.
    #[must_use]
    pub fn new(
        plan_budget: Option<Budget>,
        default_cap_usd: Option<f64>,
        ceiling_usd: Option<f64>,
    ) -> Self {
        let (mut cap, soft, action) = match plan_budget {
            Some(b) => (b.max_cost_usd, b.soft_limit_usd, b.on_exceed),
            None => (default_cap_usd, None, BudgetAction::Cancel),
        };
        if let Some(ceiling) = ceiling_usd {
            cap = Some(cap.map_or(ceiling, |c| c.min(ceiling)));
        }
        // Fail-closed sanitization (Codex P2 on #59): policy values
        // are validated at load, but a plan-carried Budget arrives
        // from the submitter — a nonsense limit becomes the STRICTEST
        // reading ($0), never "unlimited".
        let sanitize = |v: Option<f64>| v.map(|x| if x.is_finite() { x.max(0.0) } else { 0.0 });
        cap = sanitize(cap);
        let soft = sanitize(soft);
        let active = cap.is_some() || soft.is_some();
        Self {
            cap_usd: cap,
            soft_usd: soft,
            action,
            active,
            spent_usd: 0.0,
            soft_fired: false,
            exceeded_fired: false,
        }
    }

    /// Is any limit in effect at all?
    #[must_use]
    pub fn active(&self) -> bool {
        self.active
    }

    /// Actual dollars recorded so far.
    #[must_use]
    pub fn spent_usd(&self) -> f64 {
        self.spent_usd
    }

    #[must_use]
    pub fn cap_usd(&self) -> Option<f64> {
        self.cap_usd
    }

    #[must_use]
    pub fn soft_limit_usd(&self) -> Option<f64> {
        self.soft_usd
    }

    /// The action that fires on exceed (relevant when `active`).
    #[must_use]
    pub fn action(&self) -> BudgetAction {
        self.action
    }

    /// Did the hard cap fire at any point?
    #[must_use]
    pub fn triggered(&self) -> bool {
        self.exceeded_fired
    }

    /// Extract a step's actual cost from its result JSON: top-level
    /// `cost_usd`, clamped to `[0, +inf)` — NaN/negative become $0
    /// with a warn (a worker cannot "refund" a budget; ADR-0036 also
    /// notes the mcp.proxy passthrough can only INFLATE, i.e. cause a
    /// spurious stop, never a bypass).
    #[must_use]
    pub fn step_cost(output: &serde_json::Value) -> f64 {
        let raw = output.get("cost_usd").and_then(serde_json::Value::as_f64);
        match raw {
            None => 0.0,
            Some(v) if v.is_finite() && v >= 0.0 => v,
            Some(v) => {
                tracing::warn!(
                    target: "harness.budget",
                    value = v,
                    "step reported a non-finite or negative cost_usd; clamped to 0"
                );
                0.0
            }
        }
    }

    /// Record one settled step's output and report the verdict. Each
    /// limit fires exactly once; later records return `Ok`.
    pub fn record(&mut self, output: &serde_json::Value) -> BudgetVerdict {
        self.spent_usd += Self::step_cost(output);
        if !self.active {
            return BudgetVerdict::Ok;
        }
        if let Some(cap) = self.cap_usd {
            if !self.exceeded_fired && self.spent_usd > cap {
                self.exceeded_fired = true;
                // The hard cap subsumes the soft limit: a later record
                // must never emit a "soft_limit" frame AFTER
                // "exceeded" on the Progress stream (diff review on
                // #59 — 5.10 reads these in order).
                self.soft_fired = true;
                return BudgetVerdict::Exceeded {
                    spent_usd: self.spent_usd,
                    cap_usd: cap,
                    action: self.action,
                };
            }
        }
        if let Some(soft) = self.soft_usd {
            if !self.soft_fired && self.spent_usd > soft {
                self.soft_fired = true;
                return BudgetVerdict::SoftCrossed {
                    spent_usd: self.spent_usd,
                    limit_usd: soft,
                };
            }
        }
        BudgetVerdict::Ok
    }
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::float_cmp
)]
mod tests {
    use super::*;
    use serde_json::json;

    fn budget(cap: Option<f64>, soft: Option<f64>, action: BudgetAction) -> Budget {
        Budget {
            max_cost_usd: cap,
            soft_limit_usd: soft,
            on_exceed: action,
        }
    }

    #[test]
    fn t01_effective_budget_resolution() {
        // Plan budget wins over the default.
        let t = BudgetTracker::new(
            Some(budget(Some(10.0), None, BudgetAction::Notify)),
            Some(5.0),
            None,
        );
        assert_eq!(t.cap_usd(), Some(10.0));
        assert_eq!(t.action(), BudgetAction::Notify);
        // No plan budget -> policy default, Cancel.
        let t = BudgetTracker::new(None, Some(5.0), None);
        assert_eq!(t.cap_usd(), Some(5.0));
        assert_eq!(t.action(), BudgetAction::Cancel);
        assert!(t.active());
        // Nothing anywhere -> inactive.
        let t = BudgetTracker::new(None, None, None);
        assert!(!t.active());
    }

    #[test]
    fn t02_ceiling_hard_caps_even_a_waiver() {
        // Plan-carried waiver (max: None) under a ceiling: cap =
        // ceiling, the plan's own action survives.
        let t = BudgetTracker::new(
            Some(budget(None, None, BudgetAction::Notify)),
            Some(5.0),
            Some(20.0),
        );
        assert_eq!(t.cap_usd(), Some(20.0));
        assert_eq!(t.action(), BudgetAction::Notify);
        // Ceiling lowers a bigger plan cap; never raises a smaller.
        let t = BudgetTracker::new(
            Some(budget(Some(100.0), None, BudgetAction::Cancel)),
            None,
            Some(20.0),
        );
        assert_eq!(t.cap_usd(), Some(20.0));
        let t = BudgetTracker::new(
            Some(budget(Some(3.0), None, BudgetAction::Cancel)),
            None,
            Some(20.0),
        );
        assert_eq!(t.cap_usd(), Some(3.0));
        // Waiver WITHOUT a ceiling really is unlimited (trust model
        // recorded in ADR-0036).
        let t = BudgetTracker::new(
            Some(budget(None, None, BudgetAction::Cancel)),
            Some(5.0),
            None,
        );
        assert!(t.cap_usd().is_none());
        assert!(!t.active());
    }

    #[test]
    fn t03_exceed_fires_once_with_the_right_action() {
        let mut t = BudgetTracker::new(
            Some(budget(Some(1.0), None, BudgetAction::Pause)),
            None,
            None,
        );
        assert_eq!(t.record(&json!({"cost_usd": 0.6})), BudgetVerdict::Ok);
        match t.record(&json!({"cost_usd": 0.6})) {
            BudgetVerdict::Exceeded {
                spent_usd,
                cap_usd,
                action,
            } => {
                assert!((spent_usd - 1.2).abs() < 1e-9);
                assert_eq!(cap_usd, 1.0);
                assert_eq!(action, BudgetAction::Pause);
            }
            v => panic!("expected Exceeded, got {v:?}"),
        }
        // Fires once; spend keeps accumulating.
        assert_eq!(t.record(&json!({"cost_usd": 5.0})), BudgetVerdict::Ok);
        assert!(t.triggered());
        assert!((t.spent_usd() - 6.2).abs() < 1e-9);
        // And a soft limit never fires AFTER the cap did.
        let mut t = BudgetTracker::new(
            Some(budget(Some(1.0), Some(0.9), BudgetAction::Notify)),
            None,
            None,
        );
        assert!(matches!(
            t.record(&json!({"cost_usd": 1.2})),
            BudgetVerdict::Exceeded { .. }
        ));
        assert_eq!(t.record(&json!({"cost_usd": 0.1})), BudgetVerdict::Ok);
    }

    #[test]
    fn t04_soft_limit_fires_once_and_only_below_cap() {
        let mut t = BudgetTracker::new(
            Some(budget(Some(10.0), Some(1.0), BudgetAction::Cancel)),
            None,
            None,
        );
        assert_eq!(t.record(&json!({"cost_usd": 0.5})), BudgetVerdict::Ok);
        assert!(matches!(
            t.record(&json!({"cost_usd": 0.6})),
            BudgetVerdict::SoftCrossed { .. }
        ));
        assert_eq!(t.record(&json!({"cost_usd": 0.1})), BudgetVerdict::Ok);
        // Soft-limit-only budget (max: None) is active and never
        // escalates past SoftCrossed.
        let mut t = BudgetTracker::new(
            Some(budget(None, Some(1.0), BudgetAction::Cancel)),
            None,
            None,
        );
        assert!(t.active());
        assert!(matches!(
            t.record(&json!({"cost_usd": 2.0})),
            BudgetVerdict::SoftCrossed { .. }
        ));
        assert_eq!(t.record(&json!({"cost_usd": 100.0})), BudgetVerdict::Ok);
    }

    #[test]
    fn t06_nonsense_limits_fail_closed() {
        // A negative or non-finite cap is $0 (strictest), never
        // unlimited: the first costed step trips it.
        let mut t = BudgetTracker::new(
            Some(budget(Some(-3.0), None, BudgetAction::Cancel)),
            None,
            None,
        );
        assert_eq!(t.cap_usd(), Some(0.0));
        assert!(matches!(
            t.record(&json!({"cost_usd": 0.01})),
            BudgetVerdict::Exceeded { .. }
        ));
        let t = BudgetTracker::new(
            Some(budget(
                Some(f64::NAN),
                Some(f64::INFINITY),
                BudgetAction::Cancel,
            )),
            None,
            None,
        );
        assert_eq!(t.cap_usd(), Some(0.0));
        assert_eq!(t.soft_limit_usd(), Some(0.0));
    }

    #[test]
    fn t05_cost_extraction_clamps_garbage() {
        assert_eq!(BudgetTracker::step_cost(&json!({})), 0.0);
        assert_eq!(BudgetTracker::step_cost(&json!({"cost_usd": 1.5})), 1.5);
        assert_eq!(BudgetTracker::step_cost(&json!({"cost_usd": -3.0})), 0.0);
        assert_eq!(
            BudgetTracker::step_cost(&json!({"cost_usd": f64::NAN})),
            0.0
        );
        assert_eq!(BudgetTracker::step_cost(&json!({"cost_usd": "1.0"})), 0.0);
        // Nested values never count — top-level only.
        assert_eq!(
            BudgetTracker::step_cost(&json!({"metrics": {"cost_usd": 9.0}})),
            0.0
        );
    }
}
