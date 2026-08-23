//! Read-side cost aggregation over the LOCAL store (5.9, ADR-0037).
//!
//! No new database, no background task: `CostLedger::totals` runs two
//! bounded queries (costed result rows + `plan.execute` aggregates)
//! and folds them into per-plan / per-issuer / per-UTC-day totals.
//!
//! Honesty notes (ADR-0037):
//! - This is a LOCAL view. Sub-task rows for a plan exist on the
//!   coordinator (issuer-side ingest writes them), so `/costs` is
//!   meaningful on the node that ran the plan and near-empty on a
//!   bystander. Per-issuer totals collapse onto the coordinating node
//!   for plan steps (sub-tasks are `issued_by` the coordinator).
//! - The window is time-bounded (default 30 days) with a row cap as
//!   backstop; the response echoes the effective window so the UI
//!   never presents a truncated number as an all-time total.

use std::collections::BTreeMap;

use harness_store::Store;
use serde::Serialize;

/// Time window: 30 days.
const WINDOW_DAYS: u64 = 30;
const WINDOW_MS: u64 = WINDOW_DAYS * 24 * 3600 * 1000;
/// Row-cap backstops (bounded everything).
const MAX_RESULT_ROWS: usize = 5_000;
const MAX_PLAN_AGGREGATES: usize = 200;
/// Output list caps.
const MAX_PER_PLAN: usize = 100;
const MAX_PER_ISSUER: usize = 100;

/// Per-plan cost + budget context (5.10 renders this directly).
#[derive(Debug, Clone, Serialize)]
pub struct PlanCost {
    pub plan_id: String,
    pub name: Option<String>,
    /// Sum of costed step rows carrying this `plan_id` (local store).
    pub actual_usd: f64,
    /// What the 5.8 budget object recorded at execution time.
    pub reported_spent_usd: Option<f64>,
    pub cap_usd: Option<f64>,
    pub soft_limit_usd: Option<f64>,
    pub triggered: Option<bool>,
    pub status: Option<String>,
}

/// The `/costs` payload body.
#[derive(Debug, Clone, Serialize)]
pub struct CostTotals {
    /// Effective window, echoed so the UI labels totals honestly.
    pub window_days: u64,
    /// Whether the row cap truncated the window (undercount possible).
    pub truncated: bool,
    pub total_usd: f64,
    pub today_usd: f64,
    pub per_plan: Vec<PlanCost>,
    /// Descending by spend.
    pub per_issuer: Vec<IssuerCost>,
    /// Ascending by day.
    pub per_day: Vec<DayCost>,
}

/// Per-issuing-node spend (object shape so 5.10 can grow fields).
#[derive(Debug, Clone, Serialize)]
pub struct IssuerCost {
    pub node_id: String,
    pub usd: f64,
}

/// Per-UTC-day spend.
#[derive(Debug, Clone, Serialize)]
pub struct DayCost {
    /// `YYYY-MM-DD`.
    pub day: String,
    pub usd: f64,
}

/// Read-side aggregator. Stateless; construct per request.
#[derive(Debug, Default)]
pub struct CostLedger;

impl CostLedger {
    /// Fold the local store's recent costed rows into totals.
    ///
    /// # Errors
    /// Propagates store read errors.
    #[allow(clippy::too_many_lines)]
    pub fn totals(store: &Store, now_ms: u64) -> Result<CostTotals, harness_store::StoreError> {
        let since = now_ms.saturating_sub(WINDOW_MS);
        let rows = store.recent_result_costs(since, MAX_RESULT_ROWS)?;
        let mut truncated = rows.len() >= MAX_RESULT_ROWS;

        let mut total = 0.0_f64;
        let mut today = 0.0_f64;
        let mut per_plan: BTreeMap<String, f64> = BTreeMap::new();
        let mut per_issuer: BTreeMap<String, f64> = BTreeMap::new();
        let mut per_day: BTreeMap<String, f64> = BTreeMap::new();
        let today_str = utc_day(now_ms);
        for (_cap, plan_id, issued_by, at_ms, cost) in rows {
            let usd = cost.unwrap_or(0.0);
            if usd <= 0.0 {
                continue;
            }
            total += usd;
            let day = utc_day(at_ms);
            if day == today_str {
                today += usd;
            }
            *per_day.entry(day).or_insert(0.0) += usd;
            *per_issuer.entry(issued_by.to_string()).or_insert(0.0) += usd;
            if let Some(pid) = plan_id {
                *per_plan
                    .entry(pid.0.as_hyphenated().to_string())
                    .or_insert(0.0) += usd;
            }
        }

        // Budget context from plan.execute aggregates (there is no
        // plans table; the aggregate is the plan-level record).
        let aggregates =
            store.recent_outputs_for_capability("plan.execute", since, MAX_PLAN_AGGREGATES)?;
        let mut plans: Vec<PlanCost> = Vec::new();
        let mut seen_plan_ids = std::collections::HashSet::new();
        for agg in &aggregates {
            let Some(plan_id) = agg.get("plan_id").and_then(serde_json::Value::as_str) else {
                continue;
            };
            // A re-run plan yields multiple aggregates for one id —
            // the newest (rows are newest-first) wins, no duplicates.
            if !seen_plan_ids.insert(plan_id.to_string()) {
                continue;
            }
            let actual = per_plan.remove(plan_id).unwrap_or(0.0);
            let b = agg.get("budget");
            plans.push(PlanCost {
                plan_id: plan_id.to_string(),
                name: agg
                    .get("name")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string),
                actual_usd: actual,
                reported_spent_usd: b
                    .and_then(|b| b.get("spent_usd"))
                    .and_then(serde_json::Value::as_f64),
                cap_usd: b
                    .and_then(|b| b.get("cap_usd"))
                    .and_then(serde_json::Value::as_f64),
                soft_limit_usd: b
                    .and_then(|b| b.get("soft_limit_usd"))
                    .and_then(serde_json::Value::as_f64),
                triggered: b
                    .and_then(|b| b.get("triggered"))
                    .and_then(serde_json::Value::as_bool),
                status: agg
                    .get("status")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string),
            });
            if plans.len() >= MAX_PER_PLAN {
                truncated = true;
                break;
            }
        }
        // Plans with costed steps but no aggregate in the window
        // (still running, or the aggregate aged out) keep a bare row.
        for (pid, usd) in per_plan {
            if plans.len() >= MAX_PER_PLAN {
                // Their dollars stay in total_usd — say the list is
                // incomplete rather than presenting it as everything.
                truncated = true;
                break;
            }
            plans.push(PlanCost {
                plan_id: pid,
                name: None,
                actual_usd: usd,
                reported_spent_usd: None,
                cap_usd: None,
                soft_limit_usd: None,
                triggered: None,
                status: None,
            });
        }

        let mut issuers: Vec<IssuerCost> = per_issuer
            .into_iter()
            .map(|(node_id, usd)| IssuerCost { node_id, usd })
            .collect();
        issuers.sort_by(|a, b| b.usd.total_cmp(&a.usd));
        if issuers.len() > MAX_PER_ISSUER {
            truncated = true;
            issuers.truncate(MAX_PER_ISSUER);
        }

        Ok(CostTotals {
            window_days: WINDOW_DAYS,
            truncated,
            total_usd: total,
            today_usd: today,
            per_plan: plans,
            per_issuer: issuers,
            per_day: per_day
                .into_iter()
                .map(|(day, usd)| DayCost { day, usd })
                .collect(),
        })
    }
}

/// Unix ms → `YYYY-MM-DD` (UTC). Civil-from-days per Howard Hinnant's
/// algorithm — no chrono dependency for one date string.
#[must_use]
#[allow(clippy::similar_names, clippy::cast_possible_wrap)]
pub fn utc_day(unix_ms: u64) -> String {
    let days = i64::try_from(unix_ms / 86_400_000).unwrap_or(i64::MAX);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}")
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

    fn task(
        store: &Store,
        cap: &str,
        plan: Option<harness_core::PlanId>,
        issuer: [u8; 16],
    ) -> harness_core::TaskId {
        use harness_core::Signable as _;
        let id = harness_core::TaskId::new_v7();
        let mut t = harness_core::Task {
            id,
            parent: None,
            plan_id: plan,
            capability: cap.to_string(),
            input: serde_json::json!({}),
            constraints: harness_core::Constraints::default(),
            retry: harness_core::RetryPolicy::default(),
            execution: harness_core::ExecutionPolicy::default(),
            resource_hints: harness_core::ResourceHints {
                cpu_class: harness_core::protocol::CpuClass::Light,
                memory_mb: None,
                gpu_required: false,
                gpu_memory_mb: None,
                network_class: harness_core::protocol::NetworkClass::None,
                disk_io_class: harness_core::protocol::DiskIoClass::None,
                estimated_duration_ms: None,
            },
            trace_ctx: harness_core::TraceContext::default(),
            issued_by: harness_core::NodeId::from_bytes(issuer),
            issued_at: 1,
            tags: Vec::new(),
            sig: harness_core::Signature::from_bytes([0u8; 64]),
        };
        t.sign(&harness_core::Identity::generate()).expect("sign");
        store.insert_task(&t).expect("insert");
        id
    }

    #[test]
    fn t02_totals_fold_plans_issuers_days() {
        let store = Store::open_memory().expect("store");
        let now = 1_787_486_400_000_u64; // 2026-08-23 noon UTC
        let plan = harness_core::PlanId::new_v7();
        let by = harness_core::NodeId::from_bytes([9; 16]);

        // Two costed steps in the plan (one today, one yesterday), a
        // NULL-cost row, and the plan.execute aggregate with budget.
        let step_a = task(&store, "llm.cloud.claude", Some(plan), [9; 16]);
        store
            .write_task_result_done(step_a, &serde_json::json!({}), now, by)
            .expect("row");
        store.write_result_cost(step_a, 1.5).expect("cost");
        let step_b = task(&store, "llm.cloud.claude", Some(plan), [9; 16]);
        store
            .write_task_result_done(step_b, &serde_json::json!({}), now - 86_400_000, by)
            .expect("row");
        store.write_result_cost(step_b, 0.5).expect("cost");
        let step_c = task(&store, "echo", None, [7; 16]);
        store
            .write_task_result_done(step_c, &serde_json::json!({}), now, by)
            .expect("row");
        let exec = task(&store, "plan.execute", None, [9; 16]);
        store
            .write_task_result_done(
                exec,
                &serde_json::json!({
                    "plan_id": plan.0.as_hyphenated().to_string(),
                    "name": "demo",
                    "status": "done",
                    "budget": {"spent_usd": 2.0, "cap_usd": 5.0, "triggered": false},
                }),
                now,
                by,
            )
            .expect("row");

        let t = CostLedger::totals(&store, now).expect("totals");
        assert!((t.total_usd - 2.0).abs() < 1e-9);
        assert!((t.today_usd - 1.5).abs() < 1e-9);
        assert!(!t.truncated);
        assert_eq!(t.per_day.len(), 2);
        assert_eq!(t.per_issuer.len(), 1, "NULL-cost rows contribute nothing");
        assert_eq!(t.per_plan.len(), 1);
        let p = &t.per_plan[0];
        assert_eq!(p.name.as_deref(), Some("demo"));
        assert!((p.actual_usd - 2.0).abs() < 1e-9);
        assert_eq!(p.cap_usd, Some(5.0));
        assert_eq!(p.reported_spent_usd, Some(2.0));
        assert_eq!(p.triggered, Some(false));
        // Outside the window: empty.
        let t = CostLedger::totals(&store, now + WINDOW_MS + 86_400_000).expect("totals");
        assert_eq!(t.total_usd, 0.0);
        assert!(t.per_plan.is_empty());
    }

    #[test]
    fn t01_utc_day_known_values() {
        assert_eq!(utc_day(0), "1970-01-01");
        // 2026-08-23 12:00:00 UTC
        assert_eq!(utc_day(1_787_486_400_000), "2026-08-23");
        // Leap day.
        assert_eq!(utc_day(1_709_164_800_000), "2024-02-29");
    }
}
