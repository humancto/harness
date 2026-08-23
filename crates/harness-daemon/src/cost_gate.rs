//! 5.9 (ADR-0037): the result-row cost gate. Actual dollars persist
//! ONLY for capabilities whose LOCAL manifest is
//! `CostHint::CloudPaid` (first-party caps pricing provider-reported
//! usage). A `LocalFast` capability claiming `cost_usd` — the
//! `mcp.proxy` passthrough vector — is ignored with a warn: the
//! LEDGER never trusts a worker claim the local manifest doesn't
//! back. (5.8's in-loop enforcement still reads the raw output —
//! conservative by design; ADR-0037 records the split.)

use harness_core::protocol::CostHint;
use serde_json::Value as JsonValue;

/// The cost to persist for a completed result, or `None`.
#[must_use]
pub(crate) fn gated_cost(hint: CostHint, output: &JsonValue, capability: &str) -> Option<f64> {
    let claimed = output.get("cost_usd").and_then(JsonValue::as_f64);
    match (hint, claimed) {
        (CostHint::CloudPaid, Some(v)) if v.is_finite() && v >= 0.0 => Some(v),
        (CostHint::CloudPaid, Some(v)) => {
            tracing::warn!(
                target: "harness.cost",
                capability,
                value = v,
                "CloudPaid capability reported a non-finite/negative cost_usd; not persisted"
            );
            None
        }
        (_, Some(v)) => {
            // debug, not warn: this fires per RESULT at both the
            // worker and issuer sites — an mcp.proxy foreign output
            // with an incidental cost_usd field would double-spam.
            tracing::debug!(
                target: "harness.cost",
                capability,
                value = v,
                "non-CloudPaid capability claimed cost_usd; ignored by the ledger (ADR-0037)"
            );
            None
        }
        (_, None) => None,
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

    #[test]
    fn gate_admits_cloudpaid_only() {
        assert_eq!(
            gated_cost(
                CostHint::CloudPaid,
                &json!({"cost_usd": 0.5}),
                "llm.cloud.claude"
            ),
            Some(0.5)
        );
        assert_eq!(
            gated_cost(CostHint::LocalFast, &json!({"cost_usd": 1e9}), "mcp.proxy"),
            None,
            "the mcp.proxy inflate vector never reaches the ledger"
        );
        assert_eq!(
            gated_cost(CostHint::CloudPaid, &json!({"cost_usd": -1.0}), "x"),
            None
        );
        assert_eq!(
            gated_cost(CostHint::CloudPaid, &json!({"cost_usd": f64::NAN}), "x"),
            None
        );
        assert_eq!(gated_cost(CostHint::CloudPaid, &json!({}), "x"), None);
        assert_eq!(gated_cost(CostHint::LocalFast, &json!({}), "echo"), None);
    }
}
