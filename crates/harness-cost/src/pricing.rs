//! Model pricing: dollars per million input/output tokens, matched by
//! LONGEST model-id prefix across built-ins ∪ overrides (ADR-0037 —
//! plain first-match would price `gpt-4o-mini`, the `OpenAI` cap's
//! DEFAULT model, at `gpt-4o`'s ~16x rate).

use std::sync::OnceLock;

/// `(model-id prefix, $ per 1M input tokens, $ per 1M output tokens)`.
type Rate = (&'static str, f64, f64);

/// Built-in snapshot (best-effort; the `[cost.model_prices]` policy
/// override map is the operator contract — ADR-0037). Kept coarse on
/// purpose: prefixes, not SKUs.
const BUILTIN: &[Rate] = &[
    // Anthropic
    ("claude-fable-5", 10.0, 50.0),
    ("claude-opus-5", 5.0, 25.0),
    // Current Opus 4.x generation (4.5+) — longer prefixes shadow the
    // legacy claude-opus-4/4.1 rate below (longest-prefix-wins).
    ("claude-opus-4-5", 5.0, 25.0),
    ("claude-opus-4-6", 5.0, 25.0),
    ("claude-opus-4-7", 5.0, 25.0),
    ("claude-opus-4-8", 5.0, 25.0),
    ("claude-opus-4", 15.0, 75.0),
    ("claude-sonnet-5", 3.0, 15.0),
    ("claude-sonnet-4", 3.0, 15.0),
    ("claude-haiku-4", 1.0, 5.0),
    ("claude-3-5-sonnet", 3.0, 15.0),
    ("claude-3-5-haiku", 0.8, 4.0),
    // OpenAI — note gpt-4o-mini BEFORE gpt-4o is not required
    // (longest prefix wins), but both must exist.
    ("gpt-4o-mini", 0.15, 0.6),
    ("gpt-4o", 2.5, 10.0),
    ("gpt-4.1-mini", 0.4, 1.6),
    ("gpt-4.1-nano", 0.1, 0.4),
    ("gpt-4.1", 2.0, 8.0),
    ("o3-mini", 1.1, 4.4),
    ("o3", 2.0, 8.0),
    // Google
    ("gemini-2.5-pro", 1.25, 10.0),
    ("gemini-2.5-flash-lite", 0.1, 0.4),
    ("gemini-2.5-flash", 0.3, 2.5),
    ("gemini-2.0-flash-lite", 0.075, 0.3),
    ("gemini-2.0-flash", 0.1, 0.4),
];

/// A pricing table: built-ins plus operator overrides. Overrides with
/// the SAME prefix replace the built-in rate; at lookup the longest
/// matching prefix wins regardless of which table it came from.
#[derive(Debug, Clone, Default)]
pub struct Pricing {
    /// `(prefix, in_per_m, out_per_m)` — overrides.
    overrides: Vec<(String, f64, f64)>,
}

impl Pricing {
    /// Built-ins only.
    #[must_use]
    pub fn builtin() -> Self {
        Self::default()
    }

    /// Built-ins + operator overrides (already validated by policy
    /// load: finite, non-negative, non-empty prefix).
    #[must_use]
    pub fn with_overrides(overrides: Vec<(String, f64, f64)>) -> Self {
        Self { overrides }
    }

    /// Price a call, or `None` for an unknown model (never guessed).
    /// Longest-prefix-wins across built-ins ∪ overrides; on equal
    /// length the override wins.
    #[must_use]
    pub fn price_usd(
        &self,
        model: &str,
        prompt_tokens: u64,
        completion_tokens: u64,
    ) -> Option<f64> {
        let mut best: Option<(usize, f64, f64)> = None;
        for (prefix, i, o) in BUILTIN {
            if model.starts_with(prefix) {
                let len = prefix.len();
                if best.is_none_or(|(l, _, _)| len > l) {
                    best = Some((len, *i, *o));
                }
            }
        }
        for (prefix, i, o) in &self.overrides {
            if model.starts_with(prefix.as_str()) {
                let len = prefix.len();
                // >= : an override at equal length beats the built-in.
                if best.is_none_or(|(l, _, _)| len >= l) {
                    best = Some((len, *i, *o));
                }
            }
        }
        #[allow(clippy::cast_precision_loss)]
        best.map(|(_, in_per_m, out_per_m)| {
            (prompt_tokens as f64 * in_per_m + completion_tokens as f64 * out_per_m) / 1_000_000.0
        })
    }
}

static INSTALLED: OnceLock<Pricing> = OnceLock::new();

/// Install the process-wide pricing table (daemon boot, once, from
/// policy `[cost.model_prices]`). Returns `false` if already
/// installed (the first install wins; callers log).
///
/// NOTE (ADR-0037): first-install-wins is per PROCESS — tests that
/// boot multiple daemons in one process share one table, and pricing
/// never follows a policy reload. Tests asserting override behavior
/// must use [`Pricing`] instances, never this global.
pub fn install_pricing(pricing: Pricing) -> bool {
    INSTALLED.set(pricing).is_ok()
}

/// Price against the installed table (built-ins when nothing was
/// installed) — the convenience entry point for capabilities.
/// An unpriced model WARNS once per model id (ADR-0037: the
/// unknown-means-unpriced posture only bounds damage if someone can
/// notice the table went stale).
#[must_use]
pub fn price_usd(model: &str, prompt_tokens: u64, completion_tokens: u64) -> Option<f64> {
    let priced = INSTALLED.get().map_or_else(
        || Pricing::default().price_usd(model, prompt_tokens, completion_tokens),
        |p| p.price_usd(model, prompt_tokens, completion_tokens),
    );
    if priced.is_none() && !model.is_empty() {
        static WARNED: OnceLock<std::sync::Mutex<std::collections::HashSet<String>>> =
            OnceLock::new();
        let warned = WARNED.get_or_init(|| std::sync::Mutex::new(std::collections::HashSet::new()));
        if let Ok(mut set) = warned.lock() {
            if set.len() < 256 && set.insert(model.to_string()) {
                tracing::warn!(
                    target: "harness.cost",
                    model,
                    "model is not in the pricing table; calls are UNPRICED — \
                     add a [cost.model_prices] override"
                );
            }
        }
    }
    priced
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

    #[test]
    fn t01_longest_prefix_wins_gpt4o_vs_mini() {
        // The exact pair from the plan review: gpt-4o-mini must NOT
        // price at gpt-4o's rate (~16x).
        let p = Pricing::builtin();
        let mini = p
            .price_usd("gpt-4o-mini-2024-07-18", 1_000_000, 0)
            .expect("mini");
        let full = p
            .price_usd("gpt-4o-2024-08-06", 1_000_000, 0)
            .expect("full");
        assert_eq!(mini, 0.15);
        assert_eq!(full, 2.5);
    }

    #[test]
    fn t02_unknown_model_is_unpriced() {
        let p = Pricing::builtin();
        assert!(p.price_usd("llama3:8b", 1000, 1000).is_none());
        assert!(p.price_usd("", 1000, 1000).is_none());
    }

    #[test]
    fn t03_override_wins_at_equal_or_longer_prefix() {
        let p = Pricing::with_overrides(vec![("gpt-4o".into(), 1.0, 2.0)]);
        // Equal-length override replaces the built-in rate…
        assert_eq!(p.price_usd("gpt-4o-2024", 1_000_000, 0), Some(1.0));
        // …but the LONGER built-in mini prefix still wins for mini.
        assert_eq!(p.price_usd("gpt-4o-mini-x", 1_000_000, 0), Some(0.15));
        // A longer override beats everything.
        let p = Pricing::with_overrides(vec![("gpt-4o-mini-x".into(), 9.0, 9.0)]);
        assert_eq!(p.price_usd("gpt-4o-mini-x-1", 1_000_000, 0), Some(9.0));
        // Overrides can add NEW models.
        let p = Pricing::with_overrides(vec![("llama3".into(), 0.0, 0.0)]);
        assert_eq!(p.price_usd("llama3:8b", 5000, 5000), Some(0.0));
    }

    #[test]
    fn t04_arithmetic_is_per_million_both_sides() {
        let p = Pricing::builtin();
        let c = p
            .price_usd("claude-sonnet-5", 2_000_000, 1_000_000)
            .expect("priced");
        assert!((c - (2.0 * 3.0 + 1.0 * 15.0)).abs() < 1e-9);
        assert_eq!(p.price_usd("claude-sonnet-5", 0, 0), Some(0.0));
    }
}
