//! Pure `MergeStrategy` execution (roadmap 4.5, PRD §14.6, ADR-0027).
//!
//! The federated coordinator (harness-daemon) collects per-node
//! outputs and calls [`merge`]; this crate is pure — no store, no
//! runtime, no I/O. Input order (the caller passes nodes sorted by
//! `NodeId`) is the tiebreak order for `Dedupe` (first wins) and the
//! stability order for `TopK`.
//!
//! **Item convention** (ADR-0027): a node output that is a JSON array
//! is its items; an object with an `"items"` array contributes that
//! array; anything else is one opaque item. Deterministic — a
//! capability opts into item-level merging by shaping its output.

#![forbid(unsafe_code)]

use harness_core::protocol::{AggregateOp, MergeStrategy};
use harness_core::NodeId;
use serde_json::{json, Value as JsonValue};

/// Hard cap on items in a merged output (≤64 nodes × ~1000 items each
/// must not become an unbounded JSON blob). Excess is truncated and
/// REPORTED in [`Merged::truncated_items`] — never silent.
pub const MAX_MERGE_ITEMS: usize = 10_000;

/// One node's raw sub-result entering a merge.
#[derive(Debug, Clone)]
pub struct NodeOutput {
    pub node_id: NodeId,
    pub output: JsonValue,
}

/// Merged output + per-input item accounting (parallel to the input
/// slice) — the coordinator maps `item_counts[i]` into
/// `NodeContribution::item_count`. Counts are items CONTRIBUTED
/// (pre-merge): `Dedupe`/`TopK` may surface fewer than the sum
/// (ADR-0027 records the choice).
#[derive(Debug, Clone)]
pub struct Merged {
    pub output: JsonValue,
    pub item_counts: Vec<u64>,
    pub truncated_items: u64,
}

/// Merge failures — every variant is a caller-visible task error.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum MergeError {
    #[error("custom merge requires capability {capability:?}; unsupported until Phase 5")]
    CustomUnsupported { capability: String },
    #[error("aggregate field {field:?} missing or non-numeric in every item")]
    AggregateNoData { field: String },
}

/// Extract a node output's items per the crate convention.
#[must_use]
pub fn extract_items(output: &JsonValue) -> Vec<JsonValue> {
    match output {
        JsonValue::Array(items) => items.clone(),
        JsonValue::Object(obj) => match obj.get("items") {
            Some(JsonValue::Array(items)) => items.clone(),
            _ => vec![output.clone()],
        },
        other => vec![other.clone()],
    }
}

fn strategy_name(strategy: &MergeStrategy) -> &'static str {
    match strategy {
        MergeStrategy::Concat => "concat",
        MergeStrategy::Dedupe { .. } => "dedupe",
        MergeStrategy::TopK { .. } => "top_k",
        MergeStrategy::Rerank { .. } => "rerank",
        MergeStrategy::Aggregate { .. } => "aggregate",
        MergeStrategy::Custom { .. } => "custom",
        _ => "unknown",
    }
}

fn score_of(item: &JsonValue, field: &str) -> f64 {
    item.get(field).and_then(JsonValue::as_f64).unwrap_or(0.0)
}

/// Sort items by `score_field` descending (stable) and keep `k`.
/// Public because `mesh.search` shares it (score-sort degradation).
#[must_use]
pub fn top_k_by_score(mut items: Vec<JsonValue>, k: usize, score_field: &str) -> Vec<JsonValue> {
    items.sort_by(|a, b| {
        score_of(b, score_field)
            .partial_cmp(&score_of(a, score_field))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    items.truncate(k);
    items
}

/// The `Aggregate` strategy body: pull `field` as f64 from every item
/// and apply `op`. `Min`/`Max`/`Mean` over zero numeric items error —
/// a silent `0.0` extremum would be indistinguishable from a real one
/// (PR #48 diff review); `Sum`/`Count` legitimately yield 0.
fn aggregate(op: AggregateOp, field: &str, all: &[JsonValue]) -> Result<JsonValue, MergeError> {
    let nums: Vec<f64> = all
        .iter()
        .filter_map(|i| i.get(field).and_then(JsonValue::as_f64))
        .collect();
    let no_data = || MergeError::AggregateNoData {
        field: field.to_string(),
    };
    let value = match op {
        AggregateOp::Count => {
            #[allow(clippy::cast_precision_loss)]
            let v = nums.len() as f64;
            v
        }
        AggregateOp::Sum => nums.iter().sum(),
        AggregateOp::Min | AggregateOp::Max | AggregateOp::Mean if nums.is_empty() => {
            return Err(no_data())
        }
        AggregateOp::Min => nums.iter().copied().fold(f64::INFINITY, f64::min),
        AggregateOp::Max => nums.iter().copied().fold(f64::NEG_INFINITY, f64::max),
        AggregateOp::Mean => {
            #[allow(clippy::cast_precision_loss)]
            let mean = nums.iter().sum::<f64>() / nums.len() as f64;
            mean
        }
        // Future ops (non_exhaustive): no data semantics defined yet.
        _ => return Err(no_data()),
    };
    let value = if value.is_finite() { value } else { 0.0 };
    Ok(json!({
        "value": value,
        "merge": {
            "strategy": "aggregate",
            "op": format!("{op:?}").to_lowercase(),
            "field": field,
            "count": nums.len(),
        },
    }))
}

/// Execute a merge strategy over per-node outputs (see module docs).
///
/// `Rerank { top_k, .. }` degrades to `TopK { k: top_k, score_field:
/// "score" }` until a reranker capability exists (Phase 5) — the same
/// documented degradation ADR-0022 uses for `mesh.search`.
///
/// # Errors
/// [`MergeError::CustomUnsupported`] for `Custom`;
/// [`MergeError::AggregateNoData`] when `Mean` finds no numeric data.
pub fn merge(strategy: &MergeStrategy, inputs: &[NodeOutput]) -> Result<Merged, MergeError> {
    let per_node: Vec<Vec<JsonValue>> = inputs.iter().map(|n| extract_items(&n.output)).collect();
    let item_counts: Vec<u64> = per_node
        .iter()
        .map(|v| u64::try_from(v.len()).unwrap_or(u64::MAX))
        .collect();
    let all: Vec<JsonValue> = per_node.into_iter().flatten().collect();

    // `Concat` (and any future item strategy) falls to the wildcard:
    // keep everything in node order.
    let (items, extra): (Vec<JsonValue>, Option<JsonValue>) = match strategy {
        MergeStrategy::Dedupe { key } => {
            let mut seen = std::collections::HashSet::new();
            let mut out = Vec::new();
            for item in all {
                // Items missing the key are opaque: always kept.
                let id = item.get(key).map(ToString::to_string);
                match id {
                    Some(id) => {
                        if seen.insert(id) {
                            out.push(item);
                        }
                    }
                    None => out.push(item),
                }
            }
            (out, None)
        }
        MergeStrategy::TopK { k, score_field } => (top_k_by_score(all, *k, score_field), None),
        MergeStrategy::Rerank { top_k, .. } => {
            // Documented degradation until a reranker capability exists.
            (top_k_by_score(all, *top_k, "score"), None)
        }
        MergeStrategy::Aggregate { op, field } => (Vec::new(), Some(aggregate(*op, field, &all)?)),
        MergeStrategy::Custom { capability } => {
            return Err(MergeError::CustomUnsupported {
                capability: capability.clone(),
            })
        }
        _ => (all, None),
    };

    if let Some(output) = extra {
        return Ok(Merged {
            output,
            item_counts,
            truncated_items: 0,
        });
    }
    let truncated_items = u64::try_from(items.len().saturating_sub(MAX_MERGE_ITEMS)).unwrap_or(0);
    let mut items = items;
    items.truncate(MAX_MERGE_ITEMS);
    let output = json!({
        "items": items,
        "merge": {
            "strategy": strategy_name(strategy),
            "truncated_items": truncated_items,
        },
    });
    Ok(Merged {
        output,
        item_counts,
        truncated_items,
    })
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

    fn node(byte: u8, output: JsonValue) -> NodeOutput {
        NodeOutput {
            node_id: NodeId::from_bytes([byte; 16]),
            output,
        }
    }

    #[test]
    fn t01_concat_preserves_input_order() {
        let m = merge(
            &MergeStrategy::Concat,
            &[
                node(1, json!({"items": [{"v": "a1"}, {"v": "a2"}]})),
                node(2, json!([{"v": "b1"}])),
                node(3, json!("opaque")),
            ],
        )
        .expect("merge");
        let items = m.output["items"].as_array().unwrap();
        assert_eq!(items.len(), 4);
        assert_eq!(items[0]["v"], "a1");
        assert_eq!(items[2]["v"], "b1");
        assert_eq!(items[3], "opaque");
        assert_eq!(m.item_counts, vec![2, 1, 1]);
        assert_eq!(m.output["merge"]["strategy"], "concat");
        assert_eq!(m.truncated_items, 0);
    }

    #[test]
    fn t02_dedupe_first_wins_missing_key_kept() {
        let m = merge(
            &MergeStrategy::Dedupe { key: "id".into() },
            &[
                node(
                    1,
                    json!({"items": [{"id": "x", "from": 1}, {"nokey": true}]}),
                ),
                node(2, json!({"items": [{"id": "x", "from": 2}, {"id": "y"}]})),
            ],
        )
        .expect("merge");
        let items = m.output["items"].as_array().unwrap();
        assert_eq!(items.len(), 3);
        assert_eq!(items[0]["from"], 1, "first node wins the dup");
        assert_eq!(m.item_counts, vec![2, 2], "counts are pre-merge");
    }

    #[test]
    fn t03_top_k_stable_and_k_over_len() {
        let m = merge(
            &MergeStrategy::TopK {
                k: 2,
                score_field: "score".into(),
            },
            &[
                node(1, json!({"items": [{"n": "low", "score": 1.0}]})),
                node(
                    2,
                    json!({"items": [{"n": "hi", "score": 9.0}, {"n": "mid", "score": 5.0}]}),
                ),
            ],
        )
        .expect("merge");
        let items = m.output["items"].as_array().unwrap();
        assert_eq!(items[0]["n"], "hi");
        assert_eq!(items[1]["n"], "mid");
        // k over len keeps all.
        let m = merge(
            &MergeStrategy::TopK {
                k: 99,
                score_field: "score".into(),
            },
            &[node(1, json!({"items": [{"score": 1}]}))],
        )
        .expect("merge");
        assert_eq!(m.output["items"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn t04_rerank_degrades_to_top_k_score() {
        let m = merge(
            &MergeStrategy::Rerank {
                reranker_capability: "llm.rerank".into(),
                top_k: 1,
            },
            &[node(1, json!({"items": [{"score": 1.0}, {"score": 3.0}]}))],
        )
        .expect("merge");
        assert_eq!(m.output["items"].as_array().unwrap().len(), 1);
        assert_eq!(m.output["items"][0]["score"], 3.0);
    }

    #[test]
    fn t05_aggregate_ops_and_no_data() {
        let inputs = [
            node(1, json!({"items": [{"n": 2.0}, {"n": 4.0}]})),
            node(2, json!({"items": [{"n": 6.0}, {"other": true}]})),
        ];
        let sum = merge(
            &MergeStrategy::Aggregate {
                op: AggregateOp::Sum,
                field: "n".into(),
            },
            &inputs,
        )
        .expect("sum");
        assert_eq!(sum.output["value"], 12.0);
        assert_eq!(sum.output["merge"]["count"], 3);
        let mean = merge(
            &MergeStrategy::Aggregate {
                op: AggregateOp::Mean,
                field: "n".into(),
            },
            &inputs,
        )
        .expect("mean");
        assert_eq!(mean.output["value"], 4.0);
        let err = merge(
            &MergeStrategy::Aggregate {
                op: AggregateOp::Mean,
                field: "absent".into(),
            },
            &inputs,
        )
        .expect_err("no data");
        assert!(matches!(err, MergeError::AggregateNoData { .. }));
        // Min/Max over an empty numeric set error like Mean — a
        // silent 0.0 would be indistinguishable from a real extremum
        // of 0 (PR #48 diff review).
        for op in [AggregateOp::Min, AggregateOp::Max] {
            let err = merge(
                &MergeStrategy::Aggregate {
                    op,
                    field: "absent".into(),
                },
                &inputs,
            )
            .expect_err("empty extremum must error");
            assert!(matches!(err, MergeError::AggregateNoData { .. }));
        }
        // Non-empty Min/Max still work.
        let min = merge(
            &MergeStrategy::Aggregate {
                op: AggregateOp::Min,
                field: "n".into(),
            },
            &inputs,
        )
        .expect("min");
        assert_eq!(min.output["value"], 2.0);
    }

    #[test]
    fn t06_custom_is_typed_unsupported() {
        let err = merge(
            &MergeStrategy::Custom {
                capability: "merge.special".into(),
            },
            &[node(1, json!({}))],
        )
        .expect_err("custom");
        assert!(matches!(err, MergeError::CustomUnsupported { .. }));
    }

    #[test]
    fn t07_truncation_reported_and_accounted() {
        let big: Vec<JsonValue> = (0..(MAX_MERGE_ITEMS + 5))
            .map(|i| json!({"i": i}))
            .collect();
        let m = merge(&MergeStrategy::Concat, &[node(1, json!({"items": big}))]).expect("merge");
        assert_eq!(m.output["items"].as_array().unwrap().len(), MAX_MERGE_ITEMS);
        assert_eq!(m.truncated_items, 5);
        assert_eq!(m.output["merge"]["truncated_items"], 5);
        // Accounting invariant: contributed == surfaced + truncated.
        let contributed: u64 = m.item_counts.iter().sum();
        assert_eq!(contributed, MAX_MERGE_ITEMS as u64 + 5);
    }
}
