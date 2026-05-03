//! Capability cardinality and federated merge strategies (PRD §13.2, §14.6).

use serde::{Deserialize, Serialize};

/// How a capability invocation should be routed.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum Cardinality {
    /// Any eligible node is acceptable. Routed by the dispatcher to the
    /// least-loaded fit.
    Anyone,
    /// Routed by the value of the named field in the task input — the node
    /// owning that scope handles it. Field name is validated at capability
    /// registration (Phase 2+).
    Owner { scope_field: String },
    /// Fanned out to every eligible node; results are merged.
    Federated {
        merge: MergeStrategy,
        on_node_failure: PartialPolicy,
    },
}

/// How to merge federated sub-results into a single final result.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum MergeStrategy {
    Concat,
    Dedupe {
        key: String,
    },
    TopK {
        k: usize,
        score_field: String,
    },
    Rerank {
        reranker_capability: String,
        top_k: usize,
    },
    Aggregate {
        op: AggregateOp,
        field: String,
    },
    Custom {
        capability: String,
    },
}

/// Aggregation operators for [`MergeStrategy::Aggregate`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum AggregateOp {
    Sum,
    Min,
    Max,
    Mean,
    Count,
}

/// What to do when one or more nodes in a federated dispatch fail or time
/// out. Reused by `ExecutionPolicy::on_partial` (item 2.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum PartialPolicy {
    FailFast,
    ReturnPartial,
    Wait,
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    fn round_trip<T>(value: &T) -> T
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        let mut buf = Vec::new();
        ciborium::ser::into_writer(value, &mut buf).expect("serialize");
        ciborium::de::from_reader(buf.as_slice()).expect("deserialize")
    }

    #[test]
    fn cardinality_anyone_round_trip() {
        let c = Cardinality::Anyone;
        assert_eq!(c, round_trip(&c));
    }

    #[test]
    fn cardinality_owner_round_trip() {
        let c = Cardinality::Owner {
            scope_field: "directory".into(),
        };
        assert_eq!(c, round_trip(&c));
    }

    #[test]
    fn cardinality_federated_round_trip() {
        let c = Cardinality::Federated {
            merge: MergeStrategy::TopK {
                k: 16,
                score_field: "score".into(),
            },
            on_node_failure: PartialPolicy::ReturnPartial,
        };
        assert_eq!(c, round_trip(&c));
    }

    #[test]
    fn merge_strategy_each_variant_round_trip() {
        let cases = [
            MergeStrategy::Concat,
            MergeStrategy::Dedupe { key: "id".into() },
            MergeStrategy::TopK {
                k: 5,
                score_field: "score".into(),
            },
            MergeStrategy::Rerank {
                reranker_capability: "rerank.v1".into(),
                top_k: 10,
            },
            MergeStrategy::Aggregate {
                op: AggregateOp::Sum,
                field: "count".into(),
            },
            MergeStrategy::Custom {
                capability: "merge.custom".into(),
            },
        ];
        for c in &cases {
            assert_eq!(*c, round_trip(c));
        }
    }

    #[test]
    fn aggregate_op_round_trip() {
        for op in [
            AggregateOp::Sum,
            AggregateOp::Min,
            AggregateOp::Max,
            AggregateOp::Mean,
            AggregateOp::Count,
        ] {
            assert_eq!(op, round_trip(&op));
        }
    }

    #[test]
    fn partial_policy_round_trip() {
        for p in [
            PartialPolicy::FailFast,
            PartialPolicy::ReturnPartial,
            PartialPolicy::Wait,
        ] {
            assert_eq!(p, round_trip(&p));
        }
    }
}
