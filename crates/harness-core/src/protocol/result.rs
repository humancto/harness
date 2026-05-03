//! Result envelope (PRD §13.4). Either a `FinalResult` (one terminal answer)
//! or a `PartialResult` (federated/streaming progress).

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::identity::{NodeId, Signature};
use crate::ids::TaskId;
use crate::protocol::signable::Signable;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Status {
    Ok,
    Failed,
    TimedOut,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum NodeStatus {
    Ok,
    Failed,
    TimedOut,
    Skipped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogLine {
    /// Unix milliseconds.
    pub ts: u64,
    pub level: LogLevel,
    pub msg: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Cost {
    pub tokens_in: u64,
    pub tokens_out: u64,
    pub usd: f64,
    pub wall_ms: u64,
    pub node_id: NodeId,
}

/// Per-node contribution within a federated result (PRD §13.4).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeContribution {
    pub node_id: NodeId,
    pub status: NodeStatus,
    pub duration_ms: u64,
    pub item_count: u64,
}

/// Terminal result for a single task (or merged federated result).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FinalResult {
    pub task_id: TaskId,
    pub node_id: NodeId,
    /// Unix milliseconds.
    pub started_at: u64,
    pub finished_at: u64,
    pub status: Status,
    pub output: JsonValue,
    pub cost: Cost,
    pub logs: Vec<LogLine>,
    pub provenance: Vec<NodeContribution>,
    pub sig: Signature,
}

impl Signable for FinalResult {
    fn sig_field_mut(&mut self) -> &mut Signature {
        &mut self.sig
    }
    fn sig_field(&self) -> &Signature {
        &self.sig
    }
}

/// Streaming progress envelope. Issued by a worker mid-task or by a brain
/// while a federated dispatch is still draining sub-results.
///
/// `progress` is in the unit interval `[0.0, 1.0]`; `f32` is sufficient and
/// keeps the envelope compact. `output_chunk` may be `JsonValue::Null` for
/// "progress tick only."
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PartialResult {
    pub task_id: TaskId,
    pub node_id: NodeId,
    /// Monotonic per-task within a worker. Dispatcher uses it to drop
    /// out-of-order partials. Replay-protection at the transport layer is
    /// handled by `Sequenced` in `harness-mesh`.
    pub seq: u64,
    pub progress: f32,
    pub output_chunk: JsonValue,
    pub sig: Signature,
}

impl Signable for PartialResult {
    fn sig_field_mut(&mut self) -> &mut Signature {
        &mut self.sig
    }
    fn sig_field(&self) -> &Signature {
        &self.sig
    }
}

/// Either-or wire shape (PRD §13.4 — `enum TaskResult { Final, Partial }`).
///
/// Externally tagged (`#[serde(tag = "kind")]`) for the same reasons
/// `Cardinality` is — flat, self-describing, evolution-friendly.
///
/// **Not `Signable` itself.** The variant payload carries the signature.
/// The dispatcher signs the payload, then wraps in `TaskResult`; the
/// receiver unwraps and verifies on the payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum TaskResult {
    Final(FinalResult),
    Partial(PartialResult),
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::identity::Identity;

    fn sample_cost() -> Cost {
        Cost {
            tokens_in: 0,
            tokens_out: 0,
            usd: 0.0,
            wall_ms: 100,
            node_id: NodeId::from_bytes([0x22; 16]),
        }
    }

    fn sample_final() -> FinalResult {
        FinalResult {
            task_id: TaskId::new_v7(),
            node_id: NodeId::from_bytes([0x11; 16]),
            started_at: 0,
            finished_at: 100,
            status: Status::Ok,
            output: serde_json::json!({"echoed": "hello"}),
            cost: sample_cost(),
            logs: vec![],
            provenance: vec![],
            sig: Signature::from_bytes([0u8; 64]),
        }
    }

    fn sample_partial() -> PartialResult {
        PartialResult {
            task_id: TaskId::new_v7(),
            node_id: NodeId::from_bytes([0x11; 16]),
            seq: 0,
            progress: 0.5,
            output_chunk: JsonValue::Null,
            sig: Signature::from_bytes([0u8; 64]),
        }
    }

    fn round_trip<T: Serialize + for<'de> Deserialize<'de> + PartialEq + std::fmt::Debug>(
        v: &T,
    ) -> T {
        let mut buf = Vec::new();
        ciborium::ser::into_writer(v, &mut buf).expect("encode");
        ciborium::de::from_reader(buf.as_slice()).expect("decode")
    }

    #[test]
    fn final_result_round_trip() {
        let r = sample_final();
        assert_eq!(r, round_trip(&r));
    }

    #[test]
    fn partial_result_round_trip() {
        let r = sample_partial();
        assert_eq!(r, round_trip(&r));
    }

    #[test]
    fn final_result_with_logs_and_provenance_round_trip() {
        let r = FinalResult {
            logs: vec![LogLine {
                ts: 1,
                level: LogLevel::Info,
                msg: "step done".into(),
            }],
            provenance: vec![NodeContribution {
                node_id: NodeId::from_bytes([0xAA; 16]),
                status: NodeStatus::Ok,
                duration_ms: 50,
                item_count: 7,
            }],
            ..sample_final()
        };
        assert_eq!(r, round_trip(&r));
    }

    #[test]
    fn task_result_final_round_trip() {
        let v = TaskResult::Final(sample_final());
        assert_eq!(v, round_trip(&v));
    }

    #[test]
    fn task_result_partial_round_trip() {
        let v = TaskResult::Partial(sample_partial());
        assert_eq!(v, round_trip(&v));
    }

    #[test]
    fn task_result_external_tag_in_json() {
        let final_json = serde_json::to_string(&TaskResult::Final(sample_final())).unwrap();
        assert!(final_json.contains("\"kind\":\"final\""));
        let partial_json = serde_json::to_string(&TaskResult::Partial(sample_partial())).unwrap();
        assert!(partial_json.contains("\"kind\":\"partial\""));
    }

    #[test]
    fn final_result_sign_then_verify() {
        let id = Identity::generate();
        let mut r = sample_final();
        r.node_id = id.public_key().node_id();
        r.sign(&id).expect("sign");
        assert!(r.verify_signature(id.public_key()).is_ok());
    }

    #[test]
    fn partial_result_tampered_fails_verify() {
        let id = Identity::generate();
        let mut r = sample_partial();
        r.node_id = id.public_key().node_id();
        r.sign(&id).expect("sign");
        r.progress = 0.99;
        assert!(r.verify_signature(id.public_key()).is_err());
    }

    #[test]
    fn each_status_variant_round_trips() {
        for s in [
            Status::Ok,
            Status::Failed,
            Status::TimedOut,
            Status::Cancelled,
        ] {
            let r = FinalResult {
                status: s,
                ..sample_final()
            };
            assert_eq!(r, round_trip(&r));
        }
    }

    #[test]
    fn each_log_level_round_trips() {
        for level in [
            LogLevel::Trace,
            LogLevel::Debug,
            LogLevel::Info,
            LogLevel::Warn,
            LogLevel::Error,
        ] {
            let line = LogLine {
                ts: 1,
                level,
                msg: "m".into(),
            };
            assert_eq!(line, round_trip(&line));
        }
    }

    #[test]
    fn each_node_status_variant_round_trips() {
        for s in [
            NodeStatus::Ok,
            NodeStatus::Failed,
            NodeStatus::TimedOut,
            NodeStatus::Skipped,
        ] {
            let n = NodeContribution {
                node_id: NodeId::from_bytes([0; 16]),
                status: s,
                duration_ms: 1,
                item_count: 0,
            };
            assert_eq!(n, round_trip(&n));
        }
    }
}
