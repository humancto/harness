//! [`PartialStreamer`] — the daemon's frame sink for streaming
//! capabilities (3.2-stream, ADR-0020).
//!
//! One instance per daemon. It is the `FrameSink` handed to
//! `shell.exec` (`ShellExecCapability::with_frame_sink`) and routes
//! every emitted [`LogFrame`] by where the task came from:
//!
//! - **Locally-issued task** — append straight into the shared
//!   [`PartialBuffers`] ring (no wire hop): `harness run` against self
//!   sees streaming data through the same `GET /tasks/{id}` surface.
//! - **Remotely-issued task** (we are the worker; the
//!   `DispatchRuntime` holds a reply obligation) — enqueue into a
//!   bounded per-task queue ([`PENDING_QUEUE_DEPTH`] frames,
//!   drop-oldest) that a [`FLUSH_INTERVAL_MS`]-periodic flusher drains
//!   into **one** coalesced `PartialResult` per task per tick — at most
//!   ~20 wire sends per second per task. The `PeerNet` sender task
//!   stamps the per-stream seq and signs at write time, exactly like
//!   the other task envelopes.
//!
//! Everything here is fire-and-forget: a full queue drops the oldest
//! frames, a dead connection drops the batch. The terminal
//! `TaskResultMsg` is authoritative — partials are progress telemetry,
//! never recovery state (ADR-0020).

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;

use harness_api::PartialBuffers;
use harness_capabilities::{FrameSink, LogFrame};
use harness_core::{NodeId, PartialResult, TaskId};
use parking_lot::Mutex as ParkingMutex;
use tokio::sync::watch;
use tokio::time::MissedTickBehavior;

use crate::dispatch::DispatchRuntime;
use crate::peer_net::OutboundMsg;

/// Per-task bound on frames waiting for the next wire flush. Beyond it
/// the OLDEST queued frame is dropped (the newest output is the most
/// useful progress signal) and the drop is counted into the next
/// batch's `dropped` field.
pub(crate) const PENDING_QUEUE_DEPTH: usize = 256;

/// Wire-flush cadence: one coalesced `PartialResult` per task per tick
/// → at most 20 sends per second per task.
pub(crate) const FLUSH_INTERVAL_MS: u64 = 50;

/// Raw line bytes drained into a single batch. Lines are ≤ 8 KiB
/// (`shell::MAX_LINE_BYTES`) and the budget is checked before adding a
/// frame, so a batch tops out under ~12 KiB raw — comfortably inside
/// the 64 KiB `harness.task.partial` frame cap even after JSON
/// escaping (ADR-0020). Whatever doesn't fit stays queued for the next
/// tick.
const FLUSH_BUDGET_BYTES: usize = 4096;

struct PendingQueue {
    issuer: NodeId,
    frames: VecDeque<LogFrame>,
    /// Frames dropped (queue overflow) since the last flush.
    dropped: u64,
}

/// One flushable batch: `(issuer, task, frames, dropped_since_last)`.
type Batch = (NodeId, TaskId, Vec<LogFrame>, u64);

/// See the module docs.
pub(crate) struct PartialStreamer {
    local_id: NodeId,
    buffers: Arc<PartialBuffers>,
    /// Set once after the runtime exists (the sink must be constructable
    /// before the registry, which the runtime needs — same shape as
    /// `DispatchRuntime::net`). Unset ⇒ every task routes local.
    dispatch: OnceLock<Weak<DispatchRuntime>>,
    pending: ParkingMutex<HashMap<TaskId, PendingQueue>>,
}

impl PartialStreamer {
    pub(crate) fn new(local_id: NodeId, buffers: Arc<PartialBuffers>) -> Arc<Self> {
        Arc::new(Self {
            local_id,
            buffers,
            dispatch: OnceLock::new(),
            pending: ParkingMutex::new(HashMap::new()),
        })
    }

    /// Wire the back-reference after `DispatchRuntime::new`.
    pub(crate) fn attach_dispatch(&self, dispatch: &Arc<DispatchRuntime>) {
        let _ = self.dispatch.set(Arc::downgrade(dispatch));
    }

    /// The `FrameSink` to hand to streaming capabilities.
    pub(crate) fn sink(self: &Arc<Self>) -> FrameSink {
        let this = self.clone();
        Arc::new(move |task_id, frame| this.on_frame(task_id, frame))
    }

    /// Route one frame. Non-blocking (short lock; no I/O) — called from
    /// the capability's pipe-reader tasks while the child runs.
    pub(crate) fn on_frame(&self, task_id: TaskId, frame: LogFrame) {
        let issuer = self
            .dispatch
            .get()
            .and_then(Weak::upgrade)
            .and_then(|rt| rt.remote_issuer(task_id));
        match issuer {
            None => {
                // Locally-issued task: same ring the API serves; no wire
                // hop (ADR-0020).
                self.buffers
                    .append(task_id, frame.stream.as_str(), frame.line);
            }
            Some(issuer) => {
                let mut pending = self.pending.lock();
                let q = pending.entry(task_id).or_insert_with(|| PendingQueue {
                    issuer,
                    frames: VecDeque::new(),
                    dropped: 0,
                });
                if q.frames.len() >= PENDING_QUEUE_DEPTH {
                    q.frames.pop_front();
                    q.dropped += 1;
                }
                q.frames.push_back(frame);
            }
        }
    }

    /// Drain up to one budgeted batch per pending task. Emptied queues
    /// are removed (a task's entry reappears if more frames arrive);
    /// over-budget remainders stay queued for the next tick.
    pub(crate) fn flush_batches(&self) -> Vec<Batch> {
        let mut out = Vec::new();
        let mut pending = self.pending.lock();
        pending.retain(|task_id, q| {
            if q.frames.is_empty() {
                return false;
            }
            let mut frames = Vec::new();
            let mut bytes = 0usize;
            while let Some(front) = q.frames.front() {
                if !frames.is_empty() && bytes + front.line.len() > FLUSH_BUDGET_BYTES {
                    break;
                }
                bytes += front.line.len();
                // Front exists — the loop guard just matched it.
                if let Some(f) = q.frames.pop_front() {
                    frames.push(f);
                }
            }
            out.push((q.issuer, *task_id, frames, std::mem::take(&mut q.dropped)));
            !q.frames.is_empty()
        });
        out
    }

    /// One flush tick: coalesce and enqueue each batch as an
    /// `OutboundMsg::Partial` toward its issuer. Failures are dropped —
    /// fire-and-forget (ADR-0020).
    pub(crate) fn flush_once(&self) {
        let batches = self.flush_batches();
        if batches.is_empty() {
            return;
        }
        let Some(net) = self
            .dispatch
            .get()
            .and_then(Weak::upgrade)
            .and_then(|rt| rt.net())
        else {
            return;
        };
        for (issuer, task_id, frames, dropped) in batches {
            let frames_json: Vec<serde_json::Value> = frames
                .iter()
                .map(|f| {
                    serde_json::json!({
                        "stream": f.stream.as_str(),
                        "line": f.line,
                    })
                })
                .collect();
            let mut chunk = serde_json::json!({ "frames": frames_json });
            if dropped > 0 {
                chunk["dropped"] = serde_json::json!(dropped);
            }
            let msg = PartialResult {
                task_id,
                node_id: self.local_id,
                seq: 0, // stamped per-stream by the PeerNet sender task
                progress: 0.0,
                output_chunk: chunk,
                sig: harness_core::Signature::from_bytes([0u8; 64]),
            };
            if let Err(err) = net.send_to(issuer, OutboundMsg::Partial(msg)) {
                tracing::debug!(
                    target: "harness.dispatch",
                    task = %task_id.0,
                    %issuer,
                    %err,
                    "partial enqueue failed; dropped (fire-and-forget)"
                );
            }
        }
    }

    /// The flush loop — spawned from the daemon lifecycle.
    pub(crate) async fn run_flush_loop(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) {
        let mut tick = tokio::time::interval(Duration::from_millis(FLUSH_INTERVAL_MS));
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = tick.tick() => self.flush_once(),
                _ = shutdown.changed() => return,
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::peer_net::TaskChannelHandlers;
    use harness_capabilities::{CapabilityRegistry, ShellExecCapability, StreamKind};
    use harness_core::{
        Constraints, ExecutionPolicy, Identity, ResourceHints, RetryPolicy, Signable, Signature,
        Task, TaskAssign, TraceContext,
    };
    use harness_mesh::heartbeat::PeerTable;
    use harness_mesh::{AddedVia, Peer, TrustStore, TrustTier};
    use harness_store::Store;

    fn frame(kind: StreamKind, line: &str) -> LogFrame {
        LogFrame {
            stream: kind,
            line: line.to_string(),
        }
    }

    fn empty_hints() -> ResourceHints {
        ResourceHints {
            cpu_class: harness_core::protocol::CpuClass::Light,
            memory_mb: None,
            gpu_required: false,
            gpu_memory_mb: None,
            network_class: harness_core::protocol::NetworkClass::None,
            disk_io_class: harness_core::protocol::DiskIoClass::None,
            estimated_duration_ms: None,
        }
    }

    fn signed_task(issuer: &Identity, capability: &str, input: serde_json::Value) -> Task {
        let mut t = Task {
            id: TaskId::new_v7(),
            parent: None,
            plan_id: None,
            capability: capability.to_string(),
            input,
            constraints: Constraints::default(),
            retry: RetryPolicy::default(),
            execution: ExecutionPolicy::default(),
            resource_hints: empty_hints(),
            trace_ctx: TraceContext::default(),
            issued_by: issuer.node_id(),
            issued_at: 1_700_000_000_000,
            tags: vec![],
            sig: Signature::from_bytes([0u8; 64]),
        };
        t.sign(issuer).expect("sign");
        t
    }

    #[test]
    fn local_frames_go_straight_to_the_ring() {
        // No dispatch attached at all — every task is local.
        let buffers = Arc::new(PartialBuffers::new());
        let streamer = PartialStreamer::new(NodeId::from_bytes([1; 16]), buffers.clone());
        let task = TaskId::new_v7();
        streamer.on_frame(task, frame(StreamKind::Stdout, "hello"));
        streamer.on_frame(task, frame(StreamKind::Stderr, "oops"));

        let frames = buffers.frames(task);
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].stream, "stdout");
        assert_eq!(frames[0].line, "hello");
        assert_eq!(frames[1].stream, "stderr");
        // Nothing queued for the wire.
        assert!(streamer.flush_batches().is_empty());
    }

    /// A worker-side fixture: a `DispatchRuntime` with a reply
    /// obligation for a remotely-assigned task, attached to a streamer.
    struct RemoteFixture {
        streamer: Arc<PartialStreamer>,
        _runtime: Arc<DispatchRuntime>,
        remote: Arc<Identity>,
        task_id: TaskId,
        buffers: Arc<PartialBuffers>,
        _tmp: tempfile::TempDir,
    }

    fn remote_fixture() -> RemoteFixture {
        let tmp = tempfile::tempdir().expect("tempdir");
        let local = Arc::new(harness_mesh::identity::init_or_load(tmp.path()).expect("identity"));
        let remote = Arc::new(Identity::generate());
        let trust = TrustStore::open(tmp.path(), local.node_id()).expect("trust");
        trust
            .add(Peer {
                node_id: remote.node_id(),
                pubkey: *remote.public_key(),
                hostname: "remote-node".into(),
                tier: TrustTier::Default,
                added_at: 0,
                added_via: AddedVia::Static,
            })
            .expect("trust add");
        let store = Store::open_memory().expect("store");
        let runtime = DispatchRuntime::new(
            store,
            local.clone(),
            CapabilityRegistry::new(),
            harness_orchestrator::Dispatcher::new(),
            trust,
            PeerTable::new(),
            Arc::new(harness_vault::PlaintextStore::empty()),
        );
        let buffers = Arc::new(PartialBuffers::new());
        let streamer = PartialStreamer::new(local.node_id(), buffers.clone());
        streamer.attach_dispatch(&runtime);

        // Ingest a remote assignment so the runtime owes a reply — the
        // marker `remote_issuer` keys off.
        let task = signed_task(&remote, "shell.exec", serde_json::json!({"cmd": "x"}));
        let task_id = task.id;
        runtime.on_assign(
            remote.node_id(),
            TaskAssign {
                seq: 0,
                lease_id: harness_core::LeaseId::new_v7(),
                task,
                assigned_by: remote.node_id(),
                lease_expires_at: 0,
                sig: Signature::from_bytes([0u8; 64]),
            },
        );
        RemoteFixture {
            streamer,
            _runtime: runtime,
            remote,
            task_id,
            buffers,
            _tmp: tmp,
        }
    }

    #[test]
    fn remote_frames_queue_and_coalesce_into_one_batch() {
        let f = remote_fixture();
        for i in 0..5 {
            f.streamer
                .on_frame(f.task_id, frame(StreamKind::Stdout, &format!("line-{i}")));
        }
        // Remote frames must NOT hit the local ring — the issuer's ring
        // is fed on the other side of the wire.
        assert!(f.buffers.frames(f.task_id).is_empty());

        let batches = f.streamer.flush_batches();
        assert_eq!(batches.len(), 1, "5 small lines coalesce into 1 batch");
        let (issuer, task_id, frames, dropped) = &batches[0];
        assert_eq!(*issuer, f.remote.node_id());
        assert_eq!(*task_id, f.task_id);
        assert_eq!(frames.len(), 5);
        assert_eq!(frames[0].line, "line-0");
        assert_eq!(frames[4].line, "line-4");
        assert_eq!(*dropped, 0);
        // Queue is now empty and its entry gone.
        assert!(f.streamer.flush_batches().is_empty());
    }

    #[test]
    fn pending_queue_drops_oldest_beyond_depth() {
        let f = remote_fixture();
        let extra = 50;
        for i in 0..(PENDING_QUEUE_DEPTH + extra) {
            f.streamer
                .on_frame(f.task_id, frame(StreamKind::Stdout, &format!("l{i}")));
        }
        // Drain everything (multiple budgeted batches).
        let mut all_frames = Vec::new();
        let mut dropped_total = 0;
        loop {
            let batches = f.streamer.flush_batches();
            if batches.is_empty() {
                break;
            }
            for (_, _, frames, dropped) in batches {
                all_frames.extend(frames);
                dropped_total += dropped;
            }
        }
        assert_eq!(all_frames.len(), PENDING_QUEUE_DEPTH);
        assert_eq!(dropped_total, extra as u64);
        assert_eq!(
            all_frames[0].line,
            format!("l{extra}"),
            "oldest frames must be the ones dropped"
        );
    }

    #[test]
    fn flush_respects_byte_budget_across_ticks() {
        let f = remote_fixture();
        // 4 lines of 3 KiB: budget 4 KiB → 1 line per batch after the
        // first fills past the budget check.
        let big = "x".repeat(3 * 1024);
        for _ in 0..4 {
            f.streamer
                .on_frame(f.task_id, frame(StreamKind::Stdout, &big));
        }
        let first = f.streamer.flush_batches();
        assert_eq!(first.len(), 1);
        assert_eq!(
            first[0].2.len(),
            1,
            "second 3 KiB line must not fit the 4 KiB budget"
        );
        let mut remaining = 0;
        loop {
            let batches = f.streamer.flush_batches();
            if batches.is_empty() {
                break;
            }
            remaining += batches[0].2.len();
        }
        assert_eq!(remaining, 3, "leftover lines drain on later ticks");
    }

    // ── local-path integration: a locally-executed streaming task
    //    populates the shared ring via the executor (no wire hop).
    #[tokio::test]
    async fn local_shell_task_streams_into_the_ring() {
        let policy = harness_policy::load_from_str(
            r#"
[shell]
allow = [{ cmd = "/bin/sh", any_args = true }]
"#,
        )
        .expect("policy");
        let policy = Arc::new(harness_policy::PolicyEngine::new(policy));

        let buffers = Arc::new(PartialBuffers::new());
        let local_node = NodeId::from_bytes([7; 16]);
        let streamer = PartialStreamer::new(local_node, buffers.clone());

        let registry = CapabilityRegistry::new();
        registry
            .register(Arc::new(ShellExecCapability::with_frame_sink(
                policy,
                streamer.sink(),
            )))
            .expect("register");

        let store = Store::open_memory().expect("store");
        let exec = crate::executor::LocalExecutor::new(
            store.clone(),
            registry,
            local_node,
            Arc::from("self"),
            1,
        );

        let issuer = Identity::generate();
        let task = signed_task(
            &issuer,
            "shell.exec",
            serde_json::json!({
                "cmd": "/bin/sh",
                "args": ["-c", "echo streamed-one; echo streamed-two"],
            }),
        );
        let task_id = task.id;
        store.insert_task(&task).expect("insert");
        assert!(store.try_dispatch_task(task_id, local_node).expect("seed"));

        exec.poll_once().await;
        for _ in 0..200 {
            let st = store.task_state(task_id).expect("state").expect("present");
            if matches!(
                st,
                harness_store::TaskState::Done | harness_store::TaskState::Failed
            ) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            store.task_state(task_id).expect("state"),
            Some(harness_store::TaskState::Done)
        );

        let frames = buffers.frames(task_id);
        let lines: Vec<&str> = frames.iter().map(|f| f.line.as_str()).collect();
        assert_eq!(lines, vec!["streamed-one", "streamed-two"]);
        assert!(frames.iter().all(|f| f.stream == "stdout"));
    }
}
