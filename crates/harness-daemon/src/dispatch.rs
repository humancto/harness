//! `DispatchRuntime` — the 3.3-fanout dispatch service (PR-A2).
//!
//! One runtime per daemon plays both roles of the ADR-0009 seam:
//!
//! **Issuer side** — polls `Submitted` (unassigned) tasks, routes them
//! via `Dispatcher::eligible_with_rr` over the live mesh (self included),
//! CASes `Submitted → Dispatched`, and for remote nodes mints a lease and
//! enqueues a `TaskAssign` through the `PeerNet`. Inbound `TaskClaim` /
//! `TaskResultMsg` land here through [`TaskChannelHandlers`]. A 1 s
//! `expire_pass` re-dispatches lease-expired tasks until
//! `retry.max_attempts`, then terminates them as `Expired`. Tasks that
//! stay undispatchable past their deadline window are failed terminally
//! via the documented `Submitted → Failed` supervisor hop (ADR-0017).
//!
//! **Worker side** — `on_assign` verifies the issuer (connection peer ==
//! `assigned_by` == `task.issued_by`, inner task signature against the
//! trust store), ingests the task at `Dispatched(assigned = self)` for
//! the local executor, acks with `TaskClaim`, and replies with a signed
//! `TaskResultMsg` when the executor reports the task terminal. Ingest is
//! idempotent: a re-delivered assign for an already-terminal row
//! immediately re-sends the stored result under the new lease id.
//!
//! Lease TTLs dominate expected runtime:
//! `ttl = max(lease_ms, timeout_ms + 15 s)` — worker-driven lease
//! extension is Phase 4.6 (ADR-0017 / review R2). `RetryPolicy` backoff
//! fields are unused until 4.6 (R16).

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, Weak};
use std::time::{Duration, Instant};

use harness_capabilities::CapabilityRegistry;
use harness_core::{
    Cardinality, Cost, FinalResult, Identity, LeaseId, NodeId, PublicKey, ReplicatedState,
    ReplicatedTaskState, Signable, Status, Task, TaskAssign, TaskClaim, TaskId, TaskResultMsg,
};
use harness_mesh::heartbeat::{PeerTable, PEER_TIMEOUT};
use harness_mesh::TrustStore;
use harness_orchestrator::{DispatchError, DispatchPlan, Dispatcher, LiveSet, RoundRobin};
use harness_store::{Store, TaskState};
use parking_lot::Mutex as ParkingMutex;
use tokio::sync::watch;
use tokio::time::MissedTickBehavior;

use crate::peer_net::{OutboundMsg, PeerNet, SendToError, TaskChannelHandlers};

const DISPATCH_POLL_MS: u64 = 100;
const DISPATCH_BATCH: usize = 16;
const EXPIRE_PASS_MS: u64 = 1000;
const EXPIRE_BATCH: u32 = 64;
/// Slack added on top of `execution.timeout_ms` when sizing lease TTLs
/// (claim + transport + result latency). Test builds shrink it so lease
/// expiry paths run in CI time.
#[cfg(not(test))]
const LEASE_SLACK_MS: u64 = 15_000;
#[cfg(test)]
const LEASE_SLACK_MS: u64 = 700;
/// A task with no eligible node keeps retrying for this long (or until
/// `constraints.deadline`), then fails terminally.
#[cfg(not(test))]
const ELIGIBILITY_WINDOW_MS: u64 = 30_000;
#[cfg(test)]
const ELIGIBILITY_WINDOW_MS: u64 = 2_000;

/// Liveness view for routing: self is always live; peers are live while
/// their last heartbeat is younger than [`PEER_TIMEOUT`].
pub(crate) struct MeshLiveSet {
    pub table: PeerTable,
    pub timeout: Duration,
    pub self_id: NodeId,
}

impl LiveSet for MeshLiveSet {
    fn is_live(&self, node: &NodeId) -> bool {
        *node == self.self_id || self.table.is_live(node, self.timeout)
    }
}

struct ReplyObligation {
    issuer: NodeId,
    lease_id: LeaseId,
}

pub(crate) struct DispatchRuntime {
    store: Store,
    identity: Arc<Identity>,
    local_id: NodeId,
    registry: CapabilityRegistry,
    dispatcher: Dispatcher,
    rr: RoundRobin,
    /// Capabilities whose RR cursor has been seeded from the store.
    rr_seeded: ParkingMutex<std::collections::HashSet<String>>,
    trust: TrustStore,
    peers: PeerTable,
    /// Set once after `PeerNet::new` (the net holds us as its handlers —
    /// a `Weak` back-reference avoids the cycle).
    net: OnceLock<Weak<PeerNet>>,
    /// Worker side: tasks we owe a result reply for.
    reply: ParkingMutex<HashMap<TaskId, ReplyObligation>>,
    /// Issuer side: first time each task failed eligibility.
    elig_failures: ParkingMutex<HashMap<TaskId, Instant>>,
}

impl DispatchRuntime {
    pub(crate) fn new(
        store: Store,
        identity: Arc<Identity>,
        registry: CapabilityRegistry,
        dispatcher: Dispatcher,
        trust: TrustStore,
        peers: PeerTable,
    ) -> Arc<Self> {
        let local_id = identity.node_id();
        Arc::new(Self {
            store,
            identity,
            local_id,
            registry,
            dispatcher,
            rr: RoundRobin::new(),
            rr_seeded: ParkingMutex::new(std::collections::HashSet::new()),
            trust,
            peers,
            net: OnceLock::new(),
            reply: ParkingMutex::new(HashMap::new()),
            elig_failures: ParkingMutex::new(HashMap::new()),
        })
    }

    /// Test introspection: how many result replies this worker owes.
    #[cfg(test)]
    pub(crate) fn reply_obligations_len(&self) -> usize {
        self.reply.lock().len()
    }

    /// Wire the back-reference after `PeerNet::new`.
    pub(crate) fn attach_net(&self, net: &Arc<PeerNet>) {
        let _ = self.net.set(Arc::downgrade(net));
    }

    fn net(&self) -> Option<Arc<PeerNet>> {
        self.net.get().and_then(Weak::upgrade)
    }

    fn live_set(&self) -> MeshLiveSet {
        MeshLiveSet {
            table: self.peers.clone(),
            timeout: PEER_TIMEOUT,
            self_id: self.local_id,
        }
    }

    /// The dispatch loop: poll `Submitted` and route.
    pub(crate) async fn run_dispatch_loop(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) {
        let mut tick = tokio::time::interval(Duration::from_millis(DISPATCH_POLL_MS));
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = tick.tick() => self.poll_submitted_once(),
                _ = shutdown.changed() => return,
            }
        }
    }

    /// The lease reaper: re-dispatch or terminate expired leases.
    pub(crate) async fn run_expire_loop(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) {
        let mut tick = tokio::time::interval(Duration::from_millis(EXPIRE_PASS_MS));
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = tick.tick() => self.expire_pass(),
                _ = shutdown.changed() => return,
            }
        }
    }

    /// Worker reply pump: send `TaskResultMsg` for tasks we owe.
    pub(crate) async fn run_reply_pump(
        self: Arc<Self>,
        mut terminal_rx: tokio::sync::broadcast::Receiver<TaskId>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        loop {
            tokio::select! {
                r = terminal_rx.recv() => match r {
                    Ok(task_id) => self.try_reply(task_id),
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        // Missed events are recovered by the issuer's
                        // lease expiry + assign-time terminal-resend.
                        continue;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                },
                _ = shutdown.changed() => return,
            }
        }
    }

    // ------------------------------------------------------------------
    // issuer side
    // ------------------------------------------------------------------

    pub(crate) fn poll_submitted_once(&self) {
        let rows = match self
            .store
            .list_tasks_by_state_assigned(TaskState::Submitted, None)
        {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(target: "harness.dispatch", ?e, "list submitted");
                return;
            }
        };
        for row in rows.into_iter().take(DISPATCH_BATCH) {
            let task = match self.store.load_task(row.id) {
                Ok(Some(t)) => t,
                Ok(None) => continue,
                Err(e) => {
                    tracing::warn!(target: "harness.dispatch", ?e, "load_task");
                    continue;
                }
            };
            let cardinality = self.cardinality_for(&task.capability);
            self.seed_rr_cursor(&task.capability);
            let live = self.live_set();
            match self
                .dispatcher
                .eligible_with_rr(&task, &cardinality, &live, &self.rr)
            {
                Ok(DispatchPlan::Single { node }) => self.dispatch_to(&task, node),
                Ok(DispatchPlan::Federated { nodes }) => {
                    // Real federated fan-out + merge is Phase 4.5. Until
                    // then a federated-cardinality task runs on the first
                    // eligible node (ADR-0017).
                    if let Some(node) = nodes.first().copied() {
                        tracing::debug!(
                            target: "harness.dispatch",
                            task = %task.id.0,
                            "federated cardinality routed to single node until 4.5"
                        );
                        self.dispatch_to(&task, node);
                    }
                }
                Ok(_) => {
                    // `DispatchPlan` is non_exhaustive; unknown plans are
                    // a routing bug, not a task failure.
                    tracing::error!(target: "harness.dispatch", task = %task.id.0, "unknown dispatch plan");
                }
                Err(err) => self.eligibility_failure(&task, &err),
            }
        }
    }

    /// Cardinality from the local registry; remote-only capabilities
    /// (advertised by peers, not installed here) default to `Anyone`
    /// (ADR-0017).
    fn cardinality_for(&self, capability: &str) -> Cardinality {
        self.registry
            .get(capability)
            .map_or(Cardinality::Anyone, |c| c.manifest().cardinality)
    }

    fn seed_rr_cursor(&self, capability: &str) {
        {
            let seeded = self.rr_seeded.lock();
            if seeded.contains(capability) {
                return;
            }
        }
        if let Ok(Some(prev)) = self.store.last_dispatched(capability) {
            self.rr.seed(capability, prev);
        }
        self.rr_seeded.lock().insert(capability.to_string());
    }

    fn dispatch_to(&self, task: &Task, node: NodeId) {
        self.elig_failures.lock().remove(&task.id);
        if let Err(e) = self.store.set_last_dispatched(&task.capability, node) {
            tracing::warn!(target: "harness.dispatch", ?e, "persist rr cursor");
        }
        match self.store.try_dispatch_task(task.id, node) {
            Ok(true) => {}
            Ok(false) => return, // lost the race (cancelled / already routed)
            Err(e) => {
                tracing::warn!(target: "harness.dispatch", ?e, "try_dispatch_task");
                return;
            }
        }
        if node == self.local_id {
            // Local: the executor picks it up from Dispatched(self).
            return;
        }
        // Remote: lease after winning the CAS (ADR-0017 / R12).
        let attempt = self
            .store
            .list_leases_for_task(task.id)
            .map(|l| u32::try_from(l.len()).unwrap_or(u32::MAX).saturating_add(1))
            .unwrap_or(1);
        let ttl = lease_ttl_ms(task);
        let lease = match self.store.create_lease(task.id, node, ttl, attempt) {
            Ok(l) => l,
            Err(e) => {
                // A store error minting the lease is fatal for this task:
                // `Dispatched → Submitted` is not a legal hop without a
                // lease to expire, so the task terminates as Cancelled
                // WITH a visible reason (review M2 — no silent terminal).
                tracing::warn!(target: "harness.dispatch", ?e, "create_lease");
                let msg = format!("dispatch aborted: lease creation failed: {e}");
                let _ = self.store.try_transition_task(
                    task.id,
                    TaskState::Dispatched,
                    TaskState::Cancelled,
                );
                let now_ms = now_unix_ms();
                if let Err(we) =
                    self.store
                        .write_task_result_failed(task.id, &msg, now_ms, self.local_id)
                {
                    tracing::warn!(target: "harness.dispatch", ?we, "write lease-failure result");
                }
                let _ = self.store.replica_apply_local(&ReplicatedTaskState {
                    task_id: task.id,
                    state: ReplicatedState::Cancelled,
                    at_ms: now_ms,
                    source: self.local_id,
                    output_preview: Some(msg.into_bytes().into_iter().take(256).collect()),
                });
                return;
            }
        };
        let assign = TaskAssign {
            seq: 0, // stamped per-stream by the PeerNet sender task
            lease_id: lease.lease_id,
            task: task.clone(),
            assigned_by: self.local_id,
            lease_expires_at: lease.expires_at,
            sig: harness_core::Signature::from_bytes([0u8; 64]),
        };
        let send = self
            .net()
            .ok_or(SendToError::NoConnection)
            .and_then(|net| net.send_to(node, OutboundMsg::Assign(assign)));
        if let Err(err) = send {
            tracing::warn!(
                target: "harness.dispatch",
                task = %task.id.0,
                %node,
                %err,
                "assign enqueue failed; resetting for re-dispatch"
            );
            let _ = self.store.expire_and_reset_task(lease.lease_id);
        }
    }

    fn eligibility_failure(&self, task: &Task, err: &DispatchError) {
        let now = Instant::now();
        let first = *self.elig_failures.lock().entry(task.id).or_insert(now);
        let window_expired =
            now.duration_since(first) >= Duration::from_millis(ELIGIBILITY_WINDOW_MS);
        let deadline_expired = task
            .constraints
            .deadline
            .is_some_and(|d| now_unix_ms() >= d);
        if !(window_expired || deadline_expired) {
            return; // keep retrying next poll
        }
        let msg = format!("undispatchable: {err}");
        if let Ok(true) =
            self.store
                .try_transition_task(task.id, TaskState::Submitted, TaskState::Failed)
        {
            {
                tracing::warn!(target: "harness.dispatch", task = %task.id.0, %msg, "task failed terminally");
                let now_ms = now_unix_ms();
                if let Err(e) =
                    self.store
                        .write_task_result_failed(task.id, &msg, now_ms, self.local_id)
                {
                    tracing::warn!(target: "harness.dispatch", ?e, "write undispatchable result");
                }
                let _ = self.store.replica_apply_local(&ReplicatedTaskState {
                    task_id: task.id,
                    state: ReplicatedState::Failed,
                    at_ms: now_ms,
                    source: self.local_id,
                    output_preview: Some(msg.into_bytes().into_iter().take(256).collect()),
                });
            }
        }
        self.elig_failures.lock().remove(&task.id);
    }

    pub(crate) fn expire_pass(&self) {
        let now = now_unix_ms();
        let expired = match self.store.find_expired(now, EXPIRE_BATCH) {
            Ok(l) => l,
            Err(e) => {
                tracing::warn!(target: "harness.dispatch", ?e, "find_expired");
                return;
            }
        };
        for lease in expired {
            let max_attempts = self
                .store
                .load_task(lease.task_id)
                .ok()
                .flatten()
                .map_or(3, |t| u32::from(t.retry.max_attempts));
            if lease.attempt >= max_attempts {
                // Terminal expiry joins the lease-CAS discipline (review
                // M1): win `pending|claimed → expired` FIRST. Losing
                // means a result completed the lease between
                // `find_expired`'s snapshot and now — that result owns
                // the terminal state; write nothing.
                match self.store.try_expire_lease(lease.lease_id) {
                    Ok(true) => {}
                    Ok(false) => {
                        tracing::debug!(
                            target: "harness.dispatch",
                            task = %lease.task_id.0,
                            "expiry lost the lease CAS to an in-flight result; skipping"
                        );
                        continue;
                    }
                    Err(e) => {
                        tracing::warn!(target: "harness.dispatch", ?e, "try_expire_lease");
                        continue;
                    }
                }
                for from in [
                    TaskState::Dispatched,
                    TaskState::Claimed,
                    TaskState::Running,
                ] {
                    if let Ok(true) =
                        self.store
                            .try_transition_task(lease.task_id, from, TaskState::Expired)
                    {
                        break;
                    }
                }
                let msg = format!("lease expired after {} attempts", lease.attempt);
                tracing::warn!(target: "harness.dispatch", task = %lease.task_id.0, %msg, "task expired terminally");
                if let Err(e) =
                    self.store
                        .write_task_result_failed(lease.task_id, &msg, now, self.local_id)
                {
                    tracing::warn!(target: "harness.dispatch", ?e, "write expired result");
                }
                let _ = self.store.replica_apply_local(&ReplicatedTaskState {
                    task_id: lease.task_id,
                    state: ReplicatedState::Expired,
                    at_ms: now,
                    source: self.local_id,
                    output_preview: Some(msg.into_bytes().into_iter().take(256).collect()),
                });
            } else {
                tracing::info!(
                    target: "harness.dispatch",
                    task = %lease.task_id.0,
                    attempt = lease.attempt,
                    "lease expired; resetting for re-dispatch"
                );
                let _ = self.store.expire_and_reset_task(lease.lease_id);
            }
        }
    }

    fn trusted_pubkey(&self, node: NodeId) -> Option<PublicKey> {
        self.trust
            .all_peers()
            .into_iter()
            .find(|p| p.node_id == node)
            .map(|p| p.pubkey)
    }

    /// Issuer side: walk the local row from wherever it is to `Running`
    /// (synthetic hops — ADR-0017), then to the terminal.
    fn finish_local_row(&self, task_id: TaskId, terminal: TaskState) {
        for (from, to) in [
            (TaskState::Dispatched, TaskState::Claimed),
            (TaskState::Claimed, TaskState::Running),
            (TaskState::Running, terminal),
        ] {
            match self.store.try_transition_task(task_id, from, to) {
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(target: "harness.dispatch", ?e, "finish_local_row hop");
                    return;
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // worker side
    // ------------------------------------------------------------------

    fn try_reply(&self, task_id: TaskId) {
        let Some(obligation) = self.reply.lock().remove(&task_id) else {
            return;
        };
        self.send_result(task_id, obligation.issuer, obligation.lease_id);
    }

    fn send_result(&self, task_id: TaskId, issuer: NodeId, lease_id: LeaseId) {
        let Some(result) = self.build_final_result(task_id) else {
            tracing::warn!(target: "harness.dispatch", task = %task_id.0, "no stored result to reply with");
            return;
        };
        let msg = TaskResultMsg {
            seq: 0, // stamped per-stream by the sender task
            lease_id,
            result,
            sig: harness_core::Signature::from_bytes([0u8; 64]),
        };
        let send = self
            .net()
            .ok_or(SendToError::NoConnection)
            .and_then(|net| net.send_to(issuer, OutboundMsg::Result(msg)));
        if let Err(err) = send {
            // The issuer's lease expiry re-assigns; the assign-time
            // terminal-resend then covers this loss (ADR-0017).
            tracing::warn!(
                target: "harness.dispatch",
                task = %task_id.0,
                %issuer,
                %err,
                "result enqueue failed; issuer will recover via lease expiry"
            );
        }
    }

    /// Build a signed `FinalResult` from the stored result row.
    /// `started_at`/`wall_ms` are not tracked until Phase 5 cost
    /// tracking — both mirror `finished_at`/0.
    fn build_final_result(&self, task_id: TaskId) -> Option<FinalResult> {
        let row = self.store.load_task_result(task_id).ok().flatten()?;
        let (status, output) = match (&row.output, &row.error) {
            (Some(o), _) => (Status::Ok, o.clone()),
            (None, Some(e)) => (Status::Failed, serde_json::json!({ "error": e })),
            (None, None) => (
                Status::Failed,
                serde_json::json!({ "error": "result row missing output and error" }),
            ),
        };
        let mut result = FinalResult {
            task_id,
            node_id: self.local_id,
            started_at: row.completed_at_ms,
            finished_at: row.completed_at_ms,
            status,
            output,
            cost: Cost {
                tokens_in: 0,
                tokens_out: 0,
                usd: 0.0,
                wall_ms: 0,
                node_id: self.local_id,
            },
            logs: Vec::new(),
            provenance: Vec::new(),
            sig: harness_core::Signature::from_bytes([0u8; 64]),
        };
        if let Err(e) = result.sign(&self.identity) {
            tracing::error!(target: "harness.dispatch", ?e, "sign FinalResult");
            return None;
        }
        Some(result)
    }
}

impl TaskChannelHandlers for DispatchRuntime {
    /// Worker side: ingest an assignment. The transport layer already
    /// verified the outer signature + per-stream seq and that
    /// `assigned_by == from`.
    fn on_assign(&self, from: NodeId, msg: TaskAssign) {
        // v1 rule (ADR-0017 / R10): the assigner must be the issuer.
        if msg.task.issued_by != from {
            tracing::warn!(
                target: "harness.dispatch",
                %from,
                issued_by = %msg.task.issued_by,
                "assign for a task issued by a third node; rejected in v1"
            );
            return;
        }
        let Some(pk) = self.trusted_pubkey(from) else {
            tracing::warn!(target: "harness.dispatch", %from, "assigner not in trust store");
            return;
        };
        if msg.task.verify_signature(&pk).is_err() {
            tracing::warn!(target: "harness.dispatch", %from, "inner task signature invalid");
            return;
        }
        let task_id = msg.task.id;
        let state = match self.store.task_state(task_id) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(target: "harness.dispatch", ?e, "task_state on assign");
                return;
            }
        };
        match state {
            None => {
                match self.store.insert_task_dispatched(&msg.task, self.local_id) {
                    Ok(_) => {}
                    Err(e) => {
                        tracing::warn!(target: "harness.dispatch", ?e, "insert_task_dispatched");
                        return;
                    }
                }
                self.reply.lock().insert(
                    task_id,
                    ReplyObligation {
                        issuer: from,
                        lease_id: msg.lease_id,
                    },
                );
                self.send_claim(from, msg.lease_id, task_id);
            }
            Some(
                TaskState::Done | TaskState::Failed | TaskState::Expired | TaskState::Cancelled,
            ) => {
                // Re-delivered assignment for a finished task: resend the
                // stored result under the NEW lease id (ADR-0017 / R4).
                self.send_result(task_id, from, msg.lease_id);
            }
            Some(_) => {
                // In flight: refresh the obligation (new lease id) and
                // re-ack.
                self.reply.lock().insert(
                    task_id,
                    ReplyObligation {
                        issuer: from,
                        lease_id: msg.lease_id,
                    },
                );
                self.send_claim(from, msg.lease_id, task_id);
            }
        }
    }

    /// Issuer side: worker acked the assignment.
    fn on_claim(&self, from: NodeId, msg: TaskClaim) {
        match self.store.try_claim(msg.lease_id, from) {
            Ok(true) => {
                let _ = self.store.try_transition_task(
                    msg.task_id,
                    TaskState::Dispatched,
                    TaskState::Claimed,
                );
            }
            Ok(false) => {
                tracing::debug!(
                    target: "harness.dispatch",
                    task = %msg.task_id.0,
                    "claim for non-pending/expired lease ignored"
                );
            }
            Err(e) => tracing::warn!(target: "harness.dispatch", ?e, "try_claim"),
        }
    }

    /// Issuer side: worker delivered a terminal result.
    fn on_result(&self, from: NodeId, msg: TaskResultMsg) {
        let Ok(Some(lease)) = self.store.fetch_lease(msg.lease_id) else {
            tracing::debug!(target: "harness.dispatch", "result for unknown lease dropped");
            return;
        };
        if lease.task_id != msg.result.task_id
            || lease.worker_id != Some(from)
            || msg.result.node_id != from
        {
            tracing::warn!(
                target: "harness.dispatch",
                %from,
                task = %msg.result.task_id.0,
                "result identity mismatch (lease/worker/node); dropped"
            );
            return;
        }
        let Some(pk) = self.trusted_pubkey(from) else {
            tracing::warn!(target: "harness.dispatch", %from, "result from untrusted node");
            return;
        };
        if msg.result.verify_signature(&pk).is_err() {
            tracing::warn!(target: "harness.dispatch", %from, "inner result signature invalid");
            return;
        }
        // Accept from pending OR claimed — a lost claim must not drop a
        // valid result (R3). `false` = lease already terminal
        // (expired/duplicate/late): drop silently.
        match self.store.try_complete_pending_or_claimed(msg.lease_id) {
            Ok(true) => {}
            Ok(false) => {
                tracing::debug!(
                    target: "harness.dispatch",
                    task = %msg.result.task_id.0,
                    "late/duplicate result for terminal lease dropped"
                );
                return;
            }
            Err(e) => {
                tracing::warn!(target: "harness.dispatch", ?e, "try_complete");
                return;
            }
        }
        let task_id = msg.result.task_id;
        let now = msg.result.finished_at;
        if msg.result.status == Status::Ok {
            {
                self.finish_local_row(task_id, TaskState::Done);
                if let Err(e) =
                    self.store
                        .write_task_result_done(task_id, &msg.result.output, now, from)
                {
                    tracing::warn!(target: "harness.dispatch", ?e, "write remote result");
                }
                let preview = serde_json::to_vec(&msg.result.output)
                    .ok()
                    .map(|v| v.into_iter().take(256).collect());
                // Worker is the LWW source (R15) with its timestamps.
                let _ = self.store.replica_apply_local(&ReplicatedTaskState {
                    task_id,
                    state: ReplicatedState::Done,
                    at_ms: now,
                    source: from,
                    output_preview: preview,
                });
            }
        } else {
            {
                let err_msg = msg
                    .result
                    .output
                    .get("error")
                    .and_then(|v| v.as_str())
                    .unwrap_or("remote execution failed")
                    .to_string();
                self.finish_local_row(task_id, TaskState::Failed);
                if let Err(e) = self
                    .store
                    .write_task_result_failed(task_id, &err_msg, now, from)
                {
                    tracing::warn!(target: "harness.dispatch", ?e, "write remote failure");
                }
                let _ = self.store.replica_apply_local(&ReplicatedTaskState {
                    task_id,
                    state: ReplicatedState::Failed,
                    at_ms: now,
                    source: from,
                    output_preview: Some(err_msg.into_bytes().into_iter().take(256).collect()),
                });
            }
        }
    }

    /// Issuer side: the assign never made it onto the wire.
    fn on_assign_send_failed(&self, node: NodeId, lease_id: LeaseId) {
        tracing::warn!(target: "harness.dispatch", %node, "assign send failed; resetting lease");
        let _ = self.store.expire_and_reset_task(lease_id);
    }
}

impl DispatchRuntime {
    fn send_claim(&self, issuer: NodeId, lease_id: LeaseId, task_id: TaskId) {
        let claim = TaskClaim {
            seq: 0, // stamped per-stream by the sender task
            lease_id,
            task_id,
            worker: self.local_id,
            sig: harness_core::Signature::from_bytes([0u8; 64]),
        };
        let send = self
            .net()
            .ok_or(SendToError::NoConnection)
            .and_then(|net| net.send_to(issuer, OutboundMsg::Claim(claim)));
        if let Err(err) = send {
            // Non-fatal: the result path accepts pending leases (R3).
            tracing::debug!(target: "harness.dispatch", %issuer, %err, "claim enqueue failed");
        }
    }
}

/// `ttl = max(lease_ms, timeout_ms + slack)` — the lease must outlive
/// the longest legitimate execution (ADR-0017 / R2).
fn lease_ttl_ms(task: &Task) -> u32 {
    let timeout_plus_slack = u64::from(task.execution.timeout_ms).saturating_add(LEASE_SLACK_MS);
    let ttl = u64::from(task.execution.lease_ms).max(timeout_plus_slack);
    u32::try_from(ttl).unwrap_or(u32::MAX)
}

fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use harness_core::{
        Constraints, ExecutionPolicy, ResourceHints, RetryPolicy, Signature, TraceContext,
    };
    use harness_mesh::{AddedVia, Peer, TrustTier};

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

    fn signed_task(issuer: &Identity) -> Task {
        let mut t = Task {
            id: TaskId::new_v7(),
            parent: None,
            plan_id: None,
            capability: "echo".into(),
            input: serde_json::json!({"msg": "hi"}),
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

    fn signed_result(worker: &Identity, task_id: TaskId, ok: bool) -> FinalResult {
        let node = worker.node_id();
        let mut r = FinalResult {
            task_id,
            node_id: node,
            started_at: 1_700_000_000_500,
            finished_at: 1_700_000_000_500,
            status: if ok { Status::Ok } else { Status::Failed },
            output: if ok {
                serde_json::json!({"echoed": "hi"})
            } else {
                serde_json::json!({"error": "boom"})
            },
            cost: Cost {
                tokens_in: 0,
                tokens_out: 0,
                usd: 0.0,
                wall_ms: 0,
                node_id: node,
            },
            logs: vec![],
            provenance: vec![],
            sig: Signature::from_bytes([0u8; 64]),
        };
        r.sign(worker).expect("sign");
        r
    }

    struct Fixture {
        runtime: Arc<DispatchRuntime>,
        store: Store,
        local: Arc<Identity>,
        remote: Arc<Identity>,
        _tmp: tempfile::TempDir,
    }

    /// A runtime with a trust store containing `remote`; no `PeerNet`
    /// attached (send paths no-op, which these tests don't exercise).
    fn fixture() -> Fixture {
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
            store.clone(),
            local.clone(),
            harness_capabilities::CapabilityRegistry::new(),
            Dispatcher::new(),
            trust,
            PeerTable::new(),
        );
        Fixture {
            runtime,
            store,
            local,
            remote,
            _tmp: tmp,
        }
    }

    #[tokio::test]
    async fn t01_result_without_claim_completes_task_and_lease() {
        // R3: the claim was lost; the result must still land.
        let f = fixture();
        let task = signed_task(&f.local);
        f.store.insert_task(&task).expect("insert");
        assert!(f
            .store
            .try_dispatch_task(task.id, f.remote.node_id())
            .expect("dispatch"));
        let lease = f
            .store
            .create_lease(task.id, f.remote.node_id(), 60_000, 1)
            .expect("lease");

        let result = signed_result(&f.remote, task.id, true);
        let msg = TaskResultMsg {
            seq: 0,
            lease_id: lease.lease_id,
            result,
            sig: Signature::from_bytes([0u8; 64]),
        };
        f.runtime.on_result(f.remote.node_id(), msg);

        assert_eq!(
            f.store.task_state(task.id).expect("state"),
            Some(TaskState::Done)
        );
        let row = f
            .store
            .load_task_result(task.id)
            .expect("load")
            .expect("row");
        assert_eq!(row.completed_by, f.remote.node_id());
        assert_eq!(
            f.store
                .fetch_lease(lease.lease_id)
                .expect("fetch")
                .expect("lease")
                .state,
            harness_store::LeaseState::Completed
        );
    }

    #[tokio::test]
    async fn t02_forged_result_signature_rejected() {
        let f = fixture();
        let task = signed_task(&f.local);
        f.store.insert_task(&task).expect("insert");
        assert!(f
            .store
            .try_dispatch_task(task.id, f.remote.node_id())
            .expect("dispatch"));
        let lease = f
            .store
            .create_lease(task.id, f.remote.node_id(), 60_000, 1)
            .expect("lease");

        // Signed by an identity that is NOT the trusted worker.
        let intruder = Identity::generate();
        let mut result = signed_result(&f.remote, task.id, true);
        result.node_id = f.remote.node_id();
        result.sign(&intruder).expect("re-sign with wrong key");
        let msg = TaskResultMsg {
            seq: 0,
            lease_id: lease.lease_id,
            result,
            sig: Signature::from_bytes([0u8; 64]),
        };
        f.runtime.on_result(f.remote.node_id(), msg);

        assert_eq!(
            f.store.task_state(task.id).expect("state"),
            Some(TaskState::Dispatched),
            "forged result must not advance the task"
        );
        assert!(f.store.load_task_result(task.id).expect("load").is_none());
    }

    #[tokio::test]
    async fn t03_result_from_wrong_worker_rejected() {
        let f = fixture();
        let task = signed_task(&f.local);
        f.store.insert_task(&task).expect("insert");
        let other_worker = Identity::generate();
        assert!(f
            .store
            .try_dispatch_task(task.id, other_worker.node_id())
            .expect("dispatch"));
        let lease = f
            .store
            .create_lease(task.id, other_worker.node_id(), 60_000, 1)
            .expect("lease");

        // Trusted node `remote` tries to answer a lease held by another
        // worker.
        let result = signed_result(&f.remote, task.id, true);
        let msg = TaskResultMsg {
            seq: 0,
            lease_id: lease.lease_id,
            result,
            sig: Signature::from_bytes([0u8; 64]),
        };
        f.runtime.on_result(f.remote.node_id(), msg);
        assert_eq!(
            f.store.task_state(task.id).expect("state"),
            Some(TaskState::Dispatched)
        );
    }

    #[tokio::test]
    async fn t04_late_result_after_lease_expiry_dropped() {
        let f = fixture();
        let task = signed_task(&f.local);
        f.store.insert_task(&task).expect("insert");
        assert!(f
            .store
            .try_dispatch_task(task.id, f.remote.node_id())
            .expect("dispatch"));
        let lease = f
            .store
            .create_lease(task.id, f.remote.node_id(), 1, 1)
            .expect("lease");
        std::thread::sleep(Duration::from_millis(5));
        assert!(f
            .store
            .expire_and_reset_task(lease.lease_id)
            .expect("expire"));

        let result = signed_result(&f.remote, task.id, true);
        let msg = TaskResultMsg {
            seq: 0,
            lease_id: lease.lease_id,
            result,
            sig: Signature::from_bytes([0u8; 64]),
        };
        f.runtime.on_result(f.remote.node_id(), msg);
        // Task went back to Submitted on expiry; the late result must
        // not resurrect the old attempt.
        assert_eq!(
            f.store.task_state(task.id).expect("state"),
            Some(TaskState::Submitted)
        );
        assert!(f.store.load_task_result(task.id).expect("load").is_none());
    }

    #[tokio::test]
    async fn t05_assign_ingests_and_registers_obligation() {
        let f = fixture();
        // Remote is the issuer here; we are the worker.
        let task = signed_task(&f.remote);
        let assign = TaskAssign {
            seq: 0,
            lease_id: LeaseId::new_v7(),
            task: task.clone(),
            assigned_by: f.remote.node_id(),
            lease_expires_at: 0,
            sig: Signature::from_bytes([0u8; 64]),
        };
        f.runtime.on_assign(f.remote.node_id(), assign.clone());

        assert_eq!(
            f.store.task_state(task.id).expect("state"),
            Some(TaskState::Dispatched)
        );
        assert_eq!(
            f.store.assigned_node(task.id).expect("assigned"),
            Some(f.local.node_id()),
            "remote assignment must be assigned to self for the executor"
        );
        assert_eq!(f.runtime.reply_obligations_len(), 1);

        // Idempotent re-delivery: no error, still one obligation.
        f.runtime.on_assign(f.remote.node_id(), assign);
        assert_eq!(f.runtime.reply_obligations_len(), 1);
    }

    #[tokio::test]
    async fn t06_assign_third_party_issuer_rejected() {
        let f = fixture();
        let third = Identity::generate();
        let task = signed_task(&third); // issued by a third node
        let assign = TaskAssign {
            seq: 0,
            lease_id: LeaseId::new_v7(),
            task: task.clone(),
            assigned_by: f.remote.node_id(),
            lease_expires_at: 0,
            sig: Signature::from_bytes([0u8; 64]),
        };
        f.runtime.on_assign(f.remote.node_id(), assign);
        assert_eq!(f.store.task_state(task.id).expect("state"), None);
        assert_eq!(f.runtime.reply_obligations_len(), 0);
    }

    #[tokio::test]
    async fn t07_assign_terminal_row_does_not_reobligate() {
        // ADR-0017 R4: re-delivered assign for a finished task resends
        // the stored result (send is a no-op here — no net attached)
        // and must not re-register an obligation or disturb the row.
        let f = fixture();
        let task = signed_task(&f.remote);
        f.store
            .insert_task_dispatched(&task, f.local.node_id())
            .expect("ingest");
        for (from, to) in [
            (TaskState::Dispatched, TaskState::Claimed),
            (TaskState::Claimed, TaskState::Running),
            (TaskState::Running, TaskState::Done),
        ] {
            assert!(f.store.try_transition_task(task.id, from, to).expect("hop"));
        }
        f.store
            .write_task_result_done(
                task.id,
                &serde_json::json!({"ok": true}),
                1,
                f.local.node_id(),
            )
            .expect("result");

        let assign = TaskAssign {
            seq: 0,
            lease_id: LeaseId::new_v7(),
            task: task.clone(),
            assigned_by: f.remote.node_id(),
            lease_expires_at: 0,
            sig: Signature::from_bytes([0u8; 64]),
        };
        f.runtime.on_assign(f.remote.node_id(), assign);
        assert_eq!(
            f.store.task_state(task.id).expect("state"),
            Some(TaskState::Done),
            "terminal row untouched"
        );
        assert_eq!(f.runtime.reply_obligations_len(), 0);
    }

    #[tokio::test]
    async fn t09_result_wins_race_expiry_writes_nothing() {
        // M1 ordering A: the result lands (lease completed) after
        // find_expired snapshotted the lease. The terminal-expiry
        // branch must lose the lease CAS and leave the Done row alone.
        let f = fixture();
        let task = signed_task(&f.local);
        f.store.insert_task(&task).expect("insert");
        assert!(f
            .store
            .try_dispatch_task(task.id, f.remote.node_id())
            .expect("dispatch"));
        // attempt >= max_attempts (3) and TTL already elapsed → the
        // expire pass takes the terminal branch for this lease.
        let lease = f
            .store
            .create_lease(task.id, f.remote.node_id(), 1, 3)
            .expect("lease");
        std::thread::sleep(Duration::from_millis(5));

        // The result arrives first…
        let result = signed_result(&f.remote, task.id, true);
        f.runtime.on_result(
            f.remote.node_id(),
            TaskResultMsg {
                seq: 0,
                lease_id: lease.lease_id,
                result,
                sig: Signature::from_bytes([0u8; 64]),
            },
        );
        assert_eq!(
            f.store.task_state(task.id).expect("state"),
            Some(TaskState::Done)
        );

        // …then the expiry pass runs over the same (now-completed) lease.
        f.runtime.expire_pass();

        assert_eq!(
            f.store.task_state(task.id).expect("state"),
            Some(TaskState::Done),
            "expiry must not disturb the completed task"
        );
        let row = f
            .store
            .load_task_result(task.id)
            .expect("load")
            .expect("row");
        assert!(
            row.error.is_none(),
            "Done row must not be overwritten: {row:?}"
        );
        assert_eq!(row.completed_by, f.remote.node_id());
    }

    #[tokio::test]
    async fn t10_expiry_wins_race_late_result_dropped() {
        // M1 ordering B: terminal expiry wins the lease CAS; the late
        // result must be dropped without overwriting the Expired row.
        let f = fixture();
        let task = signed_task(&f.local);
        f.store.insert_task(&task).expect("insert");
        assert!(f
            .store
            .try_dispatch_task(task.id, f.remote.node_id())
            .expect("dispatch"));
        let lease = f
            .store
            .create_lease(task.id, f.remote.node_id(), 1, 3)
            .expect("lease");
        std::thread::sleep(Duration::from_millis(5));

        f.runtime.expire_pass();
        assert_eq!(
            f.store.task_state(task.id).expect("state"),
            Some(TaskState::Expired)
        );

        let result = signed_result(&f.remote, task.id, true);
        f.runtime.on_result(
            f.remote.node_id(),
            TaskResultMsg {
                seq: 0,
                lease_id: lease.lease_id,
                result,
                sig: Signature::from_bytes([0u8; 64]),
            },
        );
        assert_eq!(
            f.store.task_state(task.id).expect("state"),
            Some(TaskState::Expired),
            "late result must not resurrect an expired task"
        );
        let row = f
            .store
            .load_task_result(task.id)
            .expect("load")
            .expect("row");
        assert!(
            row.error.as_deref().unwrap_or("").contains("lease expired"),
            "expiry reason must survive the late result: {row:?}"
        );
    }

    #[tokio::test]
    async fn t08_failed_remote_result_maps_to_failed_row() {
        let f = fixture();
        let task = signed_task(&f.local);
        f.store.insert_task(&task).expect("insert");
        assert!(f
            .store
            .try_dispatch_task(task.id, f.remote.node_id())
            .expect("dispatch"));
        let lease = f
            .store
            .create_lease(task.id, f.remote.node_id(), 60_000, 1)
            .expect("lease");
        let result = signed_result(&f.remote, task.id, false);
        let msg = TaskResultMsg {
            seq: 0,
            lease_id: lease.lease_id,
            result,
            sig: Signature::from_bytes([0u8; 64]),
        };
        f.runtime.on_result(f.remote.node_id(), msg);
        assert_eq!(
            f.store.task_state(task.id).expect("state"),
            Some(TaskState::Failed)
        );
        let row = f
            .store
            .load_task_result(task.id)
            .expect("load")
            .expect("row");
        assert_eq!(row.error.as_deref(), Some("boom"));
    }
}
