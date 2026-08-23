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

use harness_api::PartialBuffers;
use harness_capabilities::CapabilityRegistry;
use harness_core::{
    Cardinality, Cost, FinalResult, Identity, LeaseId, NodeId, PartialResult, PublicKey,
    ReplicatedState, ReplicatedTaskState, Signable, Status, Task, TaskAssign, TaskClaim, TaskId,
    TaskResultMsg,
};
use harness_mesh::heartbeat::{PeerTable, PEER_TIMEOUT};
use harness_mesh::TrustStore;
use harness_orchestrator::{
    effective_hints, Breaker, DispatchError, DispatchPlan, Dispatcher, LiveSet, LoadView,
    NodeSnapshot, RoundRobin, SuccessTracker,
};
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
/// 4.6 (ADR-0028): rolling lease horizon granted per accepted
/// `LeaseExtend` — receipt time + this, hard-capped by the lease's
/// ORIGINAL budget (`issued_at + lease TTL`). Once a worker proves it
/// extends, its death is detected within ~one horizon instead of the
/// full task timeout.
#[cfg(not(test))]
const EXTEND_HORIZON_MS: u64 = 30_000;
#[cfg(test)]
const EXTEND_HORIZON_MS: u64 = 2_000;

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

/// [`MeshLiveSet`] narrowed to nodes that can satisfy the capability's
/// `requires_secrets` (3.6-encrypted, ADR-0021). Filtering *inside* the
/// live-set — rather than post-filtering the computed `DispatchPlan` —
/// keeps routing deterministic (the round-robin cursor never elects a
/// node that would then be discarded) and lets the empty case flow
/// through the existing `NoEligibleNodes` → undispatchable path with no
/// new error plumbing. The filter stays in the daemon: the orchestrator
/// remains pure/store-free.
struct SecretAwareLiveSet<'a> {
    inner: MeshLiveSet,
    runtime: &'a DispatchRuntime,
    capability: &'a str,
}

impl LiveSet for SecretAwareLiveSet<'_> {
    fn is_live(&self, node: &NodeId) -> bool {
        self.inner.is_live(node)
            && self
                .runtime
                .node_has_required_secrets(*node, self.capability)
    }
}

/// 4.6 (ADR-0028): bench-aware layer over the liveness ∩ secrets set.
/// `breaker: None` = passthrough (pinned tasks and Federated fan-outs
/// bypass the bench — operator intent and availability-first
/// respectively). Counts filtered nodes so the caller can distinguish
/// "no eligible nodes at all" from "eligible nodes exist but every one
/// is benched" — the latter is ≤60 s transient and must WAIT, not burn
/// the terminal eligibility window (plan review MAJOR-8).
struct BreakerAwareLiveSet<'a> {
    inner: SecretAwareLiveSet<'a>,
    breaker: Option<&'a Breaker>,
    filtered: std::sync::atomic::AtomicUsize,
}

impl LiveSet for BreakerAwareLiveSet<'_> {
    fn is_live(&self, node: &NodeId) -> bool {
        if !self.inner.is_live(node) {
            return false;
        }
        if let Some(breaker) = self.breaker {
            if breaker.is_benched(node) {
                self.filtered
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return false;
            }
        }
        true
    }
}

struct ReplyObligation {
    issuer: NodeId,
    lease_id: LeaseId,
}

/// Per-poll load view (4.4, ADR-0026): heartbeat snapshots from the
/// `PeerTable`, capacity from stored manifests, in-flight counts from
/// one group-by store query, plus same-poll reservations so one batch
/// doesn't pile onto a single node.
struct StoreLoadView<'a> {
    runtime: &'a DispatchRuntime,
    inflight: HashMap<NodeId, u32>,
    reserved: ParkingMutex<HashMap<NodeId, u32>>,
    /// Base snapshots memoized per pass — `load_manifest` decodes the
    /// FULL manifest CBOR, so without this the hot path pays
    /// tasks × candidates decodes per 100 ms poll (review MAJOR-2).
    base: ParkingMutex<HashMap<NodeId, NodeSnapshot>>,
}

impl<'a> StoreLoadView<'a> {
    fn new(runtime: &'a DispatchRuntime) -> Self {
        let inflight = runtime.store.count_inflight_by_node().unwrap_or_else(|e| {
            tracing::warn!(target: "harness.dispatch", ?e, "count_inflight_by_node");
            HashMap::new()
        });
        Self {
            runtime,
            inflight,
            reserved: ParkingMutex::new(HashMap::new()),
            base: ParkingMutex::new(HashMap::new()),
        }
    }

    fn note_assigned(&self, node: NodeId) {
        *self.reserved.lock().entry(node).or_insert(0) += 1;
    }
}

impl LoadView for StoreLoadView<'_> {
    fn snapshot(&self, node: &NodeId) -> NodeSnapshot {
        if let Some(cached) = self.base.lock().get(node) {
            let mut snap = *cached;
            let reserved = self.reserved.lock().get(node).copied().unwrap_or(0);
            snap.assigned_inflight = snap.assigned_inflight.saturating_add(reserved);
            return snap;
        }
        let mut snap = NodeSnapshot::default();
        // Capacity from the stored manifest (self manifest indexed at boot).
        if let Ok(Some(m)) = self.runtime.store.load_manifest(*node) {
            snap.cpu_cores = m.resources.cpu_cores;
            snap.ram_total_mb = m.resources.ram_total_mb;
            snap.has_gpu = m.resources.gpu.is_some();
            snap.gpu_total_mb = m.resources.gpu.as_ref().map_or(0, |g| g.vram_mb);
        }
        // Load from the latest heartbeat (zeros until Phase 6 sampling).
        if let Some(entry) = self.runtime.peers.get(node) {
            let hb = &entry.heartbeat;
            snap.cpu_busy_pct = hb.cpu_busy_pct;
            snap.cpu_pinned_count = hb.cpu_pinned_count;
            snap.ram_used_mb = hb.ram_used_mb;
            if hb.ram_total_mb > 0 {
                snap.ram_total_mb = hb.ram_total_mb;
            }
            snap.gpu_used_mb = hb.gpu_used_mb;
            if hb.gpu_total_mb > 0 {
                snap.gpu_total_mb = hb.gpu_total_mb;
            }
            // max, not sum: no double-count once in_flight populates (1.5).
            snap.reported_inflight = u32::from(hb.queue_depth)
                .max(u32::try_from(hb.in_flight.len()).unwrap_or(u32::MAX));
            snap.paused = hb.paused;
            snap.on_battery = hb.on_battery;
        }
        // 4.7 (ADR-0029): self has no PeerTable entry — read the local
        // pause switch directly so a paused node gates dispatch-to-self
        // exactly like peers gate dispatch-to-it (plan review MAJOR-3).
        if *node == self.runtime.local_id {
            if let Some(pause) = self.runtime.pause.get() {
                snap.paused = pause.effective();
            }
        }
        snap.assigned_inflight = self.inflight.get(node).copied().unwrap_or(0);
        self.base.lock().insert(*node, snap);
        let reserved = self.reserved.lock().get(node).copied().unwrap_or(0);
        snap.assigned_inflight = snap.assigned_inflight.saturating_add(reserved);
        snap
    }

    fn success_rate(&self, node: &NodeId) -> f64 {
        self.runtime.success.rate(node)
    }
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
    /// Local vault, consulted for *tag names only* when routing
    /// `requires_secrets`-declaring capabilities (ADR-0021). Values are
    /// never read on the dispatch path.
    secrets: Arc<dyn harness_vault::SecretsStore>,
    /// Set once after `PeerNet::new` (the net holds us as its handlers —
    /// a `Weak` back-reference avoids the cycle).
    net: OnceLock<Weak<PeerNet>>,
    /// Worker side: tasks we owe a result reply for.
    reply: ParkingMutex<HashMap<TaskId, ReplyObligation>>,
    /// Issuer side: first time each task failed eligibility.
    elig_failures: ParkingMutex<HashMap<TaskId, Instant>>,
    /// Issuer side: streaming partial-output ring buffers shared with
    /// the API (3.2-stream, ADR-0020). Set once from lifecycle; when
    /// unset (bare unit-test fixtures), inbound partials are dropped.
    partials: OnceLock<Arc<PartialBuffers>>,
    /// 4.4 (ADR-0026): per-node dispatch-outcome EWMA. Shared with the
    /// local executor so local terminals feed it too (review MAJOR-2).
    success: Arc<SuccessTracker>,
    /// Issuer side: tasks currently resource-gated (waiting, no
    /// terminal window). Batched BEHIND never-seen tasks so deadline-
    /// less gated work can't starve other capabilities (diff review
    /// BLOCKER-1).
    gated: ParkingMutex<std::collections::HashSet<TaskId>>,
    /// 4.5 (ADR-0027): federated fan-out/merge coordinator. Claims
    /// `DispatchPlan::Federated` parents atomically and drives them to
    /// a terminal with per-node provenance.
    federated: Arc<crate::federated::FederatedCoordinator>,
    /// 4.6 (ADR-0028): tasks whose next dispatch attempt is deferred
    /// (lease-expiry retry or assign-send failure), keyed to the
    /// instant the backoff ends. In-memory by design: a restart
    /// forgets backoffs, so the first post-boot poll retries
    /// immediately — one extra attempt after a crash, never a tight
    /// loop. Backing-off tasks also sit in `gated` so they batch in
    /// the WAITING class.
    backoff: ParkingMutex<HashMap<TaskId, tokio::time::Instant>>,
    /// 4.7 (ADR-0029): the node's own pause switch. The `PeerTable` has
    /// no self entry, so without this the LOCAL dispatch view would be
    /// pause-blind and a paused node would keep dispatching to itself
    /// (plan review MAJOR-3). Set once from lifecycle.
    pause: OnceLock<Arc<crate::pause::PauseState>>,
    /// 4.6 (ADR-0028, PRD §14.5): per-node circuit breaker — 5
    /// consecutive NODE-HEALTH failures (lease expiry, send failure;
    /// never task-level result statuses) bench a node for 60 s from
    /// `Anyone`/`Owner` candidate sets. Self is never benched; pinned
    /// tasks and Federated fan-outs bypass the bench.
    breaker: Arc<Breaker>,
}

impl DispatchRuntime {
    pub(crate) fn new(
        store: Store,
        identity: Arc<Identity>,
        registry: CapabilityRegistry,
        dispatcher: Dispatcher,
        trust: TrustStore,
        peers: PeerTable,
        secrets: Arc<dyn harness_vault::SecretsStore>,
    ) -> Arc<Self> {
        let local_id = identity.node_id();
        let federated =
            crate::federated::FederatedCoordinator::new(store.clone(), identity.clone());
        Arc::new(Self {
            store,
            identity,
            local_id,
            federated,
            registry,
            dispatcher,
            rr: RoundRobin::new(),
            rr_seeded: ParkingMutex::new(std::collections::HashSet::new()),
            trust,
            peers,
            secrets,
            net: OnceLock::new(),
            reply: ParkingMutex::new(HashMap::new()),
            elig_failures: ParkingMutex::new(HashMap::new()),
            partials: OnceLock::new(),
            success: Arc::new(SuccessTracker::new()),
            gated: ParkingMutex::new(std::collections::HashSet::new()),
            backoff: ParkingMutex::new(HashMap::new()),
            breaker: Arc::new(Breaker::new()),
            pause: OnceLock::new(),
        })
    }

    /// Wire the shared pause switch (4.7, lifecycle).
    pub(crate) fn attach_pause(&self, pause: Arc<crate::pause::PauseState>) {
        self.federated.attach_pause(pause.clone());
        let _ = self.pause.set(pause);
    }

    /// 4.6: node-health failure feed (lease expiry / send failure).
    /// Self is never benched — a single-node install must not gate its
    /// whole queue on its own task outcomes.
    fn record_node_failure(&self, node: NodeId) {
        if node != self.local_id {
            self.breaker.record_failure(node);
        }
    }

    /// Test introspection: the circuit breaker.
    #[cfg(test)]
    pub(crate) fn breaker(&self) -> &Breaker {
        &self.breaker
    }

    /// Test introspection: is the task inside the terminal eligibility
    /// window?
    #[cfg(test)]
    pub(crate) fn has_elig_failure(&self, id: TaskId) -> bool {
        self.elig_failures.lock().contains_key(&id)
    }

    /// The shared success tracker — the local executor records its own
    /// terminals here so the self node's rate is honest (ADR-0026).
    pub(crate) fn success_tracker(&self) -> Arc<SuccessTracker> {
        self.success.clone()
    }

    /// Test introspection: how many result replies this worker owes.
    #[cfg(test)]
    pub(crate) fn reply_obligations_len(&self) -> usize {
        self.reply.lock().len()
    }

    /// Test introspection: is the task batched in the waiting class?
    #[cfg(test)]
    pub(crate) fn is_gated(&self, id: TaskId) -> bool {
        self.gated.lock().contains(&id)
    }

    /// Wire the back-reference after `PeerNet::new`.
    pub(crate) fn attach_net(&self, net: &Arc<PeerNet>) {
        let _ = self.net.set(Arc::downgrade(net));
    }

    /// Share the API's partial-output ring buffers (3.2-stream).
    pub(crate) fn attach_partials(&self, buffers: Arc<PartialBuffers>) {
        let _ = self.partials.set(buffers);
    }

    /// Wire the federated coordinator's progress-frame sink (4.5) —
    /// the same `PartialStreamer::sink()` the wrappers use.
    pub(crate) fn attach_federated_sink(&self, sink: harness_capabilities::FrameSink) {
        self.federated.attach_sink(sink);
    }

    /// Worker side: the issuer of a task we ingested from the wire and
    /// still owe a result for. `None` for locally-issued tasks (and for
    /// remote tasks already replied to — by then the child has exited
    /// and no more frames arrive).
    pub(crate) fn remote_issuer(&self, task_id: TaskId) -> Option<NodeId> {
        self.reply.lock().get(&task_id).map(|o| o.issuer)
    }

    pub(crate) fn net(&self) -> Option<Arc<PeerNet>> {
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
        if rows.is_empty() {
            return; // no store queries on idle polls (review MINOR-1)
        }
        // 4.4: priority batch (carried risk 10 + review BLOCKER-1) —
        // never-seen tasks first, then resource-gated waiters, then
        // known-undispatchable retries. Deadline-less gated work can
        // therefore never starve other capabilities.
        let known_failing: std::collections::HashSet<TaskId> =
            self.elig_failures.lock().keys().copied().collect();
        let gated: std::collections::HashSet<TaskId> = self.gated.lock().clone();
        let batch = select_batch(rows, &gated, &known_failing, DISPATCH_BATCH);
        // One load view per pass; same-poll assignments are reserved so
        // the batch spreads (ADR-0026).
        let loads = StoreLoadView::new(self);
        for row in batch {
            let task = match self.store.load_task(row.id) {
                Ok(Some(t)) => t,
                Ok(None) => continue,
                Err(e) => {
                    tracing::warn!(target: "harness.dispatch", ?e, "load_task");
                    continue;
                }
            };
            // 4.6 (ADR-0028): a backing-off task waits out its delay
            // in the gated class (deadline enforced inside — plan
            // review MAJOR-7).
            if self.waits_in_backoff(&task) || self.gated_deadline_expired(&task) {
                continue;
            }
            let cardinality = self.cardinality_for(&task.capability);
            self.seed_rr_cursor(&task.capability);
            // Score with the max-demand union of task + capability hints
            // (the API stamps default hints on most submits).
            let cap_hints = self
                .registry
                .get(&task.capability)
                .map(|c| c.manifest().resource_hints);
            let hints = effective_hints(&task.resource_hints, cap_hints.as_ref());
            // Liveness ∩ secret capability (ADR-0021): nodes missing a
            // tag the capability requires are not candidates. When that
            // empties the set the existing eligibility-failure window →
            // terminal `undispatchable` path applies unchanged.
            let live = BreakerAwareLiveSet {
                inner: SecretAwareLiveSet {
                    inner: self.live_set(),
                    runtime: self,
                    capability: &task.capability,
                },
                // Pinned tasks (operator intent) and Federated fan-outs
                // (availability-first; a benched node just shows up
                // non-Ok in provenance) bypass the bench.
                breaker: if task.constraints.pin_to_node.is_some()
                    || matches!(cardinality, Cardinality::Federated { .. })
                {
                    None
                } else {
                    Some(&self.breaker)
                },
                filtered: std::sync::atomic::AtomicUsize::new(0),
            };
            match self.dispatcher.eligible_scored(
                &task,
                &hints,
                &cardinality,
                &live,
                &loads,
                &self.rr,
            ) {
                Ok(DispatchPlan::Single { node }) => {
                    self.gated.lock().remove(&task.id);
                    loads.note_assigned(node);
                    self.dispatch_to(&task, node);
                }
                Ok(DispatchPlan::Federated { nodes, excluded }) => {
                    // 4.5 (ADR-0027): hand the parent to the federated
                    // coordinator. It claims atomically (submitted →
                    // running(self)) and a detached driver fans out /
                    // merges / terminalizes. `false` = no coordination
                    // slot free: the task stays Submitted and retries
                    // next poll (queueing, never failure).
                    self.elig_failures.lock().remove(&task.id);
                    let Cardinality::Federated {
                        merge,
                        on_node_failure,
                    } = cardinality
                    else {
                        // eligible_scored only returns Federated plans
                        // for Federated cardinality; a mismatch is a
                        // routing bug — terminalize with a visible
                        // reason rather than retrying forever (diff
                        // review NIT-8).
                        self.gated.lock().remove(&task.id);
                        self.fail_undispatchable(
                            &task,
                            "internal: federated plan for non-federated cardinality",
                        );
                        continue;
                    };
                    if self
                        .federated
                        .try_start(&task, nodes, excluded, merge, on_node_failure)
                    {
                        self.gated.lock().remove(&task.id);
                    } else {
                        // Slot-starved parents join the WAITING batch
                        // class: a burst of ≥DISPATCH_BATCH queued
                        // federated tasks must not monopolize the
                        // fresh-first batch and starve other Submitted
                        // work for the length of a coordination (diff
                        // review MAJOR-2 — the ResourceGated doctrine).
                        self.gated.lock().insert(task.id);
                    }
                }
                Ok(_) => {
                    // `DispatchPlan` is non_exhaustive; unknown plans are
                    // a routing bug, not a task failure.
                    tracing::error!(target: "harness.dispatch", task = %task.id.0, "unknown dispatch plan");
                }
                Err(err) => {
                    let bench_filtered =
                        live.filtered.load(std::sync::atomic::Ordering::Relaxed) > 0;
                    self.eligibility_failure(&task, &err, bench_filtered);
                }
            }
        }
    }

    /// Can `node` satisfy `capability`'s `requires_secrets`? (ADR-0021)
    ///
    /// - **Self:** the local registry's manifest entry names the
    ///   required tags; the live local vault (`SecretsStore::tags`)
    ///   answers what we hold. Tag *names* only — no values move.
    /// - **Peer:** the peer's stored manifest carries both its
    ///   capability entry (with `requires_secrets`) and its advertised
    ///   `secret_tags`.
    /// - **Unknown:** a peer whose manifest we don't hold (index
    ///   warm-up race) is NOT filtered — this is a routing
    ///   optimization, not a security boundary (policy is enforced on
    ///   the executing node, PRD §10.4), and pre-3.6 behavior (route,
    ///   let the worker answer `not configured`) is the conservative
    ///   fallback.
    fn node_has_required_secrets(&self, node: NodeId, capability: &str) -> bool {
        if node == self.local_id {
            let required = self
                .registry
                .get(capability)
                .map(|c| c.manifest().requires_secrets)
                .unwrap_or_default();
            if required.is_empty() {
                return true;
            }
            let have = self.secrets.tags();
            return required.iter().all(|t| have.contains(t));
        }
        match self.store.load_manifest(node) {
            Ok(Some(m)) => {
                let Some(cap) = m.capabilities.iter().find(|c| c.id == capability) else {
                    return true;
                };
                cap.requires_secrets
                    .iter()
                    .all(|t| m.secret_tags.contains(t))
            }
            Ok(None) => true,
            Err(e) => {
                tracing::warn!(target: "harness.dispatch", ?e, %node, "load_manifest for secret routing");
                true
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
        // 4.6 (ADR-0027 carry): pinned routes never consult RR — don't
        // churn the cursor N times per federated fan-out.
        if task.constraints.pin_to_node.is_none() {
            if let Err(e) = self.store.set_last_dispatched(&task.capability, node) {
                tracing::warn!(target: "harness.dispatch", ?e, "persist rr cursor");
            }
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
            self.record_node_failure(node);
            if let Ok(true) = self.store.expire_and_reset_task(lease.lease_id) {
                // 4.6 (risk 9): back off before re-routing so a dead
                // pin isn't hammered every 100 ms poll.
                self.schedule_backoff(task.id, &task.retry, attempt);
            }
        }
    }

    /// 4.7 (diff review MINOR-1): a task WAITING in the gated class
    /// whose deadline elapsed terminalizes even when the gate opens in
    /// the same poll gap (a paused pin un-pausing, an all-benched set
    /// clearing) — the success-path dispatch must never run it
    /// posthumously. Scoped to the gated set so plain user tasks keep
    /// their semantics (deadline otherwise enforced on failure paths).
    /// Returns `true` when the task was terminalized.
    fn gated_deadline_expired(&self, task: &Task) -> bool {
        if self.gated.lock().contains(&task.id)
            && task
                .constraints
                .deadline
                .is_some_and(|d| now_unix_ms() >= d)
        {
            self.gated.lock().remove(&task.id);
            self.fail_undispatchable(task, "deadline exceeded while resource-gated");
            return true;
        }
        false
    }

    fn eligibility_failure(&self, task: &Task, err: &DispatchError, bench_filtered: bool) {
        // 4.4 (ADR-0026 / review BLOCKER-1): a load-gated task is a
        // QUEUED task, not an undispatchable one — never start the
        // terminal window for it. It waits bounded only by its own
        // deadline, exactly like work queued behind a busy executor.
        //
        // 4.6 (ADR-0028): "every candidate is benched" is the same
        // shape — a ≤60 s transient. When the breaker filtered anyone,
        // NoEligibleNodes and Owner-empty errors join the waiting arm
        // instead of burning the terminal window (the sole-benched-
        // owner case included; plan review MAJOR-8).
        let bench_gated = bench_filtered
            && matches!(
                err,
                DispatchError::NoEligibleNodes { .. } | DispatchError::Owner { .. }
            );
        if bench_gated || matches!(err, DispatchError::ResourceGated { .. }) {
            self.elig_failures.lock().remove(&task.id);
            self.gated.lock().insert(task.id);
            let deadline_expired = task
                .constraints
                .deadline
                .is_some_and(|d| now_unix_ms() >= d);
            if !deadline_expired {
                return; // keep waiting; gates re-evaluate every poll
            }
            self.gated.lock().remove(&task.id);
            self.fail_undispatchable(task, &format!("deadline exceeded while {err}"));
            return;
        }
        self.gated.lock().remove(&task.id);
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
        self.fail_undispatchable(task, &format!("undispatchable: {err}"));
        self.elig_failures.lock().remove(&task.id);
    }

    fn fail_undispatchable(&self, task: &Task, msg: &str) {
        if let Ok(true) =
            self.store
                .try_transition_task(task.id, TaskState::Submitted, TaskState::Failed)
        {
            tracing::warn!(target: "harness.dispatch", task = %task.id.0, %msg, "task failed terminally");
            let now_ms = now_unix_ms();
            if let Err(e) = self
                .store
                .write_task_result_failed(task.id, msg, now_ms, self.local_id)
            {
                tracing::warn!(target: "harness.dispatch", ?e, "write undispatchable result");
            }
            let _ = self.store.replica_apply_local(&ReplicatedTaskState {
                task_id: task.id,
                state: ReplicatedState::Failed,
                at_ms: now_ms,
                source: self.local_id,
                output_preview: Some(msg.as_bytes().iter().copied().take(256).collect()),
            });
        }
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
            let task_row = self.store.load_task(lease.task_id).ok().flatten();
            let max_attempts = task_row
                .as_ref()
                .map_or(3, |t| u32::from(t.retry.max_attempts));
            if lease.attempt >= max_attempts {
                // Terminal expiry joins the lease-CAS discipline (review
                // M1): win `pending|claimed → expired` FIRST. Losing
                // means a result completed the lease between
                // `find_expired`'s snapshot and now — that result owns
                // the terminal state; write nothing.
                match self.store.try_expire_lease(lease.lease_id, now) {
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
                if let Some(worker) = lease.worker_id {
                    self.success.record(worker, false);
                    self.record_node_failure(worker);
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
                // Guarded on expires_at < now: a LeaseExtend landing
                // after find_expired's snapshot makes this reset lose
                // (plan review MAJOR-3 — the worker is provably alive).
                // Failure signals are recorded ONLY when the expiry CAS
                // wins (PR #49 review): a lost race means the worker
                // extended — counting it could bench a live node.
                if let Ok(true) = self
                    .store
                    .expire_and_reset_task_if_unextended(lease.lease_id, now)
                {
                    // The strongest per-node signal: this worker held
                    // the lease and didn't finish (review MINOR-6).
                    if let Some(worker) = lease.worker_id {
                        self.success.record(worker, false);
                        self.record_node_failure(worker);
                    }
                    // 4.6 (ADR-0028): the retry waits out its backoff in
                    // the WAITING batch class — no more immediate
                    // re-dispatch hammering a dead pin.
                    if let Some(task) = task_row.as_ref() {
                        self.schedule_backoff(task.id, &task.retry, lease.attempt);
                    }
                }
            }
        }
        // 4.6 hygiene (plan review MINOR-12): entries whose task left
        // `Submitted` sideways (cancel, terminal) must not live forever.
        self.prune_waiting_sets();
    }

    /// 4.6 (ADR-0028): is this Submitted task still waiting out a
    /// retry backoff? `true` = skip it this poll (it stays batched in
    /// the WAITING class). The deadline is enforced HERE — no other
    /// check is reachable while the task is never scored (plan review
    /// MAJOR-7).
    fn waits_in_backoff(&self, task: &Task) -> bool {
        // Copy the instant out — holding the guard through this body
        // would deadlock on the re-locks below (parking_lot is not
        // reentrant).
        let Some(until) = self.backoff.lock().get(&task.id).copied() else {
            return false;
        };
        if tokio::time::Instant::now() >= until {
            self.backoff.lock().remove(&task.id);
            return false;
        }
        let deadline_expired = task
            .constraints
            .deadline
            .is_some_and(|d| now_unix_ms() >= d);
        if deadline_expired {
            self.backoff.lock().remove(&task.id);
            self.gated.lock().remove(&task.id);
            self.fail_undispatchable(
                task,
                "deadline exceeded while backing off between retry attempts",
            );
        } else {
            self.gated.lock().insert(task.id);
        }
        true
    }

    /// Defer `task`'s next dispatch attempt by its retry policy's
    /// backoff for the attempt that just failed (4.6, ADR-0028).
    fn schedule_backoff(&self, task_id: TaskId, retry: &harness_core::RetryPolicy, attempt: u32) {
        let delay = backoff_delay(retry, attempt);
        self.backoff
            .lock()
            .insert(task_id, tokio::time::Instant::now() + delay);
        // Inserted into the waiting class NOW so the next poll never
        // burns a fresh-class batch slot on it (review MINOR-12).
        self.gated.lock().insert(task_id);
    }

    /// Drop backoff/gated bookkeeping for tasks no longer `Submitted`.
    /// Covers the UNION of both sets (PR #49 review): resource-gated,
    /// bench-gated, and federated slot-starved tasks live in `gated`
    /// without a backoff entry, and a sideways exit (operator cancel)
    /// would otherwise leak them forever.
    fn prune_waiting_sets(&self) {
        let mut ids: std::collections::HashSet<TaskId> =
            self.backoff.lock().keys().copied().collect();
        ids.extend(self.gated.lock().iter().copied());
        // 4.7 (ADR-0029, plan risk #13): eligibility-window entries are
        // bounded by the same rule — a task that left `Submitted`
        // sideways (cancel, remote completion) must not hold its
        // first-failure instant forever.
        ids.extend(self.elig_failures.lock().keys().copied());
        for id in ids {
            let still_submitted =
                matches!(self.store.task_state(id), Ok(Some(TaskState::Submitted)));
            if !still_submitted {
                self.backoff.lock().remove(&id);
                self.gated.lock().remove(&id);
                self.elig_failures.lock().remove(&id);
            }
        }
        // Worker-side reply obligations for CANCELLED tasks are
        // immortal without this sweep: a cancelled task never fires the
        // terminal pump, so `try_reply` never consumes the entry. The
        // issuer learns the outcome via replica gossip (ADR-0019), not
        // a result reply — dropping the obligation loses nothing.
        let owed: Vec<TaskId> = self.reply.lock().keys().copied().collect();
        for id in owed {
            if matches!(self.store.task_state(id), Ok(Some(TaskState::Cancelled))) {
                self.reply.lock().remove(&id);
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
            // 4.5: federated parents persist per-node contributions in
            // the V0006 column; everything else stays empty.
            provenance: row.provenance.unwrap_or_default(),
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
        // Feed the tracker only after the CAS accepted the result —
        // duplicate frames never double-count (review MINOR-6).
        self.success.record(from, msg.result.status == Status::Ok);
        // 4.6: ANY accepted result proves node liveness — even a Failed
        // one clears the breaker streak (task-level outcomes are not
        // node-health signals).
        self.breaker.record_ok(from);
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
                // 5.9 (ADR-0037): issuer-side cost gate — judged
                // against the ISSUER'S OWN local manifest (same
                // binary, same first-party hints), never the worker's
                // gossiped announcement. This row is the one the
                // coordinator's ledger reads.
                if let Ok(Some(task)) = self.store.load_task(task_id) {
                    if let Some(cap) = self.registry.get(&task.capability) {
                        if let Some(usd) = crate::cost_gate::gated_cost(
                            cap.manifest().cost_hint,
                            &msg.result.output,
                            &task.capability,
                        ) {
                            let _ = self.store.write_result_cost(task_id, usd);
                        }
                    }
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

    /// Issuer side: streaming line frames from a worker (3.2-stream,
    /// ADR-0020). The transport verified the envelope signature against
    /// the connection peer and the per-stream seq; the recv loop checked
    /// `msg.node_id == from`. Best-effort: frames land in the in-memory
    /// ring for `GET /tasks/{id}`; any validation failure just drops the
    /// partial — the terminal result is authoritative.
    fn on_partial(&self, from: NodeId, msg: PartialResult) {
        let Some(buffers) = self.partials.get() else {
            return;
        };
        // Only the node the task is currently assigned to may stream
        // partials for it.
        match self.store.assigned_node(msg.task_id) {
            Ok(Some(node)) if node == from => {}
            Ok(_) => {
                tracing::debug!(
                    target: "harness.dispatch",
                    %from,
                    task = %msg.task_id.0,
                    "partial from a node the task is not assigned to; dropped"
                );
                return;
            }
            Err(e) => {
                tracing::warn!(target: "harness.dispatch", ?e, "assigned_node on partial");
                return;
            }
        }
        let Some(frames) = msg.output_chunk.get("frames").and_then(|f| f.as_array()) else {
            tracing::debug!(
                target: "harness.dispatch",
                task = %msg.task_id.0,
                "partial without a frames array; dropped"
            );
            return;
        };
        for frame in frames {
            let stream = frame.get("stream").and_then(|s| s.as_str());
            let line = frame.get("line").and_then(|l| l.as_str());
            // "progress" (4.2, ADR-0024): structured per-target fan-out
            // telemetry rides the same pipe as child output lines.
            if let (Some(stream @ ("stdout" | "stderr" | "progress")), Some(line)) = (stream, line)
            {
                buffers.append(msg.task_id, stream, line.to_string());
            }
        }
        // 4.7 (ADR-0029): the worker reports frames its pending queue
        // overflowed before this batch — fold them into the task's
        // lossiness flag (`partials_dropped`).
        if let Some(dropped) = msg
            .output_chunk
            .get("dropped")
            .and_then(serde_json::Value::as_u64)
        {
            buffers.add_dropped(msg.task_id, dropped);
        }
    }

    /// Issuer side: the assign never made it onto the wire.
    fn on_assign_send_failed(&self, node: NodeId, lease_id: LeaseId) {
        tracing::warn!(target: "harness.dispatch", %node, "assign send failed; resetting lease");
        self.success.record(node, false);
        self.record_node_failure(node);
        // 4.6 (risk 9, plan review MAJOR-6): the ASYNC send-failure
        // path — a half-dead peer accepts the enqueue and fails in the
        // sender task — must back off too, or the retry loop hammers
        // every 100 ms for the whole eligibility window.
        let lease = self.store.fetch_lease(lease_id).ok().flatten();
        if let Ok(true) = self.store.expire_and_reset_task(lease_id) {
            if let Some(lease) = lease {
                if let Ok(Some(task)) = self.store.load_task(lease.task_id) {
                    self.schedule_backoff(task.id, &task.retry, lease.attempt);
                }
            }
        }
    }

    /// 4.6 (ADR-0028): a worker's rolling liveness proof. The transport
    /// verified the envelope signature against the connection peer and
    /// the recv loop checked `msg.worker == from`; here the store CAS
    /// additionally guards on the lease's own `worker_id`, so a stale
    /// or cross-attempt extension is a silent no-op. The new expiry is
    /// `now + EXTEND_HORIZON_MS`, hard-capped by the lease's ORIGINAL
    /// budget (`issued_at + lease TTL`) — a wedged or malicious
    /// extender can never hold a lease past the task's own declared
    /// budget (plan review BLOCKER-2), and the unconditional set means
    /// the first extension SHRINKS a long lease to the rolling horizon
    /// (fast dead-worker detection; plan review BLOCKER-1).
    fn on_lease_extend(&self, from: NodeId, msg: harness_core::LeaseExtend) {
        let Some(lease) = self.store.fetch_lease(msg.lease_id).ok().flatten() else {
            return;
        };
        if lease.task_id != msg.task_id {
            tracing::warn!(
                target: "harness.dispatch",
                %from,
                lease = %msg.lease_id.0,
                "lease-extend task_id does not match lease; dropped"
            );
            return;
        }
        // A task that already reached a terminal state (operator
        // cancel included) must not have its lease kept alive until
        // the budget cap (PR #49 review).
        match self.store.task_state(lease.task_id) {
            Ok(Some(TaskState::Dispatched | TaskState::Claimed | TaskState::Running)) => {}
            _ => return,
        }
        let Some(task) = self.store.load_task(lease.task_id).ok().flatten() else {
            return;
        };
        let budget_cap = lease
            .issued_at
            .saturating_add(u64::from(lease_ttl_ms(&task)));
        let new_expiry = now_unix_ms()
            .saturating_add(EXTEND_HORIZON_MS)
            .min(budget_cap);
        match self
            .store
            .extend_lease_for_worker(msg.lease_id, from, new_expiry)
        {
            Ok(true) => {
                tracing::trace!(
                    target: "harness.dispatch",
                    task = %lease.task_id.0,
                    %from,
                    new_expiry,
                    "lease extended"
                );
            }
            Ok(false) => {} // terminal lease / wrong worker: silent no-op
            Err(e) => {
                tracing::warn!(target: "harness.dispatch", ?e, "extend_lease_for_worker");
            }
        }
    }
}

impl DispatchRuntime {
    /// Worker side (4.6): send one `LeaseExtend` for `task_id` if we
    /// still owe its issuer a result. Resolves `{issuer, lease_id}`
    /// from the LIVE reply-obligation map on every call — a
    /// re-delivered assign's fresh lease is picked up automatically
    /// (plan review MAJOR-5). No obligation (locally-issued task, or
    /// already replied) = no-op. Fire-and-forget sends.
    pub(crate) fn send_lease_extend(&self, task_id: TaskId) {
        let Some((issuer, lease_id)) = self
            .reply
            .lock()
            .get(&task_id)
            .map(|o| (o.issuer, o.lease_id))
        else {
            return;
        };
        let msg = harness_core::LeaseExtend {
            seq: 0, // stamped per-stream by the sender task
            lease_id,
            task_id,
            worker: self.local_id,
            sig: harness_core::Signature::from_bytes([0u8; 64]),
        };
        let send = self
            .net()
            .ok_or(SendToError::NoConnection)
            .and_then(|net| net.send_to(issuer, OutboundMsg::LeaseExtend(msg)));
        if let Err(err) = send {
            // Best-effort: a lost extension degrades to the pre-4.6
            // timeout bound; the next tick retries.
            tracing::debug!(target: "harness.dispatch", task = %task_id.0, %err, "lease-extend send failed");
        }
    }

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
/// Exponential retry backoff for the attempt that just failed
/// (1-based), from the task's own `RetryPolicy` (4.6, ADR-0028).
/// Overflow-safe: exponent clamped, saturating math, `backoff_max_ms`
/// caps the result; zero-valued policy fields are floored to 1.
fn backoff_delay(retry: &harness_core::RetryPolicy, attempt: u32) -> Duration {
    let mult = u64::from(retry.backoff_multiplier.max(1));
    let exp = attempt.saturating_sub(1);
    // Exact growth until it saturates — an exponent clamp would silently
    // flatten valid schedules below `backoff_max_ms` (PR #49 review).
    let factor = mult.checked_pow(exp).unwrap_or(u64::MAX);
    let ms = u64::from(retry.backoff_initial_ms.max(1))
        .saturating_mul(factor)
        .min(u64::from(retry.backoff_max_ms.max(1)));
    Duration::from_millis(ms)
}

fn lease_ttl_ms(task: &Task) -> u32 {
    let timeout_plus_slack = u64::from(task.execution.timeout_ms).saturating_add(LEASE_SLACK_MS);
    let ttl = u64::from(task.execution.lease_ms).max(timeout_plus_slack);
    u32::try_from(ttl).unwrap_or(u32::MAX)
}

/// Fresh-first dispatch batch (4.4, carried risk 10): tasks NOT already
/// in the eligibility-failure map go first (submission order preserved
/// within each partition), so up to `batch` known-undispatchable tasks
/// can't starve fresh work. Known-failing tasks still retry whenever the
/// batch has room. Tradeoff (ADR-0026): under a sustained full batch of
/// fresh tasks, a known-failing task's terminal write is deferred past
/// the window until the first non-full poll — harmless while waiting.
fn select_batch(
    rows: Vec<harness_store::TaskRow>,
    gated: &std::collections::HashSet<TaskId>,
    known_failing: &std::collections::HashSet<TaskId>,
    batch: usize,
) -> Vec<harness_store::TaskRow> {
    let mut fresh = Vec::new();
    let mut waiting = Vec::new();
    let mut failing = Vec::new();
    for r in rows {
        if known_failing.contains(&r.id) {
            failing.push(r);
        } else if gated.contains(&r.id) {
            waiting.push(r);
        } else {
            fresh.push(r);
        }
    }
    fresh
        .into_iter()
        .chain(waiting)
        .chain(failing)
        .take(batch)
        .collect()
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
        signed_task_for(issuer, "echo")
    }

    fn signed_task_for(issuer: &Identity, capability: &str) -> Task {
        let mut t = Task {
            id: TaskId::new_v7(),
            parent: None,
            plan_id: None,
            capability: capability.into(),
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
        /// Clone of the runtime's peer table (shared `Arc` inner) so
        /// tests can mark peers live by recording heartbeats.
        peers: PeerTable,
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
        let peers = PeerTable::new();
        let runtime = DispatchRuntime::new(
            store.clone(),
            local.clone(),
            harness_capabilities::CapabilityRegistry::new(),
            Dispatcher::new(),
            trust,
            peers.clone(),
            Arc::new(harness_vault::PlaintextStore::empty()),
        );
        Fixture {
            runtime,
            store,
            local,
            remote,
            peers,
            _tmp: tmp,
        }
    }

    /// 4.5: a result row carrying provenance round-trips into a signed
    /// `FinalResult` whose signature still verifies — the wire field
    /// was live-but-empty before; this locks the populated path.
    #[tokio::test]
    async fn t00_build_final_result_signs_with_populated_provenance() {
        let f = fixture();
        let task = signed_task(&f.local);
        f.store.insert_task(&task).expect("insert");
        let provenance = vec![
            harness_core::NodeContribution {
                node_id: f.local.node_id(),
                status: harness_core::protocol::NodeStatus::Ok,
                duration_ms: 12,
                item_count: 3,
            },
            harness_core::NodeContribution {
                node_id: f.remote.node_id(),
                status: harness_core::protocol::NodeStatus::TimedOut,
                duration_ms: 1_000,
                item_count: 0,
            },
        ];
        f.store
            .write_task_result_done_with_provenance(
                task.id,
                &serde_json::json!({"items": [1, 2, 3]}),
                1_700_000_000_500,
                f.local.node_id(),
                &provenance,
            )
            .expect("write");

        let result = f
            .runtime
            .build_final_result(task.id)
            .expect("built from stored row");
        assert_eq!(result.provenance, provenance);
        assert_eq!(result.status, Status::Ok);
        result
            .verify_signature(f.local.public_key())
            .expect("signature must verify with provenance filled");
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

    /// 5.9 (ADR-0037): the issuer-side ingest applies the cost gate
    /// against the ISSUER'S OWN local manifest — a CloudPaid result's
    /// claimed cost_usd persists on the coordinator's row (the one
    /// the ledger reads), a LocalFast claim does not.
    #[tokio::test]
    async fn t01b_remote_result_ingest_gates_cost_on_local_manifest() {
        struct PaidEcho;
        #[async_trait::async_trait]
        impl harness_capabilities::Capability for PaidEcho {
            fn id(&self) -> &'static str {
                "paid.echo"
            }
            fn manifest(&self) -> harness_core::Capability {
                let mut m = harness_capabilities::Capability::manifest(
                    &harness_capabilities::MeshInfoCapability::new(),
                );
                m.id = "paid.echo".into();
                m.cost_hint = harness_core::protocol::CostHint::CloudPaid;
                m
            }
            async fn execute(
                &self,
                _ctx: &harness_capabilities::ExecutionContext,
                _input: serde_json::Value,
            ) -> Result<serde_json::Value, harness_capabilities::CapabilityError> {
                Ok(serde_json::json!({}))
            }
        }

        let f = fixture();
        f.runtime
            .registry
            .register(Arc::new(PaidEcho))
            .expect("register paid.echo");

        // CloudPaid task: claimed cost persists on the issuer's row.
        let task = signed_task_for(&f.local, "paid.echo");
        f.store.insert_task(&task).expect("insert");
        assert!(f
            .store
            .try_dispatch_task(task.id, f.remote.node_id())
            .expect("dispatch"));
        let lease = f
            .store
            .create_lease(task.id, f.remote.node_id(), 60_000, 1)
            .expect("lease");
        let mut result = signed_result(&f.remote, task.id, true);
        result.output = serde_json::json!({"text": "x", "cost_usd": 0.25});
        result.sign(&f.remote).expect("re-sign");
        f.runtime.on_result(
            f.remote.node_id(),
            TaskResultMsg {
                seq: 0,
                lease_id: lease.lease_id,
                result,
                sig: Signature::from_bytes([0u8; 64]),
            },
        );
        let row = f
            .store
            .load_task_result(task.id)
            .expect("load")
            .expect("row");
        assert_eq!(row.cost_usd, Some(0.25), "CloudPaid claim persists");

        // LocalFast task ("echo" is not even registered here — an
        // unknown local manifest also refuses): claim ignored.
        let task2 = signed_task_for(&f.local, "echo");
        f.store.insert_task(&task2).expect("insert");
        assert!(f
            .store
            .try_dispatch_task(task2.id, f.remote.node_id())
            .expect("dispatch"));
        let lease2 = f
            .store
            .create_lease(task2.id, f.remote.node_id(), 60_000, 1)
            .expect("lease");
        let mut result2 = signed_result(&f.remote, task2.id, true);
        result2.output = serde_json::json!({"echoed": "hi", "cost_usd": 1e9});
        result2.sign(&f.remote).expect("re-sign");
        f.runtime.on_result(
            f.remote.node_id(),
            TaskResultMsg {
                seq: 0,
                lease_id: lease2.lease_id,
                result: result2,
                sig: Signature::from_bytes([0u8; 64]),
            },
        );
        let row2 = f
            .store
            .load_task_result(task2.id)
            .expect("load")
            .expect("row");
        assert_eq!(row2.cost_usd, None, "unbacked claim never persists");
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

    fn partial_msg(worker: &Identity, task_id: TaskId, lines: &[(&str, &str)]) -> PartialResult {
        let frames: Vec<serde_json::Value> = lines
            .iter()
            .map(|(stream, line)| serde_json::json!({"stream": stream, "line": line}))
            .collect();
        PartialResult {
            task_id,
            node_id: worker.node_id(),
            seq: 0,
            progress: 0.0,
            output_chunk: serde_json::json!({ "frames": frames }),
            sig: Signature::from_bytes([0u8; 64]),
        }
    }

    #[tokio::test]
    async fn t11_on_partial_appends_frames_to_ring() {
        let f = fixture();
        let buffers = Arc::new(harness_api::PartialBuffers::new());
        f.runtime.attach_partials(buffers.clone());

        let task = signed_task(&f.local);
        f.store.insert_task(&task).expect("insert");
        assert!(f
            .store
            .try_dispatch_task(task.id, f.remote.node_id())
            .expect("dispatch"));

        f.runtime.on_partial(
            f.remote.node_id(),
            partial_msg(&f.remote, task.id, &[("stdout", "one"), ("stderr", "two")]),
        );
        f.runtime.on_partial(
            f.remote.node_id(),
            partial_msg(&f.remote, task.id, &[("stdout", "three")]),
        );
        // 4.2: "progress" frames are accepted; unknown kinds still drop.
        f.runtime.on_partial(
            f.remote.node_id(),
            partial_msg(
                &f.remote,
                task.id,
                &[("progress", r#"{"completed":1}"#), ("bogus", "nope")],
            ),
        );

        let frames = buffers.frames(task.id);
        let got: Vec<(String, String)> = frames
            .iter()
            .map(|fr| (fr.stream.clone(), fr.line.clone()))
            .collect();
        assert_eq!(
            got,
            vec![
                ("stdout".into(), "one".into()),
                ("stderr".into(), "two".into()),
                ("stdout".into(), "three".into()),
                ("progress".into(), r#"{"completed":1}"#.into()),
            ]
        );
        // Ring seqs are per-task append order.
        assert_eq!(frames[0].seq, 0);
        assert_eq!(frames[3].seq, 3);
    }

    #[tokio::test]
    async fn t12_on_partial_from_unassigned_node_dropped() {
        let f = fixture();
        let buffers = Arc::new(harness_api::PartialBuffers::new());
        f.runtime.attach_partials(buffers.clone());

        let task = signed_task(&f.local);
        f.store.insert_task(&task).expect("insert");
        // Assigned to some OTHER node, not `remote`.
        assert!(f
            .store
            .try_dispatch_task(task.id, Identity::generate().node_id())
            .expect("dispatch"));

        f.runtime.on_partial(
            f.remote.node_id(),
            partial_msg(&f.remote, task.id, &[("stdout", "spoofed")]),
        );
        assert!(
            buffers.frames(task.id).is_empty(),
            "partial from a non-assigned node must not land"
        );
    }

    #[tokio::test]
    async fn t13_on_partial_malformed_chunk_dropped() {
        let f = fixture();
        let buffers = Arc::new(harness_api::PartialBuffers::new());
        f.runtime.attach_partials(buffers.clone());

        let task = signed_task(&f.local);
        f.store.insert_task(&task).expect("insert");
        assert!(f
            .store
            .try_dispatch_task(task.id, f.remote.node_id())
            .expect("dispatch"));

        // No frames array — dropped, no panic.
        let mut malformed = partial_msg(&f.remote, task.id, &[]);
        malformed.output_chunk = serde_json::json!({"not_frames": 1});
        f.runtime.on_partial(f.remote.node_id(), malformed);
        // Unknown stream tag skipped, valid one kept.
        let mixed = partial_msg(
            &f.remote,
            task.id,
            &[("bogus", "skipped"), ("stdout", "kept")],
        );
        f.runtime.on_partial(f.remote.node_id(), mixed);

        let frames = buffers.frames(task.id);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].line, "kept");
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

    // ------------------------------------------------------------------
    // 3.6-encrypted: requires_secrets-aware routing (ADR-0021)
    // ------------------------------------------------------------------

    const SECRET_CAP: &str = "llm.cloud.test";
    const SECRET_TAG: &str = "secret/test-api-key";

    fn secret_capability() -> harness_core::Capability {
        harness_core::Capability {
            id: SECRET_CAP.into(),
            version: harness_core::SemVer::new(0, 1, 0),
            cardinality: Cardinality::Anyone,
            input_schema: serde_json::json!({}),
            output_schema: serde_json::json!({}),
            cost_hint: harness_core::protocol::CostHint::CloudPaid,
            tags: vec![],
            rate_limit: None,
            resource_hints: empty_hints(),
            requires_secrets: vec![SECRET_TAG.to_string()],
        }
    }

    fn signed_peer_manifest(id: &Identity, secret_tags: Vec<String>) -> harness_core::NodeManifest {
        let mut m = harness_core::NodeManifest {
            node_id: id.node_id(),
            hostname: "peer".into(),
            pubkey: *id.public_key(),
            capabilities: vec![secret_capability()],
            scopes: vec![],
            secret_tags,
            resources: harness_core::Resources {
                cpu_cores: 1,
                ram_total_mb: 1,
                gpu: None,
                os: "linux".into(),
                arch: "x86_64".into(),
            },
            online_since: 0,
            version: harness_core::SemVer::new(0, 1, 0),
            sig: Signature::from_bytes([0u8; 64]),
        };
        m.sign(id).expect("sign manifest");
        m
    }

    fn live_heartbeat(node: NodeId) -> harness_core::Heartbeat {
        harness_core::Heartbeat {
            node_id: node,
            seq: 1,
            timestamp: 1_700_000_000_000,
            replica_head: [0u8; 32],
            queue_depth: 0,
            cpu_busy_pct: 0,
            cpu_pinned_count: 0,
            ram_used_mb: 0,
            ram_total_mb: 0,
            gpu_used_mb: 0,
            gpu_total_mb: 0,
            capabilities_hash: [0u8; 16],
            in_flight: vec![],
            leader_belief: node,
            brain_score: 0,
            on_battery: false,
            paused: false,
            version: harness_core::SemVer::new(0, 1, 0),
            sig: Signature::from_bytes([0u8; 64]),
        }
    }

    /// Register `peer` as a live routing candidate: manifest into the
    /// dispatcher's capability index + the store mirror, heartbeat into
    /// the peer table.
    fn add_live_candidate(f: &Fixture, peer: &Identity, secret_tags: Vec<String>) {
        let m = signed_peer_manifest(peer, secret_tags);
        f.runtime.dispatcher.capability_index().upsert_node(&m);
        f.store.upsert_manifest(&m).expect("upsert manifest");
        f.peers.record(live_heartbeat(peer.node_id()));
    }

    /// 4.5 diff review MAJOR-2: with every coordination slot busy, a
    /// burst of queued federated parents must drop into the WAITING
    /// batch class — not camp in the fresh class and starve every other
    /// Submitted task for the length of a coordination.
    #[tokio::test]
    async fn t15_slot_starved_federated_parents_demote_to_waiting_class() {
        let f = fixture();
        // Local registry declares the Federated cardinality…
        f.runtime
            .registry
            .register(Arc::new(harness_capabilities::MeshInfoCapability::new()))
            .expect("register mesh.info");
        // …and a live peer advertises it (the candidate set).
        let peer = Identity::generate();
        let mut m = signed_peer_manifest(&peer, vec![]);
        m.capabilities = vec![harness_capabilities::Capability::manifest(
            &harness_capabilities::MeshInfoCapability::new(),
        )];
        m.sign(&peer).expect("re-sign");
        f.runtime.dispatcher.capability_index().upsert_node(&m);
        f.store.upsert_manifest(&m).expect("upsert");
        f.peers.record(live_heartbeat(peer.node_id()));

        // One more parent than there are coordination slots. The
        // coordinations hang (no executor / no net in this fixture), so
        // the slots stay busy.
        let n = crate::federated::MAX_FEDERATED_COORDINATORS + 1;
        let mut ids = Vec::new();
        for _ in 0..n {
            let t = signed_task_for(&f.local, "mesh.info");
            f.store.insert_task(&t).expect("insert");
            ids.push(t.id);
        }
        f.runtime.poll_submitted_once();

        let mut running = 0;
        let mut submitted = Vec::new();
        for id in &ids {
            match f.store.task_state(*id).expect("state").expect("present") {
                TaskState::Running => running += 1,
                TaskState::Submitted => submitted.push(*id),
                other => panic!("unexpected state {other:?}"),
            }
        }
        assert_eq!(running, crate::federated::MAX_FEDERATED_COORDINATORS);
        assert_eq!(submitted.len(), 1, "one parent had no slot");
        assert!(
            f.runtime.is_gated(submitted[0]),
            "slot-starved parent must batch in the waiting class"
        );
    }

    // ---- 4.6 (ADR-0028): retry / send-failure backoff -----------------

    #[test]
    fn backoff_delay_schedule_and_clamps() {
        let default_policy = RetryPolicy::default(); // 250ms, x2, max 30s
        assert_eq!(
            backoff_delay(&default_policy, 1),
            Duration::from_millis(250)
        );
        assert_eq!(
            backoff_delay(&default_policy, 2),
            Duration::from_millis(500)
        );
        assert_eq!(
            backoff_delay(&default_policy, 3),
            Duration::from_millis(1000)
        );
        // Max cap engages long before overflow could.
        assert_eq!(
            backoff_delay(&default_policy, 30),
            Duration::from_millis(30_000)
        );
        // PR #49 review: growth continues EXACTLY to the configured cap
        // — no hidden exponent clamp flattening valid schedules.
        let wide = RetryPolicy {
            max_attempts: 255,
            backoff_initial_ms: 1,
            backoff_multiplier: 2,
            backoff_max_ms: 1_000_000,
        };
        assert_eq!(backoff_delay(&wide, 18), Duration::from_millis(131_072));
        assert_eq!(backoff_delay(&wide, 21), Duration::from_millis(1_000_000));
        // Hostile/zero fields are floored, never panic or zero-delay.
        let zeros = RetryPolicy {
            max_attempts: 3,
            backoff_initial_ms: 0,
            backoff_multiplier: 0,
            backoff_max_ms: 0,
        };
        assert_eq!(backoff_delay(&zeros, 5), Duration::from_millis(1));
        // Saturating exponent: u32::MAX attempt with max multiplier.
        let hot = RetryPolicy {
            max_attempts: 255,
            backoff_initial_ms: u32::MAX,
            backoff_multiplier: u8::MAX,
            backoff_max_ms: u32::MAX,
        };
        assert_eq!(
            backoff_delay(&hot, u32::MAX),
            Duration::from_millis(u64::from(u32::MAX))
        );
    }

    /// Send-failure path (risk 9): a failed assign backs the task off in
    /// the waiting class instead of re-dispatching every poll.
    #[tokio::test(start_paused = true)]
    async fn t17_send_failure_backs_off_then_redispatches() {
        let f = fixture();
        let peer = Identity::generate();
        add_live_candidate(&f, &peer, vec![SECRET_TAG.to_string()]);

        let task = signed_task_for(&f.local, SECRET_CAP);
        f.store.insert_task(&task).expect("insert");

        // Poll 1: routed to the peer, enqueue fails (no net) → reset +
        // backoff (attempt 1 → 250 ms).
        f.runtime.poll_submitted_once();
        assert_eq!(
            f.store.task_state(task.id).unwrap(),
            Some(TaskState::Submitted)
        );
        assert_eq!(f.store.list_leases_for_task(task.id).unwrap().len(), 1);
        assert!(f.runtime.is_gated(task.id), "backing off in waiting class");

        // Poll 2, still inside the backoff window: NO new lease.
        tokio::time::advance(Duration::from_millis(50)).await;
        f.runtime.poll_submitted_once();
        assert_eq!(
            f.store.list_leases_for_task(task.id).unwrap().len(),
            1,
            "no re-dispatch while backing off"
        );

        // Past the delay: re-dispatched (second lease minted).
        tokio::time::advance(Duration::from_millis(300)).await;
        f.runtime.poll_submitted_once();
        assert_eq!(
            f.store.list_leases_for_task(task.id).unwrap().len(),
            2,
            "re-dispatch after the backoff elapses"
        );
    }

    /// Lease-expiry path: the reset task waits out its attempt's backoff.
    #[tokio::test(start_paused = true)]
    async fn t18_expired_lease_backs_off_before_redispatch() {
        let f = fixture();
        let peer = Identity::generate();
        add_live_candidate(&f, &peer, vec![SECRET_TAG.to_string()]);

        let task = signed_task_for(&f.local, SECRET_CAP);
        f.store.insert_task(&task).expect("insert");
        assert!(f
            .store
            .try_dispatch_task(task.id, peer.node_id())
            .expect("dispatch"));
        // Attempt 2 of 3 (non-terminal expiry), zero TTL: expired now.
        f.store
            .create_lease(task.id, peer.node_id(), 0, 2)
            .expect("lease");
        // find_expired is strict (`expires_at < now`, wall clock —
        // start_paused only freezes tokio time): step past the minting
        // millisecond.
        std::thread::sleep(Duration::from_millis(2));

        f.runtime.expire_pass();
        assert_eq!(
            f.store.task_state(task.id).unwrap(),
            Some(TaskState::Submitted),
            "non-terminal expiry resets for re-dispatch"
        );
        assert!(f.runtime.is_gated(task.id), "waiting class at reset time");

        // Attempt 2 backoff = 500 ms: inside it, no new lease.
        f.runtime.poll_submitted_once();
        assert_eq!(f.store.list_leases_for_task(task.id).unwrap().len(), 1);

        tokio::time::advance(Duration::from_millis(600)).await;
        f.runtime.poll_submitted_once();
        assert_eq!(
            f.store.list_leases_for_task(task.id).unwrap().len(),
            2,
            "re-dispatch after attempt-2 backoff"
        );
    }

    /// Plan review MAJOR-7: the deadline is enforced IN the backoff-skip
    /// path — a task must never be dispatched to a fresh worker past
    /// `constraints.deadline`.
    #[tokio::test(start_paused = true)]
    async fn t19_deadline_elapsed_during_backoff_is_terminal() {
        let f = fixture();
        let peer = Identity::generate();
        add_live_candidate(&f, &peer, vec![SECRET_TAG.to_string()]);

        let mut task = signed_task_for(&f.local, SECRET_CAP);
        // Deadline in the near "future" relative to task data — but
        // wall-clock-elapsed by the time the backoff poll runs.
        task.constraints.deadline = Some(1);
        task.sign(&f.local).expect("re-sign");
        f.store.insert_task(&task).expect("insert");

        // Poll 1 dispatches (deadline is only enforced on failure
        // paths); send fails → backoff.
        f.runtime.poll_submitted_once();
        assert_eq!(
            f.store.task_state(task.id).unwrap(),
            Some(TaskState::Submitted)
        );
        // Poll 2 inside the backoff window: deadline elapsed → terminal.
        f.runtime.poll_submitted_once();
        assert_eq!(
            f.store.task_state(task.id).unwrap(),
            Some(TaskState::Failed),
            "deadline enforced while backing off"
        );
        let row = f
            .store
            .load_task_result(task.id)
            .expect("load")
            .expect("row");
        assert!(row
            .error
            .expect("error")
            .contains("deadline exceeded while backing off"));
        assert!(!f.runtime.is_gated(task.id), "bookkeeping cleaned up");
    }

    /// Plan review MINOR-12: entries for tasks that left `Submitted`
    /// sideways are pruned, never immortal.
    #[tokio::test(start_paused = true)]
    async fn t20_backoff_bookkeeping_pruned_for_cancelled_tasks() {
        let f = fixture();
        let peer = Identity::generate();
        add_live_candidate(&f, &peer, vec![SECRET_TAG.to_string()]);

        let task = signed_task_for(&f.local, SECRET_CAP);
        f.store.insert_task(&task).expect("insert");
        f.runtime.poll_submitted_once(); // send-fail → backoff + gated
        assert!(f.runtime.is_gated(task.id));

        // Operator cancels the task out from under the backoff.
        f.store
            .transition_task(task.id, TaskState::Cancelled)
            .expect("cancel");
        f.runtime.expire_pass(); // prune pass
        assert!(
            !f.runtime.is_gated(task.id),
            "cancelled task pruned from waiting sets"
        );
    }

    // ---- 4.6 (ADR-0028): circuit breaker -------------------------------

    fn bench(f: &Fixture, node: NodeId) {
        for _ in 0..harness_orchestrator::dispatcher::BENCH_THRESHOLD {
            f.runtime.breaker().record_failure(node);
        }
        assert!(f.runtime.breaker().is_benched(&node));
    }

    /// A benched node is excluded from Anyone routing; a healthy peer
    /// takes every dispatch.
    #[tokio::test]
    async fn t24_benched_node_excluded_from_candidates() {
        let f = fixture();
        let benched = Identity::generate();
        let healthy = Identity::generate();
        add_live_candidate(&f, &benched, vec![SECRET_TAG.to_string()]);
        add_live_candidate(&f, &healthy, vec![SECRET_TAG.to_string()]);
        bench(&f, benched.node_id());

        for _ in 0..3 {
            let t = signed_task_for(&f.local, SECRET_CAP);
            f.store.insert_task(&t).expect("insert");
            f.runtime.poll_submitted_once();
            assert_eq!(
                f.store.last_dispatched(SECRET_CAP).expect("cursor"),
                Some(healthy.node_id()),
                "benched node must never be routed to"
            );
        }
    }

    /// All candidates benched ⇒ the task WAITS in the gated class (a
    /// ≤60 s transient), never entering the terminal eligibility
    /// window; a liveness proof un-benches and it dispatches.
    #[tokio::test]
    async fn t25_all_benched_waits_gated_not_terminal() {
        let f = fixture();
        let only = Identity::generate();
        add_live_candidate(&f, &only, vec![SECRET_TAG.to_string()]);
        bench(&f, only.node_id());

        let t = signed_task_for(&f.local, SECRET_CAP);
        f.store.insert_task(&t).expect("insert");
        f.runtime.poll_submitted_once();
        assert_eq!(
            f.store.task_state(t.id).unwrap(),
            Some(TaskState::Submitted)
        );
        assert!(f.runtime.is_gated(t.id), "all-benched waits in gated class");
        assert!(
            !f.runtime.has_elig_failure(t.id),
            "the terminal window must never start for a bench-gated task"
        );

        // Liveness proof clears the bench; the task dispatches.
        f.runtime.breaker().record_ok(only.node_id());
        f.runtime.poll_submitted_once();
        assert_eq!(
            f.store.list_leases_for_task(t.id).expect("leases").len(),
            1,
            "dispatched once the bench clears"
        );
    }

    /// Pinned tasks bypass the bench — operator intent wins.
    #[tokio::test]
    async fn t26_pin_bypasses_bench() {
        let f = fixture();
        let benched = Identity::generate();
        add_live_candidate(&f, &benched, vec![SECRET_TAG.to_string()]);
        bench(&f, benched.node_id());

        let mut t = signed_task_for(&f.local, SECRET_CAP);
        t.constraints.pin_to_node = Some(benched.node_id());
        t.sign(&f.local).expect("re-sign");
        f.store.insert_task(&t).expect("insert");
        f.runtime.poll_submitted_once();
        assert_eq!(
            f.store.list_leases_for_task(t.id).expect("leases").len(),
            1,
            "pinned dispatch proceeds to the benched node"
        );
    }

    /// Self is never benched: a burst of local failures must not gate
    /// a single-node install's queue.
    #[tokio::test]
    async fn t27_self_is_never_benched() {
        let f = fixture();
        for _ in 0..20 {
            // Through the guarded feed, not the raw breaker.
            f.runtime
                .on_assign_send_failed(f.local.node_id(), harness_core::LeaseId::new_v7());
        }
        assert!(!f.runtime.breaker().is_benched(&f.local.node_id()));
    }

    /// The node-health feed benches through the real failure path:
    /// five failed sends to the same peer.
    #[tokio::test]
    async fn t28_send_failures_feed_the_breaker() {
        let f = fixture();
        let peer = Identity::generate();
        add_live_candidate(&f, &peer, vec![SECRET_TAG.to_string()]);
        for _ in 0..harness_orchestrator::dispatcher::BENCH_THRESHOLD {
            let t = signed_task_for(&f.local, SECRET_CAP);
            f.store.insert_task(&t).expect("insert");
            assert!(f
                .store
                .try_dispatch_task(t.id, peer.node_id())
                .expect("cas"));
            let lease = f
                .store
                .create_lease(t.id, peer.node_id(), 5_000, 1)
                .expect("lease");
            f.runtime
                .on_assign_send_failed(peer.node_id(), lease.lease_id);
        }
        assert!(
            f.runtime.breaker().is_benched(&peer.node_id()),
            "five consecutive send failures bench the peer"
        );
    }

    // ---- 4.6 (ADR-0028): lease extension, issuer side -----------------

    fn seeded_lease(f: &Fixture, worker: NodeId, timeout_ms: u32) -> (Task, harness_store::Lease) {
        let mut task = signed_task_for(&f.local, SECRET_CAP);
        task.execution.timeout_ms = timeout_ms;
        task.sign(&f.local).expect("re-sign");
        f.store.insert_task(&task).expect("insert");
        assert!(f.store.try_dispatch_task(task.id, worker).expect("cas"));
        let ttl = lease_ttl_ms(&task);
        let lease = f
            .store
            .create_lease(task.id, worker, ttl, 1)
            .expect("lease");
        (task, lease)
    }

    /// BLOCKER-1: the first extension SHRINKS a long lease to the
    /// rolling horizon — fast dead-worker detection engages.
    #[tokio::test]
    async fn t21_extension_shrinks_long_lease_to_rolling_horizon() {
        let f = fixture();
        let worker = Identity::generate();
        // 60 s task ⇒ lease ≈ 60.7 s out. Horizon (test) = 2 s.
        let (task, lease) = seeded_lease(&f, worker.node_id(), 60_000);
        let original_expiry = lease.expires_at;

        f.runtime.on_lease_extend(
            worker.node_id(),
            harness_core::LeaseExtend {
                seq: 1,
                lease_id: lease.lease_id,
                task_id: task.id,
                worker: worker.node_id(),
                sig: Signature::from_bytes([0u8; 64]),
            },
        );
        let now = now_unix_ms();
        let extended = f
            .store
            .fetch_lease(lease.lease_id)
            .expect("fetch")
            .expect("present");
        assert!(
            extended.expires_at < original_expiry,
            "first extension must SHRINK the lease: {} !< {original_expiry}",
            extended.expires_at
        );
        assert!(
            extended.expires_at <= now + EXTEND_HORIZON_MS + 1_000 && extended.expires_at >= now,
            "rolling horizon: {} vs now {now}",
            extended.expires_at
        );
    }

    /// BLOCKER-2: extensions never exceed the lease's original budget.
    #[tokio::test]
    async fn t22_extension_clamped_by_original_budget() {
        let f = fixture();
        let worker = Identity::generate();
        // 1 s task ⇒ budget ≈ issued_at + 1.7 s (test slack 700ms),
        // SMALLER than the 2 s horizon: the clamp must engage.
        let (task, lease) = seeded_lease(&f, worker.node_id(), 1_000);
        let budget_cap = lease.issued_at + u64::from(lease_ttl_ms(&task));

        f.runtime.on_lease_extend(
            worker.node_id(),
            harness_core::LeaseExtend {
                seq: 1,
                lease_id: lease.lease_id,
                task_id: task.id,
                worker: worker.node_id(),
                sig: Signature::from_bytes([0u8; 64]),
            },
        );
        let extended = f
            .store
            .fetch_lease(lease.lease_id)
            .expect("fetch")
            .expect("present");
        assert!(
            extended.expires_at <= budget_cap,
            "extension past the original budget: {} > {budget_cap}",
            extended.expires_at
        );
    }

    /// Guards: wrong worker, task-id mismatch, terminal lease — all
    /// silent no-ops.
    #[tokio::test]
    async fn t23_extension_guards_reject_stale_and_forged() {
        let f = fixture();
        let worker = Identity::generate();
        let (task, lease) = seeded_lease(&f, worker.node_id(), 60_000);
        let original = f
            .store
            .fetch_lease(lease.lease_id)
            .expect("fetch")
            .expect("present")
            .expires_at;

        // Wrong worker (transport pins from; the store guard is the
        // second fence).
        f.runtime.on_lease_extend(
            NodeId::from_bytes([9; 16]),
            harness_core::LeaseExtend {
                seq: 1,
                lease_id: lease.lease_id,
                task_id: task.id,
                worker: NodeId::from_bytes([9; 16]),
                sig: Signature::from_bytes([0u8; 64]),
            },
        );
        // Task-id mismatch.
        f.runtime.on_lease_extend(
            worker.node_id(),
            harness_core::LeaseExtend {
                seq: 2,
                lease_id: lease.lease_id,
                task_id: TaskId::new_v7(),
                worker: worker.node_id(),
                sig: Signature::from_bytes([0u8; 64]),
            },
        );
        assert_eq!(
            f.store
                .fetch_lease(lease.lease_id)
                .expect("fetch")
                .expect("present")
                .expires_at,
            original,
            "guarded extensions must not move the expiry"
        );

        // Terminal lease (cross-attempt replay shape): no resurrection.
        assert!(f
            .store
            .try_expire_lease(lease.lease_id, u64::MAX)
            .expect("expire"));
        f.runtime.on_lease_extend(
            worker.node_id(),
            harness_core::LeaseExtend {
                seq: 3,
                lease_id: lease.lease_id,
                task_id: task.id,
                worker: worker.node_id(),
                sig: Signature::from_bytes([0u8; 64]),
            },
        );
        let after = f
            .store
            .fetch_lease(lease.lease_id)
            .expect("fetch")
            .expect("present");
        assert_eq!(after.state, harness_store::LeaseState::Expired);
    }

    #[tokio::test]
    async fn t11_requires_secrets_routes_only_to_tag_holder() {
        let f = fixture();
        let with_tag = Identity::generate();
        let without_tag = Identity::generate();
        add_live_candidate(&f, &with_tag, vec![SECRET_TAG.to_string()]);
        add_live_candidate(&f, &without_tag, vec![]);

        // Three polls: the tag-holder must be chosen every time, never
        // the other node (round-robin must not rotate onto it). The
        // assign send fails (no PeerNet attached) so the task resets to
        // Submitted after each poll — which is exactly what lets us
        // re-poll; `last_dispatched` records each routing choice.
        for _ in 0..3 {
            let task = {
                let t = signed_task_for(&f.local, SECRET_CAP);
                f.store.insert_task(&t).expect("insert");
                t
            };
            f.runtime.poll_submitted_once();
            assert_eq!(
                f.store.last_dispatched(SECRET_CAP).expect("cursor"),
                Some(with_tag.node_id()),
                "must route to the node advertising the required tag"
            );
            // The lease proves the dispatch targeted the tag-holder.
            let leases = f.store.list_leases_for_task(task.id).expect("leases");
            assert_eq!(leases.len(), 1);
        }
    }

    #[tokio::test]
    async fn t12_requires_secrets_no_holder_is_undispatchable() {
        let f = fixture();
        let without_tag = Identity::generate();
        add_live_candidate(&f, &without_tag, vec![]);

        // Deadline already elapsed → the first eligibility failure is
        // terminal (no need to wait out the retry window).
        let mut task = signed_task_for(&f.local, SECRET_CAP);
        task.constraints.deadline = Some(1);
        f.store.insert_task(&task).expect("insert");

        f.runtime.poll_submitted_once();

        assert_eq!(
            f.store.task_state(task.id).expect("state"),
            Some(TaskState::Failed),
            "no node holds the tag → terminal undispatchable failure"
        );
        let row = f
            .store
            .load_task_result(task.id)
            .expect("load")
            .expect("row");
        let err = row.error.as_deref().unwrap_or("");
        assert!(
            err.contains("undispatchable"),
            "error must flow through the existing undispatchable path: {err:?}"
        );
        assert!(
            f.store
                .list_leases_for_task(task.id)
                .expect("leases")
                .is_empty(),
            "no lease may be minted for a filtered-out node"
        );
    }

    #[tokio::test]
    async fn t13_node_has_required_secrets_cases() {
        let f = fixture();
        let with_tag = Identity::generate();
        let without_tag = Identity::generate();
        add_live_candidate(&f, &with_tag, vec![SECRET_TAG.to_string()]);
        add_live_candidate(&f, &without_tag, vec![]);
        let unknown = Identity::generate(); // no manifest stored

        assert!(f
            .runtime
            .node_has_required_secrets(with_tag.node_id(), SECRET_CAP));
        assert!(!f
            .runtime
            .node_has_required_secrets(without_tag.node_id(), SECRET_CAP));
        // No manifest on file → cannot judge → not filtered (routing
        // optimization, not a security boundary; ADR-0021).
        assert!(f
            .runtime
            .node_has_required_secrets(unknown.node_id(), SECRET_CAP));
        // A capability the manifest doesn't list → not filtered.
        assert!(f
            .runtime
            .node_has_required_secrets(without_tag.node_id(), "echo"));
        // Self: empty local registry declares no requirements → true.
        assert!(f
            .runtime
            .node_has_required_secrets(f.local.node_id(), SECRET_CAP));
    }

    #[test]
    fn t20_select_batch_fresh_first() {
        let mk = |n: u8| harness_store::TaskRow {
            id: TaskId::new_v7(),
            capability: format!("c{n}"),
            state: TaskState::Submitted,
            issued_by: NodeId::from_bytes([n; 16]),
            issued_at: u64::from(n),
            completed_by: None,
            started_at: None,
            finished_at: None,
            parent: None,
            plan_id: None,
        };
        let rows: Vec<_> = (0..6).map(mk).collect();
        let failing: std::collections::HashSet<TaskId> =
            [rows[0].id, rows[1].id].into_iter().collect();
        // Row 2 is resource-gated: batched AFTER fresh, BEFORE failing
        // (review BLOCKER-1 — gated work can't starve other tasks).
        let gated: std::collections::HashSet<TaskId> = [rows[2].id].into_iter().collect();
        let batch = select_batch(rows.clone(), &gated, &failing, 3);
        assert_eq!(batch[0].id, rows[3].id, "never-seen first");
        assert_eq!(batch[1].id, rows[4].id);
        assert_eq!(batch[2].id, rows[5].id);
        let batch = select_batch(rows.clone(), &gated, &failing, 6);
        assert_eq!(batch[3].id, rows[2].id, "gated after fresh");
        assert_eq!(batch[4].id, rows[0].id, "failing last, in order");
        // Empty and all-failing cases.
        assert!(select_batch(vec![], &gated, &failing, 3).is_empty());
        let all: std::collections::HashSet<TaskId> = rows.iter().map(|r| r.id).collect();
        let none = std::collections::HashSet::new();
        assert_eq!(select_batch(rows, &none, &all, 2).len(), 2);
    }

    #[tokio::test]
    async fn t24_gated_backlog_cannot_starve_other_capabilities() {
        // BLOCKER-1 regression: >BATCH resource-gated tasks for a
        // paused capability must not block a fresh task for a healthy
        // one.
        let f = fixture();
        let paused_node = Identity::generate();
        add_live_candidate(&f, &paused_node, vec![SECRET_TAG.to_string()]);
        let mut hb = live_heartbeat(paused_node.node_id());
        hb.paused = true;
        hb.seq = 2;
        f.peers.record(hb);
        // 20 gated tasks for the paused capability (all deadline-less).
        for _ in 0..20 {
            let t = signed_task_for(&f.local, SECRET_CAP);
            f.store.insert_task(&t).expect("insert");
        }
        // Two polls: first marks them gated, second exercises priority.
        f.runtime.poll_submitted_once();
        // A fresh task for a DIFFERENT capability with a healthy node.
        let healthy = Identity::generate();
        let m = {
            let mut m = signed_peer_manifest(&healthy, vec![]);
            m.capabilities[0].id = "other.cap".into();
            m.capabilities[0].requires_secrets = vec![];
            m.sign(&healthy).expect("re-sign");
            m
        };
        f.runtime.dispatcher.capability_index().upsert_node(&m);
        f.store.upsert_manifest(&m).expect("upsert");
        f.peers.record(live_heartbeat(healthy.node_id()));
        let fresh = signed_task_for(&f.local, "other.cap");
        f.store.insert_task(&fresh).expect("insert");
        f.runtime.poll_submitted_once();
        assert_eq!(
            f.store.last_dispatched("other.cap").expect("cursor"),
            Some(healthy.node_id()),
            "fresh task must dispatch despite 20 older gated tasks"
        );
        // Gated tasks are still Submitted (waiting, not failed).
        assert!(
            f.store
                .list_tasks_by_state_assigned(TaskState::Submitted, None)
                .expect("list")
                .len()
                >= 20
        );
    }

    #[tokio::test]
    async fn t21_scored_dispatch_prefers_less_loaded_node() {
        let f = fixture();
        let a = Identity::generate();
        let b = Identity::generate();
        add_live_candidate(&f, &a, vec![SECRET_TAG.to_string()]);
        add_live_candidate(&f, &b, vec![SECRET_TAG.to_string()]);
        // Load node A with 3 in-flight rows.
        for _ in 0..3 {
            let t = signed_task_for(&f.local, SECRET_CAP);
            f.store.insert_task(&t).expect("insert");
            assert!(f.store.try_dispatch_task(t.id, a.node_id()).expect("cas"));
        }
        // Fresh task must route to the idle node B regardless of RR.
        for _ in 0..2 {
            let t = signed_task_for(&f.local, SECRET_CAP);
            f.store.insert_task(&t).expect("insert");
            f.runtime.poll_submitted_once();
            assert_eq!(
                f.store.last_dispatched(SECRET_CAP).expect("cursor"),
                Some(b.node_id()),
                "idle node must win argmax over the loaded one"
            );
        }
    }

    #[tokio::test]
    async fn t22_paused_node_waits_instead_of_terminal_failure() {
        // BLOCKER-1 regression: a load-gated task must NOT be failed
        // after the eligibility window — it waits like queued work.
        let f = fixture();
        let a = Identity::generate();
        add_live_candidate(&f, &a, vec![SECRET_TAG.to_string()]);
        let mut hb = live_heartbeat(a.node_id());
        hb.paused = true;
        hb.seq = 2;
        f.peers.record(hb);

        let t = signed_task_for(&f.local, SECRET_CAP);
        f.store.insert_task(&t).expect("insert");
        // Poll past the (test) eligibility window.
        f.runtime.poll_submitted_once();
        tokio::time::sleep(Duration::from_millis(ELIGIBILITY_WINDOW_MS + 200)).await;
        f.runtime.poll_submitted_once();
        assert_eq!(
            f.store.task_state(t.id).expect("state"),
            Some(TaskState::Submitted),
            "resource-gated task keeps waiting"
        );
        // Node un-pauses → next poll dispatches it.
        let mut hb = live_heartbeat(a.node_id());
        hb.seq = 3;
        f.peers.record(hb);
        f.runtime.poll_submitted_once();
        assert_eq!(
            f.store.last_dispatched(SECRET_CAP).expect("cursor"),
            Some(a.node_id()),
            "gate lifts when the node resumes"
        );
    }

    #[tokio::test]
    async fn t23_success_feed_flips_selection_to_reliable_node() {
        let f = fixture();
        let a = Identity::generate();
        let b = Identity::generate();
        add_live_candidate(&f, &a, vec![SECRET_TAG.to_string()]);
        add_live_candidate(&f, &b, vec![SECRET_TAG.to_string()]);
        // Node A accumulates failures (as the send-failed / expiry /
        // result feed points would produce them).
        for _ in 0..25 {
            f.runtime.success.record(a.node_id(), false);
        }
        assert!(f.runtime.success.rate(&a.node_id()) < 0.1);
        let t = signed_task_for(&f.local, SECRET_CAP);
        f.store.insert_task(&t).expect("insert");
        f.runtime.poll_submitted_once();
        assert_eq!(
            f.store.last_dispatched(SECRET_CAP).expect("cursor"),
            Some(b.node_id()),
            "unreliable node must lose ranking"
        );
    }

    /// 4.7 (ADR-0029, plan review MAJOR-3): a paused node stops
    /// dispatching to ITSELF — the self snapshot reads the local
    /// `PauseState` (there is no `PeerTable` self entry). Resume
    /// dispatches; already-`Dispatched` rows keep draining (executor).
    #[tokio::test]
    async fn t29_self_pause_gates_dispatch_to_self_until_resume() {
        let f = fixture();
        let pause = crate::pause::PauseState::new();
        f.runtime.attach_pause(pause.clone());
        let m = signed_peer_manifest(&f.local, vec![]);
        f.runtime.dispatcher.capability_index().upsert_node(&m);
        f.store.upsert_manifest(&m).expect("upsert");

        let mut task = signed_task_for(&f.local, SECRET_CAP);
        task.constraints.pin_to_node = Some(f.local.node_id());
        task.sign(&f.local).expect("re-sign");
        f.store.insert_task(&task).expect("insert");

        pause.set_operator(true);
        f.runtime.poll_submitted_once();
        assert_eq!(
            f.store.task_state(task.id).unwrap(),
            Some(TaskState::Submitted),
            "paused self never receives new work"
        );
        assert!(f.runtime.is_gated(task.id), "waits in the gated class");

        pause.set_operator(false);
        f.runtime.poll_submitted_once();
        assert_eq!(
            f.store.task_state(task.id).unwrap(),
            Some(TaskState::Dispatched),
            "resume dispatches to self"
        );
        assert!(!f.runtime.is_gated(task.id));
    }

    /// 4.7: a live peer advertising `paused` in its heartbeat WAITS
    /// pinned work (no lease minted), and the route resumes on the
    /// first unpaused heartbeat. Dead pins keep their fast-terminal
    /// path (s08 unit + m08).
    #[tokio::test]
    async fn t30_paused_peer_heartbeat_gates_pinned_dispatch() {
        let f = fixture();
        let peer = Identity::generate();
        add_live_candidate(&f, &peer, vec![SECRET_TAG.to_string()]);
        let mut hb = live_heartbeat(peer.node_id());
        hb.seq = 2;
        hb.paused = true;
        f.peers.record(hb);

        let mut task = signed_task_for(&f.local, SECRET_CAP);
        task.constraints.pin_to_node = Some(peer.node_id());
        task.sign(&f.local).expect("re-sign");
        f.store.insert_task(&task).expect("insert");

        f.runtime.poll_submitted_once();
        assert_eq!(
            f.store.task_state(task.id).unwrap(),
            Some(TaskState::Submitted)
        );
        assert_eq!(
            f.store.list_leases_for_task(task.id).unwrap().len(),
            0,
            "no lease minted toward a paused pin"
        );
        assert!(f.runtime.is_gated(task.id));

        let mut hb = live_heartbeat(peer.node_id());
        hb.seq = 3;
        f.peers.record(hb);
        f.runtime.poll_submitted_once();
        assert_eq!(
            f.store.list_leases_for_task(task.id).unwrap().len(),
            1,
            "unpaused heartbeat resumes the route (lease minted; the \
             no-net send failure afterwards is t17's territory)"
        );
    }

    /// 4.7 (plan review BLOCKER-2): a sub-task parked behind a paused
    /// pin terminalizes once its deadline passes — and can never run
    /// posthumously after an un-pause.
    #[tokio::test]
    async fn t31_paused_pin_deadline_elapse_terminal_never_posthumous() {
        let f = fixture();
        let pause = crate::pause::PauseState::new();
        f.runtime.attach_pause(pause.clone());
        let m = signed_peer_manifest(&f.local, vec![]);
        f.runtime.dispatcher.capability_index().upsert_node(&m);
        f.store.upsert_manifest(&m).expect("upsert");

        let mut task = signed_task_for(&f.local, SECRET_CAP);
        task.constraints.pin_to_node = Some(f.local.node_id());
        task.constraints.deadline = Some(1); // long since elapsed
        task.sign(&f.local).expect("re-sign");
        f.store.insert_task(&task).expect("insert");

        pause.set_operator(true);
        f.runtime.poll_submitted_once();
        assert_eq!(
            f.store.task_state(task.id).unwrap(),
            Some(TaskState::Failed),
            "deadline-elapsed paused-pin terminalizes"
        );
        let row = f
            .store
            .load_task_result(task.id)
            .expect("load")
            .expect("row");
        assert!(row
            .error
            .expect("error")
            .contains("deadline exceeded while"));

        pause.set_operator(false);
        f.runtime.poll_submitted_once();
        assert_eq!(
            f.store.task_state(task.id).unwrap(),
            Some(TaskState::Failed),
            "never dispatched after un-pause"
        );
        assert_eq!(f.store.list_leases_for_task(task.id).unwrap().len(), 0);
    }

    /// 4.7 (ADR-0029): the reply pump survives `Lagged` on the
    /// terminal broadcast — it logs, skips (the issuer recovers those
    /// via lease expiry + assign-time terminal-resend, ADR-0017), and
    /// KEEPS processing later terminals. Capacity-1 channel makes the
    /// lag deterministic (current-thread runtime: the burst outruns
    /// the pump's first poll).
    #[tokio::test(flavor = "current_thread")]
    async fn t32_reply_pump_survives_lag_and_processes_later_terminals() {
        let f = fixture();
        let ids: Vec<TaskId> = (0..3).map(|_| TaskId::new_v7()).collect();
        for id in &ids {
            f.runtime.reply.lock().insert(
                *id,
                ReplyObligation {
                    issuer: f.remote.node_id(),
                    lease_id: harness_core::LeaseId::new_v7(),
                },
            );
        }
        let (terminal_tx, terminal_rx) = tokio::sync::broadcast::channel(1);
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let pump = tokio::spawn(f.runtime.clone().run_reply_pump(terminal_rx, shutdown_rx));
        for id in &ids {
            let _ = terminal_tx.send(*id);
        }
        for _ in 0..200 {
            if !f.runtime.reply.lock().contains_key(&ids[2]) {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            !f.runtime.reply.lock().contains_key(&ids[2]),
            "the post-lag terminal was processed — the pump did not exit"
        );
        assert!(
            f.runtime.reply.lock().contains_key(&ids[0])
                && f.runtime.reply.lock().contains_key(&ids[1]),
            "lagged obligations remain for the assign-time resend to cover"
        );
        shutdown_tx.send(true).expect("shutdown");
        pump.await.expect("pump join");
    }

    /// 4.7 (ADR-0029, plan risk #13): the prune sweep also bounds the
    /// eligibility-failure window map and worker-side reply obligations
    /// for cancelled tasks — neither entry class is immortal.
    #[tokio::test]
    async fn t33_prune_covers_elig_failures_and_cancelled_reply_obligations() {
        let f = fixture();
        // No candidates for this capability → eligibility failure entry.
        let task = signed_task_for(&f.local, "no.such.capability");
        f.store.insert_task(&task).expect("insert");
        f.runtime.poll_submitted_once();
        assert!(
            f.runtime.elig_failures.lock().contains_key(&task.id),
            "failure window opened"
        );
        // Cancelled out from under the window: the entry must not
        // survive the next prune pass.
        f.store
            .transition_task(task.id, TaskState::Cancelled)
            .expect("cancel");
        f.runtime.expire_pass();
        assert!(
            !f.runtime.elig_failures.lock().contains_key(&task.id),
            "elig_failures pruned"
        );

        // Worker side: an obligation for a CANCELLED task never fires
        // the terminal pump — the sweep is its only exit.
        let cancelled = signed_task_for(&f.remote, SECRET_CAP);
        f.store.insert_task(&cancelled).expect("insert");
        f.store
            .transition_task(cancelled.id, TaskState::Cancelled)
            .expect("cancel");
        f.runtime.reply.lock().insert(
            cancelled.id,
            ReplyObligation {
                issuer: f.remote.node_id(),
                lease_id: harness_core::LeaseId::new_v7(),
            },
        );
        // A live obligation (Submitted task) survives the same pass.
        let live = signed_task_for(&f.remote, SECRET_CAP);
        f.store.insert_task(&live).expect("insert");
        f.runtime.reply.lock().insert(
            live.id,
            ReplyObligation {
                issuer: f.remote.node_id(),
                lease_id: harness_core::LeaseId::new_v7(),
            },
        );
        f.runtime.expire_pass();
        assert!(
            !f.runtime.reply.lock().contains_key(&cancelled.id),
            "cancelled obligation swept"
        );
        assert!(
            f.runtime.reply.lock().contains_key(&live.id),
            "live obligation untouched"
        );
    }

    /// 4.7 (diff review MINOR-1): the gate opening and the deadline
    /// elapsing inside the SAME poll gap must still terminalize — the
    /// success-path dispatch never runs a gated task posthumously.
    #[tokio::test]
    async fn t34_gate_opening_and_deadline_racing_still_terminal() {
        let f = fixture();
        let pause = crate::pause::PauseState::new();
        f.runtime.attach_pause(pause.clone());
        let m = signed_peer_manifest(&f.local, vec![]);
        f.runtime.dispatcher.capability_index().upsert_node(&m);
        f.store.upsert_manifest(&m).expect("upsert");

        let mut task = signed_task_for(&f.local, SECRET_CAP);
        task.constraints.pin_to_node = Some(f.local.node_id());
        // Deadline in the near future: alive at poll 1, elapsed by
        // poll 2 (wall clock — the deadline checks read now_unix_ms).
        task.constraints.deadline = Some(now_unix_ms() + 50);
        task.sign(&f.local).expect("re-sign");
        f.store.insert_task(&task).expect("insert");

        pause.set_operator(true);
        f.runtime.poll_submitted_once();
        assert_eq!(
            f.store.task_state(task.id).unwrap(),
            Some(TaskState::Submitted),
            "poll 1: gated behind the pause, deadline still alive"
        );
        assert!(f.runtime.is_gated(task.id));

        // The gap: deadline elapses AND the gate opens before the next
        // poll ever observes the (paused ∧ expired) combination.
        std::thread::sleep(Duration::from_millis(60));
        pause.set_operator(false);
        f.runtime.poll_submitted_once();
        assert_eq!(
            f.store.task_state(task.id).unwrap(),
            Some(TaskState::Failed),
            "poll 2 must terminalize, never dispatch posthumously"
        );
        let row = f
            .store
            .load_task_result(task.id)
            .expect("load")
            .expect("row");
        assert!(row
            .error
            .expect("error")
            .contains("deadline exceeded while resource-gated"));
        assert!(!f.runtime.is_gated(task.id));
    }
}
