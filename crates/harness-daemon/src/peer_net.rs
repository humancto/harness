//! [`PeerNet`] — the daemon's per-peer connection registry and channel
//! router (3.3-fanout PR-A1, ADR-0017).
//!
//! Owns everything the review's R1/R5/R7 findings demanded:
//!
//! - **`NodeId`-keyed `ConnMap`** with a deterministic duplicate tiebreak
//!   both endpoints compute identically: keep the connection whose
//!   *dialer* is `min(local_id, peer_id)`; among same-dialer duplicates
//!   keep the incumbent. The loser is closed (R5).
//! - **One accept-router task per connection** — the sole `accept_bi`
//!   consumer. Every stream is named (heartbeats included); the router
//!   spawns one recv task per accepted channel (R1).
//! - **One bounded outbound queue + sender task per connection** — recv
//!   tasks never write to the wire; every wire write goes through the
//!   queue (R7). Send errors evict the cached channel and retry once;
//!   a failed `TaskAssign` reports back via
//!   [`TaskChannelHandlers::on_assign_send_failed`].
//! - **Manifest announce on adopt**: each side enqueues its signed
//!   `NodeManifest`; on receipt (signature verified against the
//!   connection pubkey, node identity cross-checked) the capability and
//!   scope indexes + the store's manifest table are updated.
//!
//! The task-channel payloads (`assign`/`claim`/`result`) are delivered
//! to a [`TaskChannelHandlers`] implementation. PR-A1 ships
//! [`NoopHandlers`] (log-and-drop; nothing sends these yet) — the PR-A2
//! dispatch runtime replaces it.

use std::collections::HashMap;
use std::sync::Arc;

use harness_core::{
    Heartbeat, Identity, LeaseId, NodeId, NodeManifest, Signable, TaskAssign, TaskClaim,
    TaskResultMsg,
};
use harness_mesh::heartbeat::{HeartbeatService, PeerTable, HEARTBEAT_INTERVAL, PEER_TIMEOUT};
use harness_mesh::transport::{channels, ChannelStream, Connection, TransportError};
use harness_orchestrator::{CapabilityIndex, ScopeIndex};
use harness_store::Store;
use parking_lot::Mutex as ParkingMutex;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// Bounded per-peer outbound queue depth. Full queue = backpressure:
/// the enqueue fails and the caller handles it (for assigns, by
/// resetting the lease so the task re-dispatches).
const OUTBOUND_QUEUE_DEPTH: usize = 64;

/// Sink for inbound task-channel envelopes. The transport layer has
/// already verified the outer signature against the connection peer and
/// the per-stream sequence; `from` is the connection peer's node id.
/// Implementations must be non-blocking (called from recv tasks).
pub(crate) trait TaskChannelHandlers: Send + Sync + 'static {
    fn on_assign(&self, from: NodeId, msg: TaskAssign);
    fn on_claim(&self, from: NodeId, msg: TaskClaim);
    fn on_result(&self, from: NodeId, msg: TaskResultMsg);
    /// An enqueued `TaskAssign` could not be delivered (queue full, send
    /// error after retry, or no live connection). The dispatcher resets
    /// the lease so the task re-routes.
    fn on_assign_send_failed(&self, node: NodeId, lease_id: LeaseId) {
        let _ = (node, lease_id);
    }
}

/// Log-and-drop handlers for tests and for daemon configurations with
/// no dispatch runtime.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct NoopHandlers;

impl TaskChannelHandlers for NoopHandlers {
    fn on_assign(&self, from: NodeId, msg: TaskAssign) {
        tracing::warn!(target: "harness.fanout", %from, task = %msg.task.id.0, "assign received but no dispatch runtime is wired (PR-A2)");
    }
    fn on_claim(&self, from: NodeId, msg: TaskClaim) {
        tracing::warn!(target: "harness.fanout", %from, task = %msg.task_id.0, "claim received but no dispatch runtime is wired (PR-A2)");
    }
    fn on_result(&self, from: NodeId, msg: TaskResultMsg) {
        tracing::warn!(target: "harness.fanout", %from, task = %msg.result.task_id.0, "result received but no dispatch runtime is wired (PR-A2)");
    }
}

/// The dispatcher-facing routing indexes, fed by manifest announces.
pub(crate) struct MeshIndexes {
    pub caps: Arc<CapabilityIndex>,
    pub scopes: Arc<ScopeIndex>,
    /// Persistent mirror (`Store::upsert_manifest`). `None` in unit
    /// tests that only exercise the in-memory indexes.
    pub store: Option<Store>,
}

impl MeshIndexes {
    pub(crate) fn upsert(&self, manifest: &NodeManifest) {
        self.caps.upsert_node(manifest);
        self.scopes.upsert_node(manifest);
        if let Some(store) = &self.store {
            if let Err(err) = store.upsert_manifest(manifest) {
                tracing::warn!(target: "harness.fanout", ?err, "manifest store upsert failed");
            }
        }
    }
}

/// One outbound message. `Heartbeat` and `Announce` arrive pre-signed
/// (their seq/idempotency is channel-independent); the task envelopes
/// arrive **unsigned with a placeholder seq** — the sender task stamps
/// the per-stream seq and signs with the local identity right before
/// the wire write, because the seq is a per-stream property.
// Variant sizes differ (Assign carries a whole Task) but entries are
// transient queue items with depth 64 — boxing would just add churn.
#[allow(clippy::large_enum_variant)]
#[allow(dead_code)] // Assign/Claim/Result senders land in PR-A2
pub(crate) enum OutboundMsg {
    Heartbeat(Heartbeat),
    Announce(NodeManifest),
    Assign(TaskAssign),
    Claim(TaskClaim),
    Result(TaskResultMsg),
}

impl OutboundMsg {
    fn channel_name(&self) -> &'static str {
        match self {
            OutboundMsg::Heartbeat(_) => channels::HEARTBEAT,
            OutboundMsg::Announce(_) => channels::ANNOUNCE,
            OutboundMsg::Assign(_) => channels::TASK_ASSIGN,
            OutboundMsg::Claim(_) => channels::TASK_CLAIM,
            OutboundMsg::Result(_) => channels::TASK_RESULT,
        }
    }
}

/// Failure to hand a message to a peer's outbound queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(not(test), allow(dead_code))] // consumed by the PR-A2 dispatch runtime
pub(crate) enum SendToError {
    NoConnection,
    QueueFull,
}

impl std::fmt::Display for SendToError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SendToError::NoConnection => f.write_str("no live connection to node"),
            SendToError::QueueFull => f.write_str("outbound queue full"),
        }
    }
}

impl std::error::Error for SendToError {}

struct PeerEntry {
    conn: Arc<Connection>,
    out_tx: mpsc::Sender<OutboundMsg>,
    tasks: Vec<JoinHandle<()>>,
}

impl PeerEntry {
    fn teardown(&self) {
        self.conn.close_ref();
        for t in &self.tasks {
            t.abort();
        }
    }
}

/// See the module docs.
pub(crate) struct PeerNet {
    local_id: NodeId,
    identity: Arc<Identity>,
    heartbeat: Arc<HeartbeatService>,
    indexes: Arc<MeshIndexes>,
    handlers: Arc<dyn TaskChannelHandlers>,
    self_manifest: NodeManifest,
    peers: ParkingMutex<HashMap<NodeId, PeerEntry>>,
}

impl PeerNet {
    pub(crate) fn new(
        identity: Arc<Identity>,
        heartbeat: Arc<HeartbeatService>,
        indexes: Arc<MeshIndexes>,
        handlers: Arc<dyn TaskChannelHandlers>,
        self_manifest: NodeManifest,
    ) -> Arc<Self> {
        // Our own manifest is routing state too (self is always an
        // eligible node).
        indexes.upsert(&self_manifest);
        Arc::new(Self {
            local_id: identity.node_id(),
            identity,
            heartbeat,
            indexes,
            handlers,
            self_manifest,
            peers: ParkingMutex::new(HashMap::new()),
        })
    }

    #[cfg_attr(not(test), allow(dead_code))] // PR-A2 dispatch runtime reads these
    pub(crate) fn indexes(&self) -> &MeshIndexes {
        &self.indexes
    }

    /// Which node "won the dial" for tiebreak purposes: the connection's
    /// dialer as a `NodeId`. Both endpoints compute the same value.
    fn dialer_node(&self, conn: &Connection) -> NodeId {
        if conn.is_dialer() {
            self.local_id
        } else {
            conn.peer_pubkey().node_id()
        }
    }

    /// Adopt a freshly established connection: run the duplicate
    /// tiebreak, and if this connection is kept, spawn its router +
    /// sender tasks and enqueue our manifest announce. Returns `false`
    /// if the connection lost the tiebreak (it has been closed).
    pub(crate) fn adopt(self: &Arc<Self>, conn: Arc<Connection>) -> bool {
        let peer_id = conn.peer_pubkey().node_id();
        if peer_id == self.local_id {
            tracing::warn!(target: "harness.fanout", "refusing self-connection");
            conn.close_ref();
            return false;
        }
        let preferred_dialer = self.local_id.min(peer_id);
        let mut peers = self.peers.lock();
        if let Some(existing) = peers.get(&peer_id) {
            let keep_candidate = existing.conn.is_closed()
                || (self.dialer_node(&conn) == preferred_dialer
                    && self.dialer_node(&existing.conn) != preferred_dialer);
            if keep_candidate {
                existing.teardown();
            } else {
                tracing::debug!(
                    target: "harness.fanout",
                    peer = %peer_id,
                    "duplicate connection lost tiebreak; closing"
                );
                conn.close_ref();
                return false;
            }
        }
        let (out_tx, out_rx) = mpsc::channel::<OutboundMsg>(OUTBOUND_QUEUE_DEPTH);
        let sender = tokio::spawn(Self::sender_task(
            conn.clone(),
            out_rx,
            self.identity.clone(),
            self.handlers.clone(),
            peer_id,
        ));
        let router = tokio::spawn(self.clone().router_task(conn.clone(), peer_id));
        let entry = PeerEntry {
            conn,
            out_tx: out_tx.clone(),
            tasks: vec![sender, router],
        };
        peers.insert(peer_id, entry);
        drop(peers);

        // Announce ourselves. Best-effort: a full queue on a brand-new
        // connection cannot happen (the queue is fresh), but don't panic
        // if it somehow does.
        if out_tx
            .try_send(OutboundMsg::Announce(self.self_manifest.clone()))
            .is_err()
        {
            tracing::warn!(target: "harness.fanout", peer = %peer_id, "announce enqueue failed");
        }
        true
    }

    /// Hand `msg` to `node`'s outbound queue (non-blocking).
    #[cfg_attr(not(test), allow(dead_code))] // PR-A2 dispatch runtime sends through this
    pub(crate) fn send_to(&self, node: NodeId, msg: OutboundMsg) -> Result<(), SendToError> {
        let peers = self.peers.lock();
        let entry = peers.get(&node).ok_or(SendToError::NoConnection)?;
        if entry.conn.is_closed() {
            return Err(SendToError::NoConnection);
        }
        entry
            .out_tx
            .try_send(msg)
            .map_err(|_| SendToError::QueueFull)
    }

    /// Enqueue a heartbeat to every live peer; sweeps closed entries.
    pub(crate) fn broadcast_heartbeat(&self, hb: &Heartbeat) {
        let mut peers = self.peers.lock();
        peers.retain(|node, entry| {
            if entry.conn.is_closed() {
                tracing::debug!(target: "harness.fanout", peer = %node, "sweeping closed connection");
                entry.teardown();
                return false;
            }
            if entry
                .out_tx
                .try_send(OutboundMsg::Heartbeat(hb.clone()))
                .is_err()
            {
                tracing::warn!(
                    target: "harness.fanout",
                    peer = %node,
                    "heartbeat enqueue failed (queue full); peer sender is stalled"
                );
            }
            true
        });
    }

    /// Node ids with a live adopted connection.
    #[cfg_attr(not(test), allow(dead_code))] // PR-A2 `--all` resolution reads this
    pub(crate) fn live_peers(&self) -> Vec<NodeId> {
        self.peers
            .lock()
            .iter()
            .filter(|(_, e)| !e.conn.is_closed())
            .map(|(n, _)| *n)
            .collect()
    }

    /// Spawn the heartbeat broadcast loop (replaces the legacy
    /// `HeartbeatService::spawn_broadcaster` in the fanout daemon).
    /// `snapshot_fn` supplies per-tick publisher config + leader belief +
    /// brain score, exactly like the legacy path.
    pub(crate) fn spawn_heartbeat_broadcaster(
        self: &Arc<Self>,
        snapshot_fn: harness_mesh::heartbeat::SnapshotFn,
    ) -> JoinHandle<()> {
        let net = self.clone();
        let publisher = harness_mesh::heartbeat::HeartbeatPublisher::new(net.identity.clone());
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(HEARTBEAT_INTERVAL);
            tick.tick().await; // skip the immediate fire
            loop {
                tick.tick().await;
                let (snap, leader, score) = snapshot_fn();
                let hb = publisher.next(&snap, leader, score);
                net.broadcast_heartbeat(&hb);
            }
        })
    }

    /// The peer table this net records into (for the election pump etc.).
    #[allow(dead_code)] // PR-A2 MeshLiveSet reads this
    pub(crate) fn peer_table(&self) -> PeerTable {
        self.heartbeat.peers()
    }

    /// Liveness horizon used by `MeshLiveSet` (PR-A2).
    #[allow(dead_code)] // PR-A2 MeshLiveSet reads this
    pub(crate) fn liveness_timeout() -> std::time::Duration {
        PEER_TIMEOUT
    }

    /// Close every connection and abort per-peer tasks.
    pub(crate) fn shutdown(&self) {
        let mut peers = self.peers.lock();
        for (_, entry) in peers.drain() {
            entry.teardown();
        }
    }

    // ---------------------------------------------------------------
    // per-connection tasks
    // ---------------------------------------------------------------

    /// Drains the outbound queue. Stamps + signs task envelopes at send
    /// time (per-stream seq), evicts + retries once on send error.
    async fn sender_task(
        conn: Arc<Connection>,
        mut rx: mpsc::Receiver<OutboundMsg>,
        identity: Arc<Identity>,
        handlers: Arc<dyn TaskChannelHandlers>,
        peer_id: NodeId,
    ) {
        while let Some(msg) = rx.recv().await {
            let name = msg.channel_name();
            let mut attempt = 0u8;
            let sent = loop {
                attempt += 1;
                match Self::send_once(&conn, &identity, &msg).await {
                    Ok(()) => break true,
                    Err(err) => {
                        tracing::warn!(
                            target: "harness.fanout",
                            peer = %peer_id,
                            channel = name,
                            attempt,
                            error = %err,
                            "channel send failed"
                        );
                        conn.evict_channel(name);
                        if attempt >= 2 || conn.is_closed() {
                            break false;
                        }
                    }
                }
            };
            if !sent {
                if let OutboundMsg::Assign(assign) = &msg {
                    handlers.on_assign_send_failed(peer_id, assign.lease_id);
                }
            }
        }
    }

    async fn send_once(
        conn: &Arc<Connection>,
        identity: &Identity,
        msg: &OutboundMsg,
    ) -> Result<(), TransportError> {
        let ch = conn.channel(msg.channel_name()).await?;
        match msg {
            OutboundMsg::Heartbeat(hb) => ch.send(hb).await,
            OutboundMsg::Announce(m) => ch.send(m).await,
            OutboundMsg::Assign(a) => {
                let mut a = a.clone();
                a.seq = ch.next_seq();
                sign_or_protocol_err(&mut a, identity)?;
                ch.send(&a).await
            }
            OutboundMsg::Claim(c) => {
                let mut c = *c;
                c.seq = ch.next_seq();
                sign_or_protocol_err(&mut c, identity)?;
                ch.send(&c).await
            }
            OutboundMsg::Result(r) => {
                let mut r = r.clone();
                r.seq = ch.next_seq();
                sign_or_protocol_err(&mut r, identity)?;
                ch.send(&r).await
            }
        }
    }

    /// The sole `accept_bi` consumer for this connection. Spawns one
    /// recv task per accepted channel.
    async fn router_task(self: Arc<Self>, conn: Arc<Connection>, peer_id: NodeId) {
        loop {
            match conn.accept_channel().await {
                Ok(ch) => {
                    let net = self.clone();
                    let conn = conn.clone();
                    tokio::spawn(async move {
                        net.channel_recv_loop(&conn, ch, peer_id).await;
                    });
                }
                Err(err) => {
                    tracing::debug!(
                        target: "harness.fanout",
                        peer = %peer_id,
                        error = %err,
                        "channel router exiting"
                    );
                    break;
                }
            }
        }
    }

    /// Per-channel recv loop. Any per-stream error tears down only this
    /// stream (review R8); the registration is released so the peer can
    /// re-open the channel.
    ///
    /// One match arm per channel — long but flat; splitting it into six
    /// single-use functions would hide the routing table.
    #[allow(clippy::too_many_lines)]
    async fn channel_recv_loop(
        &self,
        conn: &Arc<Connection>,
        ch: Arc<ChannelStream>,
        peer_id: NodeId,
    ) {
        let name = ch.name();
        loop {
            let res: Result<(), TransportError> = match name {
                n if n == channels::HEARTBEAT => match ch.recv_sequenced::<Heartbeat>().await {
                    Ok(hb) => {
                        if hb.node_id == peer_id {
                            self.heartbeat.record_incoming(hb);
                        } else {
                            tracing::warn!(
                                target: "harness.fanout",
                                peer = %peer_id,
                                claimed = %hb.node_id,
                                "heartbeat node_id does not match connection peer; dropped"
                            );
                        }
                        Ok(())
                    }
                    Err(e) => Err(e),
                },
                n if n == channels::ANNOUNCE => match ch.recv::<NodeManifest>().await {
                    Ok(m) => {
                        if m.node_id == peer_id && m.pubkey == *conn.peer_pubkey() {
                            tracing::info!(
                                target: "harness.fanout",
                                peer = %peer_id,
                                capabilities = m.capabilities.len(),
                                scopes = m.scopes.len(),
                                "manifest announce received"
                            );
                            self.indexes.upsert(&m);
                        } else {
                            tracing::warn!(
                                target: "harness.fanout",
                                peer = %peer_id,
                                claimed = %m.node_id,
                                "announce identity does not match connection peer; dropped"
                            );
                        }
                        Ok(())
                    }
                    Err(e) => Err(e),
                },
                n if n == channels::TASK_ASSIGN => match ch.recv_sequenced::<TaskAssign>().await {
                    Ok(a) => {
                        if a.assigned_by == peer_id {
                            self.handlers.on_assign(peer_id, a);
                        } else {
                            tracing::warn!(
                                target: "harness.fanout",
                                peer = %peer_id,
                                claimed = %a.assigned_by,
                                "assign sender does not match connection peer; dropped"
                            );
                        }
                        Ok(())
                    }
                    Err(e) => Err(e),
                },
                n if n == channels::TASK_CLAIM => match ch.recv_sequenced::<TaskClaim>().await {
                    Ok(c) => {
                        if c.worker == peer_id {
                            self.handlers.on_claim(peer_id, c);
                        } else {
                            tracing::warn!(
                                target: "harness.fanout",
                                peer = %peer_id,
                                claimed = %c.worker,
                                "claim sender does not match connection peer; dropped"
                            );
                        }
                        Ok(())
                    }
                    Err(e) => Err(e),
                },
                n if n == channels::TASK_RESULT => {
                    match ch.recv_sequenced::<TaskResultMsg>().await {
                        Ok(r) => {
                            self.handlers.on_result(peer_id, r);
                            Ok(())
                        }
                        Err(e) => Err(e),
                    }
                }
                other => {
                    // `GOSSIP_STATE` lands in PR-B; anything else is
                    // unreachable (accept_channel validates names).
                    tracing::debug!(
                        target: "harness.fanout",
                        peer = %peer_id,
                        channel = other,
                        "no handler for channel; dropping stream"
                    );
                    break;
                }
            };
            match res {
                Ok(()) => {}
                Err(TransportError::Closed | TransportError::Read(_)) => break,
                Err(err) => {
                    tracing::warn!(
                        target: "harness.fanout",
                        peer = %peer_id,
                        channel = name,
                        error = %err,
                        "channel recv error; tearing down this stream only"
                    );
                    break;
                }
            }
        }
        conn.release_accepted_channel(name);
    }
}

fn sign_or_protocol_err<T: Signable>(
    msg: &mut T,
    identity: &Identity,
) -> Result<(), TransportError> {
    msg.sign(identity)
        .map_err(|e| TransportError::Protocol(format!("sign outbound envelope: {e}")))
}

/// Build + sign this node's manifest from the live registry state.
pub(crate) fn build_self_manifest(
    identity: &Identity,
    hostname: String,
    capabilities: Vec<harness_core::Capability>,
    scopes: Vec<harness_core::Scope>,
) -> Result<NodeManifest, harness_core::ProtocolError> {
    let cpu_cores = std::thread::available_parallelism()
        .map(|n| u8::try_from(n.get()).unwrap_or(u8::MAX))
        .unwrap_or(1);
    let online_since = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let mut manifest = NodeManifest {
        node_id: identity.node_id(),
        hostname,
        pubkey: *identity.public_key(),
        capabilities,
        scopes,
        resources: harness_core::Resources {
            cpu_cores,
            // Resource sampling is a Phase 6 hardening item; 0 = unknown.
            ram_total_mb: 0,
            gpu: None,
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
        },
        online_since,
        version: harness_core::SemVer::new(0, 1, 0),
        sig: harness_core::Signature::from_bytes([0u8; 64]),
    };
    manifest.sign(identity)?;
    Ok(manifest)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use harness_core::protocol::CostHint;
    use harness_core::{
        Capability, Cardinality, Constraints, ExecutionPolicy, ResourceHints, RetryPolicy, SemVer,
        Signature, Task, TaskId, TraceContext,
    };
    use harness_mesh::transport::{Transport, TrustStore as TransportTrust};
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    use std::time::Duration;

    fn loopback() -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
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

    fn echo_capability() -> Capability {
        Capability {
            id: "echo".into(),
            version: SemVer::new(1, 0, 0),
            cardinality: Cardinality::Anyone,
            input_schema: serde_json::json!({}),
            output_schema: serde_json::json!({}),
            cost_hint: CostHint::LocalFast,
            tags: vec![],
            rate_limit: None,
            resource_hints: empty_hints(),
            requires_secrets: vec![],
        }
    }

    struct Node {
        transport: Transport,
        identity: Arc<Identity>,
        heartbeat: Arc<HeartbeatService>,
        net: Arc<PeerNet>,
    }

    fn build_node(name: &str, handlers: Arc<dyn TaskChannelHandlers>) -> Node {
        let identity = Arc::new(Identity::generate());
        let transport =
            Transport::bind(loopback(), identity.clone(), TransportTrust::new()).expect("bind");
        let heartbeat = Arc::new(HeartbeatService::new(identity.clone()));
        let indexes = Arc::new(MeshIndexes {
            caps: Arc::new(CapabilityIndex::new()),
            scopes: Arc::new(ScopeIndex::new()),
            store: None,
        });
        let manifest =
            build_self_manifest(&identity, name.to_string(), vec![echo_capability()], vec![])
                .expect("manifest");
        let net = PeerNet::new(
            identity.clone(),
            heartbeat.clone(),
            indexes,
            handlers,
            manifest,
        );
        Node {
            transport,
            identity,
            heartbeat,
            net,
        }
    }

    /// Dial b from a and adopt on both ends.
    async fn interconnect(a: &Node, b: &Node) {
        let b_accept = tokio::spawn({
            let t = b.transport.clone();
            async move {
                let inc = t.accept_one().await.expect("accept");
                inc.accept(|_| true).expect("approve")
            }
        });
        let dialed = a
            .transport
            .dial(
                b.transport.local_addr().expect("addr"),
                b.identity.public_key(),
            )
            .await
            .expect("dial");
        let accepted = b_accept.await.expect("join");
        assert!(a.net.adopt(Arc::new(dialed)));
        assert!(b.net.adopt(Arc::new(accepted)));
    }

    async fn wait_until(mut cond: impl FnMut() -> bool, what: &str) {
        for _ in 0..200 {
            if cond() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("timed out waiting for {what}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn t01_announce_populates_indexes_both_ways() {
        let a = build_node("node-a", Arc::new(NoopHandlers));
        let b = build_node("node-b", Arc::new(NoopHandlers));
        interconnect(&a, &b).await;

        let (a_id, b_id) = (a.identity.node_id(), b.identity.node_id());
        wait_until(
            || {
                a.net.indexes().caps.nodes_for("echo").contains(&b_id)
                    && b.net.indexes().caps.nodes_for("echo").contains(&a_id)
            },
            "cross announce",
        )
        .await;
        // Self manifests were indexed at construction.
        assert!(a.net.indexes().caps.nodes_for("echo").contains(&a_id));
        assert!(b.net.indexes().caps.nodes_for("echo").contains(&b_id));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn t02_heartbeats_flow_over_named_channel() {
        let a = build_node("node-a", Arc::new(NoopHandlers));
        let b = build_node("node-b", Arc::new(NoopHandlers));
        interconnect(&a, &b).await;

        let publisher = harness_mesh::heartbeat::HeartbeatPublisher::new(a.identity.clone());
        let cfg = harness_mesh::heartbeat::HeartbeatPublisherConfig::default();
        for _ in 0..3 {
            let hb = publisher.next(&cfg, a.identity.node_id(), 7);
            a.net.broadcast_heartbeat(&hb);
        }
        let a_id = a.identity.node_id();
        wait_until(
            || b.heartbeat.peers().get(&a_id).is_some(),
            "heartbeat recorded on b",
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn t03_simultaneous_mutual_dial_leaves_one_connection_each() {
        let a = build_node("node-a", Arc::new(NoopHandlers));
        let b = build_node("node-b", Arc::new(NoopHandlers));

        // Both accept loops adopt whatever arrives.
        for (me, other_name) in [(&a, "b"), (&b, "a")] {
            let t = me.transport.clone();
            let net = me.net.clone();
            let _ = other_name;
            tokio::spawn(async move {
                loop {
                    let Ok(inc) = t.accept_one().await else { break };
                    if let Ok(conn) = inc.accept(|_| true) {
                        net.adopt(Arc::new(conn));
                    }
                }
            });
        }

        // Simultaneous dials in both directions.
        let (da, db) = tokio::join!(
            a.transport.dial(
                b.transport.local_addr().expect("addr"),
                b.identity.public_key()
            ),
            b.transport.dial(
                a.transport.local_addr().expect("addr"),
                a.identity.public_key()
            ),
        );
        // Both dials succeed at the QUIC layer; adoption tiebreaks.
        a.net.adopt(Arc::new(da.expect("a dial")));
        b.net.adopt(Arc::new(db.expect("b dial")));

        let (a_id, b_id) = (a.identity.node_id(), b.identity.node_id());
        // Exactly one live logical connection per side, and heartbeats
        // still flow across the surviving pair in both directions.
        wait_until(
            || a.net.live_peers() == vec![b_id] && b.net.live_peers() == vec![a_id],
            "one live conn each",
        )
        .await;

        let pub_a = harness_mesh::heartbeat::HeartbeatPublisher::new(a.identity.clone());
        let pub_b = harness_mesh::heartbeat::HeartbeatPublisher::new(b.identity.clone());
        let cfg = harness_mesh::heartbeat::HeartbeatPublisherConfig::default();
        // Send several ticks — the first enqueue could land on a
        // connection that loses the tiebreak an instant later.
        for _ in 0..8 {
            a.net.broadcast_heartbeat(&pub_a.next(&cfg, a_id, 1));
            b.net.broadcast_heartbeat(&pub_b.next(&cfg, b_id, 1));
            tokio::time::sleep(Duration::from_millis(25)).await;
            if a.heartbeat.peers().get(&b_id).is_some() && b.heartbeat.peers().get(&a_id).is_some()
            {
                break;
            }
        }
        assert!(a.heartbeat.peers().get(&b_id).is_some(), "b recorded on a");
        assert!(b.heartbeat.peers().get(&a_id).is_some(), "a recorded on b");
    }

    struct RecordingHandlers {
        assigns: ParkingMutex<Vec<(NodeId, TaskAssign)>>,
    }

    impl TaskChannelHandlers for RecordingHandlers {
        fn on_assign(&self, from: NodeId, msg: TaskAssign) {
            self.assigns.lock().push((from, msg));
        }
        fn on_claim(&self, _: NodeId, _: TaskClaim) {}
        fn on_result(&self, _: NodeId, _: TaskResultMsg) {}
    }

    fn signed_test_task(identity: &Identity) -> Task {
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
            issued_by: identity.node_id(),
            issued_at: 1_700_000_000_000,
            tags: vec![],
            sig: Signature::from_bytes([0u8; 64]),
        };
        t.sign(identity).expect("sign");
        t
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn t04_assign_envelope_reaches_handlers_signed_and_stamped() {
        let a = build_node("node-a", Arc::new(NoopHandlers));
        let recording = Arc::new(RecordingHandlers {
            assigns: ParkingMutex::new(Vec::new()),
        });
        let b = build_node("node-b", recording.clone());
        interconnect(&a, &b).await;

        let assign = TaskAssign {
            seq: 0, // placeholder; the sender task stamps the stream seq
            lease_id: LeaseId::new_v7(),
            task: signed_test_task(&a.identity),
            assigned_by: a.identity.node_id(),
            lease_expires_at: 1_700_000_060_000,
            sig: Signature::from_bytes([0u8; 64]),
        };
        a.net
            .send_to(b.identity.node_id(), OutboundMsg::Assign(assign.clone()))
            .expect("enqueue");

        wait_until(|| !recording.assigns.lock().is_empty(), "assign delivered").await;
        let (from, got) = recording.assigns.lock().remove(0);
        assert_eq!(from, a.identity.node_id());
        assert_eq!(got.task.id, assign.task.id);
        assert_eq!(got.lease_id, assign.lease_id);
        // The outer envelope was signed by the sender task…
        got.verify_signature(a.identity.public_key())
            .expect("outer sig");
        // …and the inner task signature is the issuer's, untouched.
        got.task
            .verify_signature(a.identity.public_key())
            .expect("inner sig");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn t05_send_to_unknown_node_errors() {
        let a = build_node("node-a", Arc::new(NoopHandlers));
        let err = a
            .net
            .send_to(
                Identity::generate().node_id(),
                OutboundMsg::Claim(TaskClaim {
                    seq: 0,
                    lease_id: LeaseId::new_v7(),
                    task_id: TaskId::new_v7(),
                    worker: a.identity.node_id(),
                    sig: Signature::from_bytes([0u8; 64]),
                }),
            )
            .expect_err("no connection");
        assert!(matches!(err, SendToError::NoConnection));
    }
}
