//! Daemon lifecycle: wires Discovery + Transport + `HeartbeatService` +
//! Election into one orchestrator.
//!
//! Phase 1.11 scope:
//!
//! - Bind QUIC transport with a snapshot of the persistent `TrustStore`.
//! - Start mDNS discovery + a static-peer fallback for tests.
//! - Spawn the heartbeat broadcaster (every 2s) + stale-peer evictor.
//! - Spawn an accept loop that hands trusted incoming connections to
//!   the heartbeat listener.
//! - Spawn a dial loop that watches discovery events + the trust store,
//!   and dials known-trusted peers as they appear.
//! - Tick the brain election once a second; push the result into the
//!   API state so `LeaderChanged` events fire.
//!
//! Out of scope (deferred): pairing-in-daemon (untrusted accept paths
//! for `harness join` over the network). Today, pairing is a one-shot
//! through the CLI that touches `peers.toml` directly.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use harness_api::ApiState;
use harness_core::Identity;
use harness_mesh::discovery::{Discovery, DiscoveryConfig, DiscoveryError, DiscoveryEvent};
use harness_mesh::election::{Election, ElectionConfig};
use harness_mesh::heartbeat::{
    BroadcasterHandle, HeartbeatPublisherConfig, HeartbeatService, ListenerHandle, PEER_TIMEOUT,
};
use harness_mesh::transport::{self as transport, Connection, Transport, TransportError};
use harness_mesh::{TrustEvent, TrustStore};
use parking_lot::Mutex as ParkingMutex;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

/// One-shot factory + run-loop for the daemon. Holds owned references
/// to every long-lived subsystem.
pub(crate) struct DaemonOrchestrator {
    api_state: ApiState,
    api_handle: harness_api::ServerHandle,
    transport: Transport,
    discovery: Arc<Discovery>,
    heartbeat: Arc<HeartbeatService>,
    election: Arc<Election>,
    persistent_trust: TrustStore,
    /// All spawned tasks live here; aborted on shutdown.
    tasks: ParkingMutex<Vec<JoinHandle<()>>>,
    /// Per-connection listener handles; drop on shutdown.
    listeners: Arc<ParkingMutex<Vec<ListenerHandle>>>,
    broadcaster: ParkingMutex<Option<BroadcasterHandle>>,
    evictor: ParkingMutex<Option<BroadcasterHandle>>,
}

impl std::fmt::Debug for DaemonOrchestrator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DaemonOrchestrator")
            .field("local_node", &self.transport.node_id())
            .field("api_addr", &self.api_handle.local_addr())
            .finish_non_exhaustive()
    }
}

/// Configuration for the daemon. CLI / `config.toml` populate this.
#[derive(Debug, Clone)]
pub(crate) struct DaemonRuntimeConfig {
    pub mesh_name: String,
    pub api_bind: SocketAddr,
    /// QUIC bind address — defaults to 0.0.0.0:19199.
    pub mesh_bind: SocketAddr,
    pub mdns_enabled: bool,
    pub static_peers: Vec<SocketAddr>,
    /// `~/.harness/` (or `--root` override). The store, identity, and
    /// admin.toml all live under here.
    pub harness_root: std::path::PathBuf,
}

impl Default for DaemonRuntimeConfig {
    fn default() -> Self {
        Self {
            mesh_name: "harness".to_string(),
            api_bind: SocketAddr::new(std::net::Ipv4Addr::LOCALHOST.into(), 19198),
            mesh_bind: SocketAddr::new(std::net::Ipv4Addr::UNSPECIFIED.into(), 19199),
            mdns_enabled: true,
            static_peers: Vec::new(),
            harness_root: std::path::PathBuf::from("/tmp/harness"),
        }
    }
}

impl DaemonOrchestrator {
    /// Build all subsystems and bind sockets. Does NOT spawn loops yet —
    /// call [`Self::run_until_signal`].
    pub(crate) async fn build(
        identity: Arc<Identity>,
        persistent_trust: TrustStore,
        config: DaemonRuntimeConfig,
    ) -> Result<Self> {
        // Build the transport-layer trust snapshot from the persistent store.
        let mut trust_snapshot = transport::TrustStore::new();
        for peer in persistent_trust.all_peers() {
            trust_snapshot.allow(peer.pubkey);
        }
        let transport_handle = Transport::bind(config.mesh_bind, identity.clone(), trust_snapshot)
            .context("bind QUIC transport")?;
        let local_quic = transport_handle
            .local_addr()
            .context("read transport bound addr")?;

        let pubkey = identity.public_key();
        let discovery_cfg = DiscoveryConfig::new(
            config.mesh_name.clone(),
            identity.node_id(),
            pubkey.fingerprint_hex(),
            harness_core::SemVer {
                major: 0,
                minor: 1,
                patch: 0,
            },
            local_quic.port(),
        )
        .map_err(|e| map_discovery(&e))?
        .with_static_peers(config.static_peers.clone())
        .with_mdns_enabled(config.mdns_enabled);
        let discovery = Arc::new(Discovery::start(discovery_cfg).map_err(|e| map_discovery(&e))?);

        let heartbeat = Arc::new(HeartbeatService::new(identity.clone()));
        let election = Arc::new(Election::new(ElectionConfig::new(identity.node_id())));

        // Open the persistent store rooted under ~/.harness/. The
        // exact path lives next to identity.key.
        let mut db_path = config.harness_root.clone();
        db_path.push("harness.db");
        let store = harness_store::Store::open(&harness_store::StoreConfig::at(&db_path))
            .context("open harness-store")?;

        // Load the admin file if present; absence means the operator
        // hasn't run `harness admin set-password` yet, and mutating
        // endpoints will surface a 503 until they do.
        let admin = match harness_mesh::admin::load(&config.harness_root) {
            Ok(a) => Some(a),
            Err(harness_mesh::admin::AdminError::NotInitialized) => None,
            Err(e) => return Err(anyhow::Error::from(e).context("load admin.toml")),
        };
        let auth = std::sync::Arc::new(harness_api::AuthProvider::new(admin));

        let api_state =
            harness_api::ApiStateBuilder::new(identity.clone(), config.mesh_name.clone())
                .with_peers(heartbeat.peers())
                .with_capabilities(vec!["builtin.echo".to_string()])
                .with_auth(auth)
                .with_store(store)
                .build();
        let api_handle = harness_api::serve(config.api_bind, api_state.clone())
            .await
            .context("bind harness-api")?;

        Ok(Self {
            api_state,
            api_handle,
            transport: transport_handle,
            discovery,
            heartbeat,
            election,
            persistent_trust,
            tasks: ParkingMutex::new(Vec::new()),
            listeners: Arc::new(ParkingMutex::new(Vec::new())),
            broadcaster: ParkingMutex::new(None),
            evictor: ParkingMutex::new(None),
        })
    }

    pub(crate) fn api_addr(&self) -> SocketAddr {
        self.api_handle.local_addr()
    }

    /// Spawn every loop and block until SIGINT/SIGTERM (or until a fatal
    /// task panics).
    pub(crate) async fn run_until_signal(self) -> Result<()> {
        // Heartbeat broadcaster: per-tick snapshot pulls fresh local
        // metadata. For 1.11 the snapshot is static (no resource
        // sampling yet — that's a Phase 6 hardening item); the brain
        // score is updated by the election pump and surfaced via the
        // API state, which the broadcaster reads each tick.
        let snapshot_state = self.api_state.clone();
        let local_id = self.transport.node_id();
        let snapshot_fn: harness_mesh::heartbeat::SnapshotFn = Box::new(move || {
            let s = snapshot_state.local_status.read();
            let cfg = HeartbeatPublisherConfig {
                version: harness_core::SemVer {
                    major: 0,
                    minor: 1,
                    patch: 0,
                },
                ..HeartbeatPublisherConfig::default()
            };
            let leader = s.leader_belief.unwrap_or(local_id);
            (cfg, leader, s.brain_score)
        });

        // Connections to broadcast to — read from a shared registry.
        let conns: Arc<ParkingMutex<Vec<Arc<Connection>>>> =
            Arc::new(ParkingMutex::new(Vec::new()));
        // Each entry is held by both the conns Vec and its listener task.
        // When the listener exits (peer dropped), only conns retains the Arc;
        // the broadcaster's targets_fn sweeps these dead entries every tick.
        // Without this sweep the Vec grows monotonically and the broadcaster
        // wastes time + log spam re-trying dead peers indefinitely.
        let conns_for_broadcaster = conns.clone();
        let targets_fn: Arc<dyn Fn() -> Vec<Arc<Connection>> + Send + Sync + 'static> =
            Arc::new(move || {
                let mut g = conns_for_broadcaster.lock();
                g.retain(|c| Arc::strong_count(c) > 1);
                g.clone()
            });

        let broadcaster = self.heartbeat.spawn_broadcaster(snapshot_fn, targets_fn);
        *self.broadcaster.lock() = Some(broadcaster);

        let evictor = self.heartbeat.spawn_evictor(PEER_TIMEOUT / 2);
        *self.evictor.lock() = Some(evictor);

        // Election pump.
        let election_pump = spawn_election_pump(
            self.heartbeat.peers(),
            self.election.clone(),
            self.api_state.clone(),
        );
        self.tasks.lock().push(election_pump);

        // Accept loop — incoming QUIC connections from trusted peers.
        let accept_loop = spawn_accept_loop(
            self.transport.clone(),
            self.heartbeat.clone(),
            self.persistent_trust.clone(),
            self.listeners.clone(),
            conns.clone(),
        );
        self.tasks.lock().push(accept_loop);

        // Dial loop — discovery events + trust store events trigger
        // dials of known-trusted peers.
        let dial_loop = spawn_dial_loop(
            self.transport.clone(),
            self.heartbeat.clone(),
            self.persistent_trust.clone(),
            self.discovery.clone(),
            self.listeners.clone(),
            conns,
        );
        self.tasks.lock().push(dial_loop);

        tokio::signal::ctrl_c().await.ok();
        tracing::info!(target: "harness.daemon", "shutdown requested");
        self.shutdown().await;
        Ok(())
    }

    async fn shutdown(self) {
        // Stop accepting first so listener tasks see Closed naturally.
        let tasks: Vec<_> = self.tasks.lock().drain(..).collect();
        for task in tasks {
            task.abort();
        }
        // Drop listener handles — each aborts its task on drop.
        self.listeners.lock().clear();
        let broadcaster = self.broadcaster.lock().take();
        if let Some(b) = broadcaster {
            b.shutdown().await;
        }
        let evictor = self.evictor.lock().take();
        if let Some(e) = evictor {
            e.shutdown().await;
        }
        if let Err(err) = self.discovery.shutdown().await {
            tracing::warn!(target: "harness.daemon", ?err, "discovery shutdown");
        }
        self.api_handle.shutdown().await;
    }
}

fn map_discovery(err: &DiscoveryError) -> anyhow::Error {
    anyhow::anyhow!("discovery: {err}")
}

fn spawn_election_pump(
    peers: harness_mesh::heartbeat::PeerTable,
    election: Arc<Election>,
    api: ApiState,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.tick().await; // skip immediate fire
        loop {
            tick.tick().await;
            // For 1.11 the local score is fixed at 100 — Phase 1.6's
            // `brain_score(BrainScoreInput)` will be wired in once
            // resource sampling lands (Phase 6 hardening).
            let local_score = 100;
            let result = election.tick(&peers, local_score);
            // IMPORTANT: only the leader_belief is updated from the
            // election result. brain_score must always be the LOCAL
            // node's score — `result.winning_score` is the leader's
            // score, and writing it here would cause the heartbeat
            // broadcaster to re-emit the leader's score as our own,
            // poisoning every peer's PeerTable.
            api.set_local_status(|s| {
                s.leader_belief = Some(result.leader);
                s.brain_score = local_score;
            });
        }
    })
}

fn spawn_accept_loop(
    transport: Transport,
    heartbeat: Arc<HeartbeatService>,
    trust: TrustStore,
    listeners: Arc<ParkingMutex<Vec<ListenerHandle>>>,
    conns: Arc<ParkingMutex<Vec<Arc<Connection>>>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match transport.accept_one().await {
                Ok(incoming) => {
                    let trust_clone = trust.clone();
                    match incoming.accept(|pk| trust_clone.lookup_by_pubkey(pk).is_some()) {
                        Ok(conn) => {
                            let conn = Arc::new(conn);
                            conns.lock().push(conn.clone());
                            listeners.lock().push(heartbeat.register_peer(conn));
                        }
                        Err(err) => {
                            tracing::warn!(target: "harness.daemon", ?err, "accept failed");
                        }
                    }
                }
                Err(TransportError::Closed) => break,
                Err(err) => {
                    tracing::warn!(target: "harness.daemon", ?err, "accept_one error");
                }
            }
        }
    })
}

fn spawn_dial_loop(
    transport: Transport,
    heartbeat: Arc<HeartbeatService>,
    trust: TrustStore,
    discovery: Arc<Discovery>,
    listeners: Arc<ParkingMutex<Vec<ListenerHandle>>>,
    conns: Arc<ParkingMutex<Vec<Arc<Connection>>>>,
) -> JoinHandle<()> {
    let mut discovery_events = discovery.subscribe();
    let mut trust_events = trust.subscribe();
    tokio::spawn(async move {
        // Track who we've dialed so we don't redial on every event.
        let mut dialed: HashSet<harness_core::NodeId> = HashSet::new();

        // Initial sweep: try every static peer + currently-known mDNS peer.
        for peer in discovery.peers() {
            try_dial_known(
                &transport,
                &heartbeat,
                &trust,
                &peer.addrs,
                Some(peer.node_id),
                &peer.pubkey_fp,
                &listeners,
                &conns,
                &mut dialed,
            )
            .await;
        }
        for addr in discovery.static_hints() {
            // For static peers we don't know which node_id is at the
            // address — try every trusted pubkey. The transport's cert
            // pinning rejects all but the right one.
            try_dial_static(
                &transport,
                &heartbeat,
                &trust,
                addr,
                &listeners,
                &conns,
                &mut dialed,
            )
            .await;
        }

        loop {
            tokio::select! {
                evt = discovery_events.recv() => {
                    match evt {
                        Ok(DiscoveryEvent::Added(peer)) => {
                            try_dial_known(
                                &transport,
                                &heartbeat,
                                &trust,
                                &peer.addrs,
                                Some(peer.node_id),
                                &peer.pubkey_fp,
                                &listeners,
                                &conns,
                                &mut dialed,
                            )
                            .await;
                        }
                        Ok(DiscoveryEvent::Removed(node_id)) => {
                            dialed.remove(&node_id);
                            // The heartbeat evictor handles per-peer timeout cleanup;
                            // nothing else to do here.
                        }
                        // `DiscoveryEvent` is `#[non_exhaustive]`; future
                        // variants are silently ignored until the daemon
                        // is updated to handle them.
                        Ok(_) => {}
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
                t = trust_events.recv() => {
                    if let Ok(TrustEvent::Added(peer)) = t {
                        // Peer just got trusted (typical: pairing
                        // completed). Proactively re-scan known mDNS
                        // peers + static hints so we don't wait for
                        // an mDNS re-announce (up to mdns_ttl=30s).
                        dialed.remove(&peer.node_id);
                        let pubkey_fp = peer.pubkey.fingerprint_hex();
                        for known in discovery.peers() {
                            if known.pubkey_fp == pubkey_fp {
                                try_dial_known(
                                    &transport,
                                    &heartbeat,
                                    &trust,
                                    &known.addrs,
                                    Some(known.node_id),
                                    &known.pubkey_fp,
                                    &listeners,
                                    &conns,
                                    &mut dialed,
                                )
                                .await;
                            }
                        }
                        for addr in discovery.static_hints() {
                            try_dial_static(
                                &transport,
                                &heartbeat,
                                &trust,
                                addr,
                                &listeners,
                                &conns,
                                &mut dialed,
                            )
                            .await;
                        }
                    }
                }
            }
        }
    })
}

#[allow(clippy::too_many_arguments)]
async fn try_dial_known(
    transport: &Transport,
    heartbeat: &Arc<HeartbeatService>,
    trust: &TrustStore,
    addrs: &[SocketAddr],
    node_id: Option<harness_core::NodeId>,
    pubkey_fp: &str,
    listeners: &Arc<ParkingMutex<Vec<ListenerHandle>>>,
    conns: &Arc<ParkingMutex<Vec<Arc<Connection>>>>,
    dialed: &mut HashSet<harness_core::NodeId>,
) {
    let Some(peer) = trust
        .all_peers()
        .into_iter()
        .find(|p| p.pubkey.fingerprint_hex() == pubkey_fp)
    else {
        // Untrusted — pairing must complete before mesh traffic flows.
        return;
    };
    if let Some(id) = node_id {
        if dialed.contains(&id) {
            return;
        }
    }
    for addr in addrs {
        match transport.dial(*addr, &peer.pubkey).await {
            Ok(conn) => {
                tracing::info!(
                    target: "harness.daemon",
                    peer_fp = %pubkey_fp,
                    %addr,
                    "dialed peer"
                );
                let conn = Arc::new(conn);
                conns.lock().push(conn.clone());
                listeners.lock().push(heartbeat.register_peer(conn));
                if let Some(id) = node_id {
                    dialed.insert(id);
                }
                return;
            }
            Err(err) => {
                tracing::debug!(
                    target: "harness.daemon",
                    peer_fp = %pubkey_fp,
                    %addr,
                    ?err,
                    "dial attempt failed; trying next addr"
                );
            }
        }
    }
}

async fn try_dial_static(
    transport: &Transport,
    heartbeat: &Arc<HeartbeatService>,
    trust: &TrustStore,
    addr: SocketAddr,
    listeners: &Arc<ParkingMutex<Vec<ListenerHandle>>>,
    conns: &Arc<ParkingMutex<Vec<Arc<Connection>>>>,
    dialed: &mut HashSet<harness_core::NodeId>,
) {
    for peer in trust.all_peers() {
        if dialed.contains(&peer.node_id) {
            continue;
        }
        if let Ok(conn) = transport.dial(addr, &peer.pubkey).await {
            tracing::info!(
                target: "harness.daemon",
                peer_fp = %peer.pubkey.fingerprint_hex(),
                %addr,
                "dialed static peer"
            );
            let conn = Arc::new(conn);
            conns.lock().push(conn.clone());
            listeners.lock().push(heartbeat.register_peer(conn));
            dialed.insert(peer.node_id);
            return;
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    /// Boot an orchestrator against a tempdir + ephemeral ports +
    /// mDNS-disabled. Confirms the full subsystem wiring compiles
    /// and binds end-to-end. The test does NOT exercise the run
    /// loop — that would require ctrl-c — but `build()` exercises
    /// every API mismatch a future `harness-mesh` change would
    /// introduce.
    #[tokio::test]
    async fn orchestrator_builds_against_tempdir() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let id = harness_mesh::identity::init_or_load(tmp.path()).expect("identity");
        let node_id = id.node_id();
        let identity = Arc::new(id);
        let trust = TrustStore::open(tmp.path(), node_id).expect("trust open");

        let cfg = DaemonRuntimeConfig {
            mesh_name: "test-mesh".into(),
            api_bind: SocketAddr::new(std::net::Ipv4Addr::LOCALHOST.into(), 0),
            mesh_bind: SocketAddr::new(std::net::Ipv4Addr::LOCALHOST.into(), 0),
            mdns_enabled: false,
            static_peers: vec![],
            harness_root: tmp.path().to_path_buf(),
        };

        let orch = DaemonOrchestrator::build(identity, trust, cfg)
            .await
            .expect("build orchestrator");
        let api_addr = orch.api_addr();
        assert!(api_addr.port() != 0, "api should bind to a real port");
    }
}
