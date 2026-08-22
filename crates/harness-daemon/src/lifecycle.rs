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
use harness_mesh::transport::{self as transport, Transport, TransportError};
use harness_mesh::{TrustEvent, TrustStore};
use parking_lot::Mutex as ParkingMutex;
use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;

use crate::dispatch::DispatchRuntime;
use crate::peer_net::{MeshIndexes, PeerNet};

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
    /// Local executor for running tasks the daemon picks up. Phase 3.3a.
    executor: crate::executor::LocalExecutor,
    /// Per-peer connection registry + channel router. Phase 3.3-fanout.
    peer_net: Arc<PeerNet>,
    /// Issuer+worker dispatch runtime. Phase 3.3-fanout PR-A2.
    dispatch: Arc<DispatchRuntime>,
    /// Replica gossip service. Phase 3.3-gossip (ADR-0019).
    gossip: Arc<crate::gossip::GossipService>,
    /// Frame router + wire coalescer for streaming partial output.
    /// Phase 3.2-stream (ADR-0020).
    partial_streamer: Arc<crate::partial_stream::PartialStreamer>,
    /// Coordinated shutdown — flipped to `true` on ctrl-c. The executor
    /// loop watches this; future loops can subscribe too.
    shutdown_tx: watch::Sender<bool>,
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
    /// Local node's mesh hostname (e.g. `"macbook-archy"`). DISTINCT
    /// from `mesh_name` (the cluster name). Defaults to OS hostname;
    /// override via `HARNESS_NODE_NAME`.
    pub node_name: String,
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
            node_name: crate::executor::default_node_name(),
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
    #[allow(clippy::too_many_lines)]
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

        // Phase 3.1: load `~/.harness/policy.toml` if present. Missing
        // file → deny-all (PRD §10.4 default). Parse / validate errors
        // are fatal — refusing to start with a clear message is better
        // than silently bypassing policy.
        let policy_path = config.harness_root.join("policy.toml");
        let policy_engine = match harness_policy::load_from_path(&policy_path) {
            Ok(p) => harness_policy::PolicyEngine::from_policy_at(policy_path.clone(), p),
            Err(harness_policy::PolicyError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::warn!(
                    path = %policy_path.display(),
                    "no policy.toml found; defaulting to deny-all"
                );
                harness_policy::PolicyEngine::new(harness_policy::Policy::deny_all())
            }
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "policy load failed at {}: {e}",
                    policy_path.display()
                ));
            }
        };
        let policy_engine = std::sync::Arc::new(policy_engine);

        // Phase 3.6-encrypted (ADR-0021): open `~/.harness/secrets.enc`,
        // encrypted under a key derived from the node identity secret.
        // A legacy plaintext `secrets.toml` is transparently migrated
        // (re-encrypted; the plaintext file is left for the operator to
        // delete). Missing both → empty store (capabilities that need a
        // secret surface a clear `not configured` error at execute
        // time). Permission / parse / decrypt errors abort startup —
        // silently bypassing a credential file is worse than refusing
        // to start.
        let vault_key = harness_vault::derive_vault_key(&identity.to_secret_bytes());
        let enc_path = config.harness_root.join("secrets.enc");
        let legacy_path = config.harness_root.join("secrets.toml");
        let (enc_store, vault_origin) =
            harness_vault::EncryptedStore::open_with_migration(&enc_path, &legacy_path, &vault_key)
                .map_err(|e| {
                    anyhow::anyhow!("secrets load failed at {}: {e}", enc_path.display())
                })?;
        drop(vault_key);
        match vault_origin {
            harness_vault::VaultOrigin::Missing => tracing::info!(
                path = %enc_path.display(),
                "no credential store found; capabilities requiring secrets will fail until configured"
            ),
            // Encrypted-load and migration details (incl. the
            // delete-the-plaintext-file warning) are logged inside
            // `open_with_migration`.
            harness_vault::VaultOrigin::Encrypted
            | harness_vault::VaultOrigin::MigratedFromPlaintext => {}
        }
        let secrets: std::sync::Arc<dyn harness_vault::SecretsStore> =
            std::sync::Arc::new(enc_store);

        // Phase 3.10a: load `~/.harness/scopes.toml` if present. Missing
        // file → empty registry (`fs.*` advertise no scopes; calls fail
        // with `InvalidInput("unknown scope")`). Permission / parse
        // errors abort startup — silently skipping a misconfigured
        // scope is worse than a clear error.
        #[cfg(feature = "fs")]
        let scope_registry = {
            let scopes_path = config.harness_root.join("scopes.toml");
            let r = match harness_capabilities::ScopeRegistry::load_from_path(&scopes_path) {
                Ok(r) => r,
                Err(harness_capabilities::ScopeError::Io { source, .. })
                    if source.kind() == std::io::ErrorKind::NotFound =>
                {
                    tracing::info!(
                        path = %scopes_path.display(),
                        "no scopes.toml found; fs.* advertise no scopes"
                    );
                    harness_capabilities::ScopeRegistry::default()
                }
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "scopes.toml load failed at {}: {e}",
                        scopes_path.display()
                    ));
                }
            };
            std::sync::Arc::new(r)
        };

        // 3.2-stream (ADR-0020): one shared partial-output ring buffer
        // set (API reader + dispatch/local writers) and the streamer
        // that routes capability line frames — local tasks append
        // directly, remote tasks coalesce onto `harness.task.partial`.
        // Built before the registry so `shell.exec` can take the sink.
        let partial_buffers = Arc::new(harness_api::PartialBuffers::new());
        let partial_streamer = crate::partial_stream::PartialStreamer::new(
            identity.node_id(),
            partial_buffers.clone(),
        );

        // Build the capability registry (echo + future Phase 3
        // additions feature-gated). The daemon advertises every
        // registered capability via NodeManifest.
        let capabilities = harness_capabilities::default_registry_with_shell_sink(
            policy_engine.clone(),
            Some(partial_streamer.sink()),
        );

        // Phase 3.10a: register fs.list + fs.read after the default
        // registry. The scope registry's `manifest_scopes()` populates
        // `NodeManifest::scopes` further below.
        #[cfg(feature = "fs")]
        {
            #[allow(clippy::expect_used)]
            capabilities
                .register(std::sync::Arc::new(
                    harness_capabilities::FsListCapability::new(scope_registry.clone()),
                ))
                .expect("BUG: fs.list registered twice");
            #[allow(clippy::expect_used)]
            capabilities
                .register(std::sync::Arc::new(
                    harness_capabilities::FsReadCapability::new(scope_registry.clone()),
                ))
                .expect("BUG: fs.read registered twice");
            // Phase 3.10-fts: fs.grep (index-free streaming scan) +
            // fs.search (sqlite-FTS5 sidecar index under
            // <harness_root>/index/). ADR-0016.
            #[allow(clippy::expect_used)]
            capabilities
                .register(std::sync::Arc::new(
                    harness_capabilities::FsGrepCapability::new(scope_registry.clone()),
                ))
                .expect("BUG: fs.grep registered twice");
            #[allow(clippy::expect_used)]
            capabilities
                .register(std::sync::Arc::new(
                    harness_capabilities::FsSearchCapability::new(
                        scope_registry.clone(),
                        config.harness_root.join("index"),
                    ),
                ))
                .expect("BUG: fs.search registered twice");
        }
        // Phase 3.4: discover locally-installed Ollama models and
        // register one `llm.local.<model>` cap per. Best-effort —
        // failures log + return; daemon continues without LLM caps.
        #[cfg(feature = "llm")]
        harness_capabilities::enrich_with_llm_local(&capabilities, policy_engine.clone()).await;
        // Phase 3.6a/3.6b: register the `llm.cloud.{claude,openai,gemini}`
        // caps. Each capability surfaces in the manifest unconditionally
        // — at execute time it errors with `not configured` if its
        // secret tag is missing. This is intentional: peers can see the
        // cap exists and route to it, and the operator gets a clear
        // diagnostic instead of silent absence. The reqwest client and
        // batcher are shared across providers (connection-pool reuse;
        // the batcher fingerprint pins the provider so cross-provider
        // coalescing is impossible).
        #[cfg(feature = "llm")]
        {
            let cloud_client = reqwest::Client::new();
            let cloud_batcher =
                std::sync::Arc::new(harness_capabilities::llm_batcher::LlmBatcher::from_env());
            harness_capabilities::enrich_with_llm_cloud_claude(
                &capabilities,
                secrets.clone(),
                policy_engine.clone(),
                cloud_batcher.clone(),
                cloud_client.clone(),
            );
            harness_capabilities::enrich_with_llm_cloud_openai(
                &capabilities,
                secrets.clone(),
                policy_engine.clone(),
                cloud_batcher.clone(),
                cloud_client.clone(),
            );
            harness_capabilities::enrich_with_llm_cloud_gemini(
                &capabilities,
                secrets.clone(),
                policy_engine.clone(),
                cloud_batcher,
                cloud_client,
            );
        }
        // Phase 3.7: load `~/.harness/mcp.toml`, spawn the configured
        // MCP servers, and register one `mcp.<server>.<tool>` cap per
        // discovered tool. Missing file → info log, no MCP caps.
        // Parse / validation errors are fatal — refusing to start
        // beats silently skipping a misconfigured integration. Each
        // configured server is then best-effort: one server failing
        // to spawn logs a warning and is skipped, the daemon still
        // boots (ADR-0018). Runs before `brain.plan` registration so
        // the planner's capability snapshot sees the MCP tools.
        #[cfg(feature = "mcp")]
        {
            let mcp_path = config.harness_root.join("mcp.toml");
            harness_capabilities::enrich_with_mcp_from_path(
                &capabilities,
                policy_engine.clone(),
                &mcp_path,
            )
            .await
            .map_err(|e| anyhow::anyhow!("mcp.toml load failed at {}: {e}", mcp_path.display()))?;
        }
        // Phase 3.11: mesh.grep / mesh.search federated wrappers.
        // Registered before brain.plan so the planner's capability
        // snapshot sees them. Self-owned scopes run in-process via the
        // weak registry; remote scopes become pinned sub-tasks routed
        // by the dispatch runtime (ADR-0022).
        {
            let mesh_exec = crate::mesh_exec::StoreMeshExec::new(
                store.clone(),
                identity.clone(),
                capabilities.downgrade(),
                heartbeat.peers(),
                // 4.2: per-target progress frames ride the same partial
                // pipeline as shell line frames (ADR-0024).
                Some(partial_streamer.sink()),
            );
            harness_capabilities::enrich_with_mesh_meta(&capabilities, mesh_exec);
        }

        // Phase 3.8/3.9: register `brain.plan` with a backend lineup.
        // Lives last in the enricher list so it observes every other
        // registered capability via `WeakCapabilityRegistry::snapshot`.
        //
        // Backend lineup resolution:
        //   1. Read `policy.planning.prefer_local_models` (PRD §15.2).
        //   2. Walk the registry's `llm.local.*` ids; pick the first
        //      preferred model that's locally registered.
        //   3. Build `[LocalFastBackend, TemplateBackend]` if a model
        //      resolved; otherwise just `[TemplateBackend]`.
        //
        // Default constraints flow from `policy.planning.confidence_threshold`
        // + `default_max_cost_usd` per ADR-0014 §9.
        #[cfg(feature = "brain")]
        {
            let policy_snapshot = policy_engine.snapshot();
            let planning = &policy_snapshot.planning;

            let mut backends: Vec<std::sync::Arc<dyn harness_brain::PlannerBackend>> = Vec::new();

            #[cfg(feature = "llm")]
            if let Some(model) =
                resolve_local_fast_model(&capabilities, &planning.prefer_local_models)
            {
                let host_str = std::env::var("OLLAMA_HOST")
                    .unwrap_or_else(|_| "http://127.0.0.1:11434".to_string());
                let host = harness_capabilities::llm_local::parse_ollama_host(&host_str)
                    .unwrap_or_else(|()| {
                        #[allow(clippy::expect_used)]
                        url::Url::parse("http://127.0.0.1:11434").expect("default host parses")
                    });
                match harness_brain::LocalFastBackend::new(host, model.clone(), identity.node_id())
                {
                    Ok(b) => {
                        tracing::info!(
                            target: "harness.brain",
                            local_fast_model = %model,
                            "registered LocalFast planner backend"
                        );
                        backends.push(std::sync::Arc::new(b));
                    }
                    Err(err) => {
                        tracing::warn!(
                            target: "harness.brain",
                            ?err,
                            "failed to construct LocalFastBackend; brain.plan will run Template-only"
                        );
                    }
                }
            }

            backends.push(std::sync::Arc::new(harness_brain::TemplateBackend::new(
                identity.node_id(),
            )));

            let brain_config = harness_capabilities::brain_plan::BrainPlanConfig {
                default_constraints: harness_brain::PlanConstraints {
                    max_cost_usd: planning.default_max_cost_usd,
                    allow_cloud: planning.allow_cloud_escalation,
                    must_be_local: false,
                    plan_max_nodes: None,
                    confidence_threshold: Some(planning.confidence_threshold),
                },
            };
            harness_capabilities::enrich_with_brain_plan(&capabilities, backends, brain_config)
                .await;
        }
        let cap_ids = capabilities.ids();

        // Phase 3.3a: local executor loop. Picks Submitted tasks off
        // the queue, walks the lifecycle ladder, invokes the capability,
        // writes results.
        let local_node_name: Arc<str> = Arc::from(config.node_name.as_str());
        let executor = crate::executor::LocalExecutor::with_default_concurrency(
            store.clone(),
            capabilities.clone(),
            identity.node_id(),
            local_node_name.clone(),
        );

        // Phase 3.3-fanout (PR-A1): the per-peer connection registry +
        // channel router. Announces our signed manifest on every adopted
        // connection and feeds peer manifests into the capability/scope
        // indexes the dispatcher routes against.
        #[cfg(feature = "fs")]
        let manifest_scopes = scope_registry.manifest_scopes();
        #[cfg(not(feature = "fs"))]
        let manifest_scopes = Vec::new();
        let self_manifest = crate::peer_net::build_self_manifest(
            &identity,
            config.node_name.clone(),
            capabilities.manifests(),
            manifest_scopes,
            // Tag NAMES only, never values (ADR-0021): lets peers route
            // `requires_secrets` capabilities to nodes holding the tags.
            secrets.tags(),
        )
        .map_err(|e| anyhow::anyhow!("sign self manifest: {e}"))?;
        let cap_index = Arc::new(harness_orchestrator::CapabilityIndex::new());
        let scope_index = Arc::new(harness_orchestrator::ScopeIndex::new());
        let mesh_indexes = Arc::new(MeshIndexes {
            caps: cap_index.clone(),
            scopes: scope_index.clone(),
            store: Some(store.clone()),
        });
        // The dispatch runtime is both the issuer-side dispatcher and
        // the worker-side assign/reply handler (PR-A2, ADR-0017).
        let dispatch_runtime = DispatchRuntime::new(
            store.clone(),
            identity.clone(),
            capabilities.clone(),
            harness_orchestrator::Dispatcher::with_indexes(cap_index, scope_index),
            persistent_trust.clone(),
            heartbeat.peers(),
            secrets.clone(),
        );
        let peer_net = PeerNet::new(
            identity.clone(),
            heartbeat.clone(),
            mesh_indexes,
            dispatch_runtime.clone(),
            self_manifest,
        );
        dispatch_runtime.attach_net(&peer_net);
        // 3.2-stream: issuer-side partials land in the shared ring; the
        // worker-side streamer needs the runtime for issuer lookup +
        // wire access.
        dispatch_runtime.attach_partials(partial_buffers.clone());
        partial_streamer.attach_dispatch(&dispatch_runtime);

        // Phase 3.3-gossip: LWW replica sync over `harness.gossip.state`
        // + heartbeat replica_head anti-entropy (ADR-0019).
        let gossip = crate::gossip::GossipService::new(store.clone(), identity.clone());
        gossip.attach_net(&peer_net);
        peer_net.attach_gossip(&gossip);

        let api_state =
            harness_api::ApiStateBuilder::new(identity.clone(), config.mesh_name.clone())
                .with_node_name(config.node_name.clone())
                .with_peers(heartbeat.peers())
                .with_capabilities(cap_ids)
                .with_auth(auth)
                .with_store(store)
                .with_policy(policy_engine)
                .with_secrets(secrets.clone())
                .with_partials(partial_buffers)
                .build();
        let api_handle = harness_api::serve(config.api_bind, api_state.clone())
            .await
            .context("bind harness-api")?;

        let (shutdown_tx, _shutdown_rx) = watch::channel(false);

        Ok(Self {
            api_state,
            api_handle,
            transport: transport_handle,
            discovery,
            heartbeat,
            election,
            persistent_trust,
            executor,
            peer_net,
            dispatch: dispatch_runtime,
            gossip,
            partial_streamer,
            shutdown_tx,
            tasks: ParkingMutex::new(Vec::new()),
            listeners: Arc::new(ParkingMutex::new(Vec::new())),
            broadcaster: ParkingMutex::new(None),
            evictor: ParkingMutex::new(None),
        })
    }

    pub(crate) fn api_addr(&self) -> SocketAddr {
        self.api_handle.local_addr()
    }

    /// Bound QUIC address (tests wire two daemons via static peers).
    #[cfg(test)]
    pub(crate) fn mesh_addr(&self) -> SocketAddr {
        #[allow(clippy::expect_used)]
        self.transport.local_addr().expect("transport bound")
    }

    /// Peer table handle for test liveness waits.
    #[cfg(test)]
    pub(crate) fn peer_table(&self) -> harness_mesh::heartbeat::PeerTable {
        self.heartbeat.peers()
    }

    /// Partial-frame ring handle for test assertions (4.2 progress).
    #[cfg(test)]
    pub(crate) fn partial_buffers(&self) -> Arc<harness_api::PartialBuffers> {
        self.api_state.partials.clone()
    }

    /// Store handle for test assertions.
    #[cfg(test)]
    pub(crate) fn store(&self) -> harness_store::Store {
        #[allow(clippy::expect_used)]
        self.api_state
            .store
            .clone()
            .expect("daemon always has a store")
    }

    /// Spawn every loop and block until SIGINT/SIGTERM (or until a fatal
    /// task panics).
    pub(crate) async fn run_until_signal(self) -> Result<()> {
        self.start_loops();
        tokio::signal::ctrl_c().await.ok();
        tracing::info!(target: "harness.daemon", "shutdown requested");
        let _ = self.shutdown_tx.send(true);
        self.shutdown().await;
        Ok(())
    }

    /// Test variant: run until `stop` flips, then shut down cleanly.
    #[cfg(test)]
    pub(crate) async fn run_until(self, mut stop: watch::Receiver<bool>) -> Result<()> {
        self.start_loops();
        let _ = stop.changed().await;
        let _ = self.shutdown_tx.send(true);
        self.shutdown().await;
        Ok(())
    }

    /// Spawn every long-running loop (idempotence not required — called
    /// exactly once from the run entrypoints).
    fn start_loops(&self) {
        // Heartbeat broadcaster: per-tick snapshot pulls fresh local
        // metadata. For 1.11 the snapshot is static (no resource
        // sampling yet — that's a Phase 6 hardening item); the brain
        // score is updated by the election pump and surfaced via the
        // API state, which the broadcaster reads each tick.
        let snapshot_state = self.api_state.clone();
        let snapshot_store = self.api_state.store.clone();
        let local_id = self.transport.node_id();
        let snapshot_fn: harness_mesh::heartbeat::SnapshotFn = Box::new(move || {
            // 3.3-gossip: advertise the replica head per tick so peers
            // can detect divergence (ADR-0019). All-zero = "no replica
            // state advertised" (also the value peers see on head
            // errors — never advertise a bogus head).
            let replica_head = snapshot_store
                .as_ref()
                .and_then(|s| s.replica_head().ok())
                .unwrap_or_default();
            let s = snapshot_state.local_status.read();
            let cfg = HeartbeatPublisherConfig {
                version: harness_core::SemVer {
                    major: 0,
                    minor: 1,
                    patch: 0,
                },
                replica_head,
                ..HeartbeatPublisherConfig::default()
            };
            let leader = s.leader_belief.unwrap_or(local_id);
            (cfg, leader, s.brain_score)
        });

        // Phase 3.3-fanout: heartbeats ride their own named channel
        // stream per peer, enqueued through the per-peer bounded
        // outbound queues (PeerNet sweeps closed connections per tick).
        let hb_broadcaster = self.peer_net.spawn_heartbeat_broadcaster(snapshot_fn);
        self.tasks.lock().push(hb_broadcaster);

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
            self.persistent_trust.clone(),
            self.peer_net.clone(),
        );
        self.tasks.lock().push(accept_loop);

        // Dial loop — discovery events + trust store events trigger
        // dials of known-trusted peers.
        let dial_loop = spawn_dial_loop(
            self.transport.clone(),
            self.persistent_trust.clone(),
            self.discovery.clone(),
            self.peer_net.clone(),
        );
        self.tasks.lock().push(dial_loop);

        // Phase 3.3a: local executor loop. Subscribes to the same
        // shutdown channel so ctrl-c drains it cleanly.
        let exec_shutdown_rx = self.shutdown_tx.subscribe();
        let exec = self.executor.clone();
        let exec_handle = tokio::spawn(async move {
            exec.run_forever(exec_shutdown_rx).await;
        });
        self.tasks.lock().push(exec_handle);

        // Phase 3.3-fanout PR-A2: dispatch + lease-expiry + worker-reply
        // loops.
        let dispatch_handle = tokio::spawn(
            self.dispatch
                .clone()
                .run_dispatch_loop(self.shutdown_tx.subscribe()),
        );
        self.tasks.lock().push(dispatch_handle);
        let expire_handle = tokio::spawn(
            self.dispatch
                .clone()
                .run_expire_loop(self.shutdown_tx.subscribe()),
        );
        self.tasks.lock().push(expire_handle);
        let reply_handle = tokio::spawn(self.dispatch.clone().run_reply_pump(
            self.executor.subscribe_terminal(),
            self.shutdown_tx.subscribe(),
        ));
        self.tasks.lock().push(reply_handle);

        // Phase 3.3-gossip: periodic replica delta push (head-triggered
        // full syncs fire from the heartbeat recv path).
        let gossip_handle = tokio::spawn(
            self.gossip
                .clone()
                .run_gossip_loop(self.shutdown_tx.subscribe()),
        );
        self.tasks.lock().push(gossip_handle);
        // Phase 3.2-stream: coalesced partial-output flusher (one
        // `harness.task.partial` send per streaming task per 50ms tick).
        let partial_handle = tokio::spawn(
            self.partial_streamer
                .clone()
                .run_flush_loop(self.shutdown_tx.subscribe()),
        );
        self.tasks.lock().push(partial_handle);
    }

    async fn shutdown(self) {
        // Stop accepting first so listener tasks see Closed naturally.
        let tasks: Vec<_> = self.tasks.lock().drain(..).collect();
        for task in tasks {
            task.abort();
        }
        // Close per-peer connections + abort their router/sender tasks.
        self.peer_net.shutdown();
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

/// Resolve which local Ollama model the `LocalFast` planner should bind
/// to. Walks `prefer_local_models` in declared order and returns the
/// first that has a corresponding `llm.local.<model>` capability
/// registered. Returns `None` when no preference matches — the daemon
/// then registers brain.plan with a Template-only lineup.
#[cfg(all(feature = "brain", feature = "llm"))]
fn resolve_local_fast_model(
    registry: &harness_capabilities::CapabilityRegistry,
    prefer_models: &[String],
) -> Option<String> {
    let registered_ids: std::collections::HashSet<String> = registry
        .ids()
        .into_iter()
        .filter_map(|id| id.strip_prefix("llm.local.").map(str::to_string))
        .collect();
    prefer_models
        .iter()
        .find(|m| registered_ids.contains(m.as_str()))
        .cloned()
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

fn spawn_accept_loop(transport: Transport, trust: TrustStore, net: Arc<PeerNet>) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match transport.accept_one().await {
                Ok(incoming) => {
                    let trust_clone = trust.clone();
                    match incoming.accept(|pk| trust_clone.lookup_by_pubkey(pk).is_some()) {
                        Ok(conn) => {
                            net.adopt(Arc::new(conn));
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
    trust: TrustStore,
    discovery: Arc<Discovery>,
    net: Arc<PeerNet>,
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
                &trust,
                &peer.addrs,
                Some(peer.node_id),
                &peer.pubkey_fp,
                &net,
                &mut dialed,
            )
            .await;
        }
        for addr in discovery.static_hints() {
            // For static peers we don't know which node_id is at the
            // address — try every trusted pubkey. The transport's cert
            // pinning rejects all but the right one.
            try_dial_static(&transport, &trust, addr, &net, &mut dialed).await;
        }

        loop {
            tokio::select! {
                evt = discovery_events.recv() => {
                    match evt {
                        Ok(DiscoveryEvent::Added(peer)) => {
                            try_dial_known(
                                &transport,
                                &trust,
                                &peer.addrs,
                                Some(peer.node_id),
                                &peer.pubkey_fp,
                                &net,
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
                                    &trust,
                                    &known.addrs,
                                    Some(known.node_id),
                                    &known.pubkey_fp,
                                    &net,
                                    &mut dialed,
                                )
                                .await;
                            }
                        }
                        for addr in discovery.static_hints() {
                            try_dial_static(&transport, &trust, addr, &net, &mut dialed).await;
                        }
                    }
                }
            }
        }
    })
}

async fn try_dial_known(
    transport: &Transport,
    trust: &TrustStore,
    addrs: &[SocketAddr],
    node_id: Option<harness_core::NodeId>,
    pubkey_fp: &str,
    net: &Arc<PeerNet>,
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
                net.adopt(Arc::new(conn));
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
    trust: &TrustStore,
    addr: SocketAddr,
    net: &Arc<PeerNet>,
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
            net.adopt(Arc::new(conn));
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
            node_name: "test-node".into(),
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
