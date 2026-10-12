//! Shared state injected into every axum handler.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::SystemTime;

use harness_core::{Identity, NodeId, PublicKey};
use harness_mesh::heartbeat::PeerTable;
use harness_policy::PolicyEngine;
use harness_vault::SecretsStore;
use parking_lot::RwLock;
use tokio::sync::{broadcast, watch};

use crate::event::MeshEvent;

/// Per-tick election outputs the daemon writes into the API state.
/// The UI displays these values.
#[derive(Clone, Debug)]
pub struct LocalStatus {
    pub mesh_name: String,
    /// This node's mesh hostname (`HARNESS_NODE_NAME` / OS hostname).
    /// Distinct from `mesh_name`. Empty until the daemon sets it.
    pub node_name: String,
    pub leader_belief: Option<NodeId>,
    pub brain_score: i32,
    pub seq: u64,
    pub capabilities: Vec<String>,
    pub started_at: SystemTime,
}

impl LocalStatus {
    #[must_use]
    pub fn new(mesh_name: impl Into<String>) -> Self {
        Self {
            mesh_name: mesh_name.into(),
            node_name: String::new(),
            leader_belief: None,
            brain_score: 0,
            seq: 0,
            capabilities: vec![],
            started_at: SystemTime::now(),
        }
    }
}

/// 4.7 (ADR-0029): the daemon's pause switch, seen through a narrow
/// trait so the API crate stays daemon-agnostic. `paused()` is the
/// effective (operator OR auto) flag surfaced on `GET /status`;
/// `set_operator` backs `POST /admin/pause|resume`.
pub trait PauseControl: Send + Sync + std::fmt::Debug {
    fn paused(&self) -> bool;
    fn operator_paused(&self) -> bool;
    fn set_operator(&self, paused: bool);
}

/// 5.13c-2 (ADR-0041): asking the mesh to pull a peer's audit entries,
/// seen through a narrow trait for the same reason as
/// [`PauseControl`] — `harness-daemon` depends on `harness-api`, not
/// the reverse, and `ApiState` holds only a liveness `PeerTable`.
///
/// The call is a REQUEST, not a result: entry pulls cross the network
/// and settle asynchronously, so the handler reports that a walk was
/// started and the caller re-reads the pin's status.
pub trait AuditPuller: Send + Sync + std::fmt::Debug {
    /// Ask `peer` for `subject`'s entries up to `target_seq`.
    /// Returns false if no request could be sent (no connection, or
    /// too many already in flight).
    fn request_range(
        &self,
        peer: harness_core::NodeId,
        subject: harness_core::NodeId,
        from_seq: u64,
        target_seq: u64,
    ) -> bool;
}

/// Capacity of the mesh-event broadcast feeding `WS /events`. A slow
/// WebSocket consumer that lags past this is closed with 1011
/// ("lagged") and reconnects to resync from a fresh snapshot — the
/// documented lag-recovery story (4.7 names the former bare literal).
pub const MESH_EVENT_CAPACITY: usize = 1024;

/// State shared by every API route. Cheaply cloneable (`Arc` internally).
#[derive(Clone)]
pub struct ApiState {
    pub identity: Arc<Identity>,
    pub local_node_id: NodeId,
    pub local_status: Arc<RwLock<LocalStatus>>,
    pub peers: PeerTable,
    pub events: broadcast::Sender<MeshEvent>,
    /// Auth provider — `is_initialized()` is `false` until `harness admin
    /// set-password` has been run.
    pub auth: Arc<crate::auth::AuthProvider>,
    /// Persistent task store. `None` only in narrow tests that exercise
    /// the public read endpoints without a DB. Production daemons always
    /// pass `Some(store)`.
    pub store: Option<harness_store::Store>,
    /// Local policy engine — consulted by the executor before running
    /// privileged actions (Phase 3.1, used by `shell.exec` in 3.2).
    pub policy: Arc<PolicyEngine>,
    /// Tagged credential store. Phase 3.6a: plaintext file-backed; the
    /// 3.6-encrypted form will swap the impl behind the same trait.
    /// Surfaced on `ApiState` so future admin endpoints (e.g. "which
    /// secrets are configured?") and capability-execute paths share
    /// the same handle.
    pub secrets: Arc<dyn SecretsStore>,
    /// Streaming partial-output ring buffers (3.2-stream, ADR-0020).
    /// The daemon shares one instance between its dispatch runtime /
    /// local sink (writers) and `GET /tasks/{id}` (reader).
    pub partials: Arc<crate::partials::PartialBuffers>,
    /// 4.7 (ADR-0029): pause switch. `None` in bare test fixtures —
    /// status reports `paused: false` and the admin endpoints 503.
    pub pause: Option<Arc<dyn PauseControl>>,
    /// 5.13c-2: entry-pull requests into the mesh.
    pub audit_puller: Option<Arc<dyn AuditPuller>>,
    /// 5.5 (ADR-0033): webhook adapter runtime (sender allowlist,
    /// public-URL override, driver semaphore, outbound Twilio base).
    /// Built from the environment by default; tests inject their own.
    pub webhook: Arc<crate::routes::webhook::WebhookRuntime>,
}

impl std::fmt::Debug for ApiState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApiState")
            .field("local_node_id", &self.local_node_id)
            .field("peer_count", &self.peers.len())
            .finish_non_exhaustive()
    }
}

impl ApiState {
    pub fn local_pubkey(&self) -> &PublicKey {
        self.identity.public_key()
    }

    /// Mutate the local status — typically called by the election
    /// state machine when leader belief or brain score changes.
    /// Publishes a `LeaderChanged` event when the leader belief flips.
    pub fn set_local_status<F>(&self, mutate: F)
    where
        F: FnOnce(&mut LocalStatus),
    {
        let (prev_leader, new_leader, brain_score) = {
            let mut g = self.local_status.write();
            let prev = g.leader_belief;
            mutate(&mut g);
            (prev, g.leader_belief, g.brain_score)
        };
        if prev_leader != new_leader {
            let _ = self.events.send(MeshEvent::LeaderChanged {
                leader: new_leader.map(|id| format!("{id}")),
                brain_score,
            });
        }
    }
}

/// Builder for [`ApiState`] — clearer call sites in tests + main.
#[derive(Debug)]
pub struct ApiStateBuilder {
    identity: Arc<Identity>,
    mesh_name: String,
    node_name: String,
    peers: Option<PeerTable>,
    events: Option<broadcast::Sender<MeshEvent>>,
    capabilities: Vec<String>,
    auth: Option<Arc<crate::auth::AuthProvider>>,
    store: Option<harness_store::Store>,
    policy: Option<Arc<PolicyEngine>>,
    secrets: Option<Arc<dyn SecretsStore>>,
    partials: Option<Arc<crate::partials::PartialBuffers>>,
    pause: Option<Arc<dyn PauseControl>>,
    audit_puller: Option<Arc<dyn AuditPuller>>,
    webhook: Option<Arc<crate::routes::webhook::WebhookRuntime>>,
}

impl ApiStateBuilder {
    #[must_use]
    pub fn new(identity: Arc<Identity>, mesh_name: impl Into<String>) -> Self {
        Self {
            identity,
            mesh_name: mesh_name.into(),
            node_name: String::new(),
            peers: None,
            events: None,
            capabilities: vec![],
            auth: None,
            store: None,
            policy: None,
            secrets: None,
            partials: None,
            pause: None,
            audit_puller: None,
            webhook: None,
        }
    }

    #[must_use]
    pub fn with_node_name(mut self, node_name: impl Into<String>) -> Self {
        self.node_name = node_name.into();
        self
    }

    #[must_use]
    pub fn with_peers(mut self, peers: PeerTable) -> Self {
        self.peers = Some(peers);
        self
    }

    #[must_use]
    pub fn with_events(mut self, sender: broadcast::Sender<MeshEvent>) -> Self {
        self.events = Some(sender);
        self
    }

    #[must_use]
    pub fn with_capabilities(mut self, caps: Vec<String>) -> Self {
        self.capabilities = caps;
        self
    }

    #[must_use]
    pub fn with_auth(mut self, auth: Arc<crate::auth::AuthProvider>) -> Self {
        self.auth = Some(auth);
        self
    }

    #[must_use]
    pub fn with_store(mut self, store: harness_store::Store) -> Self {
        self.store = Some(store);
        self
    }

    #[must_use]
    pub fn with_policy(mut self, policy: Arc<PolicyEngine>) -> Self {
        self.policy = Some(policy);
        self
    }

    #[must_use]
    pub fn with_secrets(mut self, secrets: Arc<dyn SecretsStore>) -> Self {
        self.secrets = Some(secrets);
        self
    }

    /// Share the daemon's partial-output ring buffers (3.2-stream).
    /// When not set, a fresh (never-written) instance is created so
    /// `partials` is always empty-but-present.
    #[must_use]
    pub fn with_partials(mut self, partials: Arc<crate::partials::PartialBuffers>) -> Self {
        self.partials = Some(partials);
        self
    }

    /// Wire the daemon's pause switch (4.7, ADR-0029).
    #[must_use]
    pub fn with_pause(mut self, pause: Arc<dyn PauseControl>) -> Self {
        self.pause = Some(pause);
        self
    }

    /// Wire the mesh's audit entry puller (5.13c-2, ADR-0041).
    #[must_use]
    pub fn with_audit_puller(mut self, puller: Arc<dyn AuditPuller>) -> Self {
        self.audit_puller = Some(puller);
        self
    }

    /// 5.5: inject a webhook runtime (tests point the Twilio base at
    /// wiremock and set an explicit allowlist). Default: from env.
    #[must_use]
    pub fn with_webhook_runtime(
        mut self,
        webhook: Arc<crate::routes::webhook::WebhookRuntime>,
    ) -> Self {
        self.webhook = Some(webhook);
        self
    }

    #[must_use]
    pub fn build(self) -> ApiState {
        let local_node_id = self.identity.public_key().node_id();
        let mut status = LocalStatus::new(self.mesh_name);
        status.node_name = self.node_name;
        status.capabilities = self.capabilities;
        let events = self
            .events
            .unwrap_or_else(|| broadcast::channel::<MeshEvent>(MESH_EVENT_CAPACITY).0);
        let auth = self
            .auth
            .unwrap_or_else(|| Arc::new(crate::auth::AuthProvider::new(None)));
        // Default to deny-all policy when not set — tests and dev paths
        // get a safe default; production daemon supplies a real one.
        let policy = self
            .policy
            .unwrap_or_else(|| Arc::new(PolicyEngine::new(harness_policy::Policy::deny_all())));
        // Default to an empty secrets store. Production daemons supply
        // a `PlaintextStore::load_default()`. Tests get an empty store
        // unless they explicitly wire one.
        let secrets = self
            .secrets
            .unwrap_or_else(|| Arc::new(harness_vault::PlaintextStore::empty()));
        let partials = self
            .partials
            .unwrap_or_else(|| Arc::new(crate::partials::PartialBuffers::new()));
        let webhook = self
            .webhook
            .unwrap_or_else(|| Arc::new(crate::routes::webhook::WebhookRuntime::from_env()));
        ApiState {
            audit_puller: self.audit_puller,
            identity: self.identity,
            local_node_id,
            local_status: Arc::new(RwLock::new(status)),
            peers: self.peers.unwrap_or_default(),
            events,
            auth,
            store: self.store,
            policy,
            secrets,
            partials,
            pause: self.pause,
            webhook,
        }
    }
}

/// Owned handle for an axum server task, mirroring `ListenerHandle`
/// in `harness-mesh`. Drop to abort, `shutdown` to stop gracefully.
pub struct ServerHandle {
    local_addr: SocketAddr,
    task: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
    shutdown_tx: watch::Sender<bool>,
}

impl std::fmt::Debug for ServerHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerHandle")
            .field("local_addr", &self.local_addr)
            .finish_non_exhaustive()
    }
}

impl ServerHandle {
    pub(crate) fn new(
        local_addr: SocketAddr,
        task: tokio::task::JoinHandle<()>,
        shutdown_tx: watch::Sender<bool>,
    ) -> Self {
        Self {
            local_addr,
            task: parking_lot::Mutex::new(Some(task)),
            shutdown_tx,
        }
    }

    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        let task = self.task.lock().take();
        if let Some(t) = task {
            // Give the server up to 5s to drain in-flight requests.
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), t).await;
        }
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
        if let Some(t) = self.task.lock().take() {
            t.abort();
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn set_local_status_publishes_leader_changed() {
        let id = Arc::new(Identity::generate());
        let state = ApiStateBuilder::new(id, "test").build();
        let mut sub = state.events.subscribe();
        let leader = NodeId::from_bytes([7u8; 16]);
        state.set_local_status(|s| {
            s.leader_belief = Some(leader);
            s.brain_score = 250;
        });
        let event = sub.recv().await.expect("event must arrive");
        match event {
            MeshEvent::LeaderChanged {
                leader,
                brain_score,
            } => {
                assert!(leader.is_some());
                assert_eq!(brain_score, 250);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn set_local_status_no_event_when_leader_unchanged() {
        let id = Arc::new(Identity::generate());
        let state = ApiStateBuilder::new(id, "test").build();
        let mut sub = state.events.subscribe();
        state.set_local_status(|s| s.brain_score = 100);
        let res = tokio::time::timeout(std::time::Duration::from_millis(50), sub.recv()).await;
        assert!(res.is_err(), "no event should fire");
    }
}
