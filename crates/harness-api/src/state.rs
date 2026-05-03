//! Shared state injected into every axum handler.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::SystemTime;

use harness_core::{Identity, NodeId, PublicKey};
use harness_mesh::heartbeat::PeerTable;
use parking_lot::RwLock;
use tokio::sync::{broadcast, watch};

use crate::event::MeshEvent;

/// Per-tick election outputs the daemon writes into the API state.
/// The UI displays these values.
#[derive(Clone, Debug)]
pub struct LocalStatus {
    pub mesh_name: String,
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
            leader_belief: None,
            brain_score: 0,
            seq: 0,
            capabilities: vec![],
            started_at: SystemTime::now(),
        }
    }
}

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
    peers: Option<PeerTable>,
    events: Option<broadcast::Sender<MeshEvent>>,
    capabilities: Vec<String>,
    auth: Option<Arc<crate::auth::AuthProvider>>,
    store: Option<harness_store::Store>,
}

impl ApiStateBuilder {
    #[must_use]
    pub fn new(identity: Arc<Identity>, mesh_name: impl Into<String>) -> Self {
        Self {
            identity,
            mesh_name: mesh_name.into(),
            peers: None,
            events: None,
            capabilities: vec![],
            auth: None,
            store: None,
        }
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
    pub fn build(self) -> ApiState {
        let local_node_id = self.identity.public_key().node_id();
        let mut status = LocalStatus::new(self.mesh_name);
        status.capabilities = self.capabilities;
        let events = self
            .events
            .unwrap_or_else(|| broadcast::channel::<MeshEvent>(1024).0);
        let auth = self
            .auth
            .unwrap_or_else(|| Arc::new(crate::auth::AuthProvider::new(None)));
        ApiState {
            identity: self.identity,
            local_node_id,
            local_status: Arc::new(RwLock::new(status)),
            peers: self.peers.unwrap_or_default(),
            events,
            auth,
            store: self.store,
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
