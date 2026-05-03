//! [`HeartbeatService`] — the daemon-side glue. Spawns the broadcast
//! loop and a per-peer listener loop, wires them through
//! [`crate::transport::Connection`] and the local [`PeerTable`].

use std::sync::Arc;
use std::time::Duration;

use harness_core::{Heartbeat, NodeId};
use parking_lot::Mutex;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use crate::transport::{channels, Connection, TransportError};

use super::{
    HeartbeatPublisher, HeartbeatPublisherConfig, PeerTable, EVENT_CHANNEL_CAPACITY,
    HEARTBEAT_INTERVAL, PEER_TIMEOUT,
};

/// How the daemon supplies a fresh per-tick snapshot to the publisher.
pub type SnapshotFn =
    Box<dyn Fn() -> (HeartbeatPublisherConfig, NodeId, i32) + Send + Sync + 'static>;

/// Events published by [`HeartbeatService`] every time the local
/// [`PeerTable`] mutates. The API layer (1.10) subscribes and translates
/// these into the public WebSocket protocol.
#[derive(Debug, Clone)]
pub enum TableEvent {
    /// A heartbeat was just recorded. `was_new = true` if it was the
    /// first heartbeat we'd seen from this peer.
    Recorded { node_id: NodeId, was_new: bool },
    /// A peer was evicted because their last heartbeat is older than
    /// [`PEER_TIMEOUT`].
    Evicted { node_id: NodeId },
}

/// Per-connection registration handle. Drop to deregister + abort the
/// listener task; explicit `shutdown` joins cleanly.
pub struct ListenerHandle {
    task: Mutex<Option<JoinHandle<()>>>,
}

impl std::fmt::Debug for ListenerHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ListenerHandle").finish_non_exhaustive()
    }
}

impl ListenerHandle {
    pub async fn shutdown(self) {
        // Take the JoinHandle out of the parking_lot::Mutex synchronously
        // (no awaits while the guard is alive) before awaiting the join.
        let task = self.task.lock().take();
        if let Some(t) = task {
            t.abort();
            let _ = t.await;
        }
    }
}

impl Drop for ListenerHandle {
    fn drop(&mut self) {
        if let Some(t) = self.task.lock().take() {
            t.abort();
        }
    }
}

/// The daemon-side heartbeat coordinator.
///
/// One instance per node. `register_peer(conn)` spawns a per-connection
/// listener that records incoming heartbeats into the shared
/// [`PeerTable`]. `broadcast_once(...)` builds + sends a heartbeat to a
/// caller-supplied set of connections (typically called from a
/// `tokio::time::interval` loop, but exposed as a single tick so the
/// caller can drive the cadence).
pub struct HeartbeatService {
    publisher: HeartbeatPublisher,
    peers: PeerTable,
    events: broadcast::Sender<TableEvent>,
}

impl std::fmt::Debug for HeartbeatService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeartbeatService")
            .field("publisher", &self.publisher)
            .field("peer_count", &self.peers.len())
            .finish_non_exhaustive()
    }
}

impl HeartbeatService {
    pub fn new(identity: Arc<harness_core::Identity>) -> Self {
        let (events, _) = broadcast::channel::<TableEvent>(EVENT_CHANNEL_CAPACITY);
        Self {
            publisher: HeartbeatPublisher::new(identity),
            peers: PeerTable::new(),
            events,
        }
    }

    /// Shared peer table — the election state machine and UI both
    /// read from this.
    pub fn peers(&self) -> PeerTable {
        self.peers.clone()
    }

    /// Subscribe to [`TableEvent`]s. Each subscriber gets every event
    /// published after they call this. Lagged subscribers see
    /// `RecvError::Lagged(n)` and should refetch the snapshot.
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<TableEvent> {
        self.events.subscribe()
    }

    /// Clone of the events sender — for direct publishing from external
    /// machinery (e.g., tests or 1.6 election integration).
    #[must_use]
    pub fn events_sender(&self) -> broadcast::Sender<TableEvent> {
        self.events.clone()
    }

    /// Build + sign a heartbeat from `snapshot`, then `send` it to
    /// every connection in `targets`. Errors per-connection are
    /// `tracing::warn`-ed and swallowed — one slow / dead peer must
    /// not stall the broadcast.
    pub async fn broadcast_once(
        &self,
        snapshot: &HeartbeatPublisherConfig,
        leader_belief: NodeId,
        brain_score: i32,
        targets: &[Arc<Connection>],
    ) {
        let hb = self.publisher.next(snapshot, leader_belief, brain_score);
        for conn in targets {
            if let Err(e) = conn.send(&hb).await {
                tracing::warn!(
                    target: "harness.heartbeat",
                    peer = %conn.peer_pubkey().fingerprint_hex(),
                    error = ?e,
                    "heartbeat send failed; will retry next tick"
                );
            }
        }
    }

    /// Spawn a listener task that loops `recv_sequenced` on this
    /// connection, recording each heartbeat into the peer table and
    /// publishing a [`TableEvent::Recorded`] for every successful read.
    /// Returns a handle for shutdown.
    pub fn register_peer(&self, conn: Arc<Connection>) -> ListenerHandle {
        let table = self.peers.clone();
        let events = self.events.clone();
        let task = tokio::spawn(async move {
            loop {
                match conn.recv_sequenced::<Heartbeat>(channels::HEARTBEAT).await {
                    Ok(hb) => {
                        let node_id = hb.node_id;
                        let was_new = table.record(hb);
                        let _ = events.send(TableEvent::Recorded { node_id, was_new });
                    }
                    Err(TransportError::Closed | TransportError::Read(_)) => break,
                    Err(e) => {
                        tracing::warn!(
                            target: "harness.heartbeat",
                            peer = %conn.peer_pubkey().fingerprint_hex(),
                            error = ?e,
                            "recv heartbeat failed; closing listener"
                        );
                        break;
                    }
                }
            }
        });
        ListenerHandle {
            task: Mutex::new(Some(task)),
        }
    }

    /// Spawn a self-driven broadcast loop. Owns the cadence (every
    /// [`HEARTBEAT_INTERVAL`]). Pulls fresh `snapshot` /
    /// `leader_belief` / `brain_score` per tick from `snapshot_fn`.
    /// Spawn another task to `evict_stale` if you want stale-peer
    /// cleanup.
    ///
    /// Returns a [`ListenerHandle`] (same shape — abort on drop).
    pub fn spawn_broadcaster(
        &self,
        snapshot_fn: SnapshotFn,
        targets_fn: Arc<dyn Fn() -> Vec<Arc<Connection>> + Send + Sync + 'static>,
    ) -> BroadcasterHandle {
        let publisher = self.publisher_arc();
        let task = tokio::spawn(async move {
            let mut tick = tokio::time::interval(HEARTBEAT_INTERVAL);
            // Skip the first immediate tick so we don't broadcast before
            // the caller has had a chance to register peers.
            tick.tick().await;
            loop {
                tick.tick().await;
                let (snap, leader, score) = snapshot_fn();
                let hb = publisher.next(&snap, leader, score);
                let targets = targets_fn();
                for conn in &targets {
                    if let Err(e) = conn.send(&hb).await {
                        tracing::warn!(
                            target: "harness.heartbeat",
                            peer = %conn.peer_pubkey().fingerprint_hex(),
                            error = ?e,
                            "broadcast send failed"
                        );
                    }
                }
            }
        });
        BroadcasterHandle {
            task: Mutex::new(Some(task)),
        }
    }

    fn publisher_arc(&self) -> Arc<HeartbeatPublisher> {
        // The HeartbeatService owns the publisher uniquely, but the
        // broadcaster task needs to share it. Wrap in an Arc by cloning
        // through a fresh publisher on the same Identity — the seq
        // counters are independent because we want broadcast loops to
        // not interfere with manual broadcast_once calls in the same
        // process. Document.
        Arc::new(HeartbeatPublisher::new(self.publisher.identity_arc()))
    }

    /// Garbage-collect peers whose last heartbeat is older than
    /// [`PEER_TIMEOUT`]. Returns the dropped node ids.
    pub fn evict_stale(&self) -> Vec<NodeId> {
        self.peers.evict_stale(PEER_TIMEOUT)
    }

    /// Convenience: spawn an evict-stale loop that runs every
    /// `interval` (recommended `PEER_TIMEOUT`). Publishes a
    /// [`TableEvent::Evicted`] for every dropped peer so the API
    /// layer can fan out a `peer_evicted` to UI subscribers.
    pub fn spawn_evictor(&self, interval: Duration) -> BroadcasterHandle {
        let peers = self.peers.clone();
        let events = self.events.clone();
        let task = tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            tick.tick().await;
            loop {
                tick.tick().await;
                let dropped = peers.evict_stale(PEER_TIMEOUT);
                if !dropped.is_empty() {
                    tracing::info!(
                        target: "harness.heartbeat",
                        dropped = ?dropped,
                        "evicted stale peers"
                    );
                    for node_id in dropped {
                        let _ = events.send(TableEvent::Evicted { node_id });
                    }
                }
            }
        });
        BroadcasterHandle {
            task: Mutex::new(Some(task)),
        }
    }
}

/// Handle for the spawned broadcaster / evictor task. Drop to abort,
/// or `await` `shutdown()` to join cleanly.
pub struct BroadcasterHandle {
    task: Mutex<Option<JoinHandle<()>>>,
}

impl std::fmt::Debug for BroadcasterHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BroadcasterHandle").finish_non_exhaustive()
    }
}

impl BroadcasterHandle {
    pub async fn shutdown(self) {
        // Take the JoinHandle out of the parking_lot::Mutex synchronously
        // (no awaits while the guard is alive) before awaiting the join.
        let task = self.task.lock().take();
        if let Some(t) = task {
            t.abort();
            let _ = t.await;
        }
    }
}

impl Drop for BroadcasterHandle {
    fn drop(&mut self) {
        if let Some(t) = self.task.lock().take() {
            t.abort();
        }
    }
}
