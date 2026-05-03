//! mDNS service discovery + static-peer fallback for the harness mesh.
//!
//! Each node advertises itself as `_harness._tcp.local.` with a TXT record
//! carrying its `node_id`, `pubkey_fp`, `mesh_name`, and `version`. Peers
//! are surfaced via a [`Discovery`] handle's broadcast event stream.
//!
//! mDNS is **advisory**. The discovered `pubkey_fp` is the first 8 bytes
//! of `blake3(pubkey)`; the *full* pubkey is verified later via the QUIC
//! handshake and the heartbeat signature. A peer that lies about its
//! `node_id` over mDNS is caught at handshake by the
//! `PinnedKeyVerifier` (1.4) and at gossip by the `Signable::verify_signature`
//! check (1.2).
//!
//! Static peers are merged with mDNS results, **not** failover-only —
//! both sources contribute. Static peers don't carry a `node_id` until
//! a handshake confirms it; they surface via [`Discovery::static_hints`]
//! as a one-shot snapshot at startup (NOT in [`DiscoveryEvent`]) and
//! are dial-only hints for item 1.4's transport.

mod mdns;
mod static_peers;
mod txt;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use harness_core::{NodeId, SemVer};
use parking_lot::RwLock;
use tokio::sync::broadcast;

pub use txt::{TxtError, TxtPayload};

/// A peer discovered via mDNS or static config. Static-peer hints are a
/// separate event variant — see [`DiscoveryEvent`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredPeer {
    pub node_id: NodeId,
    /// 16 lowercase hex chars; matches `PublicKey::fingerprint_hex()`.
    pub pubkey_fp: String,
    /// One or more reachable QUIC addresses for this peer. Sorted IPv4
    /// first, IPv6 second so dialers have a deterministic order.
    pub addrs: Vec<SocketAddr>,
    pub mesh_name: String,
    pub version: SemVer,
    pub source: DiscoverySource,
}

/// Which discovery channel surfaced this peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum DiscoverySource {
    Mdns,
    Static,
}

/// Discovery events broadcast to all subscribers.
///
/// **Static peers are NOT in this event stream.** They're surfaced via
/// [`Discovery::static_hints`] as a one-shot snapshot at startup. The
/// reason: the broadcast channel doesn't replay events for late
/// subscribers, and emitting `StaticHint`s before any subscriber exists
/// would silently drop them. Callers wire static peers into their dial
/// loop via `static_hints()`; they wire mDNS peers via `subscribe()`.
#[derive(Debug, Clone)]
#[non_exhaustive]
#[allow(clippy::large_enum_variant)]
pub enum DiscoveryEvent {
    /// An mDNS peer appeared (or its TXT/addrs changed).
    Added(DiscoveredPeer),
    /// An mDNS peer departed (TTL expiry / goodbye packet).
    Removed(NodeId),
}

/// Errors from discovery setup or operation.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DiscoveryError {
    #[error("mesh_name invalid: {0}")]
    InvalidMeshName(&'static str),
    #[error("port must be > 0")]
    InvalidPort,
    #[error("pubkey_fp must be 16 lowercase hex chars: {0}")]
    InvalidPubkeyFp(&'static str),
    #[error("event_buffer must be >= 1")]
    InvalidEventBuffer,
    #[error("mdns daemon: {0}")]
    Mdns(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

/// Discovery configuration. Construct via [`DiscoveryConfig::new`] which
/// validates `mesh_name` / `pubkey_fp` / `port` up front so registration
/// fails fast on bad input.
#[derive(Debug, Clone)]
pub struct DiscoveryConfig {
    pub mesh_name: String,
    pub node_id: NodeId,
    pub pubkey_fp: String,
    pub version: SemVer,
    /// QUIC listener port. Advertised in the SRV record.
    pub port: u16,
    /// Static-peer dial hints; merged with mDNS results, not failover.
    pub static_peers: Vec<SocketAddr>,
    /// Disable mDNS for tests / hosts where it's broken (e.g. some
    /// systemd-resolved setups).
    pub mdns_enabled: bool,
    /// Broadcast channel capacity for [`Discovery::subscribe`].
    pub event_buffer: usize,
    /// How long after the last announcement before we synthesize Removed.
    /// Pass-through to `mdns-sd`; ignored for static peers.
    pub mdns_ttl: Duration,
}

impl DiscoveryConfig {
    /// Construct + validate. Returns `Err` if any field violates the
    /// rules in `phase-1.3-mdns.plan.md` §4.2.
    pub fn new(
        mesh_name: String,
        node_id: NodeId,
        pubkey_fp: String,
        version: SemVer,
        port: u16,
    ) -> Result<Self, DiscoveryError> {
        txt::validate_mesh_name(&mesh_name).map_err(DiscoveryError::InvalidMeshName)?;
        txt::validate_pubkey_fp(&pubkey_fp).map_err(DiscoveryError::InvalidPubkeyFp)?;
        if port == 0 {
            return Err(DiscoveryError::InvalidPort);
        }
        Ok(Self {
            mesh_name,
            node_id,
            pubkey_fp,
            version,
            port,
            static_peers: Vec::new(),
            mdns_enabled: true,
            event_buffer: 32,
            mdns_ttl: Duration::from_secs(30),
        })
    }

    #[must_use]
    pub fn with_static_peers(mut self, peers: Vec<SocketAddr>) -> Self {
        self.static_peers = peers;
        self
    }

    #[must_use]
    pub fn with_mdns_enabled(mut self, enabled: bool) -> Self {
        self.mdns_enabled = enabled;
        self
    }

    #[must_use]
    pub fn with_event_buffer(mut self, n: usize) -> Self {
        self.event_buffer = n.max(1);
        self
    }

    #[must_use]
    pub fn with_mdns_ttl(mut self, ttl: Duration) -> Self {
        self.mdns_ttl = ttl;
        self
    }
}

/// Active discovery handle. Holds the mDNS daemon, the spawned listener
/// task, and the snapshot of currently-known peers. Cheap to clone via
/// `Arc` internally? **No** — Discovery owns the daemon and is not
/// cloneable. Subscribe to the event stream with [`Discovery::subscribe`]
/// or query the snapshot with [`Discovery::peers`].
pub struct Discovery {
    events_tx: broadcast::Sender<DiscoveryEvent>,
    state: Arc<RwLock<PeerTable>>,
    /// `Some` until `shutdown` is called.
    inner: parking_lot::Mutex<Option<DiscoveryInner>>,
}

struct DiscoveryInner {
    /// `None` if `mdns_enabled = false`.
    mdns: Option<mdns::MdnsTask>,
    /// Static peers don't have lifecycle; they're emitted once at
    /// startup and stay in the snapshot until shutdown.
    static_addrs: Vec<SocketAddr>,
}

#[derive(Default)]
struct PeerTable {
    peers: std::collections::HashMap<NodeId, DiscoveredPeer>,
}

impl std::fmt::Debug for Discovery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Discovery")
            .field("peer_count", &self.state.read().peers.len())
            .field("subscribers", &self.events_tx.receiver_count())
            .finish_non_exhaustive()
    }
}

impl Discovery {
    /// Spawn discovery: start the mDNS daemon (if enabled), advertise
    /// our service, browse for peers, and stash de-duplicated
    /// `static_peers` for retrieval via [`Self::static_hints`].
    ///
    /// Sync today (mdns-sd init is sync). Kept as a regular fn rather
    /// than async because there's nothing to await on the happy path;
    /// 1.5+ may add an async variant if it needs an await for resolver
    /// lookups.
    pub fn start(config: DiscoveryConfig) -> Result<Self, DiscoveryError> {
        let (events_tx, _rx) = broadcast::channel(config.event_buffer.max(1));
        let state = Arc::new(RwLock::new(PeerTable::default()));

        let mdns_task = if config.mdns_enabled {
            Some(mdns::MdnsTask::start(
                &config,
                events_tx.clone(),
                state.clone(),
            )?)
        } else {
            None
        };

        // Static peers are exposed via `static_hints()` — see the
        // `DiscoveryEvent` doc-comment for why they're NOT in the
        // broadcast stream. De-dup before storing so callers don't
        // see the same address twice.
        let mut static_addrs = config.static_peers;
        static_addrs.sort();
        static_addrs.dedup();

        Ok(Self {
            events_tx,
            state,
            inner: parking_lot::Mutex::new(Some(DiscoveryInner {
                mdns: mdns_task,
                static_addrs,
            })),
        })
    }

    /// Subscribe to live events. Late subscribers miss prior events;
    /// pair with [`Self::peers`] for a snapshot if you need at-least-once.
    pub fn subscribe(&self) -> broadcast::Receiver<DiscoveryEvent> {
        self.events_tx.subscribe()
    }

    /// Snapshot of currently-known mDNS peers. Static-peer addresses
    /// are exposed via [`Self::static_hints`].
    pub fn peers(&self) -> Vec<DiscoveredPeer> {
        let mut v: Vec<DiscoveredPeer> = self.state.read().peers.values().cloned().collect();
        v.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        v
    }

    /// Static-peer dial hints supplied at config time. Returns the
    /// de-duplicated set; callers wire these into their dial loop
    /// alongside mDNS [`Self::subscribe`] events. **Not** in
    /// [`DiscoveryEvent`] because static peers carry no `node_id`
    /// until 1.4's TLS handshake confirms it.
    pub fn static_hints(&self) -> Vec<SocketAddr> {
        self.inner
            .lock()
            .as_ref()
            .map(|i| i.static_addrs.clone())
            .unwrap_or_default()
    }

    /// Shut down the daemon, drain in-flight events, and join the
    /// listener task.
    ///
    /// `&self` (not `self`) so callers holding `Arc<Discovery>` can
    /// invoke shutdown from any reference; idempotent under repeated
    /// calls. After shutdown:
    /// - `peers()` continues to return the snapshot at shutdown time.
    /// - `static_hints()` returns an empty vec.
    /// - `subscribe()` returns a Receiver that will see no further
    ///   events (the broadcast channel is intact but no senders
    ///   remain, so `recv` returns `RecvError::Closed` once any
    ///   buffered events drain).
    pub async fn shutdown(&self) -> Result<(), DiscoveryError> {
        let inner = self.inner.lock().take();
        if let Some(inner) = inner {
            if let Some(mdns) = inner.mdns {
                mdns.shutdown().await?;
            }
        }
        Ok(())
    }
}

impl Drop for Discovery {
    fn drop(&mut self) {
        // Best-effort sync close. mdns-sd's Drop on ServiceDaemon also
        // runs; we additionally try to clear the listener task here.
        if let Some(inner) = self.inner.lock().take() {
            if let Some(mdns) = inner.mdns {
                mdns.shutdown_sync();
            }
        }
    }
}
