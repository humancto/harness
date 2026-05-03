//! `mdns-sd` advertise + browse. Owned by [`super::Discovery`].

use std::collections::HashMap;
use std::sync::Arc;

use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo};
use parking_lot::RwLock;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use super::txt::{self, TxtPayload};
use super::{
    DiscoveredPeer, DiscoveryConfig, DiscoveryError, DiscoveryEvent, DiscoverySource, PeerTable,
};

const SERVICE_TYPE: &str = "_harness._tcp.local.";

pub(crate) struct MdnsTask {
    daemon: ServiceDaemon,
    fullname: String,
    listener: Option<JoinHandle<()>>,
    /// Set when shutdown is in progress so the listener loop exits cleanly.
    shutdown_flag: Arc<std::sync::atomic::AtomicBool>,
}

impl MdnsTask {
    pub(crate) fn start(
        config: &DiscoveryConfig,
        events_tx: broadcast::Sender<DiscoveryEvent>,
        state: Arc<RwLock<PeerTable>>,
    ) -> Result<Self, DiscoveryError> {
        let daemon = ServiceDaemon::new().map_err(|e| DiscoveryError::Mdns(e.to_string()))?;

        // Build our own ServiceInfo and register it.
        let info = build_service_info(config)?;
        let fullname = info.get_fullname().to_string();
        daemon
            .register(info)
            .map_err(|e| DiscoveryError::Mdns(format!("register: {e}")))?;

        // Start browsing for peers of the same service type.
        let receiver = daemon
            .browse(SERVICE_TYPE)
            .map_err(|e| DiscoveryError::Mdns(format!("browse: {e}")))?;

        let shutdown_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let listener = spawn_listener(
            receiver,
            events_tx,
            state,
            fullname.clone(),
            shutdown_flag.clone(),
        );

        Ok(Self {
            daemon,
            fullname,
            listener: Some(listener),
            shutdown_flag,
        })
    }

    pub(crate) async fn shutdown(mut self) -> Result<(), DiscoveryError> {
        self.shutdown_flag
            .store(true, std::sync::atomic::Ordering::SeqCst);
        // Best-effort unregister. Errors here are non-fatal for callers.
        let _ = self.daemon.unregister(&self.fullname);
        // Stop the daemon (drops it, which closes the receiver).
        let _ = self.daemon.shutdown();
        if let Some(handle) = self.listener.take() {
            // Give the listener up to 1s to wind down cleanly.
            let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle).await;
        }
        Ok(())
    }

    pub(crate) fn shutdown_sync(self) {
        self.shutdown_flag
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let _ = self.daemon.unregister(&self.fullname);
        // Closing the daemon drops the broadcast channel feeding the
        // listener; the for-loop terminates and the spawn_blocking
        // task winds down. We do NOT call `handle.abort()` because
        // `abort()` does not interrupt blocking-pool tasks (per Tokio
        // docs); it only flips a flag the future polls. Daemon
        // shutdown is the actual signal.
        let _ = self.daemon.shutdown();
        // Forget the handle — the OS thread will exit on its own when
        // the channel closes.
        let _ = self.listener;
    }
}

/// Build the `ServiceInfo` we register and advertise.
///
/// Instance name uses the **first 16 hex chars** of `node_id` (8 bytes,
/// 64 bits of entropy — orders of magnitude more than what we need for
/// uniqueness in a small LAN mesh) plus `-<mesh_name>`. The full
/// `node_id` (32 hex) plus a 63-byte mesh name would push the DNS
/// label past RFC 1035's 63-octet limit. We reject at start time if
/// 16 + 1 + `mesh_name.len()` > 63. The peer's full pubkey-derived
/// `node_id` is in the TXT record where length isn't a concern.
fn build_service_info(config: &DiscoveryConfig) -> Result<ServiceInfo, DiscoveryError> {
    let node_prefix: String = config.node_id.to_string().chars().take(16).collect();
    // 16 + 1 + mesh_name.len(); reject if it'd overflow 63 bytes.
    if 17 + config.mesh_name.len() > 63 {
        return Err(DiscoveryError::InvalidMeshName(
            "mesh_name too long once combined with node_id prefix",
        ));
    }
    let instance = format!("{node_prefix}-{}", config.mesh_name);

    // Hostname: 16-hex prefix is enough for collision-free `.harness.local.`
    // resolution; full node_id would push past the 63-octet label cap.
    let host = format!("{node_prefix}.harness.local.");

    let payload = TxtPayload {
        node_id: config.node_id,
        pubkey_fp: config.pubkey_fp.clone(),
        mesh_name: config.mesh_name.clone(),
        version: config.version,
    };
    // Plan §4.3 mandates fixed-order TXT emission. mdns-sd 0.13 accepts
    // a HashMap which we collect into; field order on the wire depends
    // on mdns-sd's internal iteration. We document this drift.
    let txt: HashMap<String, String> = txt::encode(&payload).into_iter().collect();

    let info = ServiceInfo::new(
        SERVICE_TYPE,
        &instance,
        &host,
        "", // ips: empty -> mdns-sd auto-detects local interfaces
        config.port,
        Some(txt),
    )
    .map_err(|e| DiscoveryError::Mdns(format!("ServiceInfo::new: {e}")))?
    .enable_addr_auto();
    Ok(info)
}

fn spawn_listener(
    receiver: mdns_sd::Receiver<ServiceEvent>,
    events_tx: broadcast::Sender<DiscoveryEvent>,
    state: Arc<RwLock<PeerTable>>,
    self_fullname: String,
    shutdown_flag: Arc<std::sync::atomic::AtomicBool>,
) -> JoinHandle<()> {
    tokio::task::spawn_blocking(move || {
        // Side map fullname -> node_id so ServiceRemoved is O(1) and
        // doesn't depend on instance-name string parsing.
        let mut fullname_to_node: std::collections::HashMap<String, harness_core::NodeId> =
            std::collections::HashMap::new();

        for event in &receiver {
            if shutdown_flag.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }
            match event {
                ServiceEvent::ServiceResolved(info) => {
                    let fullname = info.get_fullname().to_string();
                    if fullname == self_fullname {
                        continue; // ignore our own announcement
                    }
                    if let Some(peer) = info_to_peer(&info) {
                        let node_id = peer.node_id;
                        fullname_to_node.insert(fullname, node_id);
                        state.write().peers.insert(node_id, peer.clone());
                        let _ = events_tx.send(DiscoveryEvent::Added(peer));
                    } else {
                        tracing::debug!(
                            target: "harness.discovery",
                            fullname = info.get_fullname(),
                            "ignoring peer with malformed TXT record"
                        );
                    }
                }
                ServiceEvent::ServiceRemoved(_, fullname) => {
                    if fullname == self_fullname {
                        continue;
                    }
                    if let Some(node_id) = fullname_to_node.remove(&fullname) {
                        state.write().peers.remove(&node_id);
                        let _ = events_tx.send(DiscoveryEvent::Removed(node_id));
                    }
                }
                _ => {} // SearchStarted / SearchStopped / ServiceFound (pre-resolution)
            }
        }
    })
}

fn info_to_peer(info: &ServiceInfo) -> Option<DiscoveredPeer> {
    let txt_pairs: Vec<(String, String)> = info
        .get_properties()
        .iter()
        .map(|prop| (prop.key().to_string(), prop.val_str().to_string()))
        .collect();
    let payload = txt::decode(txt_pairs).ok()?;

    let port = info.get_port();
    let mut addrs: Vec<std::net::SocketAddr> = info
        .get_addresses()
        .iter()
        .map(|a| std::net::SocketAddr::new(*a, port))
        .collect();
    addrs.sort();
    addrs.dedup();
    if addrs.is_empty() {
        return None;
    }

    Some(DiscoveredPeer {
        node_id: payload.node_id,
        pubkey_fp: payload.pubkey_fp,
        addrs,
        mesh_name: payload.mesh_name,
        version: payload.version,
        source: DiscoverySource::Mdns,
    })
}
