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
        let _ = self.daemon.shutdown();
        if let Some(handle) = self.listener {
            handle.abort();
        }
    }
}

fn build_service_info(config: &DiscoveryConfig) -> Result<ServiceInfo, DiscoveryError> {
    // Instance name = <node_id_hex>-<mesh_name>. mDNS uses instance name
    // as the unique discriminator; node_id already serves that role.
    let instance = format!("{}-{}", config.node_id, config.mesh_name);

    // Hostname: the OS hostname, or `node_id`.harness.local. as fallback.
    let host = format!("{}.harness.local.", config.node_id);

    let payload = TxtPayload {
        node_id: config.node_id,
        pubkey_fp: config.pubkey_fp.clone(),
        mesh_name: config.mesh_name.clone(),
        version: config.version,
    };
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
    // SRV TTL — the daemon's default is fine (120s); we don't override.
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
        for event in &receiver {
            if shutdown_flag.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }
            match event {
                ServiceEvent::ServiceResolved(info) => {
                    if info.get_fullname() == self_fullname {
                        continue; // ignore our own announcement
                    }
                    if let Some(peer) = info_to_peer(&info) {
                        let node_id = peer.node_id;
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
                    // Find the node_id for this fullname and drop it.
                    let removed = {
                        let mut t = state.write();
                        let id = t
                            .peers
                            .iter()
                            .find(|(_, p)| {
                                // We don't store the fullname, so we have
                                // to match heuristically via the instance
                                // prefix. Instance names start with
                                // <node_id>-... so strip the prefix.
                                fullname.starts_with(&p.node_id.to_string())
                            })
                            .map(|(id, _)| *id);
                        id.and_then(|id| t.peers.remove(&id).map(|_| id))
                    };
                    if let Some(id) = removed {
                        let _ = events_tx.send(DiscoveryEvent::Removed(id));
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
