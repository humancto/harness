//! Mesh events broadcast over `WS /api/v1/events`.
//!
//! The internal channel inside `harness-mesh` (`HeartbeatService::events`)
//! carries `TableEvent`s. This module translates those into the public
//! [`MeshEvent`] DTO used by the UI. Two enums are intentionally kept
//! separate so the wire shape can evolve without churning the internal
//! channel and vice versa.

use harness_mesh::heartbeat::{PeerTable, TableEvent};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use crate::dto::PeerDto;

/// Events the UI consumes.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MeshEvent {
    PeerAdded {
        peer: PeerDto,
    },
    PeerUpdated {
        peer: PeerDto,
    },
    PeerEvicted {
        node_id: String,
    },
    LeaderChanged {
        leader: Option<String>,
        brain_score: i32,
    },
}

impl MeshEvent {
    /// Returns `None` for the race window where a `Recorded` event
    /// was published but the peer was already evicted by the time we
    /// looked it up — emitting a `peer_added` for a peer that no
    /// longer exists would race ahead of the inevitable `peer_evicted`
    /// and confuse the UI's reducer.
    #[must_use]
    pub fn from_table_event(
        event: &TableEvent,
        mesh_name: &str,
        peers: &PeerTable,
    ) -> Option<Self> {
        match event {
            TableEvent::Recorded { node_id, was_new } => {
                let entry = peers.get(node_id)?;
                let peer = PeerDto::from_entry(&entry, mesh_name);
                Some(if *was_new {
                    MeshEvent::PeerAdded { peer }
                } else {
                    MeshEvent::PeerUpdated { peer }
                })
            }
            TableEvent::Evicted { node_id } => Some(MeshEvent::PeerEvicted {
                node_id: format!("{node_id}"),
            }),
        }
    }
}

/// Spawns a task that bridges `harness-mesh`'s internal `TableEvent`
/// stream into the public `MeshEvent` stream consumed by the WS handler.
///
/// Returns a handle that aborts the bridge task on drop.
pub struct TableEventBridge {
    task: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl std::fmt::Debug for TableEventBridge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableEventBridge").finish_non_exhaustive()
    }
}

impl TableEventBridge {
    #[must_use]
    pub fn spawn(
        mut source: broadcast::Receiver<TableEvent>,
        sink: broadcast::Sender<MeshEvent>,
        peers: PeerTable,
        mesh_name: String,
    ) -> Self {
        let task = tokio::spawn(async move {
            loop {
                match source.recv().await {
                    Ok(table_event) => {
                        if let Some(event) =
                            MeshEvent::from_table_event(&table_event, &mesh_name, &peers)
                        {
                            let _ = sink.send(event);
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(
                            target: "harness.api.events",
                            dropped = n,
                            "TableEvent bridge fell behind; consumers may need to refetch snapshot"
                        );
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });
        Self {
            task: parking_lot::Mutex::new(Some(task)),
        }
    }

    pub async fn shutdown(self) {
        let task = self.task.lock().take();
        if let Some(t) = task {
            t.abort();
            let _ = t.await;
        }
    }
}

impl Drop for TableEventBridge {
    fn drop(&mut self) {
        if let Some(t) = self.task.lock().take() {
            t.abort();
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use harness_core::{Heartbeat, NodeId, Signature};

    fn sample_hb(seq: u64) -> Heartbeat {
        Heartbeat {
            node_id: NodeId::from_bytes([0x33; 16]),
            seq,
            timestamp: 1_700_000_000_000,
            queue_depth: 0,
            cpu_busy_pct: 10,
            cpu_pinned_count: 0,
            ram_used_mb: 1024,
            ram_total_mb: 8192,
            gpu_used_mb: 0,
            gpu_total_mb: 0,
            capabilities_hash: [0; 16],
            replica_head: [0u8; 32],
            in_flight: vec![],
            leader_belief: NodeId::from_bytes([0x33; 16]),
            brain_score: 100,
            on_battery: true,
            paused: false,
            version: harness_core::SemVer {
                major: 0,
                minor: 1,
                patch: 0,
            },
            sig: Signature::from_bytes([0u8; 64]),
        }
    }

    #[test]
    fn translates_recorded_was_new_to_peer_added() {
        let peers = PeerTable::new();
        peers.record(sample_hb(1));
        let event = TableEvent::Recorded {
            node_id: NodeId::from_bytes([0x33; 16]),
            was_new: true,
        };
        let mesh_event =
            MeshEvent::from_table_event(&event, "lan", &peers).expect("recorded peer present");
        assert!(matches!(mesh_event, MeshEvent::PeerAdded { .. }));
    }

    #[test]
    fn translates_recorded_was_old_to_peer_updated() {
        let peers = PeerTable::new();
        peers.record(sample_hb(1));
        let event = TableEvent::Recorded {
            node_id: NodeId::from_bytes([0x33; 16]),
            was_new: false,
        };
        let mesh_event =
            MeshEvent::from_table_event(&event, "lan", &peers).expect("recorded peer present");
        assert!(matches!(mesh_event, MeshEvent::PeerUpdated { .. }));
    }

    #[test]
    fn translates_evicted_to_peer_evicted() {
        let peers = PeerTable::new();
        let event = TableEvent::Evicted {
            node_id: NodeId::from_bytes([0xAB; 16]),
        };
        let mesh_event =
            MeshEvent::from_table_event(&event, "lan", &peers).expect("evicted always emits");
        match mesh_event {
            MeshEvent::PeerEvicted { node_id } => {
                assert_eq!(node_id, "abababababababababababababababab");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn recorded_for_evicted_peer_returns_none() {
        // Race window: a Recorded event was published, then the peer
        // got evicted before this lookup ran. Returning None instead
        // of fabricating a stale dto avoids racing ahead of the
        // forthcoming Evicted event.
        let peers = PeerTable::new();
        let event = TableEvent::Recorded {
            node_id: NodeId::from_bytes([0x77; 16]),
            was_new: true,
        };
        assert!(MeshEvent::from_table_event(&event, "lan", &peers).is_none());
    }

    #[test]
    fn each_variant_round_trips_json() {
        let cases = vec![
            MeshEvent::PeerEvicted {
                node_id: "00112233445566778899aabbccddeeff".into(),
            },
            MeshEvent::LeaderChanged {
                leader: Some("00112233445566778899aabbccddeeff".into()),
                brain_score: 250,
            },
            MeshEvent::LeaderChanged {
                leader: None,
                brain_score: -200,
            },
        ];
        for event in cases {
            let json = serde_json::to_string(&event).expect("serialize");
            let back: MeshEvent = serde_json::from_str(&json).expect("deserialize");
            // PartialEq isn't derived; round-trip via JSON.
            let json2 = serde_json::to_string(&back).expect("serialize round 2");
            assert_eq!(json, json2);
        }
    }
}
