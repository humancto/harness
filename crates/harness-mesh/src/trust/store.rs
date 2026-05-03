//! `TrustStore` — atomic-write, concurrent-safe wrapper around
//! `~/.harness/peers.toml`.
//!
//! Hot lookups (`tier`, `lookup_by_pubkey`, `all_peers`, `contains`) hit an
//! in-memory cache backed by a `parking_lot::Mutex`; the lock is taken sync
//! and never held across `.await` (or, for that matter, across the file
//! rewrite — `add` and `remove` clone the cache, mutate, persist, and only
//! then commit the cache).
//!
//! Mutation events are broadcast via `tokio::sync::broadcast`. Slow
//! subscribers see `RecvError::Lagged(n)` and resync via `all_peers()` —
//! that's the documented contract.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fs, io};

use harness_core::{NodeId, PublicKey};
use parking_lot::Mutex;
use tokio::sync::broadcast;

use crate::fs_util::{create_root_dir, enforce_mode_0600, write_atomic};

use super::file::PeersFile;
use super::{Peer, TrustError, TrustTier};

const PEERS_FILENAME: &str = "peers.toml";
const EVENT_CHANNEL_CAPACITY: usize = 256;

/// Mutation events broadcast by [`TrustStore::subscribe`].
///
/// `Added(Peer)` carries a full `Peer` (~248 bytes) while the other
/// variants are tens of bytes. `clippy::large_enum_variant` would suggest
/// boxing, but the event's whole purpose is to hand subscribers a `Peer`
/// they can use directly without indirection — and broadcast's bounded
/// 256-event buffer caps the worst-case footprint at ~64 KB per
/// subscriber, which is negligible.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
#[allow(clippy::large_enum_variant)]
pub enum TrustEvent {
    Added(Peer),
    Removed(NodeId),
    TierChanged {
        node_id: NodeId,
        from: TrustTier,
        to: TrustTier,
    },
}

/// Concurrent-safe trust store backed by `<root>/peers.toml`.
///
/// Cheap to clone (`Arc` internally) so callers can share one store across
/// tokio tasks. **Single daemon per `~/.harness/`** — running two harness
/// daemons against the same root corrupts state in both `identity.key` and
/// `peers.toml` (same TOCTOU class as `identity.rs`; documented).
#[derive(Clone)]
pub struct TrustStore {
    inner: Arc<Mutex<Inner>>,
    tx: broadcast::Sender<TrustEvent>,
}

impl std::fmt::Debug for TrustStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.lock();
        f.debug_struct("TrustStore")
            .field("root", &inner.root)
            .field("self_node_id", &inner.self_node_id)
            .field("peer_count", &inner.cache.len())
            .field("subscribers", &self.tx.receiver_count())
            .finish()
    }
}

struct Inner {
    root: PathBuf,
    self_node_id: NodeId,
    cache: HashMap<NodeId, Peer>,
}

impl TrustStore {
    /// Open or create `<root>/peers.toml`.
    ///
    /// - File present: load + validate every entry; reject self-entry with
    ///   `TrustError::SelfPeer`.
    /// - File absent: create an empty `format_version = 1` file mode `0600`.
    /// - Permissions: hard-error on Unix if mode != `0600`; warn on Windows.
    pub fn open(root: &Path, self_node_id: NodeId) -> Result<Self, TrustError> {
        create_root_dir(root)?;
        let path = root.join(PEERS_FILENAME);

        let cache = match fs::metadata(&path) {
            Ok(_) => {
                enforce_mode_0600(&path)?;
                let bytes = fs::read(&path).map_err(|e| TrustError::io(&path, e))?;
                let peers = PeersFile::parse(&bytes)?;
                let mut cache = HashMap::with_capacity(peers.len());
                for p in peers {
                    if p.node_id == self_node_id {
                        return Err(TrustError::SelfPeer(self_node_id));
                    }
                    cache.insert(p.node_id, p);
                }
                cache
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                let body = PeersFile::empty();
                let tmp = root.join(format!("{PEERS_FILENAME}.tmp.{}", std::process::id()));
                write_atomic(&tmp, &path, body.as_bytes())?;
                HashMap::new()
            }
            Err(e) => return Err(TrustError::io(&path, e)),
        };

        let (tx, _rx) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        Ok(Self {
            inner: Arc::new(Mutex::new(Inner {
                root: root.to_path_buf(),
                self_node_id,
                cache,
            })),
            tx,
        })
    }

    /// Idempotent insert. Re-adding the same `node_id` updates fields. Emits
    /// `TierChanged` if only the tier changed; otherwise `Added`. Refuses to
    /// add a peer whose `node_id` matches the local node's
    /// (`TrustError::SelfPeer`). Atomically rewrites the file.
    ///
    /// Persist-then-commit semantics: if the file rewrite fails (EROFS, disk
    /// full, fsync error), the in-memory cache is **not** mutated. Either
    /// both succeed or both stay at the old state.
    pub fn add(&self, peer: Peer) -> Result<(), TrustError> {
        let event = {
            let mut inner = self.inner.lock();
            if peer.node_id == inner.self_node_id {
                return Err(TrustError::SelfPeer(peer.node_id));
            }
            let prior = inner.cache.get(&peer.node_id).cloned();

            // Build the new cache without mutating the live one. If persist
            // fails, we drop the new cache and `inner.cache` is unchanged.
            let mut next = inner.cache.clone();
            next.insert(peer.node_id, peer.clone());
            Self::persist_cache(&inner.root, &next)?;
            inner.cache = next;

            match prior {
                Some(prev) if prev.tier != peer.tier => TrustEvent::TierChanged {
                    node_id: peer.node_id,
                    from: prev.tier,
                    to: peer.tier,
                },
                Some(_) | None => TrustEvent::Added(peer),
            }
        };
        // Send is best-effort — broadcast::send only fails when there are
        // zero receivers, which is fine. Lagging subscribers see Lagged
        // and resync via all_peers.
        let _ = self.tx.send(event);
        Ok(())
    }

    /// Remove by `node_id`. Returns `true` if a peer was removed. Emits
    /// `Removed` if so. Atomically rewrites the file.
    ///
    /// Persist-then-commit semantics: if the file rewrite fails, the
    /// in-memory cache is **not** mutated. This matters for trust:
    /// a remove that "appeared to succeed" in memory but didn't reach disk
    /// would silently downgrade a peer until daemon restart.
    pub fn remove(&self, node_id: &NodeId) -> Result<bool, TrustError> {
        let removed = {
            let mut inner = self.inner.lock();
            if !inner.cache.contains_key(node_id) {
                return Ok(false);
            }
            let mut next = inner.cache.clone();
            next.remove(node_id);
            Self::persist_cache(&inner.root, &next)?;
            inner.cache = next;
            true
        };
        if removed {
            let _ = self.tx.send(TrustEvent::Removed(*node_id));
        }
        Ok(removed)
    }

    /// O(1) lookup. Unknown peers get `TrustTier::Default` per PRD §10.2.
    #[must_use]
    pub fn tier(&self, node_id: &NodeId) -> TrustTier {
        self.inner
            .lock()
            .cache
            .get(node_id)
            .map_or(TrustTier::Default, |p| p.tier)
    }

    /// O(n) lookup. Used by the QUIC transport (1.4) at handshake time.
    #[must_use]
    pub fn lookup_by_pubkey(&self, pk: &PublicKey) -> Option<Peer> {
        self.inner
            .lock()
            .cache
            .values()
            .find(|p| &p.pubkey == pk)
            .cloned()
    }

    /// Snapshot of all peers, sorted by `node_id`.
    #[must_use]
    pub fn all_peers(&self) -> Vec<Peer> {
        let mut v: Vec<Peer> = self.inner.lock().cache.values().cloned().collect();
        v.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        v
    }

    /// Returns `true` if `node_id` is in the store at any tier.
    #[must_use]
    pub fn contains(&self, node_id: &NodeId) -> bool {
        self.inner.lock().cache.contains_key(node_id)
    }

    /// Subscribe to mutation events. Each subscriber gets their own
    /// receiver. Lag semantics: if a subscriber falls more than
    /// `EVENT_CHANNEL_CAPACITY` events behind, they get `RecvError::Lagged`
    /// and must resync via [`TrustStore::all_peers`].
    pub fn subscribe(&self) -> broadcast::Receiver<TrustEvent> {
        self.tx.subscribe()
    }

    /// Serialize `cache` and atomically write it to `<root>/peers.toml`.
    /// On any error the file is left untouched (the atomic-rename + tmp
    /// scrub in `write_atomic` makes that unconditional).
    fn persist_cache(
        root: &std::path::Path,
        cache: &HashMap<NodeId, Peer>,
    ) -> Result<(), TrustError> {
        let peers: Vec<Peer> = cache.values().cloned().collect();
        let body = PeersFile::serialize(&peers)?;
        let final_path = root.join(PEERS_FILENAME);
        let tmp = root.join(format!("{PEERS_FILENAME}.tmp.{}", std::process::id()));
        write_atomic(&tmp, &final_path, body.as_bytes())?;
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use harness_core::Identity;
    use proptest::prelude::*;
    use tempfile::TempDir;

    use crate::AddedVia;

    #[derive(Debug, Clone)]
    enum Op {
        Add { seed: u8, tier: TrustTier },
        Remove { seed: u8 },
    }

    fn arb_op() -> impl Strategy<Value = Op> {
        prop_oneof![
            (any::<u8>(), arb_tier()).prop_map(|(seed, tier)| Op::Add { seed, tier }),
            any::<u8>().prop_map(|seed| Op::Remove { seed }),
        ]
    }

    fn arb_tier() -> impl Strategy<Value = TrustTier> {
        prop_oneof![
            Just(TrustTier::Trusted),
            Just(TrustTier::Default),
            Just(TrustTier::Guest),
        ]
    }

    fn peer_for(seed: u8, tier: TrustTier) -> Peer {
        let id = Identity::from_secret_bytes(&[seed; 32]);
        Peer {
            node_id: id.node_id(),
            pubkey: *id.public_key(),
            hostname: format!("host-{seed:02x}"),
            tier,
            added_at: u64::from(seed),
            added_via: AddedVia::Imported,
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            // Each case opens a TrustStore and runs up to 32 ops; keep the
            // case count modest so the suite stays fast.
            cases: 64,
            ..ProptestConfig::default()
        })]

        /// Random sequence of add/remove preserves the store invariants:
        /// every persisted entry is internally consistent (`node_id ==
        /// blake3(pubkey)[..16]`), no duplicates, sorted by `node_id`,
        /// and reopening yields the same state.
        #[test]
        fn random_add_remove_sequence_preserves_invariants(ops in proptest::collection::vec(arb_op(), 0..32)) {
            let tmp = TempDir::new().expect("tempdir");
            let root = tmp.path().to_path_buf();
            let self_id = NodeId::from_bytes([0xCC; 16]);
            let store = TrustStore::open(&root, self_id).expect("open");

            for op in &ops {
                match op {
                    Op::Add { seed, tier } => {
                        let peer = peer_for(*seed, *tier);
                        if peer.node_id == self_id {
                            // Skip self-id; would always error.
                            continue;
                        }
                        store.add(peer).expect("add");
                    }
                    Op::Remove { seed } => {
                        let peer = peer_for(*seed, TrustTier::Default);
                        let _ = store.remove(&peer.node_id).expect("remove");
                    }
                }
            }

            // Invariants on the live store.
            let peers = store.all_peers();
            for window in peers.windows(2) {
                prop_assert!(window[0].node_id < window[1].node_id, "must be sorted");
            }
            for p in &peers {
                prop_assert_eq!(p.node_id, NodeId::from_pubkey(&p.pubkey),
                    "every persisted peer must satisfy node_id == blake3(pubkey)[..16]");
            }
            // Reopen and assert equality.
            drop(store);
            let store2 = TrustStore::open(&root, self_id).expect("reopen");
            prop_assert_eq!(store2.all_peers(), peers);
        }
    }
}
