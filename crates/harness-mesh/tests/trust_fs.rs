//! Filesystem + concurrency integration tests for `harness_mesh::trust`.
//!
//! All tests use `tempfile::TempDir`; **no writes touch the user's real
//! `~/.harness/`**.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::fs;
use std::io::Write;

use harness_core::{Identity, NodeId};
use harness_mesh::{AddedVia, Peer, TrustError, TrustEvent, TrustStore, TrustTier};
use tempfile::TempDir;

#[cfg(unix)]
fn mode_of(path: &std::path::Path) -> u32 {
    use std::os::unix::fs::PermissionsExt;
    fs::metadata(path).expect("stat").permissions().mode() & 0o777
}

fn sample_peer(seed_byte: u8, tier: TrustTier) -> Peer {
    let id = Identity::from_secret_bytes(&[seed_byte; 32]);
    Peer {
        node_id: id.node_id(),
        pubkey: *id.public_key(),
        hostname: format!("host-{seed_byte:02x}"),
        tier,
        added_at: 1_700_000_000 + u64::from(seed_byte),
        added_via: AddedVia::PairingCode,
    }
}

fn random_self_id() -> NodeId {
    NodeId::from_bytes([0xCC; 16])
}

#[test]
fn open_creates_empty_file_with_mode_0600() {
    let tmp = TempDir::new().expect("tempdir");
    let root = tmp.path().join(".harness");
    let store = TrustStore::open(&root, random_self_id()).expect("open");
    let path = root.join("peers.toml");
    assert!(path.exists());
    assert!(store.all_peers().is_empty());

    #[cfg(unix)]
    {
        assert_eq!(mode_of(&path), 0o600, "peers.toml must be born 0600");
        assert_eq!(mode_of(&root), 0o700, "root dir must be 0700");
    }
}

#[test]
fn add_persists_across_reopen() {
    let tmp = TempDir::new().expect("tempdir");
    let root = tmp.path();
    {
        let store = TrustStore::open(root, random_self_id()).expect("open");
        store
            .add(sample_peer(0x10, TrustTier::Trusted))
            .expect("add");
        store
            .add(sample_peer(0x20, TrustTier::Default))
            .expect("add");
        store.add(sample_peer(0x30, TrustTier::Guest)).expect("add");
    }
    let store = TrustStore::open(root, random_self_id()).expect("reopen");
    let peers = store.all_peers();
    assert_eq!(peers.len(), 3);
    // Sorted by node_id (assertion: adjacent pairs are ordered).
    for window in peers.windows(2) {
        assert!(window[0].node_id < window[1].node_id);
    }
}

#[test]
fn remove_persists_across_reopen() {
    let tmp = TempDir::new().expect("tempdir");
    let root = tmp.path();
    let target_id;
    {
        let store = TrustStore::open(root, random_self_id()).expect("open");
        let target = sample_peer(0x20, TrustTier::Default);
        target_id = target.node_id;
        store
            .add(sample_peer(0x10, TrustTier::Trusted))
            .expect("add");
        store.add(target).expect("add");
        store.add(sample_peer(0x30, TrustTier::Guest)).expect("add");
        assert!(store.remove(&target_id).expect("remove"));
    }
    let store = TrustStore::open(root, random_self_id()).expect("reopen");
    assert_eq!(store.all_peers().len(), 2);
    assert!(!store.contains(&target_id));
}

#[test]
fn add_idempotent_by_node_id() {
    let tmp = TempDir::new().expect("tempdir");
    let store = TrustStore::open(tmp.path(), random_self_id()).expect("open");
    let p = sample_peer(0x42, TrustTier::Default);
    store.add(p.clone()).expect("add 1");
    store.add(p.clone()).expect("add 2");
    let peers = store.all_peers();
    assert_eq!(peers.len(), 1);
    assert_eq!(peers[0], p);
}

#[test]
fn tier_unknown_returns_default() {
    let tmp = TempDir::new().expect("tempdir");
    let store = TrustStore::open(tmp.path(), random_self_id()).expect("open");
    let unknown = NodeId::from_bytes([0xFE; 16]);
    assert_eq!(store.tier(&unknown), TrustTier::Default);
}

#[test]
fn lookup_by_pubkey_returns_peer() {
    let tmp = TempDir::new().expect("tempdir");
    let store = TrustStore::open(tmp.path(), random_self_id()).expect("open");
    let p = sample_peer(0xA1, TrustTier::Trusted);
    store.add(p.clone()).expect("add");
    let found = store.lookup_by_pubkey(&p.pubkey).expect("found");
    assert_eq!(found, p);
}

#[test]
fn cannot_add_self() {
    let tmp = TempDir::new().expect("tempdir");
    let id = Identity::from_secret_bytes(&[0xAA; 32]);
    let store = TrustStore::open(tmp.path(), id.node_id()).expect("open");
    let self_peer = Peer {
        node_id: id.node_id(),
        pubkey: *id.public_key(),
        hostname: "myself".into(),
        tier: TrustTier::Trusted,
        added_at: 0,
        added_via: AddedVia::PairingCode,
    };
    let err = store.add(self_peer).expect_err("self_add");
    match err {
        TrustError::SelfPeer(nid) => assert_eq!(nid, id.node_id()),
        other => panic!("expected SelfPeer, got {other:?}"),
    }
}

#[test]
fn open_rejects_self_in_existing_file() {
    let tmp = TempDir::new().expect("tempdir");
    let root = tmp.path();
    let id = Identity::from_secret_bytes(&[0xBB; 32]);
    {
        let store = TrustStore::open(root, random_self_id()).expect("open");
        let self_peer = Peer {
            node_id: id.node_id(),
            pubkey: *id.public_key(),
            hostname: "spurious".into(),
            tier: TrustTier::Trusted,
            added_at: 0,
            added_via: AddedVia::Imported,
        };
        store.add(self_peer).expect("add");
    }
    let err = TrustStore::open(root, id.node_id()).expect_err("self in file");
    match err {
        TrustError::SelfPeer(nid) => assert_eq!(nid, id.node_id()),
        other => panic!("expected SelfPeer, got {other:?}"),
    }
}

#[cfg(unix)]
#[test]
fn unsafe_permissions_rejected() {
    use std::os::unix::fs::PermissionsExt;
    let tmp = TempDir::new().expect("tempdir");
    let root = tmp.path();
    let _ = TrustStore::open(root, random_self_id()).expect("first open");
    let path = root.join("peers.toml");
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).expect("chmod");
    let err = TrustStore::open(root, random_self_id()).expect_err("0644");
    match err {
        TrustError::UnsafePermissions { actual } => assert_eq!(actual, 0o644),
        other => panic!("expected UnsafePermissions, got {other:?}"),
    }
}

#[test]
fn corrupt_toml_rejected() {
    let tmp = TempDir::new().expect("tempdir");
    let root = tmp.path();
    fs::create_dir_all(root).expect("mkdir");
    let path = root.join("peers.toml");

    // Write garbage with the right mode so we exercise the parse path,
    // not the permission path.
    let mut opts = fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut f = opts.open(&path).expect("open");
    f.write_all(b"this is not valid toml }}}").expect("write");
    drop(f);

    let err = TrustStore::open(root, random_self_id()).expect_err("corrupt");
    assert!(matches!(err, TrustError::Parse(_)), "got {err:?}");
}

#[tokio::test]
async fn events_stream_delivers_added() {
    let tmp = TempDir::new().expect("tempdir");
    let store = TrustStore::open(tmp.path(), random_self_id()).expect("open");
    let mut rx = store.subscribe();
    let peer = sample_peer(0x77, TrustTier::Trusted);

    let store_clone = store.clone();
    let peer_clone = peer.clone();
    tokio::spawn(async move {
        store_clone.add(peer_clone).expect("add");
    });

    let evt = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
        .await
        .expect("timeout")
        .expect("recv");
    match evt {
        TrustEvent::Added(p) => assert_eq!(p, peer),
        other => panic!("expected Added, got {other:?}"),
    }
}

#[tokio::test]
async fn events_stream_delivers_tier_changed() {
    let tmp = TempDir::new().expect("tempdir");
    let store = TrustStore::open(tmp.path(), random_self_id()).expect("open");

    let initial = sample_peer(0x10, TrustTier::Default);
    store.add(initial.clone()).expect("first add");

    let mut rx = store.subscribe();
    let upgraded = Peer {
        tier: TrustTier::Trusted,
        ..initial.clone()
    };
    store.add(upgraded.clone()).expect("upgrade");

    let evt = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
        .await
        .expect("timeout")
        .expect("recv");
    match evt {
        TrustEvent::TierChanged { node_id, from, to } => {
            assert_eq!(node_id, initial.node_id);
            assert_eq!(from, TrustTier::Default);
            assert_eq!(to, TrustTier::Trusted);
        }
        other => panic!("expected TierChanged, got {other:?}"),
    }
}

#[tokio::test]
async fn events_stream_delivers_removed() {
    let tmp = TempDir::new().expect("tempdir");
    let store = TrustStore::open(tmp.path(), random_self_id()).expect("open");
    let peer = sample_peer(0x55, TrustTier::Default);
    store.add(peer.clone()).expect("add");
    let mut rx = store.subscribe();
    assert!(store.remove(&peer.node_id).expect("remove"));

    let evt = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
        .await
        .expect("timeout")
        .expect("recv");
    match evt {
        TrustEvent::Removed(nid) => assert_eq!(nid, peer.node_id),
        other => panic!("expected Removed, got {other:?}"),
    }
}

/// Regression test for the persist-then-commit invariant.
///
/// If `write_atomic` fails (here we force it by precreating a stale tmp
/// file with `create_new(true)` semantics), the in-memory cache must be
/// unchanged — otherwise a remove that failed to hit disk would silently
/// downgrade a previously-trusted peer until daemon restart.
#[cfg(unix)]
#[test]
fn persist_failure_does_not_mutate_cache() {
    let tmp = TempDir::new().expect("tempdir");
    let root = tmp.path();
    let store = TrustStore::open(root, random_self_id()).expect("open");
    let alice = sample_peer(0xA1, TrustTier::Trusted);
    store.add(alice.clone()).expect("add alice");
    assert_eq!(store.tier(&alice.node_id), TrustTier::Trusted);

    // Block the next persist by precreating the exact tmp filename
    // `write_atomic` uses (`<root>/peers.toml.tmp.<pid>`). `create_new(true)`
    // will fail with `AlreadyExists` and the persist-then-commit guard must
    // leave the cache unchanged.
    let tmp_path = root.join(format!("peers.toml.tmp.{}", std::process::id()));
    fs::write(&tmp_path, b"sentinel").expect("seed stale tmp");

    let err = store
        .remove(&alice.node_id)
        .expect_err("remove must fail when tmp exists");
    assert!(matches!(err, TrustError::Io { .. }), "got {err:?}");

    // Cache must reflect the pre-failure state — alice still Trusted, not
    // silently downgraded to Default.
    assert_eq!(
        store.tier(&alice.node_id),
        TrustTier::Trusted,
        "persist failure must not mutate the cache"
    );
    assert!(store.contains(&alice.node_id));

    // Clean up the sentinel; subsequent calls work again.
    fs::remove_file(&tmp_path).expect("cleanup");
    assert!(store.remove(&alice.node_id).expect("remove after cleanup"));
    assert!(!store.contains(&alice.node_id));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_adds_4_distinct_peers_all_persist() {
    let tmp = TempDir::new().expect("tempdir");
    let root = tmp.path().to_path_buf();
    let store = TrustStore::open(&root, random_self_id()).expect("open");

    let mut handles = Vec::new();
    for i in 0u8..4 {
        let s = store.clone();
        handles.push(tokio::spawn(async move {
            s.add(sample_peer(0xA0 + i, TrustTier::Default))
                .expect("add");
        }));
    }
    for h in handles {
        h.await.expect("join");
    }

    drop(store);
    let store = TrustStore::open(&root, random_self_id()).expect("reopen");
    let peers = store.all_peers();
    assert_eq!(peers.len(), 4, "all 4 concurrent adds must persist");
}
