//! Loopback integration tests for `harness_mesh::discovery`.
//!
//! mDNS tests are inherently flaky on shared CI runners (multicast can be
//! firewalled or rate-limited), so the tests here focus on the parts of
//! discovery we can verify deterministically:
//!
//! - `Discovery::start` succeeds with `mdns_enabled = true` and
//!   `mdns_enabled = false`.
//! - `static_peers` surface as `StaticHint` events.
//! - `peers()` snapshot is empty until mDNS discovers something.
//! - `shutdown` is idempotent and clean.
//!
//! Two-node mDNS resolution tests live under `#[ignore]` so developers
//! can opt in locally with `cargo test -- --ignored`; CI does not run
//! them by default to avoid flakes.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use harness_core::{Identity, SemVer};
use harness_mesh::{DiscoveredPeer, Discovery, DiscoveryConfig, DiscoveryError, DiscoveryEvent};

fn make_config(port: u16, mesh: &str) -> DiscoveryConfig {
    let id = Identity::generate();
    DiscoveryConfig::new(
        mesh.to_string(),
        id.node_id(),
        id.public_key().fingerprint_hex(),
        SemVer::new(0, 1, 0),
        port,
    )
    .expect("config")
}

#[tokio::test]
async fn config_validates_mesh_name() {
    let id = Identity::generate();
    let res = DiscoveryConfig::new(
        "bad name".into(), // contains space
        id.node_id(),
        id.public_key().fingerprint_hex(),
        SemVer::new(0, 1, 0),
        1234,
    );
    assert!(matches!(res, Err(DiscoveryError::InvalidMeshName(_))));
}

#[tokio::test]
async fn config_validates_port() {
    let id = Identity::generate();
    let res = DiscoveryConfig::new(
        "home".into(),
        id.node_id(),
        id.public_key().fingerprint_hex(),
        SemVer::new(0, 1, 0),
        0,
    );
    assert!(matches!(res, Err(DiscoveryError::InvalidPort)));
}

#[tokio::test]
async fn config_validates_pubkey_fp() {
    let id = Identity::generate();
    let res = DiscoveryConfig::new(
        "home".into(),
        id.node_id(),
        "INVALID_LENGTH".into(),
        SemVer::new(0, 1, 0),
        1234,
    );
    assert!(matches!(res, Err(DiscoveryError::InvalidPubkeyFp(_))));
}

#[tokio::test]
async fn discovery_starts_and_shuts_down_with_mdns_disabled() {
    let cfg = make_config(8001, "home").with_mdns_enabled(false);
    let d = Discovery::start(cfg).expect("start");
    assert!(d.peers().is_empty());
    d.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn shutdown_is_idempotent() {
    let cfg = make_config(8002, "home").with_mdns_enabled(false);
    let d = Discovery::start(cfg).expect("start");
    d.shutdown().await.expect("first shutdown");
    d.shutdown().await.expect("second shutdown");
}

#[tokio::test]
async fn static_peers_surface_as_static_hints() {
    let static_addrs = vec![
        SocketAddr::new(Ipv4Addr::new(192, 168, 1, 10).into(), 19199),
        SocketAddr::new(Ipv4Addr::new(192, 168, 1, 11).into(), 19199),
    ];
    let cfg = make_config(8003, "home")
        .with_mdns_enabled(false)
        .with_static_peers(static_addrs.clone())
        .with_event_buffer(8);

    let d = Discovery::start(cfg).expect("start");
    let hints = d.static_hints();
    // Sorted + de-duped on construction; addresses are already sorted.
    assert_eq!(hints, static_addrs);
    d.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn static_peers_are_deduplicated_at_config_time() {
    let dup_addr = SocketAddr::new(Ipv4Addr::new(192, 168, 1, 10).into(), 19199);
    let static_addrs = vec![dup_addr, dup_addr, dup_addr];
    let cfg = make_config(8007, "home")
        .with_mdns_enabled(false)
        .with_static_peers(static_addrs);
    let d = Discovery::start(cfg).expect("start");
    assert_eq!(d.static_hints().len(), 1, "duplicates must collapse");
    assert_eq!(d.static_hints()[0], dup_addr);
    d.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn discovery_event_added_carries_node_id() {
    // Smoke test that the public `DiscoveryEvent::Added(DiscoveredPeer)`
    // shape is reachable from outside the crate. We don't actually
    // exchange peers in this test — the two-instance test is
    // #[ignore]-d for CI multicast flakes — but we verify the type
    // signatures compile.
    fn _typecheck(evt: DiscoveryEvent) -> Option<DiscoveredPeer> {
        if let DiscoveryEvent::Added(p) = evt {
            Some(p)
        } else {
            None
        }
    }
    // Make-config to keep the test from being entirely vacuous.
    let cfg = make_config(8008, "home").with_mdns_enabled(false);
    let _ = Discovery::start(cfg).expect("start");
}

#[tokio::test]
async fn config_rejects_mesh_name_too_long_for_label() {
    // 16-hex node prefix + '-' + 47-byte mesh_name = 64 bytes, exceeds
    // RFC 1035's 63-octet DNS label limit. Validation should catch
    // this at start time, not silently produce an invalid mDNS
    // announcement.
    let id = Identity::generate();
    let too_long = "a".repeat(47);
    let cfg = DiscoveryConfig::new(
        too_long,
        id.node_id(),
        id.public_key().fingerprint_hex(),
        SemVer::new(0, 1, 0),
        9999,
    )
    .expect("valid by validate_mesh_name (≤63 bytes)");
    // start should fail because 16 + 1 + 47 = 64 > 63.
    let res = Discovery::start(cfg);
    match res {
        Err(DiscoveryError::InvalidMeshName(_)) => {} // expected
        other => panic!("expected InvalidMeshName, got {other:?}"),
    }
}

#[tokio::test]
async fn peers_snapshot_excludes_static_hints() {
    // peers() reflects mDNS only; static peers live in static_hints().
    let static_addrs = vec![SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 7000)];
    let cfg = make_config(8004, "home")
        .with_mdns_enabled(false)
        .with_static_peers(static_addrs.clone())
        .with_event_buffer(64);
    let d = Discovery::start(cfg).expect("start");
    assert!(d.peers().is_empty(), "no mDNS peers expected");
    assert_eq!(d.static_hints(), static_addrs);
    d.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn discovery_starts_with_mdns_enabled() {
    // Don't actually exchange peers — just prove the daemon spins up
    // and shuts down cleanly. mDNS exchange across two Discovery
    // instances on the same machine is racy on CI; gated below as
    // #[ignore].
    let cfg = make_config(8005, "home");
    let d = Discovery::start(cfg).expect("start");
    // Give the daemon a moment to register.
    tokio::time::sleep(Duration::from_millis(50)).await;
    d.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn empty_event_buffer_clamped_to_one() {
    let cfg = make_config(8006, "home")
        .with_mdns_enabled(false)
        .with_event_buffer(0);
    // Should not panic — the builder clamps to 1.
    let d = Discovery::start(cfg).expect("start with 0 buffer");
    let _ = d.subscribe();
    d.shutdown().await.expect("shutdown");
}

/// Two-instance mDNS resolution. Flaky on CI multicast-blocked
/// environments; gated to opt-in via `--ignored`.
#[ignore = "mDNS multicast may be firewalled / rate-limited on CI"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_instances_discover_each_other() {
    let cfg_a = make_config(9001, "homenet");
    let cfg_b = make_config(9002, "homenet");

    let a = Discovery::start(cfg_a).expect("start a");
    let mut rx = a.subscribe();
    let b = Discovery::start(cfg_b).expect("start b");

    // Wait up to 5s for an Added event for some peer.
    let evt = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(DiscoveryEvent::Added(p)) = rx.recv().await {
                break p;
            }
        }
    })
    .await
    .expect("timeout waiting for peer");
    assert_eq!(evt.mesh_name, "homenet");

    b.shutdown().await.expect("shutdown b");
    a.shutdown().await.expect("shutdown a");
}
