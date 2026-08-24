//! 5.13c-1 money tests — two full daemons over loopback QUIC, pinning
//! each other's audit heads and catching a fork.
//!
//! This is the property the item exists to deliver: a node can rewrite
//! its own chain and it will still verify locally, but it cannot
//! un-tell a peer that already pinned `(seq, entry_hash)`.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::time::Duration;

use harness_core::{AuditAction, AuditActor, AuditRecord, AuditSink, Signable, Signature};
use harness_store::{PinStatus, StoreAuditSink};

use crate::fanout_tests::{boot_pair, wait_for_mesh, TestDaemon};

fn append_entries(daemon: &TestDaemon, n: usize) {
    let sink = StoreAuditSink::new(daemon.store.clone(), daemon.node_id());
    for i in 0..n {
        sink.record(
            AuditRecord::new(AuditAction::TaskDispatched, AuditActor::System)
                .with_subject(format!("task-{i}")),
        );
    }
}

async fn wait_for_pin(daemon: &TestDaemon, subject: harness_core::NodeId, what: &str) -> u64 {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let pins = daemon.store.peer_head_pins(subject).expect("pins");
        if let Some(newest) = pins.last() {
            return newest.seq;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn m01_peers_pin_each_others_heads() {
    let (a, b) = boot_pair(None).await;
    wait_for_mesh(&a, &b).await;

    append_entries(&a, 5);
    let seq = wait_for_pin(&b, a.node_id(), "b to pin a's head").await;
    assert!(seq >= 1, "b pinned a real position, got {seq}");

    let pins = b.store.peer_head_pins(a.node_id()).expect("pins");
    let pin = pins.last().expect("pin");
    assert_eq!(pin.status, PinStatus::Unchecked, "no entries pulled yet");

    // The pin matches a's actual chain at that seq.
    let rows = a.store.audit_recent(None, None, None, 100).expect("recent");
    let at_seq = rows
        .iter()
        .find(|r| r.seq == pin.seq && r.node_id == a.node_id())
        .expect("a has that entry");
    assert_eq!(
        pin.entry_hash, at_seq.entry_hash,
        "the pin is a's real hash at that position"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn m02_growth_adds_pins_without_replacing_the_old_ones() {
    // Append-only is the whole mechanism: "higher seq replaces" would
    // leave one pin, and a truncate-and-regrow would erase the
    // evidence of itself on the way past.
    let (a, b) = boot_pair(None).await;
    wait_for_mesh(&a, &b).await;

    append_entries(&a, 3);
    let first = wait_for_pin(&b, a.node_id(), "first pin").await;

    append_entries(&a, 20);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let pins = b.store.peer_head_pins(a.node_id()).expect("pins");
        if pins.len() >= 2 {
            assert_eq!(pins[0].seq, first, "the earlier pin survived growth");
            assert!(pins.last().expect("last").seq > first, "and a newer one");
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "b never pinned a second position, have {}",
            pins.len()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn m03_a_rewritten_chain_is_caught_as_a_fork() {
    // The money test. A appends, B pins. A then rewrites its own
    // history at the pinned position and re-advertises. A's chain
    // verifies locally — it re-signed everything — but B holds a pin
    // A cannot un-tell.
    let (a, b) = boot_pair(None).await;
    wait_for_mesh(&a, &b).await;

    append_entries(&a, 4);
    let pinned_seq = wait_for_pin(&b, a.node_id(), "b to pin a's head").await;
    let held = b
        .store
        .peer_head_pins(a.node_id())
        .expect("pins")
        .last()
        .expect("pin")
        .entry_hash;

    // A rewrites: a different hash at the SAME position, validly
    // signed with A's own key. This is what a node lying about its
    // own history looks like from outside.
    let mut forged = harness_core::AuditHead {
        node_id: a.node_id(),
        seq: pinned_seq,
        entry_hash: [0x99; 32],
        at_ms: 9_999_999,
        sig: Signature::from_bytes([0u8; 64]),
    };
    forged.sign(a.identity.as_ref()).expect("sign");
    assert_ne!(forged.entry_hash, held, "the rewrite really differs");

    let outcome = b
        .store
        .pin_peer_head(&forged, a.node_id(), 10_000_000)
        .expect("pin");
    assert!(
        matches!(outcome, harness_store::PinOutcome::Fork { .. }),
        "a second signed head at a pinned position is a fork, got {outcome:?}"
    );

    let conflicts = b.store.head_conflicts(10).expect("conflicts");
    assert_eq!(conflicts.len(), 1);
    assert_eq!(conflicts[0].node_id, a.node_id());
    assert_eq!(conflicts[0].seq, pinned_seq);
    assert_eq!(conflicts[0].held_hash, held);
    assert_eq!(conflicts[0].other_hash, [0x99; 32]);

    // A's own chain still verifies — which is exactly why the pin is
    // needed. Local verification cannot catch this.
    assert!(
        matches!(
            a.store.audit_verify_chain(a.node_id(), 1, 1_000),
            Ok(harness_store::ChainStatus::Verified { .. })
        ),
        "the liar's own chain is self-consistent"
    );
    assert_eq!(
        b.store
            .peer_head_pins(a.node_id())
            .expect("pins")
            .iter()
            .find(|p| p.seq == pinned_seq)
            .expect("pin")
            .status,
        PinStatus::Contradicted
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn m04_a_head_from_an_unknown_node_is_dropped() {
    // The relay key must come from OUR trust store. If a relayer could
    // supply the key, it would mint a keypair and forge the whole
    // history it claims to be reporting.
    let (a, b) = boot_pair(None).await;
    wait_for_mesh(&a, &b).await;

    let stranger = harness_core::Identity::generate();
    let mut head = harness_core::AuditHead {
        node_id: stranger.node_id(),
        seq: 7,
        entry_hash: [0x77; 32],
        at_ms: 1_000,
        sig: Signature::from_bytes([0u8; 64]),
    };
    head.sign(&stranger).expect("sign");

    let mut env = harness_core::AuditSyncEnvelope {
        source: a.node_id(),
        assembled_at: 1_000,
        heads: vec![head],
        sig: Signature::from_bytes([0u8; 64]),
    };
    env.sign(a.identity.as_ref()).expect("sign");

    // Feed it through the ingest path b's recv arm uses.
    let svc = crate::audit_sync::AuditSyncService::new(
        b.store.clone(),
        b.identity.clone(),
        b.trust.clone(),
    );
    let outcomes = svc.ingest(a.node_id(), &env, 2_000);
    assert!(outcomes.is_empty(), "unknown-node head dropped");
    assert!(b
        .store
        .peer_head_pins(stranger.node_id())
        .expect("pins")
        .is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn m05_a_forged_signature_is_rejected_even_from_a_trusted_relay() {
    let (a, b) = boot_pair(None).await;
    wait_for_mesh(&a, &b).await;

    // A head claiming to be A's, but signed by nobody.
    let head = harness_core::AuditHead {
        node_id: a.node_id(),
        seq: 5,
        entry_hash: [0x55; 32],
        at_ms: 1_000,
        sig: Signature::from_bytes([0xEE; 64]),
    };
    let mut env = harness_core::AuditSyncEnvelope {
        source: a.node_id(),
        assembled_at: 1_000,
        heads: vec![head],
        sig: Signature::from_bytes([0u8; 64]),
    };
    env.sign(a.identity.as_ref()).expect("sign");

    let svc = crate::audit_sync::AuditSyncService::new(
        b.store.clone(),
        b.identity.clone(),
        b.trust.clone(),
    );
    let before = b.store.peer_head_pins(a.node_id()).expect("pins").len();
    let outcomes = svc.ingest(a.node_id(), &env, 2_000);
    assert!(outcomes.is_empty(), "bad inner signature rejected");
    assert_eq!(
        b.store.peer_head_pins(a.node_id()).expect("pins").len(),
        before,
        "nothing pinned from a forged head"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn m06_wire_failures_drive_the_backoff() {
    // Codex P2 on #66: `send_to` reports only that the message reached
    // the local queue — the wire send is asynchronous — so a backoff
    // driven from the push loop never fires, and a pre-5.13c peer gets
    // its unknown channel opened and reset on every tick forever.
    // Failures are reported from `sender_task`, where they surface.
    let (a, b) = boot_pair(None).await;
    wait_for_mesh(&a, &b).await;

    let svc = crate::audit_sync::AuditSyncService::new(
        a.store.clone(),
        a.identity.clone(),
        a.trust.clone(),
    );
    assert!(!svc.is_backing_off(b.node_id()), "starts clear");

    for _ in 0..3 {
        svc.note_send_failed(b.node_id());
    }
    assert!(
        svc.is_backing_off(b.node_id()),
        "three wire failures back the peer off"
    );

    // And a peer that starts working again clears its streak, so a
    // transient outage cannot strand it.
    let fresh = crate::audit_sync::AuditSyncService::new(
        a.store.clone(),
        a.identity.clone(),
        a.trust.clone(),
    );
    fresh.note_send_failed(b.node_id());
    fresh.note_send_failed(b.node_id());
    fresh.note_send_ok(b.node_id());
    fresh.note_send_failed(b.node_id());
    assert!(
        !fresh.is_backing_off(b.node_id()),
        "a success reset the streak, so the next failure is not the third"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn m07_housekeeping_actually_thins_the_pin_table() {
    // Codex P1 on #66: `thin_peer_head_pins` had no production caller
    // — the same defect `audit_prune` shipped with in 5.13a, where the
    // ADR described live retention that never ran. This drives the
    // housekeeping entry point, not the store method directly.
    let store = harness_store::Store::open_memory().expect("store");
    let subject = harness_core::NodeId::from_bytes([7u8; 16]);
    let reporter = harness_core::NodeId::from_bytes([8u8; 16]);

    for i in 1..=400u64 {
        let head = harness_core::AuditHead {
            node_id: subject,
            seq: i,
            entry_hash: [u8::try_from(i % 251).unwrap_or(0); 32],
            at_ms: i,
            sig: Signature::from_bytes([1u8; 64]),
        };
        store
            .pin_peer_head(&head, reporter, i * 60_000)
            .expect("pin");
    }
    // One piece of evidence deep in the tail.
    store
        .set_pin_status(subject, 3, PinStatus::Contradicted)
        .expect("status");
    assert_eq!(store.peer_head_pins(subject).expect("pins").len(), 400);

    crate::executor::thin_peer_head_pins(&store);

    let after = store.peer_head_pins(subject).expect("pins");
    assert!(
        after.len() < 400,
        "housekeeping thinned, kept {}",
        after.len()
    );
    assert!(
        after.iter().any(|p| p.seq == 3),
        "the contradicted pin survived — it IS the evidence"
    );
    assert!(
        after.iter().any(|p| p.seq == 1),
        "the oldest pin still anchors the range"
    );
}
