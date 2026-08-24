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

    // Delivered the way a real one would be: through the ingest path
    // b's `harness.audit` recv arm calls, with signature verification
    // and trust-store key lookup in the loop. An earlier version
    // called `pin_peer_head` directly, which made this a duplicate of
    // store test p07 rather than a test of the wire path.
    let mut env = harness_core::AuditSyncEnvelope {
        source: a.node_id(),
        assembled_at: 10_000_000,
        heads: vec![forged],
        sig: Signature::from_bytes([0u8; 64]),
    };
    env.sign(a.identity.as_ref()).expect("sign envelope");
    let svc = crate::audit_sync::AuditSyncService::new(
        b.store.clone(),
        b.identity.clone(),
        b.trust.clone(),
    );
    let outcomes = svc.ingest(a.node_id(), &env, 10_000_000);
    assert_eq!(outcomes.len(), 1, "the head verified and was processed");
    assert!(
        matches!(outcomes[0].1, harness_store::PinOutcome::Fork { .. }),
        "a second signed head at a pinned position is a fork, got {:?}",
        outcomes[0].1
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

/// Three trusted identities, no QUIC: enough to express the attacks
/// `boot_pair` structurally cannot (diff review BLOCKER-2 / MAJOR-8).
/// B is a trusted peer of ours; C is a trusted third node.
fn trio() -> (
    harness_store::Store,
    std::sync::Arc<harness_core::Identity>,
    harness_mesh::trust::TrustStore,
    harness_core::Identity,
    harness_core::Identity,
    tempfile::TempDir,
) {
    let tmp = tempfile::tempdir().expect("tmp");
    let me = std::sync::Arc::new(harness_mesh::identity::init_or_load(tmp.path()).expect("id"));
    let trust = harness_mesh::trust::TrustStore::open(tmp.path(), me.node_id()).expect("trust");
    let b = harness_core::Identity::generate();
    let c = harness_core::Identity::generate();
    for (id, name) in [(&b, "node-b"), (&c, "node-c")] {
        trust
            .add(harness_mesh::trust::Peer {
                node_id: id.node_id(),
                pubkey: *id.public_key(),
                hostname: name.into(),
                tier: harness_mesh::trust::TrustTier::Default,
                added_at: 0,
                added_via: harness_mesh::trust::AddedVia::Static,
            })
            .expect("trust add");
    }
    let store = harness_store::Store::open_memory().expect("store");
    (store, me, trust, b, c, tmp)
}

fn envelope(
    from: &harness_core::Identity,
    heads: Vec<harness_core::AuditHead>,
) -> harness_core::AuditSyncEnvelope {
    let mut env = harness_core::AuditSyncEnvelope {
        source: from.node_id(),
        assembled_at: 1_000,
        heads,
        sig: Signature::from_bytes([0u8; 64]),
    };
    env.sign(from).expect("sign envelope");
    env
}

#[test]
fn m08_a_trusted_relayer_cannot_forge_a_head_for_a_third_node() {
    // The invariant ADR Decision 11 spends the most words on, and the
    // one no earlier test expressed: B is trusted, C is trusted, and B
    // sends a head CLAIMING to be C's but signed with B's own key. If
    // the key came from the relayer instead of our trust store, B
    // would be able to forge C's entire history.
    let (store, me, trust, b, c, _tmp) = trio();
    let svc = crate::audit_sync::AuditSyncService::new(store.clone(), me, trust);

    let mut forged = harness_core::AuditHead {
        node_id: c.node_id(),
        seq: 42,
        entry_hash: [0xAB; 32],
        at_ms: 1_000,
        sig: Signature::from_bytes([0u8; 64]),
    };
    // Signed by B, claiming to be C.
    forged.sign(&b).expect("sign");

    let outcomes = svc.ingest(b.node_id(), &envelope(&b, vec![forged]), 2_000);
    assert!(
        outcomes.is_empty(),
        "a head signed by the relayer, not the subject, must be dropped"
    );
    assert!(
        store.peer_head_pins(c.node_id()).expect("pins").is_empty(),
        "nothing pinned for C from B's forgery"
    );
}

#[test]
fn m09_a_genuine_relayed_head_is_accepted_from_a_third_party() {
    // The other half: relaying is the point. C's own signature makes
    // its head verifiable no matter who carries it, which is what
    // lets a pin outlive C going offline.
    let (store, me, trust, b, c, _tmp) = trio();
    let svc = crate::audit_sync::AuditSyncService::new(store.clone(), me, trust);

    let mut genuine = harness_core::AuditHead {
        node_id: c.node_id(),
        seq: 42,
        entry_hash: [0xCD; 32],
        at_ms: 1_000,
        sig: Signature::from_bytes([0u8; 64]),
    };
    genuine.sign(&c).expect("sign");

    // Carried by B, signed by C.
    let outcomes = svc.ingest(b.node_id(), &envelope(&b, vec![genuine]), 2_000);
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].0, c.node_id());
    let pins = store.peer_head_pins(c.node_id()).expect("pins");
    assert_eq!(pins.len(), 1);
    assert_eq!(pins[0].entry_hash, [0xCD; 32]);
}

#[test]
fn m10_one_huge_seq_cannot_immunize_a_node_from_further_pinning() {
    // diff review BLOCKER-1: a validly-signed head at u64::MAX used to
    // be pinned and then compare above every genuine head forever, so
    // the node was never pinned again and could rewrite freely. The
    // classification may say "stale"; the storage must not.
    let (store, me, trust, _b, c, _tmp) = trio();
    let svc = crate::audit_sync::AuditSyncService::new(store.clone(), me, trust);

    let sign_head = |seq: u64, hash: u8| {
        let mut h = harness_core::AuditHead {
            node_id: c.node_id(),
            seq,
            entry_hash: [hash; 32],
            at_ms: seq,
            sig: Signature::from_bytes([0u8; 64]),
        };
        h.sign(&c).expect("sign");
        h
    };

    svc.ingest(c.node_id(), &envelope(&c, vec![sign_head(10, 0x10)]), 1_000);
    // The poison: signed, enormous.
    svc.ingest(
        c.node_id(),
        &envelope(&c, vec![sign_head(u64::MAX, 0xFF)]),
        2_000,
    );
    // Genuine growth afterwards.
    for seq in 11..=15u64 {
        svc.ingest(
            c.node_id(),
            &envelope(&c, vec![sign_head(seq, u8::try_from(seq).unwrap_or(0))]),
            3_000 + seq,
        );
    }

    let pins = store.peer_head_pins(c.node_id()).expect("pins");
    let seqs: Vec<u64> = pins.iter().map(|p| p.seq).collect();
    for seq in 11..=15u64 {
        assert!(
            seqs.contains(&seq),
            "genuine head {seq} still pinned after the poison, have {seqs:?}"
        );
    }
    assert!(
        !seqs.contains(&u64::MAX),
        "an unrepresentable seq is rejected, not clamped onto a real row"
    );

    // And a contradiction at one of those positions still forks.
    let mut contra = sign_head(12, 0xEE);
    contra.sign(&c).expect("sign");
    let outcomes = svc.ingest(c.node_id(), &envelope(&c, vec![contra]), 9_000);
    assert!(
        matches!(outcomes[0].1, harness_store::PinOutcome::Fork { .. }),
        "fork detection survives the poison, got {:?}",
        outcomes[0].1
    );
}
