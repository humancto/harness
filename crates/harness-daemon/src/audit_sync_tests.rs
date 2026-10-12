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

use crate::fanout_tests::{boot_pair, now_ms, wait_for_mesh, TestDaemon};

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
    // The poison: signed, enormous, but REPRESENTABLE — so it reaches
    // the insert path rather than being filtered by `RejectedSeq`.
    // An earlier version of this test used u64::MAX, which the seq
    // rejection caught first, so it passed even with the always-insert
    // fix reverted (re-review (c)).
    svc.ingest(
        c.node_id(),
        &envelope(&c, vec![sign_head(1_000_000_000, 0xFF)]),
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
        seqs.contains(&1_000_000_000),
        "the poison itself is stored — classification, not discard"
    );
    // And an UNrepresentable seq is rejected outright rather than
    // clamped onto a real row.
    let outcomes = svc.ingest(
        c.node_id(),
        &envelope(&c, vec![sign_head(u64::MAX, 0xAA)]),
        8_000,
    );
    assert_eq!(outcomes[0].1, harness_store::PinOutcome::RejectedSeq);
    assert!(
        !store
            .peer_head_pins(c.node_id())
            .expect("pins")
            .iter()
            .any(|p| p.seq == u64::MAX || p.seq == i64::MAX as u64),
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

#[test]
fn m11_relay_ranks_on_our_clock_and_selects_our_newest_observation() {
    // Two things the re-review found untested, both exploitable by a
    // field the subject chooses:
    //   - cross-node relay order used the head's own `at_ms`, so a
    //     node stamping u64::MAX held a relay slot forever;
    //   - within-node selection used `seq DESC`, so one head signed at
    //     i64::MAX would be relayed for that node permanently.
    let (store, me, trust, b, c, _tmp) = trio();
    let svc = crate::audit_sync::AuditSyncService::new(store.clone(), me.clone(), trust);

    let head_of = |id: &harness_core::Identity, seq: u64, at_ms: u64, hash: u8| {
        let mut h = harness_core::AuditHead {
            node_id: id.node_id(),
            seq,
            entry_hash: [hash; 32],
            at_ms,
            sig: Signature::from_bytes([0u8; 64]),
        };
        h.sign(id).expect("sign");
        h
    };

    // B stamps an enormous `at_ms` on its head — the field its own
    // signer chooses — but we observed it EARLY. The two orderings
    // must disagree, or the test proves nothing.
    store
        .pin_peer_head(&head_of(&b, 5, u64::MAX, 0xB1), b.node_id(), 1_000)
        .expect("pin");
    // C signs an enormous `seq`, observed early.
    store
        .pin_peer_head(&head_of(&c, i64::MAX as u64, 500, 0xC1), c.node_id(), 1_000)
        .expect("pin");
    // C's later, genuine head — smaller seq, but observed most recently.
    store
        .pin_peer_head(&head_of(&c, 7, 2_000, 0xC2), c.node_id(), 9_500)
        .expect("pin");

    // Within-node: our newest OBSERVATION wins, not the biggest seq.
    let (selected, _) = store
        .newest_pin_as_head(c.node_id())
        .expect("read")
        .expect("some");
    assert_eq!(
        selected.entry_hash, [0xC2; 32],
        "the i64::MAX head must not be relayed for C forever"
    );

    // Cross-node: ranked by when WE saw them, so C's u64::MAX `at_ms`
    // buys nothing.
    let heads = svc.relayable_heads_for_test();
    let order: Vec<[u8; 32]> = heads.iter().map(|h| h.entry_hash).collect();
    assert_eq!(
        order.first().copied(),
        Some([0xC2; 32]),
        "most recently OBSERVED first — B's u64::MAX at_ms buys nothing, got {order:?}"
    );
    assert!(order.contains(&[0xB1; 32]), "B's head is still relayed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn m12_a_pin_is_corroborated_from_entries_over_the_wire() {
    // The 5.13c-2 property, end to end: B pins A's head, asks A for
    // the entries behind it, walks them, and upgrades the pin from
    // `unchecked` to `corroborated`. Nothing here reaches into a
    // store directly — the request and the batch both cross QUIC.
    let (a, b) = boot_pair(None).await;
    wait_for_mesh(&a, &b).await;

    append_entries(&a, 12);
    let target = wait_for_pin(&b, a.node_id(), "b to pin a's head").await;

    let pins = b.store.peer_head_pins(a.node_id()).expect("pins");
    assert_eq!(
        pins.last().expect("pin").status,
        PinStatus::Unchecked,
        "a fresh pin claims nothing until the entries are walked"
    );

    // b asks a for everything up to the pinned position.
    let svc = b.audit_sync.clone();
    assert!(
        svc.request_range(a.node_id(), a.node_id(), 1, target, now_ms()),
        "request sent"
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if b.store
            .audit_run_corroborates(a.node_id(), target)
            .expect("check")
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for corroboration"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let settled = b
        .store
        .peer_head_pins(a.node_id())
        .expect("pins")
        .into_iter()
        .find(|p| p.seq == target)
        .expect("pin");
    assert_eq!(settled.status, PinStatus::Corroborated);

    // And the entries really landed, stamped with OUR receive clock.
    let rows = b
        .store
        .audit_entries_for_range(a.node_id(), 1, target)
        .expect("read")
        .0;
    assert_eq!(u64::try_from(rows.len()).unwrap_or(0), target);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn m13_an_unmatched_response_is_dropped() {
    // No request/response primitive exists in this mesh, so a reply is
    // matched by an explicit req_id. An unsolicited batch — or one
    // answering a question we never asked — must not be ingested.
    let (a, b) = boot_pair(None).await;
    wait_for_mesh(&a, &b).await;
    append_entries(&a, 5);

    let (entries, _) = a
        .store
        .audit_entries_for_range(a.node_id(), 1, 5)
        .expect("serve");
    let mut env = harness_core::AuditEntriesEnvelope {
        source: a.node_id(),
        assembled_at: now_ms(),
        range_req: None,
        range_resp: Some(harness_core::AuditRangeResp {
            req_id: [0xAB; 16],
            node_id: a.node_id(),
            entries,
            truncated: false,
        }),
        sig: Signature::from_bytes([0u8; 64]),
    };
    env.sign(a.identity.as_ref()).expect("sign");

    let svc = crate::audit_sync::AuditSyncService::new(
        b.store.clone(),
        b.identity.clone(),
        b.trust.clone(),
    );
    svc.ingest_entries(a.node_id(), &env, now_ms());

    let (rows, _) = b
        .store
        .audit_entries_for_range(a.node_id(), 1, 5)
        .expect("read");
    assert!(
        rows.is_empty(),
        "an unsolicited batch must not be ingested, got {} rows",
        rows.len()
    );
}

#[test]
fn m14_a_response_must_start_where_we_asked() {
    // Codex P1 on #67. A malicious subject answers a request for
    // 1..target with ONLY its valid entry at `target`. Without this
    // check a fresh run accepts that starting point, the final hash
    // equals the pin, and the pin is marked corroborated without one
    // preceding entry being walked — the whole guarantee, gone.
    //
    // Deliberately not a two-daemon test: a live peer answers the
    // request honestly before a crafted reply can land, which would
    // corroborate the pin for the RIGHT reason and hide the bug.
    let (store, me, trust, _b, c, _tmp) = trio();
    let svc = crate::audit_sync::AuditSyncService::new(store.clone(), me, trust);

    // Build a real chain for C and pin its head.
    let subject_store = harness_store::Store::open_memory().expect("subject store");
    let sink = StoreAuditSink::new(subject_store.clone(), c.node_id());
    for i in 0..10 {
        sink.record(
            AuditRecord::new(AuditAction::TaskDispatched, AuditActor::System)
                .with_subject(format!("task-{i}")),
        );
    }
    let (all, _) = subject_store
        .audit_entries_for_range(c.node_id(), 1, 10)
        .expect("serve");
    let head = all.last().expect("entries");
    let mut pin_head = harness_core::AuditHead {
        node_id: c.node_id(),
        seq: head.seq,
        entry_hash: head.entry_hash,
        at_ms: now_ms(),
        sig: Signature::from_bytes([0u8; 64]),
    };
    pin_head.sign(&c).expect("sign");
    store
        .pin_peer_head(&pin_head, c.node_id(), now_ms())
        .expect("pin");

    // Also pin the PREDECESSOR, so the store's own anchor check would
    // accept a batch starting at 10. That leaves the daemon's
    // start-position guard as the only thing standing between a
    // one-row answer and a corroborated pin — without it this test
    // would pass for the wrong reason.
    let prev = &all[8];
    let mut pin_prev = harness_core::AuditHead {
        node_id: c.node_id(),
        seq: prev.seq,
        entry_hash: prev.entry_hash,
        at_ms: now_ms(),
        sig: Signature::from_bytes([0u8; 64]),
    };
    pin_prev.sign(&c).expect("sign");
    store
        .pin_peer_head(&pin_prev, c.node_id(), now_ms())
        .expect("pin");

    // We asked for 1..=10; the answer contains only entry 10.
    let req_id = svc.track_inflight_for_test(c.node_id(), c.node_id(), 1, head.seq, now_ms());
    let mut env = harness_core::AuditEntriesEnvelope {
        source: c.node_id(),
        assembled_at: now_ms(),
        range_req: None,
        range_resp: Some(harness_core::AuditRangeResp {
            req_id,
            node_id: c.node_id(),
            entries: vec![head.clone()],
            truncated: false,
        }),
        sig: Signature::from_bytes([0u8; 64]),
    };
    env.sign(&c).expect("sign");
    svc.ingest_entries(c.node_id(), &env, now_ms());

    assert!(
        !store
            .audit_run_corroborates(c.node_id(), head.seq)
            .expect("check"),
        "a truncated answer must not corroborate the pin"
    );
    let (rows, _) = store
        .audit_entries_for_range(c.node_id(), 1, head.seq)
        .expect("read");
    assert!(rows.is_empty(), "and nothing was ingested");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn m15_the_follow_up_walk_is_bounded() {
    // A server that always sets `truncated` and returns one row per
    // reply would otherwise keep us asking forever. `may_serve` limits
    // what WE serve; nothing limited what we chase.
    //
    // MUST use `boot_pair`, not `trio()` (diff review BLOCKER-1 on
    // #67): `trio` wires no PeerNet, so `request_range_round` returns
    // false at the net lookup regardless of `round` and the test was
    // insensitive to the guard it is named for — it passed with the
    // whole bound deleted. With a live net the two cases differ.
    let (a, b) = boot_pair(None).await;
    wait_for_mesh(&a, &b).await;
    let svc = b.audit_sync.clone();

    // The SUBJECT is a third node neither peer holds rows for, so `a`
    // can never answer and the in-flight slot cannot be retired
    // underneath the assertions below. Asking about `a` itself would
    // leave a window in which a reply removes the slot and the count
    // flakes (round 4 MINOR-8); nothing about the round bound needs a
    // subject the peer can serve.
    let absent_subject = harness_core::NodeId::from_bytes([0x5A; 16]);

    // At the budget the request really goes out and takes a slot.
    assert!(
        svc.request_range_round_for_test(
            a.node_id(),
            absent_subject,
            1,
            100,
            crate::audit_sync::MAX_FOLLOWUP_ROUNDS,
            now_ms(),
        ),
        "a walk at the budget is still attempted"
    );
    let at_budget = svc.inflight_len_for_test();
    assert_eq!(at_budget, 1, "and holds an in-flight slot");

    // Past the budget it is abandoned before anything is sent.
    assert!(
        !svc.request_range_round_for_test(
            a.node_id(),
            absent_subject,
            1,
            100,
            crate::audit_sync::MAX_FOLLOWUP_ROUNDS + 1,
            now_ms(),
        ),
        "past the budget the walk is abandoned"
    );
    assert_eq!(
        svc.inflight_len_for_test(),
        at_budget,
        "and takes no additional slot"
    );
}

/// A request-bearing entries envelope from `from`, signed by it.
fn range_req_envelope(
    from: &harness_core::Identity,
    subject: harness_core::NodeId,
    req_id: [u8; 16],
) -> harness_core::AuditEntriesEnvelope {
    let mut env = harness_core::AuditEntriesEnvelope {
        source: from.node_id(),
        assembled_at: 1_000,
        range_req: Some(harness_core::AuditRangeReq {
            req_id,
            node_id: subject,
            from_seq: 1,
            to_seq: 5,
        }),
        range_resp: None,
        sig: Signature::from_bytes([0u8; 64]),
    };
    env.sign(from).expect("sign envelope");
    env
}

#[test]
fn m16_serving_is_rate_limited_per_peer_and_the_window_resets() {
    // ADR Decision 18's rate limit. Round 4 of the review deleted the
    // `may_serve` call and the ENTIRE harness-daemon suite stayed
    // green: the limit was a security property asserted in the ADR
    // with no test at all.
    //
    // The observable is the budget counter, not a sent frame. Both
    // guards return before `serve_range` looks up the net, so a test
    // watching the wire cannot distinguish "refused" from "allowed but
    // no connection" — the same insensitivity that let the follow-up
    // bound ship untested (BLOCKER-1).
    let (store, me, trust, b, _c, _tmp) = trio();
    let svc = crate::audit_sync::AuditSyncService::new(store, me, trust);
    let now = 10_000;

    for i in 0..crate::audit_sync::SERVE_BATCHES_PER_MINUTE {
        let env = range_req_envelope(&b, b.node_id(), [u8::try_from(i % 256).unwrap_or(0); 16]);
        svc.ingest_entries(b.node_id(), &env, now);
    }
    assert_eq!(
        svc.served_in_window_for_test(b.node_id()),
        crate::audit_sync::SERVE_BATCHES_PER_MINUTE,
        "the whole per-minute allowance is usable"
    );

    // One past the allowance is refused, and refusal does not grow the
    // counter — so a flooder cannot push the window forward either.
    let env = range_req_envelope(&b, b.node_id(), [0xFE; 16]);
    svc.ingest_entries(b.node_id(), &env, now);
    assert_eq!(
        svc.served_in_window_for_test(b.node_id()),
        crate::audit_sync::SERVE_BATCHES_PER_MINUTE,
        "the request past the allowance is refused"
    );

    // A fresh window on OUR clock lets the peer through again: this is
    // a rate limit, not a permanent ban.
    let env = range_req_envelope(&b, b.node_id(), [0xFF; 16]);
    svc.ingest_entries(b.node_id(), &env, now + crate::audit_sync::SERVE_WINDOW_MS);
    assert_eq!(
        svc.served_in_window_for_test(b.node_id()),
        1,
        "the window resets and the counter restarts"
    );
}

#[test]
fn m17_a_guest_peer_is_not_served_and_does_not_spend_the_budget() {
    // ADR Decision 18's tier gate, which also survived deletion with
    // all 126 daemon tests green (round 4 MAJOR-3).
    //
    // The gate is deliberately checked BEFORE the budget, so a refused
    // Guest cannot burn the allowance of a peer we would have served.
    // Asserting the counter is still 0 tests the ORDER as well as the
    // gate — a gate moved after the budget would leave it at 1.
    let (store, me, trust, b, _c, _tmp) = trio();
    trust
        .add(harness_mesh::trust::Peer {
            node_id: b.node_id(),
            pubkey: *b.public_key(),
            hostname: "guest-node".into(),
            tier: harness_mesh::trust::TrustTier::Guest,
            added_at: 0,
            added_via: harness_mesh::trust::AddedVia::Static,
        })
        .expect("re-add as guest");
    let svc = crate::audit_sync::AuditSyncService::new(store, me, trust);

    let env = range_req_envelope(&b, b.node_id(), [0x11; 16]);
    svc.ingest_entries(b.node_id(), &env, 10_000);

    assert_eq!(
        svc.served_in_window_for_test(b.node_id()),
        0,
        "a Guest is refused before the budget is consulted"
    );
}

#[test]
fn m18_a_subject_cannot_own_the_verify_target_through_a_tie_break() {
    // Round 5 BLOCKER-1: the fix for round 4's `MAX(seq)` defect moved
    // the defect into the tie-break instead of removing it.
    //
    // Every head in one envelope is pinned with the SAME `now_ms`, so
    // a subject that pairs its honest head with an inflated one makes
    // `observed_at_ms` tie, and `ORDER BY ... seq DESC` then resolved
    // the tie with a field the subject signed. Re-sending the pair
    // each tick takes the `Refreshed` branch and re-stamps BOTH rows
    // with the same new instant, so the tie never breaks: the verify
    // endpoint aims past the honest chain forever, `reached_pin` never
    // fires, and no pin for that node is ever corroborated.
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

    // Both heads validly signed by the subject, both in ONE envelope,
    // the inflated one representable so `RejectedSeq` does not catch
    // it first (the trap m10 had to be rewritten to avoid).
    let absurd = u64::try_from(i64::MAX).expect("i64::MAX fits u64");
    let paired = envelope(&c, vec![sign_head(9, 0x09), sign_head(absurd, 0xFF)]);

    // Re-sent every tick, which is what made the capture permanent.
    for tick in 1..=4u64 {
        svc.ingest(c.node_id(), &paired, tick * 1_000);
        let target = store
            .newest_pin_by_observation(c.node_id())
            .expect("pin")
            .expect("some pin");
        assert_eq!(
            target.seq, 9,
            "tick {tick}: the verify target must be the honest head, not the subject's biggest number"
        );
    }

    // Relay is NOT asserted here. With a single relay slot the two
    // capture directions are mutually exclusive — `seq DESC` hands it
    // to an inflated self-claim, `seq ASC` to a captor's genuine low
    // head — so no single-slot rule satisfies both, which is why the
    // bounded relay SET is ROADMAP 5.13c-1b. `m25` asserts what the
    // one slot does guarantee today.

    // SECOND shape, which the per-envelope dedupe does NOT catch: two
    // SEPARATE envelopes inside one millisecond. They tie on
    // `observed_at_ms` just the same, so the tie-break itself has to
    // be un-inflatable — this is the half that keeps `seq ASC` honest
    // rather than relying on the dedupe alone.
    let (store2, me2, trust2, _b2, c2, _tmp2) = trio();
    let svc2 = crate::audit_sync::AuditSyncService::new(store2.clone(), me2, trust2);
    let sign2 = |seq: u64, hash: u8| {
        let mut h = harness_core::AuditHead {
            node_id: c2.node_id(),
            seq,
            entry_hash: [hash; 32],
            at_ms: seq,
            sig: Signature::from_bytes([0u8; 64]),
        };
        h.sign(&c2).expect("sign");
        h
    };
    for tick in 1..=3u64 {
        // Honest first, inflated second — so insert order favours the
        // attacker and only the VALUE comparison can save us.
        svc2.ingest(
            c2.node_id(),
            &envelope(&c2, vec![sign2(9, 0x09)]),
            tick * 1_000,
        );
        svc2.ingest(
            c2.node_id(),
            &envelope(&c2, vec![sign2(absurd, 0xFF)]),
            tick * 1_000,
        );
        assert_eq!(
            store2
                .newest_pin_by_observation(c2.node_id())
                .expect("pin")
                .expect("some pin")
                .seq,
            9,
            "tick {tick}: two envelopes in one millisecond must not hand the subject the slot"
        );
    }
}

#[test]
fn m19_an_envelope_claiming_another_source_is_dropped() {
    // Round 5 MAJOR-3: this cross-check sat in `channel_recv_loop`,
    // where nothing could cheaply test it — neutralizing it to
    // `if false` left all 128 daemon tests green. It now lives in the
    // service, which is reachable from here.
    //
    // Both envelope types, because both carry a `source` the sending
    // peer could lie about.
    let (store, me, trust, b, c, _tmp) = trio();
    let svc = crate::audit_sync::AuditSyncService::new(store.clone(), me, trust);

    let mut head = harness_core::AuditHead {
        node_id: c.node_id(),
        seq: 5,
        entry_hash: [0x55; 32],
        at_ms: 1_000,
        sig: Signature::from_bytes([0u8; 64]),
    };
    head.sign(&c).expect("sign head");

    // C signs an envelope naming itself as source, and B hands it to
    // us as if it were B's own channel traffic. The head inside is
    // genuine, so only the source check can refuse this.
    let relayed_as_own = envelope(&c, vec![head]);
    let out = svc.ingest(b.node_id(), &relayed_as_own, 2_000);
    assert!(
        out.is_empty(),
        "an envelope whose source is not the sending peer is dropped, got {out:?}"
    );
    assert!(
        store.peer_head_pins(c.node_id()).expect("pins").is_empty(),
        "and nothing was pinned from it"
    );

    // Same for the entries channel: a request that claims to come from
    // C while arriving from B must not be served.
    let entries = range_req_envelope(&c, c.node_id(), [0x77; 16]);
    svc.ingest_entries(b.node_id(), &entries, 3_000);
    assert_eq!(
        svc.served_in_window_for_test(b.node_id()),
        0,
        "a mismatched source is refused before the serve budget"
    );
    assert_eq!(
        svc.served_in_window_for_test(c.node_id()),
        0,
        "and is not charged to the claimed source either"
    );
}

#[test]
fn m21_a_replayed_old_head_cannot_take_the_verify_slot() {
    // Round 6 BLOCKER-1(a). The envelope is deliberately NOT
    // replay-protected, and `pin_peer_head`'s `Refreshed` branch
    // re-stamps `observed_at_ms`. So ordering on `observed_at_ms`
    // meant our clock stamped the row but the SUBJECT chose which row
    // got stamped: re-push a genuine old head and it becomes "newest".
    //
    // `first_seen_ms` is written once on INSERT and never updated,
    // which is why the ordering keys on it.
    let (store, me, trust, b, c, _tmp) = trio();
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

    // Both positions are known to us, oldest first — the ordinary
    // case after a peer has been up for a while.
    let old_head = envelope(&c, vec![sign_head(2, 0x02)]);
    svc.ingest(c.node_id(), &old_head, 1_000);
    svc.ingest(b.node_id(), &envelope(&b, vec![sign_head(50, 0x50)]), 2_000);
    assert_eq!(
        store
            .newest_pin_by_observation(c.node_id())
            .expect("pin")
            .expect("some pin")
            .seq,
        50,
        "the newest position we learned about is the target"
    );

    // Now the replay: C re-pushes the SAME old head it already got us
    // to pin. `Refreshed` re-stamps `observed_at_ms` on that row, so
    // an `observed_at_ms` ordering handed the slot straight back to
    // seq 2 — our clock stamping it, the subject choosing what got
    // stamped. `first_seen_ms` is written once and never updated.
    for tick in 3..=6u64 {
        svc.ingest(c.node_id(), &old_head, tick * 1_000);
        assert_eq!(
            store
                .newest_pin_by_observation(c.node_id())
                .expect("pin")
                .expect("some pin")
                .seq,
            50,
            "tick {tick}: re-pushing a head we already hold must not re-take the slot"
        );
    }

    // Both rows exist — the fix is in the ORDERING, not in discarding
    // evidence (round 5 dropped one of them, which was worse).
    let seqs: Vec<u64> = store
        .peer_head_pins(c.node_id())
        .expect("pins")
        .into_iter()
        .map(|p| p.seq)
        .collect();
    assert!(seqs.contains(&2) && seqs.contains(&50), "have {seqs:?}");
}

#[test]
fn m22_a_second_head_for_itself_cannot_delete_a_subjects_honest_head() {
    // Round 6 BLOCKER-1(b): round 5's "keep the LOWEST seq per
    // subject" dedupe DROPPED the honest head when a subject attached
    // a lower bogus one — never pinned at all, so nothing downstream
    // could see it, and every honest peer relayed the bogus position
    // for it. Pin density is the security parameter of this design.
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

    let paired = envelope(&c, vec![sign_head(9, 0x09), sign_head(1, 0xBB)]);
    svc.ingest(c.node_id(), &paired, 1_000);

    let seqs: Vec<u64> = store
        .peer_head_pins(c.node_id())
        .expect("pins")
        .into_iter()
        .map(|p| p.seq)
        .collect();
    assert!(
        seqs.contains(&9),
        "the honest head must be PINNED, not discarded; have {seqs:?}"
    );
    assert!(
        seqs.contains(&1),
        "and the low one is stored too; have {seqs:?}"
    );

    // An exact repeat is still collapsed — that is pure waste.
    let repeated = envelope(&c, vec![sign_head(4, 0x04), sign_head(4, 0x04)]);
    let out = svc.ingest(c.node_id(), &repeated, 2_000);
    assert_eq!(out.len(), 1, "an exact duplicate is collapsed, got {out:?}");
}

/// The 5.13c-1 head-envelope shape, frozen. Do not add fields.
///
/// `deny_unknown_fields` so an added field is caught at DECODE, not
/// only at signature verification.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct FrozenAuditSyncEnvelopeV1 {
    source: harness_core::NodeId,
    assembled_at: u64,
    heads: Vec<harness_core::AuditHead>,
    sig: Signature,
}

impl harness_core::Signable for FrozenAuditSyncEnvelopeV1 {
    fn sig_field_mut(&mut self) -> &mut Signature {
        &mut self.sig
    }
    fn sig_field(&self) -> &Signature {
        &self.sig
    }
}

#[test]
fn m23_the_envelope_production_sends_is_still_verifiable_by_a_5_13c_1_node() {
    // Round 6 MAJOR-2. ADR-0041 Decision 24 says a new field on a
    // signed wire type is never additive across versions, and nothing
    // enforced it: `wire_format.rs` pinned the gossip envelope but no
    // audit type. Round 5 added snapshots plus a cross-version test,
    // and round 6 showed BOTH miss the real defect — they are
    // hand-written fixtures, so a field added to the struct and
    // populated in `push_round` while the fixtures keep their defaults
    // (what the compiler error nudges you to write) leaves every test
    // green.
    //
    // So this reads the envelope PRODUCTION assembles, via the same
    // `assemble_push_envelope` that `push_round` sends.
    let (store, me, trust, _b, _c, _tmp) = trio();
    let local = me.node_id();
    let sink = harness_store::audit::StoreAuditSink::new(store.clone(), local);
    sink.record(
        harness_core::AuditRecord::new(
            harness_core::AuditAction::TaskDispatched,
            harness_core::AuditActor::System,
        )
        .with_subject("fixture"),
    );
    let svc = crate::audit_sync::AuditSyncService::new(store, me.clone(), trust);

    let env = svc
        .assemble_push_envelope(now_ms())
        .expect("production assembles a head push");
    assert!(
        !env.heads.is_empty(),
        "an empty envelope would verify against anything"
    );

    let mut buf = Vec::new();
    ciborium::ser::into_writer(&env, &mut buf).expect("serialize");
    let old: FrozenAuditSyncEnvelopeV1 = ciborium::de::from_reader(buf.as_slice())
        .expect("a 5.13c-1 node must be able to DECODE what we send");
    harness_core::Signable::verify_signature(&old, me.public_key()).expect(
        "and must verify its signature — otherwise it resets the stream and head pushes die",
    );
}

#[test]
fn m24_a_head_flood_cannot_sweep_away_a_nodes_corroborated_pins() {
    // Round 7 BLOCKER-1, the half that belongs to THIS item.
    // `MAX_HEADS_PER_ENVELOPE` used to be 64, exactly
    // `PIN_KEEP_RECENT`, so ONE envelope filled a node's whole
    // recent-pin window — and the sweep used to then delete its honest
    // CORROBORATED pins, keeping only `MIN(rowid)` per node.
    // Unrecoverably: `audit_run_corroborates` reads the pin, so losing
    // the row loses the verdict, and only the subject can mint a
    // replacement.
    //
    // 5.13c-1b-i narrowed the cap to 24, so one envelope no longer
    // reaches 64 pins. That is a real improvement and exactly why this
    // test must not quietly become weaker than its name: it keeps the
    // ORIGINAL pressure by sending as many full envelopes as it takes
    // to overfill `PIN_KEEP_RECENT`, so the sweep is still asked the
    // question the round-7 defect got wrong.
    //
    // `corroborated` is the status THIS item introduces, so protecting
    // it from the sweep is this item's job. Bounding the flood itself
    // is ROADMAP 5.13c-1b-ii.
    let (store, me, trust, b, c, _tmp) = trio();
    let svc = crate::audit_sync::AuditSyncService::new(store.clone(), me, trust);

    let sign_c = |seq: u64, hash: u8| {
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

    // An OLDER unchecked pin first, and it is load-bearing. The sweep
    // exempts `MIN(rowid)` per node and `MIN(rowid)` per time bucket,
    // and BOTH exemptions are status-blind. Ingesting the corroborated
    // pin first made it rowid 1 for this node, so it was protected
    // twice over by rules that never look at `status` — deleting the
    // sweep's `status = 'unchecked'` filter outright, which is the
    // round-7 defect in full, left this test passing. Seeding seq 50
    // ahead of it takes both exemptions, so seq 100 survives only
    // because it is corroborated, which is the property named above.
    svc.ingest(c.node_id(), &envelope(&c, vec![sign_c(50, 0x32)]), 500);
    svc.ingest(c.node_id(), &envelope(&c, vec![sign_c(100, 0x64)]), 1_000);
    store
        .set_pin_status(c.node_id(), 100, PinStatus::Corroborated)
        .expect("corroborate");

    // B floods genuine, validly signed positions of C, in as many
    // full envelopes as it takes to overfill the recent window.
    let cap = harness_core::MAX_HEADS_PER_ENVELOPE;
    let needed = crate::executor::PIN_KEEP_RECENT + cap;
    let mut sent = 0usize;
    let mut tick = 2_000u64;
    while sent < needed {
        let flood: Vec<harness_core::AuditHead> = (0..cap)
            .map(|j| {
                let i = u64::try_from(sent + j).unwrap_or(0);
                sign_c(
                    1_000_000 + i + 1,
                    u8::try_from((sent + j) % 251).unwrap_or(0),
                )
            })
            .collect();
        let mut env = harness_core::AuditSyncEnvelope {
            source: b.node_id(),
            assembled_at: tick,
            heads: flood,
            sig: Signature::from_bytes([0u8; 64]),
        };
        env.sign(&b).expect("sign");
        svc.ingest(b.node_id(), &env, tick);
        sent += cap;
        tick += 1_000;
    }
    assert!(
        store.peer_head_pins(c.node_id()).expect("pins").len() > crate::executor::PIN_KEEP_RECENT,
        "the flood must actually overfill the recent window, or the sweep is never asked"
    );

    store
        .thin_peer_head_pins(crate::executor::PIN_KEEP_RECENT, 3_600_000)
        .expect("thin");
    let survivors: Vec<(u64, PinStatus)> = store
        .peer_head_pins(c.node_id())
        .expect("pins")
        .into_iter()
        .map(|p| (p.seq, p.status))
        .collect();
    assert!(
        survivors.contains(&(100, PinStatus::Corroborated)),
        "the corroborated pin must survive a full-envelope flood plus the sweep; have {survivors:?}"
    );
}

#[test]
fn m25_the_relay_slot_prefers_the_higher_claim_and_resists_a_replay() {
    // Round 7 BLOCKER-2 found a trusted peer could own the position
    // the mesh relays for a third party by forwarding a genuine,
    // validly signed OLD head — and `seq ASC` actively helped, because
    // it preferred the lower claim. `seq ASC` is right for a verify
    // TARGET (a low position is at least walkable) and wrong for
    // relay (a low position is just worse for everyone).
    //
    // Relaying a bounded SET is the real fix and is ROADMAP 5.13c-1b;
    // the first attempt at it selected the set by `first_seen_ms`,
    // which the captor also controls, so three unseen old heads took
    // all three slots and the honest head was EVICTED — worse than
    // one slot, shipped as its fix. What this item keeps is the
    // ordering: the higher claim, and `first_seen_ms` so a re-push
    // cannot re-take the slot.
    let (store, me, trust, b, a, _tmp) = trio();
    let svc = crate::audit_sync::AuditSyncService::new(store.clone(), me, trust);

    let sign_a = |seq: u64, hash: u8| {
        let mut h = harness_core::AuditHead {
            node_id: a.node_id(),
            seq,
            entry_hash: [hash; 32],
            at_ms: seq,
            sig: Signature::from_bytes([0u8; 64]),
        };
        h.sign(&a).expect("sign");
        h
    };

    // A's real head and a captor's lower one, same envelope and so the
    // same instant — the tie must not go to the low position.
    let paired = envelope(&b, vec![sign_a(9_000, 0x90), sign_a(1, 0x01)]);
    svc.ingest(b.node_id(), &paired, 1_000);
    let relayed: Vec<u64> = svc
        .relayable_heads_for_test()
        .into_iter()
        .filter(|h| h.node_id == a.node_id())
        .map(|h| h.seq)
        .collect();
    assert_eq!(
        relayed,
        vec![9_000],
        "the relay slot takes the higher claim, not the captor's low one"
    );
    // The documented residual of a SINGLE slot: the higher claim wins,
    // so a SUBJECT inflating its own position takes the slot from
    // itself. That is self-harm a recipient already handles (`m10`:
    // a huge seq does not immunize a node), and it is strictly
    // preferable to letting a THIRD party make the mesh learn an old
    // position for an honest node. Both directions need the bounded
    // set — ROADMAP 5.13c-1b.

    // And re-pushing a head we already hold must not re-take the slot:
    // `Refreshed` re-stamps `observed_at_ms` (what `main` ordered on)
    // but never `first_seen_ms`.
    let replay = envelope(&b, vec![sign_a(1, 0x01)]);
    for tick in 2..=4u64 {
        svc.ingest(b.node_id(), &replay, tick * 1_000);
        let relayed: Vec<u64> = svc
            .relayable_heads_for_test()
            .into_iter()
            .filter(|h| h.node_id == a.node_id())
            .map(|h| h.seq)
            .collect();
        assert_eq!(
            relayed,
            vec![9_000],
            "tick {tick}: a re-pushed old head must not re-take the relay slot"
        );
    }
}

#[test]
fn m26_the_ingest_subject_ceiling_matches_the_channel_frame_cap() {
    // Round 9 finding 4. Two earlier ceilings each left a band of
    // legacy rows that could be SERVED and framed but not ingested —
    // and ingest refusal is fatal, stranding the whole chain past the
    // offending row. The second of them (768 KiB) asserted in the ADR
    // that no such band existed, while the frame cap was 1 MiB.
    //
    // `harness-store` cannot depend on `harness-mesh`, so the two
    // constants are declared apart and pinned together here. If the
    // frame cap moves, this fails rather than silently reopening the
    // band.
    assert_eq!(
        harness_store::MAX_INGESTED_SUBJECT_BYTES,
        harness_mesh::transport::channels::frame_cap(
            harness_mesh::transport::channels::AUDIT_ENTRIES
        ),
        "a row that can be framed must be ingestable, or refusing it strands the chain"
    );
}

#[test]
fn m27_an_oversized_envelope_is_truncated_and_the_cap_covers_an_honest_sender() {
    // `MAX_HEADS_PER_ENVELOPE` is the ONLY bound on per-envelope
    // crypto: `ingest` verifies each head before anything downstream
    // can refuse it, so a sender that ignores the cap must be cut off
    // by the receiver's own `take()` and not by trusting the count.
    //
    // The cap must also be wide enough for an HONEST envelope
    // (`1 + RELAY_HEADS`), or the same `take()` discards the tail of a
    // legitimate relay set. A const assert in `audit_sync` pins that
    // direction; this pins the truncation itself.
    let (store, me, trust, b, c, _tmp) = trio();
    let svc = crate::audit_sync::AuditSyncService::new(store.clone(), me, trust);
    let cap = harness_core::MAX_HEADS_PER_ENVELOPE;
    assert!(
        cap > 16,
        "the cap must fit an honest sender's own head plus its relay set"
    );

    let over: Vec<harness_core::AuditHead> = (1..=u64::try_from(cap + 8).unwrap_or(0))
        .map(|i| {
            let mut h = harness_core::AuditHead {
                node_id: c.node_id(),
                seq: i,
                entry_hash: [u8::try_from(i % 251).unwrap_or(0); 32],
                at_ms: i,
                sig: Signature::from_bytes([0u8; 64]),
            };
            h.sign(&c).expect("sign");
            h
        })
        .collect();
    let outcomes = svc.ingest(b.node_id(), &envelope(&b, over), 2_000);
    assert_eq!(
        outcomes.len(),
        cap,
        "exactly the cap is classified; the tail is ignored, not trusted"
    );
    assert_eq!(
        store.peer_head_pins(c.node_id()).expect("pins").len(),
        cap,
        "and nothing past the cap reached the store"
    );

    // The truncation WARN is rated, and both halves of that rating
    // need covering: deleting the gate AND the counter's reset left
    // the entire workspace green. The reset is the sharper half — with
    // it gone the warn fires once per peer per PROCESS, so an upgraded
    // sender truncated on every push looks exactly like the quiet peer
    // Decision 31 says must not happen.
    assert_eq!(
        svc.oversize_logs_in_window_for_test(b.node_id()),
        crate::audit_sync::OVERSIZE_LOGS_PER_WINDOW,
        "the first oversized envelope spends the window's budget"
    );
    let over2: Vec<harness_core::AuditHead> = (1..=u64::try_from(cap + 8).unwrap_or(0))
        .map(|i| {
            let mut h = harness_core::AuditHead {
                node_id: c.node_id(),
                seq: 10_000 + i,
                entry_hash: [u8::try_from(i % 251).unwrap_or(0); 32],
                at_ms: i,
                sig: Signature::from_bytes([0u8; 64]),
            };
            h.sign(&c).expect("sign");
            h
        })
        .collect();
    svc.ingest(b.node_id(), &envelope(&b, over2.clone()), 2_500);
    assert_eq!(
        svc.oversize_logs_in_window_for_test(b.node_id()),
        crate::audit_sync::OVERSIZE_LOGS_PER_WINDOW,
        "a second oversized envelope in the same window adds no line"
    );
    // Count the LINE across the roll. A counter-only assertion cannot
    // discriminate: with the reset deleted the counter sits at its
    // limit forever, which reads the same as a correctly rolled one.
    let warns = count_audit_warns(|| {
        svc.ingest(
            b.node_id(),
            &envelope(&b, over2),
            2_500 + crate::audit_sync::LOG_WINDOW_MS,
        );
    });
    assert_eq!(
        warns, 1,
        "the window must ROLL, or the warn fires once per peer per process          and an upgraded sender truncated on every push looks quiet"
    );
}

#[test]
fn m28_a_same_envelope_fork_at_an_unheld_position_is_caught_at_ingest() {
    // `p10`/`p11` prove the STORE classifies a same-envelope fork at a
    // position we have never pinned. Nothing proved the second branch
    // still reaches the store: the only thing that gets two branches
    // at one position past `ingest` is the dedupe key being
    // `(node_id, seq, entry_hash)` rather than `(node_id, seq)`, and
    // narrowing that key left the whole workspace green. `m03` covers
    // only a CROSS-envelope fork at an already-held position.
    //
    // That is a live trap rather than a hypothetical: ADR-0041
    // Decision 28 tells 5.13c-1b-ii to dedupe candidates by
    // `(node_id, seq)` before applying the allowance. Done at the
    // wrong layer, that silently destroys the evidence — one ordinary
    // pin, no conflict row, no log line, and a subject double-signs
    // with impunity. This test is what makes that regression loud.
    let (store, me, trust, b, c, _tmp) = trio();
    let svc = crate::audit_sync::AuditSyncService::new(store.clone(), me, trust);

    let sign_c = |seq: u64, hash: u8| {
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

    // B relays two differently-signed branches of C at one position we
    // do not hold, in ONE envelope.
    let env = envelope(&b, vec![sign_c(7, 0xAA), sign_c(7, 0xBB)]);
    let outcomes = svc.ingest(b.node_id(), &env, 2_000);

    assert_eq!(
        outcomes.len(),
        2,
        "both branches must be classified, not collapsed, got {outcomes:?}"
    );
    assert!(
        matches!(outcomes[0].1, harness_store::PinOutcome::Pinned),
        "the first branch pins, got {:?}",
        outcomes[0].1
    );
    assert!(
        matches!(outcomes[1].1, harness_store::PinOutcome::Fork { .. }),
        "and the second is a FORK, got {:?}",
        outcomes[1].1
    );
    let conflicts = store.head_conflicts(10).expect("conflicts");
    assert_eq!(conflicts.len(), 1, "with a conflict row holding both sides");
    assert_eq!(conflicts[0].held_hash, [0xAA; 32]);
    assert_eq!(conflicts[0].other_hash, [0xBB; 32]);
    assert_eq!(
        conflicts[0].reported_by,
        b.node_id(),
        "provenance is the relayer, not the accused"
    );
}

/// Counts ERROR events on `harness.audit.sync`, per emitting thread.
///
/// Exists because asserting the limiter's COUNTER is not asserting the
/// LINE: a refactor that keeps the charge and emits unconditionally
/// restores the whole flood lever with the counter still saturating,
/// and the review that found it confirmed all 141 daemon tests stayed
/// green. `AUDIT CHAIN FORK` is the only ERROR this target emits, so
/// counting events at that level counts fork lines.
///
/// Installed as the GLOBAL default, not via `with_default`, and this
/// is the load-bearing part. Callsite interest is cached process-wide
/// and `rebuild_interest_cache` re-registers against the GLOBAL
/// dispatcher: with only a thread-local subscriber the callsite caches
/// as disabled and the layer sees nothing. That failed as `m29`
/// counting 0 in parallel while passing alone and under
/// `--test-threads=1`.
///
/// The count is a THREAD-LOCAL, so parallel tests that emit the same
/// line on their own threads cannot inflate it.
struct AuditErrorCounter;

thread_local! {
    static AUDIT_ERRORS_SEEN: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static AUDIT_WARNS_SEEN: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for AuditErrorCounter {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let m = event.metadata();
        if m.target() != "harness.audit.sync" {
            return;
        }
        match *m.level() {
            tracing::Level::ERROR => AUDIT_ERRORS_SEEN.with(|c| c.set(c.get() + 1)),
            tracing::Level::WARN => AUDIT_WARNS_SEEN.with(|c| c.set(c.get() + 1)),
            _ => {}
        }
    }
}

/// Run `f` and return how many `harness.audit.sync` ERROR lines it
/// emitted on this thread.
fn install_audit_counter() {
    use tracing_subscriber::layer::SubscriberExt;
    static INIT: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    INIT.get_or_init(|| {
        let subscriber = tracing_subscriber::registry().with(AuditErrorCounter);
        // Another test installing one first is not a failure here, it
        // just means these helpers cannot count.
        if tracing::subscriber::set_global_default(subscriber).is_ok() {
            // Any callsite that already fired is cached against the
            // previous (absent) subscriber; re-register against ours.
            tracing::callsite::rebuild_interest_cache();
        }
    });
}

fn count_audit_errors(f: impl FnOnce()) -> usize {
    install_audit_counter();
    AUDIT_ERRORS_SEEN.with(|c| c.set(0));
    f();
    AUDIT_ERRORS_SEEN.with(std::cell::Cell::get)
}

/// As [`count_audit_errors`], for WARN — the truncation line's level.
///
/// Counting the LINE and not the counter is load-bearing here: with
/// the window's reset deleted the counter sits at its limit forever,
/// which reads identically to a correctly rolled one, so a
/// counter-only assertion cannot tell a working roll from a warn that
/// fires once per process.
fn count_audit_warns(f: impl FnOnce()) -> usize {
    install_audit_counter();
    AUDIT_WARNS_SEEN.with(|c| c.set(0));
    f();
    AUDIT_WARNS_SEEN.with(std::cell::Cell::get)
}

#[test]
fn m29_fork_log_lines_are_bounded_per_peer_and_the_window_rolls() {
    // The conflict ROW is written for every fork regardless — the
    // evidence is never rated. What is rated is the ERROR line, which
    // a sender can otherwise mint at will: it signs branches at its
    // own position, and `harness.audit` has no message rate limit, so
    // our own loudest alarm becomes its log flood.
    //
    // The limiter's window is its OWN. 1b-i's withdrawn version rolled
    // the PIN window's start from the fork limiter while leaving that
    // window's counters, which was unreachable only because of call
    // order; this keeps one window with one roll point.
    let (store, me, trust, b, c, _tmp) = trio();
    let svc = crate::audit_sync::AuditSyncService::new(store.clone(), me, trust);

    let sign_c = |seq: u64, hash: u8| {
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

    // One pin, then a run of distinct branches at that same position:
    // every one after the first is a fork. Count the EMITTED LINES —
    // asserting only the counter let a mutation that keeps the charge
    // and logs unconditionally pass with the whole suite green.
    let lines = count_audit_errors(|| {
        for i in 0..14u8 {
            svc.ingest(b.node_id(), &envelope(&b, vec![sign_c(7, 0x40 + i)]), 1_000);
        }
    });
    assert_eq!(
        lines,
        crate::audit_sync::FORK_LOGS_PER_WINDOW as usize,
        "exactly the budgeted number of fork LINES may be emitted"
    );
    let capped = svc.fork_logs_in_window_for_test(b.node_id());
    assert_eq!(
        capped,
        crate::audit_sync::FORK_LOGS_PER_WINDOW,
        "fork log lines must saturate at the per-window limit"
    );
    // The rows are NOT rated: 14 branches at one position, first
    // pinned, 13 forks, all recorded.
    assert_eq!(
        store.head_conflicts(100).expect("conflicts").len(),
        13,
        "every fork is still RECORDED; only the log line is rated"
    );

    // A later window starts fresh.
    svc.ingest(
        b.node_id(),
        &envelope(&b, vec![sign_c(7, 0xFE)]),
        1_000 + crate::audit_sync::LOG_WINDOW_MS,
    );
    assert_eq!(
        svc.fork_logs_in_window_for_test(b.node_id()),
        1,
        "the window must roll, not stay saturated forever"
    );
}

#[test]
fn m30_the_fork_log_budget_is_per_peer_not_global() {
    // A noisy peer must not silence the alarm for an honest one.
    let (store, me, trust, b, c, _tmp) = trio();
    let svc = crate::audit_sync::AuditSyncService::new(store.clone(), me, trust);

    let sign_c = |seq: u64, hash: u8| {
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

    for i in 0..14u8 {
        svc.ingest(b.node_id(), &envelope(&b, vec![sign_c(7, 0x40 + i)]), 1_000);
    }
    assert_eq!(
        svc.fork_logs_in_window_for_test(b.node_id()),
        crate::audit_sync::FORK_LOGS_PER_WINDOW,
        "b has spent its budget"
    );
    assert_eq!(
        svc.fork_logs_in_window_for_test(c.node_id()),
        0,
        "and c's is untouched"
    );
}

#[test]
fn m31_a_per_window_budget_must_derive_from_the_prod_push_interval() {
    // `HEAD_PUSH_INTERVAL` is 200ms under `cfg(test)`, so a budget
    // derived from it would be ~150x too large and no test could ever
    // reach it — the suite would pass vacuously, which is the failure
    // mode ADR-0041 keeps recording. 5.13c-1b-ii derives its cap from
    // the prod constant; this pins that the two genuinely differ in a
    // test build, so the distinction cannot quietly collapse.
    assert_eq!(
        crate::audit_sync::HEAD_PUSH_INTERVAL_PROD,
        std::time::Duration::from_secs(30)
    );
    assert_ne!(
        crate::audit_sync::HEAD_PUSH_INTERVAL,
        crate::audit_sync::HEAD_PUSH_INTERVAL_PROD,
        "in a test build these must differ, or this guard proves nothing"
    );
}

#[test]
fn m29b_a_backwards_clock_rolls_the_window_instead_of_freezing_it() {
    // `saturating_sub` prevents the underflow, not the freeze. If the
    // first envelopes land while the clock reads AHEAD — a batteryless
    // RTC before NTP settles, which is exactly when the mesh first
    // connects — the window start is stamped in the future, every
    // later subtraction saturates to 0, and the counters never reset.
    // `AUDIT CHAIN FORK` would then be silenced for that peer for the
    // life of the process. `may_serve` shares the pattern but fails
    // OPEN (serves more); this one fails CLOSED, on the loudest alarm
    // the subsystem has.
    let (store, me, trust, b, c, _tmp) = trio();
    let svc = crate::audit_sync::AuditSyncService::new(store.clone(), me, trust);

    let sign_c = |seq: u64, hash: u8| {
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

    // Saturate the budget with the clock far in the future.
    let future = 2_000_000_000_000u64;
    for i in 0..14u8 {
        svc.ingest(
            b.node_id(),
            &envelope(&b, vec![sign_c(7, 0x40 + i)]),
            future,
        );
    }
    assert_eq!(
        svc.fork_logs_in_window_for_test(b.node_id()),
        crate::audit_sync::FORK_LOGS_PER_WINDOW,
        "precondition: the budget is spent"
    );

    // NTP corrects the clock DOWN. The window must roll, and a
    // genuine fork must be loud again.
    let lines = count_audit_errors(|| {
        svc.ingest(b.node_id(), &envelope(&b, vec![sign_c(7, 0xFE)]), 1_000);
    });
    assert_eq!(
        lines, 1,
        "a corrected clock must not leave the fork alarm silenced"
    );
    assert_eq!(
        svc.fork_logs_in_window_for_test(b.node_id()),
        1,
        "and the window must have rolled to the corrected time"
    );
}

#[test]
fn m29c_a_suppressed_fork_line_is_counted_and_reported_not_silent() {
    // ADR-0041's `shell.denied` limiter settled this for the same
    // problem: "a completed flood shows its first ten rows and no
    // trace of the rest" is not a record. A bare `if may_log { log }`
    // leaves an operator unable to tell 8 forks from 8000, and the
    // residual attack is real — a peer burns the budget on self-forks
    // at its own position, and the one fork it later relays about a
    // third party is dropped with nothing saying so.
    //
    // So the count rides the next line that gets through, and it is
    // cleared by being REPORTED, never by time passing.
    let (store, me, trust, b, c, _tmp) = trio();
    let svc = crate::audit_sync::AuditSyncService::new(store.clone(), me, trust);

    let sign_c = |seq: u64, hash: u8| {
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

    // 14 branches: 1 pin + 13 forks, of which 8 are emitted and 5 are
    // dropped and counted.
    for i in 0..14u8 {
        svc.ingest(b.node_id(), &envelope(&b, vec![sign_c(7, 0x40 + i)]), 1_000);
    }
    assert_eq!(svc.fork_logs_dropped_for_test(b.node_id()), 5);

    // The next window's first line carries the count and clears it.
    svc.ingest(
        b.node_id(),
        &envelope(&b, vec![sign_c(7, 0xFE)]),
        1_000 + crate::audit_sync::LOG_WINDOW_MS,
    );
    assert_eq!(
        svc.fork_logs_dropped_for_test(b.node_id()),
        0,
        "the count is cleared by being reported, not by the roll"
    );
}
