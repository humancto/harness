//! 3.3-gossip money tests — two full daemons over loopback QUIC
//! (reusing the `fanout_tests` boot harness), exercising both
//! convergence paths of ADR-0019: the periodic delta push and the
//! heartbeat-`replica_head`-triggered full sync.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::time::Duration;

use harness_core::{ReplicatedState, ReplicatedTaskState, TaskId};

use crate::fanout_tests::{boot_pair, now_ms, wait_for_mesh, TestDaemon};

fn apply_local(
    daemon: &TestDaemon,
    task_id: TaskId,
    state: ReplicatedState,
    at_ms: u64,
    preview: Option<&[u8]>,
) {
    daemon
        .store
        .replica_apply_local(&ReplicatedTaskState {
            task_id,
            state,
            at_ms,
            source: daemon.node_id(),
            output_preview: preview.map(<[u8]>::to_vec),
        })
        .expect("replica_apply_local");
}

async fn wait_for_entry(
    daemon: &TestDaemon,
    task_id: TaskId,
    state: ReplicatedState,
    budget: Duration,
    what: &str,
) {
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        if let Ok(Some(entry)) = daemon.store.replica_view_task(task_id) {
            if entry.state == state {
                return;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            let last = daemon.store.replica_view_task(task_id).ok().flatten();
            panic!("timed out waiting for {what}; last entry {last:?}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_equal_heads(a: &TestDaemon, b: &TestDaemon, budget: Duration) {
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        let ha = a.store.replica_head().expect("head a");
        let hb = b.store.replica_head().expect("head b");
        if ha == hb {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "replica heads never converged: {ha:02x?} vs {hb:02x?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Delta-push path: entries written locally on A converge to B via the
/// periodic gossip tick, entry-for-entry, and the blake3 heads agree
/// once converged (the determinism the anti-entropy trigger rests on).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn g01_replica_entries_converge_via_periodic_gossip() {
    let (a, b) = boot_pair(None).await;
    wait_for_mesh(&a, &b).await;

    let now = now_ms();
    let ids: Vec<TaskId> = (0..3).map(|_| TaskId::new_v7()).collect();
    apply_local(&a, ids[0], ReplicatedState::Done, now, Some(b"ok"));
    apply_local(&a, ids[1], ReplicatedState::Running, now + 1, None);
    apply_local(&a, ids[2], ReplicatedState::Failed, now + 2, Some(b"boom"));

    for (id, state) in [
        (ids[0], ReplicatedState::Done),
        (ids[1], ReplicatedState::Running),
        (ids[2], ReplicatedState::Failed),
    ] {
        wait_for_entry(&b, id, state, Duration::from_secs(15), "entry on b").await;
    }
    // Full LWW rows (not just states) made it across.
    let done = b
        .store
        .replica_view_task(ids[0])
        .expect("view")
        .expect("present");
    assert_eq!(done.source, a.node_id());
    assert_eq!(done.at_ms, now);
    assert_eq!(done.output_preview.as_deref(), Some(b"ok".as_slice()));

    wait_for_equal_heads(&a, &b, Duration::from_secs(15)).await;

    a.stop().await;
    b.stop().await;
}

/// Anti-entropy path: an entry whose `at_ms` is far older than A's
/// per-peer watermark is invisible to the delta push. Convergence must
/// come from the heartbeat `replica_head` mismatch triggering a full
/// chunked snapshot send (ADR-0019).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn g02_stale_timestamp_entry_recovers_via_head_triggered_full_sync() {
    let (a, b) = boot_pair(None).await;
    wait_for_mesh(&a, &b).await;

    // Seed + converge so A's watermark for B advances to ~now.
    let seed = TaskId::new_v7();
    apply_local(&a, seed, ReplicatedState::Done, now_ms(), None);
    wait_for_entry(
        &b,
        seed,
        ReplicatedState::Done,
        Duration::from_secs(15),
        "seed",
    )
    .await;
    wait_for_equal_heads(&a, &b, Duration::from_secs(15)).await;

    // Inject the divergence: at_ms = 1 is older than the advanced
    // watermark, so the delta filter (`at_ms > watermark`) can never
    // select it. Only the head-triggered full sync can deliver it.
    let stale = TaskId::new_v7();
    apply_local(&a, stale, ReplicatedState::Done, 1, Some(b"stale"));
    assert_ne!(
        a.store.replica_head().expect("head a"),
        b.store.replica_head().expect("head b"),
        "the stale entry must diverge the heads"
    );

    // Heartbeats (2 s) carry the heads; the full-sync rate limit is 2 s
    // in test builds — allow a generous budget.
    wait_for_entry(
        &b,
        stale,
        ReplicatedState::Done,
        Duration::from_secs(20),
        "stale entry on b (full sync)",
    )
    .await;
    let entry = b
        .store
        .replica_view_task(stale)
        .expect("view")
        .expect("present");
    assert_eq!(entry.at_ms, 1);
    assert_eq!(entry.output_preview.as_deref(), Some(b"stale".as_slice()));
    wait_for_equal_heads(&a, &b, Duration::from_secs(15)).await;

    a.stop().await;
    b.stop().await;
}
