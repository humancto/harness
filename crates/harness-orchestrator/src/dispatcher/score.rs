//! `fit_score` — resource-aware scheduler scoring (roadmap 4.4,
//! PRD §14.3/§14.9, ADR-0026).
//!
//! Pure: the daemon assembles [`NodeSnapshot`]s from heartbeats,
//! manifests, and store counts; this module only computes. Score 0 is
//! reserved for genuine impossibilities (**hard gates**: paused,
//! GPU-required without a GPU, memory/VRAM demonstrably over capacity,
//! pinned-CPU-full); load pressure is **soft** — each pressure factor
//! is floored at [`PRESSURE_FLOOR`], so a saturated node ranks last but
//! is never excluded (plan review BLOCKER-1: saturation is transient
//! and must never convert into task failure).
//!
//! With today's unpopulated heartbeats (all-zero load fields) and no
//! in-flight rows, equal-capacity candidates score identically and
//! selection degrades to round-robin; heterogeneous fleets get
//! capacity-proportional placement from day one (ADR-0026 — a
//! deliberate behavior change, locked by test).

use std::collections::HashMap;

use harness_core::{NodeId, ResourceHints};

/// Floor for each soft pressure factor: a fully-saturated dimension
/// contributes this instead of 0, keeping the node rankable.
pub const PRESSURE_FLOOR: f64 = 0.05;
/// Estimated cores consumed per in-flight task when the node reports no
/// real CPU telemetry (heartbeat sampling is a Phase 6 item).
pub const INFLIGHT_CORES: f64 = 0.5;
/// Assumed task memory demand when hints don't declare one.
pub const DEFAULT_TASK_MB: u32 = 256;
/// Score divisor for battery-powered nodes (PRD §14.3 `cost_weight`).
pub const BATTERY_COST_WEIGHT: f64 = 2.0;
/// EWMA smoothing for [`SuccessTracker`].
const SUCCESS_ALPHA: f64 = 0.2;
/// Floor for a node's success rate — a bad streak degrades ranking
/// without permanently zeroing the node (successes decay it back up).
pub const MIN_SUCCESS_RATE: f64 = 0.05;

/// Point-in-time view of one candidate node. Zero means "unknown" for
/// capacity fields (today's unpopulated heartbeats) and is treated
/// optimistically at the gates.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct NodeSnapshot {
    // Capacity (manifest `Resources`; heartbeat totals preferred).
    pub cpu_cores: u8,
    pub ram_total_mb: u32,
    pub has_gpu: bool,
    pub gpu_total_mb: u32,
    // Load (heartbeat; zeros until Phase 6 sampling).
    pub cpu_busy_pct: u8,
    pub cpu_pinned_count: u8,
    pub ram_used_mb: u32,
    pub gpu_used_mb: u32,
    /// `max(queue_depth, in_flight.len())` from the heartbeat — max,
    /// not sum, so populating `in_flight` later never double-counts.
    pub reported_inflight: u32,
    pub paused: bool,
    pub on_battery: bool,
    /// Issuer-side count: tasks THIS node has placed on the candidate
    /// (store `assigned_node` in Dispatched|Claimed|Running, plus
    /// same-poll reservations).
    pub assigned_inflight: u32,
}

/// Caller-provided per-node load + reliability view. The daemon
/// implements this over `PeerTable` + `Store`; tests use
/// [`StaticLoadView`].
pub trait LoadView {
    fn snapshot(&self, node: &NodeId) -> NodeSnapshot;
    /// `[MIN_SUCCESS_RATE, 1.0]`; 1.0 for unseen nodes.
    fn success_rate(&self, node: &NodeId) -> f64;
}

/// HashMap-backed [`LoadView`] for tests and static callers.
#[derive(Debug, Default)]
pub struct StaticLoadView {
    pub snapshots: HashMap<NodeId, NodeSnapshot>,
    pub rates: HashMap<NodeId, f64>,
}

impl LoadView for StaticLoadView {
    fn snapshot(&self, node: &NodeId) -> NodeSnapshot {
        self.snapshots.get(node).copied().unwrap_or_default()
    }
    fn success_rate(&self, node: &NodeId) -> f64 {
        self.rates.get(node).copied().unwrap_or(1.0)
    }
}

fn clamp01(v: f64) -> f64 {
    v.clamp(0.0, 1.0)
}

/// Soft pressure factor: `1 - pressure`, floored.
fn soft(pressure: f64) -> f64 {
    (1.0 - clamp01(pressure)).max(PRESSURE_FLOOR)
}

fn task_cores(hints: &ResourceHints) -> f64 {
    match hints.cpu_class {
        harness_core::protocol::CpuClass::Light => 0.1,
        // Heavy, Pinned, and any future class: conservative full core.
        _ => 1.0,
    }
}

/// PRD §14.3, adapted to the data the mesh actually carries (ADR-0026).
/// Result is in `[0.0, 1.0]`; exactly 0.0 only on a hard gate.
#[must_use]
pub fn fit_score(hints: &ResourceHints, snap: &NodeSnapshot, success_rate: f64) -> f64 {
    // --- Hard gates: genuine impossibilities only. ---
    if snap.paused {
        return 0.0; // §14.10 backpressure
    }
    if hints.gpu_required {
        if !snap.has_gpu {
            return 0.0;
        }
        if snap.gpu_total_mb > 0 {
            let gpu_free = snap.gpu_total_mb.saturating_sub(snap.gpu_used_mb);
            if hints.gpu_memory_mb.unwrap_or(0) > gpu_free {
                return 0.0;
            }
        }
    }
    if snap.ram_total_mb > 0 {
        let ram_free = snap.ram_total_mb.saturating_sub(snap.ram_used_mb);
        if hints.memory_mb.unwrap_or(0) > ram_free {
            return 0.0;
        }
    }
    let is_pinned = matches!(hints.cpu_class, harness_core::protocol::CpuClass::Pinned);
    if is_pinned
        && snap.cpu_cores > 0
        && u32::from(snap.cpu_pinned_count) >= u32::from(snap.cpu_cores)
    {
        return 0.0; // §14.9: full for further pinned tasks
    }

    // --- Soft pressure (ranking only, never excluding). ---
    let cores = f64::from(snap.cpu_cores.max(1));
    let inflight = f64::from(snap.assigned_inflight.max(snap.reported_inflight));
    let used_cores = (cores * f64::from(snap.cpu_busy_pct) / 100.0).max(inflight * INFLIGHT_CORES);
    let cpu_pressure = (used_cores + task_cores(hints)) / cores;

    let mem_pressure = if snap.ram_total_mb > 0 {
        // f64 BEFORE adding: u32 addition here overflows on a
        // heartbeat-supplied ram_used_mb near u32::MAX (diff review
        // MAJOR-1 — panic in debug, inverted ranking in release).
        (f64::from(snap.ram_used_mb) + f64::from(hints.memory_mb.unwrap_or(DEFAULT_TASK_MB)))
            / f64::from(snap.ram_total_mb)
    } else {
        0.0 // unknown → neutral
    };

    let gpu_pressure = if hints.gpu_required && snap.gpu_total_mb > 0 {
        (f64::from(snap.gpu_used_mb) + f64::from(hints.gpu_memory_mb.unwrap_or(0)))
            / f64::from(snap.gpu_total_mb)
    } else {
        0.0
    };

    let cost_weight = if snap.on_battery {
        BATTERY_COST_WEIGHT
    } else {
        1.0
    };

    soft(cpu_pressure)
        * soft(mem_pressure)
        * soft(gpu_pressure)
        * success_rate.clamp(MIN_SUCCESS_RATE, 1.0)
        / cost_weight
}

/// The max-demand union of a task's declared hints and the capability
/// manifest's hints — the API currently stamps all-default hints on
/// every submitted task, so the capability's own declaration is the
/// better floor.
#[must_use]
pub fn effective_hints(task: &ResourceHints, capability: Option<&ResourceHints>) -> ResourceHints {
    fn max_opt(a: Option<u32>, b: Option<u32>) -> Option<u32> {
        match (a, b) {
            (Some(x), Some(y)) => Some(x.max(y)),
            (v, None) | (None, v) => v,
        }
    }
    fn cpu_rank(c: harness_core::protocol::CpuClass) -> u8 {
        match c {
            harness_core::protocol::CpuClass::Light => 0,
            harness_core::protocol::CpuClass::Heavy => 1,
            _ => 2, // Pinned and future classes: most demanding
        }
    }
    let Some(cap) = capability else {
        return task.clone();
    };
    ResourceHints {
        cpu_class: if cpu_rank(task.cpu_class) >= cpu_rank(cap.cpu_class) {
            task.cpu_class
        } else {
            cap.cpu_class
        },
        memory_mb: max_opt(task.memory_mb, cap.memory_mb),
        gpu_required: task.gpu_required || cap.gpu_required,
        gpu_memory_mb: max_opt(task.gpu_memory_mb, cap.gpu_memory_mb),
        network_class: task.network_class, // no scoring term yet (ADR-0026)
        disk_io_class: task.disk_io_class,
        estimated_duration_ms: max_opt(task.estimated_duration_ms, cap.estimated_duration_ms),
    }
}

/// Per-node EWMA of dispatch outcomes. In-memory on the issuer, not
/// persisted, not gossiped. Optimistic prior (unseen = 1.0), floored at
/// [`MIN_SUCCESS_RATE`] so a bad streak degrades ranking without
/// permanently zeroing a node.
#[derive(Debug, Default)]
pub struct SuccessTracker {
    inner: parking_lot::Mutex<HashMap<NodeId, f64>>,
}

impl SuccessTracker {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(&self, node: NodeId, ok: bool) {
        let mut inner = self.inner.lock();
        let r = inner.entry(node).or_insert(1.0);
        let outcome = if ok { 1.0 } else { 0.0 };
        *r = ((1.0 - SUCCESS_ALPHA) * *r + SUCCESS_ALPHA * outcome).max(MIN_SUCCESS_RATE);
    }

    #[must_use]
    pub fn rate(&self, node: &NodeId) -> f64 {
        self.inner.lock().get(node).copied().unwrap_or(1.0)
    }
}

/// 4.6 (ADR-0028, PRD §14.5): per-node circuit breaker — after
/// [`BENCH_THRESHOLD`] CONSECUTIVE node-health failures (lease expiry,
/// send failure — never task-level result statuses), the node is
/// benched for [`BENCH_MS`] and excluded from `Anyone`/`Owner`
/// candidate sets. ANY accepted result from the node (even a Failed
/// one — it proves liveness) resets the streak and clears the bench.
/// Issuer-local and in-memory, like [`SuccessTracker`]; the daemon
/// never benches self, pinned tasks, or Federated fan-outs.
#[derive(Debug, Default)]
pub struct Breaker {
    inner: parking_lot::Mutex<HashMap<NodeId, BreakerEntry>>,
}

#[derive(Debug, Clone, Copy, Default)]
struct BreakerEntry {
    consecutive_fails: u32,
    benched_until: Option<std::time::Instant>,
}

/// Consecutive node-health failures that bench a node (PRD §14.5).
pub const BENCH_THRESHOLD: u32 = 5;
/// Bench duration (PRD §14.5: "60s bench").
pub const BENCH_MS: u64 = 60_000;

impl Breaker {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// One node-health failure. Benches the node once the streak
    /// reaches [`BENCH_THRESHOLD`] (and re-benches while it persists).
    pub fn record_failure(&self, node: NodeId) {
        self.record_failure_at(node, std::time::Instant::now());
    }

    /// Deterministic seam for tests.
    pub fn record_failure_at(&self, node: NodeId, now: std::time::Instant) {
        let mut inner = self.inner.lock();
        let e = inner.entry(node).or_default();
        e.consecutive_fails = e.consecutive_fails.saturating_add(1);
        if e.consecutive_fails >= BENCH_THRESHOLD {
            e.benched_until = Some(now + std::time::Duration::from_millis(BENCH_MS));
        }
    }

    /// Proof of liveness: clears the streak and any bench.
    pub fn record_ok(&self, node: NodeId) {
        let mut inner = self.inner.lock();
        if let Some(e) = inner.get_mut(&node) {
            e.consecutive_fails = 0;
            e.benched_until = None;
        }
    }

    #[must_use]
    pub fn is_benched(&self, node: &NodeId) -> bool {
        self.is_benched_at(node, std::time::Instant::now())
    }

    /// Deterministic seam for tests. Expiry auto-clears.
    #[must_use]
    pub fn is_benched_at(&self, node: &NodeId, now: std::time::Instant) -> bool {
        let mut inner = self.inner.lock();
        let Some(e) = inner.get_mut(node) else {
            return false;
        };
        match e.benched_until {
            Some(until) if now < until => true,
            Some(_) => {
                // Bench served: the node gets a fresh chance (streak
                // resets so one more failure doesn't instantly re-bench).
                e.benched_until = None;
                e.consecutive_fails = 0;
                false
            }
            None => false,
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::float_cmp
)]
mod tests {
    use super::*;

    #[test]
    fn breaker_threshold_reset_and_expiry() {
        let b = Breaker::new();
        let node = NodeId::from_bytes([1; 16]);
        let t0 = std::time::Instant::now();
        for _ in 0..BENCH_THRESHOLD - 1 {
            b.record_failure_at(node, t0);
        }
        assert!(!b.is_benched_at(&node, t0), "under threshold: not benched");
        b.record_failure_at(node, t0);
        assert!(
            b.is_benched_at(&node, t0),
            "5th consecutive failure benches"
        );
        // Liveness proof clears everything.
        b.record_ok(node);
        assert!(!b.is_benched_at(&node, t0));
        // Re-bench and serve the full bench: auto-clears with a fresh
        // streak.
        for _ in 0..BENCH_THRESHOLD {
            b.record_failure_at(node, t0);
        }
        assert!(b.is_benched_at(&node, t0));
        let after = t0 + std::time::Duration::from_millis(BENCH_MS + 1);
        assert!(!b.is_benched_at(&node, after), "bench expires");
        b.record_failure_at(node, after);
        assert!(
            !b.is_benched_at(&node, after),
            "streak reset after a served bench — one failure must not re-bench"
        );
    }

    #[test]
    fn breaker_ok_interleaved_never_benches() {
        let b = Breaker::new();
        let node = NodeId::from_bytes([2; 16]);
        let t0 = std::time::Instant::now();
        for _ in 0..20 {
            b.record_failure_at(node, t0);
            b.record_failure_at(node, t0);
            b.record_ok(node); // consecutive requirement
        }
        assert!(!b.is_benched_at(&node, t0));
    }
    use harness_core::protocol::{CpuClass, DiskIoClass, NetworkClass};

    fn hints() -> ResourceHints {
        ResourceHints {
            cpu_class: CpuClass::Light,
            memory_mb: None,
            gpu_required: false,
            gpu_memory_mb: None,
            network_class: NetworkClass::None,
            disk_io_class: DiskIoClass::None,
            estimated_duration_ms: None,
        }
    }

    #[test]
    fn t01_hard_gates_zero_the_score() {
        let h = hints();
        let snap = NodeSnapshot {
            paused: true,
            ..NodeSnapshot::default()
        };
        assert_eq!(fit_score(&h, &snap, 1.0), 0.0, "paused");

        let mut gpu = hints();
        gpu.gpu_required = true;
        let snap = NodeSnapshot::default(); // has_gpu = false
        assert_eq!(fit_score(&gpu, &snap, 1.0), 0.0, "gpu missing");

        let mut gpu_mem = hints();
        gpu_mem.gpu_required = true;
        gpu_mem.gpu_memory_mb = Some(9_000);
        let snap = NodeSnapshot {
            has_gpu: true,
            gpu_total_mb: 8_192,
            ..NodeSnapshot::default()
        };
        assert_eq!(fit_score(&gpu_mem, &snap, 1.0), 0.0, "vram over capacity");

        let mut big = hints();
        big.memory_mb = Some(64_000);
        let snap = NodeSnapshot {
            ram_total_mb: 16_000,
            ..NodeSnapshot::default()
        };
        assert_eq!(fit_score(&big, &snap, 1.0), 0.0, "ram over capacity");

        let mut pinned = hints();
        pinned.cpu_class = CpuClass::Pinned;
        let snap = NodeSnapshot {
            cpu_cores: 4,
            cpu_pinned_count: 4,
            ..NodeSnapshot::default()
        };
        assert_eq!(fit_score(&pinned, &snap, 1.0), 0.0, "pinned full (§14.9)");
    }

    #[test]
    fn t02_saturation_is_soft_never_zero() {
        // BLOCKER-1: a Heavy task on an exactly-fitting idle 1-core node
        // must score > 0 (100% utilization is the ideal last placement).
        let mut heavy = hints();
        heavy.cpu_class = CpuClass::Heavy;
        let snap = NodeSnapshot {
            cpu_cores: 1,
            ..NodeSnapshot::default()
        };
        let s = fit_score(&heavy, &snap, 1.0);
        assert!(s > 0.0, "exactly-fitting Heavy task is soft: {s}");

        // Massively oversubscribed node: still rankable, floored.
        let snap = NodeSnapshot {
            cpu_cores: 2,
            assigned_inflight: 50,
            ..NodeSnapshot::default()
        };
        let s = fit_score(&hints(), &snap, 1.0);
        assert!(s >= PRESSURE_FLOOR * 0.9, "saturated but rankable: {s}");
    }

    #[test]
    fn t03_unknown_capacity_neutral_and_uniform() {
        // All-zero snapshots (today's mesh): every equal candidate
        // scores identically and positively.
        let a = fit_score(&hints(), &NodeSnapshot::default(), 1.0);
        let b = fit_score(&hints(), &NodeSnapshot::default(), 1.0);
        assert!(a > 0.0);
        assert_eq!(a, b);
    }

    #[test]
    fn t04_load_monotonicity() {
        let base = NodeSnapshot {
            cpu_cores: 4,
            ..NodeSnapshot::default()
        };
        let s0 = fit_score(&hints(), &base, 1.0);
        let s3 = fit_score(
            &hints(),
            &NodeSnapshot {
                assigned_inflight: 3,
                ..base
            },
            1.0,
        );
        let s6 = fit_score(
            &hints(),
            &NodeSnapshot {
                assigned_inflight: 6,
                ..base
            },
            1.0,
        );
        assert!(s0 > s3 && s3 > s6, "{s0} > {s3} > {s6}");
        // Busy% moves the score the same direction.
        let busy = fit_score(
            &hints(),
            &NodeSnapshot {
                cpu_busy_pct: 90,
                ..base
            },
            1.0,
        );
        assert!(busy < s0);
    }

    #[test]
    fn t05_battery_halves_and_success_scales() {
        let snap = NodeSnapshot {
            cpu_cores: 4,
            ..NodeSnapshot::default()
        };
        let ac = fit_score(&hints(), &snap, 1.0);
        let batt = fit_score(
            &hints(),
            &NodeSnapshot {
                on_battery: true,
                ..snap
            },
            1.0,
        );
        assert!((batt - ac / BATTERY_COST_WEIGHT).abs() < 1e-9);
        let flaky = fit_score(&hints(), &snap, 0.5);
        assert!((flaky - ac * 0.5).abs() < 1e-9);
    }

    #[test]
    fn t06_pinned_full_node_still_takes_network_bound_work() {
        // §14.9 acceptance: CPU-pinned-full is NOT full for a light
        // network-bound task.
        let mut net = hints();
        net.network_class = NetworkClass::Heavy;
        let snap = NodeSnapshot {
            cpu_cores: 4,
            cpu_pinned_count: 4,
            ..NodeSnapshot::default()
        };
        assert!(fit_score(&net, &snap, 1.0) > 0.0);
    }

    #[test]
    fn t07_success_tracker_ewma_floor_and_recovery() {
        let t = SuccessTracker::new();
        let n = NodeId::from_bytes([7; 16]);
        assert_eq!(t.rate(&n), 1.0, "optimistic prior");
        for _ in 0..50 {
            t.record(n, false);
        }
        assert_eq!(t.rate(&n), MIN_SUCCESS_RATE, "floored, never zero");
        for _ in 0..50 {
            t.record(n, true);
        }
        assert!(t.rate(&n) > 0.9, "recovers: {}", t.rate(&n));
    }

    #[test]
    fn t08_effective_hints_max_union() {
        let mut task = hints();
        task.memory_mb = Some(100);
        let mut cap = hints();
        cap.cpu_class = CpuClass::Heavy;
        cap.memory_mb = Some(512);
        cap.gpu_required = true;
        let e = effective_hints(&task, Some(&cap));
        assert_eq!(e.cpu_class, CpuClass::Heavy);
        assert_eq!(e.memory_mb, Some(512));
        assert!(e.gpu_required);
        // No capability hints → task's own.
        let alone = effective_hints(&task, None);
        assert_eq!(alone.memory_mb, Some(100));
    }

    #[test]
    fn t09_score_always_finite_in_unit_interval() {
        // Coarse sweep in lieu of a proptest dep: extremes everywhere.
        let extremes = [0u32, 1, 255, 65_535, u32::MAX];
        for &infl in &extremes {
            for &ram in &extremes {
                for cores in [0u8, 1, 255] {
                    let snap = NodeSnapshot {
                        cpu_cores: cores,
                        ram_total_mb: ram,
                        ram_used_mb: ram / 2,
                        assigned_inflight: infl,
                        reported_inflight: infl / 2,
                        cpu_busy_pct: 100,
                        ..NodeSnapshot::default()
                    };
                    let mut h = hints();
                    h.memory_mb = Some(ram / 4);
                    let s = fit_score(&h, &snap, 1.0);
                    assert!(s.is_finite() && (0.0..=1.0).contains(&s), "{s}");
                    // MAJOR-1 regression: hostile/buggy heartbeat with
                    // ram_used at the ceiling and NO declared memory
                    // hint must not overflow the u32 addition.
                    let hostile = NodeSnapshot {
                        ram_total_mb: ram.max(1),
                        ram_used_mb: u32::MAX,
                        ..snap
                    };
                    let s = fit_score(&hints(), &hostile, 1.0);
                    assert!(s.is_finite() && (0.0..=1.0).contains(&s), "{s}");
                }
            }
        }
    }
}
