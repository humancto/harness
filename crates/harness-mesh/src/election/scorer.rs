//! `brain_score` per PRD §12.2:
//!
//! ```text
//! brain_score(node) =
//!     base_score(node_id)              // small deterministic tiebreak
//!   + 100  * has_local_fast_planner
//!   + 200  * has_local_strong_planner
//!   + 50   * has_cloud_planner_keys
//!   + 30   * cpu_cores
//!   + 50   * (ram_gb / 16)
//!   - 100  * is_battery_powered_unplugged
//!   - 200  * was_brain_recently_demoted   // 60s anti-flap
//!   - 1000 * recent_planner_error_rate
//! ```

use harness_core::NodeId;

/// Inputs to the brain-score calculation. The local daemon refreshes
/// these whenever the underlying capabilities change (Ollama models
/// loaded/unloaded, AC unplugged, planner errors, etc.).
///
/// Multiple bool fields here directly mirror PRD §12.2 — pre-aggregating
/// into a bitfield would obscure the formula. `clippy::struct_excessive_bools`
/// is allowed for that reason.
#[derive(Debug, Clone, Copy)]
#[allow(clippy::struct_excessive_bools)]
pub struct BrainScoreInput {
    pub node_id: NodeId,
    pub has_local_fast_planner: bool,
    pub has_local_strong_planner: bool,
    pub has_cloud_planner_keys: bool,
    pub cpu_cores: u8,
    pub ram_gb: u16,
    pub on_battery_unplugged: bool,
    /// 0 if not recently demoted; non-zero (typically 1) inside the
    /// 60-second anti-flap window.
    pub recently_demoted: u8,
    /// Recent planner error rate as a 0-100 percentage.
    pub planner_error_rate_pct: u8,
}

impl BrainScoreInput {
    /// Reasonable defaults for a node that hasn't yet finished its
    /// capability detection: assume worst case (no planners, no GPU,
    /// 1 core, 1 GB RAM, on battery, never demoted).
    #[must_use]
    pub fn minimal(node_id: NodeId) -> Self {
        Self {
            node_id,
            has_local_fast_planner: false,
            has_local_strong_planner: false,
            has_cloud_planner_keys: false,
            cpu_cores: 1,
            ram_gb: 1,
            on_battery_unplugged: false,
            recently_demoted: 0,
            planner_error_rate_pct: 0,
        }
    }
}

/// Small deterministic tiebreak from the node id. The first byte
/// alone is plenty (256 buckets) — we mix in the next 3 for safety
/// without dominating the scoring table.
fn base_score(id: NodeId) -> i32 {
    let bytes = id.as_bytes();
    // Sum first 4 bytes, max 4 * 255 = 1020.
    i32::from(bytes[0]) + i32::from(bytes[1]) + i32::from(bytes[2]) + i32::from(bytes[3])
}

/// Compute the weighted brain score.
///
/// Outputs an `i32`; the formula's worst-case range is roughly
/// `-100_000` (heavy planner errors) to `+5_000` (fully-loaded
/// big box) so `i32` is comfortable.
#[must_use]
pub fn brain_score(input: &BrainScoreInput) -> i32 {
    let mut s: i32 = 0;
    s += base_score(input.node_id);
    if input.has_local_fast_planner {
        s += 100;
    }
    if input.has_local_strong_planner {
        s += 200;
    }
    if input.has_cloud_planner_keys {
        s += 50;
    }
    s += 30 * i32::from(input.cpu_cores);
    s += 50 * (i32::from(input.ram_gb) / 16);
    if input.on_battery_unplugged {
        s -= 100;
    }
    s -= 200 * i32::from(input.recently_demoted.min(1));
    s -= 10 * i32::from(input.planner_error_rate_pct);
    s
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    fn test_node() -> NodeId {
        NodeId::from_bytes([0; 16])
    }

    #[test]
    fn minimal_node_scores_low_but_positive() {
        let s = brain_score(&BrainScoreInput::minimal(test_node()));
        // 0 base + 30*1 cores = 30
        assert_eq!(s, 30);
    }

    #[test]
    fn local_fast_planner_adds_100() {
        let mut input = BrainScoreInput::minimal(test_node());
        input.has_local_fast_planner = true;
        // base 0 + 100 + 30 = 130
        assert_eq!(brain_score(&input), 130);
    }

    #[test]
    fn local_strong_planner_adds_200() {
        let mut input = BrainScoreInput::minimal(test_node());
        input.has_local_strong_planner = true;
        assert_eq!(brain_score(&input), 230);
    }

    #[test]
    fn cloud_keys_add_50() {
        let mut input = BrainScoreInput::minimal(test_node());
        input.has_cloud_planner_keys = true;
        assert_eq!(brain_score(&input), 80);
    }

    #[test]
    fn cores_scale_linearly_at_30_per_core() {
        let mut input = BrainScoreInput::minimal(test_node());
        input.cpu_cores = 16;
        // 0 + 30*16 = 480
        assert_eq!(brain_score(&input), 480);
    }

    #[test]
    fn ram_scales_at_50_per_16_gb_chunk() {
        let mut input = BrainScoreInput::minimal(test_node());
        input.cpu_cores = 1;
        input.ram_gb = 64;
        // 0 + 30 + 50*(64/16) = 0 + 30 + 200 = 230
        assert_eq!(brain_score(&input), 230);
    }

    #[test]
    fn ram_below_16_gb_contributes_zero() {
        let mut input = BrainScoreInput::minimal(test_node());
        input.ram_gb = 8;
        // 0 + 30 + 50*0 = 30
        assert_eq!(brain_score(&input), 30);
    }

    #[test]
    fn on_battery_subtracts_100() {
        let mut input = BrainScoreInput::minimal(test_node());
        input.on_battery_unplugged = true;
        // 0 + 30 - 100 = -70
        assert_eq!(brain_score(&input), -70);
    }

    #[test]
    fn recently_demoted_subtracts_200() {
        let mut input = BrainScoreInput::minimal(test_node());
        input.recently_demoted = 1;
        // 0 + 30 - 200 = -170
        assert_eq!(brain_score(&input), -170);
    }

    #[test]
    fn planner_errors_at_50pct_subtract_500() {
        let mut input = BrainScoreInput::minimal(test_node());
        input.planner_error_rate_pct = 50;
        // 0 + 30 - 500 = -470
        assert_eq!(brain_score(&input), -470);
    }

    #[test]
    fn fully_loaded_node_scores_high() {
        let mut input = BrainScoreInput::minimal(test_node());
        input.has_local_fast_planner = true;
        input.has_local_strong_planner = true;
        input.has_cloud_planner_keys = true;
        input.cpu_cores = 16;
        input.ram_gb = 64;
        // 0 + 100 + 200 + 50 + 30*16 + 50*4 = 1030
        assert_eq!(brain_score(&input), 1030);
    }

    #[test]
    fn base_score_breaks_ties_deterministically() {
        let mut a = BrainScoreInput::minimal(NodeId::from_bytes([0x01; 16]));
        let mut b = BrainScoreInput::minimal(NodeId::from_bytes([0x02; 16]));
        a.cpu_cores = 4;
        b.cpu_cores = 4;
        let sa = brain_score(&a);
        let sb = brain_score(&b);
        assert_ne!(sa, sb, "different node_ids must produce different scores");
        assert!(sb > sa, "higher first-byte node_id should score higher");
    }
}
