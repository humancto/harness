//! Support types referenced by `Capability`, `NodeManifest`, and `ResourceHints`.
//!
//! Several of these (`Resources`, `CostHint`, `RateLimit`) are carried
//! forward verbatim from the v1 PRD because v2 names them in §13.2 without
//! re-defining them. See `docs/decisions/0001-resources-and-costhint-from-v1.md`.

use serde::{Deserialize, Serialize};

/// Coarse hint at how expensive a capability call is. Drives planner-side
/// cost estimation before any LLM call is made.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum CostHint {
    LocalFast,
    LocalSlow,
    Gpu,
    CloudPaid,
}

/// Coarse classification of CPU consumption for scheduling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum CpuClass {
    Light,
    Heavy,
    Pinned,
}

/// Coarse classification of network consumption for scheduling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum NetworkClass {
    None,
    Light,
    Heavy,
}

/// Coarse classification of disk-I/O consumption for scheduling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum DiskIoClass {
    None,
    Light,
    Heavy,
}

/// Token-bucket-shaped rate limit.
///
/// PRD §13.2 references `Option<RateLimit>` on `Capability` without
/// defining the struct. This is the minimal shape — enforcement is out of
/// 1.2 scope. See ADR-0001.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RateLimit {
    pub per_second: u32,
    pub burst: u32,
}

/// Hardware GPU description as advertised on `Resources`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GpuInfo {
    /// `"nvidia"` | `"apple"` | `"amd"` etc.
    pub vendor: String,
    pub model: String,
    pub vram_mb: u32,
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    fn round_trip<T>(value: &T) -> T
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        let mut buf = Vec::new();
        ciborium::ser::into_writer(value, &mut buf).expect("serialize");
        ciborium::de::from_reader(buf.as_slice()).expect("deserialize")
    }

    #[test]
    fn cost_hint_round_trip_each_variant() {
        for v in [
            CostHint::LocalFast,
            CostHint::LocalSlow,
            CostHint::Gpu,
            CostHint::CloudPaid,
        ] {
            assert_eq!(v, round_trip(&v));
        }
    }

    #[test]
    fn cpu_class_round_trip() {
        for v in [CpuClass::Light, CpuClass::Heavy, CpuClass::Pinned] {
            assert_eq!(v, round_trip(&v));
        }
    }

    #[test]
    fn network_class_round_trip() {
        for v in [NetworkClass::None, NetworkClass::Light, NetworkClass::Heavy] {
            assert_eq!(v, round_trip(&v));
        }
    }

    #[test]
    fn disk_io_class_round_trip() {
        for v in [DiskIoClass::None, DiskIoClass::Light, DiskIoClass::Heavy] {
            assert_eq!(v, round_trip(&v));
        }
    }

    #[test]
    fn rate_limit_round_trip() {
        let rl = RateLimit {
            per_second: 100,
            burst: 200,
        };
        assert_eq!(rl, round_trip(&rl));
    }

    #[test]
    fn gpu_info_round_trip() {
        let g = GpuInfo {
            vendor: "apple".into(),
            model: "M2 Max".into(),
            vram_mb: 32_768,
        };
        assert_eq!(g, round_trip(&g));
    }
}
