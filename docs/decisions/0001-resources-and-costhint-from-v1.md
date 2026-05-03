# ADR-0001 — Carry `Resources`, `CostHint`, and `RateLimit` forward from v1 PRD

|                |                                                                         |
| -------------- | ----------------------------------------------------------------------- |
| **Status**     | Accepted                                                                |
| **Date**       | 2026-05-03                                                              |
| **Phase**      | 1.2                                                                     |
| **Deciders**   | repo author (`humancto`)                                                |
| **Supersedes** | —                                                                       |
| **Context PR** | https://github.com/humancto/harness/pull/3 (Phase 1.2 — protocol types) |

## Context

`HARNESS_PRD_v2.md` §13.2 names `Resources`, `CostHint`, and `RateLimit` as
fields of `NodeManifest` and `Capability`, but does not redefine their
shapes — they are left implicit, presumably to be carried forward from the
v1 PRD where they were spelled out. v2 §22 (repository structure) and §23
(roadmap) treat the protocol types as named-but-shapeless stubs to be
filled in during Phase 1.2.

Phase 1.2 lands these structs in `harness-core`. We need a definitive
record of where each shape comes from, why it has the fields it does, and
what is deliberately deferred — so a future maintainer reading
`crates/harness-core/src/protocol/{manifest,support}.rs` doesn't have to
reverse-engineer the lineage.

## Decision

### `Resources` (shape from v1 PRD §13.2)

```rust
pub struct Resources {
    pub cpu_cores: u8,
    pub ram_total_mb: u32,
    pub gpu: Option<GpuInfo>,
    pub os: String,             // "macos" | "linux" | "windows"
    pub arch: String,           // "x86_64" | "aarch64"
}
```

Lifted verbatim from v1 §13.2. v2 names `Resources` in §13.2 (the
`NodeManifest` field) and §14.3 (the `fit_score` scheduler) without
redefining it; v1's shape is the only explicit definition in either
document.

### `CostHint` (shape from v1 PRD §13.2)

```rust
#[non_exhaustive]
pub enum CostHint { LocalFast, LocalSlow, Gpu, CloudPaid }
```

Variants taken verbatim from v1 §13.2. v2 §13.2 names `cost_hint:
CostHint` on `Capability` without enumerating the variants.

### `RateLimit` (shape proposed by Phase 1.2)

```rust
pub struct RateLimit {
    pub per_second: u32,
    pub burst: u32,
}
```

**Neither** the v1 PRD **nor** the v2 PRD defines `RateLimit`. Both name
it as `Option<RateLimit>` on `Capability`. We ship the minimal
token-bucket shape: a steady-state rate (`per_second`) and a burst
allowance (`burst`). This is the smallest defensible surface that captures
what every realistic enforcement library wants to know.

### `GpuInfo` (shape proposed by Phase 1.2)

```rust
pub struct GpuInfo {
    pub vendor: String,         // "nvidia" | "apple" | "amd"
    pub model: String,
    pub vram_mb: u32,
}
```

`Resources::gpu: Option<GpuInfo>` requires a `GpuInfo` shape. v1 §13.2
names the field but doesn't enumerate; we ship the smallest fields that
let the scheduler answer "does this node have enough VRAM for this task."

## Consequences

### What this PR enables

- `harness-core::Capability` and `harness-core::NodeManifest` compile and
  round-trip through CBOR.
- The §13.2 wire format is fully defined; `harness-mesh` (Phase 1.3+) can
  gossip manifests without revisiting the protocol shape.
- The `insta` fixtures in `crates/harness-core/tests/wire_format.rs`
  capture today's encoding so any future change is loud.

### What this PR explicitly does NOT do

- **No enforcement.** `RateLimit` is a wire-format type only. Token-bucket
  enforcement, queue admission, and per-node rate accounting all belong to
  Phase 4 (resource-aware scheduling, PRD §14.3).
- **No GPU detection.** `GpuInfo` is what nodes _report_. Auto-detection
  (PRD §9.4) is in `harness-capabilities` (Phase 3+).
- **No cost computation from `CostHint`.** Cost tracking is Phase 5 work
  (PRD §17.8); `CostHint` is a hint, not a price.

### Carried risks

1. **`RateLimit` shape is inferred, not specified.** If Phase 4 lands an
   enforcement model that needs a different field set (e.g., separate
   read/write limits, or per-caller rather than per-capability), we'll
   need a wire-format migration. Mitigation: `#[non_exhaustive]` not
   applicable to structs without breaking, but we can add fields with
   `#[serde(default)]` for backward compatibility.

2. **`Resources::os` and `Resources::arch` are unconstrained `String`s.**
   A typo (`"macOS"` vs `"macos"`) silently flows through. Phase 4's
   `fit_score` will need exact matches, so we'll either add validation at
   manifest construction or switch to typed enums when the failure modes
   are concrete enough to model. Plan §10 footgun #4 documents the
   parallel concern with `Cardinality::Owner { scope_field }`.

3. **`CostHint` is an opaque label, not a number.** Planner-side cost
   estimation (Phase 5) will need a translation table from `CostHint`
   variants to USD-per-call estimates. This decouples the manifest (cheap
   to gossip) from the cost model (cheap to update without manifest
   churn) — a feature, not a bug, but worth noting.

## Revisit

When Phase 4 (resource-aware scheduling, PRD §14.3) starts, audit:

- Whether `Resources` needs disk, network bandwidth, or thermal headroom
  fields.
- Whether `CostHint` needs sub-variants (e.g., `Cloud { tier: ... }` vs
  flat `CloudPaid`).
- Whether `RateLimit` needs the burst-leak model swapped for a sliding
  window or a token bucket with explicit refill rate.

Any change here is a wire-format break and follows the same review
process as any other ADR.

## References

- v1 PRD: `HARNESS_PRD.md` §13.2 (where `Resources`, `CostHint` originated).
- v2 PRD: `HARNESS_PRD_v2.md` §13.2 (where they're named but not redefined).
- Roadmap: `ROADMAP.md` item 1.2.
- Plan: `.planning/phase-1.2-protocol-types.plan.md` (local-only).
- Implementation: `crates/harness-core/src/protocol/{support,manifest}.rs`.
