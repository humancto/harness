# ADR-0002 — `Plan::edges` orientation and content-addressing deferral

**Status:** Accepted
**Date:** 2026-05-03 (Phase 2.1)

## Context

`Plan` (PRD §13.5) carries a DAG of tasks via:

```rust
pub tasks: HashMap<TaskId, PlanNode>,
pub edges: Vec<(TaskId, TaskId)>,
```

The PRD specifies `Vec<(TaskId, TaskId)>` without giving the edge orientation. Two readings are possible:

- `(from, to)` = "from depends on to" (i.e. `to` must complete before `from` starts) — the **precedence reading**.
- `(parent, child)` = "parent → child" (i.e. `parent` produces output `child` consumes) — the **dataflow reading**.

Phase 3.9's plan validation needs an unambiguous answer because cycle-detection and topo-sort have to agree with the dispatcher's "which tasks are ready" computation.

## Decision

**Orientation is `(from, to)` = "from depends on to."**

`to` must complete before `from` starts. Equivalently: edges point from a dependent toward its dependency. This makes the common "find roots" query "nodes with no outgoing edges" — i.e. tasks that depend on nothing — which matches the intuition that roots are starting points.

Locked in code via:

- Doc-comment on `Plan::edges` in `crates/harness-core/src/protocol/plan.rs`.
- Regression test `plan_edges_express_from_depends_on_to` in the same file's `mod tests`.

## Content-addressing deferred

`tasks` is a `HashMap<TaskId, PlanNode>`. CBOR encodes maps in iteration order, which `HashMap` randomizes. Two brains constructing the _same_ logical plan will produce different byte sequences and different signatures:

- **Sign / verify works fine** — both sides clone, zero `sig`, encode with `ciborium`, and compare. The encoded bytes are identical between sign and verify on the same machine, and `verify_strict` on the other end re-encodes the received struct (which has the same map iteration order on its end) and verifies against that.
- **Cross-machine byte equality is broken.** Two brains that produce equivalent plans cannot agree on a hash. This breaks any "did we generate the same plan?" or "is this plan in the cache?" query.

For Phase 2.1 / 2.2 / 2.4 this is fine — a plan is signed by exactly one brain, sent to exactly one dispatcher, and the dispatcher only verifies the signature, not a content hash.

**Plan content-addressing (sorting `tasks` by `TaskId` before hashing) lands in 3.9** alongside plan validation. At that point, validation will:

1. Sort `tasks` by `TaskId` ascending.
2. Encode the sorted projection into a canonical byte sequence (separate from the signature commit).
3. Hash with `blake3` for the plan's content address.

The signature stays committed to the original CBOR bytes — moving the signature commit to the canonicalized form is a wire-format change requiring its own ADR.

## Consequences

- 2.1 ships the wire shape with an explicit orientation contract and a regression test.
- 2.2's dispatcher cannot rely on plan equality across nodes.
- 3.9 owns the canonicalization + content-addressing implementation when plan validation lands.
- Any Phase 4+ feature that hashes plans (e.g. plan caching, plan dedupe) must wait for 3.9 or carry its own canonicalization pass.

## Alternatives considered

- **Use a `Vec<PlanNode>` instead of `HashMap<TaskId, PlanNode>`** — fixes determinism but loses O(1) lookup that the dispatcher and validator need. Rejected.
- **Use a `BTreeMap<TaskId, PlanNode>`** — gives deterministic iteration order. Tempting, but increases insertion cost from O(1) amortized to O(log n) and changes the wire shape encoding (CBOR map order is the same, but `serde` round-trip via `BTreeMap` re-sorts). The cost matters because the dispatcher mutates the in-memory plan as tasks complete (status transitions touch the map). Deferred to 3.9 with the rest of canonicalization — at that point we can pick once with full information.
- **Bake the orientation into the wire format via separate `dependencies` and `dependents` fields** — wire-bloat, redundant, and the type-system already enforces orientation through the test + doc-comment. Rejected.
