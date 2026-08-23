# Harness Implementation State

**Current phase:** 4 — Distribution patterns (Phase 3 COMPLETE as of #42)
**Last updated:** 2026-08-23 (post-5.1 LocalStrong planner — Phase 4 COMPLETE, Phase 5 begun)

## Phase 3 summary (post-merge) — COMPLETE

Every 3.x roadmap item is checked. Demo gate **satisfied**: `harness run --all -- uname -a`
executes across QUIC-connected daemons with exactly-once semantics (money tests m01–m03),
and `harness search` / `harness grep` federate across every live node's `fs.*` scopes with
merged, origin-annotated results (m04). Tests: **890 passing** (410 at Phase 2 close).

Phase 3 PRs after the A2 table below:

| PR  | Item        | What shipped                                                                                                                                                     |
| --- | ----------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| #36 | 3.3-A2      | Dispatch runtime + `harness run --all/--on/--where` (details in the A2 row below)                                                                                 |
| #37 | 3.7         | `mcp.proxy` via rmcp — subprocess MCP servers exposed as `mcp.<server>.<tool>`                                                                                    |
| #38 | 3.11        | `mesh.grep` / `mesh.search` federated wrappers + CLI `harness search`/`grep`. ADR-0022                                                                            |
| #39 | 3.3-ui      | UI Remote Shell page with `[node-name]` interleaving                                                                                                              |
| #40 | 3.6-encrypted | Encrypted-at-rest secrets (ChaCha20-Poly1305, blake3-derived key) + `secret_tags` manifest field + `SecretAwareLiveSet` routing. ADR-0021                        |
| #41 | 3.3-gossip  | `harness.gossip.state` replica sync over QUIC + heartbeat `replica_head` anti-entropy + `WS /api/v1/runs/<id>` + axum `:id` route fix. ADR-0019                    |
| #42 | 3.2-stream  | shell.exec line-frame streaming: frame sink → coalesced signed `PartialResult` batches (≤20/s/task) over `harness.task.partial` → 500-frame ring buffers → `partials` in `GET /tasks/:id`. ADR-0020 |

Phase-2 carryovers 1/2/3/5 all closed (gossip channel, replica_head, dispatch runtime, WS run
stream). Carryovers 4 (JSON-Schema input validation at submit), 6 (registry compile-time dup
assert), 7 (full `input_schema` in `GET /capabilities`) remain open — none block Phase 4.

## Phase 2 summary (post-merge)

Phase 2 ships in 9 PRs (#13–#21). Demo gate **partially satisfied**:

- ✅ Submit `echo` from CLI (`harness submit echo --input ...`) and UI (`/submit` page) — works end-to-end against the local daemon.
- ✅ Persist tasks in `~/.harness/harness.db`, list via `harness tasks` and `/runs` UI page.
- ✅ Cardinality routing (`Anyone` / `Owner` / `Federated`) typed and tested in the dispatcher.
- ✅ LWW replication of task state across the mesh (custom CRDT per ADR-0006).
- ⚠️ Cross-node task execution over QUIC is **not yet wired**. The dispatcher's runtime that pulls Tasks off `harness.task.assign` and writes results to `harness.task.result` is the explicit deferral that rolls into **Phase 3.3** (`harness run --all`) since both need the same QUIC envelope channels and worker registry plumbing. Phase 2 ships the persistence + auth + UI surface; Phase 3.3 wires the wire-level dispatch.

PRs merged in Phase 2 (each minimal-scope but production-quality, with rust-expert review on every diff):

| PR  | item | what shipped                                                                                  |
| --- | ---- | --------------------------------------------------------------------------------------------- |
| #13 | 2.1  | Task / Result / Plan envelopes (PRD §13.3-§13.5), Signable, ADR-0002                          |
| #14 | 2.2  | Cardinality enforcement: `Dispatcher::eligible`, `LiveSet`, `CapabilityIndex`, `ScopeIndex`   |
| #15 | 2.3  | SQLite schema (V0001): tasks, manifests, capability/scope indexes, WAL pragmas                |
| #16 | 2.4  | V0002 leases + `dispatcher_cursors`; round-robin selector                                     |
| #17 | 2.5  | Replicated task state (LWW Map, ADR-0006), V0003 `task_replica_state`, `ReplicaApplier` trait |
| #18 | 2.6  | Argon2id admin auth (ADR-0007), bearer sessions, `POST /api/v1/tasks`                         |
| #19 | 2.7  | CLI: `admin set-password`, `submit`, `tasks`, `~/.harness/.session` cache                     |
| #20 | 2.8  | `harness-capabilities` crate: `Capability` trait + registry + built-in `echo`                 |
| #21 | 2.9  | UI Submit + Runs pages, `GET /api/v1/capabilities`, README + GitHub Pages                     |

Tests: 410 passing. ADRs 0001-0007 in place. Each PR atomic, expert-reviewed, CI-gated on macOS + Linux.

### Phase 3 progress (sub-items merged so far)

| PR  | Item  | Title                                                                                                                                                                                                                                   |
| --- | ----- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| #22 | 3.1   | Policy engine — `~/.harness/policy.toml`, ArcSwap-backed reload, deny-all default                                                                                                                                                       |
| #24 | 3.2a  | `shell.exec` synchronous form (Unix-only) + policy gate. ADR-0008 (streaming deferred)                                                                                                                                                  |
| #25 | 3.3a  | Local executor loop + `harness run --on self` + `GET /tasks/<id>` + node_name plumbing                                                                                                                                                  |
| #26 | 3.4   | `llm.local.<model>` auto-registration via Ollama `/api/tags`. ADR-0010                                                                                                                                                                  |
| #27 | 3.5   | LLM micro-batcher (dedup-within-window). ADR-0011                                                                                                                                                                                       |
| TBD | 3.6a  | `llm.cloud.claude` + `harness-vault` plaintext store + `Action::Secret` hook. ADR-0012                                                                                                                                                  |
| TBD | 3.8   | `brain.plan` Template tier — `harness-brain` crate, `WeakCapabilityRegistry`, `Unsigned<Plan>` + `CapabilityRef` in core. ADR-0013                                                                                                      |
| TBD | 3.9   | `brain.plan` LocalFast tier — `LocalFastBackend` (Ollama, feature-gated), `CapabilitySchemaIndex`, `validate_plan` (schema + cost), `PlanningPolicy` gets `confidence_threshold`/`prefer_local_models`/`default_max_cost_usd`. ADR-0014 |
| TBD | 3.10a | Scope plumbing + `fs.list` + `fs.read` with TOCTOU-free path confinement via `cap-std`. `~/.harness/scopes.toml` operator config; `Owner` cardinality declared (forward-compat for 3.3-fanout); `fs` feature opt-in. ADR-0015           |
| #33 | 3.6b  | `llm.cloud.openai` + `llm.cloud.gemini` — mechanical mirrors of 3.6a; Gemini path-embedded model-name validation; key never in URLs. +34 tests                                                                                          |
| #34 | 3.10-fts | `fs.search` (sqlite-FTS5 sidecar index per scope, incremental mtime reindex, bm25+snippet) + `fs.grep` (index-free bounded cap-std walk + regex). ADR-0016. Completes item 3.10. +34 tests                                          |
| #35 | 3.3-A1 | Fanout wire+index plumbing: named channel streams (header + per-channel caps + per-stream replay), PeerNet (ConnMap w/ min-dialer tiebreak, single accept-router per conn, bounded per-peer outbound queues), manifest announce → CapabilityIndex/ScopeIndex/store, TaskAssign/Claim/ResultMsg envelopes, V0005 `assigned_node`. ADR-0017 |
| A2  | 3.3-A2 | Dispatch runtime + CLI: DispatchService (Submitted-poll → eligible_with_rr over MeshLiveSet → self/remote route, lease TTL = max(lease_ms, timeout+15s), expire_pass w/ max-attempts → Expired, undispatchable → Submitted→Failed supervisor hop), worker ingest (idempotent + terminal-resend) + claim ack + signed result reply, executor now polls Dispatched(assigned=self), API node_name/os/capabilities enrichment + SubmitRequest.execution, CLI `run --all/--on/--where` with concurrent fan-out + `[node]` interleaving. 3 two-daemon money tests incl. exactly-once under short leases and worker-death terminal failure |

Tests as of 3.3-fanout PR-A2: **784 passing** (669 at 3.10a merge) (+37 vs. 3.9). Round-1 review caught path-safety TOCTOU (canonicalize+prefix-check has a race window; fixed by switching to `cap_std::fs::Dir`), unbounded `fs.read` allocation (fixed via stat-first + `take(HARD_MAX + 1)`), false `--on <node>` claim (rewritten — `submit --on` doesn't exist; 3.10a only routes end-to-end on the owning node). All 8 round-1 fixes applied; round-2 caught `cap-std` major version (`"3"` → `"4"`), `Dir::entries()` ordering (fragility in t13), and base64 expansion documentation. Implementation surfaced one runtime bug: sync `open()` on a FIFO blocks the runtime worker thread, defeating `tokio::time::timeout` — fixed by stat-first via `Dir::metadata` (`fstatat`, never blocks) before `open`.

## Phase 3 carryovers (deferred from Phase 2)

These will land alongside their natural Phase 3 home, not as a "phase 2.10":

1. ~~**QUIC envelope channels** (`harness.task.assign|claim|result`)~~ → SHIPPED in 3.3-fanout (ADR-0017). `harness.gossip.state` remains → **3.3-gossip**.
2. **Heartbeat `replica_head` field for anti-entropy** (wire-format change, ADR-pending) → **3.3-gossip**.
3. ~~**Dispatcher async runtime**~~ → SHIPPED in 3.3-fanout PR-A2 (`DispatchRuntime`: poll/route/lease/expire/claim/result).
4. **JSON-Schema input validation** of `Task.input` against `Capability::input_schema` → ships with **3.1** (policy engine) since both gate on capability metadata.
5. **WS `/api/v1/runs/<task_id>` per-task result stream** → **3.3-gossip** (workers now emit `FinalResult` over QUIC; the WS bridge remains).
6. **`assert!`-on-duplicate at registry compile time** → cosmetic; runtime check is fine for the static built-in set.
7. **`GET /api/v1/capabilities` schema completeness** — currently emits id/version/cardinality/cost_hint without the full `input_schema`. Phase 3.x will surface schemas as part of the registry.

## Done

- **Repo bootstrap** — PRDs imported, `CLAUDE.md`, `ROADMAP.md`, `STATE.md`, `README.md`, `.gitignore` (`693ea4d`).
- **Phase 0 — workspace bootstrap** (PR #1, squash-merged as `85551d3`):
  - `0.1` Cargo workspace with 13 crate stubs per PRD §22, `rust-toolchain.toml` pinned to `1.85.0`, MSRV `1.85` in workspace package, centralized `[workspace.dependencies]` + `[workspace.lints]` (`clippy::pedantic` warn, `unwrap_used = deny`).
  - `0.2` GitHub Actions CI on `ubuntu-latest` + `macos-latest`: `fmt --check`, `clippy --all-targets --all-features -- -D warnings`, `test --workspace --all-features`. Linux-only fast `fmt` job runs first. All 4 checks green on the merge commit.
  - `0.3` `harness-daemon` binary with `[[bin]] name = "harness"` so `cargo install --path crates/harness-daemon` produces `/usr/local/bin/harness`. `--version` prints `harness 0.0.0` via clap 4.5 derive. Real `assert_cmd` integration test in `crates/harness-daemon/tests/version.rs` asserts stdout `starts_with("harness ")` AND `contains(env!("CARGO_PKG_VERSION"))`. Dual MIT/Apache-2.0 license.

## In progress

- Phase 1 — mesh skeleton.
  - `1.1` shipped (PR #2, `e395d9f`): identity primitives + `~/.harness/` filesystem layout.
  - `1.2` shipped (PR #3, `fa5d23b`): the §13.1–§13.2 wire types (`Heartbeat`, `NodeManifest`, `Capability`, `Cardinality`, `MergeStrategy`, `PartialPolicy`, `Scope`, `ResourceHints`, `Resources`, `TaskId`, `PlanId`, `SemVer`) + `Signable` trait (canonical-encoding-with-sig-zeroed, routes through `verify_strict`) + `ProtocolError`. ADR-0001 records the v1→v2 carry-forward of `Resources`/`CostHint`/`RateLimit`/`GpuInfo`. 17 unit + 4 property tests (256 cases each) + 2 insta wire-format fixtures (`heartbeat_wire_v0`, `node_manifest_wire_v0`) + size-budget regression gate.
  - `1.8` shipped (PR #4, `6bb2474`): `harness_mesh::trust` with `Peer` / `TrustTier` / `AddedVia` / `TrustEvent` / `TrustError` / `TrustStore` (open / add / remove / tier / lookup_by_pubkey / all_peers / contains / subscribe). Hex-encoded TOML on disk (`format_version = 1`); hard-error on every inconsistency including self-add, self-in-loaded-file, mode≠0600, format-version mismatch, node_id/pubkey mismatch, duplicate node_ids. Persist-then-commit semantics in add/remove (cache stays at the prior state on persist failure) — review-driven correction with a regression test that locks the invariant. Refactor commit pulls `create_root_dir` / `write_atomic` / `enforce_mode_0600` out of `identity.rs` into `harness_mesh::fs_util` so 1.1 and 1.8 share one implementation; existing 1.1 tests pass unchanged. 5 fs_util unit + 12 trust-file unit + 14 integration + 1 property (64×32 ops). Workspace dep `parking_lot 0.12` added (also unlocks 1.5).

## Next

- **`1.4` (QUIC transport)** — the largest single PR in Phase 1 (5 commits per plan: TLS over Noise via rustls + custom `PinnedKeyVerifier` against `expected_pubkey`, cert deterministic from `Identity` via `rcgen`, cancel-safe `RecvFramer` state machine on `Connection<Mutex>`, per-channel replay protection via `Sequenced` trait on top of `Signable`, 0-RTT disabled, dedupe deferred to 1.5). Plan ready at `.planning/phase-1.4-quic.plan.md`.
- **`1.3` (mDNS)** — runs after 1.4 lands; advertises the QUIC port discovered by 1.4. Plan ready.
- After those: `1.5` (heartbeat broadcast loop) depends on 1.3+1.4; `1.6` (election) depends on 1.5; `1.7` (pairing) depends on 1.4; `1.9` (CLI peers/status) depends on 1.8 (just shipped); `1.10` (UI Mesh page) depends on 1.5.

## Blocked

- (nothing)

## Phase 4 progress

| PR  | Item | What shipped                                                                                                                                                                                          |
| --- | ---- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| #44 | 4.1  | `FanoutController` (harness-orchestrator): pure pull-based Stream, window = clamp(2×N_workers, 4, 64) recomputed per refill, Drop-as-cancel, deadline + FailFast with drop accounting. mesh_meta rewired: remote sub-task rows now O(window) via two concurrently-polled controllers (locals keep the ADR-0022 Fixed(4) bound). ADR-0023. +17 tests (907 total) |

| #45 | 4.2  | Result streams (ADR-0024): `results::task_results` — the §14.8 `Stream<TaskResult>` over FanoutStream (recoverable ResultMapper, never Final); `StreamKind::Progress` riding the 3.2-stream pipe (no new wire channels); mesh_meta emits per-target + one summary frame; `WS /runs/:id` pushes seq-deduped `partials` batches with a guaranteed pre-terminal sweep; CLI `grep`/`search` render TTY-gated incremental progress; types.ts mirrors the full contract for 4.8. +14 tests |

| #46 | 4.3  | DAG executor (ADR-0025): pure `DagScheduler` (Kahn ready-sets, exact skip-cascade, output retention) + `plan.execute` driver (channel-fed 4.1 controller, unpinned signed step rows with `parent`/`plan_id`, `$task_output` threading + resolved-input schema recheck, driver-owned FailFast/continue, try-acquire one-plan-per-node) + `StorePlanExec` wiring + `list_tasks_by_plan` + `Signature` JSON seq-visitor + CLI `harness plan`/`exec`. m05 E2E on a real daemon. +34 tests |

| #47 | 4.4  | Resource-aware scheduler (ADR-0026): pure `fit_score` (hard gates for impossibilities only; soft floored pressure — saturation never fails a task), `eligible_scored` argmax + RR tie-band (equal fleets keep the exact RR sequence), `StoreLoadView` (heartbeat ∪ store counts ∪ same-poll reservations, max-composed), `SuccessTracker` EWMA fed remote + local + expiry + send-fail, truthful heartbeat `queue_depth`, ResourceGated-waits-not-fails, fresh-first batches (risk 10), `ExecutionPolicy::clamped` + submit `resource_hints` (risk 12), `known_capabilities` liveness (4.3 carry). +23 tests |

| #48 | 4.5  | Federated lifecycle (ADR-0027): `FederatedCoordinator` — atomic `submitted→running(self)` claim (`try_start_coordination`, executor can never double-claim), ≤8 coordinations/daemon (no slot = stays Submitted, queueing never failure), ≤64-node fan-out over the 4.1 controller with remaining-budget timeouts, driven through the 4.2 `into_channel` bounded-mpsc bridge (its first detached consumer), stage/streaming frames on the partial pipe; pure `harness-merge` engine (Concat/Dedupe first-wins/TopK stable/Rerank→TopK degradation/Aggregate, items convention, 10k reported truncation); merge-time policy semantics (FailFast=unsettled Skipped; Wait=all-or-Failed with flattened per-node errors; ReturnPartial=`merge.failures` block); provenance persisted (V0006 `task_results.provenance`) → `FinalResult.provenance` (zero wire changes) → `GET /tasks/:id` + types.ts; `ExecutionClass::{Work,Coordination}` + 16 coord permits close the ADR-0022 wedge (peek-skip keeps Work polls live); Federated+pin ⇒ `Single` (sub-tasks execute, never re-coordinate — also the mixed-version path); first federated built-in `mesh.info`; m06/m07 money tests. +40 tests |

| #49 | 4.6  | Lease extension + retry backoff (ADR-0028): `LeaseExtend` on the additive `harness.task.lease` channel (worker extender per remote task, aborted on settle, live-lease lookup per tick; issuer CAS-sets rolling `now+30s` horizon UNCONDITIONALLY — first extension shrinks a long lease so dead workers are caught in seconds — hard-capped by `issued_at + original TTL` so wedged/malicious extenders gain nothing; expiry CASes require `expires_at < now` so extensions beat racing expiry); backoff finally reads `RetryPolicy` (expiry + both send-failure paths, waiting-class pacing, deadline enforced in the skip path, in-memory + pruned); circuit breaker (5 consecutive node-health fails ⇒ 60s bench; results — even Failed — prove liveness; self/pin/Federated exempt; all-benched waits in the gated arm, sole-benched-owner included); boot orphan sweep (local⇒Failed with reason, remote⇒Dispatched(self) re-execution — no retry-budget poisoning); FrameSink promoted into ExecutionContext (`progress_sink` trait methods deleted); RR cursor skipped for pinned routes. m08 money test: live worker's 60s lease shrinks over real QUIC and completes exactly once; killed worker detected in seconds. +30 tests |


| #50 | 4.7  | Backpressure (ADR-0029): the heartbeat `paused` field finally has a producer — `PauseState` (queue-depth hysteresis latch at `max_queue_depth`/release at 3/4, OR operator `POST /admin/pause\|resume` + `GET /status` surfacing) with coordination-permit RAII subtraction so `queue_depth` means WORK depth; `DaemonRuntimeConfig.max_queue_depth` is the §14.10 knob; submit admission (`429` + `Retry-After: 2` over 1024 `Submitted` rows, indexed COUNT, fail-open, internal mints exempt); pause-aware routing in every `eligible_scored` arm (live-paused pin ⇒ ResourceGated while dead pins stay fast-terminal; paused owners wait, never reroute; unpinned federated sets exclude paused into `DispatchPlan::Federated::excluded` → `Skipped` provenance rows, policy-exempt; self gated via the shared `PauseState` in `StoreLoadView`); sub-tasks + plan steps stamp `constraints.deadline = issued_at + timeout_ms` (the pre-dispatch wait is bounded — no posthumous runs); full/lag-condition tests for every shipped bound (peer_net QueueFull both arms, reply pump/bridge/WS Lagged recovery, `into_channel` producer-park, `MESH_EVENT_CAPACITY`/`TERMINAL_EVENT_CAPACITY` named); bounds hygiene (llm_batcher `MAX_SLOT_SENDERS=64` flush-on-full + `MAX_LIVE_FINGERPRINTS=256` bypass; partial_stream pending task cap 256 evict-oldest-warned; sweep covers `elig_failures` + Cancelled reply obligations; additive `partials_dropped` on `GET /tasks/:id` + types.ts; `CLI_FANOUT_WINDOW=16`). m09 money test: saturate→pause→pinned waits/Anyone reroutes/federated Skips→drain→resume→exactly-once. +32 tests |

| #52 | 5.1  | LocalStrong planner (ADR-0030): `LocalFastBackend`+`LocalStrongBackend` as newtypes over one `LocalLlmCore` (tier knobs only: `localfast:`/`localstrong:` ids, 30s/120s timeouts, 8/16KiB prompt caps; 3.9 wiremock suite covers the shared core unchanged); `classify_local_model` over VERBATIM Ollama tags (`:`+`-` tokenization, `<n>x<m>b` MoE multiply, decimals, quantized suffixes; ≥20B effective ⇒ Strong; sizeless ⇒ Fast); `resolve_local_models` partitions one `prefer_local_models` list into `[fast?, strong?, template]` (mixed-list behavior change documented: 3.9 bound the 70B to tier 1); CLI `--timeout-ms` wired through planning + default 180s (the 60s hardcode would have starved Template behind a 120s tier 2 — plan review MAJOR-2). Zero executor changes (three-tier walk test t29). +4 tests |

| #51 | 4.8  | UI DAG viz + live progress: `GET /tasks` lists recent tasks across ALL states (limit clamp, `?state=` exact filter preserves the pre-4.8 view, 400 on unknown; additive `parent`/`plan_id` via `TaskRow`+`list_recent_tasks` over `idx_tasks_by_issued_at`); `WS /runs/:id` session-gated before upgrade (it serves task output — cookie rides the browser handshake, tests/CLI use bearer); plan.execute emits additive `in_flight` step frames at submit (settle frames only ever carried terminal states — the live DAG had nothing to light); UI: runs list rework (grouping, live badges), `/runs/[id]` (prerender=false) with WS live view + close-code-aware poll fallback, progress bar reduced from plan/mesh/federated frame families, log tail, `partials_dropped` banner, provenance table incl. Skipped; `$lib/dag.ts` pure Kahn layout (no new deps, cycle-defensive) + DagView SVG, arrows dependency→dependent mirroring the Rust orientation lock. +5 Rust tests, +12 vitest (35 UI total) |

4.8 review round 1 (plan): REVISE, all adopted pre-implementation — MAJOR-1: `ws_run` had
NO session auth (only loopback-Origin); 4.8 gates it with `is_authenticated` pre-upgrade
(deliberate contract change; headerless clients must present a session). MAJOR-2: step
frames were settle-only (`InFlight` never emitted; t08 pinned 2 frames) — additive
in_flight emit at submit_step makes the live DAG real. MAJOR-3: `/runs/[id]` needs
`prerender = false` under the root prerender=true SPA shell or adapter-static fails the
build. MINOR-4..7 + NIT-8 (query extractor is new code w/ 400s; progress vocab corrected
to completed/total + FederatedProgressChunk; TaskSummaryDto added to types.ts; TaskRow
ripple through both positional SELECTs; 1000-close never reconnects) adopted; money tests
(parent linkage through the LIST endpoint, DAG arrow direction, three-family reducer).

4.7 review round 1 (plan): REVISE, all adopted pre-implementation — BLOCKER-1: no
`PauseAwareLiveSet` (liveness ≠ load); the gate lives inside `eligible_scored` via the
LoadView, non-Anyone arms stop delegating to the LoadView-less `eligible()`; BLOCKER-2:
sub-task/plan-step deadlines bound the new pre-dispatch waiting phase (ADR-0022 extended
to pre-lease); MAJOR-3 shared self-view (paused node stops dispatching to itself);
MAJOR-4 coordination subtraction (8+16 phantom rows vs default 64); MAJOR-5 Skipped
provenance for excluded federated nodes + the wrappers-wait asymmetry recorded; MAJOR-6
honest coverage (Anyone pins were already 4.4-gated; new ground is Federated pins,
unpinned sets, Owner); MAJOR-7 config knob not cfg(test); MINOR-8..13 + NIT-14 adopted
(indexed COUNT + TOCTOU note, two QueueFull arms, flush-on-full via atomic remove,
replica-gossip cancel recovery correction, wire-`dropped` accumulation, refill-gate
equivalence note for ADR-0023).
**Carried (ADR-0029): operator-pause persistence (in-memory by design; §25.2 revisits);
federated-parent leases → Phase 5 (ADR-0028 carry); shell.exec ctor sink → Phase 6.**

4.6 review round 1 (plan): REVISE, all adopted pre-implementation — BLOCKER-1: the
extension CAS is an unconditional `min(now+horizon, budget)` set (the drafted `max()` was
dead code and "first-extension" detection unnecessary); BLOCKER-2: issuer-side budget cap
`issued_at + lease TTL` (never trust the worker to stop); expiry CASes gained
`expires_at < now` (extension-vs-expiry race); extension accepts `pending|claimed` (R3);
extender resolves the live lease per tick (re-assign staleness); async send-fail path
feeds backoff; deadline enforced in the backoff-skip path (no other check reachable);
breaker feeds narrowed to node-health signals + self-exempt + structural pin/Federated
bypass + all-benched→gated remap; remote-issued orphans reset (not Failed — synthetic
terminals would poison the issuer's retry budget via terminal-resend).
**Carried (ADR-0028): federated-parent leases → Phase 5 wrapper work; retry-aware Wait →
Phase 5; federated scoring → 4.7; shell.exec ctor sink → Phase 6.**

4.5 review round 1 (plan): REVISE, all adopted pre-implementation — BLOCKER-1: coordinator
claims the parent with one atomic `submitted→running(self)` UPDATE (never observable at
`Dispatched`, executor can't double-claim; race regression f07); MAJOR-1: per-node error
text homes (Failed = flattened bounded error string; ReturnPartial-Done = `merge.failures`
block); MAJOR-2: coordination peek-skip filters before consuming batch slots and uses
`try_acquire_owned` (no TOCTOU). Scope cut: commit 6 (ExecutionContext FrameSink
promotion, ~37 sites, zero functional dependency) moved to 4.6's first commit.
**Carried (ADR-0027): leaseless-Running parent orphan until 4.6's sweep; retry-aware Wait
needs 4.6 backoff; federated fan-outs are pressure-blind (bypass `eligible_scored`);
coordinator self-load double-count; `must_be_local` still the Phase-2 stub for every
cardinality.**

4.4 review round 1 (plan): BLOCKER fixed pre-implementation — pressure is soft-floored and
ResourceGated is exempt from the eligibility terminal window (saturation = queueing, never
failure); local terminals feed the success EWMA (self-bias fix); heterogeneous-capacity
placement is a deliberate, test-locked behavior change.

4.3 reviews: plan round 1 REVISE (2 blockers fixed pre-implementation: try-acquire
one-plan permit instead of queuing; entry schema index from the manifest union). Diff
round REVISE → fixed in-PR: fail_fast now honors feed-time (resolution/schema-recheck)
step failures; ADR-0025 corrected to the `on_failure` input-field mechanism; UTF-8-safe
CLI preview truncation; DagScheduler retains outputs only for unsettled dependents.
**Carried (ADR-0025 documented): manifest union lacks a liveness filter (capability from
a departed peer validates then fails at the eligibility window — 4.4 follow-up), and step
rows drop parent-task tags (interactive plans lose the LLM batcher-bypass tag).**

4.1 review round 1 (plan): 2 majors fixed pre-implementation — index-resolved failure
provenance; two controller instances so local scans stay ≤4 and remote submission isn't
starved behind local work. `PartialPolicy::Wait` aliased to `ReturnPartial` at the
controller until 4.5 (ADR-0023).
4.2 reviews: plan round 1 REVISE (single recoverable mapper object instead of two boxed
closures; `timed_out` added to the counters so the summary schema is producible; ADR-0024
reconciles 4.2's telemetry-over-partials with 4.5's deferred federated `PartialResult`
streaming). The spawned-driver/bounded-mpsc bridge is deliberately unbuilt until a
detached consumer exists (owner: 4.5). Diff review APPROVE with 4 telemetry-edge notes
recorded in ADR-0024 "Accepted losses"; **Phase 6 hardening carry**: preserve
`PartialBuffers` `next_seq` across ring-entry eviction/recreation so seq-cursored
consumers (WS, CLI) don't suppress reborn frames under >256 concurrent tasks.

## Phase 4 — next up

Spine is sequential: ~~4.1~~ → ~~4.2~~ → ~~4.3~~ → ~~4.4~~ → ~~4.5~~ → ~~4.6 lease
extension/retry backoff~~ (risks 9/10 CLOSED; orphan sweep + FrameSink promotion landed)
→ ~~4.7 backpressure~~ (federated-scoring debt from ADR-0026/0028 RESOLVED: score-blind
by design, pressure-aware via exclusion) → **4.8 UI DAG viz** (provenance +
federated-stage types shipped in 4.5; `partials_dropped` shipped in 4.7). Phase-4-owned obligations from below: risks 2 (drop-guard), 9
(send-failure backoff → 4.6), 10 (dispatch head-of-line → 4.4), 11 (unknown-capability
fast-fail), 12 (`SubmitRequest.execution` clamps → 4.4).

## 3.3-fanout carried risks (review follow-ups; owners named)

From the PR-A1 diff review (APPROVE, 5 minors) + ADR-0017:

1. ~~`expire_and_reset_task` worker guard + `try_complete_pending_or_claimed`~~ — landed in PR-A2 with tests.
2. **Drop-guard for `release_accepted_channel`** — a panicking `TaskChannelHandlers` impl would skip the release and permanently block that channel name until reconnect. Handlers are currently panic-free store calls; fix alongside 3.2-stream's channel work.
3. **Router header-read is inline** — a malicious trusted peer opening headerless streams stalls that peer's own router 5 s per stream (self-inflicted only). Move the header read off the accept loop in 3.2-stream.
4. **Announce is fire-once per connection** — a twice-failed announce send isn't retried until re-adopt; with index-driven routing that becomes a 30 s terminal-Failed for tasks aimed at that node. Consider announce-failure → connection close in 3.3-gossip.
5. **Same-dialer duplicate divergence** — both sides can briefly close both duplicate connections; recovery leans on discovery-event redial (the `dialed` set never forgets). Note for Phase 4 hardening.
6. **Wire fixtures for TaskAssign/Claim/ResultMsg** — no insta pins yet (heartbeat/manifest have them). Add in 3.3-gossip before mixed-version nodes exist.
7. **Federated cardinality routes to a single node until Phase 4.5** (documented in ADR-0017); `redundancy=2` speculative execution untouched (6.2).
8. **`FinalResult.started_at`/`wall_ms` not tracked** until Phase 5 cost tracking (mirror `finished_at`/0).

From the PR-A2 diff review (M1/M2/m4 fixed in-PR; deferred minors below):

9. **Send-failure resets bypass `max_attempts`** — a heartbeat-live but unsendable peer produces a ~10 Hz dispatch→lease→reset loop (bounded only by the peer going stale). Belongs to the 4.6 backoff work (R16).
10. **Dispatch batch head-of-line** — 16 persistently-undispatchable FIFO tasks can stall dispatch for up to the eligibility window; skip-known-failing or rotate the batch in 4.x scheduler work.
11. **Unknown-capability tasks** now fail after the 30 s eligibility window instead of instantly (pre-A2 the executor failed them immediately); consider a fast-fail when NO node advertises the capability.
12. **`SubmitRequest.execution` unclamped** — authenticated callers can set ~49-day lease/timeout values; `redundancy != 1` accepted but ignored until 6.2. Add sanity clamps in the Phase 4 scheduler PR.
13. `elig_failures`/`reply` map entries can strand if a future cancel API removes tasks out-of-band; sweep or key eviction when 5.x cancellation lands.

## Open decisions / carried risks

- **Phase 0 review surfaced two Risks** to address before Phase 3 (not blocking now):
  1. `harness-capabilities` shape (single crate with feature flags, not sub-crates) is decided but not physically discoverable in the empty `lib.rs`. Add `pub mod registry;` + a `[features]` section in Phase 1 or early Phase 3 prep so a fresh contributor doesn't drift into spawning sub-crates.
  2. `tokio = ["full"]` in `[workspace.dependencies]` is free at Phase 0 (Cargo doesn't resolve unused) but will propagate heavy features to every consuming crate in Phase 1+ unless we override per-crate with `default-features = false` + minimal features.
- **Phase 1.1 review surfaced two Risks** carried as follow-ups:
  1. `write_atomic` does not `fsync` the parent directory after rename. Crash-durability gap on Linux/macOS. Plan §7.3 #8 descoped this; file as a follow-up issue once the issue tracker is in active use.
  2. Windows ACL enforcement on `identity.key` (currently `tracing::warn` only). PRD §10.1 wants 0600-equivalent; needs `windows-acl` integration. Acceptable for the "two laptops" demo per plan §11 R1.
- **Phase 1.2 review surfaced four Risks** carried as follow-ups:
  1. Wire-format insta fixtures don't lock the externally-tagged `Cardinality` / `MergeStrategy` shape (the two `_v0` fixtures are a `Heartbeat` with no `Cardinality` field and a `NodeManifest` with empty `capabilities`). Add a `cardinality_wire_v0` fixture in 1.5 to pin the on-wire bytes.
  2. `getrandom` is pulled transitively by the `uuid` `v7` cargo feature regardless of whether we generate UUIDs (only deserialize). No functional impact (~20 KB, builds clean on Darwin/Linux/Windows); update plan/ADR comments when 2.1 lands `TaskId::new_v7()`.
  3. Heartbeat size budget at 512 B leaves only ~30 B headroom over the real-world ~480 B encoding. Acceptable now (well under any QUIC datagram); a future PR can swap struct field names for stable numeric IDs to drop ~60% — wire-format change requiring a separate ADR + version bump.
  4. `Heartbeat::leader_belief: NodeId` has no "no belief yet" sentinel (other than zeroed bytes). Consider `Option<NodeId>` in 1.6 if pre-election heartbeats are real. Wire-format change.
- **Missing in 1.2**, queued for 1.3+: `NodeManifest` property tests (no-capabilities form is mechanical), `ed25519` deterministic-signature property test (one line — catches a future swap to randomized signing).
- **Phase 1.8 review surfaced two carried Risks** (the cache-vs-disk drift was fixed in this PR; remaining items are minor):
  1. `add` / `remove` clone the entire cache on every successful mutation. O(N) per call. Fine at PRD scale (hundreds of peers); revisit with `im::HashMap` if Phase 6 multi-tenant pushes counts into the thousands.
  2. Lagged-subscriber test deferred (would require flooding the 256-event broadcast). The property test (random_add_remove × 64 cases × ≤32 ops × reopen) covers the more important file/cache invariant. Land a dedicated lag test before 1.5's gossip wires up subscribers.
- **`profile.release.panic = "abort"`** may need to flip to `"unwind"` for cost-tracker / brain-handover work in Phase 5. Revisit then.
- **PRD §27 open questions** remain deferred to their relevant phase (mDNS resilience → Phase 1, UI framework → Phase 6, CRDT vs Raft → Phase 2, etc.). UI framework is now decided as **SvelteKit 2 + Svelte 5 + TypeScript + Tailwind 3** (Phase 1.10).
- **Phase 1.10 carried risks (post-merge follow-ups):**
  1. **Daemon lifecycle wiring is incomplete.** The `harness daemon` subcommand starts only the API + UI server. Discovery (1.3), Transport (1.4), HeartbeatService (1.5), Pairing (1.7), and Election (1.6) are NOT yet wired into the `daemon_main` task. The Phase 1 demo gate ("two laptops discover each other, brain reweights when stronger node joins") therefore is NOT runnable from a single binary as-shipped. **This is the FIRST Phase 2 follow-up PR** — daemon lifecycle wiring before any new feature work. Tracking: a dedicated `feat/phase-2.0-daemon-lifecycle` branch + PR.
  2. `~/.harness/config.toml` parsing was promised in the 1.10 plan §3 but not implemented; the only knob is `--bind` / `HARNESS_API_BIND`. `[api]`, `[mesh]`, `[discovery]` sections will land alongside the daemon-lifecycle PR.
  3. `npm audit --audit-level=high` is `continue-on-error: true` (advisory). Phase 6.8 hardening MUST flip this to blocking.
  4. `ApiState::set_local_status` calls a caller-supplied closure while holding a write guard. A future caller that re-enters ApiState methods would deadlock (`parking_lot::RwLock` is not reentrant). Documented in code; potential refactor to two-step (mutate-into-buffer, then write) if a real caller hits this.
  5. The Mesh page is a card grid sorted by `brain_score desc` — not a true topology graph. The roadmap line says "live topology over WebSocket relay" but the Phase 1 demo gate only requires visual confirmation of brain rebalancing; the card grid satisfies that. A topology graph (Cytoscape.js or sigma.js) is a Phase 6 polish item if needed.

## How this file is updated

Every merged PR that closes a roadmap item updates this file in the same commit that flips the `ROADMAP.md` checkbox. No silent drift.
