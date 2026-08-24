# Harness Implementation State

**Current phase:** 4 — Distribution patterns (Phase 3 COMPLETE as of #42)
**Last updated:** 2026-08-24 (post-5.13c-1 peer head pins)

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

| #57 | 5.6  | SMS webhook (ADR-0034): channel-generic conversation core extracted from 5.5 (`webhook/conversation.rs`; `Channel` name = route = task tag = log field, threaded through the DRIVER so both `brain.plan` and `plan.execute` mints carry it — plan review MAJOR-1, pinned by tag assertions on both rows in BOTH suites); `POST /webhook/sms` with bare-E.164 senders (ONE allowlist, channel-native entry forms — channels are distinct authorization surfaces, `whatsapp:+X` never admits SMS `+X`, near-miss drop-log hint when the other form is listed); shared signature/dedup/admission/driver/reply machinery unchanged (5.5's 8 whatsapp rows keep passing verbatim + symmetric exec-tag assertion added); same 1600 cap (SMS UCS-2 segment economics recorded); STOP/HELP: inbounds tagged `OptOutType` (Twilio's own opt-out handling fired) are acked empty and never minted — a compliance keyword is not a goal (Codex P2 on #57). Zero new deps. +7 tests |
| #58 | 5.7  | iOS Shortcuts adapter (ADR-0035): signed JSON token — `harness_vault::shortcuts_token` (`base64url(payload).base64url(blake3_keyed_mac)`, not-JWT by design, constant-time compare, strict URL_SAFE_NO_PAD, 4KiB token cap, `sub` 1..=64 printable ASCII at mint AND verify); `POST /webhook/shortcuts` (Bearer header only, JSON body `deny_unknown_fields` — `constraints` smuggle = 400; fail-closed 503 on missing/malformed key; admission + shared 16-permit semaphore = 429) with synchronous wait (default 55s, clamp 1–120s) → `{task_id, status, reply}`, 202 running on timeout; bounded `ShortcutsLedger` (256 outcomes / 512 request ids, FIFO) is late-result store + `request_id` retry dedup (duplicate returns the ORIGINAL task_id + state) + authorization scope for `GET /webhook/shortcuts/result/:id` (non-ledger ids 404 — a shortcut token cannot probe other adapters' tasks); permit rides the spawned driver (client disconnect ≠ lost work); `run_conversation` extracted to return `(reply, ok)` with channel threading intact (Twilio suites unchanged); `harness admin issue-shortcut-token --sub [--ttl-days 90|--no-expiry]` reuses-or-generates the signing key via new atomic `EncryptedStore::upsert` (tmp+fsync+rename, file-values-only) and prints the restart-the-daemon note (startup vault snapshot staleness documented). Post-open Codex round (1 P1 + 2 P2, all verified real and fixed): atomic dedup+reserve under one ledger lock via sync `admit_goal` (concurrent same-request_id retries serialize — exactly one mints), `request_id` capped 128 printable ASCII + `goal` capped 4096 chars (bounded-in-bytes, not just entries), mapped-but-evicted request_id = 410 `result_expired` with the original task_id (never a re-mint); diff-review extras: clock failure = 503 fail-closed (expiry never checks against now=0), case-insensitive Bearer scheme, upsert tmp cleanup on rename failure, saturating expiry arithmetic, Zeroizing key hex in CLI, no duplicate rids in the FIFO. Zero new external deps. +23 tests |
| #59 | 5.8  | Budget enforcement (ADR-0036): first reader of the Phase-2 `Plan.budget` wire type. `BudgetTracker` (harness-orchestrator::budget — sync, in-loop, fires-once SoftCrossed/Exceeded verdicts; `cost_usd` top-level actuals only, NaN/negative clamped; projection deferred to 5.9); per-completion enforcement in plan.execute: Notify = warn+frame, Cancel = stop-dispatch (in-flight dropped like fail-fast), Pause = drop the fan-out sender → in-flight finish and cost-record → SourceDrained settles promptly (plan review B1 deadlock avoided) + `unscheduled` ids recorded for 5.12 resume; envelope `TaskState` untouched (B2) — plan outcome = aggregate `status` field + `budget` object, budget stops return Ok with the aggregate (M1), exceed-after-last-step stays `done` (m3); effective budget: plan Budget wins (§17.8 explicit approval; planners CANNOT self-approve — LLM schema rejects a budget field and backends hardcode None, pinned by test M3) else `[execution].default_plan_budget_usd` ($5 Cancel), `plan_budget_ceiling_usd` hard-caps even waivers; failed steps contribute $0 (frozen, M4 — 5.9 result-row costing fixes); webhook reply + CLI renderer read `status` (M2 — a paused plan never says ✅); `BudgetInconsistent` validation rule (strengthening, future-proofing). Post-open Codex round (1 P1 + 2 P2, verified real, fixed): Pause now raises a source-side flag so the window cannot refill from channel-BUFFERED ReadySteps (wide-DAG pause previously ran the whole initial fan-out; e08 pins ok≤window), `status` discriminates on unscheduled work not done<total (continue-mode failure + last-step exceed = done, e09), policy load rejects non-finite/negative budget knobs and the tracker sanitizes plan-carried nonsense limits to $0 fail-closed. Diff-review round (verdict REVISE→fixed; F1/F2/F3 confirmed the Codex fixes empirically): MAJOR — the budget-stop Ok early-return preceded the deadline/abort checks, so a pause racing a deadline or fail-fast abort misreported as paused_budget; deadline and abort now WIN (the settled plan decision, ADR §8). Minors: hard cap subsumes soft (no soft_limit frame after exceeded), e02 claim honesty (e08 owns window behavior), CLI mentions a cap tripped on the final step, ADR notes unscheduled conflates parked vs dropped in-flight for 5.12. Zero new deps, zero wire changes. +18 tests |
| #60 | 5.9  | Real-time cost tracking (ADR-0037): harness-cost born — pricing table (longest-prefix-wins across builtins ∪ `[cost.model_prices]` overrides, pinned by the gpt-4o/gpt-4o-mini 16x pair; unknown model = unpriced, never guessed; installed once at boot); cloud caps price provider usage into top-level `cost_usd` (5.8 enforcement now sees real dollars); `task_results.cost_usd` (V0007; V0001's dead tasks.cost_* columns documented as legacy) written behind the CloudPaid hint gate at ALL the sites that matter — local executor AND issuer-side remote-result ingest (plan review B1: both stores get a row and the coordinator's is the one the ledger reads; gate judges the ISSUER'S own manifest, never the worker-signed announcement — mcp.proxy inflate vector closed for the ledger, in-loop enforcement stays conservatively open), federated parent NULL; 30-day windowed CostLedger (time-bounded on the existing index + 5000-row backstop, `truncated` echoed; per-plan context parsed from bounded plan.execute aggregates incl. the 5.8 budget object; per-user ≡ per issuing node; estimates cut — no honest source) + session-auth GET /api/v1/costs; ADR-0036 failed-step promise formally RETRACTED (error paths discard bodies). Zero new external deps; local-only column, no wire change (gossip carries ReplicatedTaskState only, verified). +15 tests. Codex P1 post-open (verified, fixed): the ledger feed now excludes NULL/zero-cost rows IN SQL before the row cap — a burst of free local completions can no longer evict paid rows from the window (pinned in store t08). Diff-review round (REVISE→fixed): MAJOR — built-in Anthropic rates were wrong for current-gen models (fable-5 10/50 not 15/75, opus-5 5/25 not 10/40; opus-4-5..-4-8 prefixes added at 5/25 above the legacy opus-4 row — 5.8 would have seen 2-3x inflated dollars); MAJOR — the promised unpriced-model warn now exists (once per model at the global pricing entry). Minors: per_plan aggregate dedup on re-run plans, list truncation now raises the `truncated` flag, per_issuer/per_day are objects not tuples (5.10 freezes this shape), cost-write errors warn, non-CloudPaid claim log downgraded to debug, OnceLock test-pollution doc. CARRIED: executor-site persistence has no daemon-level integration test (gate logic + capture-order unit/ingest-tested; booted-daemon CloudPaid harness deemed too heavy this PR); cloud PLANNING spend untracked (ADR-0037 limitation — CloudBackend doesn't parse usage) |
| #61 | 5.10 | Cost dashboard + task cancel (ADR-0038): `/costs` page — per-UTC-day SVG spend bars (window gap-filled so the x-axis is time-honest; hand-rolled, zero chart deps), 30-day total / today / projected run rate tiles, `truncated` honesty banner, per-plan table with 5.8 budget context (status chips: aborted_budget/paused_budget/cancelled/running + `▲ budget` badge), per-issuer split, 5s visibility-gated poll with generation guard + per-cycle AbortController (the 5.4 hygiene, NOT the runs page's double-fire); active `plan.execute` rows carry the §17.8 red Stop button → confirm dialog → `POST /api/v1/tasks/:id/cancel`. Cancel is a record-level forward-spend stop, not an interrupt: atomic `Store::cancel_task` transaction (state read + transition-table check + flip in one tx; 404 unknown, 409 names the terminal state; `Claimed→Cancelled` ADDED to the table — it was not legal, plan review M3), live leases released so late worker results drop at the terminal-lease guard instead of writing Done-over-Cancelled and gossiping Done / expiry writing Expired / breakers penalizing workers for operator cancels (M4), executor terminal writes gate on winning the Running→Done/Failed CAS — lost CAS skips result write + replica broadcast but still signals terminal_tx (M6; the no-local-timeout honesty is in the ADR), replica mirrors `Cancelled` like every local transition (no wire change — the state existed since Phase 2). Plan review BLOCKERs, both structural: (B1) the exec loop never read its own task row — a cancelled runaway plan would keep minting; `PlanExec::own_cancelled` checked per step completion stops it at the next completion boundary exactly like budget-Cancel (aggregate status "cancelled", stranded steps Skipped; e10 pins no-further-mints); (B2) the active-plans join premise was false — `mint_task` hardcoded plan_id None for ALL top-level rows; plan.execute mints now stamp `input.plan.id` (pinned). Projection = window total ÷ ELAPSED days × 30 (elapsed since max(window start, earliest spend), ≥3-day gate — spend-days would 10x a sparse month, M5). `ui/src/lib/costs.ts` pure module (fillDays/runRate/activePlans/fmtUsd). Post-open Codex round (2 P1 + 1 P2, all verified real, all fixed): plan.execute is Cardinality::Anyone so a REMOTELY-executing plan never saw the issuer's row flip — own_cancelled now also consumes the replicated Cancelled state the cancel mirrors into gossip (bounded by gossip latency; ADR amended); the UI's unfiltered /tasks page (default 50) let a wide plan's freshly-minted steps evict its own coordinator row — precisely the plan the stop button exists for — new `?capability=` listing filter (composes with `?state=`, store-side WHERE) and the page fetches `?capability=plan.execute&limit=200` (test pins the eviction premise AND the filter); fail_now_sync (unknown-capability path) still ignored its Running→Failed CAS — now gated like every other terminal write (t08b: cancel survives, no result row, no Failed replica). Diff-review round (REVISE→fixed; confirmed the Codex fixes + found the deeper seam): MAJOR — the ISSUER-SIDE result ingest was the remaining Done-over-Cancelled hole: cancel's state flip and lease release were two transactions (a dispatch pass could interleave) and on_result wrote result+replica unconditionally after the lease CAS, and since Done(8) outranks Cancelled(6) in the replica LWW order the gossiped Done would supersede the cancel MESH-WIDE, permanently — lease release moved INTO cancel_task's transaction and finish_local_row now reports whether the final CAS won, with on_result dropping the whole ingest on a lost CAS (t01c pins live-lease+cancelled-row → no result row, no Done gossip; on_result split into ingest_done/ingest_failed for the line cap). Minors adopted: t08c pins the MAIN async executor path losing its CAS (gated capability + terminal_tx still fires); runRate now reads exactly the chart's days (the server's ms-precise window can hand a partial 31st UTC day the chart never draws — was counted in the numerator against a clamped denominator); stop() surfaces non-401/409 failures instead of silently doing nothing; cancelled-while-budget-paused keeps the budget status label (doc'd, cosmetic); ADR owns the lost-CAS cost undercount (billed-but-cancelled steps record $0 — accepted over re-opening the divergence). Zero new deps. +9 Rust tests, +7 vitest (63 UI total) |

| #62 | 5.11 | Checkpoint store (ADR-0039): `Plan.checkpoint` — on the signed wire since Phase 2, never read — is finally real. V0008 `checkpoints` keyed `UNIQUE(plan_id, node_id)` with the resolved-input blake3 as a VALIDITY column, NOT the identity (plan review BLOCKER-1: the PRD's literal "hash each input, skip if the hash exists" collides `fs.read {"path":"/x"}` with `fs.delete {"path":"/x"}` — one side effect silently never happens — and collapses two legitimately distinct nodes sharing an input, e.g. a `notify.send` at both ends of a plan, WITHOUT any crash: lookup and record share a table inside one run); node ids come from the signed plan so they survive resubmission and bound the table at MAX_PLAN_STEPS=64 rows/plan. `harness_core::step_hash` (blake3 over canonical JSON of capability + resolved input — `serde_json`'s BTreeMap map is key-sorted here, `preserve_order` off and pinned by test; `HashFn` matched exhaustively inside harness-core so a future variant is a compile error; a mismatch is a MISS = re-run, never a wrong replay). `PlanExec::checkpoint_{lookup,record,finish}` (default no-ops) + `StoreMeshExec` impls; the settle happens in `feed_ready` BEFORE minting — `scheduler.complete(node, Done(cached))`, the same call the resolution-failure arm makes — so no row, no dispatch, no spend, dependents resolve against replayed outputs and `newly_ready` cascades normally. Three things fall out: budget bypass is automatic (accounting lives in the Item arm a settled step never reaches) so the aggregate gained `replayed: K` + per-step `from_checkpoint: true` to keep `ok: N, spent_usd: 0.00` legible; the stop button is checked IN the settle path (a fully-checkpointed replay never reaches the completion arm and would otherwise replay a cancelled plan straight to "done" — e15); and only successful steps are recorded. GC fires on `summary.done == summary.total`, NOT on `status == "done"` (plan review BLOCKER-2: a fail-fast abort also reports "done", and GC there deletes exactly the successful prefix the operator resubmits for — e12 pins both halves) + a 7-day boot age sweep for plans nobody resumes. `File` storage runs UNcheckpointed with a warning frame (`None` = off, silent; `#[non_exhaustive]` wildcard covers future variants). Rowid table, not WITHOUT ROWID (a 256 KiB output column inside the index B-tree degrades the lookup the table exists for). Zero new deps, zero wire change. Post-open Codex round (1 P1 + 1 P2, both verified real, both fixed): the capability joined the hash preimage — `step_hash(capability, input)` — because node ids are the checkpoint's identity and a repaired plan can keep a node's id and input while changing its capability, replaying the wrong output and skipping the dispatch entirely; and GC moved OUT of the plan driver, since deleting when the aggregate is built leaves a crash window where the rows are gone but the plan is not recorded done (the resubmission then re-runs every step, side effects included). Diff-review round (REVISE→fixed) caught that the durability fix had REINTRODUCED the plan-review blocker: `Store::checkpoint_sweep_completed_plans` checked only "row done + result exists", but `drive_plan` returns Ok — and the executor writes done + a result — for partial plans too (a continue-mode failure, a budget pause parking half the graph), exactly the plans an operator resubmits; the sweep now requires the persisted aggregate to report zero failed/timed-out/skipped (an unreadable aggregate counts as incomplete) AND no in-flight run of the same plan id, since an earlier terminal row satisfies the durability predicate forever and would sweep a live rerun's fresh checkpoints mid-run (c07b/c07c/c07d pin all three; the reviewer noted the old assertion had been deleted rather than relocated). Minors: newly-skipped bookkeeping in the checkpoint settle path, e14 now pins the unimplemented-backend warning frame instead of assuming it, e15 asserts duration so a missing loop guard fails instead of passing 120s later, `created_at` index for the now-hourly sweeps, honest comment on the defensive `step_row` guard, `interval_items`-is-ignored documented. +19 tests (1198 workspace). CARRIED, both named in the ADR: (a) webhook restart durability came here from ADR-0033 §6 but is NOT in this change — it needs its own `webhook_conversations` table (reply address + the brain.plan→plan.execute link + a `reply_sent` flag for idempotency) and specifically NOT a `reply_to` task tag, since tags ride the signed envelope across the LAN and would replicate the user's phone number into every peer's DB that executes the task (a PRD stopping-condition surface); (b) checkpoints are LOCAL and never gossiped, so 5.12 "resume on brain handover" is not free — it must pin the resumed plan, gossip checkpoints (wire + privacy decision), or scope itself to same-node restart. Also recorded honestly: this checkpoints ≤64 DAG steps, not §14.11's 100k fan-out items (`harness fanout --checkpoint` stays backlog); and replay bypasses policy evaluation — the invariant is that a checkpoint replays a decision a prior evaluation already permitted, kept auditable by `from_checkpoint` |

| #63 | 5.12 | Plan resume (ADR-0040) — the roadmap says "checkpoint resume on brain handover"; the ADR records why that frame does not fit this architecture and what shipped instead. Plan review round 1 (REVISE, all adopted) found the item's premise broken THREE ways before a line was written. (1) BLOCKER: 5.11 was INERT in production — every planner backend emits `checkpoint: None` and the only `CheckpointConfig` built outside harness-core was a test helper, so no plan the product produces was ever checkpointed and every resume path would replay zero steps; checkpointing is now ON by default via `[execution] checkpoint_plans` (plan-carried config still wins), which is a real semantics choice (replay a recorded output vs re-run the step) and is documented as one. (2) BLOCKER: resume cannot re-dispatch the original row — a plan.execute that ran locally takes NO lease (`dispatch_to` returns early for self) and the boot orphan sweep marks locally-issued Claimed|Running(self) rows `Failed`, a terminal state with no outgoing transition; `POST /api/v1/tasks/:id/resume` therefore mints a NEW plan.execute carrying the SAME plan id (what makes 5.11's checkpoints hit), guarded by `409 already_running` while any non-terminal run of that plan exists, with an optional raised cap (re-signed with the local identity; still clamped by `plan_budget_ceiling_usd` at execute time) and a `replayable` count that is honest about checkpoints being local and expiring. (3) BLOCKER: the election-driven "resume stranded plans" sweep was CUT — it keyed on PEER_TIMEOUT (6s) while the working mechanism keys on lease expiry (30s, with a CAS a live worker defeats by extending), so a routine wifi blip would have reset a row whose coordinator still held its lease, its plan semaphore and its in-flight steps: two concurrent coordinators for one plan, both writing checkpoints and side effects. The placement pin was cut with it (an unreachable pin fails the task terminally, and pinning to the checkpoint holder aims at the node most likely to still be running the plan, where the one-plan-per-node semaphore turns it into an immediate failure) — carried as a soft scorer preference. MAJOR: the `unscheduled`/`in_flight` split keys on minted ROWS, not scheduler state (a step is marked InFlight when it leaves the ready set, before the window pulls it, and the 5.8 Pause path leaves buffered steps unpulled — scheduler state would flag nearly every budget resume as unsafe); `in_flight` steps need explicit `allow_in_flight`. Honest limits in the ADR: checkpoints are never gossiped (full outputs on the LAN where only 256-byte previews go = new exposure + wire change + unbounded volume, to cover only the never-returns case), so a permanently-departed coordinator's plan re-runs; resume within the 7-day retention window; a resumed plan's ledger shows actual spend exceeding the newest aggregate's reported cap. Post-open Codex round (1 P1 + 1 P2, both verified real, both fixed): the idempotence check was check-then-insert, so two concurrent resumes could both observe "nothing live" and both mint a coordinator (SQLite serializes statements, not sequences) — new `Store::insert_task_unless_plan_live` does the check and the insert in ONE transaction and returns the live run's id when it refuses, with the handler's up-front check demoted to a fast path; and a plan that finished every step was resumable, which would silently re-run the whole DAG and its side effects (its checkpoints are eligible for deletion by then) — now `409 nothing_to_resume`, while a plan with NO result row stays resumable since that is the crashed-coordinator case. +10 tests, incl. four concurrent resumes yielding exactly one CREATED. Diff-review round (REVISE→fixed): BLOCKER — the whole in-flight safety mechanism fired only where it was least needed. `drive_plan` returns Err on fail-fast abort, deadline expiry and "no step succeeded", so the executor persists an error string and NO aggregate; a crashed coordinator writes no result row at all. Reading `resume` from the aggregate therefore reported `in_flight: []` on exactly the paths that STRAND dispatched steps, and re-dispatched them with no prompt (a budget Pause, by contrast, drains to SourceDrained so its in_flight is empty by construction — the mechanism only ever worked where nothing was at risk). The authority is now the plan's own STEP ROWS via list_tasks_by_plan (a row exists iff the step was dispatched; non-terminal = never settled), with the aggregate list as a fast path. MAJORs: `Option<Json<T>>` turns EVERY axum rejection into None, so a typo'd field under deny_unknown_fields silently resumed at the OLD cap and returned 201 — now an absent body defaults and a malformed one is 400; and the resumed mint dropped input.timeout_ms/on_failure, the execution policy, constraints and tags, downgrading an explicit 10-minute keep-going run to a 2-minute fail-fast one with a 30s envelope — everything but the plan now comes from the original submission. Test gaps closed: the guarded insert is pinned at the STORE (the HTTP concurrency test could not interleave — no await point between check and insert, so it passed against the pre-fix code too), stranded steps without any aggregate, malformed-body rejection, submission-shape preservation, and the plan-carried checkpoint opt-out. Minors: resume now honors the 4.7 admission cap; one shared tasks INSERT statement; a malformed id blob errors instead of reporting the new task as the live run; the re-signed plan stamps issued_by; `replayable` renamed `replayable_local` (checkpoints live on the node that RAN the plan, and a resumed plan is placed unpinned); one shared aggregate_is_complete predicate with the sweep; ADR corrected on restarted-vs-departed nodes and priced for the new storage draw. +16 tests (1209 workspace). Also fixed doc drift: ROADMAP 5.11 and this table said `input_hash`, which became `step_hash` when the capability joined the preimage |


| #66 | 5.13c-1 | Peer head pins + fork detection (ADR-0041 amendment, Decisions 8–12). The item that starts turning 5.13a's chain into EVIDENCE: a node can rewrite its own DB and its chain still verifies — what it cannot do is un-tell a peer that already pinned `(seq, entry_hash)`. New `harness.audit` QUIC channel (const + `channels::known()` + a `peer_net` recv arm — registering the name without a handler accepts the stream and drops it at the `other =>` fallback, which is how a channel ships inert), `AuditSyncEnvelope` carrying signed `AuditHead`s, V0010 `audit_peer_heads` + `audit_head_conflicts`, and an `AuditSyncService` pushing our head every 30s with the newest third-party heads relayed alongside. FIVE design points, each one a defect avoided: (1) pins are APPEND-ONLY keyed `(node_id, seq)` — "higher seq replaces" deletes the pin the check needs, so a truncate-and-regrow reads as ordinary growth and the ingest of the lie erases the evidence of itself; the PK collision IS the fork detector, and keying on the hash too would store both histories as ordinary pins and detect nothing. (2) A lower-seq head is NOT evidence by itself — the envelope is not `Sequenced`, so any peer can rebroadcast a genuine old head forever and a node returning from a partition does it by accident; "lower = regression" would be a one-packet permanent defamation of an honest node. (3) NO heartbeat field: `Signable::canonical_bytes` re-encodes the DECODED struct, so an added field makes a pre-5.13c node drop the key, re-encode without it, and FAIL signature verification — the heartbeat channel dies and the peer ages out. That is a wire break, not the addition ADR-0019's `replica_head` was assumed to be; a new channel is genuinely additive (unknown name → reset, connection survives). (4) The inner verification key comes from our OWN trust store, never the relaying envelope — a relayer that supplied the key would mint a keypair and forge the history it claims to report; an unkeyable node's head is dropped, because an unverifiable accusation is worse than none. (5) Thinning is status-aware and buckets on `first_seen_ms`: a generic age sweep would eventually evict a `contradicted` pin and reach (1)'s failure by the back door, and bucketing on `seq` would let a flooder push honest history into the tail. +15 tests (10 store, 5 two-daemon money tests over loopback QUIC incl. m03: A rewrites, A's own chain still reports `Verified`, B catches the fork with both signatures retained; 1261 workspace). HONEST LIMIT, in the ADR: pins alone catch a same-seq fork, NOT truncate-and-regrow-past-the-pin — that needs the entry walk 5.13c-2 adds, so every pin sits at `unchecked` until then and the UI must not render that as corroboration |
| #65 | 5.13b | History UI (PRD §18.6) + the denial rate limit ADR-0041 deferred here. `/history` renders the §10.6 log with the chain banner ABOVE the table, because a hash chain nobody can see verified is decoration: the banner reports exactly what the server checked (`verificationSummary` never upgrades `checked: false` to "verified"), and a `broken` verdict names the seq. Filters cover all four §18.6 axes but not by one mechanism: `action` and `node` narrow the QUERY (the endpoint indexes them), `actor` prefix and the half-open time window filter the fetched page client-side, and the footer says which count is which. Notable rows (cloud.escalated, shell.denied, secret.accessed) are highlighted and chipped — §18.6 asks for cloud escalations, but what an auditor scans for is the privileged few: work leaving the LAN, a policy refusal, a secret read. Export ships what is ON SCREEN and is named for it. Paging pushes the full compound `(at_ms, node, seq)` cursor 5.13a's diff review established. STORE SIDE: `shell.denied` is attacker-triggerable at submit rate (`rate_limit` is declared in manifests and enforced nowhere), so an unbounded append lets a peer push genuine entries out of the 100k retention window one denial at a time; `StoreAuditSink` now rate-limits floodable actions in a 60s window keyed `(action, actor)`. Within a window the first `BURST_ALLOWANCE` (10) records append IN FULL — distinct denials keep their own argv, reason and task id at any rate a person produces — and past it a record is dropped and counted with a bounded sample of the distinct subjects dropped; when the window CLOSES a summary entry carries the counts, the sample and the window's own start. The count cannot bump the row it repeats (already hashed into the chain), and closing runs on the housekeeping tick as well as inline. Review found the first draft of this mechanism was WORSE THAN NOTHING and it was rebuilt before merge — see the two review rounds below. Re-review then found the summary's own detail was the next erasure path — sampled subjects come from adversary-chosen commands, a control character escapes to six JSON bytes, and eight such samples blow `MAX_AUDIT_DETAIL_BYTES`, at which point `audit_append` drops the detail WHOLE and the count goes with it; control characters are now folded out and samples admitted against a byte budget so the counts are written first and cannot be crowded out. Round 2 also force-closed open windows on daemon shutdown (an operator restarting mid-flood is the expected reaction to a flood, and the periodic close skips an unexpired window), keyed `distinct_subjects` on the UNTRUNCATED subject (commands sharing a 128-byte prefix collapsed to `distinct: 1` with `capped: false` — a flag claiming exactness it lacked), and taught the History page to render a summary row as "shell DENIED · N suppressed / K distinct, from T" instead of a denial of nothing. Round 3 caught that the shutdown flush was wired to a branch the shutdown path aborts before reaching — `shutdown()` drains and aborts every spawned task synchronously before its first await, so the housekeeping task's `shutdown.changed()` arm is racy on a multi-thread runtime and UNREACHABLE on the current-thread one the daemon uses; the flush moved into `shutdown()` itself, pinned by a daemon test that drives the real run loop (it fails against the old wiring, 10 rows vs 11). Round 3 also found the encoded sample budget — the guard against the erasure rounds 1 and 2 both got wrong — was untested: deleting the loop left all eleven store tests green, because after control-folding those samples never reach the budget; `a09l` reaches it with backslashes, which folding does not touch and which cost two JSON bytes each. `sample_dropped` renamed `sample_dropped_for_size` (it counted byte rejections only, so a bare `0` beside `distinct_subjects: 64` read as "all 64 shown"). +23 tests (12 store, 10 UI, 1 daemon; 1246 workspace + 74 UI). CARRIED, found in round 4: the daemon handles SIGINT only — `run_until_signal` awaits `tokio::signal::ctrl_c()` and no SIGTERM handler exists — so `systemctl stop` / `docker stop`, the NORMAL production stop, kills the process without running `shutdown()`. That costs every shutdown-time behavior (peer connections, listeners, the broadcaster), not just this item's suppression flush, so it is its own item rather than a silent widening of this PR; the ADR residual and the `run_until_signal` doc-comment now say SIGINT only instead of claiming SIGINT/SIGTERM |
| #64 | 5.13a | Audit log core (ADR-0041): PRD §10.6's append-only, hash-chained log — V0009 `audit_log` keyed `(node_id, seq)`, ONE CHAIN PER NODE because a mesh-wide chain needs agreement on who appends next, i.e. consensus, which "no broker, no consensus" rules out (the cost, stated: no global order; the History view merges by `at_ms` and clock skew makes that approximate). `entry_hash` covers the entry's fields AND its position and predecessor, hashed as a JSON OBJECT — never `entry ‖ prev_hash`, the concatenation this repo already rejected in `step_hash`, with `subject`/`detail`/`actor` free-form enough to make it a live collision surface; `detail` hashed exactly as stored, never re-serialized. `AuditSink` trait in harness-core (the `ReplicaApplier` precedent) because harness-capabilities and harness-mesh are core-only by design and four of the eight record sites live there — sink rides `ExecutionContext` beside `frame_sink`; secret access is audited by wrapping `SecretsStore` (covering every present and future consumer, and the ACTUAL access site — `SecretAwareLiveSet` is a routing filter whose own docs say it is not a security boundary); peer approval by subscribing the existing `TrustEvent` broadcast. All eight §10.6 sites recorded: dispatch, shell allow AND deny, secret access, peer approval, policy load, cloud escalation, cancel, resume. Records carry identifiers, never payloads: `actor` is a CLOSED enum (a "session" actor would persist bearer-token material; a webhook actor naming the sender would replicate the user's phone number across the LAN — the defect 5.11 refused for `reply_to`), dispatch detail names the capability not the input (a webhook plan's input IS the user's SMS), shell records `argv_hash` not argv (which routinely carries credentials), cloud records the backend and triggers not the goal, secrets record the TAG. Retention prunes THROUGH an `audit.truncated` marker carrying `{through_seq, through_hash}` appended BEFORE the delete — the naive order leaves N+1 pointing at a deleted row, so every node that ever hit retention would show a permanent BROKEN banner and operators would learn to ignore the one signal that matters. `GET /api/v1/audit` pages by TIME (seq is per-node, meaningless once chains interleave) and verifies only the page it returns (an O(N) walk inside the store mutex per request would let any authenticated caller stall the 100ms dispatch poll). Signed `AuditHead` ships now so 5.13c is pure transport. ADR-0006's standing promise to introduce automerge here is RETIRED, not deferred: a CRDT sequence's defining property is convergent reordering of concurrent inserts, incompatible with a hash chain over positions, and per-node append-only logs have exactly one writer each. Post-open Codex round (2 P1 + 3 P2, all verified real, all fixed) and diff-review round (REVISE→fixed), which between them found that the chain did NOT yet keep its central promise. P1/BLOCKER: an unknown stored `action` fell back to TaskDispatched, so rewriting a genuine task.dispatched row's action to any unrecognized string rehashed under the ORIGINAL action, reproduced the hash, and displayed the forged action under a green banner — unknown actions now break verification. P1/BLOCKER: deleting the chain's PREFIX verified clean (the oldest surviving row supplied both its own prev_hash and the expected seq), so `DELETE WHERE seq <= 900` erased everything before an incriminating entry and still read "verified" — and prefix truncation preserves the head, so even 5.13c's head-pinning would not catch it; verification now ANCHORS (genesis links to the zero hash, or the predecessor exists, or a truncation marker accounts for the gap — the marker verify_rows never actually read). BLOCKER: the feed silently skipped entries sharing a millisecond (cursor was `at_ms` alone while the sink stamps ms and a fan-out appends in bursts) — an entire history older than a burst became unreachable; the cursor now carries the full (at_ms, node, seq) key. MAJORs: page verification walked to the HEAD (cheapest request = worst case, a session-reachable mutex stall), now bounded by ROW count — a seq-span bound would restore the full walk under an `?action=` filter; `verified: true` overstated a page-scoped check and claimed truth for pages with no local rows, now a `verification` block reporting scope/from/through and `checked: false` when nothing was checked; `audit_prune` had ZERO callers while the ADR described live retention — now wired to the hourly housekeeping tick at 100k entries/node; seven of eight record sites were untested, so a `CapturingAuditSink` plus tests now pin shell allow/deny (asserting argv and Authorization headers never appear), the secrets decorator (tag yes, value no), and dispatch (capability yes, input no); no golden hash vector existed despite the doc claiming one — added, because `serde_json/preserve_order` enabled anywhere in the workspace would silently reorder the preimage and stop every stored chain from verifying. Minors: one string form for AuditAction (serde `snake_case` vs the dotted `as_str` that is stored AND hashed would have made 5.13c's replicated rows re-verify wrong); provenance now rides the envelope so operator and webhook work stop being recorded as `system`; ADR corrected on the prune's transaction shape, on verification not being cached, on `peer.approved` being wired but DORMANT (nothing calls TrustStore::add in this build), and on argv_hash being a confirmation oracle for low-entropy arguments once replication makes it LAN-visible. +24 tests (1233 workspace). HONEST LIMIT, in the ADR: a node holds its own DB and key and can rebuild its chain end to end — what it cannot do is un-tell a peer that already pinned `(seq, entry_hash)`, so until 5.13c replicates, this is a local integrity check, not evidence; and because policy is evaluated on the EXECUTING node, denials/secret reads/escalations land on the worker's chain |


| #56 | 5.5  | WhatsApp webhook (ADR-0033): root-path `POST /webhook/whatsapp` — fail-closed Twilio signature validation (`Base64(HMAC-SHA1(token, url+sorted params))`, vault token absent ⇒ 503, constant-time `verify_slice`, independent known-vector test; signed URL includes the query string; `HARNESS_WEBHOOK_BASE_URL` REQUIRED behind TLS termination); deny-all-by-default sender allowlist (BLOCKER-3: the signature authenticates TWILIO, not the sender — `HARNESS_WEBHOOK_ALLOW_FROM`, `*` opts into allow-all, WhatsApp senders match in full `whatsapp:+E164` form); the mint sequence extracted into ONE shared `mint_task` (clamp→build→sign→insert→replica-mirror; BLOCKER-1) used by submit handler + webhook; message Body → `brain.plan` (tags webhook+whatsapp, NO cloud_ok, constraint-smuggle pinned inert, 4.7 admission-subject) → TwiML ⏳ ack → detached store-polling driver (16 `OwnedSemaphorePermit`s, 600s deadline, CLI-parity envelopes) → `plan.execute` → Twilio Messages API reply (From=inbound To; missing SID ⇒ ack-only degraded mode). Tests ACT as the executor (BLOCKER-2: harness-api has no executor — legal state-chain walks + canned plan JSON + wiremock Twilio). MessageSid retry dedup (bounded 512 ring, same-ack) + result-row wait after terminal (the 5.3 executor gap) per Codex P1s; restart durability documented-deferred to 5.11. Deps: `hmac` is the only new lockfile entry (`sha1` already rides axum ws). +17 tests |

| #55 | 5.4  | NL Submit UI: Submit page reworked into "Describe it" (default) + "Advanced" (pre-5.4 form, behavior preserved; cold-load capabilities double-fetch fixed in passing) tabs; `$lib/nlsubmit.ts` — CLI-parity request bodies INCLUDING the execution envelope (plan review BLOCKER-1: bare submits get the 30s `ExecutionPolicy` default, which would have killed the 210s planner chain; brain.plan rides 245s, plan.execute 125s), tolerant `parsePlanOutcome`, typed `pollTask` (401 ⇒ AuthGate flip mid-poll, other non-OK ⇒ fail-fast `http_error`, AbortSignal, budget = envelope + 2×slack), `inputPreview` (`truncate_chars` parity); NL state machine with generation guards + `$effect`-owned AbortController teardown (the 4.8 stale-callback lesson); DAG preview reuses `DagView` with an empty steps map + confidence bar/cost/duration/rationale strip + per-step input previews; confirm resubmits the plan VERBATIM to `plan.execute` (server re-validates — 5.3 ruleset) and navigates to the live `/runs/:id` page; planning failures render the 5.3 per-tier diagnostics verbatim. `fallback_plan` deliberately not rendered. +13 vitest (49 UI total) |

| #54 | 5.3  | Validation ruleset + escalation triggers (ADR-0032): §15.4 rule 4 finally enforced — `validate_plan` gains `cloud_caps` (snapshot detects `CostHint::CloudPaid` / `"cloud"`-tagged caps) and rejects `must_be_local` plans naming one (`LocalityConflict`, checked first; foreign caps deferred to §10.4, `plan.execute` passes the empty set); typed `CloudTrigger` policy knob `escalate_to_cloud_if` (typos fail policy load; default ALL FOUR triggers — production local tiers only emit Confident/Err, so the PRD's two-string example would lock cloud out of every real failure mode and regress 5.2 reachability); executor rewrite (`walk_lineup`): per-tier attempt loops with validation-repair retries (`PlanRequest.repair` → 1KiB-capped repair prompt block; `max_replanning_attempts` default 2, cloud capped at 1 paid retry; low-confidence/NoMatch never retried), trigger-gated cloud (NoMatch deliberately NOT a trigger; cloud-as-baseline when no local LLM tier exists — nothing to escalate FROM), chain planning budget `Some(210_000)` from the daemon (attempts neither start past nor run beyond it via `tokio::time::timeout`; Template exempt — the §15.2 floor is now real, resolving the 5.1/5.2 carried risk). +14 tests |

| #52 | 5.1  | LocalStrong planner (ADR-0030): `LocalFastBackend`+`LocalStrongBackend` as newtypes over one `LocalLlmCore` (tier knobs only: `localfast:`/`localstrong:` ids, 30s/120s timeouts, 8/16KiB prompt caps; 3.9 wiremock suite covers the shared core unchanged); `classify_local_model` over VERBATIM Ollama tags (`:`+`-` tokenization, `<n>x<m>b` MoE multiply, decimals, quantized suffixes; ≥20B effective ⇒ Strong; sizeless ⇒ Fast); `resolve_local_models` partitions one `prefer_local_models` list into `[fast?, strong?, template]` (mixed-list behavior change documented: 3.9 bound the 70B to tier 1); CLI `--timeout-ms` wired through planning + default 180s (the 60s hardcode would have starved Template behind a 120s tier 2 — plan review MAJOR-2). Zero executor changes (three-tier walk test t29). +4 tests |


| #53 | 5.2  | Cloud planner (ADR-0031): `harness-brain::CloudBackend` (feature `cloud`) — Anthropic Messages API over the SAME planner pipeline as the local tiers (`llm_common.rs` pure-move extraction; NO sampling params — current models 400 them, diff review BLOCKER-1; 60s, 16KiB prompt, `max_tokens` 16k for thinking-budget headroom, id `cloud:<model>`); double-gated escalation — policy cap at registration (`allow_cloud_escalation` default-false + new `cloud_planner_model` knob, empty disables; requests can never resurrect an unregistered tier) AND per-task `cloud_ok` opt-in at the executor (tag or explicit `constraints.allow_cloud: true`; narrowing only) — plus the in-backend `!allow_cloud \|\| must_be_local ⇒ NoMatch` gate before any I/O; `local_only_for_tags` FIRST enforcement (§10.4, parsed since Phase 2 — tagged tasks plan `must_be_local`, force-to-true only); vault-free key handling (daemon closure → sensitive `HeaderValue` from borrowed bytes; missing key ⇒ `Internal` diagnostic naming the tag, chain degrades to Template); CLI plan budget 180s→240s (30+120+60+Template). Cloud cost enforcement stays nominal until 5.9 (self-reported `estimated_cost_usd`) — recorded in ADR. CLI `--cloud` opt-in flag (Codex P1: primary flows submitted bare goals, cloud unreachable). +13 tests |

| #51 | 4.8  | UI DAG viz + live progress: `GET /tasks` lists recent tasks across ALL states (limit clamp, `?state=` exact filter preserves the pre-4.8 view, 400 on unknown; additive `parent`/`plan_id` via `TaskRow`+`list_recent_tasks` over `idx_tasks_by_issued_at`); `WS /runs/:id` session-gated before upgrade (it serves task output — cookie rides the browser handshake, tests/CLI use bearer); plan.execute emits additive `in_flight` step frames at submit (settle frames only ever carried terminal states — the live DAG had nothing to light); UI: runs list rework (grouping, live badges), `/runs/[id]` (prerender=false) with WS live view + close-code-aware poll fallback, progress bar reduced from plan/mesh/federated frame families, log tail, `partials_dropped` banner, provenance table incl. Skipped; `$lib/dag.ts` pure Kahn layout (no new deps, cycle-defensive) + DagView SVG, arrows dependency→dependent mirroring the Rust orientation lock. +5 Rust tests, +12 vitest (35 UI total) |

**Carried (5.4): PRD §18.3 mode-1 planner-backend selector (auto/local-fast/local-strong/
cloud/template) — deferred, not dropped: `brain.plan`'s input schema is
`additionalProperties: false` with no tier-selection field; needs a schema extension
(natural home: 5.x backlog alongside escalation knobs).**

5.11 review round 1 (plan): REVISE, all findings verified against code and adopted
pre-merge — BLOCKERs: the input-only dedup key (the PRD's own phrasing) collides across
capabilities and collapses repeated-input steps within a single run, so the key became
(plan_id, node_id) with the hash as validation; GC on `status == "done"` would delete the
successful prefix of a fail-fast abort, so it now requires every step done. The webhook
half was cut to its own PR: a `reply_to` TAG would have exported the user's phone number
across the LAN in the signed envelope (tags ride to whichever peer executes an Anyone
task), there is no persisted brain.plan→plan.execute link so a boot re-attach would
double-execute, and nothing records "reply already sent" so a restart would re-send every
historical SMS. MAJORs: local-only checkpoints do not serve 5.12's cross-node handover
(named in the ADR, 5.12 must choose); §14.11's 100k-item framing is fan-out, not
plan.execute (capped at 64 steps); WITHOUT ROWID is wrong for a 256 KiB column. Minors:
a fully-checkpointed replay never reaches the completion arm, so cancel/budget/deadline
checks had to move into the settle path (e15 pins the cancel); `CheckpointStorage::None`
must not warn; `replayed` count added so a resume's $0.00 spend reads as replay, not free
work; boot age sweep for never-resumed plans.

5.13b review round 1 (diff) + Codex: REVISE → fixed. The finding that mattered: the
suppression window as first written was WORSE THAN NOTHING. It keyed on
`(action, subject, actor)`, and a denial's `subject` IS the command the peer submitted —
so `/bin/x1`, `/bin/x2`, … minted a fresh key per attempt and the flood passed untouched,
while the only records dropped were a real operator's repeated denials of ONE command.
The trade was exactly backwards, and three documents asserted otherwise. Rebuilt: the key
is `(action, actor)` (subject-free by construction, which also bounds the map in bytes —
the old key embedded a 64 KiB-capable command), with a `BURST_ALLOWANCE` of 10 full
appends per window so distinct denials keep their argv, reason and task id at any human
rate, and a bounded sample of dropped subjects in the summary. MAJORs: the 1024-key
eviction discarded exactly the pending counts, so a flooder could erase the record of its
own attempts — the ceiling now CLOSES windows (emitting summaries) instead of dropping
them; the count rode an arbitrarily-later entry and a burst that simply STOPPED was never
recorded at all (Codex P1 too) — summaries are now emitted on window close, inline and on
the housekeeping tick, carrying the window's own start; CSV export was a formula-injection
vector with a remote-controlled field (a peer plants `=cmd|'/c calc'!A1` by having it
DENIED, and quoting does not help because spreadsheets evaluate the unquoted cell) — cells
opening with a formula trigger are now apostrophe-prefixed; the pager lived inside the
empty-state branch, so a filter that emptied a page stranded the operator with no way back
(Codex P2 too); and the green banner claimed coverage the server had not given, since
`verify_page` walks a bounded ROW count up from the lowest local seq and can stop below
the newest row on screen — the banner now downgrades to amber and names the uncovered
range. Codex P2: a filter change while a fetch was in flight could let the older response
overwrite the newer — generation guard. Minors: a 503 showed the password gate forever;
the fold could destroy a non-JSON detail (that path is gone entirely — a summary is its
own record now); export named as if it were the whole log.

5.13a review round 1 (plan): REVISE, all adopted pre-implementation — BLOCKERs: (1) "one
`append` call per site" was unimplementable for half of them — harness-capabilities does not
depend on harness-store and four sites live there, so an `AuditSink` trait in core was
needed (the ReplicaApplier precedent); (2) the truncation marker as specified did not
verify — appending it at the head leaves N+1 pointing at a deleted row, a permanent BROKEN
banner on any node that ever hit retention; (3) `blake3(entry ‖ prev_hash)` reintroduced
the concatenation anti-pattern this repo documented in step_hash, and "canonical entry" was
undefined in three ways (position in the preimage? NULL vs ""? detail re-serialized?).
MAJORs: deferring replication guts the feature's meaning, so the ROADMAP item is SPLIT
5.13a/b/c rather than silently shipping half of an item whose title says "replicated", and
the signed head ships now; `actor` as free text would persist bearer-token material and
could reproduce 5.11's phone-number-across-the-LAN defect; per-request chain verification
is a session-reachable self-DoS against the dispatch poll; denial flooding can evict
evidence (rate_limit is declared in manifests but enforced nowhere). Minors: three of the
eight sites have NO logging today (net-new instrumentation, not mirroring a log line); the
secret-access site is SecretsStore::get, not the SecretAwareLiveSet routing filter; module
in harness-store, not a new crate; and ADR-0006's automerge promise had to be answered
rather than deferred a second time.

5.10 review round 1 (plan): REVISE, all adopted pre-implementation — BLOCKERs: (1) the
stop button as planned would not stop a runaway plan: drive_plan never reads its own task
row and feed_ready keeps minting Submitted children the dispatcher happily runs — cancel
would have flipped a row while spend continued; PlanExec::own_cancelled hook per step
completion, break like budget-Cancel. (2) The active-plans join premise was FALSE despite
the plan claiming it "verified": mint_task hardcodes plan_id None for every top-level row —
the join column the UI needed did not exist; stamped from input.plan.id at mint. MAJORs:
Claimed→Cancelled was NOT in the transition table (the plan hedged on a checkable fact);
cancel must terminalize live leases or late results write Done-over-Cancelled and gossip
it; executor discarded its CAS results and wrote results unconditionally (cancel would be
overwritten at completion); run-rate denominator switched to elapsed days (spend-days
projects 10x on sparse months). Minors: step-row cancel semantics documented as the
fail-fast lever; poll loop per-fetch AbortController + single-start guard (the 5.4
lesson, not the runs page's double-fire); 409-race = silent refetch.

5.9 review round 1 (plan): REVISE, all adopted pre-implementation — BLOCKER: "the
executor writes it" was false for the path the ledger needs: there are THREE done-row
write sites (executor, issuer-side ingest of remote results, federated parent) and the
ingest one — writing the coordinator's row, the one /costs reads — would have persisted
NULL for every remotely-executed cloud step; gate added there against the issuer's own
local manifest. MAJORs: the schema already carried dead tasks.cost_* columns since V0001
(documented as legacy, actuals live on task_results); longest-prefix-wins specified and
pinned (gpt-4o-mini is the OpenAI DEFAULT model — first-match would 16x it); the window
became time-bounded (30d + row-cap backstop, truncated echoed) instead of a silent
last-1000 that undercounts busy days; per-plan estimates cut (no plans table, plan.execute
rows carry plan_id: None — context parsed from bounded aggregates instead, budget object
included for 5.10). Minors: ADR-0036 failed-step fix promise retracted honestly; brain
estimated_cost_usd repricing dropped (category error); local-view corollary stated
(bystander /costs ≈ empty, per-issuer collapses onto the coordinator); ON CONFLICT retry
counts once.

5.8 review round 1 (plan): REVISE, all adopted pre-implementation — B1: the planned
Pause ("skip feed_ready") deadlocks: DagScheduler marks newly_ready InFlight internally, so
is_settled() never fires and stream.next() pends until the plan deadline, misreported as
deadline-exceeded; replaced with drop-the-sender → SourceDrained + skip_remaining, pinned
by a prompt-settle assertion. B2: "new terminal state strings" would have broken every
consumer hard-matching the closed TaskState set (CLI poll loop would never terminate) and
been a store/API wire change — status lives ONLY in the aggregate. M1: Cancel-as-Err would
discard the whole aggregate incl. budget figures — budget stops return Ok. M2: webhook
✅-reply used the plan's task count — now reads aggregate status/ok. M3: the
budget-as-approval opt-out is real; bounded by the planner-cannot-self-approve invariant
(pinned: deny_unknown_fields rejects a budget field in LLM responses; backends hardcode
None) + new plan_budget_ceiling_usd hard-caps even waivers; trust model stated in ADR.
M4: cost-then-fail steps contribute $0 (frozen by test; 5.9 result-row fix). M5: dead
projection machinery dropped (both real callers send no estimate). Minors: knobs in
[execution] not [planning] + TOML-cannot-None note; last-step/soft-only/waiver/failed-step
tests; mcp.proxy inflate-only vector noted; unscheduled ids for 5.12; #[non_exhaustive]
wildcard = Cancel+warn; orchestrator/cost split noted.

5.7 review round 1 (plan): REVISE, all adopted pre-implementation — BLOCKER: the
late-result GET as planned read the store, but `mint_task` writes no plan→exec linkage
and the reply string is driver-local (never persisted) — its own t04 could not pass;
replaced with the bounded in-memory `ShortcutsLedger`, which simultaneously fixed
MAJOR-4 (any valid token could probe ANY task's outcome — ledger membership is now the
authz boundary, non-ledger ids 404), MAJOR-5 (the dedup duplicate-response was a dead
end for the timed-out client — it now carries the original task_id), and MINOR-6
(shortcuts traffic sharing `SeenSids` could evict Twilio SIDs mid-retry-window).
MAJOR-2: `EncryptedStore` had NO write API (`write_encrypted` is pub(crate),
truncate-in-place) — added public atomic `upsert` (tmp+fsync+rename, file-values-only so
env overrides never bake into the file). MAJOR-3: daemon holds a startup vault snapshot —
first-use key generation now prints the restart-the-daemon note (env-var escape hatch
documented). MINORs: extraction signature carries channel+started (5.6 MAJOR-1 lesson),
permit-in-driver disconnect semantics spelled out, 90-day default TTL with explicit
--no-expiry, malformed vault key = 503 never panic, matrix additions (driver-cap busy row, GET
400/404, dedup-carries-task_id, standard-alphabet rejection). NITs: vault gains
base64+serde_json dep edges (recorded), sub length/charset caps, deny_unknown_fields=400,
TLS posture stated stricter than ADR-0033 (static bearer vs per-request signature).

5.6 review round 1 (plan): REVISE, all adopted pre-implementation — MAJOR: the driver's
second mint (plan.execute) had no channel-tag guard; a half-parameterized extraction
would ship SMS exec tasks tagged `whatsapp` with every test green — `Channel` threads the
driver and both suites assert tags on BOTH mints. MINORs: near-miss allowlist drop-log
hint (other-channel form listed); short ADR-0034 over mutating Accepted ADR-0033; UCS-2
segment-economics + Twilio-platform STOP/HELP sentences. NIT: cross-route dedup test
re-signs per URL and allowlists the bare sender (allowlist precedes the ring).

5.5 review round 2 (diff): APPROVE at head — mint extraction byte-preserving, signature
soundness (constant-time verified in vendored digest source; independent Python vector),
no secret leakage (reqwest basic_auth sets the header sensitive — verified in vendored
source), canned-plan fixture EMPIRICALLY deserialized against the real `Plan` serde,
env coupling harmless, docs exact. Adopted follow-ups: sid recorded only AFTER a
successful mint (a refused delivery stays retryable); t04 asserts the outbound basic-auth
header; NIT doc-comments (redaction-wall crossing; Twilio's separator-less HMAC concat
attacked and found unexploitable — ADR'd).

5.5 review round 1 (plan): REVISE, all adopted pre-implementation — BLOCKER-1: no
reusable mint path existed (submit_handler inlines auth+clamp+sign+insert+replica) —
extracted `mint_task`; BLOCKER-2: harness-api has no executor, so the planned
"stub lineup" driver test would hang — tests act as the executor via legal store
transitions; BLOCKER-3: default-allow senders made a valid Twilio signature a remote
NL-command surface (the signature authenticates Twilio, not the sender) — deny-all
default with an explicit `*` escape. MAJORs: store-polling driver (HTTP-to-self cannot
authenticate); outbound reply addresses = echoed inbound pair. MINORs: query string in
the signed URL + TLS-termination note; dep claim precision (sha1 already in the lock,
hmac the only new entry, reqwest a new edge); ApiState.secrets already existed;
webhook mints are admission-subject; semaphore permits ride the drivers with a 600s
deadline. NITs: pinned reply format; forwarded-to-brain equivalence recorded.

5.3 review round 2 (diff): APPROVE — every attacked invariant verified clean (retry
bounds, fresh repair threading, budget arithmetic, cloud-cap detection sweep across all
manifests, config wiring, byte-stable prompts, +14 count). One MINOR adopted: trigger
accounting is now explicitly Llm-tier-only (`fire_trigger`) so a cloud tier's own failure
can never open a later cloud tier (latent with today's single-cloud lineup); ADR-0032 §6
records it plus the budget-vs-policy skip-diagnostic caveat and the budget-skipped-tier
baseline reasoning. Ledger row order NIT fixed.

5.3 review round 1 (plan): REVISE, all adopted pre-implementation — MAJOR-1: the
PRD-verbatim two-trigger default would have locked cloud out of every failure mode
production local tiers actually produce (they emit only Confident/Err — never
NoMatch/MatchedButUnsupported), regressing 5.2 reachability and contradicting §15.3;
default became all four triggers with the deviation ADR'd, plus a reconciliation of
cloud-as-baseline with `escalation_only`. MAJOR-2: start-gating alone let a default
chain overrun the CLI window before Template ran — attempts are now clipped with
`tokio::time::timeout(budget − elapsed)`. MAJOR-3: `chain_budget_ms: u64` with
"0 = exhausted" collided with the derived `Default` (every test would have gone
Template-only) — `Option<u64>`, `None` = unbounded. MINORs: second `validate_plan`
call site (plan_exec) documented with an explicit empty set; 1KiB repair-text cap;
cloud retries capped at 1 (paid); typed `CloudTrigger` enum for loud-fail policy
parsing instead of warn-and-drop free strings. NITs: PlanRequest/CapabilitySnapshot
literal churn enumerated; new sequence-spy fixture for replanning tests.

5.2 review round 2 (diff): REVISE, all adopted — BLOCKER-1: hardcoded `temperature: 0.0`
would draw HTTP 400 from current Anthropic models (sampling params removed on the 5-family)
— every cloud attempt would have silently failed into Template, a fake-success tier; the
body now carries no sampling params and a wiremock matcher pins their absence. MINOR-2:
`max_tokens` 4096→16k (adaptive thinking shares the budget; truncated plan JSON ⇒ Decode).
MINOR-3: t01 now asserts the request body (model/max_tokens/messages + sampling-free).
Codex P1 (real): the CLI submitted bare goals so the cloud_ok gate made the tier
unreachable from `harness plan`/`exec` — `--cloud` flag added, translated to the explicit
constraint. Gating truth table, pure-move claim, key-handling redaction, feature matrix,
and validate_plan non-regression all verified clean by the reviewer.

5.2 review round 1 (plan): REVISE, all adopted pre-implementation — MAJOR-1: the plan
implemented only the policy half of PRD §15.2's "Cloud (if `cloud_ok`-tagged)" (once
`allow_cloud_escalation = true`, every request would have defaulted to cloud); the
executor now requires the per-task opt-in (tag or explicit constraint) on top of policy
approval. MINOR-2: `PlannerBackend: Debug` vs non-Debug closure → manual redacting impl.
MINOR-3: `local_only_for_tags` arrives via a `with_local_only_tags` builder (the 3-arg
`new` keeps its 10 test call sites). MINOR-4: the key provider returns a ready
`set_sensitive` `HeaderValue` instead of raw `Vec<u8>` — no unzeroized owned key crosses
crates. MINOR-5 + NIT-6/7: ADR names the unimplemented §15.2 planning-mode enum, the
truncation warn string went tier-neutral, and nominal cloud cost enforcement (until 5.9)
is recorded.

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
