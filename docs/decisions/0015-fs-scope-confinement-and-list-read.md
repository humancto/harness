# ADR-0015 — `fs.list` + `fs.read` with TOCTOU-free scope confinement

**Status:** Accepted (2026-05-04)
**Context:** Phase 3.10a of `HARNESS_PRD_v2.md` / `ROADMAP.md`. PRD §16.5 lists four scoped (Owner) filesystem capabilities — `fs.list`, `fs.read`, `fs.search`, `fs.write` — plus `fs.index`. ROADMAP item 3.10 groups list/read/search/grep into one bullet; this ADR records the design for the read-only half (3.10a). Index-backed `fs.search` + `fs.grep` ship as 3.10-fts.
**Supersedes:** —
**Superseded by:** —

## 1. Why split 3.10 into 3.10a + 3.10-fts

CLAUDE.md "no half-implementations": 3.10a delivers a complete bounded read surface — `fs.list` + `fs.read` work end-to-end with no index, and the path-confinement story is fully tested. 3.10-fts is mechanically a sibling layer (sqlite-FTS5 + 2 caps) on top of the scope registry from this PR. Splitting keeps each PR reviewable and the security review (path confinement) doesn't get diluted by FTS storage concerns. ADR-0012 sets the precedent for the same split rationale on `llm.cloud.{claude,openai,gemini}`.

## 2. `Owner` cardinality with no dispatcher routing yet

Both caps advertise `Cardinality::Owner { scope_field: "scope" }` so the manifest is forward-compatible with **3.3-fanout**, which will:

1. Extract `task.input.<scope_field>` at submit time.
2. Synthesize a `Constraints.pin_to_scope` constraint pointing at the node that owns that scope (per `NodeManifest::scopes`).
3. Route the dispatch.

Until 3.3-fanout lands, the dispatcher only honors `Anyone` and explicit constraints. There is **no** `harness submit --on <node>` flag (`harness run --on <node>` exists but `submit --on` does not). For 3.10a, end-to-end routing on a multi-node mesh requires `harness submit fs.list` to be invoked **on the node that owns the scope** — otherwise the executor lands on a node whose `ScopeRegistry::get(scope)` returns `None` and the call fails with `InvalidInput("unknown scope")`. Single-node demo gates work today.

Defense-in-depth: a unit test validates that `scope_field` ("scope") is in `input_schema.required` so a typo doesn't ship a manifest pointing at a missing field.

## 3. `~/.harness/scopes.toml` operator config

```toml
[[scope]]
id    = "documents"
kind  = "directory"
label = "Documents"
root  = "/Users/me/Documents"
```

`#[serde(deny_unknown_fields)]` everywhere; typos surface at parse. No `0600` permission enforcement (no secrets in the file; matches `policy.toml` precedent — `harness-vault::secrets.toml` is the only operator-config that enforces `0600`).

Future fields (e.g. `read_only: bool`, `index = { fts5 = true }` in 3.10-fts) land additively. **No live reload** — daemon restart picks up changes; matches `policy.toml`. 3.10-fts may swap `Arc<ScopeRegistry>` for `arc_swap::ArcSwap<ScopeRegistry>` if FTS-aware reload becomes useful.

## 4. Path confinement: `cap-std` `Dir` handle, NOT `canonicalize` + `starts_with`

The traditional pattern — `path.canonicalize()` then `canonical.starts_with(scope_root)` — has a real TOCTOU window. Between time T0 (canonicalize) and time T1 (`File::open`), an attacker who can write under the scope root can swap a file for a symlink to `/etc/passwd`; the kernel resolves at T1, not T0. This is CWE-367.

`cap_std::fs::Dir::open(rel_path)` resolves through one persistent directory handle with `O_NOFOLLOW`-equivalent semantics on every component. Symlinks under the scope are followed only if their target stays under the same `Dir` handle's tree — at the resolution step, not via a separate post-hoc check. No TOCTOU.

For `fs.read` we additionally **stat-first** via `Dir::metadata(path)` (uses `fstatat(2)`, no open) so FIFOs / device files / sockets are rejected without a blocking `open()` call (a FIFO with no writer blocks the calling thread until a writer connects, which would deadlock `tokio::time::timeout`).

Defense-in-depth: even with cap-std enforcing confinement, `parse_relative_path` rejects `Component::ParentDir` / `Component::RootDir` / `Component::Prefix` at parse time. Two reasons: (a) `InvalidInput("..")` is a clearer operator diagnostic than `PermissionDenied`; (b) if a future refactor accidentally bypasses cap-std on a path-handling helper, parse-time rejection limits the blast radius.

`Component::CurDir` is allowed at any depth (`./foo/./bar` parses fine — cap-std treats them as no-ops in resolution).

## 5. `fs.list` output shape decisions

- **Symlink `target` field omitted.** Returning `read_link()` would leak filesystem layout outside the scope as a recon vector. Symlink kind is observable (`kind: "symlink"`); target is not. Operators wanting to chase the symlink can `fs.read` it — cap-std confines that resolution.
- **Non-UTF-8 entry names: skip + `skipped_non_utf8: u32` counter.** A single non-UTF-8 filename shouldn't kill the entire enumeration. The counter is observable so operators know data was elided. Alternative (lossy U+FFFD substitution) was rejected because lossy names can't round-trip to a `fs.read` call.
- **Directory `size_bytes: null`** (not `0`). `0` would falsely imply "empty directory"; `null` honestly says "we don't compute this; use a recursive `du` if you need it."
- **Symlinks not followed during enumeration.** Loop risk + scope-leak. A symlink to a directory inside the scope is listed as `kind: "symlink"`; the listing does NOT recurse into it. Operators wanting to traverse a symlinked subdirectory can `fs.list` with that symlink's relative path explicitly.
- **`cap_std::fs::ReadDir` ordering is unspecified** (matches `std::fs::ReadDir` — platform-dependent). Tests that check the truncation flag assert set membership + flag, NOT positional order. Clients that need an ordering should sort post-hoc.

## 6. Hard caps: clamp, don't reject

`max_entries=10_000`, `max_depth=8`, `max_bytes=16 MiB`. User-supplied values **above** the cap are clamped, not rejected — the response carries `truncated: true`. Reasoning:

- Rejecting with `InvalidInput("too big")` leaks "the cap exists" to callers and forces them to retry with a guess.
- Clamping plus a flag lets a caller submit `max_bytes: 1_000_000_000` once, see `truncated: true`, and either accept the head or chunk via repeated bounded reads.
- The schema-level upper bound (`maximum: HARD_MAX_BYTES`) makes the cap discoverable in the manifest without callers having to ask.

## 7. No streaming in 3.10a

A 1 GB file requested via `fs.read` returns the truncated 16 MiB head with `truncated: true` and `size_bytes: 1_000_000_000`. The caller knows the size and can chunk via repeated reads — there's no `offset` parameter today, but adding one is additive. Streaming over QUIC is a 3.2-stream-style follow-up; the binary-large case for `fs.read` is rare enough that v1 doesn't need it.

## 8. Stat-first bounded `fs.read`

Prevents two failure modes:

- **OOM via large file**: `tokio::fs::read(path).await?` (returns `Vec<u8>`) on a 32 GB file allocates 32 GB before any cap kicks in. Stat-first + `take(HARD_MAX + 1)` allocation caps at `min(file_size, cap) + 1` bytes, regardless of the file's real size. `t30_read_oversize_file_does_not_overallocate` writes a 32 MiB file and asserts the response is capped at the 1 MiB default.
- **Block forever on FIFO**: opening a FIFO with no writer blocks the open syscall (sync) on the calling thread; `tokio::time::timeout` cannot interrupt it. `Dir::metadata(path)` is `fstatat(2)` and never blocks; with the `is_file()` gate, FIFOs are rejected before `open()` is called. `t28a_read_fifo_returns_invalid_input` proves it (with an outer 2s timeout that the test would never finish if the gate were missing).

## 9. Non-regular-file rejection

`fs.read` rejects directories, FIFOs, sockets, block devices, character devices, and "unknown" with `InvalidInput("not a regular file: <kind>")`. Without this gate:

- `fs.read` of `/dev/zero` would happily produce 16 MiB of zeros and "succeed" — a low-effort DoS / lease-burner.
- `fs.read` of a directory would return `EISDIR` from the read syscall — confusing.
- `fs.read` of a Unix socket would block on `read()` waiting for a writer.

The gate is at the cap-std level (`Dir::metadata` + `is_file()`); cap-std confines the metadata call too, so a symlink-to-/dev/zero from inside the scope is rejected at the metadata step (cap-std permission-denied) before ever getting to the `is_file` check. Belt and suspenders.

## 10. `encoding: "base64"` for binary content

Standard JSON binary carrier. `content` field stays a single string; an `encoding` discriminator picks UTF-8 (default) or base64 (opt-in). UTF-8 decode failure on `encoding=utf8` returns `InvalidInput("file is not UTF-8; request encoding=base64")` — a clear remediation breadcrumb.

Alternative `content_base64: Option<String>` (separate field) was rejected: it forces every consumer to check both fields, and forbidding `content` for binary changes the response shape per request — not a JSON-friendly pattern.

**Note on expansion:** a 16 MiB file requested with `encoding: "base64"` produces a ~21.3 MiB content string (4/3 expansion) plus JSON envelope overhead. We accept this for v1 — the bound is on disk read, not wire size. 3.10-fts may add a streaming variant; until then, operators with regularly-large binaries should chunk via repeated `max_bytes`-bounded reads.

## 11. `fs` feature is opt-in in `harness-capabilities`

`default = ["echo", "shell", "llm", "brain"]` (no `fs`). Daemon explicitly enables via `harness-capabilities = { workspace = true, features = ["fs"] }`. Reasoning: `cap-std` + `base64` + `toml` + `dirs` add transitive dep weight; minimal consumers (CLI, future embedded UIs) shouldn't pay for it. Same pattern as `llm` (`reqwest` opt-in) and `brain` (`harness-brain` opt-in).

## 12. `cost_hint: LocalFast` assumption

Operators point scopes at fast local storage. If measured P95 in the wild exceeds 100ms we revisit (split into `fs.read.large` for streaming, or relax to `LocalSlow`). For now, the assumption holds for SSDs and the typical "scope = local working directory" case.

**Note on base64 expansion:** documented in §10. The wire-size cost is the operator's lever, not the planner's hint.

## 13. Windows path semantics deferred to Phase 6.x

`cap-std` handles Windows correctly out of the box (drive letters, `\\?\` prefixes, case-folding via `Dir::metadata`). 3.10a runs on Unix in CI; tests that exercise FIFO / `/dev/zero` / non-UTF-8 paths are gated `#[cfg(unix)]`. Cross-platform smoke is acceptable for v1 — Phase 6.x cross-compile work picks up the slack.

## 14. `cap_std::ambient_authority()` is `fn`, not `const fn`

Used once per scope at `ScopeRegistry::load_*` time to open the initial `Dir`. Cannot be used in a `static`; if a future refactor wants `LazyLock<ScopeRegistry>` it must thread the authority through the closure body. Documented so 3.10-fts doesn't trip on it.

## Consequences

- The mesh has its first scoped (Owner) filesystem capability surface. Operators with `~/.harness/scopes.toml` see `documents` / `repos` / etc. in their `NodeManifest::scopes` advertisement (when manifest gossip lands; see §15.5 PRD).
- Path confinement is via cap-std at every step — no canonicalize/check/open TOCTOU window.
- 3.10-fts builds on this PR's `ScopeRegistry` to add sqlite-FTS5 indexing; the `fs.search` + `fs.grep` capabilities slot in via the same Owner cardinality.
- `Owner` routing is forward-compat for 3.3-fanout. Until then, operators must submit on the owning node — documented.

## Forward link

The next PR (3.10-fts) layers:

- `harness-capabilities/src/fs/index.rs` — sqlite-FTS5 index per scope; `~/.harness/<scope_id>.fts.db`.
- `fs.search { scope, query }` — full-text query over the index.
- `fs.grep { scope, pattern }` — regex match over indexed text.
- Index build on first search (`fs.index { scope }` is implicit) + explicit `harness fs reindex` CLI.
