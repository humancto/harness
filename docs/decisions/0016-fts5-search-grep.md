# ADR-0016 — `fs.search` + `fs.grep` via sqlite-FTS5 (3.10-fts)

**Status:** Accepted (2026-08-22)
**Context:** Phase 3.10-fts of `ROADMAP.md` — the second half of item 3.10 ("Tantivy or sqlite-FTS index"). Builds on ADR-0015's `ScopeRegistry` / cap-std confinement. Completes PRD §16.5's read-side filesystem surface (`fs.write` remains v2-backlog).
**Supersedes:** —
**Superseded by:** —

## 1. sqlite-FTS5 over Tantivy

ROADMAP offered both. FTS5 wins decisively for this product:

- **Zero new native dependency.** `rusqlite` with the `bundled` feature is already a workspace dependency (`harness-store`); the bundled SQLite is compiled with `SQLITE_ENABLE_FTS5`. A unit test (`fts5_module_is_compiled_into_bundled_sqlite`) asserts this loudly so a future rusqlite bump that drops the flag fails CI instead of failing at runtime.
- **Single-binary property preserved** (CLAUDE.md: "no broker, no DB server" is load-bearing). Tantivy would add ~40 direct+transitive crates and megabytes of binary for capabilities most nodes exercise lightly.
- **Good enough relevance.** FTS5 ships bm25 ranking and `snippet()` out of the box — exactly the `{path, score, snippet}` output shape we need. Tantivy's superior scaling (segment merges, faceting) buys nothing at the "one scope = one laptop's Documents folder" cardinality this feature targets.

If profiling ever shows FTS5 as the bottleneck on huge scopes, swapping the index behind `fs/index.rs` is a contained change — the capability I/O shapes don't encode FTS5 specifics (the MATCH query syntax is the one leak, documented in the input schema).

## 2. Sidecar DB per scope, not the main store

Index lives at `<harness_root>/index/<sanitized-id>-<blake3(id)/8>.fts.db`, not inside `harness-store`'s DB:

- The main store holds mesh state (tasks, peers, costs) with its own migration discipline; bulk-loading megabytes of file content into it would bloat backups and WAL churn for data that is *derived* and rebuildable.
- Per-scope files mean "delete the scope → delete one file", and index corruption is recoverable by deleting the sidecar (next `fs.search` lazily rebuilds).
- Scope ids are operator-chosen free text; the filename is `[A-Za-z0-9._-]`-sanitized plus 8 hex chars of `blake3(id)` so distinct ids can never collide post-sanitization.

`harness-capabilities` takes the `index_dir` as a constructor argument (daemon passes `<harness_root>/index`), keeping the crate free of daemon-config knowledge.

## 3. Index schema + incremental reindex

`files(path PRIMARY KEY, mtime_ns, size_bytes, fts_rowid)` is the bookkeeping table; `fts = fts5(path UNINDEXED, content, tokenize='unicode61')` holds the searchable text. One reindex pass = one bounded cap-std walk inside **one transaction**:

- unchanged file (same `mtime_ns`): untouched, not re-read;
- new/changed file: delete old FTS row (if any), insert, upsert bookkeeping;
- file gone from disk: FTS row + bookkeeping purged — **skipped when the walk was truncated**, so an elided subtree is never mistaken for mass deletion;
- crash mid-pass: transaction rolls back, previous index intact.

Staleness model: **the index is refreshed only when asked.** `reindex: true` on `fs.search` runs the incremental pass; a missing/empty index triggers a lazy first build regardless. We deliberately did NOT add mtime-watch daemons or TTL auto-refresh — v1 keeps freshness an explicit caller decision, and `reindexed`/`index_stats` in the output make it observable. Nanosecond mtime equality is the change detector; a same-mtime content rewrite (sub-ns clock granularity) is missed — acceptable for v1, same trade every mtime-based build tool makes.

Successful builds call `ScopeConfig::mark_indexed(now)` (new atomics on `ScopeConfig`), so `NodeManifest::scopes` now advertises honest `indexed` / `last_indexed` values — closing the 3.10a TODO in `manifest_scopes()`.

## 4. `fs.grep` needs no index

Grep is a *streaming* scan: bounded cap-std walk (shared `fs/walk.rs`) + `regex` crate line matching. Rationale: grep semantics (exact regex over current bytes) and index semantics (tokenized, possibly stale) diverge; forcing grep through FTS5 would make regex behavior a lie. The `regex` crate is linear-time (no catastrophic backtracking) and was already in the dependency tree transitively via `tracing-subscriber`, so the workspace dependency adds no weight.

`literal: true` escapes the pattern (`regex::escape`) for exact-substring mode; `ignore_case` maps to `RegexBuilder::case_insensitive`. `file_glob` is a hand-rolled glob→regex translation (`*` not crossing `/`, `**` crossing, `?` single char; glob without `/` matches basenames) rather than a `globset` dependency — ~30 lines, unit-tested.

## 5. Bounds (both capabilities)

| Bound | Value | Where |
|---|---|---|
| Walk depth | 32 | `walk.rs` |
| Walk file count | 100 000 | `walk.rs` |
| Grep: per-file scan cap | 8 MiB (larger skipped + counted) | `grep.rs` |
| Grep: match cap | default 100, hard 1 000 (clamped, ADR-0015 §6) | `grep.rs` |
| Grep: line length | 512 bytes, char-boundary truncation + `line_truncated` | `grep.rs` |
| Index: per-file cap | 4 MiB (larger skipped + counted) | `index.rs` |
| Search: hit limit | default 20, hard 100 (clamped) | `search.rs` |
| Binary detection | NUL byte in first 8 KiB → skip + count | `walk.rs` |

Every elision is observable: `truncated` flags plus `files_skipped_binary` / `files_skipped_too_large` / `index_stats` counters. Non-UTF-8 *content* in otherwise-text files is lossy-decoded (U+FFFD) rather than skipped — a stray byte must not hide a whole file from search.

## 6. Confinement + policy posture

Identical to ADR-0015: `Owner { scope_field: "scope" }` cardinality, all resolution through the scope's `cap_std::fs::Dir` (symlinks never followed during enumeration; a symlinked file's open re-confines), output paths always scope-relative, never absolute. Like `fs.list`/`fs.read`, there is no separate `PolicyEngine` action hook — the scope registry *is* the authorization surface for read-only fs capabilities, and policy remains evaluated on the executing node by construction (the registry lives there). The walk sorts directory entries for deterministic truncation behavior.

Both capabilities do their disk/SQLite work in `tokio::task::spawn_blocking` so executor worker threads stay responsive.

## 7. Scoring

`fs.search` reports `score = -bm25(fts)` so higher = better (bm25 returns lower-is-better negatives); hits are ordered best-first. FTS5 MATCH syntax errors (unbalanced quotes etc.) surface as `InvalidInput` with SQLite's diagnostic — the query language is FTS5's, documented in the input schema description.

## Consequences

- Item 3.10 is complete: `fs.list`, `fs.read`, `fs.search`, `fs.grep`, all Owner-cardinality, all cap-std-confined, all bounded.
- 3.11 (`mesh.search`/`mesh.grep`) can fan these out per-scope across nodes and merge — the per-node output shapes (relative paths + scores) were chosen to be merge-friendly.
- `~/.harness/index/` is a new on-disk artifact; docs/packaging (Phase 6) should mention it's safe to delete.
