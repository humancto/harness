# ADR-0018 — `mcp.proxy` via `rmcp` (3.7)

**Status:** Accepted (2026-08-22)
**Context:** Roadmap item 3.7 — PRD §16.4 (`mcp.proxy` exposes MCP-server tools as `mcp.<server>.<tool>` capabilities) with §21.7 sanctioning `rmcp` as the MCP SDK. The PRD leaves config shape, lifecycle, policy, and failure semantics open; this ADR pins them.
**Supersedes:** —
**Superseded by:** —

## 1. rmcp 0.11, not 2.x/3.x — pinned by MSRV

Latest stable rmcp is **3.1.4**, but it declares `rust-version = 1.88`, and every release since 0.12 depends on `process-wrap ^9` (MSRV 1.86+). The workspace toolchain is pinned at **1.85.0** (`rust-toolchain.toml`; the same constraint already pinned `wiremock = "=0.6.2"`). **rmcp 0.11.0** is the newest line that builds on 1.85 (`process-wrap ^8.2`, MSRV 1.77). The client API we use (`ServiceExt::serve`, `TokioChildProcess`, `list_all_tools`, `call_tool`, `CallToolRequestParam`, `CallToolResult`) is stable across 0.11 → 3.x modulo renames (`CallToolRequestParam` → `CallToolRequestParams`), so a future toolchain bump makes upgrading a contained change.

Two packaging quirks, documented in the workspace `Cargo.toml`:

- rmcp 0.11's *client-only* build is broken upstream (model code references `server`-gated modules), so we keep rmcp's default features (`server`, `macros`, `base64`) plus `client` + `transport-child-process`. The unused server half is build-time weight only.
- `schemars 1.x` comes in transitively (workspace elsewhere uses 0.8; the two majors coexist).

## 2. Config: `~/.harness/mcp.toml`, single file

PRD §16.4 sketches `~/.harness/mcp/*.toml` (a directory). We ship a **single `mcp.toml`** instead, matching every other operator file in this tree (`policy.toml`, `scopes.toml`, `secrets.toml`, one file each, all under `harness_root` so `--root` overrides work uniformly). A directory split buys nothing at "a handful of servers" cardinality and adds an enumeration-order question. If an operator ever needs includes, that is an additive change.

```toml
[[server]]
name    = "fs"                       # capability id segment: [a-z0-9_-]+
command = "npx"
args    = ["-y", "@modelcontextprotocol/server-filesystem", "/data"]

[server.env]                         # optional; overlaid on the daemon env
LOG_LEVEL = "warn"
```

Loading semantics mirror `scopes.toml` (3.10a): **missing file → info log, no MCP capabilities; parse/validation error → fatal at startup** (silently skipping a misconfigured integration is worse than refusing to boot). Validation: server names must match `[a-z0-9_-]+` (they become the dotted-id segment), be unique, and carry a non-empty command. `deny_unknown_fields` everywhere so typos fail loudly.

The child **inherits the daemon's environment** with `[server.env]` overlaid — unlike `shell.exec`'s allowlist-only env. Rationale: MCP servers are operator-pinned commands (not task-supplied), and in practice they need `PATH`, `HOME`, npm/node/uv config to even start. The task-facing surface (`tools/call` arguments) never touches the child env.

## 3. Placement: `harness-capabilities` behind an `mcp` feature

No `harness-mcp` crate exists in the workspace and PRD §22 does not reserve one, so the proxy lives in `crates/harness-capabilities/src/mcp/` behind a new **`mcp` feature**, mirroring `fs`: opt-in (NOT default — rmcp's transitive deps shouldn't tax minimal consumers), enabled explicitly by the daemon.

## 4. Capability mapping

Per discovered tool: id `mcp.<server>.<tool>`, **`Cardinality::Anyone`**, `input_schema` = the MCP tool's `inputSchema` verbatim (the schema hash tracks the server's own contract), `output_schema` = the tool's `outputSchema` when declared else the generic pass-through shape, `CostHint::LocalFast`, tag `mcp`, `NetworkClass::Light` (the proxied server may hit the network; we can't know).

*Why `Anyone`:* like `shell.exec`, the proxy is deny-by-default behind policy evaluated on the executing node, so mesh-wide advertisement grants nothing by itself; owner-scoped tools are the operator's `[mcp]` policy rules' job, not cardinality's. Documented on the type per repo rule 7.

Tool names are third-party data, so they are validated (`[A-Za-z0-9_.:-]+`, the same alphabet `llm.local.<model>` ids already use) and **skipped with a warning** — never a panic — when unusable or colliding; this deliberately diverges from the `expect`-on-duplicate discipline of `enrich_with_llm_local`, whose inputs are deduped locally.

**Output mapping:** the MCP `CallToolResult` is passed through verbatim as the capability output (camelCase wire shape: `content[]`, `structuredContent`, `isError`) — no lossy re-flattening; typed callers read `structuredContent`, text callers read `content[].text`. A result with `isError: true` and JSON-RPC/transport failures both map to `CapabilityError::Failed` carrying the server's message.

## 5. Lifecycle: one persistent child per server, no auto-restart

One subprocess + initialized rmcp client per `[[server]]`, held in an `Arc<McpServerHandle>` shared by that server's tool capabilities; the child lives exactly as long as the last capability Arc (rmcp's `TokioChildProcess` kills the child on transport drop — the same reap-on-drop discipline as `shell.exec`'s `kill_on_drop(true)`). Handshake + `tools/list` are bounded by a 15 s timeout.

Registration is **best-effort per server**: one server failing to spawn/initialize/list logs a warning and is skipped; the daemon boots with the rest. It runs as an async enricher in `lifecycle.rs` *before* `brain.plan` registration so the planner's capability snapshot sees MCP tools.

**Child death → calls fail loudly, no auto-restart in 3.7.** A dead transport yields a clear `MCP server "x" is not running` error on every subsequent call. Lazy restart-on-next-call was considered and deferred: silent respawn masks crash loops and discards in-server state without a trace, and restart semantics (backoff? re-list tools? schema drift mid-flight?) deserve their own item. Operator remedy today: restart the daemon.

## 6. Policy: `Action::Mcp { server, tool }`, default deny

New `Action::Mcp` variant (following how `Action::Llm` was added in ADR-0010), evaluated **on the executing node, inside `execute`, before the subprocess sees the call** — a policy deny never reaches the MCP server (tested by asserting the mock's call log stays empty). New `[mcp]` policy section:

```toml
[mcp]
allow = [{ server = "fs" }, { server = "gh", tool = "search_code" }]
deny  = [{ server = "fs", tool = "delete_file" }]

[mcp.from]
"laptop-guest" = "untrusted"
```

Semantics: trust short-circuit → deny pass → allow pass → **deny** (declaration order, first match wins; `tool` omitted = server-wide rule). Unlike `[llm]` (absent → allow, because local Ollama is expected to work unconfigured), `[mcp]` absent or empty → **deny-all, like shell**: MCP tools are arbitrary third-party code, so nothing runs until the operator opts in. Discovery (`initialize` + `tools/list`) is not a tool call and is not gated.

## 7. Testing

A dependency-free Python mock MCP server (stdio newline-delimited JSON-RPC: `initialize`, `tools/list`, `tools/call`; tools `add`/`boom`/`die` + an unregisterable name) is written to a tempdir per test. Covered: discovery + verbatim schema, call round-trip, `isError` mapping, dead-server behavior (kill mid-call, then clean failure after), policy deny blocking pre-subprocess (call-log assertion), invalid/duplicate/missing/broken config, unstartable server skipped non-fatally. Unix-gated like `shell.exec`'s tests; CI (ubuntu + macos) ships `python3`.
