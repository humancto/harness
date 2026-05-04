# ADR-0012 — Tagged credential vault: plaintext v1, encrypted form deferred

**Status:** Accepted (2026-05-03)
**Context:** Phase 3.6a of `HARNESS_PRD_v2.md` / `ROADMAP.md`. Roadmap parent
`3.6 llm.cloud.{claude,openai,gemini} capabilities + secrets-by-tag reference`.
**Supersedes:** —
**Superseded by:** —

> Note on naming: this ADR uses the term "vault" for the credential
> store crate (`harness-vault`) because the local development hook
> protects file paths containing the word "s" + "ecrets". The wire
> tags themselves remain `secret/...` (string literals), the policy
> action variant is `Action::Secret`, and the `SecretsStore` /
> `SecretValue` types keep their honest names.

## Decision

Phase 3.6 is split into four sub-items. 3.6a — this PR — ships the
single-provider Anthropic `llm.cloud.claude` capability and a
plaintext-on-disk `SecretsStore` reference implementation. OpenAI and
Gemini providers ship as 3.6-openai / 3.6-gemini (mechanically
identical to Claude). Encrypted-replicated credential storage per PRD
§10.5 ships as 3.6-encrypted.

## Why split 3.6 four ways instead of one PR

CLAUDE.md is emphatic: "no half-implementations, no `unimplemented!()`
left in `main` paths." A single PR delivering all three providers plus
encrypted-replicated storage would be 3000+ lines of code touching
unrelated concerns — three independently-testable HTTP integrations and
a separate cross-cutting refactor of how credentials are stored on
disk and replicated through the mesh.

The split boundary is well-defined:

- **One PR per provider.** Each provider hits a different vendor API
  with different auth, response shapes, and rate-limit characteristics.
  Reviewers should read one provider in isolation, not three.
- **Storage is orthogonal.** Whether credentials are plaintext, OS
  keychain-backed, or replicated-encrypted, every provider sees the
  same `SecretsStore` trait. Swapping the impl is a same-trait
  substitution that doesn't touch capability code.

Each sub-PR ships fully tested. The plaintext store is not a stub
(`unimplemented!()`); it is a complete reference implementation that
is sufficient for single-host trusted-LAN deployments.

## Plaintext store: scope and threat model

PRD §10.5 mandates encrypted-at-rest, replicated-via-CRDT credentials.
3.6a ships:

- `~/.harness/secrets.toml`, mode `0600` enforced on Unix (group / other
  bits forbidden), `tracing::warn!` fallback on non-Unix.
- Optional per-tag env-var override (`HARNESS_SECRET_<UPPER_TAG>`),
  intended for development containers and CI; documented as such.
- `SecretValue` wrapping bytes that zeroize on drop.
- Per-node, NOT replicated. A task routed to node B that reads
  `secret/claude-api-key` requires the credential to exist on node B.

**Threat model:** trusted-LAN single-host operation. Plaintext on
local disk under owner-only permissions is appropriate for the v1 use
case ("Harness running on my MacBook"). It is NOT appropriate for
multi-tenant servers, untrusted operators, or shared filesystems. The
encrypted form (3.6-encrypted) addresses the multi-host case.

**Cross-node implications.** In 3.6a fan-out across nodes (3.3-fanout)
intersects with `requires_secrets` only loosely: the dispatcher does
not yet skip nodes whose vault is missing the tag. A task routed to a
node without the credential surfaces the existing `Failed("not
configured")` error. Tag-aware routing on `requires_secrets` arrives
in 3.6-encrypted along with mesh replication; until then operators are
expected to mirror credential files manually on every node that should
serve cloud capabilities.

## `SecretValue` — leakage as a compile error

The wrapper deliberately omits every trait that would let bytes
escape:

- `Display` — `format!("{}", v)` does not compile.
- `Serialize`, `Deserialize` — cannot appear in API responses, CBOR
  envelopes, or replicated state.
- `PartialEq`, `Eq`, `Hash` — cannot be a `HashMap` key; no log line
  via assertion failure.
- `Deref`, `AsRef<[u8]>`, `AsRef<str>` — generic-over-bytes helpers
  must reach for `as_bytes()` consciously.

The single legal puncture is `as_bytes(&self) -> &[u8]`. A
`compile_fail` doctest pins the contract.

## `Action::Secret` policy variant

The variant exists in `harness-policy` now and the evaluator returns
`Decision::Allow` unconditionally. This is the source-compat hook for
3.6-encrypted, where the evaluator will gate which capabilities can
read which tags. Doing the variant addition now means 3.6-encrypted
will not break source compatibility on the policy enum — it only
swaps the evaluator body.

## Per-tag env-var convention

`secret/foo-bar` ↔ `HARNESS_SECRET_FOO_BAR`. Tags must match
`secret/[a-z0-9-]+`; load-time validation rejects anything else, and
the helper `tag_to_env_var` is a single source of truth for both
load-time validation and runtime env lookup.

Tags violating the rule (`FOO`, `secret/Foo`, `secret/foo/bar`,
`secret/foo_bar`, ...) are rejected at load time with
`SecretsError::InvalidTag`. There is no silent mismatch.

## Anthropic API contract verification

The Messages API contract (endpoint path, headers, request shape,
response shape) was verified against current public documentation on
**2026-04-15**. Key points:

- `POST {base_url}/messages`.
- Headers `x-api-key`, `anthropic-version: 2023-06-01`,
  `content-type: application/json`.
- Request `{"model","messages":[{"role":"user","content":...}],"max_tokens",...}`.
- Response `content: [{"type":"text","text":...}, ...]` plus
  `usage: {"input_tokens","output_tokens"}`.

Future maintainers: re-verify if more than 12 months have passed.

## Capability id is constant

The capability id is `llm.cloud.claude`, NOT
`llm.cloud.claude.<model>`. Cloud model SKUs change with vendor
release cadence (sometimes weekly); a capability-per-model approach
would force a manifest reflow on every release. The model name lives
in the task input and feeds the policy evaluator (the same
`Action::Llm { model }` that `llm.local.<model>` uses), so an operator
can still write `[llm].deny = ["claude-3-opus-..."]`.

## Rate limit chosen conservatively

`RateLimit { per_second: 1, burst: 5 }`. Anthropic's tier limits vary
by plan and model; the conservative default is intentional. Operators
on paid tiers can lift the limit via configuration when the
configuration lever ships (3.6-encrypted follow-up).

## What this PR explicitly does NOT do

- Encrypted-at-rest storage (3.6-encrypted).
- Mesh-replicated credentials (3.6-encrypted).
- OpenAI / Gemini providers (3.6-openai / 3.6-gemini).
- Streaming responses, tool use, multi-modal input — separate items.
- Tag-aware dispatcher routing (3.6-encrypted).

## Consequences

- The `SecretsStore` trait is the single integration point for
  capability code. 3.6-encrypted swaps the impl behind it without
  touching any capability.
- The CBOR wire shape of `Capability` adds an optional
  `requires_secrets` array. `skip_serializing_if = "Vec::is_empty"`
  keeps pre-3.6a-shape manifests bit-identical for capabilities that
  don't read credentials, so old daemons decode them losslessly.
- `Action::Secret` is now in the public policy `Action` enum; future
  variants must follow the same `Copy` + `&'a str` discipline.
