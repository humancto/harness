# ADR-0021 — Encrypted-at-rest vault + `requires_secrets`-aware routing; replication deferred to 6.5

**Status:** Accepted (2026-08-22)
**Context:** Roadmap item 3.6-encrypted ("Encrypted-at-rest + replicated
credentials per PRD §10.5; tag-aware dispatcher routing on
`requires_secrets`"). Builds on ADR-0012 (plaintext vault v1).
**Supersedes:** parts of ADR-0012 (the plaintext store is now a
migration source, no longer the daemon's runtime store).
**Superseded by:** —

## Decision

3.6-encrypted ships two of the three concerns PRD §10.5 names, and
explicitly defers the third:

1. **Encrypted at rest** — the daemon's credential store moves from
   `~/.harness/secrets.toml` (plaintext, ADR-0012) to
   `~/.harness/secrets.enc`: ChaCha20-Poly1305 AEAD over the same
   tag→value TOML map, key derived from the node identity secret.
2. **Tag-aware dispatcher routing** — a task whose capability declares
   `requires_secrets` routes only to nodes that advertise every
   required tag. `NodeManifest` gains a `secret_tags: Vec<String>`
   field (names only, never values).
3. **Replication — explicitly OUT of scope.** Cross-node credential
   replication ("Replicated encrypted across mesh", PRD §10.5) is a
   major security surface of its own: per-node re-encryption (each
   node has a different identity-derived key), an authorization model
   for *which* nodes may receive *which* tags, and revocation. Roadmap
   item **6.5** already exists for exactly this ("Encrypted secrets
   store `~/.harness/secrets.enc` (replicated, ...)"); it lands there.
   Shipping a hasty replication scheme here would violate the "no
   security regression / no half-implementations" bar.

## Key derivation — from the identity secret, no new key file

`harness_core::Identity` exposes its 32-byte Ed25519 seed via
`to_secret_bytes() -> Zeroizing<[u8; 32]>` (it exists for the on-disk
key file written by `harness-mesh::identity`). That is sufficient to
derive a vault key **without touching harness-core's Identity API**,
so the dedicated-random-key-file fallback considered in planning is
unnecessary. The derivation is:

```text
vault_key = blake3::derive_key("harness secrets v1", identity_seed)
```

`blake3::derive_key` is BLAKE3's KDF mode (domain-separated by the
context string), already in-tree — no `hkdf` dependency needed.
Properties:

- **Domain separation.** The context string ties the derived key to
  this exact use. The Ed25519 signing keypair is derived from the seed
  through SHA-512 inside `ed25519-dalek`; the vault key through BLAKE3
  `derive_key`. Neither output reveals anything about the other.
- **No new secret material on disk.** The vault key exists only in
  memory (zeroized on drop), derived at daemon startup from the
  identity file the node already protects at mode 0600.
- **Consequence, documented:** whoever holds `identity.key` can
  decrypt `secrets.enc`. This matches the v1 threat model (ADR-0012):
  both files sit in `~/.harness/` under the same owner-only
  permissions; the encryption defends against backup leakage, file
  sync services, repo-adjacent exfiltration of a *single* file, and
  casual reads — not against an attacker who already owns the home
  directory. PRD §10.5 mentions "identity + admin password"; admin
  password mixing is deferred to 6.5 alongside replication because the
  admin password is optional today (daemon boots without one) and a
  key that appears/disappears with `admin.toml` would corrupt-lock the
  vault. Recorded here as the 6.5 design input.

## Cipher and file format

- **Cipher:** ChaCha20-Poly1305 (RustCrypto `chacha20poly1305`, pure
  Rust, same ecosystem as the existing `ed25519-dalek`/RustCrypto
  stack). AEAD gives tamper detection for free; wrong key and modified
  ciphertext both fail the Poly1305 tag check.
- **Nonce:** 12 random bytes from `OsRng` per write. Writes are rare
  (migration, future CLI edits), so random-nonce collision risk is
  negligible.
- **Envelope** (`secrets.enc`, TOML — consistent with every other
  config file in `~/.harness/`):

  ```toml
  format_version = 1
  nonce = "<24 hex chars>"
  ciphertext = "<hex>"
  ```

  `format_version` is bound into the AEAD as associated data
  (`b"harness-secrets-enc-v1"`), so a downgrade edit of the envelope
  is detected as tampering. The plaintext inside is exactly the
  ADR-0012 TOML map (`"secret/tag" = "value"`), so future format
  evolution has one obvious `format_version = 2` seam.
- **Permissions:** `secrets.enc` is created 0600 and load refuses
  group/other bits, same as the plaintext store. Defense in depth —
  the ciphertext alone is useless without the identity key, but there
  is no reason to loosen the posture.

## Migration from plaintext

On daemon startup (`EncryptedStore::open_with_migration`):

- `secrets.enc` exists → load it. If a legacy `secrets.toml` is
  *also* present, warn the operator that it is ignored and should be
  deleted (we never delete user files ourselves).
- Only `secrets.toml` exists → load it (full ADR-0012 validation:
  0600, tag grammar), write `secrets.enc`, keep serving from memory,
  and warn the operator to delete the plaintext file.
- Neither exists → empty store, same "not configured" behavior as
  before.

All `SecretsStore` behavior is preserved: `get(tag)` still consults
the `HARNESS_SECRET_<UPPER_TAG>` env override first, `SecretValue`
redaction rules are unchanged, and every capability keeps talking to
the same trait object.

## `secret_tags` in the manifest — privacy consideration

Routing needs to know *whether* a node holds a tag, which is private
today. We add to `NodeManifest`:

```rust
#[serde(default, skip_serializing_if = "Vec::is_empty")]
pub secret_tags: Vec<String>,
```

- **Names only, never values.** Tags are non-sensitive identifiers
  (`secret/openai-api-key`); the grammar `secret/[a-z0-9-]+` makes it
  impossible to smuggle a value into a tag. What leaks to the mesh is
  "this node has an OpenAI key configured" — an availability fact
  peers can already infer by routing a task there and reading the
  `not configured` failure. The manifest is gossiped only to paired,
  trusted peers over the encrypted QUIC mesh.
- The advertised set is the union of vault file tags and
  `HARNESS_SECRET_*` env overrides, matching exactly what `get()`
  would resolve — so routing and execution agree.
- **Wire compat:** `serde(default)` + `skip_serializing_if` follows
  the `Capability::requires_secrets` precedent from ADR-0012: nodes
  with an empty vault emit byte-identical manifests (the wire-format
  snapshot test is unchanged), and old daemons decode new manifests
  losslessly. As with `requires_secrets`, a manifest that carries a
  *non-empty* `secret_tags` fails signature re-verification on
  pre-3.6-encrypted daemons (their canonical re-encoding drops the
  unknown field) — same accepted upgrade story as 3.6a: meshes using
  the feature need version parity, meshes not using it see identical
  bytes.

## Routing filter — in the daemon, via `LiveSet`

The filter lives in `harness-daemon` (`DispatchRuntime`), not in
`harness-orchestrator` — the orchestrator's routing API stays pure and
store-free. Instead of post-filtering the computed `DispatchPlan`
(which would interact badly with round-robin: the RR cursor could keep
electing a filtered-out node, burning eligibility-window retries), the
daemon wraps its `MeshLiveSet` in a `SecretAwareLiveSet` that treats a
node missing a required tag as not-eligible *before* candidate
selection. Consequences:

- Deterministic: with nodes A (has tag) and B (lacks it), A is chosen
  on the first poll, not after RR happens to rotate past B.
- The empty-set case flows through the *existing* undispatchable path
  (`DispatchError::NoEligibleNodes` → eligibility window → terminal
  `Failed("undispatchable: ...")`) with zero new error plumbing.
- Per-node check: **self** uses the local capability registry's
  `requires_secrets` against the live local vault (`SecretsStore::
  tags()`); **peers** use their stored manifest (`Store::
  load_manifest`) — the capability entry's `requires_secrets` checked
  against that same manifest's `secret_tags`.
- A peer whose manifest we do not hold (index warm-up race, unit-test
  fixtures) is **not** filtered — we cannot judge it, and pre-3.6
  behavior (route, let the worker fail with `not configured`) is the
  correct conservative fallback for a routing *optimization*. This is
  routing, not policy: the executing node's policy engine remains the
  security boundary (PRD §10.4), so the permissive fallback cannot
  grant anything.
- Known limitation (mixed mesh): a pre-upgrade peer that *does* hold
  the credential advertises no `secret_tags` and will be skipped for
  secret-requiring capabilities until upgraded. Routing degrades
  toward nodes that provably have the tag; nothing breaks on the wire.

## `SecretsStore::tags()`

The trait gains `fn tags(&self) -> Vec<String>` with a default empty
implementation, so the existing test doubles in
`harness-capabilities/tests/` keep compiling. Both real stores
implement it (file tags ∪ env-override tags). Only tag *names* cross
this boundary; `SecretValue` redaction is untouched.

## What this item explicitly does NOT do

- Mesh replication of credentials (→ 6.5, see above).
- Admin-password key mixing (→ 6.5 design input, see above).
- `Action::Secret` policy gating (the evaluator still allows; the
  hook from ADR-0012 remains for a policy follow-up).
- Key rotation / re-encryption on identity change (a node with a new
  identity is a new node; its vault starts empty — same as 3.6a).
