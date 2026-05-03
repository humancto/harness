# ADR-0007 — Admin authentication for the HTTP API

**Status:** Accepted
**Date:** 2026-05-03 (Phase 2.6)

## Context

Phase 1.10 shipped a read-only HTTP API at `127.0.0.1:19198` (peers, status, mesh events). Phase 2.6 introduces the first **mutating** endpoint: `POST /api/v1/tasks`. We need an authentication mechanism that:

- Works without an external IdP, OAuth, or a third-party service. Single-laptop developer setups are the primary use case.
- Survives a daemon restart (the password / hash lives on disk).
- Is usable from both a browser (cookie) and a CLI (bearer token).
- Cannot be bypassed by accident on the wire (`POST /api/v1/tasks` without auth must be a 401).
- Doesn't require schema additions to the `harness-mesh` trust model — admin auth is a _local-only_ property of the daemon, not a mesh-replicated concept.

## Decision

**Single admin password, hashed with `argon2id` (OWASP defaults), stored in `~/.harness/admin.toml`. Sessions are 32-byte random bearer tokens with a sliding 30-day TTL, kept in an in-memory `DashMap` (per-daemon).**

Wire model:

- `POST /api/v1/auth/login { password }` → `{ token, expires_at_ms }`. Sets a same-site=Lax cookie (`harness_session=<token>`) for browser flow; the bearer is also returned in JSON for the CLI.
- All mutating endpoints require `Authorization: Bearer <token>` OR a valid cookie. `POST /api/v1/auth/logout` invalidates.
- Read-only endpoints (`/health`, `/status`, `/peers`, `/events`) stay public on loopback. The user has access to the daemon already if they can reach localhost; admin-grade requires the password.

`~/.harness/admin.toml`:

```toml
format_version = 1
hash = "$argon2id$v=19$m=19456,t=2,p=1$<salt>$<hash>"
created_at = 1700000000   # unix seconds
```

Hash params follow OWASP 2024 cheat-sheet defaults (`m=19MiB`, `t=2`, `p=1`). Tunable via env in 6.x hardening.

First-run UX: `harness init` prints "Admin password not set. Run `harness admin set-password`" — the CLI subcommand prompts via `rpassword::prompt_password` (no echo), hashes, writes the toml. If `admin.toml` is missing, every mutating endpoint returns 503 with `error: admin_not_initialized`.

## Why not...?

- **No auth (loopback-only).** Insufficient — any local process can reach the API, including malicious npm packages. The user has been emphatic about production-grade.
- **mTLS client certs.** Overweight; the user installs harness via `curl|sh` and would need to manage cert provisioning. Admin-password is the table stakes.
- **OAuth / external IdP.** PRD §10 says "no external IdP." Closed.
- **Bearer with a long-lived secret in the env.** No rotation story; conflates "machine running CLI" identity with admin identity.
- **Per-mesh-peer signing key for admin.** Conflates the mesh trust model with admin-grade local access. Two different concepts; keeping them separate per PRD §10.

## Consequences

- 6.5 (encrypted secrets store) reuses argon2id for password-derived key wrapping.
- 5.5/5.6/5.7 (WhatsApp/SMS/iOS Shortcuts adapters) need separate signing-token mechanisms because they trigger task submission _without_ an admin login. PRD §12 already specifies signed-JWT pattern for those — different keys, different rotation.
- A user who forgets the password can `rm ~/.harness/admin.toml` and re-run `harness admin set-password`. Documented.
- DashMap session store is per-daemon; restart logs everyone out. Acceptable for v1; persist sessions to SQLite in a Phase 6 hardening pass if user complaints arise.
- 5.13 audit log captures every successful + failed login attempt with source IP.

## Alternatives considered

- **`bcrypt`.** Slower hash but lacks argon2id's resistance to GPU/ASIC cracking. Argon2id is the modern default.
- **`scrypt`.** Argon2id won the PHC 2015 competition; argon2id is the standard recommendation in 2024.
- **JWT with HMAC.** No revocation story without an in-memory blocklist, which is functionally identical to our DashMap. Skip the JWT machinery.
