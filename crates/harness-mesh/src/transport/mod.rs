//! QUIC transport for harness mesh nodes (item 1.4 — in progress).
//!
//! Phase 1.4 commit ordering:
//! 1. **`cert`** (shipped) — deterministic self-signed cert from `Identity`.
//! 2. **`verifier`** (this commit) — pinned-pubkey rustls
//!    `ServerCertVerifier` / `ClientCertVerifier`.
//! 3. `envelope` — length-prefixed framer + `Sequenced` trait + replay
//!    table.
//! 4. `quic` + `connection` — `Transport`, `Connection`, `IncomingConnection`.
//! 5. cancel-safety + property tests + tracing.
//!
//! See `phase-1.4-quic.plan.md` for the full design.

mod cert;
mod verifier;
