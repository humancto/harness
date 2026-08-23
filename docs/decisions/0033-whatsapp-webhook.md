# ADR-0033 — WhatsApp webhook adapter (5.5)

**Status:** Accepted (2026-08-23)
**Context:** Roadmap 5.5, PRD §20.2 (webhook adapters validate the
provider signature and convert to Task/Plan submission), §26
walkthrough. CLAUDE.md external-service rule: build the handler +
signature verification with MOCK signatures; production cutover is
operator work, never a blocker.

## What ships

1. **`POST /webhook/whatsapp`** at the root router (PRD path shape).
   Form-encoded Twilio webhook; the body is read raw so the SAME
   decoded pairs feed the signature check and the field extraction.

2. **Signature validation, fail-closed.**
   `X-Twilio-Signature = Base64(HMAC-SHA1(auth_token, url + params
   sorted by key))` — HMAC-SHA1 is Twilio's wire contract, not a
   choice; ours are the fail-closed posture (no
   `secret/twilio-auth-token` in the vault ⇒ 503, never an unsigned
   accept) and the constant-time comparison (`Mac::verify_slice`).
   The signed URL includes the query string. Behind a port forward /
   TLS terminator the public URL differs from what the daemon sees:
   `HARNESS_WEBHOOK_BASE_URL` is REQUIRED there; the Host-header
   fallback serves direct exposure and is correctness-fragile, never
   a security hole (forging still requires the token).

3. **Sender allowlist, deny-all default.** The signature
   authenticates Twilio, NOT the sender — Twilio validly signs a
   webhook for anyone who messages the bot number. Default posture
   is therefore deny-all: `HARNESS_WEBHOOK_ALLOW_FROM` unset/empty
   drops every message (log hint names the knob); explicit `*` opts
   into allow-all; otherwise exact-match on the full
   `whatsapp:+E164` form. Failure diagnostics in replies are
   acceptable for a self-hosted product GIVEN this default.

4. **Conversion.** The message `Body` is the NL goal, minted as a
   `brain.plan` task through the ONE shared mint path (extracted
   from the submit handler: clamp → build → sign → insert → replica
   mirror). Tags `webhook`+`whatsapp`; NO `cloud_ok` and no
   constraints — webhook text is the least-trusted input in the
   system, the 5.2/5.3 gates keep cloud shut, and the goal rides as
   a JSON string (cannot smuggle constraints; pinned by test).
   Root-level external input passes the 4.7 admission gate
   (in-channel "mesh busy"). PRD §20.2's "forwarded to brain" is
   realized by capability routing (`brain.plan` is
   Anyone-cardinality) — equivalent-or-better than literal
   forwarding, degrading gracefully when the receiver IS the brain.

5. **Execute-and-reply driver.** Per accepted message, a detached
   tokio task polls the STORE (HTTP-to-self cannot authenticate;
   the driver lives in the process that owns the store): brain.plan
   → mint `plan.execute` from the planned JSON (CLI-parity input
   envelope) → final reply through the Twilio Messages API
   (outbound `From` = inbound `To`, outbound `To` = inbound `From`
   — zero extra config; `secret/twilio-account-sid` missing ⇒ work
   still runs, reply skipped with a warn; the TwiML `⏳` ack already
   named the task id in-channel). Bounded: 16 `OwnedSemaphorePermit`s
   move into the drivers and release on drop; a 600 s overall
   deadline turns a wedged mesh into a "timed out" reply instead of
   16 bricked conversations. Reply format: `✅ done — N steps in Xs`
   / `❌ failed — <diagnostic>`, char-capped at WhatsApp's 1600
   (cost figures join when 5.9 lands).

6. **Delivery dedup + known restart limitation (Codex review).**
   Twilio retries deliveries when a response is lost; the handler
   dedups on `MessageSid` (bounded 512-entry in-memory ring —
   same-ack semantics for the retry, one task per message). The
   driver reads result rows through a bounded poll because task
   state flips terminal BEFORE the result row lands (the executor
   gap the 5.3 money tests hit). KNOWN LIMITATION, deliberately
   deferred: a daemon restart forgets both the dedup ring and any
   in-flight conversations — the minted tasks persist and are
   visible on the Runs page (the ack named the task id), but the
   final WhatsApp reply for a conversation caught mid-restart is
   never sent. Durable conversation state is 5.11 checkpoint-store
   territory; recorded here rather than half-built now.

## Dependencies (stopping-condition record)

Twilio's key‖value concatenation has no separators, so an on-path
attacker could re-split captured pairs without breaking the MAC —
attacked and found unexploitable here (re-splitting cannot forge an
allowlisted `From` nor alter `Body` bytes; worst case deletes a
field, which fails the allowlist or empties the goal). Inherited
from Twilio's scheme, not fixable on our side.

`hmac` is the only NEW lockfile entry. `sha1` was already in the
lock (axum's `ws` feature via tungstenite) — the direct dep adds no
new code to the tree. `base64`, `serde_urlencoded`, `reqwest` are
existing workspace deps gaining a harness-api edge. Pure-Rust
RustCrypto; the single-binary/no-broker property is untouched.

## Production cutover (operator steps — not code)

1. Twilio account + WhatsApp sender (sandbox or approved number).
2. Vault: `secret/twilio-auth-token`, `secret/twilio-account-sid`
   (`~/.harness/secrets.toml` / env fallbacks).
3. Bind beyond loopback (`harness up --bind 0.0.0.0:19198`) + router
   port forward; HTTPS termination in front.
4. `HARNESS_WEBHOOK_BASE_URL=https://<public-host>` (mandatory
   behind the terminator) and
   `HARNESS_WEBHOOK_ALLOW_FROM=whatsapp:+<your number>`.
5. Point the Twilio webhook at
   `https://<public-host>/webhook/whatsapp`.

## Not in 5.5 (deliberate)

- Meta-native WhatsApp signature variant (PRD lists Twilio/Meta —
  Twilio only here; the validation module is provider-shaped for the
  next adapter).
- SMS (5.6, reuses `twilio.rs` verbatim), iOS Shortcuts (5.7),
  Slack/Telegram/Email (backlog).
- Per-sender budgets/rate limits beyond the global driver cap and
  admission gate.
- Reply cost summary (5.9 cost tracking).

## Rejected

- Accepting signed webhooks from any sender by default: a phone
  number is not a credential.
- HTTP-to-self for the driver: no session exists and minting one
  would need the admin password at runtime.
- A per-adapter submission path: one mint fn, shared with the HTTP
  submit handler, or drift wins eventually.
