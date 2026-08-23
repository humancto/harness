# ADR-0034 — SMS webhook adapter (5.6)

**Status:** Accepted (2026-08-23)
**Context:** Roadmap 5.6, PRD §20.2 ("SMS (Twilio)"). Direct sibling
of the 5.5 WhatsApp adapter (ADR-0033) — same provider, signature
scheme, security posture, and conversion contract.

## What ships

1. **Channel-generic core.** The 5.5 handler + execute-and-reply
   driver moved to `webhook/conversation.rs`, parameterized by a
   `Channel` (name = route suffix = task tag = log field);
   `whatsapp.rs` and the new `sms.rs` are thin delegates. The
   channel is threaded through the DRIVER too — both the
   `brain.plan` and `plan.execute` mints carry the channel tag
   (pinned by tag assertions on both rows in both suites).
2. **`POST /webhook/sms`.** Identical flow: fail-closed signature,
   deny-all allowlist, admission gate, shared `SeenSids` dedup ring
   (Twilio message SIDs are globally unique across channels), TwiML
   ack, bounded driver, Messages-API reply with echoed addressing.

## Decisions

- **One allowlist, channel-native entry forms.**
  `HARNESS_WEBHOOK_ALLOW_FROM` serves both channels; entries match
  exactly in each channel's native form — `whatsapp:+E164` for
  WhatsApp, bare `+E164` for SMS. A `whatsapp:+X` entry never admits
  SMS from `+X` (and vice versa): channels are distinct
  authorization surfaces, and SMS `From` is the more spoofable of
  the two. The drop log emits an explicit near-miss hint when the
  number is listed in the other channel's form. Per-channel env vars
  stay a future knob.
- **Same 1600-char reply cap.** Twilio's Message-resource body limit
  is 1600 for SMS exactly as for Twilio-WhatsApp; one cap, one fewer
  knob. Consequence recorded: the `✅`/`❌`/`⏳` reply glyphs force
  UCS-2 encoding on SMS (67-char segments instead of 153), so a long
  diagnostic reply costs more segments — acceptable for the
  deny-all, operator-talks-to-their-own-mesh posture; revisit if
  reply economics ever matter.
- **STOP/HELP opt-out** rests on Twilio's platform-level handling
  (default opt-out on long codes/toll-free; suppressed sends surface
  as the existing rejected-reply warn path). The adapter's only
  compliance code is a guard, not a replacement: inbound webhooks
  carrying `OptOutType` (Twilio's marker that its opt-out handling
  fired) are acked empty and never minted as goals — an allowlisted
  operator texting STOP must not launch a mesh task or trigger a
  reply Twilio would suppress.
- **No new machinery decisions** — everything else is recorded reuse
  of ADR-0033 (which gains a cross-reference).

## Production cutover delta vs ADR-0033

Same steps; the Twilio webhook for the SMS-capable number points at
`https://<public-host>/webhook/sms`, and the allowlist entry is the
bare `+E164` number.
