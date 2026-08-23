//! `POST /webhook/sms` — SMS-via-Twilio adapter (5.6, ADR-0034,
//! PRD §20.2). Identical machinery to `WhatsApp` via
//! [`super::conversation`]; the channel deltas are the route, the
//! task tag (`sms`), and the sender allowlist form (BARE E.164 —
//! `+15551234567`, no `whatsapp:` prefix; entries are channel-native
//! and channels are distinct authorization surfaces).

use axum::{
    extract::State,
    http::{HeaderMap, Uri},
};

use crate::state::ApiState;

pub async fn sms_handler(
    State(state): State<ApiState>,
    uri: Uri,
    headers: HeaderMap,
    body: String,
) -> axum::response::Response {
    super::conversation::handle(super::conversation::SMS, &state, &uri, &headers, &body)
}
