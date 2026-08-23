//! `POST /webhook/whatsapp` — WhatsApp-via-Twilio adapter (5.5,
//! ADR-0033, PRD §20.2 + §26 walkthrough). The shared body lives in
//! [`super::conversation`] (channel-parameterized in 5.6).

use axum::{
    extract::State,
    http::{HeaderMap, Uri},
};

pub use super::conversation::{ACCOUNT_SID_TAG, AUTH_TOKEN_TAG};
use crate::state::ApiState;

pub async fn whatsapp_handler(
    State(state): State<ApiState>,
    uri: Uri,
    headers: HeaderMap,
    body: String,
) -> axum::response::Response {
    super::conversation::handle(super::conversation::WHATSAPP, &state, &uri, &headers, &body)
}
