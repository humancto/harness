//! Signed JSON tokens for the iOS Shortcuts adapter (5.7, ADR-0035,
//! PRD §20.2). Shortcuts has no provider signature (the phone talks
//! straight to the mesh), so the "signature" is a bearer credential
//! the operator mints: a compact JSON payload `MAC`ed with a vault-held
//! key.
//!
//! Wire form: `base64url(payload) "." base64url(mac)` where
//! `mac = blake3::keyed_hash(key, payload_bytes)`. Deliberately
//! NOT-JWT: one fixed MAC, no header, no `alg` field — the whole
//! algorithm-confusion surface is absent by construction. The
//! verifier MACs and parses the exact transported bytes, so there is
//! no JSON canonicalization round-trip to get wrong.
//!
//! Holding a validly-signed token IS the authorization; `sub` is an
//! audit label, not an allowlist key. Revocation = rotate the signing
//! key (which revokes every token at once — ADR-0035 records the
//! tradeoff and the default 90-day expiry that bounds it).

use serde::{Deserialize, Serialize};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;

/// Vault tag holding the 32-byte (64 hex chars) signing key.
pub const SHORTCUTS_KEY_TAG: &str = "secret/shortcuts-signing-key";

/// Hard cap on the token string before any decoding — a bearer header
/// should never be near this; anything larger is garbage or abuse.
pub const MAX_TOKEN_LEN: usize = 4096;

/// Payload `sub` label limits: 1..=64 printable ASCII. `sub` flows
/// into audit log lines, so control characters (log-line injection)
/// are rejected at mint AND verify.
pub const MAX_SUB_LEN: usize = 64;

const TOKEN_VERSION: u8 = 1;

/// The signed claims. Compact JSON on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenPayload {
    /// Format version — must be 1.
    pub v: u8,
    /// Audit label for the token holder (e.g. "archys-phone").
    pub sub: String,
    /// Issued-at, unix seconds.
    pub iat: u64,
    /// Expiry, unix seconds. `None` = no expiry (explicit CLI opt-in).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exp: Option<u64>,
}

/// Why a token failed to mint or verify. Variants are coarse on
/// purpose: the API layer logs the CLASS, never the token bytes.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TokenError {
    #[error("sub must be 1..={MAX_SUB_LEN} printable ASCII characters")]
    BadSub,
    #[error("token is malformed")]
    Malformed,
    #[error("token signature mismatch")]
    BadSignature,
    #[error("token expired")]
    Expired,
    #[error("unsupported token version")]
    BadVersion,
    #[error("signing key must be 64 hex characters (32 bytes)")]
    BadKey,
}

fn sub_ok(sub: &str) -> bool {
    !sub.is_empty() && sub.len() <= MAX_SUB_LEN && sub.bytes().all(|b| (0x20..=0x7e).contains(&b))
}

/// Parse the vault value (64 hex chars) into the raw signing key.
///
/// # Errors
/// [`TokenError::BadKey`] on wrong length or non-hex input — callers
/// on the serving path map this to fail-closed 503, never a panic.
pub fn parse_signing_key(hex_value: &str) -> Result<[u8; 32], TokenError> {
    let bytes = hex::decode(hex_value.trim()).map_err(|_| TokenError::BadKey)?;
    <[u8; 32]>::try_from(bytes.as_slice()).map_err(|_| TokenError::BadKey)
}

/// Mint a signed token.
///
/// # Errors
/// [`TokenError::BadSub`] when `sub` violates the label rules.
pub fn mint_token(
    key: &[u8; 32],
    sub: &str,
    iat: u64,
    exp: Option<u64>,
) -> Result<String, TokenError> {
    if !sub_ok(sub) {
        return Err(TokenError::BadSub);
    }
    let payload = TokenPayload {
        v: TOKEN_VERSION,
        sub: sub.to_string(),
        iat,
        exp,
    };
    // Serialization of this struct cannot fail (no maps, no non-string
    // keys); an error here would be a serde_json bug — map it to
    // Malformed rather than panicking on the serving path.
    let bytes = serde_json::to_vec(&payload).map_err(|_| TokenError::Malformed)?;
    let mac = blake3::keyed_hash(key, &bytes);
    Ok(format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(&bytes),
        URL_SAFE_NO_PAD.encode(mac.as_bytes())
    ))
}

/// Verify a token string against the signing key at time `now`
/// (unix seconds). Returns the payload on success.
///
/// The MAC comparison is constant-time (`blake3::Hash`'s `PartialEq`).
/// The payload is only parsed AFTER the MAC checks out.
///
/// # Errors
/// [`TokenError::Malformed`] on structural problems (length cap,
/// segment count, non-base64url, bad JSON), [`TokenError::BadSignature`]
/// on MAC mismatch, [`TokenError::BadVersion`] / [`TokenError::BadSub`] /
/// [`TokenError::Expired`] on claim failures.
pub fn verify_token(key: &[u8; 32], token: &str, now: u64) -> Result<TokenPayload, TokenError> {
    if token.is_empty() || token.len() > MAX_TOKEN_LEN {
        return Err(TokenError::Malformed);
    }
    let mut parts = token.split('.');
    let (Some(payload_b64), Some(mac_b64), None) = (parts.next(), parts.next(), parts.next())
    else {
        return Err(TokenError::Malformed);
    };
    // Strict URL_SAFE_NO_PAD: a standard-alphabet ('+'/'/') or padded
    // token is rejected, not normalized.
    let payload_bytes = URL_SAFE_NO_PAD
        .decode(payload_b64)
        .map_err(|_| TokenError::Malformed)?;
    let mac_bytes = URL_SAFE_NO_PAD
        .decode(mac_b64)
        .map_err(|_| TokenError::Malformed)?;
    let mac: [u8; 32] =
        <[u8; 32]>::try_from(mac_bytes.as_slice()).map_err(|_| TokenError::Malformed)?;
    let expected = blake3::keyed_hash(key, &payload_bytes);
    if expected != blake3::Hash::from(mac) {
        return Err(TokenError::BadSignature);
    }
    let payload: TokenPayload =
        serde_json::from_slice(&payload_bytes).map_err(|_| TokenError::Malformed)?;
    if payload.v != TOKEN_VERSION {
        return Err(TokenError::BadVersion);
    }
    if !sub_ok(&payload.sub) {
        return Err(TokenError::BadSub);
    }
    if let Some(exp) = payload.exp {
        if now >= exp {
            return Err(TokenError::Expired);
        }
    }
    Ok(payload)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    const KEY: [u8; 32] = [0x42; 32];
    const NOW: u64 = 1_756_000_000;

    #[test]
    fn t01_round_trip_verifies_and_surfaces_claims() {
        let tok = mint_token(&KEY, "archys-phone", NOW, Some(NOW + 3600)).expect("mint");
        let p = verify_token(&KEY, &tok, NOW).expect("verify");
        assert_eq!(p.sub, "archys-phone");
        assert_eq!(p.iat, NOW);
        assert_eq!(p.exp, Some(NOW + 3600));
    }

    #[test]
    fn t02_tampered_payload_or_mac_rejected() {
        let tok = mint_token(&KEY, "phone", NOW, None).expect("mint");
        let (payload, mac) = tok.split_once('.').expect("dot");
        // Re-encode a modified payload with the ORIGINAL mac.
        let mut bytes = URL_SAFE_NO_PAD.decode(payload).expect("b64");
        let pos = bytes.iter().position(|&b| b == b'p').expect("byte");
        bytes[pos] = b'q';
        let forged = format!("{}.{mac}", URL_SAFE_NO_PAD.encode(&bytes));
        assert_eq!(
            verify_token(&KEY, &forged, NOW),
            Err(TokenError::BadSignature)
        );
        // Flip a mac bit.
        let mut mac_bytes = URL_SAFE_NO_PAD.decode(mac).expect("b64");
        mac_bytes[0] ^= 1;
        let forged = format!("{payload}.{}", URL_SAFE_NO_PAD.encode(&mac_bytes));
        assert_eq!(
            verify_token(&KEY, &forged, NOW),
            Err(TokenError::BadSignature)
        );
    }

    #[test]
    fn t03_wrong_key_rejected() {
        let tok = mint_token(&KEY, "phone", NOW, None).expect("mint");
        assert_eq!(
            verify_token(&[0x43; 32], &tok, NOW),
            Err(TokenError::BadSignature)
        );
    }

    #[test]
    fn t04_expiry_enforced_and_open_ended_allowed() {
        let expired = mint_token(&KEY, "phone", NOW, Some(NOW + 10)).expect("mint");
        assert_eq!(
            verify_token(&KEY, &expired, NOW + 10),
            Err(TokenError::Expired)
        );
        assert!(verify_token(&KEY, &expired, NOW + 9).is_ok());
        let forever = mint_token(&KEY, "phone", NOW, None).expect("mint");
        assert!(verify_token(&KEY, &forever, NOW + 1_000_000_000).is_ok());
    }

    #[test]
    fn t05_structural_garbage_rejected() {
        for bad in [
            "",
            "notdotted",
            "a.b.c",
            "!!!.***",
            &"x".repeat(MAX_TOKEN_LEN + 1),
        ] {
            assert_eq!(
                verify_token(&KEY, bad, NOW),
                Err(TokenError::Malformed),
                "{bad:.20}"
            );
        }
    }

    #[test]
    fn t06_standard_alphabet_and_padding_rejected() {
        use base64::engine::general_purpose::{STANDARD, URL_SAFE};
        let tok = mint_token(&KEY, "phone", NOW, None).expect("mint");
        let (payload, mac) = tok.split_once('.').expect("dot");
        let bytes = URL_SAFE_NO_PAD.decode(payload).expect("b64");
        let std_form = format!("{}.{mac}", STANDARD.encode(&bytes));
        // A standard-alphabet re-encode either differs ('+'/'/'/'=' →
        // Malformed) or is byte-identical (then it MUST still verify —
        // same bytes). Assert it never becomes a distinct valid token.
        if std_form != tok {
            assert_eq!(
                verify_token(&KEY, &std_form, NOW),
                Err(TokenError::Malformed)
            );
        }
        let padded = format!("{}.{mac}", URL_SAFE.encode(&bytes));
        if padded != tok {
            assert_eq!(verify_token(&KEY, &padded, NOW), Err(TokenError::Malformed));
        }
    }

    #[test]
    fn t07_version_pinned() {
        // Hand-build a v2 payload signed with the real key: version
        // gate must reject even a validly-signed future format.
        let bytes = serde_json::to_vec(&serde_json::json!({
            "v": 2, "sub": "phone", "iat": NOW
        }))
        .expect("json");
        let mac = blake3::keyed_hash(&KEY, &bytes);
        let tok = format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(&bytes),
            URL_SAFE_NO_PAD.encode(mac.as_bytes())
        );
        assert_eq!(verify_token(&KEY, &tok, NOW), Err(TokenError::BadVersion));
    }

    #[test]
    fn t08_sub_rules_enforced_at_mint_and_verify() {
        assert_eq!(mint_token(&KEY, "", NOW, None), Err(TokenError::BadSub));
        assert_eq!(
            mint_token(&KEY, &"s".repeat(MAX_SUB_LEN + 1), NOW, None),
            Err(TokenError::BadSub)
        );
        assert_eq!(
            mint_token(&KEY, "line\nbreak", NOW, None),
            Err(TokenError::BadSub)
        );
        // Verify-side: hand-sign a payload with an injection sub.
        let bytes = serde_json::to_vec(&serde_json::json!({
            "v": 1, "sub": "a\nb", "iat": NOW
        }))
        .expect("json");
        let mac = blake3::keyed_hash(&KEY, &bytes);
        let tok = format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(&bytes),
            URL_SAFE_NO_PAD.encode(mac.as_bytes())
        );
        assert_eq!(verify_token(&KEY, &tok, NOW), Err(TokenError::BadSub));
    }

    #[test]
    fn t09_signing_key_parse() {
        assert!(parse_signing_key(&hex::encode([7u8; 32])).is_ok());
        assert_eq!(parse_signing_key("abc"), Err(TokenError::BadKey));
        assert_eq!(parse_signing_key(&"zz".repeat(32)), Err(TokenError::BadKey));
        assert_eq!(
            parse_signing_key(&hex::encode([7u8; 16])),
            Err(TokenError::BadKey)
        );
    }
}
