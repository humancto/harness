//! Twilio webhook signature validation (5.5, ADR-0033).
//!
//! Twilio signs every webhook it delivers:
//! `X-Twilio-Signature = Base64(HMAC-SHA1(auth_token, url + params))`
//! where `url` is the webhook URL exactly as configured in the Twilio
//! console (scheme, host, path AND query string) and `params` is the
//! decoded POST form parameters sorted alphabetically by key, each
//! appended as `key` then `value` with no separators.
//!
//! HMAC-SHA1 is Twilio's wire contract, not our choice; what is ours:
//! fail-closed handling (no token configured ⇒ nothing validates) and
//! constant-time comparison (`Mac::verify_slice`).

use hmac::{Hmac, Mac};
use sha1::Sha1;

type HmacSha1 = Hmac<Sha1>;

/// Compute the expected signature for `url` + `params` (decoded form
/// pairs, any order — sorted here). Exposed for tests to mint valid
/// mock signatures with the same primitive (CLAUDE.md: external
/// services are tested with mock signatures, never live accounts).
#[must_use]
pub fn compute_twilio_signature(
    auth_token: &[u8],
    url: &str,
    params: &[(String, String)],
) -> String {
    let mut sorted: Vec<&(String, String)> = params.iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));

    // Length-mismatch on the token is a code bug surface, not runtime
    // input; HMAC accepts any key length so this cannot fail.
    #[allow(clippy::expect_used)]
    let mut mac = HmacSha1::new_from_slice(auth_token).expect("HMAC accepts any key length");
    mac.update(url.as_bytes());
    for (k, v) in sorted {
        mac.update(k.as_bytes());
        mac.update(v.as_bytes());
    }
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
}

/// Validate a received `X-Twilio-Signature` header value. Constant-time
/// on the MAC comparison; `false` on undecodable header input.
#[must_use]
pub fn validate_twilio_signature(
    auth_token: &[u8],
    url: &str,
    params: &[(String, String)],
    header_b64: &str,
) -> bool {
    use base64::Engine as _;
    let Ok(expected_tag) = base64::engine::general_purpose::STANDARD.decode(header_b64.trim())
    else {
        return false;
    };

    let mut sorted: Vec<&(String, String)> = params.iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));

    #[allow(clippy::expect_used)]
    let mut mac = HmacSha1::new_from_slice(auth_token).expect("HMAC accepts any key length");
    mac.update(url.as_bytes());
    for (k, v) in sorted {
        mac.update(k.as_bytes());
        mac.update(v.as_bytes());
    }
    // `verify_slice` is the RustCrypto constant-time comparison.
    mac.verify_slice(&expected_tag).is_ok()
}

/// Reconstruct the public URL Twilio signed. Production behind a port
/// forward / TLS terminator MUST set the base override
/// (`HARNESS_WEBHOOK_BASE_URL`, e.g. `https://home.example.com`) —
/// Twilio signs the URL as IT sees it, and a proxy rewrites
/// scheme/host. The fallback (`http://<Host><path?query>`) serves
/// direct-exposure and test setups; it is correctness-fragile, never
/// a security hole — forging still requires the auth token.
#[must_use]
pub fn public_url(base_override: Option<&str>, host_header: &str, path_and_query: &str) -> String {
    match base_override {
        Some(base) => format!("{}{}", base.trim_end_matches('/'), path_and_query),
        None => format!("http://{host_header}{path_and_query}"),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    fn pairs(raw: &[(&str, &str)]) -> Vec<(String, String)> {
        raw.iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    /// Known vector computed independently with Python's hmac/sha1
    /// over `url + "CalledStatus" + "completed" + "From" +
    /// "+14158675309"` (params sorted by key) with key `12345`.
    #[test]
    fn known_vector_round_trips() {
        let token = b"12345";
        let url = "https://mycompany.com/myapp.php?foo=1&bar=2";
        let params = pairs(&[("From", "+14158675309"), ("CalledStatus", "completed")]);
        let sig = compute_twilio_signature(token, url, &params);
        assert_eq!(sig, "kxbelq85ML/Ie3CDrsZ3tkc9D0Y=");
        assert!(validate_twilio_signature(token, url, &params, &sig));
        // Query string is part of the signed URL — dropping it fails.
        assert!(!validate_twilio_signature(
            token,
            "https://mycompany.com/myapp.php",
            &params,
            &sig
        ));
    }

    #[test]
    fn params_sorted_by_key_regardless_of_arrival_order() {
        let token = b"secret";
        let url = "https://h.example/webhook/whatsapp";
        let a = pairs(&[("Body", "hello world"), ("From", "whatsapp:+15551234567")]);
        let b = pairs(&[("From", "whatsapp:+15551234567"), ("Body", "hello world")]);
        assert_eq!(
            compute_twilio_signature(token, url, &a),
            compute_twilio_signature(token, url, &b)
        );
    }

    #[test]
    fn tampered_value_or_wrong_token_rejected() {
        let token = b"secret";
        let url = "https://h.example/webhook/whatsapp";
        let params = pairs(&[("Body", "run: ls"), ("From", "whatsapp:+15551234567")]);
        let sig = compute_twilio_signature(token, url, &params);

        let tampered = pairs(&[("Body", "run: rm -rf /"), ("From", "whatsapp:+15551234567")]);
        assert!(!validate_twilio_signature(token, url, &tampered, &sig));
        assert!(!validate_twilio_signature(b"other", url, &params, &sig));
        assert!(!validate_twilio_signature(
            token,
            url,
            &params,
            "!!not-base64!!"
        ));
        assert!(!validate_twilio_signature(token, url, &params, ""));
    }

    #[test]
    fn unicode_and_spaces_sign_over_decoded_values() {
        // Twilio signs DECODED values ("+" and %XX already resolved).
        let token = b"secret";
        let url = "https://h.example/webhook/whatsapp";
        let params = pairs(&[("Body", "héllo wörld ✓"), ("From", "whatsapp:+15551234567")]);
        let sig = compute_twilio_signature(token, url, &params);
        assert!(validate_twilio_signature(token, url, &params, &sig));
    }

    #[test]
    fn public_url_prefers_override_and_keeps_query() {
        assert_eq!(
            public_url(
                Some("https://home.example.com/"),
                "ignored:19198",
                "/webhook/whatsapp?x=1"
            ),
            "https://home.example.com/webhook/whatsapp?x=1"
        );
        assert_eq!(
            public_url(None, "harness.local:19198", "/webhook/whatsapp"),
            "http://harness.local:19198/webhook/whatsapp"
        );
    }
}
