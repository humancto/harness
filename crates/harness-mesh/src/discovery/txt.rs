//! TXT record encoding for the `_harness._tcp.local` service.
//!
//! Four required fields, encoded as ASCII `key=value` strings inside the
//! TXT record (RFC 6763):
//!
//! | Key         | Value example                          | Validation             |
//! |-------------|----------------------------------------|------------------------|
//! | `node_id`   | 32 lowercase hex chars                 | `NodeId::from_str`     |
//! | `pubkey_fp` | 16 lowercase hex chars                 | length + hex           |
//! | `mesh_name` | UTF-8, `[a-zA-Z0-9_-]+`, 1..=63 bytes  | `validate_mesh_name`   |
//! | `version`   | `M.m.p`                                | three `u16` fields     |
//!
//! Encode emits keys in fixed order (`node_id`, `pubkey_fp`, `mesh_name`,
//! `version`) for reviewable packet diffs. Decode accepts any order and
//! ignores unknown keys (forward compatibility for future fields).

use std::collections::HashMap;
use std::str::FromStr;

use harness_core::{NodeId, ParseNodeIdError, SemVer};

/// Errors decoding a TXT record into a [`TxtPayload`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TxtError {
    #[error("missing required TXT key: {0}")]
    MissingKey(&'static str),
    #[error("duplicate TXT key: {0}")]
    DuplicateKey(String),
    #[error("TXT field {0} has invalid value")]
    BadField(&'static str),
    #[error("TXT contains non-UTF-8 bytes")]
    NotUtf8,
    #[error("TXT version is not a valid SemVer")]
    BadVersion,
    #[error("node_id parse: {0}")]
    NodeIdParse(#[from] ParseNodeIdError),
}

/// The 4 fields we encode/decode in the TXT record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxtPayload {
    pub node_id: NodeId,
    /// 16 lowercase hex chars (first 8 bytes of `blake3(pubkey)` in hex).
    pub pubkey_fp: String,
    pub mesh_name: String,
    pub version: SemVer,
}

/// Validate `mesh_name` per the rules in plan §4.2:
///
/// - 1..=63 bytes UTF-8.
/// - Only ASCII alphanumerics, `-`, `_`.
///
/// Rejecting weird unicode here saves debugging "why did my mesh
/// disappear when I renamed it" later. Mesh names appear in TXT records,
/// CLI output, the UI, and pairing flows; conservative is safer.
pub(super) fn validate_mesh_name(s: &str) -> Result<(), &'static str> {
    if s.is_empty() {
        return Err("mesh_name must be non-empty");
    }
    if s.len() > 63 {
        return Err("mesh_name must be ≤63 bytes");
    }
    if !s
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err("mesh_name must match [a-zA-Z0-9_-]+");
    }
    Ok(())
}

/// Validate `pubkey_fp`: exactly 16 lowercase hex chars.
pub(super) fn validate_pubkey_fp(s: &str) -> Result<(), &'static str> {
    if s.len() != 16 {
        return Err("pubkey_fp must be exactly 16 chars");
    }
    if !s
        .bytes()
        .all(|b| b.is_ascii_hexdigit() && (b.is_ascii_digit() || b.is_ascii_lowercase()))
    {
        return Err("pubkey_fp must be lowercase hex");
    }
    Ok(())
}

/// Encode a [`TxtPayload`] as the `(key, value)` pairs `mdns-sd` expects.
pub(super) fn encode(p: &TxtPayload) -> Vec<(String, String)> {
    vec![
        ("node_id".into(), p.node_id.to_string()),
        ("pubkey_fp".into(), p.pubkey_fp.clone()),
        ("mesh_name".into(), p.mesh_name.clone()),
        (
            "version".into(),
            format!(
                "{}.{}.{}",
                p.version.major, p.version.minor, p.version.patch
            ),
        ),
    ]
}

/// Decode `(key, value)` pairs into a [`TxtPayload`]. Order-independent.
/// Unknown keys are ignored for forward compatibility.
pub(super) fn decode<I, S>(pairs: I) -> Result<TxtPayload, TxtError>
where
    I: IntoIterator<Item = (S, S)>,
    S: AsRef<str>,
{
    let mut map: HashMap<String, String> = HashMap::new();
    for (k, v) in pairs {
        let key = k.as_ref().to_string();
        if map.insert(key.clone(), v.as_ref().to_string()).is_some() {
            return Err(TxtError::DuplicateKey(key));
        }
    }

    let node_id_str = map.get("node_id").ok_or(TxtError::MissingKey("node_id"))?;
    let node_id = NodeId::from_str(node_id_str)?;

    let pubkey_fp = map
        .get("pubkey_fp")
        .ok_or(TxtError::MissingKey("pubkey_fp"))?
        .clone();
    validate_pubkey_fp(&pubkey_fp).map_err(|_| TxtError::BadField("pubkey_fp"))?;

    let mesh_name = map
        .get("mesh_name")
        .ok_or(TxtError::MissingKey("mesh_name"))?
        .clone();
    validate_mesh_name(&mesh_name).map_err(|_| TxtError::BadField("mesh_name"))?;

    let version_str = map.get("version").ok_or(TxtError::MissingKey("version"))?;
    let version = parse_semver(version_str).ok_or(TxtError::BadVersion)?;

    Ok(TxtPayload {
        node_id,
        pubkey_fp,
        mesh_name,
        version,
    })
}

fn parse_semver(s: &str) -> Option<SemVer> {
    let parts: Vec<&str> = s.split('.').collect();
    if parts.len() != 3 {
        return None;
    }
    let major = parts[0].parse::<u16>().ok()?;
    let minor = parts[1].parse::<u16>().ok()?;
    let patch = parts[2].parse::<u16>().ok()?;
    Some(SemVer::new(major, minor, patch))
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use harness_core::Identity;

    fn sample() -> TxtPayload {
        let id = Identity::from_secret_bytes(&[0x42; 32]);
        TxtPayload {
            node_id: id.node_id(),
            pubkey_fp: id.public_key().fingerprint_hex(),
            mesh_name: "home".into(),
            version: SemVer::new(0, 1, 0),
        }
    }

    #[test]
    fn round_trip_preserves_fields() {
        let p = sample();
        let pairs = encode(&p);
        let back = decode(pairs).expect("decode");
        assert_eq!(p, back);
    }

    #[test]
    fn encode_emits_fixed_order() {
        let p = sample();
        let pairs = encode(&p);
        let keys: Vec<&str> = pairs.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, vec!["node_id", "pubkey_fp", "mesh_name", "version"]);
    }

    #[test]
    fn decode_accepts_arbitrary_order() {
        let pairs = vec![
            ("version".to_string(), "1.2.3".to_string()),
            ("mesh_name".to_string(), "team".to_string()),
            (
                "node_id".to_string(),
                "00112233445566778899aabbccddeeff".to_string(),
            ),
            ("pubkey_fp".to_string(), "0123456789abcdef".to_string()),
        ];
        let p = decode(pairs).expect("decode");
        assert_eq!(p.mesh_name, "team");
        assert_eq!(p.version, SemVer::new(1, 2, 3));
    }

    #[test]
    fn decode_ignores_unknown_keys() {
        let mut pairs = encode(&sample());
        pairs.push(("future_field".into(), "something".into()));
        let p = decode(pairs).expect("decode with extra");
        assert_eq!(p.mesh_name, "home");
    }

    #[test]
    fn decode_rejects_missing_required() {
        let pairs: Vec<(String, String)> = vec![
            ("mesh_name".into(), "home".into()),
            ("pubkey_fp".into(), "0123456789abcdef".into()),
            ("version".into(), "0.1.0".into()),
        ];
        let err = decode(pairs).expect_err("missing node_id");
        assert!(matches!(err, TxtError::MissingKey("node_id")));
    }

    #[test]
    fn decode_rejects_duplicate() {
        let pairs = vec![
            ("node_id".into(), "00".repeat(16)),
            ("node_id".into(), "11".repeat(16)),
        ];
        let err = decode(pairs).expect_err("dup");
        assert!(matches!(err, TxtError::DuplicateKey(_)));
    }

    #[test]
    fn validate_mesh_name_accepts_alnum_dash_underscore() {
        validate_mesh_name("home").expect("simple");
        validate_mesh_name("home-2").expect("dash");
        validate_mesh_name("home_2").expect("underscore");
        validate_mesh_name("a").expect("single char");
    }

    #[test]
    fn validate_mesh_name_rejects_dots_and_unicode() {
        validate_mesh_name("").expect_err("empty");
        validate_mesh_name("home.local").expect_err("dot");
        validate_mesh_name("home space").expect_err("space");
        validate_mesh_name("ho=me").expect_err("equals");
        validate_mesh_name(&"a".repeat(64)).expect_err("too long");
    }

    #[test]
    fn validate_pubkey_fp_strict() {
        validate_pubkey_fp("0123456789abcdef").expect("valid");
        validate_pubkey_fp("0123456789ABCDEF").expect_err("uppercase");
        validate_pubkey_fp("123").expect_err("short");
        validate_pubkey_fp("zzzzzzzzzzzzzzzz").expect_err("non-hex");
    }

    #[test]
    fn decode_rejects_bad_pubkey_fp() {
        let mut p = encode(&sample());
        // Find and replace pubkey_fp with junk.
        for entry in &mut p {
            if entry.0 == "pubkey_fp" {
                entry.1 = "INVALID".into();
            }
        }
        let err = decode(p).expect_err("bad fp");
        assert!(matches!(err, TxtError::BadField("pubkey_fp")));
    }

    #[test]
    fn decode_rejects_bad_version() {
        let mut p = encode(&sample());
        for entry in &mut p {
            if entry.0 == "version" {
                entry.1 = "not.a.version".into();
            }
        }
        let err = decode(p).expect_err("bad version");
        assert!(matches!(err, TxtError::BadVersion));
    }

    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 64,
            ..ProptestConfig::default()
        })]

        /// Property test: any valid `TxtPayload` round-trips through
        /// encode → decode. Generated identities use real seeds so
        /// `node_id` and `pubkey_fp` are always self-consistent.
        #[test]
        fn random_payload_round_trips(seed: [u8; 32], major: u16, minor: u16, patch: u16) {
            let id = Identity::from_secret_bytes(&seed);
            let p = TxtPayload {
                node_id: id.node_id(),
                pubkey_fp: id.public_key().fingerprint_hex(),
                mesh_name: "home".into(),
                version: SemVer::new(major, minor, patch),
            };
            let pairs = encode(&p);
            let back = decode(pairs).expect("decode");
            prop_assert_eq!(p, back);
        }

        /// Property test: decode never panics on adversarial input.
        /// Always returns an Err for malformed input; never UB.
        #[test]
        fn decode_never_panics_on_arbitrary_pairs(
            pairs in proptest::collection::vec(
                (any::<String>(), any::<String>()),
                0..16,
            ),
        ) {
            // Filter out non-printable / very-long strings that mdns-sd
            // would reject anyway — the property is about decode itself,
            // not about the upstream.
            let pairs: Vec<(String, String)> = pairs
                .into_iter()
                .filter(|(k, v)| k.len() < 128 && v.len() < 256)
                .collect();
            // Any result is fine; we just want no panic.
            let _ = decode(pairs);
        }
    }
}
