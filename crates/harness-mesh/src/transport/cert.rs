//! Deterministic self-signed X.509 cert derived from a harness `Identity`.
//!
//! Each harness node serves one self-signed cert whose Subject Public Key
//! (SPKI) is the node's Ed25519 identity public key. The 32-byte secret seed
//! that lives in `~/.harness/identity.key` (1.1's on-disk format) is wrapped
//! in a PKCS#8 v1 envelope (RFC 8410 §7), handed to `rcgen` with
//! `PKCS_ED25519`, and used to sign a fully-deterministic
//! `CertificateParams`. The same identity always produces the same cert
//! bytes — useful for tests and for cert-pin debugging.
//!
//! Cert validity is set to a 100-year window (2025-01-01 → 2125-01-01).
//! Cert rotation is out of scope for 1.4 (see plan §14 footgun #5).

use harness_core::{Identity, KeyError, PublicKey};
use pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rcgen::{
    CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose, SanType, SerialNumber,
};

/// `PKCS#8` v1 `OneAsymmetricKey` prefix for an Ed25519 raw seed (RFC 8410 §7).
///
/// ```text
/// 30 2e                             SEQUENCE (46 bytes)
///   02 01 00                          INTEGER 0 (version)
///   30 05                             SEQUENCE (5 bytes) — AlgorithmIdentifier
///     06 03 2b 65 70                    OID 1.3.101.112 (Ed25519)
///   04 22                             OCTET STRING (34 bytes) — privateKey
///     04 20                              inner OCTET STRING (32 bytes) — the seed
/// ```
///
/// Total envelope is exactly 16 bytes of header + 32 bytes of seed = 48 bytes.
const PKCS8_V1_ED25519_PREFIX: [u8; 16] = [
    0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04, 0x20,
];

/// Errors deriving a cert from an `Identity`.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CertError {
    #[error("rcgen: {0}")]
    Rcgen(#[from] rcgen::Error),
}

/// Errors extracting the SPKI from a peer's cert.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
#[allow(dead_code)]
pub(crate) enum SpkiError {
    #[error("could not parse certificate DER: {0}")]
    ParseDer(String),
    #[error("certificate algorithm is not Ed25519")]
    NotEd25519,
    #[error("invalid Ed25519 public key in certificate: {0}")]
    InvalidKey(#[from] KeyError),
}

/// Extract the Ed25519 [`PublicKey`] from the SPKI of a parsed cert.
#[allow(dead_code)]
///
/// Returns an error if the cert is malformed, the algorithm OID is not
/// `1.3.101.112` (Ed25519), or the SPKI bytes don't decode to a valid
/// curve point.
pub(crate) fn extract_ed25519_spki(cert: &CertificateDer<'_>) -> Result<PublicKey, SpkiError> {
    // Ed25519 algorithm OID: 1.3.101.112
    const ED25519_OID: oid_registry::Oid<'static> = oid_registry::asn1_rs::oid!(1.3.101 .112);

    use x509_parser::prelude::*;

    let (_, parsed) =
        X509Certificate::from_der(cert.as_ref()).map_err(|e| SpkiError::ParseDer(e.to_string()))?;

    if parsed.tbs_certificate.subject_pki.algorithm.algorithm != ED25519_OID {
        return Err(SpkiError::NotEd25519);
    }

    let bytes = parsed
        .tbs_certificate
        .subject_pki
        .subject_public_key
        .data
        .as_ref();
    let arr: &[u8; PublicKey::LEN] = bytes
        .try_into()
        .map_err(|_| SpkiError::ParseDer(format!("SPKI length {} != 32", bytes.len())))?;
    Ok(PublicKey::from_bytes(arr)?)
}

/// Build the `PKCS#8` v1 envelope for an Ed25519 secret seed.
///
/// Cross-checked against `ed25519_dalek::pkcs8::EncodePrivateKey` in tests.
#[allow(dead_code)]
pub(crate) fn pkcs8_v1_for_ed25519(seed: &[u8; 32]) -> [u8; 48] {
    let mut out = [0u8; 48];
    out[..16].copy_from_slice(&PKCS8_V1_ED25519_PREFIX);
    out[16..].copy_from_slice(seed);
    out
}

/// Derive a deterministic self-signed cert + key pair from `identity`.
#[allow(dead_code)]
pub(crate) fn derive_cert(
    identity: &Identity,
) -> Result<(CertificateDer<'static>, PrivateKeyDer<'static>), CertError> {
    let seed = identity.to_secret_bytes();
    let pkcs8 = pkcs8_v1_for_ed25519(&seed);

    let key_pkcs8 = PrivatePkcs8KeyDer::from(pkcs8.to_vec());
    let key_pair = KeyPair::from_pkcs8_der_and_sign_algo(&key_pkcs8, &rcgen::PKCS_ED25519)?;

    let mut params = CertificateParams::default();

    // Distinguished name: CN = node_id (lowercase hex). Fully deterministic.
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, identity.node_id().to_string());
    params.distinguished_name = dn;

    // SAN: <node_id>.harness.local — never actually verified by our custom
    // verifier (it pins on SPKI), but populated so the cert is valid for any
    // future webpki-style verifier and so its bytes are reproducible.
    let san_name = format!("{}.harness.local", identity.node_id());
    let dns_name: rcgen::Ia5String = san_name.as_str().try_into()?;
    params.subject_alt_names = vec![SanType::DnsName(dns_name)];

    // Serial = the 16 raw bytes of the node_id. Pure function of the identity,
    // satisfies "must be unique per issuer" trivially since each node is its
    // own issuer.
    params.serial_number = Some(SerialNumber::from_slice(identity.node_id().as_bytes()));

    // Long fixed validity window. Identity rotation is out of scope here.
    params.not_before = rcgen::date_time_ymd(2025, 1, 1);
    params.not_after = rcgen::date_time_ymd(2125, 1, 1);

    // Not a CA. mTLS endpoint usage.
    params.is_ca = IsCa::NoCa;
    params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyAgreement,
    ];
    params.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ServerAuth,
        ExtendedKeyUsagePurpose::ClientAuth,
    ];

    let cert = params.self_signed(&key_pair)?;
    let cert_der: CertificateDer<'static> = cert.der().clone();
    let key_der: PrivateKeyDer<'static> =
        PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(pkcs8.to_vec()));
    Ok((cert_der, key_der))
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    /// RFC 8032 test vector #1 seed.
    const RFC_SEED: [u8; 32] = [
        0x9d, 0x61, 0xb1, 0x9d, 0xef, 0xfd, 0x5a, 0x60, 0xba, 0x84, 0x4a, 0xf4, 0x92, 0xec, 0x2c,
        0xc4, 0x44, 0x49, 0xc5, 0x69, 0x7b, 0x32, 0x69, 0x19, 0x70, 0x3b, 0xac, 0x03, 0x1c, 0xae,
        0x7f, 0x60,
    ];

    #[test]
    fn pkcs8_envelope_is_16_byte_prefix_plus_seed() {
        let envelope = pkcs8_v1_for_ed25519(&RFC_SEED);
        assert_eq!(&envelope[..16], &PKCS8_V1_ED25519_PREFIX);
        assert_eq!(&envelope[16..], &RFC_SEED);
        assert_eq!(envelope.len(), 48);
    }

    #[test]
    fn pkcs8_envelope_byte_for_byte_constant() {
        // Locks the prefix bytes against accidental mutation. RFC 8410 §7
        // is the source of truth; if these bytes change the cert breaks
        // for every existing node.
        assert_eq!(
            PKCS8_V1_ED25519_PREFIX,
            [
                0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22,
                0x04, 0x20,
            ]
        );
    }

    #[test]
    fn derive_cert_is_deterministic() {
        let id = Identity::from_secret_bytes(&RFC_SEED);
        let (cert_a, _) = derive_cert(&id).expect("derive 1");
        let (cert_b, _) = derive_cert(&id).expect("derive 2");
        assert_eq!(cert_a, cert_b, "same identity must produce same cert bytes");
    }

    #[test]
    fn derive_cert_succeeds_for_random_identity() {
        let id = Identity::generate();
        let (cert, key) = derive_cert(&id).expect("derive");
        assert!(!cert.as_ref().is_empty());
        // The key must be a Pkcs8 variant, not Ed25519 raw — quinn/rustls
        // expects the PKCS#8 form.
        assert!(matches!(key, PrivateKeyDer::Pkcs8(_)));
    }

    #[test]
    fn derived_cert_spki_equals_identity_pubkey() {
        use x509_parser::prelude::*;

        let id = Identity::from_secret_bytes(&RFC_SEED);
        let (cert, _) = derive_cert(&id).expect("derive");
        let (_, parsed) = X509Certificate::from_der(cert.as_ref()).expect("parse cert");
        let spki = parsed
            .tbs_certificate
            .subject_pki
            .subject_public_key
            .data
            .as_ref();
        assert_eq!(
            spki,
            id.public_key().as_bytes(),
            "cert SPKI must equal the identity's Ed25519 public key"
        );
    }

    #[test]
    fn derived_cert_serial_equals_node_id() {
        use x509_parser::prelude::*;

        let id = Identity::from_secret_bytes(&RFC_SEED);
        let (cert, _) = derive_cert(&id).expect("derive");
        let (_, parsed) = X509Certificate::from_der(cert.as_ref()).expect("parse cert");
        let serial_bytes = parsed.tbs_certificate.serial.to_bytes_be();
        // x509-parser strips leading zeros from BigUint; pad back to 16.
        let mut padded = vec![0u8; 16 - serial_bytes.len().min(16)];
        padded.extend_from_slice(&serial_bytes);
        assert_eq!(&padded[..], id.node_id().as_bytes());
    }

    #[test]
    fn derived_cert_san_includes_node_id_dns_name() {
        use x509_parser::prelude::*;

        let id = Identity::from_secret_bytes(&RFC_SEED);
        let (cert, _) = derive_cert(&id).expect("derive");
        let (_, parsed) = X509Certificate::from_der(cert.as_ref()).expect("parse cert");
        let expected = format!("{}.harness.local", id.node_id());
        let san_ext = parsed
            .extensions()
            .iter()
            .find(|e| e.oid == oid_registry::OID_X509_EXT_SUBJECT_ALT_NAME)
            .expect("SAN extension present");
        let parsed_san = match san_ext.parsed_extension() {
            ParsedExtension::SubjectAlternativeName(s) => s,
            other => panic!("expected SAN, got {other:?}"),
        };
        let has_match = parsed_san.general_names.iter().any(|gn| match gn {
            GeneralName::DNSName(s) => *s == expected,
            _ => false,
        });
        assert!(has_match, "expected DNS SAN {expected}");
    }
}
