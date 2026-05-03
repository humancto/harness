//! Custom rustls verifier that pins on the peer's Ed25519 public key.
//!
//! Both sides of the mTLS handshake use this verifier. It bypasses webpki
//! path validation entirely and instead extracts the Ed25519 SPKI from the
//! peer's cert and compares it byte-for-byte against an `expected_pubkey`
//! supplied at construction.
//!
//! Why this is sound: harness has no CA, no DNS-PKI assumptions, and no
//! time-validity story beyond the cert's own dates. The pubkey in the cert
//! IS the identity. Any binding tighter than "the peer holds the secret half
//! of `expected_pubkey`" is what mTLS already proves; everything weaker is
//! useless for our threat model.
//!
//! The "danger" naming on rustls's custom-verifier API reflects that we are
//! bypassing webpki validation; the substitution we make (strict pubkey
//! pinning) is strictly stronger for our use case.

use std::sync::Arc;

use harness_core::PublicKey;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::{
    CertificateError, DigitallySignedStruct, DistinguishedName, Error as RustlsError,
    SignatureScheme,
};

use crate::transport::cert::extract_ed25519_spki;

/// Pin to a single expected Ed25519 public key. Cloneable and cheap to
/// construct — share one across many connections, or build per-connection
/// when the expected key varies.
///
/// Constructed by `Transport::dial` (commit 4); the `#[allow(dead_code)]`
/// keeps the workspace lints happy until then.
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct PinnedKeyVerifier {
    expected: PublicKey,
    crypto: Arc<CryptoProvider>,
}

#[allow(dead_code)]
impl PinnedKeyVerifier {
    pub(crate) fn new(expected: PublicKey, crypto: Arc<CryptoProvider>) -> Self {
        Self { expected, crypto }
    }

    fn check_cert(&self, end_entity: &CertificateDer<'_>) -> Result<(), RustlsError> {
        let pk = extract_ed25519_spki(end_entity).map_err(|e| {
            RustlsError::InvalidCertificate(CertificateError::Other(rustls::OtherError(Arc::new(
                e,
            ))))
        })?;
        if pk == self.expected {
            Ok(())
        } else {
            Err(RustlsError::InvalidCertificate(
                CertificateError::ApplicationVerificationFailure,
            ))
        }
    }

    fn verify_signature_inner(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.crypto.signature_verification_algorithms,
        )
    }
}

impl ServerCertVerifier for PinnedKeyVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, RustlsError> {
        self.check_cert(end_entity)?;
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        // TLS 1.2 is disabled at the protocol-version layer; this should be
        // unreachable. Returning Err defends against config drift.
        Err(RustlsError::PeerIncompatible(
            rustls::PeerIncompatible::Tls12NotOfferedOrEnabled,
        ))
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        self.verify_signature_inner(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        // Ed25519 only. Any other scheme means the peer's cert isn't a
        // harness identity cert.
        vec![SignatureScheme::ED25519]
    }
}

impl ClientCertVerifier for PinnedKeyVerifier {
    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> Result<ClientCertVerified, RustlsError> {
        self.check_cert(end_entity)?;
        Ok(ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        Err(RustlsError::PeerIncompatible(
            rustls::PeerIncompatible::Tls12NotOfferedOrEnabled,
        ))
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        self.verify_signature_inner(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![SignatureScheme::ED25519]
    }

    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        // No CA hierarchy; we don't suggest acceptable issuers to the peer.
        &[]
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::transport::cert::derive_cert;
    use harness_core::Identity;

    fn provider() -> Arc<CryptoProvider> {
        Arc::new(rustls::crypto::ring::default_provider())
    }

    #[test]
    fn pinned_verifier_accepts_matching_cert() {
        let id = Identity::generate();
        let (cert, _) = derive_cert(&id).expect("derive");
        let v = PinnedKeyVerifier::new(*id.public_key(), provider());
        v.check_cert(&cert).expect("matching cert must verify");
    }

    #[test]
    fn pinned_verifier_rejects_mismatched_cert() {
        let signer = Identity::generate();
        let (cert, _) = derive_cert(&signer).expect("derive");
        let other = Identity::generate();
        let v = PinnedKeyVerifier::new(*other.public_key(), provider());
        let err = v.check_cert(&cert).expect_err("must reject");
        assert!(
            matches!(
                err,
                RustlsError::InvalidCertificate(CertificateError::ApplicationVerificationFailure)
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn server_cert_verifier_full_path_accepts_matching() {
        let id = Identity::generate();
        let (cert, _) = derive_cert(&id).expect("derive");
        let v = PinnedKeyVerifier::new(*id.public_key(), provider());
        let server_name = ServerName::try_from("placeholder.local").expect("name");
        v.verify_server_cert(&cert, &[], &server_name, &[], UnixTime::now())
            .expect("verify_server_cert must accept");
    }

    #[test]
    fn server_cert_verifier_full_path_rejects_mismatched() {
        let signer = Identity::generate();
        let (cert, _) = derive_cert(&signer).expect("derive");
        let other = Identity::generate();
        let v = PinnedKeyVerifier::new(*other.public_key(), provider());
        let server_name = ServerName::try_from("placeholder.local").expect("name");
        v.verify_server_cert(&cert, &[], &server_name, &[], UnixTime::now())
            .expect_err("verify_server_cert must reject");
    }

    #[test]
    fn client_cert_verifier_full_path() {
        let id = Identity::generate();
        let (cert, _) = derive_cert(&id).expect("derive");
        let v = PinnedKeyVerifier::new(*id.public_key(), provider());
        v.verify_client_cert(&cert, &[], UnixTime::now())
            .expect("verify_client_cert must accept");
    }

    #[test]
    fn supported_verify_schemes_is_ed25519_only() {
        let v = PinnedKeyVerifier::new(*Identity::generate().public_key(), provider());
        assert_eq!(
            <PinnedKeyVerifier as ServerCertVerifier>::supported_verify_schemes(&v),
            vec![SignatureScheme::ED25519]
        );
        assert_eq!(
            <PinnedKeyVerifier as ClientCertVerifier>::supported_verify_schemes(&v),
            vec![SignatureScheme::ED25519]
        );
    }

    #[test]
    fn extract_ed25519_spki_round_trips_through_cert() {
        let id = Identity::generate();
        let (cert, _) = derive_cert(&id).expect("derive");
        let pk = crate::transport::cert::extract_ed25519_spki(&cert).expect("extract");
        assert_eq!(pk, *id.public_key());
    }
}
