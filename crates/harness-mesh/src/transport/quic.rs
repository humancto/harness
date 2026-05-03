//! [`Transport`] — the QUIC endpoint and its dial / accept surface.
//!
//! Each harness node runs **one** [`Transport`]. It binds a UDP socket, owns
//! a [`quinn::Endpoint`], holds an `Arc<Identity>` for cert derivation, and
//! services every outbound dial + every inbound accept. mTLS is on; both
//! sides verify the peer's pubkey via the [`crate::transport::verifier::PinnedKeyVerifier`].

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use crate::transport::cert::{derive_cert, extract_ed25519_spki};
use crate::transport::connection::{Connection, IncomingConnection};
use crate::transport::error::TransportError;
use crate::transport::verifier::PinnedKeyVerifier;
use harness_core::{Identity, PublicKey};
use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use quinn::{ClientConfig, Endpoint, IdleTimeout, ServerConfig, TransportConfig, VarInt};

/// ALPN id for the harness wire protocol. Mismatch = handshake failure.
const ALPN: &[u8] = b"harness/1";

/// Idle timeout — laptop-lid-close + WiFi-flap forgiveness.
const IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// NAT keepalive interval. Half the idle timeout per PRD §11.3.
const KEEP_ALIVE: Duration = Duration::from_secs(15);

/// Per-connection bidi stream cap. 1.4 uses one stream; 16 leaves headroom
/// for Phase 4's per-task multiplexing.
const MAX_BIDI_STREAMS: u32 = 16;

/// Default `Transport::dial` timeout. Quinn's own idle timeout is 30s but
/// 10s is plenty for a handshake on a healthy LAN.
const DIAL_TIMEOUT: Duration = Duration::from_secs(10);

/// In-memory snapshot of the pubkeys this node trusts. 1.8's `TrustStore`
/// is the persistent, event-emitting version; this transport-local view is
/// just the snapshot bind+accept needs at handshake time.
#[derive(Clone, Default, Debug)]
pub struct TrustStore {
    allowed: HashSet<PublicKey>,
}

impl TrustStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn allow(&mut self, pk: PublicKey) {
        self.allowed.insert(pk);
    }

    #[must_use]
    pub fn contains(&self, pk: &PublicKey) -> bool {
        self.allowed.contains(pk)
    }
}

/// One QUIC endpoint per harness node. Cheap to clone (shares the
/// underlying `quinn::Endpoint` via `Arc` internally — but `Endpoint` is
/// already `Clone` and cheap, so we don't add a wrapper).
#[derive(Clone)]
pub struct Transport {
    endpoint: Endpoint,
    identity: Arc<Identity>,
    /// Reserved for the dedupe layer that 1.5 will own; 1.4 doesn't read it.
    _trust: TrustStore,
}

impl std::fmt::Debug for Transport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Transport")
            .field("local_addr", &self.endpoint.local_addr().ok())
            .field("node_id", &self.identity.node_id())
            .finish_non_exhaustive()
    }
}

impl Transport {
    /// Bind a UDP socket and start the QUIC endpoint. Synchronous; nothing
    /// async runs until the caller awaits [`Self::dial`] or pulls from
    /// [`Self::accept`].
    pub fn bind(
        socket: SocketAddr,
        identity: Arc<Identity>,
        trust: TrustStore,
    ) -> Result<Self, TransportError> {
        ensure_default_crypto_provider();

        let (cert, key) = derive_cert(&identity)?;

        let crypto = Arc::new(rustls::crypto::ring::default_provider());

        // Server config — uses our PinnedKeyVerifier for mTLS client cert
        // verification. The verifier is `pin_to: None` here because the
        // server doesn't know the dialer's pubkey before handshake — it
        // accepts any peer cert that's a well-formed Ed25519 cert and lets
        // the application layer (IncomingConnection::accept) decide whether
        // to keep the connection.
        let server_crypto = build_server_crypto(cert.clone(), key.clone_key(), crypto.clone())?;
        let server_quic = Arc::new(QuicServerConfig::try_from(server_crypto)?);
        let mut server_config = ServerConfig::with_crypto(server_quic);
        server_config.transport = Arc::new(transport_config());

        // Client config seed — actual ClientConfig is built per-dial with
        // the PinnedKeyVerifier set to the dialer's expected_pubkey.
        let mut endpoint = Endpoint::server(server_config, socket).map_err(TransportError::Bind)?;
        // Default client config so dial() can build per-call configs
        // without re-binding the socket.
        endpoint.set_default_client_config(default_client_config(cert, key, crypto)?);

        Ok(Self {
            endpoint,
            identity,
            _trust: trust,
        })
    }

    /// Local address (post-bind, in case the caller bound `:0`).
    pub fn local_addr(&self) -> Result<SocketAddr, TransportError> {
        self.endpoint.local_addr().map_err(TransportError::Bind)
    }

    /// The local identity's node id. Useful for dedupe at the layer above.
    #[must_use]
    pub fn node_id(&self) -> harness_core::NodeId {
        self.identity.node_id()
    }

    /// Dial `peer_addr`. Succeeds only if the peer's TLS cert presents an
    /// Ed25519 SPKI equal to `expected_pubkey`. Times out after
    /// `DIAL_TIMEOUT`.
    pub async fn dial(
        &self,
        peer_addr: SocketAddr,
        expected_pubkey: &PublicKey,
    ) -> Result<Connection, TransportError> {
        let crypto = Arc::new(rustls::crypto::ring::default_provider());
        let (cert, key) = derive_cert(&self.identity)?;
        let client_crypto = build_client_crypto(cert, key, *expected_pubkey, crypto)?;
        let client_quic = Arc::new(QuicClientConfig::try_from(client_crypto)?);
        let mut client_config = ClientConfig::new(client_quic);
        client_config.transport_config(Arc::new(transport_config()));

        // Match the cert's SAN exactly — the cert is `<node_id>.harness.local`
        // (cert.rs §4.2). Our verifier ignores ServerName, but rustls may
        // still surface a "name mismatch" error before the verifier runs.
        let server_name = format!("{}.harness.local", expected_pubkey.node_id());
        let connecting = self
            .endpoint
            .connect_with(client_config, peer_addr, &server_name)
            .map_err(|source| TransportError::Dial {
                addr: peer_addr,
                source,
            })?;

        let connection = match tokio::time::timeout(DIAL_TIMEOUT, connecting).await {
            Ok(Ok(c)) => c,
            Ok(Err(source)) => {
                return Err(TransportError::Connect {
                    addr: peer_addr,
                    source,
                });
            }
            Err(_) => return Err(TransportError::DialTimeout(DIAL_TIMEOUT)),
        };

        let pubkey = peer_pubkey_from_connection(&connection)?;
        if pubkey != *expected_pubkey {
            // Defense in depth — should be unreachable since the verifier
            // already enforced this. If we reach here, the verifier was
            // bypassed somehow; refuse to use the connection.
            return Err(TransportError::CertMismatch);
        }

        Connection::from_quinn_dialer(connection, pubkey).await
    }

    /// Stream of incoming connection attempts. The caller decides per
    /// attempt whether to accept (typically: yes if pubkey is in the
    /// trust store, otherwise drop).
    pub async fn accept_one(&self) -> Result<IncomingConnection, TransportError> {
        let incoming = self.endpoint.accept().await.ok_or(TransportError::Closed)?;
        let connection = incoming.await.map_err(TransportError::Accept)?;
        let pubkey = peer_pubkey_from_connection(&connection)?;
        Ok(IncomingConnection::from_quinn(connection, pubkey))
    }

    /// Graceful shutdown: stop accepting and close all connections.
    /// Idempotent.
    pub async fn shutdown(&self, grace: Duration) {
        self.endpoint.close(0u32.into(), b"shutdown");
        // Best-effort wait; not a hard timeout.
        let _ = tokio::time::timeout(grace, self.endpoint.wait_idle()).await;
    }
}

// -----------------------------------------------------------------------------
// rustls / quinn config helpers
// -----------------------------------------------------------------------------

fn transport_config() -> TransportConfig {
    let mut t = TransportConfig::default();
    t.max_idle_timeout(Some(
        IdleTimeout::try_from(IDLE_TIMEOUT)
            .unwrap_or_else(|_| IdleTimeout::from(VarInt::from_u32(30_000))),
    ));
    t.keep_alive_interval(Some(KEEP_ALIVE));
    t.max_concurrent_bidi_streams(VarInt::from_u32(MAX_BIDI_STREAMS));
    t.max_concurrent_uni_streams(VarInt::from_u32(0));
    t
}

fn build_server_crypto(
    cert: rustls::pki_types::CertificateDer<'static>,
    key: rustls::pki_types::PrivateKeyDer<'static>,
    crypto: Arc<rustls::crypto::CryptoProvider>,
) -> Result<Arc<rustls::ServerConfig>, TransportError> {
    // Server-side "verify any harness cert" verifier — accepts any cert that
    // decodes to a valid Ed25519 SPKI. Application-layer accept (in
    // IncomingConnection::accept) makes the trust call once it sees the pubkey.
    let verifier = Arc::new(AnyEd25519ClientVerifier {
        crypto: crypto.clone(),
    });

    let mut config = rustls::ServerConfig::builder_with_provider(crypto)
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_client_cert_verifier(verifier)
        .with_single_cert(vec![cert], key)?;
    config.alpn_protocols = vec![ALPN.to_vec()];
    config.max_early_data_size = 0; // 0-RTT disabled; see plan §14 footgun #1.
    Ok(Arc::new(config))
}

fn build_client_crypto(
    cert: rustls::pki_types::CertificateDer<'static>,
    key: rustls::pki_types::PrivateKeyDer<'static>,
    expected_pubkey: PublicKey,
    crypto: Arc<rustls::crypto::CryptoProvider>,
) -> Result<Arc<rustls::ClientConfig>, TransportError> {
    let verifier = Arc::new(PinnedKeyVerifier::new(expected_pubkey, crypto.clone()));
    let mut config = rustls::ClientConfig::builder_with_provider(crypto)
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_client_auth_cert(vec![cert], key)?;
    config.alpn_protocols = vec![ALPN.to_vec()];
    Ok(Arc::new(config))
}

fn default_client_config(
    cert: rustls::pki_types::CertificateDer<'static>,
    key: rustls::pki_types::PrivateKeyDer<'static>,
    crypto: Arc<rustls::crypto::CryptoProvider>,
) -> Result<ClientConfig, TransportError> {
    // Default never accepts (placeholder pubkey). Real dials always go through
    // `Endpoint::connect_with(per_call_client_config, ..)`.
    let placeholder = PublicKey::from_bytes(&[1u8; 32]).map_err(|_| {
        TransportError::Protocol(
            "placeholder pubkey for default client config rejected by curve check".into(),
        )
    })?;
    let client_crypto = build_client_crypto(cert, key, placeholder, crypto)?;
    let client_quic = Arc::new(QuicClientConfig::try_from(client_crypto)?);
    let mut config = ClientConfig::new(client_quic);
    config.transport_config(Arc::new(transport_config()));
    Ok(config)
}

/// Server-side mTLS verifier that accepts any well-formed Ed25519 cert
/// without checking the pubkey. The application-layer
/// `IncomingConnection::accept` callback enforces trust once the handshake
/// has surfaced the pubkey.
#[derive(Debug)]
struct AnyEd25519ClientVerifier {
    crypto: Arc<rustls::crypto::CryptoProvider>,
}

impl rustls::server::danger::ClientCertVerifier for AnyEd25519ClientVerifier {
    fn verify_client_cert(
        &self,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::server::danger::ClientCertVerified, rustls::Error> {
        // We require the cert decode to a valid Ed25519 SPKI; trust check
        // happens at the application layer.
        extract_ed25519_spki(end_entity).map_err(|e| {
            rustls::Error::InvalidCertificate(rustls::CertificateError::Other(rustls::OtherError(
                Arc::new(e),
            )))
        })?;
        Ok(rustls::server::danger::ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Err(rustls::Error::PeerIncompatible(
            rustls::PeerIncompatible::Tls12NotOfferedOrEnabled,
        ))
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.crypto.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![rustls::SignatureScheme::ED25519]
    }

    fn root_hint_subjects(&self) -> &[rustls::DistinguishedName] {
        &[]
    }
}

/// Extract the peer's pubkey from a finished quinn connection.
pub(crate) fn peer_pubkey_from_connection(
    conn: &quinn::Connection,
) -> Result<PublicKey, TransportError> {
    let identity = conn.peer_identity().ok_or_else(|| {
        TransportError::Protocol("peer presented no identity (mTLS not negotiated)".into())
    })?;
    let certs = identity
        .downcast::<Vec<rustls::pki_types::CertificateDer<'static>>>()
        .map_err(|_| TransportError::Protocol("peer identity not a rustls cert chain".into()))?;
    let end_entity = certs
        .first()
        .ok_or_else(|| TransportError::Protocol("peer cert chain empty".into()))?;
    extract_ed25519_spki(end_entity).map_err(|e| TransportError::BadPeerCert(e.to_string()))
}

/// rustls 0.23 requires installing a default `CryptoProvider` once per
/// process. Tests in particular construct `Transport`s repeatedly; the
/// install is idempotent under `OnceLock` so we do it lazily here.
fn ensure_default_crypto_provider() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        // It's fine if another part of the program already installed one;
        // we only fail if rustls' install API itself returns an Err, which
        // happens iff a different provider is already installed. We tolerate
        // that — rustls operations that don't take a provider explicitly
        // will use whatever's installed; we always pass our provider
        // explicitly when building configs.
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}
