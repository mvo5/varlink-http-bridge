// SPDX-License-Identifier: LGPL-2.1-or-later

//! The rustls backend of [`crate::tls`], with ring for the crypto.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Context, bail};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, WebPkiSupportedAlgorithms};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::sign::{CertifiedKey, SingleCertAndKey};
use rustls::{
    CertificateError, ClientConfig, DigitallySignedStruct, DistinguishedName, RootCertStore,
    ServerConfig, SignatureScheme,
};
use tokio::io::{AsyncRead, AsyncWrite};
use x509_cert::der::Decode;

use super::{ClientCert, Roots, Verify};
use crate::{TLS_CHANNEL_BINDING_LABEL, TLS_CHANNEL_BINDING_LEN, TlsChannelBinding};

/// An established TLS session over `S`.
pub type Stream<S> = tokio_rustls::TlsStream<S>;

fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// Test-only: makes ring the process default for rustls users that do not
/// pick a provider themselves, like reqwest.
#[cfg(any(test, feature = "test-helpers"))]
pub fn install_default_provider() {
    // an error means one is installed already
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// An X.509 certificate.
#[derive(Clone)]
pub struct Certificate(CertificateDer<'static>);

impl Certificate {
    /// Parses every certificate in `pem`. A PEM without any certificate is
    /// an empty vector, not an error.
    ///
    /// # Errors
    /// Returns an error if a certificate in `pem` is malformed.
    pub fn from_pem_bundle(pem: &[u8]) -> anyhow::Result<Vec<Self>> {
        CertificateDer::pem_slice_iter(pem)
            .map(|der| {
                let der = der.context("decoding PEM")?;
                // a CertificateDer is not parsed until it is used
                webpki::EndEntityCert::try_from(&der).context("parsing certificate")?;
                Ok(Self(der))
            })
            .collect()
    }

    /// Returns the fingerprint of the public key: base64 of the SHA-256
    /// digest of the DER-encoded `SubjectPublicKeyInfo`, which is what
    /// `curl --pinnedpubkey sha256//…` expects.
    ///
    /// # Errors
    /// Returns an error if the certificate cannot be parsed.
    pub fn public_key_pin(&self) -> anyhow::Result<String> {
        let cert = webpki::EndEntityCert::try_from(&self.0).context("parsing certificate")?;
        Ok(super::base64(&sha256(&cert.subject_public_key_info())))
    }

    /// Returns whether the certificate names itself as its own issuer.
    #[must_use]
    pub fn is_self_signed(&self) -> bool {
        webpki::EndEntityCert::try_from(&self.0).is_ok_and(|c| c.issuer() == c.subject())
    }

    /// Returns the subject as text, like `CN=host, O=org`.
    #[must_use]
    pub fn subject(&self) -> String {
        let Ok(cert) = x509_cert::Certificate::from_der(&self.0) else {
            return String::new();
        };
        // not the Display of the name, that is RFC 4514 (reversed) and
        // would differ from what the openssl backend shows
        let rdns = &cert.tbs_certificate.subject.0;
        rdns.iter()
            .flat_map(|rdn| rdn.0.iter())
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// Exports the RFC 9266 channel binding of an established TLS 1.3 session.
///
/// # Panics
/// Panics if the export fails or yields only zeros, neither of which
/// happens with TLS 1.3.
#[must_use]
pub fn channel_binding<S>(stream: &Stream<S>) -> TlsChannelBinding {
    let buf = [0u8; TLS_CHANNEL_BINDING_LEN];
    let label = TLS_CHANNEL_BINDING_LABEL.as_bytes();
    let buf = match stream {
        Stream::Client(s) => s.get_ref().1.export_keying_material(buf, label, Some(&[])),
        Stream::Server(s) => s.get_ref().1.export_keying_material(buf, label, Some(&[])),
    }
    .expect("export_keying_material must succeed with TLS 1.3");
    assert!(
        buf.iter().any(|&b| b != 0),
        "TLS channel binding must not be all zeros"
    );
    TlsChannelBinding(super::base64(&buf))
}

/// Returns the certificate the peer presented, if any.
#[must_use]
pub fn peer_certificate<S>(stream: &Stream<S>) -> Option<Certificate> {
    let leaf = stream.get_ref().1.peer_certificates()?.first()?;
    Some(Certificate(leaf.clone().into_owned()))
}

/// Loads a PEM certificate chain and its key.
fn load_identity(cert_path: &Path, key_path: &Path) -> anyhow::Result<Arc<CertifiedKey>> {
    let chain = CertificateDer::pem_file_iter(cert_path)
        .and_then(Iterator::collect::<Result<Vec<_>, _>>)
        .with_context(|| format!("loading certificate {}", cert_path.display()))?;
    if chain.is_empty() {
        bail!("no PEM certificate found in {}", cert_path.display());
    }
    let key = PrivateKeyDer::from_pem_file(key_path)
        .with_context(|| format!("loading key {}", key_path.display()))?;
    let key = provider()
        .key_provider
        .load_private_key(key)
        .with_context(|| format!("loading key {}", key_path.display()))?;
    let identity = CertifiedKey::new(chain, key);
    identity
        .keys_match()
        .context("certificate and key do not match")?;
    Ok(Arc::new(identity))
}

/// What mTLS without a CA has to do. `WebPkiClientVerifier` cannot, it
/// refuses to be built without a trust anchor.
#[derive(Debug)]
struct RejectAllClients(WebPkiSupportedAlgorithms);

impl ClientCertVerifier for RejectAllClients {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        Err(CertificateError::UnknownIssuer.into())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.0)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.0)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.supported_schemes()
    }
}

/// The server side of TLS: a certificate and key, TLS 1.3 only.
#[derive(Clone)]
pub struct Acceptor {
    identity: Arc<CertifiedKey>,
    require_client_cert: bool,
    /// For handshakes that do not ask for a client certificate.
    no_client_auth: Arc<ServerConfig>,
}

impl Acceptor {
    /// Loads the PEM certificate chain and key. With `require_client_cert`
    /// a handshake only succeeds for a client certificate that the CAs
    /// passed to [`Acceptor::accept`] vouch for.
    ///
    /// # Errors
    /// Returns an error if the certificate or key cannot be loaded or do
    /// not match.
    pub fn new(cert_path: &str, key_path: &str, require_client_cert: bool) -> anyhow::Result<Self> {
        let identity = load_identity(Path::new(cert_path), Path::new(key_path))?;
        let no_client_auth = Arc::new(
            Self::config_builder()?
                .with_no_client_auth()
                .with_cert_resolver(Arc::new(SingleCertAndKey::from(Arc::clone(&identity)))),
        );
        Ok(Self {
            identity,
            require_client_cert,
            no_client_auth,
        })
    }

    fn config_builder() -> anyhow::Result<rustls::ConfigBuilder<ServerConfig, rustls::WantsVerifier>>
    {
        // export_keying_material for the channel binding requires 1.3
        ServerConfig::builder_with_provider(provider())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .context("TLS server configuration")
    }

    /// Performs the TLS handshake on an accepted `stream`. `client_cas` are
    /// the CAs a client certificate is verified against in this handshake.
    ///
    /// # Errors
    /// Returns an error if the handshake fails.
    pub async fn accept<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        client_cas: Option<&[Certificate]>,
        stream: S,
    ) -> anyhow::Result<Stream<S>> {
        let config = if self.require_client_cert {
            let mut roots = RootCertStore::empty();
            for ca in client_cas.unwrap_or_default() {
                roots.add(ca.0.clone()).context("adding a client CA")?;
            }
            let verifier: Arc<dyn ClientCertVerifier> = if roots.is_empty() {
                Arc::new(RejectAllClients(
                    provider().signature_verification_algorithms,
                ))
            } else {
                rustls::server::WebPkiClientVerifier::builder_with_provider(
                    Arc::new(roots),
                    provider(),
                )
                .build()
                .context("building the client certificate verifier")?
            };
            Arc::new(
                Self::config_builder()?
                    .with_client_cert_verifier(verifier)
                    .with_cert_resolver(Arc::new(SingleCertAndKey::from(Arc::clone(
                        &self.identity,
                    )))),
            )
        } else {
            Arc::clone(&self.no_client_auth)
        };
        let stream = tokio_rustls::TlsAcceptor::from(config)
            .accept(stream)
            .await
            .context("TLS handshake failed")?;
        Ok(Stream::Server(stream))
    }
}

fn root_store(roots: &Roots) -> anyhow::Result<RootCertStore> {
    let mut store = RootCertStore::empty();
    match roots {
        Roots::System => {
            // unreadable or unparsable system certificates are skipped,
            // one bad file must not take TLS down
            let loaded = rustls_native_certs::load_native_certs();
            store.add_parsable_certificates(loaded.certs);
        }
        Roots::Exclusive(path) => {
            let certs = CertificateDer::pem_file_iter(path)
                .and_then(Iterator::collect::<Result<Vec<_>, _>>)
                .with_context(|| format!("reading CA certificate {}", path.display()))?;
            if certs.is_empty() {
                bail!("no PEM certificate found in {}", path.display());
            }
            for cert in certs {
                store
                    .add(cert)
                    .with_context(|| format!("adding a certificate from {}", path.display()))?;
            }
        }
    }
    Ok(store)
}

/// Validates the chain like rustls does by default, then lets a [`Verify`]
/// overrule the outcome.
struct ServerVerifier {
    /// `None` without any root, where `WebPkiServerVerifier` cannot be
    /// built and nothing validates.
    webpki: Option<Arc<rustls::client::WebPkiServerVerifier>>,
    algorithms: WebPkiSupportedAlgorithms,
    verify_hostname: bool,
    verify: Option<Verify>,
    /// Whether the chain validated, for [`Connector::connect`] to return.
    validated: Arc<AtomicBool>,
}

impl std::fmt::Debug for ServerVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerVerifier").finish_non_exhaustive()
    }
}

impl ServerCertVerifier for ServerVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let chain = match &self.webpki {
            Some(webpki) => webpki
                .verify_server_cert(end_entity, intermediates, server_name, ocsp_response, now)
                .map(|_| ()),
            None => Err(CertificateError::UnknownIssuer.into()),
        };
        let chain = match chain {
            // the name is checked last, so the chain itself validated
            Err(rustls::Error::InvalidCertificate(
                CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. },
            )) if !self.verify_hostname => Ok(()),
            other => other,
        };
        self.validated.store(chain.is_ok(), Ordering::Relaxed);

        match &self.verify {
            None => chain?,
            Some(verify) => {
                let cert = Certificate(end_entity.clone().into_owned());
                if !verify(&cert, chain.is_ok()) {
                    return Err(chain.err().unwrap_or_else(|| {
                        rustls::Error::General("server certificate refused".into())
                    }));
                }
            }
        }
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algorithms.supported_schemes()
    }
}

/// The client side of TLS. Building one is expensive, so reuse it.
#[derive(Clone)]
pub struct Connector {
    webpki: Option<Arc<rustls::client::WebPkiServerVerifier>>,
    identity: Option<Arc<CertifiedKey>>,
    verify: Option<Verify>,
}

impl Connector {
    /// Returns a TLS 1.3 only connector that verifies servers against the
    /// system CA store, without a client certificate.
    ///
    /// # Errors
    /// Returns an error if the TLS library cannot be set up.
    pub fn system() -> anyhow::Result<Self> {
        Self::new(&Roots::System, None, None)
    }

    /// Returns a TLS 1.3 only connector. `client_cert` is presented to
    /// servers that ask for one. With `verify` the callback has the last
    /// word on the server certificate, otherwise it has to validate against
    /// `roots`.
    ///
    /// # Errors
    /// Returns an error if a file cannot be loaded, or the client
    /// certificate and key do not match.
    pub fn new(
        roots: &Roots,
        client_cert: Option<&ClientCert>,
        verify: Option<Verify>,
    ) -> anyhow::Result<Self> {
        let identity = client_cert
            .map(|ClientCert { cert, key }| {
                load_identity(cert, key).context("loading client certificate")
            })
            .transpose()?;
        let roots = root_store(roots)?;
        let webpki = if roots.is_empty() {
            None
        } else {
            Some(
                rustls::client::WebPkiServerVerifier::builder_with_provider(
                    Arc::new(roots),
                    provider(),
                )
                .build()
                .context("building the server certificate verifier")?,
            )
        };
        Ok(Self {
            webpki,
            identity,
            verify,
        })
    }

    /// Performs the TLS handshake on a connected `stream`. Returns the
    /// session and whether the server certificate validated against the
    /// roots, which can be false only if a [`Verify`] accepted it.
    ///
    /// `verify_hostname=false` is for transports without a hostname; the
    /// certificate chain is still verified.
    ///
    /// # Errors
    /// Returns an error if the handshake fails.
    pub async fn connect<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        domain: &str,
        verify_hostname: bool,
        stream: S,
    ) -> anyhow::Result<(Stream<S>, bool)> {
        // the verifier reports back per connection, so the config is too
        let validated = Arc::new(AtomicBool::new(false));
        let verifier = Arc::new(ServerVerifier {
            webpki: self.webpki.clone(),
            algorithms: provider().signature_verification_algorithms,
            verify_hostname,
            verify: self.verify.clone(),
            validated: Arc::clone(&validated),
        });
        let builder = ClientConfig::builder_with_provider(provider())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .context("TLS configure")?
            .dangerous()
            .with_custom_certificate_verifier(verifier);
        let config = match &self.identity {
            Some(identity) => builder
                .with_client_cert_resolver(Arc::new(SingleCertAndKey::from(Arc::clone(identity)))),
            None => builder.with_no_client_auth(),
        };

        let name = ServerName::try_from(domain.to_owned()).context("TLS setup")?;
        let stream = tokio_rustls::TlsConnector::from(Arc::new(config))
            .connect(name, stream)
            .await?;
        Ok((Stream::Client(stream), validated.load(Ordering::Relaxed)))
    }
}

/// Returns the SHA-256 digest of `data`.
#[must_use]
pub fn sha256(data: &[u8]) -> [u8; 32] {
    let mut out = [0u8; 32];
    out.copy_from_slice(ring::digest::digest(&ring::digest::SHA256, data).as_ref());
    out
}

/// Returns the HMAC-SHA256 of `msg` under `key`.
///
/// # Errors
/// Never fails with this backend.
pub fn hmac_sha256(key: &[u8], msg: &[u8]) -> anyhow::Result<[u8; 32]> {
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, key);
    let mut out = [0u8; 32];
    out.copy_from_slice(ring::hmac::sign(&key, msg).as_ref());
    Ok(out)
}

/// Fills `buf` with cryptographically secure random bytes.
///
/// # Errors
/// Returns an error if the system has no randomness to give.
pub fn random_bytes(buf: &mut [u8]) -> anyhow::Result<()> {
    use ring::rand::SecureRandom;

    ring::rand::SystemRandom::new()
        .fill(buf)
        .map_err(|_| anyhow::anyhow!("reading random bytes"))
}

/// Compares `a` and `b` in time that depends only on their length.
#[must_use]
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    use subtle::ConstantTimeEq;

    a.ct_eq(b).into()
}
