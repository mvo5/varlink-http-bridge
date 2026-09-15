// SPDX-License-Identifier: LGPL-2.1-or-later

//! Listening helpers for the server code.

/// Returns a SslAcceptorBuilder from the given {cert,key}_path with
/// a minimium TLS1.3 version requirement for TLS channel binding.
///
/// # Errors
/// Returns an error if the certificate or key cannot be loaded or do
/// not match.
pub fn tls_acceptor_builder(
    cert_path: &str,
    key_path: &str,
) -> anyhow::Result<openssl::ssl::SslAcceptorBuilder> {
    use openssl::ssl::{SslAcceptor, SslFiletype, SslMethod};

    let mut builder = SslAcceptor::mozilla_modern_v5(SslMethod::tls_server())?;
    // mozilla_modern_v5 allows TLS 1.2, but we need 1.3 for channel binding
    // (export_keying_material requires TLS 1.3).
    builder.set_min_proto_version(Some(openssl::ssl::SslVersion::TLS1_3))?;
    builder.set_certificate_chain_file(cert_path)?;
    builder.set_private_key_file(key_path, SslFiletype::PEM)?;
    builder.check_private_key()?;
    Ok(builder)
}

/// Perform a TLS handshake on an already-accepted
/// stream. `client_store` holds the CAs a client certificate is
/// verified against. This runs per handshake the CAs can be rotated
/// without rebuilding the acceptor.
///
/// # Errors
/// Returns an error if the handshake fails.
pub async fn tls_accept<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
    acceptor: &openssl::ssl::SslAcceptor,
    client_store: Option<openssl::x509::store::X509Store>,
    stream: S,
) -> anyhow::Result<tokio_openssl::SslStream<S>> {
    use anyhow::Context;
    let mut ssl = openssl::ssl::Ssl::new(acceptor.context()).context("SSL context error")?;
    if let Some(store) = client_store {
        ssl.set_verify_cert_store(store)
            .context("installing client CA store")?;
    }
    let mut tls_stream =
        tokio_openssl::SslStream::new(ssl, stream).context("SSL stream creation failed")?;
    std::pin::Pin::new(&mut tls_stream)
        .accept()
        .await
        .context("TLS handshake failed")?;
    Ok(tls_stream)
}
