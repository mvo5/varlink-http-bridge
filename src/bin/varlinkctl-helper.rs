use std::io::{Read, Write};
use std::net::TcpStream;
use std::os::fd::{FromRawFd, OwnedFd};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use log::{debug, warn};
use openssl::ssl::{SslConnector, SslFiletype, SslMethod};
use rustix::event::{PollFd, PollFlags, poll};
use tungstenite::{Message, WebSocket};

use varlink_http_bridge::SSHAUTH_MAGIC_PREFIX;

/// Select the signing key from the agent.
///
/// If `$VARLINK_SSH_KEY` is set, read the public key from that file and find
/// the matching key in the agent (by fingerprint). Otherwise pick the first
/// supported (non-RSA) key.
fn select_signing_key(keys: Vec<ssh_key::PublicKey>) -> Result<ssh_key::PublicKey> {
    if let Ok(key_path) = std::env::var("VARLINK_SSH_KEY") {
        let key_text = std::fs::read_to_string(&key_path)
            .with_context(|| format!("reading VARLINK_SSH_KEY={key_path}"))?;
        let wanted = ssh_key::PublicKey::from_openssh(key_text.trim())
            .with_context(|| format!("parsing public key from {key_path}"))?;

        if matches!(wanted.algorithm(), ssh_key::Algorithm::Rsa { .. }) {
            bail!(
                "VARLINK_SSH_KEY={key_path} is an RSA key, which is not supported; use Ed25519 or ECDSA"
            );
        }

        let wanted_fp = wanted.fingerprint(ssh_key::HashAlg::Sha256);
        return keys
            .into_iter()
            .find(|k| k.fingerprint(ssh_key::HashAlg::Sha256) == wanted_fp)
            .with_context(|| {
                format!("key {wanted_fp} from {key_path} not found in ssh-agent (is it loaded?)")
            });
    }

    // No explicit key requested — pick the first supported one, warn about RSA
    for k in &keys {
        if matches!(k.algorithm(), ssh_key::Algorithm::Rsa { .. }) {
            warn!(
                "skipping RSA key {} ({}): RSA signing is not supported, use Ed25519 or ECDSA",
                k.fingerprint(ssh_key::HashAlg::Sha256),
                k.comment(),
            );
        }
    }
    keys.into_iter()
        .find(|k| !matches!(k.algorithm(), ssh_key::Algorithm::Rsa { .. }))
        .context("no Ed25519 or ECDSA key in ssh-agent (RSA is not supported)")
}

/// Build a Bearer token by signing via ssh-agent. Returns None if `SSH_AUTH_SOCK` is not set.
fn build_auth_token(method: &str, path_and_query: &str) -> Result<Option<String>> {
    let Ok(auth_sock) = std::env::var("SSH_AUTH_SOCK") else {
        return Ok(None);
    };

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .context("creating tokio runtime for SSH agent")?;

    let token = rt.block_on(async {
        let keys = sshauth::agent::list_keys(&auth_sock)
            .await
            .context("listing ssh-agent keys")?;
        let key = select_signing_key(keys)?;
        debug!(
            "SSH auth: using {} key {} ({})",
            key.algorithm(),
            key.fingerprint(ssh_key::HashAlg::Sha256),
            key.comment(),
        );

        let mut builder = sshauth::TokenSigner::using_authsock(&auth_sock)?;
        builder
            .key(key)
            .include_fingerprint(true)
            .magic_prefix(SSHAUTH_MAGIC_PREFIX);
        let signer = builder.build()?;

        let mut tb = signer.sign_for();
        tb.action("method", method).action("path", path_and_query);
        let token: sshauth::token::Token = tb.sign().await?;
        Ok::<_, anyhow::Error>(token.encode())
    })?;

    Ok(Some(format!("Bearer {token}")))
}

enum Stream {
    Plain(TcpStream),
    Tls(openssl::ssl::SslStream<TcpStream>),
}

impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Stream::Plain(s) => s.read(buf),
            Stream::Tls(s) => s.read(buf),
        }
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Stream::Plain(s) => s.write(buf),
            Stream::Tls(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Stream::Plain(s) => s.flush(),
            Stream::Tls(s) => s.flush(),
        }
    }
}

type Ws = WebSocket<Stream>;

fn ws_tcp_stream(ws: &Ws) -> &TcpStream {
    match ws.get_ref() {
        Stream::Plain(s) => s,
        Stream::Tls(s) => s.get_ref(),
    }
}

/// Build an `SslConnector` with client certs and a custom CA loaded from the
/// first existing directory:
/// 1. `$XDG_CONFIG_HOME/varlink-http-bridge/`
/// 2. `~/.config/varlink-http-bridge/`
/// 3. `$CREDENTIALS_DIRECTORY` (systemd, see systemd.exec(5))
fn build_ssl_connector() -> Result<SslConnector> {
    let mut builder = SslConnector::builder(SslMethod::tls_client())?;

    let maybe_credentials_dirs = [
        std::env::var_os("XDG_CONFIG_HOME").map(|d| PathBuf::from(d).join("varlink-http-bridge")),
        std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".config/varlink-http-bridge")),
        std::env::var_os("CREDENTIALS_DIRECTORY").map(PathBuf::from),
    ];
    if let Some(dir) = maybe_credentials_dirs
        .into_iter()
        .flatten()
        .find(|d| d.is_dir())
    {
        let cert = dir.join("client-cert-file");
        let key = dir.join("client-key-file");
        let ca = dir.join("server-ca-file");

        if cert.exists() && key.exists() {
            builder
                .set_certificate_chain_file(&cert)
                .with_context(|| format!("loading client certificate {}", cert.display()))?;
            builder
                .set_private_key_file(&key, SslFiletype::PEM)
                .with_context(|| format!("loading client key {}", key.display()))?;
            builder
                .check_private_key()
                .context("client certificate and key do not match")?;
        }

        if ca.exists() {
            builder
                .set_ca_file(&ca)
                .with_context(|| format!("loading CA certificate {}", ca.display()))?;
        }
    }

    Ok(builder.build())
}

fn connect_ws(url: &str) -> Result<Ws> {
    use tungstenite::client::IntoClientRequest;

    let ws_url = if let Some(rest) = url.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = url.strip_prefix("http://") {
        format!("ws://{rest}")
    } else {
        url.to_string()
    };
    let uri: tungstenite::http::Uri = ws_url.parse().context("invalid WebSocket URL")?;
    let use_tls = uri.scheme_str() == Some("wss");
    let host = uri.host().context("URL has no host")?;
    let port = uri.port_u16().unwrap_or(if use_tls { 443 } else { 80 });

    let tcp = TcpStream::connect((host, port))
        .with_context(|| format!("TCP connect to {host}:{port} failed"))?;

    let stream =
        if use_tls {
            let connector = build_ssl_connector()?;
            Stream::Tls(connector.connect(host, tcp).context(
                "TLS handshake failed: check client certificate if server requires mTLS",
            )?)
        } else {
            Stream::Plain(tcp)
        };

    // Build auth token if ssh-agent is available; proceed without on failure
    let path_and_query = uri
        .path_and_query()
        .map_or(uri.path(), tungstenite::http::uri::PathAndQuery::as_str);
    let auth_header = match build_auth_token("GET", path_and_query) {
        Ok(h) => h,
        Err(e) => {
            warn!("SSH auth token failed, proceeding without: {e:#}");
            None
        }
    };

    // IntoClientRequest auto-generates standard WS upgrade headers
    let mut request = ws_url
        .into_client_request()
        .context("building WS request")?;

    if let Some(auth) = auth_header {
        request.headers_mut().insert(
            "Authorization",
            auth.parse().context("invalid auth header value")?,
        );
    }

    let ws_context = if use_tls {
        "WebSocket handshake failed: check client cert if server requires mTLS"
    } else {
        "WebSocket handshake failed"
    };
    let (ws, _) = tungstenite::client(request, stream).context(ws_context)?;
    Ok(ws)
}

/// Forward all data from the WebSocket to fd3 until it would block or the peer closes.
/// Returns Ok(true) if a Close frame was received.
fn forward_ws_until_would_block(ws: &mut Ws, fd3: &mut UnixStream) -> Result<bool> {
    loop {
        match ws.read() {
            Ok(Message::Binary(data)) => fd3.write_all(&data).context("fd3 write")?,
            Ok(Message::Text(_)) => bail!("unexpected text WebSocket frame"),
            Ok(Message::Close(_)) => return Ok(true),
            Ok(_) => {}
            Err(tungstenite::Error::Io(ref e)) if e.kind() == std::io::ErrorKind::WouldBlock => {
                return Ok(false);
            }
            Err(e) => return Err(e).context("ws read"),
        }
    }
}

fn graceful_close(ws: &mut Ws) -> Result<()> {
    let tcp = ws_tcp_stream(ws);
    tcp.set_nonblocking(false)?;
    tcp.set_read_timeout(Some(Duration::from_secs(2)))?;
    tcp.set_write_timeout(Some(Duration::from_secs(2)))?;

    // close and wait up to aboves timeout
    ws.close(None)?;
    while ws.can_read() {
        match ws.read() {
            Ok(Message::Close(_)) => break,
            Err(e) => return Err(e).context("waiting for close response"),
            Ok(_) => {}
        }
    }
    Ok(())
}

fn main() -> Result<()> {
    env_logger::init();

    let listen_fds: i32 = std::env::var("LISTEN_FDS")
        .context("LISTEN_FDS is not set")?
        .parse()
        .context("LISTEN_FDS is not a valid integer")?;
    if listen_fds != 1 {
        bail!("LISTEN_FDS must be 1, got {listen_fds}");
    }

    // XXX: once https://github.com/systemd/systemd/issues/40640 is implemented
    // we can remove the env_url and this confusing match
    let env_url = std::env::var("VARLINK_BRIDGE_URL").ok();
    let arg_url = std::env::args().nth(1);
    let bridge_url = match (env_url, arg_url) {
        (Some(_), Some(_)) => bail!("cannot set both VARLINK_BRIDGE_URL and argv[1]"),
        (None, None) => bail!("bridge URL required via VARLINK_BRIDGE_URL or argv[1]"),
        (Some(url), None) | (None, Some(url)) => url,
    };

    // Safety: fd 3 is passed to us via the sd_listen_fds() protocol.
    let fd3 = unsafe { OwnedFd::from_raw_fd(3) };
    rustix::io::fcntl_getfd(&fd3).context("fd 3 is not valid (LISTEN_FDS protocol error?)")?;
    let mut fd3 = UnixStream::from(fd3);

    let mut ws = connect_ws(&bridge_url)?;

    // Set non-blocking so that we deal with incomplete websocket
    // frames in ws.read() - they return WouldBlock now and we can
    // continue when waking up from PollFd next time.
    ws_tcp_stream(&ws).set_nonblocking(true)?;

    let shutdown = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::consts::SIGINT, Arc::clone(&shutdown))?;
    signal_hook::flag::register(signal_hook::consts::SIGTERM, Arc::clone(&shutdown))?;

    let mut buf = vec![0u8; 8192];
    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        let mut pollfds = [
            PollFd::new(&fd3, PollFlags::IN),
            PollFd::new(ws_tcp_stream(&ws), PollFlags::IN),
        ];
        match poll(&mut pollfds, None) {
            // signal interrupted poll: continue to re-check shutdown flag
            Err(rustix::io::Errno::INTR) => continue,
            result => {
                result?;
            }
        }
        let fd3_revents = pollfds[0].revents();
        let ws_revents = pollfds[1].revents();

        if fd3_revents.contains(PollFlags::IN) {
            let n = fd3.read(&mut buf).context("fd3 read")?;
            if n == 0 {
                break;
            }
            ws.send(Message::Binary(buf[..n].to_vec().into()))
                .context("ws send")?;
        }

        if ws_revents.contains(PollFlags::IN) && forward_ws_until_would_block(&mut ws, &mut fd3)? {
            break; // peer sent Close
        }

        if fd3_revents.contains(PollFlags::HUP) {
            break;
        }
    }

    if let Err(e) = graceful_close(&mut ws) {
        warn!("WebSocket close failed: {e:#}");
    }
    Ok(())
}
