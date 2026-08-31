// SPDX-License-Identifier: LGPL-2.1-or-later

//! Dial-out to a varlink-relayd (README.relayd.md): the node opens one
//! WebSocket to the relay, serves HTTP/2 over it with the roles
//! reversed, and every h2 stream the relay opens becomes one accepted
//! connection for axum, so [`DialOutListener`] composes under
//! [`crate::AsyncTlsListener`] like any other listener and all existing
//! auth paths apply untouched.

use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use axum::Router;
use axum::extract::connect_info::Connected;
use axum::serve::IncomingStream;
use log::{debug, error, info, warn};
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::WebSocketStream;

use varlink_http_bridge::tunnel::{
    NodeId, STREAM_WINDOW, TUNNEL_PATH, WsByteStream, h2_server_builder, splice,
};

pub(crate) trait AsyncStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> AsyncStream for T {}
// one erased stream type so the whole tunnel path costs a single
// instantiation of the axum/hyper serving stack, TLS or not
pub(crate) type BoxedStream = Box<dyn AsyncStream>;

/// What [`DialOutListener::accept`] reports as the peer: there is no
/// real address, and the TLS channel binding must travel here because
/// the boxed stream hides the TLS layer from `connect_info`.
#[derive(Clone)]
pub(crate) struct TunnelPeer {
    describe: std::sync::Arc<str>,
    // the h2 stream this connection came in on: the only name the relay
    // and this bridge both see, so their log lines can be matched up
    stream: Option<u32>,
    binding: Option<varlink_http_bridge::TlsChannelBinding>,
}

impl std::fmt::Display for TunnelPeer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.describe)?;
        match self.stream {
            Some(id) => write!(f, " stream {id}"),
            None => Ok(()),
        }
    }
}

impl std::fmt::Debug for TunnelPeer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // never the binding: it feeds the auth token computation
        write!(f, "TunnelPeer({})", self.describe)
    }
}

/// An `axum::serve::Listener` fed by the relay tunnel instead of a
/// socket: a background task dials the relay, keeps redialing with
/// backoff and jitter, and forwards each h2 stream as one connection.
pub(crate) struct DialOutListener {
    conns: mpsc::Receiver<(BoxedStream, TunnelPeer)>,
    describe: std::sync::Arc<str>,
}

impl DialOutListener {
    /// The caller's TLS terminates here, per accepted tunnel stream,
    /// when an acceptor is configured (the relay never sees plaintext).
    pub(crate) fn start(
        url: &str,
        node_id: NodeId,
        tls_acceptor: Option<openssl::ssl::SslAcceptor>,
    ) -> Result<Self> {
        let target = TunnelUrl::parse(url)?;
        let (tx, rx) = mpsc::channel(16);
        let describe: std::sync::Arc<str> = format!("{url} as {node_id}").into();
        tokio::spawn(dial_loop(
            target,
            node_id,
            StreamSink {
                conns: tx,
                tls_acceptor,
                describe: std::sync::Arc::clone(&describe),
            },
        ));
        Ok(Self {
            conns: rx,
            describe,
        })
    }
}

impl axum::serve::Listener for DialOutListener {
    type Io = BoxedStream;
    type Addr = TunnelPeer;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        if let Some(conn) = self.conns.recv().await {
            return conn;
        }
        // the dial loop never returns; ending up here is a bug, but the
        // other listeners must keep serving
        error!("tunnel dial loop is gone, dial-out is dead");
        std::future::pending().await
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        Ok(TunnelPeer {
            describe: std::sync::Arc::clone(&self.describe),
            stream: None,
            binding: None,
        })
    }
}

impl Connected<IncomingStream<'_, DialOutListener>> for crate::VarlinkConnCache {
    fn connect_info(target: IncomingStream<'_, DialOutListener>) -> Self {
        let peer = target.remote_addr();
        // one line per caller, so debug: at info the tunnel's own
        // lifecycle has to stay findable among them
        debug!("new tunnel connection via {peer}");
        Self::new(peer.binding.clone())
    }
}

/// Where accepted tunnel streams go: TLS-handshaken when configured,
/// erased, and handed to the axum listener.
#[derive(Clone)]
struct StreamSink {
    conns: mpsc::Sender<(BoxedStream, TunnelPeer)>,
    tls_acceptor: Option<openssl::ssl::SslAcceptor>,
    describe: std::sync::Arc<str>,
}

impl StreamSink {
    /// Runs the per-stream TLS handshake in its own task so a stalling
    /// caller cannot block the tunnel accept loop.
    fn deliver(&self, io: DuplexStream, stream: u32) {
        let sink = self.clone();
        tokio::spawn(async move {
            let peer = TunnelPeer {
                describe: std::sync::Arc::clone(&sink.describe),
                stream: Some(stream),
                binding: None,
            };
            let conn: (BoxedStream, TunnelPeer) = match &sink.tls_acceptor {
                Some(acceptor) => {
                    let tls = match varlink_http_bridge::tls_accept(acceptor, io).await {
                        Ok(tls) => tls,
                        Err(e) => {
                            // one caller's problem, and a caller that
                            // speaks no TLS can repeat it at will
                            debug!("TLS handshake on {peer}: {e:#}");
                            return;
                        }
                    };
                    let binding = varlink_http_bridge::export_tls_channel_binding(tls.ssl());
                    (
                        Box::new(tls),
                        TunnelPeer {
                            binding: Some(binding),
                            ..peer
                        },
                    )
                }
                None => (Box::new(io), peer),
            };
            let _ = sink.conns.send(conn).await;
        });
    }
}

/// Serve `app` over the tunnel.
pub(crate) async fn serve(listener: DialOutListener, app: Router) -> Result<()> {
    let make_svc = app.into_make_service_with_connect_info::<crate::VarlinkConnCache>();
    axum::serve(listener, make_svc)
        .with_graceful_shutdown(crate::shutdown_signal())
        .await?;
    Ok(())
}

struct TunnelUrl {
    tls: bool,
    host: String,
    port: u16,
}

impl TunnelUrl {
    /// `ws://host[:port]` or `wss://host[:port]`, no path: the tunnel
    /// path is fixed. The port defaults to the relay's node listener.
    fn parse(url: &str) -> Result<Self> {
        let (tls, rest) = if let Some(rest) = url.strip_prefix("wss://") {
            (true, rest)
        } else if let Some(rest) = url.strip_prefix("ws://") {
            (false, rest)
        } else {
            bail!("--relay must be a ws:// or wss:// URL, got {url:?}");
        };
        if rest.contains('/') {
            bail!("--relay takes no path, {TUNNEL_PATH} is implied: {url:?}");
        }
        let (host, port) = match rest.rsplit_once(':') {
            Some((host, port)) => (
                host,
                port.parse()
                    .with_context(|| format!("invalid port in {url:?}"))?,
            ),
            None => (rest, 8443),
        };
        if host.is_empty() {
            bail!("--relay needs a host: {url:?}");
        }
        Ok(Self {
            tls,
            host: host.to_string(),
            port,
        })
    }

    fn ws_url(&self, node_id: NodeId) -> String {
        let scheme = if self.tls { "wss" } else { "ws" };
        format!(
            "{scheme}://{}:{}{TUNNEL_PATH}?id={node_id}",
            self.host, self.port
        )
    }
}

const REDIAL_MIN: Duration = Duration::from_secs(1);
const REDIAL_MAX: Duration = Duration::from_secs(60);

/// Keep one tunnel connection alive forever: redial on every end, with
/// exponential backoff on failed dials and jitter so a fleet does not
/// stampede a restarted relay.
///
/// An outage is one event, not one per attempt: the first failure is
/// loud, the rest are debug, and coming back says how long it took and
/// how many attempts it needed. Without that, a relay that is down for
/// an hour writes a warning every minute and the log stops being read.
/// A reminder every [`OUTAGE_REMINDER`] keeps a lasting outage visible
/// to a log that was only opened after it started.
async fn dial_loop(target: TunnelUrl, node_id: NodeId, sink: StreamSink) {
    let relay = format!("{}:{}", target.host, target.port);
    let mut backoff = REDIAL_MIN;
    let mut outage: Option<Outage> = None;
    loop {
        let attempt = outage.as_ref().map_or(0, |o| o.attempts) + 1;
        let started_down = outage.as_ref().map(|o| o.since);
        match dial_once(&target, node_id, &sink, &relay, started_down, attempt).await {
            // the connection was established and later ended: normal
            // operation (middleboxes reap long-lived connections)
            Ok(lived) => {
                info!("tunnel to {relay} closed after {lived:?}, redialing");
                backoff = REDIAL_MIN;
                outage = None;
            }
            Err(e) => {
                let reason = format!("{e:#}");
                let outage = outage.get_or_insert_with(Outage::new);
                match outage.report(attempt, &reason) {
                    Some(loud) => warn!("tunnel to {relay} {loud}"),
                    None => debug!("tunnel to {relay}, attempt {attempt} failed: {reason}"),
                }
            }
        }
        tokio::time::sleep(with_jitter(backoff)).await;
        backoff = (backoff * 2).min(REDIAL_MAX);
    }
}

// how often a lasting outage repeats itself in the log
const OUTAGE_REMINDER: Duration = Duration::from_secs(600);

/// A run of failed dials, so that the log can treat it as one event.
struct Outage {
    since: std::time::Instant,
    reminded: Option<std::time::Instant>,
    attempts: u32,
    // what the last loud line said, so a changed cause is not hidden
    // behind the reminder interval
    reason: String,
}

impl Outage {
    fn new() -> Self {
        Self {
            since: std::time::Instant::now(),
            reminded: None,
            attempts: 0,
            reason: String::new(),
        }
    }

    /// What to say about this failed attempt, if anything: the start of
    /// an outage and a change of cause are worth a line, a lasting
    /// outage repeats itself only every [`OUTAGE_REMINDER`], and the
    /// attempts in between belong in a debug log.
    fn report(&mut self, attempt: u32, reason: &str) -> Option<String> {
        self.attempts = attempt;
        let due = match self.reminded {
            None => true,
            Some(_) if self.reason != reason => true,
            Some(at) => at.elapsed() >= OUTAGE_REMINDER,
        };
        if !due {
            return None;
        }
        let first = self.reminded.is_none() || self.reason != reason;
        self.reminded = Some(std::time::Instant::now());
        self.reason = reason.to_string();
        Some(if first {
            format!("is down: {reason}")
        } else {
            format!(
                "still down after {:?} and {attempt} attempts: {reason}",
                self.since.elapsed()
            )
        })
    }
}

/// Add up to ~25% of random jitter.
fn with_jitter(base: Duration) -> Duration {
    let mut byte = [0u8; 1];
    // on rand failure the base alone is fine, jitter is best-effort
    let _ = openssl::rand::rand_bytes(&mut byte);
    base + base.mul_f32(f32::from(byte[0]) / 1024.0)
}

/// One dial and, if it works, the life of that tunnel. Returns how long
/// the tunnel lasted.
async fn dial_once(
    target: &TunnelUrl,
    node_id: NodeId,
    sink: &StreamSink,
    relay: &str,
    down_since: Option<std::time::Instant>,
    attempts: u32,
) -> Result<Duration> {
    let tcp = TcpStream::connect((target.host.as_str(), target.port))
        .await
        .context("connecting to relay")?;
    varlink_http_bridge::set_tcp_keepalive_and_nodelay(&tcp)?;
    let url = target.ws_url(node_id);
    let ws_upgrade = async {
        if target.tls {
            let tls = tls_connect(&target.host, tcp).await?;
            let (ws, _response) = tokio_tungstenite::client_async(url, tls)
                .await
                .context("tunnel WebSocket upgrade")?;
            anyhow::Ok(Tunnel::Tls(ws))
        } else {
            let (ws, _response) = tokio_tungstenite::client_async(url, tcp)
                .await
                .context("tunnel WebSocket upgrade")?;
            anyhow::Ok(Tunnel::Plain(ws))
        }
    };
    let tunnel = ws_upgrade.await.map_err(|e| upgrade_hint(e, node_id))?;

    match down_since {
        Some(since) => info!(
            "tunnel to {relay} is back as node {node_id} after {:?} and {attempts} attempts",
            since.elapsed()
        ),
        None => info!("tunnel to {relay} established as node {node_id}"),
    }
    let started = std::time::Instant::now();
    match tunnel {
        Tunnel::Tls(ws) => serve_tunnel(ws, sink).await,
        Tunnel::Plain(ws) => serve_tunnel(ws, sink).await,
    }?;
    Ok(started.elapsed())
}

/// The two transports a tunnel can run on, so the dial and the serving
/// half stay separate (the log line in between needs the dial to be
/// done and the serving not to have started).
enum Tunnel {
    Tls(WebSocketStream<tokio_openssl::SslStream<TcpStream>>),
    Plain(WebSocketStream<TcpStream>),
}

/// Turn the two rejections an operator can actually fix into advice.
/// Backoff will not help with either, so they stay loud on every retry.
fn upgrade_hint(e: anyhow::Error, node_id: NodeId) -> anyhow::Error {
    use tokio_tungstenite::tungstenite::Error as WsError;
    let Some(WsError::Http(response)) = e.downcast_ref::<WsError>() else {
        return e;
    };
    match response.status().as_u16() {
        409 => e.context(format!(
            "the relay says node id {node_id} is already connected; another bridge on this \
             machine claims the same id -- give one of them --instance"
        )),
        401 | 403 => {
            e.context("the relay refused this node's credentials; it may not be registered there")
        }
        _ => e,
    }
}

/// TLS to the relay, verified against the system trust store. (This
/// leg only protects the id claim at L0; the caller's TLS session to
/// this bridge rides through it end-to-end either way.)
async fn tls_connect(host: &str, tcp: TcpStream) -> Result<tokio_openssl::SslStream<TcpStream>> {
    let connector = openssl::ssl::SslConnector::builder(openssl::ssl::SslMethod::tls_client())
        .context("TLS connector")?
        .build();
    let ssl = connector
        .configure()
        .context("TLS configure")?
        .into_ssl(host)
        .context("TLS setup")?;
    let mut stream = tokio_openssl::SslStream::new(ssl, tcp).context("TLS stream")?;
    std::pin::Pin::new(&mut stream)
        .connect()
        .await
        .context("TLS connect to relay")?;
    Ok(stream)
}

/// One established tunnel: h2 server role-reversed over the WebSocket;
/// every stream the relay opens is answered with 200 and handed to
/// axum as a connection, spliced by its own task.
async fn serve_tunnel<S>(ws: WebSocketStream<S>, sink: &StreamSink) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut conn = h2_server_builder()
        .handshake::<_, bytes::Bytes>(WsByteStream::new(ws))
        .await
        .context("h2 handshake with relay")?;
    let mut served: u64 = 0;
    // once per tunnel: a full queue means axum is not taking
    // connections as fast as callers arrive, which is worth knowing but
    // not worth a line per stream
    let mut queue_warned = false;
    while let Some(next) = conn.accept().await {
        let (request, mut respond) = next.context("tunnel connection failed")?;
        let body = request.into_body();
        let send = respond
            .send_response(http::Response::new(()), false)
            .context("accepting tunnel stream")?;
        let stream_id = u32::from(send.stream_id());
        let who = format!("relay stream {stream_id}");
        served += 1;
        // the hand-off to axum. A hung local service fills this buffer,
        // then the stream's h2 window, and then splice stops releasing
        // window and the relay stops sending: a wedged stream costs two
        // windows of memory, no more.
        let (io, ours) = tokio::io::duplex(STREAM_WINDOW as usize);
        tokio::spawn(async move {
            let started = std::time::Instant::now();
            match splice(io, body, send, &who).await {
                Ok(moved) => debug!(
                    "{who}: done after {:?}, {} bytes to the caller, {} back",
                    started.elapsed(),
                    moved.received,
                    moved.sent
                ),
                Err(e) => debug!("{who}: ended after {:?}: {e:#}", started.elapsed()),
            }
        });
        if sink.conns.is_closed() {
            bail!("the server dropped the tunnel listener");
        }
        if sink.conns.capacity() == 0 && !queue_warned {
            queue_warned = true;
            debug!("tunnel stream queue is full, callers are waiting for the server to accept");
        }
        sink.deliver(ours, stream_id);
    }
    debug!("tunnel ending; streams served: {served}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_outage_is_one_log_line_not_one_per_attempt() {
        let mut outage = Outage::new();
        let refused = "connecting to relay: Connection refused";

        let first = outage.report(1, refused).expect("the start must be loud");
        assert!(first.contains("is down"), "{first}");
        // the retries in between are for a debug log
        for attempt in 2..20 {
            assert_eq!(outage.report(attempt, refused), None, "attempt {attempt}");
        }

        // a changed cause is news, whenever it happens
        let rejected = "the relay says node id ... is already connected";
        let changed = outage
            .report(20, rejected)
            .expect("a new cause must be loud");
        assert!(changed.contains("is down"), "{changed}");
        assert_eq!(outage.report(21, rejected), None);

        // and an outage that will not end says so again eventually
        outage.reminded = Some(std::time::Instant::now() - OUTAGE_REMINDER);
        let reminder = outage.report(22, rejected).expect("a reminder is due");
        assert!(
            reminder.contains("still down") && reminder.contains("22 attempts"),
            "{reminder}"
        );
        assert_eq!(outage.report(23, rejected), None, "and then quiet again");
    }

    #[test]
    fn tunnel_url_parsing() {
        let url = TunnelUrl::parse("wss://relay.example:8443").unwrap();
        assert!(url.tls);
        assert_eq!((url.host.as_str(), url.port), ("relay.example", 8443));

        let url = TunnelUrl::parse("ws://relay.example").unwrap();
        assert!(!url.tls);
        assert_eq!((url.host.as_str(), url.port), ("relay.example", 8443));

        let id: NodeId = "0123456789abcdef0123456789abcdef".parse().unwrap();
        assert_eq!(
            TunnelUrl::parse("ws://r:80").unwrap().ws_url(id),
            "ws://r:80/v1/tunnel?id=0123456789abcdef0123456789abcdef"
        );

        for bad in [
            "https://relay.example",
            "ws://relay.example/path",
            "ws://",
            "ws://host:notaport",
        ] {
            assert!(TunnelUrl::parse(bad).is_err(), "must reject {bad:?}");
        }
    }
}
