// SPDX-License-Identifier: LGPL-2.1-or-later

//! Dial-out to a varlink-relayd (README.relayd.md): the node opens one
//! WebSocket to the relay, serves HTTP/2 over it with the roles
//! reversed, and every h2 stream the relay opens becomes one accepted
//! connection for axum, so [`DialOutListener`] composes under
//! [`crate::AsyncTlsListener`] like any other listener and all existing
//! auth paths apply untouched.

use std::time::Duration;

use anyhow::{Context as _, Result, anyhow, bail};
use axum::extract::connect_info::Connected;
use axum::serve::IncomingStream;
use log::{debug, error, info, warn};
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::WebSocketStream;

use varlink_http_bridge::tunnel::{
    MAX_TUNNEL_STREAMS, NodeId, STREAM_WINDOW, StreamLoad, StreamSlot, TUNNEL_PATH, WsByteStream,
    h2_server_builder, splice,
};

/// What [`DialOutListener::accept`] reports as the peer: there is no
/// real address, only the tunnel and the stream on it.
#[derive(Clone, Debug)]
pub(crate) struct TunnelPeer {
    describe: std::sync::Arc<str>,
    // the h2 stream this connection came in on: the only name the relay
    // and this bridge both see, so their log lines can be matched up
    stream: Option<u32>,
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

/// An `axum::serve::Listener` fed by the relay tunnel instead of a
/// socket: a background task dials the relay, keeps redialing with
/// backoff and jitter, and forwards each h2 stream as one connection.
/// Plaintext: the caller's TLS terminates in the
/// [`crate::AsyncTlsListener`] wrapped around this, per stream, so the
/// relay never sees it.
pub(crate) struct DialOutListener {
    conns: mpsc::Receiver<(DuplexStream, TunnelPeer)>,
    describe: std::sync::Arc<str>,
}

impl DialOutListener {
    pub(crate) fn start(url: &str, node_id: NodeId) -> Result<Self> {
        let target = TunnelUrl::parse(url)?;
        let relay_tls = target.tls.then(relay_tls_connector).transpose()?;
        let (tx, rx) = mpsc::channel(16);
        let describe: std::sync::Arc<str> = format!("{url} as node {node_id}").into();
        tokio::spawn(dial_loop(
            target,
            node_id,
            relay_tls,
            StreamSink {
                conns: tx,
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
    type Io = DuplexStream;
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
        })
    }
}

// one line per caller, so debug: at info the tunnel's own lifecycle has
// to stay findable among them
impl Connected<IncomingStream<'_, DialOutListener>> for crate::VarlinkConnCache {
    fn connect_info(target: IncomingStream<'_, DialOutListener>) -> Self {
        debug!("new tunnel connection via {}", target.remote_addr());
        Self::new(None)
    }
}

impl Connected<IncomingStream<'_, crate::AsyncTlsListener<DialOutListener>>>
    for crate::VarlinkConnCache
{
    fn connect_info(target: IncomingStream<'_, crate::AsyncTlsListener<DialOutListener>>) -> Self {
        let ssl = target.io().ssl();
        debug!("new TLS tunnel connection via {}", target.remote_addr());
        Self::new(Some(varlink_http_bridge::export_tls_channel_binding(ssl)))
    }
}

/// Where accepted tunnel streams go: the queue [`DialOutListener`]
/// accepts from.
#[derive(Clone)]
struct StreamSink {
    conns: mpsc::Sender<(DuplexStream, TunnelPeer)>,
    describe: std::sync::Arc<str>,
}

impl StreamSink {
    fn deliver(&self, io: DuplexStream, stream: u32) {
        let conns = self.conns.clone();
        let peer = TunnelPeer {
            describe: std::sync::Arc::clone(&self.describe),
            stream: Some(stream),
        };
        // off the accept loop: a full queue has to stall this caller, not
        // the h2 connection that carries every other one (and the pings)
        tokio::spawn(async move {
            let _ = conns.send((io, peer)).await;
        });
    }
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
        if rest.contains('@') {
            bail!("--relay takes no credentials: {url:?}");
        }
        // an IPv6 literal has colons of its own, so host and port split
        // as a URI authority does: bracketed, and the port after
        let authority: http::uri::Authority = rest
            .parse()
            .with_context(|| format!("invalid host or port in {url:?}"))?;
        let host = authority
            .host()
            .trim_start_matches('[')
            .trim_end_matches(']');
        if host.is_empty() {
            bail!("--relay needs a host: {url:?}");
        }
        // anything behind the host is the port, and has to be one
        let port = if authority.as_str().len() > authority.host().len() {
            authority
                .port_u16()
                .with_context(|| format!("invalid port in {url:?}"))?
        } else {
            8443
        };
        Ok(Self {
            tls,
            host: host.to_string(),
            port,
        })
    }

    /// `host:port` as a URL wants it, an IPv6 host bracketed.
    fn authority(&self) -> String {
        if self.host.contains(':') {
            format!("[{}]:{}", self.host, self.port)
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }

    fn ws_url(&self, node_id: NodeId) -> String {
        let scheme = if self.tls { "wss" } else { "ws" };
        format!(
            "{scheme}://{}{TUNNEL_PATH}?node_id={node_id}",
            self.authority()
        )
    }
}

const REDIAL_MIN: Duration = Duration::from_secs(1);
const REDIAL_MAX: Duration = Duration::from_secs(60);

// the relay's heartbeat (node.rs) mirrored, so both ends give up on a
// silent path in the same ~40s
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(10);
// connect, TLS, WebSocket upgrade and h2 handshake together
const DIAL_TIMEOUT: Duration = Duration::from_secs(10);
// The stream id at which a tunnel is rotated. The relay opens every
// caller as a new stream on the one connection and never rotates it,
// and client stream ids are the odd numbers below 2^31: after 2^30
// callers send_request fails for good on a connection that still looks
// healthy (PINGs answered, no GOAWAY). This side sees every id, so it
// asks for a fresh connection 2^20 ids short of the ceiling: at 100
// callers/s that is ~124 days in, with ~3h of headroom left.
const ROTATE_AT_STREAM_ID: u32 = (1 << 31) - (1 << 21);

/// The limits of one tunnel, in one place so a test can shrink them.
struct Tuning {
    dial_timeout: Duration,
    heartbeat_interval: Duration,
    heartbeat_timeout: Duration,
    rotate_at: u32,
}

impl Tuning {
    const DEFAULT: Self = Self {
        dial_timeout: DIAL_TIMEOUT,
        heartbeat_interval: HEARTBEAT_INTERVAL,
        heartbeat_timeout: HEARTBEAT_TIMEOUT,
        rotate_at: ROTATE_AT_STREAM_ID,
    };
}

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
async fn dial_loop(
    target: TunnelUrl,
    node_id: NodeId,
    relay_tls: Option<openssl::ssl::SslConnector>,
    sink: StreamSink,
) {
    let relay = target.authority();
    let tuning = Tuning::DEFAULT;
    let mut backoff = REDIAL_MIN;
    let mut outage: Option<Outage> = None;
    loop {
        let attempt = outage.as_ref().map_or(0, |o| o.attempts) + 1;
        let started_down = outage.as_ref().map(|o| o.since);
        match dial_once(
            &target,
            relay_tls.as_ref(),
            node_id,
            &sink,
            started_down,
            attempt,
            &tuning,
        )
        .await
        {
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
    // Some iff the URL is wss://, see DialOutListener::start
    relay_tls: Option<&openssl::ssl::SslConnector>,
    node_id: NodeId,
    sink: &StreamSink,
    down_since: Option<std::time::Instant>,
    attempts: u32,
    tuning: &Tuning,
) -> Result<Duration> {
    let relay = target.authority();
    // one bound over the whole prelude: none of these steps has a
    // timeout of its own, and a relay that accepts and then says
    // nothing (a black-holed path, a wedged frontend) would otherwise
    // park the dial loop for good
    let prelude = async {
        let tcp = TcpStream::connect((target.host.as_str(), target.port))
            .await
            .context("connecting to relay")?;
        varlink_http_bridge::set_tcp_keepalive_and_nodelay(&tcp)?;
        let url = target.ws_url(node_id);
        if let Some(connector) = relay_tls {
            let tls = tls_connect(connector, &target.host, tcp).await?;
            let (ws, _response) = tokio_tungstenite::client_async(url, tls)
                .await
                .context("tunnel WebSocket upgrade")?;
            anyhow::Ok(Tunnel::Tls(accept_h2(ws).await?))
        } else {
            let (ws, _response) = tokio_tungstenite::client_async(url, tcp)
                .await
                .context("tunnel WebSocket upgrade")?;
            anyhow::Ok(Tunnel::Plain(accept_h2(ws).await?))
        }
    };
    let tunnel = match tokio::time::timeout(tuning.dial_timeout, prelude).await {
        Ok(dialed) => dialed.map_err(|e| upgrade_hint(e, node_id))?,
        Err(_) => bail!(
            "the tunnel handshake did not complete within {:?}",
            tuning.dial_timeout
        ),
    };

    match down_since {
        Some(since) => info!(
            "tunnel to {relay} is back as node {node_id} after {:?} and {attempts} attempts",
            since.elapsed()
        ),
        None => info!("tunnel to {relay} established as node {node_id}"),
    }
    let started = std::time::Instant::now();
    match tunnel {
        Tunnel::Tls(conn) => serve_tunnel(conn, sink, tuning).await,
        Tunnel::Plain(conn) => serve_tunnel(conn, sink, tuning).await,
    }?;
    Ok(started.elapsed())
}

/// The h2 server side of a tunnel over one of its transports.
type H2Conn<S> = h2::server::Connection<WsByteStream<S>, bytes::Bytes>;

/// The two transports a tunnel can run on, so the dial and the serving
/// half stay separate (the log line in between needs the dial to be
/// done and the serving not to have started).
enum Tunnel {
    Tls(H2Conn<tokio_openssl::SslStream<TcpStream>>),
    Plain(H2Conn<TcpStream>),
}

/// The h2 handshake, roles reversed: the relay sends the client preface.
async fn accept_h2<S>(ws: WebSocketStream<S>) -> Result<H2Conn<S>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    h2_server_builder()
        .handshake::<_, bytes::Bytes>(WsByteStream::new(ws))
        .await
        .context("h2 handshake with relay")
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

/// The TLS client for the relay leg, verifying against the system trust
/// store. Built once, at startup: loading the store is not free, and a
/// broken one should fail there rather than on every dial.
fn relay_tls_connector() -> Result<openssl::ssl::SslConnector> {
    Ok(
        openssl::ssl::SslConnector::builder(openssl::ssl::SslMethod::tls_client())
            .context("TLS connector")?
            .build(),
    )
}

/// TLS to the relay, verified against the system trust store. (This
/// leg only protects an unsigned id claim; the caller's TLS session to
/// this bridge rides through it end-to-end either way.)
async fn tls_connect(
    connector: &openssl::ssl::SslConnector,
    host: &str,
    tcp: TcpStream,
) -> Result<tokio_openssl::SslStream<TcpStream>> {
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
async fn serve_tunnel<S>(mut conn: H2Conn<S>, sink: &StreamSink, tuning: &Tuning) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut ping_pong = conn.ping_pong().expect("first ping_pong handle");
    // the relay pings too, but only the side that notices a silent path
    // can end its half: after a NAT mapping expires the relay reaps this
    // node in ~40s, while accept() alone would wait for the OS TCP
    // keepalive, ~2h by default, serving nobody
    let heartbeat = async {
        loop {
            tokio::time::sleep(tuning.heartbeat_interval).await;
            let ping = ping_pong.ping(h2::Ping::opaque());
            match tokio::time::timeout(tuning.heartbeat_timeout, ping).await {
                Ok(Ok(_pong)) => {}
                Ok(Err(e)) => return anyhow::Error::new(e).context("PING to the relay"),
                Err(_) => {
                    return anyhow!(
                        "no PONG from the relay within {:?}",
                        tuning.heartbeat_timeout
                    );
                }
            }
        }
    };
    tokio::pin!(heartbeat);
    let load = std::sync::Arc::new(StreamLoad::default());
    let mut served: u64 = 0;
    // once per tunnel: a full queue means axum is not taking
    // connections as fast as callers arrive, which is worth knowing but
    // not worth a line per stream
    let mut queue_warned = false;
    let mut rotating = false;
    loop {
        let next = tokio::select! {
            next = conn.accept() => next,
            // reachable but not answering, or not reachable at all: an
            // error here is what makes the dial loop call it an outage
            why = &mut heartbeat => return Err(why),
        };
        let Some(next) = next else { break };
        let (request, mut respond) = next.context("tunnel connection failed")?;
        let stream_id = u32::from(respond.stream_id());
        // GOAWAY to the relay; the streams in flight finish, accept()
        // then returns None, and the dial loop redials. Between the
        // relay refusing new streams on this connection and the fresh
        // tunnel registering, callers get 502: the redial backoff (~1s)
        // plus the dial, and longer if a stream in flight takes its
        // time, since the relay frees this node's id only when the
        // connection has closed
        if stream_id >= tuning.rotate_at && !rotating {
            rotating = true;
            info!("tunnel stream ids reached {stream_id}, rotating the connection");
            conn.graceful_shutdown();
        }
        let body = request.into_body();
        // a caller gone before its stream is answered (hung up, or the
        // relay gave up queueing it for a slot) is that caller's
        // problem, not the tunnel's
        let send = match respond.send_response(http::Response::new(()), false) {
            Ok(send) => send,
            Err(e) => {
                debug!("tunnel stream was gone before it was accepted: {e}");
                continue;
            }
        };
        let who = format!("relay stream {stream_id}");
        served += 1;
        let (slot, report) = StreamSlot::open(&load);
        if let Some(report) = report {
            log::log!(
                report.level(),
                "this tunnel is carrying {}/{MAX_TUNNEL_STREAMS} streams ({}%)",
                report.active,
                report.tier
            );
        }
        // the hand-off to axum. A hung local service fills this buffer,
        // then the stream's h2 window, and then splice stops releasing
        // window and the relay stops sending: a wedged stream costs two
        // windows of memory, no more.
        let (io, ours) = tokio::io::duplex(STREAM_WINDOW as usize);
        tokio::spawn(async move {
            let _busy = slot;
            let started = std::time::Instant::now();
            match splice(io, body, send, &who).await {
                Ok(moved) => debug!(
                    "{who}: done after {:?}, {} bytes to the caller, {} back",
                    started.elapsed(),
                    moved.received,
                    moved.sent
                ),
                Err(e) => debug!(
                    "{who}: ended after {:?} on the {} side: {e:#}",
                    started.elapsed(),
                    // "relay" covers the caller behind it: a hangup
                    // there reaches this end as a reset stream
                    if e.is_local() { "server" } else { "relay" }
                ),
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
    use futures_util::{SinkExt, StreamExt};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio_tungstenite::tungstenite::Message;
    use varlink_http_bridge::tunnel::h2_client_builder;

    const TEST_NODE: &str = "0123456789abcdef0123456789abcdef";

    /// A relay stub's address, and the tunnel URL that dials it.
    async fn stub_relay() -> (TcpListener, TunnelUrl) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target = TunnelUrl {
            tls: false,
            host: "127.0.0.1".to_string(),
            port: listener.local_addr().unwrap().port(),
        };
        (listener, target)
    }

    fn plain_sink(conns: mpsc::Sender<(DuplexStream, TunnelPeer)>) -> StreamSink {
        StreamSink {
            conns,
            describe: "stub relay".into(),
        }
    }

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

    /// A caller that CONNECTs and then sends nothing holds one of the
    /// node's stream slots, and the relay has stopped counting the time
    /// by then: the node has to hang up on it.
    #[tokio::test]
    async fn a_silent_caller_does_not_keep_its_tunnel_stream() {
        use axum::serve::Listener as _;

        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = crate::tls_cert::load_or_generate(dir.path()).unwrap();
        let mut tls =
            crate::load_tls_config(cert.to_str().unwrap(), key.to_str().unwrap(), None, false)
                .unwrap();
        tls.handshake_timeout = Duration::from_millis(100);
        let describe: std::sync::Arc<str> = "test relay".into();
        let (tx, rx) = mpsc::channel(1);
        let sink = StreamSink {
            conns: tx,
            describe: std::sync::Arc::clone(&describe),
        };
        let plain = DialOutListener {
            conns: rx,
            describe,
        };
        let mut listener = crate::AsyncTlsListener::new(plain, tls, log::Level::Debug).unwrap();

        let (io, mut caller) = tokio::io::duplex(1024);
        sink.deliver(io, 1);
        // the caller says nothing; the node's end of the stream must go
        // away, which is what the caller sees as EOF
        let mut byte = [0u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(5), caller.read(&mut byte))
            .await
            .expect("the stream must be closed on a silent caller");
        assert_eq!(read.unwrap(), 0);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err(),
            "nothing must have been handed to axum"
        );
    }

    #[test]
    fn tunnel_url_parsing() {
        let url = TunnelUrl::parse("wss://relay.example:8443").unwrap();
        assert!(url.tls);
        assert_eq!((url.host.as_str(), url.port), ("relay.example", 8443));

        let url = TunnelUrl::parse("ws://relay.example").unwrap();
        assert!(!url.tls);
        assert_eq!((url.host.as_str(), url.port), ("relay.example", 8443));

        // an IPv6 relay: bracketed in the URL, bare for the dial (where
        // the socket address wants it), bracketed again in the tunnel URL
        let url = TunnelUrl::parse("wss://[2001:db8::1]:8443").unwrap();
        assert_eq!((url.host.as_str(), url.port), ("2001:db8::1", 8443));
        let url = TunnelUrl::parse("ws://[::1]").unwrap();
        assert_eq!((url.host.as_str(), url.port), ("::1", 8443));

        let id: NodeId = "0123456789abcdef0123456789abcdef".parse().unwrap();
        assert_eq!(
            TunnelUrl::parse("ws://r:80").unwrap().ws_url(id),
            "ws://r:80/v1/tunnel?node_id=0123456789abcdef0123456789abcdef"
        );
        assert_eq!(
            TunnelUrl::parse("ws://[::1]:80").unwrap().ws_url(id),
            "ws://[::1]:80/v1/tunnel?node_id=0123456789abcdef0123456789abcdef"
        );

        for bad in [
            "https://relay.example",
            "ws://relay.example/path",
            "ws://",
            "ws://host:notaport",
            // an unbracketed IPv6 literal would dial host "2001:db8:" on
            // port 1
            "ws://2001:db8::1",
            "ws://user@relay.example",
        ] {
            assert!(TunnelUrl::parse(bad).is_err(), "must reject {bad:?}");
        }
    }

    /// The path went silent under an established tunnel (a NAT mapping
    /// expired): the relay has long reaped this node, and only the
    /// node's own heartbeat can notice and redial.
    #[tokio::test]
    async fn a_relay_that_stops_answering_pings_is_an_outage() {
        let (listener, target) = stub_relay().await;
        // upgrades, speaks just enough h2 for the node's handshake to
        // succeed (the client preface and an empty SETTINGS), and then
        // never says another word
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            let mut preface = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".to_vec();
            preface.extend_from_slice(&[0, 0, 0, 4, 0, 0, 0, 0, 0]);
            ws.send(Message::Binary(preface.into())).await.unwrap();
            // keep reading so nothing backs up, answer nothing
            while let Some(Ok(_)) = ws.next().await {}
        });
        let (tx, _rx) = mpsc::channel(1);
        let sink = plain_sink(tx);
        let tuning = Tuning {
            heartbeat_interval: Duration::from_millis(50),
            heartbeat_timeout: Duration::from_millis(100),
            ..Tuning::DEFAULT
        };
        let dial = dial_once(
            &target,
            None,
            TEST_NODE.parse().unwrap(),
            &sink,
            None,
            1,
            &tuning,
        );
        let result = tokio::time::timeout(Duration::from_secs(5), dial)
            .await
            .expect("the heartbeat must end a tunnel nobody answers on");
        let e = result.expect_err("a silent relay is an outage, not a clean close");
        assert!(format!("{e:#}").contains("no PONG"), "{e:#}");
    }

    /// A relay that accepts TCP and then never finishes the upgrade
    /// must fail the dial, not park the dial loop.
    #[tokio::test]
    async fn a_relay_that_never_finishes_the_handshake_fails_the_dial() {
        let (listener, target) = stub_relay().await;
        tokio::spawn(async move {
            let _held = listener.accept().await.unwrap();
            std::future::pending::<()>().await;
        });
        let (tx, _rx) = mpsc::channel(1);
        let sink = plain_sink(tx);
        let tuning = Tuning {
            dial_timeout: Duration::from_millis(200),
            ..Tuning::DEFAULT
        };
        let dial = dial_once(
            &target,
            None,
            TEST_NODE.parse().unwrap(),
            &sink,
            None,
            1,
            &tuning,
        );
        let result = tokio::time::timeout(Duration::from_secs(5), dial)
            .await
            .expect("the dial must be bounded");
        let e = result.expect_err("a handshake that never completes is a failed dial");
        assert!(format!("{e:#}").contains("200ms"), "{e:#}");
    }

    /// The relay never rotates the connection and cannot tell when its
    /// stream ids are about to run out, so the node does it: the
    /// streams already open finish, the connection then ends cleanly
    /// (no outage), and the next dial gets a fresh one.
    #[tokio::test]
    async fn a_tunnel_rotates_before_the_relay_runs_out_of_stream_ids() {
        let (listener, target) = stub_relay().await;
        let again = TunnelUrl {
            tls: false,
            host: target.host.clone(),
            port: target.port,
        };
        let (tx, mut rx) = mpsc::channel(16);
        let sink = plain_sink(tx);
        let node: NodeId = TEST_NODE.parse().unwrap();
        // client stream ids are 1, 3, 5, ...: the third caller crosses
        let tuning = || Tuning {
            rotate_at: 5,
            ..Tuning::DEFAULT
        };
        let first = {
            let sink = sink.clone();
            tokio::spawn(
                async move { dial_once(&target, None, node, &sink, None, 1, &tuning()).await },
            )
        };

        // the relay: h2 client over the WebSocket, as node.rs does it
        let (tcp, _) = listener.accept().await.unwrap();
        let ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
        let (mut h2, conn) = h2_client_builder()
            .handshake::<_, bytes::Bytes>(WsByteStream::new(ws))
            .await
            .unwrap();
        let mut conn = tokio::spawn(conn);
        let mut callers = Vec::new();
        for _ in 0..3 {
            let request = http::Request::post("https://node/").body(()).unwrap();
            let (response, mut send) = h2.send_request(request, false).unwrap();
            send.send_data(bytes::Bytes::from_static(b"hello"), true)
                .unwrap();
            callers.push(response);
        }

        // the node hands each one to axum; read the greeting and hold
        // the connection, as a caller mid-request would
        let mut accepted = Vec::new();
        for _ in 0..3 {
            let (mut io, peer) = rx.recv().await.unwrap();
            let mut greeting = [0u8; 5];
            io.read_exact(&mut greeting).await.unwrap();
            assert_eq!(&greeting, b"hello");
            accepted.push((io, peer.stream.unwrap()));
        }
        accepted.sort_by_key(|(_, id)| *id);
        let ids: Vec<u32> = accepted.iter().map(|(_, id)| *id).collect();
        assert_eq!(ids, [1, 3, 5]);

        // answering the stream that crossed the mark must not take the
        // two opened before it down with it
        let reply = |(mut io, _): (DuplexStream, u32)| async move {
            io.write_all(b"world").await.unwrap();
        };
        reply(accepted.pop().unwrap()).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(300), &mut conn)
                .await
                .is_err(),
            "the connection must stay up while streams are in flight"
        );
        for open in accepted {
            reply(open).await;
        }
        for response in callers {
            let response = response.await.unwrap();
            assert_eq!(response.status(), http::StatusCode::OK);
            let mut body = response.into_body();
            let mut answer = Vec::new();
            while let Some(chunk) = body.data().await {
                answer.extend_from_slice(&chunk.unwrap());
            }
            assert_eq!(answer, b"world");
        }

        // with nothing in flight the node closes, cleanly on both ends...
        tokio::time::timeout(Duration::from_secs(5), conn)
            .await
            .expect("the connection must end once the streams are done")
            .unwrap()
            .expect("a rotation is a clean close for the relay");
        tokio::time::timeout(Duration::from_secs(5), first)
            .await
            .expect("the dial must return once the connection ended")
            .unwrap()
            .expect("a rotation is a clean close for the node, not an outage");

        // ...and the next dial is a fresh connection
        let second =
            tokio::spawn(
                async move { dial_once(&again, None, node, &sink, None, 1, &tuning()).await },
            );
        let (tcp, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
            .await
            .expect("the node must dial again")
            .unwrap();
        let ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
        let (_h2, _conn) = h2_client_builder()
            .handshake::<_, bytes::Bytes>(WsByteStream::new(ws))
            .await
            .expect("a fresh tunnel");
        second.abort();
    }
}
