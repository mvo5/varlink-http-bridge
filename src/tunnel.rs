// SPDX-License-Identifier: LGPL-2.1-or-later

//! Transport primitives for the varlink relay tunnel (see
//! README.relayd.md).
//!
//! The node dials out to the relay with a WebSocket and both ends then
//! run HTTP/2 over it with the roles reversed (relay as h2 client, node
//! as h2 server), because in standard HTTP/2 only the client may open
//! streams and here the dialing side must be the one accepting them.
//! [`WsByteStream`] provides the byte pipe that makes the reversal
//! possible, [`send_all`]/[`splice`] move data with h2 flow control as
//! the end-to-end backpressure, [`h2_server_builder`]/[`h2_client_builder`]
//! size the h2 windows so that backpressure stays per stream, and
//! [`MachineId::node_id`] derives the [`NodeId`] a node registers under.

use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, ready};
use std::time::{Duration, Instant};

use anyhow::{Context as _, bail};
use bytes::{Buf, Bytes};
use futures_util::{Sink, Stream};
use h2::{RecvStream, SendStream};
use log::{info, warn};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::error::{Error as WsError, ProtocolError};

/// A WebSocket treated as a plain byte stream.
///
/// Binary message payloads are the byte stream; a close frame, the end
/// of the WebSocket, or an abrupt connection loss is EOF; a text frame
/// is an error (only ever a confused peer or middlebox, same as
/// `run_proxy` in varlinkctl-http). Shutdown sends a close frame, so
/// the peer sees a clean EOF rather than a dropped connection.
///
/// The bridge has two older ws byte pipes (varlink-httpd's `/ws`
/// endpoint, varlinkctl-http's `run_proxy`); consolidating them onto
/// this type is future work.
pub struct WsByteStream<S> {
    ws: WebSocketStream<S>,
    // unread remainder of the last data message
    readbuf: Bytes,
}

impl<S> WsByteStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    pub fn new(ws: WebSocketStream<S>) -> Self {
        Self {
            ws,
            readbuf: Bytes::new(),
        }
    }
}

impl<S> AsyncRead for WsByteStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        loop {
            if !this.readbuf.is_empty() {
                let n = this.readbuf.len().min(buf.remaining());
                buf.put_slice(&this.readbuf[..n]);
                this.readbuf.advance(n);
                return Poll::Ready(Ok(()));
            }
            match ready!(Pin::new(&mut this.ws).poll_next(cx)) {
                Some(Ok(Message::Binary(data))) => this.readbuf = data,
                Some(Ok(Message::Text(_))) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unexpected text WebSocket frame",
                    )));
                }
                // tungstenite's read path answers pings itself, with
                // retry on a blocked socket; flushing here would bypass
                // that retry and could strand the pong
                Some(Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_))) => {}
                // EOF: a clean close, or the peer vanishing without a
                // close handshake; the latter is an expected event here
                // (middleboxes reap idle and long-lived connections), not
                // a protocol violation, and manifests as a FIN, an RST,
                // or a cut mid-frame depending on the middlebox
                Some(
                    Ok(Message::Close(_))
                    | Err(
                        WsError::Protocol(ProtocolError::ResetWithoutClosingHandshake)
                        | WsError::ConnectionClosed,
                    ),
                )
                | None => return Poll::Ready(Ok(())),
                Some(Err(WsError::Io(e)))
                    if matches!(
                        e.kind(),
                        io::ErrorKind::ConnectionReset | io::ErrorKind::UnexpectedEof
                    ) =>
                {
                    return Poll::Ready(Ok(()));
                }
                Some(Err(e)) => return Poll::Ready(Err(io::Error::other(e))),
            }
        }
    }
}

impl<S> AsyncWrite for WsByteStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        ready!(Pin::new(&mut this.ws).poll_ready(cx)).map_err(io::Error::other)?;
        Pin::new(&mut this.ws)
            .start_send(Message::Binary(Bytes::copy_from_slice(buf)))
            .map_err(io::Error::other)?;
        Poll::Ready(Ok(buf.len()))
    }

    // without this h2 splits every frame >= 1KB into separate writes,
    // i.e. separate allocated, masked WebSocket messages
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let total = bufs.iter().map(|b| b.len()).sum();
        if total == 0 {
            return Poll::Ready(Ok(0));
        }
        let this = self.get_mut();
        ready!(Pin::new(&mut this.ws).poll_ready(cx)).map_err(io::Error::other)?;
        let mut data = bytes::BytesMut::with_capacity(total);
        for buf in bufs {
            data.extend_from_slice(buf);
        }
        Pin::new(&mut this.ws)
            .start_send(Message::Binary(data.freeze()))
            .map_err(io::Error::other)?;
        Poll::Ready(Ok(total))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().ws)
            .poll_flush(cx)
            .map_err(io::Error::other)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().ws)
            .poll_close(cx)
            .map_err(io::Error::other)
    }
}

/// How long one side may fail to make progress before it is worth a log
/// line: a peer that stopped reading, or a local service that hung, is
/// the shape of trouble that used to be invisible until callers
/// complained. Generous, because a slow link is not a stall.
#[cfg(not(test))]
const STALL_WARN: Duration = Duration::from_secs(30);
// so that the tests can actually stall a stream and watch it recover
#[cfg(test)]
const STALL_WARN: Duration = Duration::from_millis(100);

/// Why [`splice`] (or [`send_all`]) gave up, by which side failed. The
/// same code runs on both tunnel ends, so the sides are named relative
/// to it: `Local` is the byte stream it was handed (the caller's TCP on
/// the relay, the hand-off to the server on the node), `Peer` is the h2
/// stream to the other tunnel end. That is what decides how loud the
/// end of a stream is: a caller hanging up is routine, a failing tunnel
/// is news.
#[derive(Debug)]
pub enum SpliceError {
    Local(anyhow::Error),
    Peer(anyhow::Error),
}

impl SpliceError {
    /// True if the local byte stream failed rather than the h2 peer.
    #[must_use]
    pub fn is_local(&self) -> bool {
        matches!(self, Self::Local(_))
    }

    /// The failure itself, whichever side it is from.
    #[must_use]
    pub fn cause(&self) -> &anyhow::Error {
        match self {
            Self::Local(e) | Self::Peer(e) => e,
        }
    }

    /// Add context to the cause, keeping the side.
    #[must_use]
    pub fn context<C>(self, context: C) -> Self
    where
        C: std::fmt::Display + Send + Sync + 'static,
    {
        match self {
            Self::Local(e) => Self::Local(e.context(context)),
            Self::Peer(e) => Self::Peer(e.context(context)),
        }
    }
}

// transparent: the cause already says which side ("reading local
// stream", "receiving h2 data"), the variant is there to be matched on
impl std::fmt::Display for SpliceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self.cause(), f)
    }
}

impl std::error::Error for SpliceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.cause().source()
    }
}

/// Send `data` on an h2 stream, never ahead of the peer's flow-control
/// window: waits for capacity and sends in whatever chunks the window
/// allows. This is the sending half of the end-to-end backpressure.
///
/// `who` names this stream in the log, see [`splice`].
///
/// # Errors
/// Returns [`SpliceError::Peer`] if the stream is reset or closed by
/// the peer; nothing here can fail on the local side.
pub async fn send_all(
    send: &mut SendStream<Bytes>,
    mut data: Bytes,
    who: &str,
) -> Result<(), SpliceError> {
    let mut stalled: Option<std::time::Instant> = None;
    while !data.is_empty() {
        send.reserve_capacity(data.len());
        // dropping this future only stops polling a capacity check, so
        // the timeout cannot lose data the way a cancelled write would
        let wait = std::future::poll_fn(|cx| send.poll_capacity(cx));
        let available = match tokio::time::timeout(STALL_WARN, wait).await {
            Ok(available) => available,
            Err(_) => {
                if stalled.is_none() {
                    stalled = Some(std::time::Instant::now());
                    warn!(
                        "{who}: peer has not taken any data for {}s, it is not reading; \
                         other streams on this tunnel are unaffected",
                        STALL_WARN.as_secs()
                    );
                }
                continue;
            }
        }
        .context("h2 stream closed while waiting for send capacity")
        .map_err(SpliceError::Peer)?
        .context("h2 stream error while waiting for send capacity")
        .map_err(SpliceError::Peer)?;
        if let Some(since) = stalled.take() {
            info!("{who}: peer resumed reading after {:?}", since.elapsed());
        }
        if available == 0 {
            continue;
        }
        send.send_data(data.split_to(available.min(data.len())), false)
            .context("sending h2 data")
            .map_err(SpliceError::Peer)?;
    }
    Ok(())
}

/// How many bytes one spliced stream moved, local-to-h2 and back. Only
/// the log cares, and only to answer "did anything actually flow?".
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Transferred {
    pub sent: u64,
    pub received: u64,
}

/// Splice a byte stream onto an h2 stream, both directions, until both
/// sides reached EOF or either failed.
///
/// Backpressure is preserved end to end: bytes from `io` are sent only
/// within the h2 window ([`send_all`]), and the h2 window is only opened
/// (`release_capacity`) after the bytes were written to `io`, so a slow
/// reader on either side stalls the sender instead of growing a buffer.
///
/// `who` names this stream in the log; give it the h2 stream id, which
/// both tunnel ends see, so their lines can be matched up.
///
/// # Errors
/// Returns an error if either side fails, saying which; EOFs in both
/// directions are a clean completion.
pub async fn splice<S>(
    io: S,
    mut recv: RecvStream,
    mut send: SendStream<Bytes>,
    who: &str,
) -> Result<Transferred, SpliceError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // the local read buffer: topped up by READ_CHUNK once the room left
    // is under READ_FLOOR, so one allocation serves many small reads (a
    // varlink message is a few hundred bytes) and a full read gets a
    // fresh one
    const READ_CHUNK: usize = 16 * 1024;
    const READ_FLOOR: usize = 4 * 1024;

    let (mut rd, mut wr) = tokio::io::split(io);

    // io -> h2; "up"/"down" are relative to this splice: the same code
    // runs on both tunnel ends, where the global directions invert
    let up = async {
        let mut sent = 0;
        let mut buf = bytes::BytesMut::new();
        loop {
            // buf is empty here, its capacity is the room; there must
            // be some before every read, because read_buf on a full
            // BytesMut returns Ok(0), which would read as a false EOF
            if buf.capacity() < READ_FLOOR {
                buf.reserve(READ_CHUNK);
            }
            let n = rd
                .read_buf(&mut buf)
                .await
                .context("reading local stream")
                .map_err(SpliceError::Local)?;
            if n == 0 {
                send.send_data(Bytes::new(), true)
                    .context("sending h2 end-of-stream")
                    .map_err(SpliceError::Peer)?;
                return Ok::<_, SpliceError>(sent);
            }
            sent += n as u64;
            // split().freeze() hands the chunk to h2 without recopying
            send_all(&mut send, buf.split().freeze(), who).await?;
        }
    };

    // h2 -> io
    let down = async {
        let mut received = 0;
        let mut stalled: Option<std::time::Instant> = None;
        while let Some(chunk) = recv.data().await {
            let chunk = chunk
                .context("receiving h2 data")
                .map_err(SpliceError::Peer)?;
            // write_all must never be dropped mid-write -- it would
            // leave an unknown number of bytes written -- so the stall
            // timer runs beside it and the write keeps its state
            let write = wr.write_all(&chunk);
            tokio::pin!(write);
            loop {
                tokio::select! {
                    result = &mut write => {
                        result
                            .context("writing local stream")
                            .map_err(SpliceError::Local)?;
                        break;
                    }
                    () = tokio::time::sleep(STALL_WARN), if stalled.is_none() => {
                        stalled = Some(std::time::Instant::now());
                        warn!(
                            "{who}: local side has not taken any data for {}s, it is \
                             hung or not reading; other streams on this tunnel are \
                             unaffected",
                            STALL_WARN.as_secs()
                        );
                    }
                }
            }
            if let Some(since) = stalled.take() {
                info!(
                    "{who}: local side resumed reading after {:?}",
                    since.elapsed()
                );
            }
            received += chunk.len() as u64;
            recv.flow_control()
                .release_capacity(chunk.len())
                .context("releasing h2 window")
                .map_err(SpliceError::Peer)?;
        }
        wr.shutdown()
            .await
            .context("closing local stream")
            .map_err(SpliceError::Local)?;
        Ok::<_, SpliceError>(received)
    };

    let (sent, received) = tokio::try_join!(up, down)?;
    Ok(Transferred { sent, received })
}

/// Per-stream h2 receive window, matching the buffer each spliced
/// stream has on the far end: a stream cannot hold more than this much
/// of the connection window unreleased, because `splice` releases
/// window as it drains bytes into that buffer.
///
/// This is also the ceiling on what one caller can push through a
/// stream over a long link, at roughly window/RTT. Fine for a varlink
/// call and its reply, which fit in a packet or two; it is bulk
/// transfer that pays. One stream, one way, 16MiB:
///
/// | window | loopback | 50ms RTT |
/// | ------ | -------- | -------- |
/// | 32KiB (this) | 136MB/s | 0.60MB/s |
/// | 256KiB | -- | 4.89MB/s |
/// | 1MiB | -- | 17.65MB/s |
///
/// TODO: raise this to 256KiB or 1MiB if the tunnel ever carries bulk
/// data (a file transfer, SFTP: OpenSSH's sftp keeps 64 requests of
/// 32KiB in flight, so it wants ~2MiB of window and gets none of that
/// pipelining here). It is not a one-line change: [`CONNECTION_WINDOW`]
/// is this times [`MAX_TUNNEL_STREAMS`], so 1MiB would promise 256MiB
/// per tunnel and direction. Either trade concurrency for it (1MiB x 32
/// streams keeps the same 32MiB), or raise the window per connection at
/// runtime once a stream turns out to be moving volume
/// (`h2::server::Connection::set_initial_window_size`, which is how
/// hyper does BDP estimation) and leave control traffic small.
pub const STREAM_WINDOW: u32 = 32 * 1024;

/// Streams per tunnel, i.e. callers one node serves at once; further
/// callers wait for a slot.
pub const MAX_TUNNEL_STREAMS: u32 = 256;

/// Connection-level h2 receive window, with room for every stream to
/// hold its full [`STREAM_WINDOW`]: with h2's default of one stream
/// window per connection, a single caller whose local service hangs
/// exhausts it and every other caller on the tunnel starves. A promise,
/// not an allocation: only a tunnel whose callers all wedge at once
/// holds this much.
pub const CONNECTION_WINDOW: u32 = STREAM_WINDOW * MAX_TUNNEL_STREAMS;

/// Stream count of one tunnel, reported when it crosses a tier: running
/// into [`MAX_TUNNEL_STREAMS`] is silent by itself, callers just queue.
/// A tier is reported once per crossing and re-armed only once usage
/// drops clear of it, so a tunnel hovering at a tier stays quiet.
///
/// On the relay it is also the admission gate: a caller takes one of
/// the node's slots ([`StreamSlot::acquire`]) before it opens an h2
/// stream, and waits in line here when all are taken. Left to h2, the
/// wait would be its unbounded `pending_open` queue, where a caller
/// that gave up still leaves its HEADERS behind and is drained through
/// the node's slots as a phantom stream, one per abandoned waiter.
#[derive(Debug)]
pub struct StreamLoad {
    active: AtomicU32,
    // in percent, 0 for none
    reported: AtomicU32,
    // the node's advertised limit, mirrored so callers queue here
    slots: Arc<Semaphore>,
    queue: Mutex<Queue>,
}

impl Default for StreamLoad {
    fn default() -> Self {
        Self {
            active: AtomicU32::new(0),
            reported: AtomicU32::new(0),
            slots: Arc::new(Semaphore::new(MAX_TUNNEL_STREAMS as usize)),
            queue: Mutex::new(Queue {
                waiting: 0,
                since: Instant::now(),
                served: 0,
                gave_up: 0,
            }),
        }
    }
}

/// The callers waiting for a slot. One run of them, from the first to
/// queue until the last leaves, is one event: a line when it starts,
/// one with the numbers when it drains, nothing per caller in between.
#[derive(Debug)]
struct Queue {
    waiting: u32,
    since: Instant,
    served: u32,
    gave_up: u32,
}

/// In percent of [`MAX_TUNNEL_STREAMS`], ascending.
const TIERS: [u32; 4] = [50, 75, 90, 100];

/// Percentage points usage must drop below a tier before crossing it is
/// news again; without it a hovering tunnel reports on every stream.
const TIER_HYSTERESIS: u32 = 10;

#[derive(Debug, PartialEq, Eq)]
pub struct LoadReport {
    pub active: u32,
    /// in percent of [`MAX_TUNNEL_STREAMS`]
    pub tier: u32,
}

impl LoadReport {
    /// From 90% on an operator is needed: the tunnel is nearly out of
    /// slots, or callers are already queueing.
    #[must_use]
    pub fn level(&self) -> log::Level {
        if self.tier >= 90 {
            log::Level::Warn
        } else {
            log::Level::Info
        }
    }
}

impl StreamLoad {
    pub fn active(&self) -> u32 {
        self.active.load(Relaxed)
    }

    /// Callers waiting for a slot right now.
    pub fn queued(&self) -> u32 {
        self.queue().waiting
    }

    // the lock only guards counters, so a panic elsewhere while it was
    // held is no reason to take every later caller down with it
    fn queue(&self) -> std::sync::MutexGuard<'_, Queue> {
        self.queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn enqueue<'a>(&'a self, who: &'a str) -> Queued<'a> {
        let mut queue = self.queue();
        queue.waiting += 1;
        if queue.waiting == 1 {
            queue.since = Instant::now();
            queue.served = 0;
            queue.gave_up = 0;
            info!(
                "{who}: all {MAX_TUNNEL_STREAMS} streams are in use, callers are queueing for a slot"
            );
        }
        Queued {
            load: self,
            who,
            served: false,
        }
    }

    fn dequeue(&self, who: &str, served: bool) {
        let mut queue = self.queue();
        queue.waiting -= 1;
        if served {
            queue.served += 1;
        } else {
            queue.gave_up += 1;
        }
        if queue.waiting == 0 {
            info!(
                "{who}: the queue drained after {:?}, {} callers were served from it, {} gave up",
                queue.since.elapsed(),
                queue.served,
                queue.gave_up
            );
        }
    }

    fn opened(&self) -> Option<LoadReport> {
        let active = self.active.fetch_add(1, Relaxed) + 1;
        let tier = tier_reached(active, 0);
        // fetch_max, so two streams opening at once report once
        (tier > self.reported.fetch_max(tier, Relaxed)).then_some(LoadReport { active, tier })
    }

    fn closed(&self) {
        let active = self.active.fetch_sub(1, Relaxed) - 1;
        self.reported
            .fetch_min(tier_reached(active, TIER_HYSTERESIS), Relaxed);
    }
}

fn tier_reached(active: u32, margin: u32) -> u32 {
    let percent = active * 100 / MAX_TUNNEL_STREAMS + margin;
    TIERS
        .into_iter()
        .rev()
        .find(|tier| percent >= *tier)
        .unwrap_or(0)
}

/// One caller in the line for a slot, counted there until it is served
/// or gives up; a dropped wait (the caller's timeout) is giving up.
struct Queued<'a> {
    load: &'a StreamLoad,
    who: &'a str,
    served: bool,
}

impl Queued<'_> {
    fn served(mut self) {
        self.served = true;
    }
}

impl Drop for Queued<'_> {
    fn drop(&mut self) {
        self.load.dequeue(self.who, self.served);
    }
}

/// A stream counted in a [`StreamLoad`] until dropped, so that no exit
/// path can leak the count or, on the relay, the node's slot it holds.
#[derive(Debug)]
pub struct StreamSlot {
    load: Arc<StreamLoad>,
    // None on the node, which has no gate of its own (see `open`)
    _permit: Option<OwnedSemaphorePermit>,
}

impl StreamSlot {
    /// The node side: h2 already holds the relay to the advertised
    /// limit, so an accepted stream only counts, it never waits.
    pub fn open(load: &Arc<StreamLoad>) -> (Self, Option<LoadReport>) {
        Self::counted(load, None)
    }

    /// The relay side: take one of the node's slots, waiting in line
    /// when all are taken. The wait is unbounded here, the caller puts
    /// its timeout around it and dropping the future leaves the line.
    /// `who` names the node in the queue's two log lines.
    ///
    /// # Panics
    /// If the gate were closed, which nothing ever does.
    pub async fn acquire(load: &Arc<StreamLoad>, who: &str) -> (Self, Option<LoadReport>) {
        let permit = if let Ok(permit) = Arc::clone(&load.slots).try_acquire_owned() {
            permit
        } else {
            let queued = load.enqueue(who);
            let permit = Arc::clone(&load.slots)
                .acquire_owned()
                .await
                .expect("the slot gate is never closed");
            queued.served();
            permit
        };
        Self::counted(load, Some(permit))
    }

    fn counted(
        load: &Arc<StreamLoad>,
        permit: Option<OwnedSemaphorePermit>,
    ) -> (Self, Option<LoadReport>) {
        let report = load.opened();
        (
            Self {
                load: Arc::clone(load),
                _permit: permit,
            },
            report,
        )
    }
}

impl Drop for StreamSlot {
    fn drop(&mut self) {
        // runs before the permit is released, so the next caller through
        // the gate never sees a count above the limit
        self.load.closed();
    }
}

/// h2 settings for the node end (the server, roles being reversed). Only
/// the accepting side can advertise the stream limit.
pub fn h2_server_builder() -> h2::server::Builder {
    let mut builder = h2::server::Builder::new();
    builder
        .initial_window_size(STREAM_WINDOW)
        .initial_connection_window_size(CONNECTION_WINDOW)
        .max_concurrent_streams(MAX_TUNNEL_STREAMS);
    builder
}

/// h2 settings for the relay end: a caller that stops reading stalls its
/// stream here the way a hung local service does on the node, so the
/// same windows apply.
pub fn h2_client_builder() -> h2::client::Builder {
    let mut builder = h2::client::Builder::new();
    builder
        .initial_window_size(STREAM_WINDOW)
        .initial_connection_window_size(CONNECTION_WINDOW);
    builder
}

/// URL path a node's dial-out connects to on the relay.
pub const TUNNEL_PATH: &str = "/v1/tunnel";

/// Application id for [`MachineId::node_id`], in the sense of
/// `sd_id128_get_machine_app_specific(3)`. Fixed forever: changing it
/// renames every node in every deployment at once.
const TUNNEL_APP_ID: u128 = 0x7a16_4c93_87b5_48ae_aaa0_8cbf_80d2_7865;

/// The [`NodeId`] this machine registers under, from `/etc/machine-id`
/// and, for an additional bridge instance on the same host, its label.
///
/// # Errors
/// Returns an error if `/etc/machine-id` cannot be read or parsed
/// (empty and "uninitialized" are valid states per `machine-id(5)`).
pub fn local_node_id(instance: Option<&str>) -> anyhow::Result<NodeId> {
    let text = std::fs::read_to_string("/etc/machine-id").context("reading /etc/machine-id")?;
    let machine_id: MachineId = text.trim().parse()?;
    match instance {
        Some(label) => machine_id.instance_node_id(label),
        None => machine_id.node_id(),
    }
}

/// The machine id from `/etc/machine-id`.
///
/// Confidential per `machine-id(5)` and must never go on the wire; the
/// newtype enforces that: no `Display`, a redacting `Debug`, and the
/// only way out is the derivation in [`MachineId::node_id`].
pub struct MachineId(u128);

impl std::fmt::Debug for MachineId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MachineId(<redacted>)")
    }
}

impl std::str::FromStr for MachineId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // never echo the input: on a near-valid parse (untrimmed
        // newline, one bad char) it IS the confidential value
        Ok(Self(
            parse_id128(s).context("invalid machine id (value redacted)")?,
        ))
    }
}

impl MachineId {
    /// The id this machine registers under at the relay: a
    /// reimplementation of `sd_id128_get_machine_app_specific(3)` with
    /// the tunnel's fixed application id, so
    /// `systemd-id128 machine-id -a <app-id>` prints the same value.
    ///
    /// # Errors
    /// Returns an error if the HMAC computation fails.
    pub fn node_id(&self) -> anyhow::Result<NodeId> {
        Ok(NodeId(hmac_id128(
            &self.0.to_be_bytes(),
            &TUNNEL_APP_ID.to_be_bytes(),
        )?))
    }

    /// The id an additional bridge instance on this host registers
    /// under: the instance's own app id, derived from the fixed one
    /// plus the label, put through the same machine-specific
    /// derivation. `systemd-id128 machine-id -a <instance-app-id>`
    /// still prints the same value.
    ///
    /// # Errors
    /// Returns an error if the HMAC computation fails.
    pub fn instance_node_id(&self, label: &str) -> anyhow::Result<NodeId> {
        let instance_app_id = hmac_id128(&TUNNEL_APP_ID.to_be_bytes(), label.as_bytes())?;
        Ok(NodeId(hmac_id128(
            &self.0.to_be_bytes(),
            &instance_app_id.to_be_bytes(),
        )?))
    }
}

/// One `sd_id128_get_app_specific(3)` step: HMAC-SHA256, first 16
/// bytes, v4-UUID fixup.
fn hmac_id128(key: &[u8], msg: &[u8]) -> anyhow::Result<u128> {
    // the libsystemd crate implements this too, but would currently
    // add nom plus a duplicate nix major; revisit once the bridge
    // needs more of libsystemd than this one derivation
    use openssl::{hash::MessageDigest, pkey::PKey, sign::Signer};

    let key = PKey::hmac(key).context("creating HMAC key")?;
    let mut signer = Signer::new(MessageDigest::sha256(), &key).context("creating signer")?;
    signer.update(msg).context("hashing")?;
    let mac = signer.sign_to_vec().context("computing HMAC")?;

    let mut id = [0u8; 16];
    id.copy_from_slice(&mac[..16]);
    // the sd_id128 v4-UUID fixup: version 4, variant DCE
    id[6] = (id[6] & 0x0F) | 0x40;
    id[8] = (id[8] & 0x3F) | 0x80;
    Ok(u128::from_be_bytes(id))
}

/// The id a node registers under at the relay, from
/// [`MachineId::node_id`]. Public by design; displays as 32 lowercase
/// hex chars, the `machine-id(5)` format.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(u128);

impl std::str::FromStr for NodeId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // node ids are public, echoing the input is fine here
        Ok(Self(parse_id128(s).with_context(|| {
            format!("not a 128-bit id in hex format: {s:?}")
        })?))
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:032x}", self.0)
    }
}

impl std::fmt::Debug for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NodeId({self})")
    }
}

/// Parse an id128 (32 hex chars, `machine-id(5)` format). The error
/// never contains the input, which may be a confidential machine id;
/// callers add their own context.
fn parse_id128(s: &str) -> anyhow::Result<u128> {
    // from_str_radix alone would tolerate a leading '+'
    if s.len() != 32 || s.starts_with('+') {
        bail!("not 32 hex chars");
    }
    u128::from_str_radix(s, 16).context("not 32 hex chars")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{DuplexStream, duplex};
    use tokio_tungstenite::tungstenite::protocol::Role;

    async fn ws_pair() -> (WsByteStream<DuplexStream>, WsByteStream<DuplexStream>) {
        let (a, b) = duplex(4096);
        let (client, server) = tokio::join!(
            WebSocketStream::from_raw_socket(a, Role::Client, None),
            WebSocketStream::from_raw_socket(b, Role::Server, None),
        );
        (WsByteStream::new(client), WsByteStream::new(server))
    }

    /// h2 client over `ws` with one stream opened: the connection
    /// driver task, the kept-alive `SendRequest`, the response future,
    /// and the stream's send half.
    async fn h2_connect(
        ws: WsByteStream<DuplexStream>,
    ) -> (
        tokio::task::JoinHandle<Result<(), h2::Error>>,
        h2::client::SendRequest<Bytes>,
        h2::client::ResponseFuture,
        SendStream<Bytes>,
    ) {
        let (send_request, conn) = h2::client::handshake(ws).await.unwrap();
        let conn = tokio::spawn(conn);
        let mut send_request = send_request.ready().await.unwrap();
        let request = http::Request::builder()
            .uri("https://node.invalid/")
            .body(())
            .unwrap();
        let (response, send) = send_request.send_request(request, false).unwrap();
        (conn, send_request, response, send)
    }

    #[tokio::test]
    async fn text_frames_are_an_error() {
        use futures_util::SinkExt as _;
        let (a, b) = duplex(4096);
        let (mut client, server) = tokio::join!(
            WebSocketStream::from_raw_socket(a, Role::Client, None),
            WebSocketStream::from_raw_socket(b, Role::Server, None),
        );
        let mut server = WsByteStream::new(server);
        client.send(Message::Text("nope".into())).await.unwrap();
        let mut buf = [0u8; 4];
        let err = server.read_exact(&mut buf).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn ws_byte_stream_roundtrip_and_eof() {
        let (mut client, mut server) = ws_pair().await;

        // a small exchange in both directions
        client.write_all(b"ping").await.unwrap();
        client.flush().await.unwrap();
        let mut small = [0u8; 4];
        server.read_exact(&mut small).await.unwrap();
        assert_eq!(&small, b"ping");
        server.write_all(b"pong").await.unwrap();
        server.flush().await.unwrap();
        client.read_exact(&mut small).await.unwrap();
        assert_eq!(&small, b"pong");

        // bulk transfer larger than every buffer in the path, then EOF
        let data: Vec<u8> = (0..256 * 1024)
            .map(|i| u8::try_from(i % 251).unwrap())
            .collect();
        let expected = data.clone();
        let writer = tokio::spawn(async move {
            client.write_all(&data).await.unwrap();
            client.shutdown().await.unwrap();
        });
        let mut got = Vec::new();
        server.read_to_end(&mut got).await.unwrap();
        writer.await.unwrap();
        assert_eq!(got, expected);
    }

    /// The node side of a tunnel: h2 server, splicing the one stream
    /// onto a local echo. Ends once the client connection is gone, with
    /// what the splice returned.
    fn echo_node(
        server_ws: WsByteStream<DuplexStream>,
    ) -> tokio::task::JoinHandle<Result<Transferred, SpliceError>> {
        tokio::spawn(async move {
            let mut conn = h2::server::handshake(server_ws).await.unwrap();
            let (req, mut respond) = conn.accept().await.unwrap().unwrap();
            let body = req.into_body();
            let send = respond
                .send_response(http::Response::new(()), false)
                .unwrap();
            let (io, echo) = duplex(1024);
            let echo = tokio::spawn(async move {
                let (mut rd, mut wr) = tokio::io::split(echo);
                tokio::io::copy(&mut rd, &mut wr).await.unwrap();
                wr.shutdown().await.unwrap();
            });
            // the stream runs in its own task; accept() must keep being
            // polled meanwhile, it also drives the connection I/O
            let stream = tokio::spawn(async move { splice(io, body, send, "test node").await });
            // ends on connection loss: None on a clean close, Err when
            // the transport just vanished
            while let Some(Ok(next)) = conn.accept().await {
                drop(next);
            }
            let result = stream.await.unwrap();
            echo.await.unwrap();
            result
        })
    }

    /// The full stack: caller bytes -> h2 stream -> WebSocket -> h2
    /// server -> local echo, and back.
    #[tokio::test(flavor = "multi_thread")]
    async fn h2_over_ws_splice_end_to_end() {
        let (client_ws, server_ws) = ws_pair().await;
        let node = echo_node(server_ws);

        // relay side: h2 client, splicing the caller onto the stream
        let (client_conn, send_request, response, send) = h2_connect(client_ws).await;
        let recv = response.await.unwrap().into_body();
        let (io, mut caller) = duplex(1024);
        let relay = tokio::spawn(async move { splice(io, recv, send, "test relay").await });

        let msg = b"hello through four layers";
        caller.write_all(msg).await.unwrap();
        let mut got = [0u8; 25];
        caller.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, msg);

        drop(caller); // EOF ripples through all layers and ends both splices
        let step = std::time::Duration::from_secs(3);
        tokio::time::timeout(step, relay)
            .await
            .expect("relay splice must end on caller EOF")
            .unwrap()
            .unwrap();
        // end the connection the way the real world does: abruptly;
        // the node must treat that as EOF and wind down
        client_conn.abort();
        drop(send_request);
        tokio::time::timeout(step, node)
            .await
            .expect("node side must end once the client is gone")
            .unwrap()
            .unwrap();
    }

    /// Varlink traffic is many small messages; the splice read buffer
    /// hands each read to h2 as its own chunk and tops itself up only
    /// when nearly spent, and however it carves up its capacity every
    /// byte must come out the other end, in order.
    #[tokio::test(flavor = "multi_thread")]
    async fn many_small_messages_arrive_intact() {
        let (client_ws, server_ws) = ws_pair().await;
        let node = echo_node(server_ws);

        let (client_conn, send_request, response, send) = h2_connect(client_ws).await;
        let recv = response.await.unwrap().into_body();
        let (io, mut caller) = duplex(1024);
        let relay = tokio::spawn(async move { splice(io, recv, send, "test relay").await });

        // one round trip per message keeps each splice read to one
        // message, so the buffer is refilled many times over; well over
        // a chunk's worth of them in total
        let mut total = 0;
        for i in 0..4096u32 {
            let msg: Vec<u8> = (0..=i % 200)
                .map(|j| u8::try_from((i + j) % 251).unwrap())
                .collect();
            caller.write_all(&msg).await.unwrap();
            let mut got = vec![0u8; msg.len()];
            caller.read_exact(&mut got).await.unwrap();
            assert_eq!(got, msg, "message {i} was mangled");
            total += msg.len() as u64;
        }

        drop(caller);
        let step = std::time::Duration::from_secs(3);
        let moved = tokio::time::timeout(step, relay)
            .await
            .expect("relay splice must end on caller EOF")
            .unwrap()
            .unwrap();
        assert_eq!(
            moved,
            Transferred {
                sent: total,
                received: total
            }
        );
        client_conn.abort();
        drop(send_request);
        tokio::time::timeout(step, node)
            .await
            .expect("node side must end once the client is gone")
            .unwrap()
            .unwrap();
    }

    /// A local stream whose read fails at once: the caller's TCP reset
    /// under the relay.
    struct ResetOnRead;

    impl AsyncRead for ResetOnRead {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Ready(Err(io::ErrorKind::ConnectionReset.into()))
        }
    }

    impl AsyncWrite for ResetOnRead {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Ok(buf.len()))
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    /// The local side going away is the caller's own affair and must be
    /// reported as such, so the relay can keep it at debug.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_failing_local_side_is_a_local_error() {
        let (client_ws, server_ws) = ws_pair().await;
        let node = echo_node(server_ws);

        let (client_conn, send_request, response, send) = h2_connect(client_ws).await;
        let recv = response.await.unwrap().into_body();
        let err = splice(ResetOnRead, recv, send, "test relay")
            .await
            .unwrap_err();
        assert!(err.is_local(), "expected a local error, got {err:?}");
        // the message names the cause, whether printed directly or
        // through anyhow
        assert_eq!(format!("{err:#}"), "reading local stream: connection reset");
        assert_eq!(
            format!("{:#}", anyhow::Error::from(err)),
            "reading local stream: connection reset"
        );

        client_conn.abort();
        drop(send_request);
        tokio::time::timeout(std::time::Duration::from_secs(3), node)
            .await
            .expect("node side must end once the client is gone")
            .unwrap()
            .unwrap_err();
    }

    /// The h2 peer resetting the stream is the tunnel's affair, and
    /// must not be blamed on the local side.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_failing_peer_is_a_peer_error() {
        let (client_ws, server_ws) = ws_pair().await;

        // node side: takes the first bytes, then resets the stream. The
        // relay has its response by then, so the reset can only land in
        // the splice
        let node = tokio::spawn(async move {
            let mut conn = h2::server::handshake(server_ws).await.unwrap();
            let (req, mut respond) = conn.accept().await.unwrap().unwrap();
            let mut body = req.into_body();
            let mut send = respond
                .send_response(http::Response::new(()), false)
                .unwrap();
            let first = async {
                body.data().await.unwrap().unwrap();
                send.send_reset(h2::Reason::INTERNAL_ERROR);
            };
            let drive = async {
                while let Some(Ok(next)) = conn.accept().await {
                    drop(next);
                }
            };
            tokio::join!(first, drive);
        });

        let (client_conn, send_request, response, send) = h2_connect(client_ws).await;
        let recv = response.await.unwrap().into_body();
        let (io, mut caller) = duplex(1024);
        let relay = tokio::spawn(async move { splice(io, recv, send, "test relay").await });
        caller.write_all(b"hello?").await.unwrap();

        let step = std::time::Duration::from_secs(3);
        let err = tokio::time::timeout(step, relay)
            .await
            .expect("relay splice must end on the reset")
            .unwrap()
            .unwrap_err();
        assert!(!err.is_local(), "expected a peer error, got {err:?}");
        assert!(
            format!("{err:#}").starts_with("receiving h2 data: "),
            "unexpected message: {err:#}"
        );

        client_conn.abort();
        drop(send_request);
        tokio::time::timeout(step, node)
            .await
            .expect("node side must end once the client is gone")
            .unwrap();
    }

    /// A stalled local side must only cost time, never data: the stall
    /// timer runs beside the write rather than cancelling it, because a
    /// cancelled `write_all` leaves an unknown number of bytes written.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_stalled_local_side_loses_nothing() {
        const PAYLOAD: usize = 256 * 1024;
        let (client_ws, server_ws) = ws_pair().await;

        // the node end: splice the stream onto a local peer that stops
        // reading for several stall windows in the middle
        let (io, mut peer) = duplex(4096);
        let node = tokio::spawn(async move {
            let mut conn = h2::server::handshake(server_ws).await.unwrap();
            let (req, mut respond) = conn.accept().await.unwrap().unwrap();
            let body = req.into_body();
            let send = respond
                .send_response(http::Response::new(()), false)
                .unwrap();
            let stream = tokio::spawn(async move { splice(io, body, send, "stall test").await });
            while let Some(Ok(next)) = conn.accept().await {
                drop(next);
            }
            stream.await.unwrap()
        });

        let (conn, send_request, response, mut send) = h2_connect(client_ws).await;
        let (_conn, _send_request) = (conn, send_request);
        let sender = tokio::spawn(async move {
            let data: Vec<u8> = (0..PAYLOAD)
                .map(|i| u8::try_from(i % 251).unwrap())
                .collect();
            let expected = data.clone();
            send_all(&mut send, Bytes::from(data), "stall test")
                .await
                .unwrap();
            send.send_data(Bytes::new(), true).unwrap();
            expected
        });

        let mut got = Vec::new();
        let mut head = vec![0u8; 1024];
        peer.read_exact(&mut head).await.unwrap();
        got.extend_from_slice(&head);
        // long enough for both sides to notice and log the stall
        tokio::time::sleep(STALL_WARN * 3).await;
        peer.read_to_end(&mut got).await.unwrap();

        let expected = sender.await.unwrap();
        assert_eq!(got.len(), expected.len(), "a stall must not lose bytes");
        assert_eq!(got, expected, "nor reorder or duplicate them");

        // wind the stream down: the local peer going away ends the
        // other direction, and the node's accept loop only returns
        // once the h2 client is gone
        drop(peer);
        drop(response);
        drop(_send_request);
        _conn.abort();
        let moved = tokio::time::timeout(std::time::Duration::from_secs(5), node)
            .await
            .expect("the spliced stream must finish once both ends are gone")
            .unwrap()
            .unwrap();
        assert_eq!(moved.received, PAYLOAD as u64);
    }

    /// The point of `send_all`: with a closed window the sender must
    /// stall instead of buffering, and every byte must arrive once the
    /// receiver opens the window by consuming.
    #[tokio::test(flavor = "multi_thread")]
    async fn send_all_stalls_until_the_window_opens() {
        const PAYLOAD: usize = 64 * 1024;
        let (client_ws, server_ws) = ws_pair().await;

        let (drain_tx, drain_rx) = tokio::sync::oneshot::channel::<()>();
        let node = tokio::spawn(async move {
            let mut conn = h2::server::Builder::new()
                .initial_window_size(1024)
                .handshake::<_, Bytes>(server_ws)
                .await
                .unwrap();
            let (req, _respond) = conn.accept().await.unwrap().unwrap();
            let mut body = req.into_body();
            let drive = async {
                while let Some(next) = conn.accept().await {
                    drop(next);
                }
            };
            let read = async {
                drain_rx.await.unwrap();
                let mut total = 0;
                while let Some(chunk) = body.data().await {
                    let chunk = chunk.unwrap();
                    total += chunk.len();
                    body.flow_control().release_capacity(chunk.len()).unwrap();
                }
                total
            };
            tokio::select! {
                total = read => total,
                () = drive => panic!("connection must outlive the body"),
            }
        });

        let (_conn, _send_request, _response, mut send) = h2_connect(client_ws).await;

        let sender = tokio::spawn(async move {
            send_all(&mut send, Bytes::from(vec![0x42; PAYLOAD]), "test sender")
                .await
                .unwrap();
            send.send_data(Bytes::new(), true).unwrap();
        });

        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert!(
            !sender.is_finished(),
            "sender must stall on the closed window, not buffer"
        );

        drain_tx.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            sender.await.unwrap();
            assert_eq!(node.await.unwrap(), PAYLOAD);
        })
        .await
        .expect("draining must unblock the sender");
    }

    #[test]
    fn stream_load_reports_every_tier_once() {
        use log::Level;
        let load = Arc::new(StreamLoad::default());
        let cap = MAX_TUNNEL_STREAMS;
        let mut held = Vec::new();
        let mut reports = Vec::new();
        for _ in 0..cap {
            let (slot, report) = StreamSlot::open(&load);
            held.push(slot);
            reports.extend(report);
        }
        assert_eq!(load.active(), cap);

        assert_eq!(
            reports.iter().map(|r| r.tier).collect::<Vec<_>>(),
            vec![50, 75, 90, 100]
        );
        for report in &reports {
            // reported at the first stream that reaches the tier
            assert!(report.active * 100 / cap >= report.tier);
            assert!((report.active - 1) * 100 / cap < report.tier);
        }
        assert_eq!(
            reports.iter().map(|r| r.level()).collect::<Vec<_>>(),
            vec![Level::Info, Level::Info, Level::Warn, Level::Warn],
            "only the last two tiers are worth a warning"
        );

        // no exit path leaks the count
        drop(held);
        assert_eq!(load.active(), 0);
    }

    #[test]
    fn stream_load_rearms_a_tier_only_after_dropping_clear_of_it() {
        let load = Arc::new(StreamLoad::default());
        let cap = MAX_TUNNEL_STREAMS;
        let mut held = Vec::new();
        // just onto the first tier
        loop {
            let (slot, report) = StreamSlot::open(&load);
            held.push(slot);
            if report.is_some() {
                break;
            }
        }
        let at_tier = load.active();

        // hovering there must stay quiet
        for _ in 0..5 {
            held.pop();
            let (slot, report) = StreamSlot::open(&load);
            assert_eq!(report, None, "no repeat while hovering at a tier");
            held.push(slot);
        }

        // dropping clear of it (by the hysteresis margin) arms it again
        while load.active() * 100 / cap >= 50 - TIER_HYSTERESIS {
            held.pop();
        }
        let mut crossings = 0;
        while load.active() < at_tier {
            let (slot, report) = StreamSlot::open(&load);
            held.push(slot);
            crossings += u32::from(report.is_some());
        }
        assert_eq!(crossings, 1, "the tier must be armed again");
    }

    /// The relay's gate: `MAX_TUNNEL_STREAMS` slots are handed out
    /// without a wait, the caller beyond them waits in line, and slots
    /// that free up go to the line in order.
    #[tokio::test]
    async fn stream_slots_beyond_the_limit_wait_their_turn() {
        use futures_util::FutureExt as _;
        let load = Arc::new(StreamLoad::default());
        let mut held = Vec::new();
        for _ in 0..MAX_TUNNEL_STREAMS {
            let (slot, _report) = StreamSlot::acquire(&load, "node")
                .now_or_never()
                .expect("a free slot needs no wait");
            held.push(slot);
        }
        assert_eq!(load.active(), MAX_TUNNEL_STREAMS);

        let waiter = |load: &Arc<StreamLoad>| {
            let load = Arc::clone(load);
            tokio::spawn(async move { StreamSlot::acquire(&load, "node").await })
        };
        let in_line = |n: u32| {
            let load = Arc::clone(&load);
            async move {
                tokio::time::timeout(Duration::from_secs(5), async {
                    while load.queued() != n {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap_or_else(|_| panic!("{n} callers must be in line"));
            }
        };
        let first = waiter(&load);
        in_line(1).await;
        let second = waiter(&load);
        in_line(2).await;
        assert!(!first.is_finished() && !second.is_finished());

        // one slot frees: the head of the line gets it, nobody else
        held.pop();
        let (slot, _report) = tokio::time::timeout(Duration::from_secs(5), first)
            .await
            .expect("the freed slot must go to the first waiter")
            .unwrap();
        in_line(1).await;
        assert_eq!(load.active(), MAX_TUNNEL_STREAMS);
        assert!(!second.is_finished());

        drop(slot);
        let (_slot, _report) = tokio::time::timeout(Duration::from_secs(5), second)
            .await
            .expect("the next freed slot must go to the second waiter")
            .unwrap();
        in_line(0).await;
        assert_eq!(load.active(), MAX_TUNNEL_STREAMS);
    }

    /// A waiter that gives up (its future dropped, which is what the
    /// relay's timeout does) is gone at once: it holds no slot and no
    /// place in line, and the next slot to free goes to a live caller.
    #[tokio::test]
    async fn a_dropped_wait_leaves_no_trace_in_the_line() {
        use futures_util::FutureExt as _;
        let load = Arc::new(StreamLoad::default());
        let mut held = Vec::new();
        for _ in 0..MAX_TUNNEL_STREAMS {
            let (slot, _report) = StreamSlot::acquire(&load, "node")
                .now_or_never()
                .expect("a free slot needs no wait");
            held.push(slot);
        }

        for _ in 0..3 {
            // polled once and dropped, like a timed-out caller
            assert!(
                StreamSlot::acquire(&load, "node").now_or_never().is_none(),
                "a full gate must make the caller wait"
            );
        }
        assert_eq!(load.queued(), 0, "a dropped wait must leave the line");
        assert_eq!(load.active(), MAX_TUNNEL_STREAMS);

        held.pop();
        let (_slot, _report) = StreamSlot::acquire(&load, "node")
            .now_or_never()
            .expect("the freed slot must be there for the next live caller");
        assert_eq!(load.active(), MAX_TUNNEL_STREAMS);
    }

    #[test]
    fn node_id_matches_fixed_vector() {
        // computed independently (python hmac) for TUNNEL_APP_ID
        let machine_id: MachineId = "0123456789abcdef0123456789abcdef".parse().unwrap();
        assert_eq!(
            machine_id.node_id().unwrap().to_string(),
            "f2315cbd916742ecaab3178006407118"
        );
    }

    #[test]
    fn instance_node_id_matches_fixed_vector() {
        // computed independently (python hmac) for TUNNEL_APP_ID; the
        // derived instance app id for "update" is
        // 4608112769664e2db94e37a233b9572b, still usable with
        // `systemd-id128 machine-id -a`
        let machine_id: MachineId = "0123456789abcdef0123456789abcdef".parse().unwrap();
        assert_eq!(
            machine_id.instance_node_id("update").unwrap().to_string(),
            "0e4178539a8045b492c42674e271b422"
        );
        // distinct namespaces: default, per label, per machine
        assert_ne!(
            machine_id.instance_node_id("update").unwrap(),
            machine_id.node_id().unwrap()
        );
        assert_ne!(
            machine_id.instance_node_id("update").unwrap(),
            machine_id.instance_node_id("metrics").unwrap()
        );
    }

    #[test]
    fn machine_id_never_prints_its_value() {
        let machine_id: MachineId = "0123456789abcdef0123456789abcdef".parse().unwrap();
        assert_eq!(format!("{machine_id:?}"), "MachineId(<redacted>)");

        // nor may the parse error echo a near-valid input, such as an
        // untrimmed read of /etc/machine-id
        let err = "0123456789abcdef0123456789abcdef\n"
            .parse::<MachineId>()
            .unwrap_err();
        assert!(!format!("{err:#}").contains("0123456789"));
    }

    #[test_with::path(/etc/machine-id)]
    #[test_with::executable(systemd-id128)]
    #[test]
    fn node_id_matches_systemd_id128() {
        // deal with empty/uninitialized machine-id
        let Ok(machine_id) = std::fs::read_to_string("/etc/machine-id") else {
            return;
        };
        let Ok(machine_id) = machine_id.trim().parse::<MachineId>() else {
            return;
        };
        let app_id = format!("{TUNNEL_APP_ID:032x}");
        let out = std::process::Command::new("systemd-id128")
            .args(["machine-id", "-a", &app_id])
            .output()
            .unwrap();
        if !out.status.success() {
            return;
        }
        let theirs = String::from_utf8(out.stdout).unwrap();
        assert_eq!(machine_id.node_id().unwrap().to_string(), theirs.trim());
    }

    #[test]
    fn parse_id128_rejects_garbage() {
        let id = "0123456789abcdef0123456789abcdef";
        // Display is the inverse of parsing
        assert_eq!(id.parse::<NodeId>().unwrap().to_string(), id);
        for bad in [
            "",
            "0123",
            "g123456789abcdef0123456789abcdef",
            "+123456789abcdef0123456789abcdef",
        ] {
            assert!(bad.parse::<NodeId>().is_err(), "must reject {bad:?}");
        }
    }
}
