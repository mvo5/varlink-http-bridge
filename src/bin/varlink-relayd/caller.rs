// SPDX-License-Identifier: LGPL-2.1-or-later

//! The caller face: accepts `CONNECT <id>` requests, looks the node up
//! in the [`Nodes`] registry, and splices the caller onto one h2
//! stream to it.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result, anyhow, bail};
use bytes::Bytes;
use log::debug;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use varlink_http_bridge::tunnel::{
    MAX_TUNNEL_STREAMS, NodeId, SpliceError, StreamSlot, send_all, splice,
};

use crate::HANDSHAKE_TIMEOUT;
use crate::registry::Nodes;

// a CONNECT request is one line plus few headers
const MAX_CONNECT_REQUEST: usize = 8 * 1024;
const MAX_CONNECT_HEADERS: usize = 16;

/// Accept callers and splice them onto their node. `slot_timeout`
/// bounds the wait for a stream slot on a full node, and then the
/// node's accept of the stream.
pub(crate) async fn serve(
    listener: TcpListener,
    nodes: Arc<Nodes>,
    slot_timeout: Duration,
) -> Result<()> {
    loop {
        let (stream, peer) = varlink_http_bridge::accept_and_configure(&listener).await;
        let nodes = Arc::clone(&nodes);
        tokio::spawn(async move {
            if let Err(denied) = handle(stream, &nodes, slot_timeout).await {
                log::log!(denied.level(), "caller {peer}: {:#}", denied.error());
            }
        });
    }
}

/// Best-effort error reply to a caller: the connection is being torn
/// down anyway, so a failed write is not worth reporting.
async fn deny(stream: &mut TcpStream, status: &str) {
    let _ = stream
        .write_all(format!("HTTP/1.1 {status}\r\n\r\n").as_bytes())
        .await;
}

/// Why a caller was turned away, which decides how loud that is: what a
/// public port sees all day (nonsense, unknown nodes) must not bury the
/// journal, and a node out of stream slots is one event its load tier
/// reports once, not once more per caller that runs into it.
enum Denied {
    Caller(anyhow::Error),
    Node(anyhow::Error),
    Overload(anyhow::Error),
}

impl Denied {
    fn level(&self) -> log::Level {
        match self {
            Self::Caller(_) | Self::Overload(_) => log::Level::Debug,
            Self::Node(_) => log::Level::Info,
        }
    }

    fn error(&self) -> &anyhow::Error {
        match self {
            Self::Caller(e) | Self::Node(e) | Self::Overload(e) => e,
        }
    }
}

// which side of the splice failed says whose problem it is: the local
// side is the caller's own tcp (hung up, reset, a scanner that left),
// the h2 side is the node's
impl From<SpliceError> for Denied {
    fn from(e: SpliceError) -> Self {
        match e {
            SpliceError::Local(e) => Self::Caller(e),
            SpliceError::Peer(e) => Self::Node(e),
        }
    }
}

/// One caller: read `CONNECT <id>[:port]`, take one of the node's
/// stream slots, open an h2 stream to the node, reply `200 Connection
/// established`, then splice opaque bytes.
async fn handle(
    mut stream: TcpStream,
    nodes: &Nodes,
    slot_timeout: Duration,
) -> Result<(), Denied> {
    let (id, early_data) =
        match tokio::time::timeout(HANDSHAKE_TIMEOUT, read_connect_request(&mut stream)).await {
            Ok(Ok(target)) => target,
            Ok(Err(e)) => {
                deny(&mut stream, "400 Bad Request").await;
                return Err(Denied::Caller(e));
            }
            Err(_) => {
                deny(&mut stream, "408 Request Timeout").await;
                return Err(Denied::Caller(anyhow!(
                    "sent no complete CONNECT request in time"
                )));
            }
        };
    let Some((h2, load)) = nodes.get(id) else {
        deny(&mut stream, "502 Bad Gateway").await;
        return Err(Denied::Node(anyhow!("no connected node {id}")));
    };

    // the slot comes first, before any h2 stream exists: a caller that
    // gives up waiting then leaves nothing behind for the node to drain
    let who = format!("node {id}");
    let Ok((_busy, report)) =
        tokio::time::timeout(slot_timeout, StreamSlot::acquire(&load, &who)).await
    else {
        deny(&mut stream, "503 Service Unavailable").await;
        return Err(Denied::Overload(anyhow!(
            "gave up after {slot_timeout:?}: node {id} kept all {MAX_TUNNEL_STREAMS} \
             streams busy, {} more callers are waiting",
            load.queued()
        )));
    };
    if let Some(report) = report {
        log::log!(
            report.level(),
            "node {id} is using {}/{MAX_TUNNEL_STREAMS} tunnel streams ({}%)",
            report.active,
            report.tier
        );
    }

    let stream_to_node = async {
        let mut h2 = h2.ready().await.context("h2 send handle")?;
        let request = http::Request::builder()
            .uri(format!("https://{id}/"))
            .method(http::Method::POST)
            .body(())
            .context("building request")?;
        let (response, send) = h2.send_request(request, false).context("opening stream")?;
        let response = response.await.context("waiting for the node's accept")?;
        if response.status() != http::StatusCode::OK {
            bail!("node rejected the stream: {}", response.status());
        }
        anyhow::Ok((response.into_body(), send))
    };
    // with a slot in hand the stream goes out at once, so this wait is
    // the node's to answer: past it the node is not accepting, which is
    // not the relay's capacity and so not a 503
    let (recv, mut send) = match tokio::time::timeout(slot_timeout, stream_to_node).await {
        Ok(Ok(pair)) => pair,
        Ok(Err(e)) => {
            deny(&mut stream, "502 Bad Gateway").await;
            return Err(Denied::Node(e));
        }
        Err(_) => {
            deny(&mut stream, "502 Bad Gateway").await;
            return Err(Denied::Node(anyhow!(
                "node {id} did not accept the stream within {slot_timeout:?}"
            )));
        }
    };

    // the h2 stream id is the one name both ends of the tunnel see, so
    // the node's lines for this caller can be found from the relay's
    let who = format!("{who} stream {}", u32::from(send.stream_id()));

    stream
        .write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
        .await
        .context("confirming CONNECT")
        .map_err(Denied::Caller)?;
    // bytes a caller pipelined behind its CONNECT must not be lost
    if !early_data.is_empty() {
        send_all(&mut send, early_data, &who).await?;
    }
    debug!(
        "{who}: spliced, tunnel now at {}/{MAX_TUNNEL_STREAMS} streams",
        load.active()
    );

    let started = std::time::Instant::now();
    // a failure is logged once, by serve, at the level its side earns
    let moved = splice(stream, recv, send, &who)
        .await
        .map_err(|e| e.context(format!("{who}: ended after {:?}", started.elapsed())))?;
    // the one line that answers "did anything actually flow?"
    debug!(
        "{who}: done after {:?}, {} bytes to the node, {} back",
        started.elapsed(),
        moved.sent,
        moved.received
    );
    Ok(())
}

/// Parse `CONNECT <id>[:port] HTTP/1.1` plus headers (all ignored); the
/// port is routing-irrelevant, the relay serves exactly one bridge per
/// node. Returns the target id and any bytes the caller sent behind the
/// request head.
async fn read_connect_request(stream: &mut TcpStream) -> Result<(NodeId, Bytes)> {
    let mut buf = Vec::with_capacity(256);
    loop {
        if buf.len() >= MAX_CONNECT_REQUEST {
            bail!("CONNECT request too large");
        }
        let n = stream.read_buf(&mut buf).await.context("reading CONNECT")?;
        if n == 0 {
            bail!("caller closed before completing the CONNECT request");
        }
        let mut headers = [httparse::EMPTY_HEADER; MAX_CONNECT_HEADERS];
        let mut req = httparse::Request::new(&mut headers);
        match req.parse(&buf).context("parsing CONNECT request")? {
            httparse::Status::Partial => {}
            httparse::Status::Complete(head_end) => {
                if req.method != Some("CONNECT") {
                    bail!("expected a CONNECT request, got {:?}", req.method);
                }
                let authority = req.path.context("CONNECT without authority")?;
                let id = authority
                    .split(':')
                    .next()
                    .unwrap_or_default()
                    .parse::<NodeId>()
                    .with_context(|| format!("CONNECT authority {authority:?}"))?;
                return Ok((id, Bytes::copy_from_slice(&buf[head_end..])));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn how_loud_a_denied_caller_is() {
        use log::Level;
        // a public port sees these all day: scanners, half-open
        // connections, callers naming a node that is not connected
        assert_eq!(
            Denied::Caller(anyhow!("expected a CONNECT request")).level(),
            Level::Debug
        );
        assert_eq!(
            Denied::Node(anyhow!("no connected node")).level(),
            Level::Info
        );
        // a full node is one event, reported once by its load tier; a
        // caller retrying against it every second must not add a line
        // above debug per attempt
        assert_eq!(
            Denied::Overload(anyhow!("kept all streams busy")).level(),
            Level::Debug
        );
        // a spliced stream ending badly: the caller's own side going
        // away is routine, the node's is news
        assert_eq!(
            Denied::from(SpliceError::Local(anyhow!("reading local stream"))).level(),
            Level::Debug
        );
        assert_eq!(
            Denied::from(SpliceError::Peer(anyhow!("receiving h2 data"))).level(),
            Level::Info
        );
    }
}
