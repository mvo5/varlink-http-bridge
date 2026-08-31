// SPDX-License-Identifier: LGPL-2.1-or-later

//! The caller face: accepts `CONNECT <id>` requests, looks the node up
//! in the [`Nodes`] registry, and splices the caller onto one h2
//! stream to it.

use std::sync::Arc;

use anyhow::{Context as _, Result, anyhow, bail};
use bytes::Bytes;
use log::debug;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use varlink_http_bridge::tunnel::{NodeId, send_all, splice};

use crate::HANDSHAKE_TIMEOUT;
use crate::registry::Nodes;

// a CONNECT request is one line plus few headers
const MAX_CONNECT_REQUEST: usize = 8 * 1024;
const MAX_CONNECT_HEADERS: usize = 16;

/// Accept callers and splice them onto their node.
pub(crate) async fn serve(listener: TcpListener, nodes: Arc<Nodes>) -> Result<()> {
    loop {
        let (stream, peer) = varlink_http_bridge::accept_and_configure(&listener).await;
        let nodes = Arc::clone(&nodes);
        tokio::spawn(async move {
            // Level by who has to act on it: a caller that sends
            // nonsense is routine on a public port and must not bury
            // the journal, a node nobody has heard of is worth a look.
            if let Err(denied) = handle(stream, &nodes).await {
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

/// Why a caller was turned away, which is also how loud that is: the
/// caller's own fault, or this relay not knowing the node.
enum Denied {
    Caller(anyhow::Error),
    Node(anyhow::Error),
}

impl Denied {
    /// How loud this is. A caller that sends nonsense or asks for a
    /// node nobody has heard of is routine on a public port.
    fn level(&self) -> log::Level {
        match self {
            Self::Caller(_) => log::Level::Debug,
            Self::Node(_) => log::Level::Info,
        }
    }

    fn error(&self) -> &anyhow::Error {
        match self {
            Self::Caller(e) | Self::Node(e) => e,
        }
    }
}

/// One caller: read `CONNECT <id>[:port]`, open an h2 stream to the
/// node, reply `200 Connection established`, then splice opaque bytes.
async fn handle(mut stream: TcpStream, nodes: &Nodes) -> Result<(), Denied> {
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
    let Some(h2) = nodes.get(id) else {
        deny(&mut stream, "502 Bad Gateway").await;
        return Err(Denied::Node(anyhow!("no connected node {id}")));
    };

    let stream_to_node = async {
        let mut h2 = h2.ready().await.context("h2 stream slot")?;
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
    let (recv, mut send) = match stream_to_node.await {
        Ok(pair) => pair,
        Err(e) => {
            deny(&mut stream, "502 Bad Gateway").await;
            return Err(Denied::Node(e));
        }
    };

    // the h2 stream id is the one name both ends of the tunnel see, so
    // the node's lines for this caller can be found from the relay's
    let who = format!("node {id} stream {}", u32::from(send.stream_id()));

    stream
        .write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
        .await
        .context("confirming CONNECT")
        .map_err(Denied::Caller)?;
    // bytes a caller pipelined behind its CONNECT must not be lost
    if !early_data.is_empty() {
        send_all(&mut send, early_data, &who)
            .await
            .map_err(Denied::Node)?;
    }
    debug!("{who}: spliced");

    let started = std::time::Instant::now();
    let result = splice(stream, recv, send, &who).await;
    // the one line that answers "did anything actually flow?"
    match &result {
        Ok(moved) => debug!(
            "{who}: done after {:?}, {} bytes to the node, {} back",
            started.elapsed(),
            moved.sent,
            moved.received
        ),
        Err(e) => debug!("{who}: ended after {:?}: {e:#}", started.elapsed()),
    }
    result.map(|_| ()).map_err(Denied::Node)
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
    }
}
