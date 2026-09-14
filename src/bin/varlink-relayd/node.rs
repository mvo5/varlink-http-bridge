// SPDX-License-Identifier: LGPL-2.1-or-later

//! The node face: accepts nodes dialing out to the relay, registers
//! them in the [`Nodes`] registry, and keeps each connection alive.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result, anyhow, bail};
use log::{debug, info, warn};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::handshake::server::{
    ErrorResponse, Request as WsRequest, Response as WsResponse,
};

use varlink_http_bridge::tunnel::{NodeId, TUNNEL_PATH, WsByteStream, h2_client_builder};

use crate::HANDSHAKE_TIMEOUT;
use crate::registry::{Collision, Nodes, ReservationGuard};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
#[cfg(not(test))]
pub(crate) const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(10);
// so the tests can watch a silent peer being given up on
#[cfg(test)]
pub(crate) const HEARTBEAT_TIMEOUT: Duration = Duration::from_millis(1000);

/// Accept connecting nodes and register them.
pub(crate) async fn serve(
    listener: TcpListener,
    tls: Option<openssl::ssl::SslAcceptor>,
    nodes: Arc<Nodes>,
) -> Result<()> {
    loop {
        let (stream, peer) = varlink_http_bridge::accept_and_configure(&listener).await;
        let nodes = Arc::clone(&nodes);
        let tls = tls.clone();
        tokio::spawn(async move {
            let result = match tls {
                Some(tls) => {
                    match tokio::time::timeout(
                        HANDSHAKE_TIMEOUT,
                        varlink_http_bridge::tls_accept(&tls, None, stream),
                    )
                    .await
                    .map_err(|_| anyhow!("TLS handshake timed out"))
                    .and_then(|r| r)
                    {
                        Ok(stream) => handle(stream, &nodes, peer).await,
                        Err(e) => Err(e),
                    }
                }
                None => handle(stream, &nodes, peer).await,
            };
            if let Err(e) = result {
                // everything here is a peer that failed to become a
                // tunnel (a scanner, a TLS mismatch, a half-open
                // connection), which a public port sees all day. That
                // includes a claim on a taken id: it is usually the same
                // node back after an abrupt death, and the holder's
                // heartbeat says so at warn if the probe finds it alive
                // instead
                debug!("node connection from {peer}: {e:#}");
            }
        });
    }
}

/// One node connection: WebSocket upgrade carrying the claimed id
/// (reserving it), then HTTP/2 with the roles reversed (this side is
/// the h2 client), registered once the node answers a first PING and
/// kept alive by PINGs until the connection dies or stops answering.
/// The [`ReservationGuard`] releases the id when this returns,
/// whichever way.
async fn handle<S>(stream: S, nodes: &Nodes, peer: std::net::SocketAddr) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut reserved = None;
    let mut duplicate = None;
    let upgrade = tokio_tungstenite::accept_hdr_async(
        stream,
        // the error type is fixed by tungstenite's callback contract
        #[allow(clippy::result_large_err)]
        |req: &WsRequest, resp: WsResponse| match reserve_from_upgrade(req, nodes, &mut duplicate) {
            Ok(guard) => {
                reserved = Some(guard);
                Ok(resp)
            }
            Err(reject) => Err(reject),
        },
    );
    let ws = match tokio::time::timeout(HANDSHAKE_TIMEOUT, upgrade).await {
        Ok(Ok(ws)) => ws,
        Ok(Err(e)) => {
            // the rejection this side chose is more use than
            // tungstenite's "HTTP error response" wrapper around it
            return Err(match duplicate {
                Some(id) => anyhow!("claimed node id {id}, which another connection holds"),
                None => anyhow::Error::new(e).context("node WebSocket upgrade"),
            });
        }
        Err(_) => return Err(anyhow!("node WebSocket upgrade timed out")),
    };
    let guard = reserved.take().expect("upgrade callback reserved");
    let reservation = &guard.reservation;
    let id = reservation.id;

    // not bounded: h2's client handshake only writes the preface and
    // queues SETTINGS, it never waits for the peer
    let (h2, mut conn) = h2_client_builder()
        .handshake::<_, bytes::Bytes>(WsByteStream::new(ws))
        .await
        .context("h2 handshake with node")?;
    let mut ping_pong = conn.ping_pong().expect("first ping_pong handle");

    // so the first pong is the first proof anyone is listening: a peer
    // that upgrades and then goes silent stays a socket, not a node
    // callers get routed at
    tokio::select! {
        result = &mut conn => {
            return Err(match result {
                Ok(()) => anyhow!("node {id} closed before answering the first PING"),
                Err(e) => anyhow::Error::new(e)
                    .context(format!("node {id} failed before answering the first PING")),
            });
        }
        answered = ping(&mut ping_pong) => {
            if !answered {
                bail!("node {id} did not answer the first PING within {HEARTBEAT_TIMEOUT:?}");
            }
        }
    }
    nodes.attach(reservation, h2);
    let load = std::sync::Arc::clone(&reservation.load);
    let connected = std::time::Instant::now();
    info!("node {id} connected from {peer}");

    // resolves with whether the PING that failed was a probe
    let heartbeat = async {
        loop {
            let probed = tokio::select! {
                () = tokio::time::sleep(HEARTBEAT_INTERVAL) => false,
                // a colliding claim asked whether this holder is alive
                () = reservation.probe.notified() => true,
            };
            if !ping(&mut ping_pong).await {
                return probed;
            }
            // alive, so the claim was not this node back from an abrupt
            // death: two machines share an id, or two bridges on one
            if probed {
                if nodes.claim_is_news(reservation) {
                    warn!(
                        "node {id} is live, but another connection claimed its id; if this is \
                         a second bridge on the same machine, give it --instance"
                    );
                } else {
                    debug!("node {id} is live, another connection claimed its id again");
                }
            }
        }
    };
    // the lifetime tells a redial storm from a middlebox reaping an idle
    // tunnel, the stream count says how many callers went with it
    let lived = || {
        format!(
            "after {:?} with {} streams open",
            connected.elapsed(),
            load.active()
        )
    };
    // select tears the connection down when the heartbeat gives up
    tokio::select! {
        result = &mut conn => match result {
            Ok(()) => info!("node {id} disconnected {}", lived()),
            Err(e) => info!("node {id} connection failed {}: {e}", lived()),
        },
        // the connection that claimed the id is right that this one is
        // dead: a node back after an abrupt reboot, and the redial it
        // is about to make is the recovery
        probed = heartbeat => if probed {
            info!(
                "node {id} did not answer a probe {}, dropping it for the connection claiming its id",
                lived()
            );
        } else {
            // it is reachable but not answering: a wedged node, a black
            // hole in the path, something an operator wants to see
            warn!("node {id} stopped answering PINGs {}, dropping it", lived());
        },
    }
    Ok(())
}

/// One PING, bounded by [`HEARTBEAT_TIMEOUT`]: false when the node did
/// not answer in time or the connection is gone.
async fn ping(ping_pong: &mut h2::PingPong) -> bool {
    matches!(
        tokio::time::timeout(HEARTBEAT_TIMEOUT, ping_pong.ping(h2::Ping::opaque())).await,
        Ok(Ok(_pong))
    )
}

/// Validate a node's upgrade request and reserve its id: the tunnel
/// path and a well-formed `?node_id=` (the id rides in the query string so
/// an L7 load balancer can hash on it, see README.relayd.md).
/// Reserving here makes the 409 the only collision outcome a node ever
/// sees; nothing fails after a completed upgrade except real I/O.
// the error type is fixed by tungstenite's upgrade-callback contract
#[allow(clippy::result_large_err)]
fn reserve_from_upgrade<'a>(
    req: &WsRequest,
    nodes: &'a Nodes,
    // set on the 409 path: the rejection travels to the node as a
    // status, and this tells our own log which one it was
    duplicate: &mut Option<NodeId>,
) -> Result<ReservationGuard<'a>, ErrorResponse> {
    let reject = |status: u16, msg: &str| {
        let mut resp = ErrorResponse::new(Some(msg.to_string()));
        *resp.status_mut() = http::StatusCode::from_u16(status).expect("static status");
        resp
    };
    if req.uri().path() != TUNNEL_PATH {
        return Err(reject(404, "unknown path"));
    }
    let id = req
        .uri()
        .query()
        .and_then(|q| q.split('&').find_map(|kv| kv.strip_prefix("node_id=")))
        .ok_or_else(|| reject(400, "missing node_id= in query string"))?;
    let id = id
        .parse::<NodeId>()
        .map_err(|_| reject(400, "malformed id"))?;
    nodes.reserve(id).map_err(|_: Collision| {
        *duplicate = Some(id);
        reject(409, "id already connected")
    })
}
