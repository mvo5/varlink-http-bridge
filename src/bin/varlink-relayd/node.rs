// SPDX-License-Identifier: LGPL-2.1-or-later

//! The node face: accepts nodes dialing out to the relay, registers
//! them in the [`Nodes`] registry, and keeps each connection alive.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result, anyhow};
use log::{debug, info, warn};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::handshake::server::{
    ErrorResponse, Request as WsRequest, Response as WsResponse,
};

use varlink_http_bridge::tunnel::{NodeId, TUNNEL_PATH, WsByteStream, h2_client_builder};

use crate::HANDSHAKE_TIMEOUT;
use crate::registry::{Nodes, ReservationGuard};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(10);

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
                        varlink_http_bridge::tls_accept(&tls, stream),
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
                // A node that claims a taken id is a real
                // misconfiguration and stays visible; everything else
                // here is a peer that failed to become a tunnel (a
                // scanner, a TLS mismatch, a half-open connection),
                // which a public port sees all day.
                match e.downcast_ref::<DuplicateClaim>() {
                    // the same node retries forever, so it says this
                    // once and then only as an occasional reminder
                    Some(claim) if claim.loud => warn!("node connection from {peer}: {e:#}"),
                    _ => debug!("node connection from {peer}: {e:#}"),
                }
            }
        });
    }
}

/// One node connection: WebSocket upgrade carrying the claimed id
/// (reserving it), then HTTP/2 with the roles reversed (this side is
/// the h2 client), kept alive by PINGs until the connection dies or
/// stops answering. The [`ReservationGuard`] releases the id when this
/// returns, whichever way.
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
                Some((id, loud)) => anyhow::Error::new(DuplicateClaim { id, loud }),
                None => anyhow::Error::new(e).context("node WebSocket upgrade"),
            });
        }
        Err(_) => return Err(anyhow!("node WebSocket upgrade timed out")),
    };
    let guard = reserved.take().expect("upgrade callback reserved");
    let reservation = &guard.reservation;
    let id = reservation.id;

    let (h2, mut conn) = tokio::time::timeout(
        HANDSHAKE_TIMEOUT,
        h2_client_builder().handshake::<_, bytes::Bytes>(WsByteStream::new(ws)),
    )
    .await
    .map_err(|_| anyhow!("h2 handshake with node timed out"))?
    .context("h2 handshake with node")?;
    let mut ping_pong = conn.ping_pong().expect("first ping_pong handle");
    nodes.attach(reservation, h2);
    let connected = std::time::Instant::now();
    info!("node {id} connected from {peer}");

    let heartbeat = async {
        loop {
            tokio::select! {
                () = tokio::time::sleep(HEARTBEAT_INTERVAL) => {}
                // a colliding claim asked whether this holder is alive
                () = reservation.probe.notified() => {}
            }
            match tokio::time::timeout(HEARTBEAT_TIMEOUT, ping_pong.ping(h2::Ping::opaque())).await
            {
                Ok(Ok(_pong)) => {}
                Ok(Err(_)) | Err(_) => return,
            }
        }
    };
    // The lifetime is what tells a redial storm (short lives) apart
    // from a middlebox reaping an idle tunnel.
    let lived = || format!("after {:?}", connected.elapsed());
    // select tears the connection down when the heartbeat gives up
    tokio::select! {
        result = conn => match result {
            Ok(()) => info!("node {id} disconnected {}", lived()),
            Err(e) => info!("node {id} connection failed {}: {e}", lived()),
        },
        // it is reachable but not answering: a wedged node, a black
        // hole in the path, something an operator wants to see
        () = heartbeat => warn!(
            "node {id} stopped answering PINGs {}, dropping it", lived()
        ),
    }
    Ok(())
}

/// Validate a node's upgrade request and reserve its id: the tunnel
/// path and a well-formed `?id=` (the id rides in the query string so
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
    duplicate: &mut Option<(NodeId, bool)>,
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
        .and_then(|q| q.split('&').find_map(|kv| kv.strip_prefix("id=")))
        .ok_or_else(|| reject(400, "missing id= in query string"))?;
    let id = id
        .parse::<NodeId>()
        .map_err(|_| reject(400, "malformed id"))?;
    nodes.reserve(id).map_err(|collision| {
        *duplicate = Some((id, collision.loud));
        reject(409, "id already connected")
    })
}

/// A node claimed an id another live connection already holds. It has
/// its own type so the accept loop can be loud about it: unlike the
/// rest of what a public port sees, this one needs an operator, and it
/// usually means two bridge instances on one host without `--instance`.
#[derive(Debug)]
struct DuplicateClaim {
    id: NodeId,
    /// whether this collision is worth a warning, see
    /// [`crate::registry::Collision`]
    loud: bool,
}

impl std::fmt::Display for DuplicateClaim {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "claimed node id {}, which another live connection holds; if this is a second \
             bridge on the same machine, give it --instance",
            self.id
        )
    }
}

impl std::error::Error for DuplicateClaim {}
