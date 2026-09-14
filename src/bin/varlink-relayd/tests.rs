// SPDX-License-Identifier: LGPL-2.1-or-later

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use varlink_http_bridge::tunnel::{
    MAX_TUNNEL_STREAMS, NodeId, STREAM_WINDOW, TUNNEL_PATH, WsByteStream, h2_server_builder, splice,
};

use crate::registry::Nodes;
use crate::{caller, node};

const TEST_ID: &str = "0123456789abcdef0123456789abcdef";
const OTHER_ID: &str = "fedcba9876543210fedcba9876543210";
const STEP: Duration = Duration::from_secs(5);
// short, so the 503 test does not sit out the production 10s
const SLOT_TIMEOUT: Duration = Duration::from_millis(500);

async fn start_relay() -> (SocketAddr, SocketAddr, Arc<Nodes>) {
    let node_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let connect_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let node_addr = node_listener.local_addr().unwrap();
    let connect_addr = connect_listener.local_addr().unwrap();
    let nodes = Arc::new(Nodes::default());
    tokio::spawn(node::serve(node_listener, None, Arc::clone(&nodes)));
    tokio::spawn(caller::serve(
        connect_listener,
        Arc::clone(&nodes),
        SLOT_TIMEOUT,
    ));
    (node_addr, connect_addr, nodes)
}

/// A minimal node built from the chunk 1 primitives, the same shape
/// varlink-httpd's `--relay` has: dial the relay, serve h2 role-reversed
/// with the tunnel's h2 settings, answer every stream with 200 and echo
/// its bytes.
async fn stub_node(
    node_addr: SocketAddr,
    id: &str,
) -> Result<tokio::task::JoinHandle<()>, tokio_tungstenite::tungstenite::Error> {
    stub_node_hanging_streams(node_addr, id, 0).await
}

/// The same node, except the local peers of its first `hang_first`
/// streams never read a byte: the hung-local-service case, where
/// `splice` stops releasing h2 window once the stream buffer is full.
async fn stub_node_hanging_streams(
    node_addr: SocketAddr,
    id: &str,
    hang_first: usize,
) -> Result<tokio::task::JoinHandle<()>, tokio_tungstenite::tungstenite::Error> {
    let (node, _seen) = stub_node_counting_streams(node_addr, id, hang_first).await?;
    Ok(node)
}

/// The same node again, also counting every stream the relay opened
/// towards it, answered or not: the relay's admission is only right if
/// the node hears of exactly the callers that were let through.
async fn stub_node_counting_streams(
    node_addr: SocketAddr,
    id: &str,
    hang_first: usize,
) -> Result<(tokio::task::JoinHandle<()>, Arc<AtomicUsize>), tokio_tungstenite::tungstenite::Error>
{
    let tcp = TcpStream::connect(node_addr).await.unwrap();
    let url = format!("ws://{node_addr}{TUNNEL_PATH}?node_id={id}");
    let (ws, _response) = tokio_tungstenite::client_async(url, tcp).await?;
    let seen = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&seen);
    let node = tokio::spawn(async move {
        let mut conn = h2_server_builder()
            .handshake::<_, bytes::Bytes>(WsByteStream::new(ws))
            .await
            .unwrap();
        let mut streams = 0;
        // keeps the hung streams' local peers open without reading them
        let mut hung = Vec::new();
        while let Some(Ok((req, mut respond))) = conn.accept().await {
            counted.fetch_add(1, Ordering::Relaxed);
            let body = req.into_body();
            // like the bridge: a caller gone before its stream is
            // answered must not end the tunnel
            let Ok(send) = respond.send_response(http::Response::new(()), false) else {
                continue;
            };
            streams += 1;
            // the buffer the bridge gives a tunnel stream
            let (io, peer) = tokio::io::duplex(STREAM_WINDOW as usize);
            if streams <= hang_first {
                hung.push(peer);
            } else {
                tokio::spawn(async move {
                    let (mut rd, mut wr) = tokio::io::split(peer);
                    let _ = tokio::io::copy(&mut rd, &mut wr).await;
                    let _ = wr.shutdown().await;
                });
            }
            tokio::spawn(async move {
                let _ = splice(io, body, send, "stub node").await;
            });
        }
    });
    Ok((node, seen))
}

/// A node that keeps the tunnel up but never answers a stream: the
/// path to it is black-holed, or its accept loop is wedged. The unanswered
/// streams are kept so h2 does not reset them.
async fn stub_node_never_answering(
    node_addr: SocketAddr,
    id: &str,
) -> Result<tokio::task::JoinHandle<()>, tokio_tungstenite::tungstenite::Error> {
    let tcp = TcpStream::connect(node_addr).await.unwrap();
    let url = format!("ws://{node_addr}{TUNNEL_PATH}?node_id={id}");
    let (ws, _response) = tokio_tungstenite::client_async(url, tcp).await?;
    Ok(tokio::spawn(async move {
        let mut conn = h2_server_builder()
            .handshake::<_, bytes::Bytes>(WsByteStream::new(ws))
            .await
            .unwrap();
        let mut unanswered = Vec::new();
        while let Some(Ok(stream)) = conn.accept().await {
            unanswered.push(stream);
        }
    }))
}

/// A node that registers and then dies without closing its socket: a
/// machine that lost power, as the relay sees it until a PING goes
/// unanswered. Driving the connection is what answers PINGs, so the
/// stub stops doing that once the test awaits the returned future, and
/// the socket stays open but mute.
async fn stub_node_that_goes_silent(
    node_addr: SocketAddr,
    id: &str,
) -> Result<
    (tokio::task::JoinHandle<()>, impl Future<Output = ()>),
    tokio_tungstenite::tungstenite::Error,
> {
    let tcp = TcpStream::connect(node_addr).await.unwrap();
    let url = format!("ws://{node_addr}{TUNNEL_PATH}?node_id={id}");
    let (ws, _response) = tokio_tungstenite::client_async(url, tcp).await?;
    let (silence, silenced) = tokio::sync::oneshot::channel::<()>();
    let (muted, mute) = tokio::sync::oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        let mut conn = h2_server_builder()
            .handshake::<_, bytes::Bytes>(WsByteStream::new(ws))
            .await
            .unwrap();
        tokio::select! {
            _ = conn.accept() => {}
            _ = silenced => {}
        }
        let _ = muted.send(());
        let _open = conn;
        std::future::pending::<()>().await;
    });
    Ok((task, async move {
        let _ = silence.send(());
        mute.await.expect("the stub must confirm it went mute");
    }))
}

/// The relay attaches a node's h2 handle only once the node answered
/// its first PING, which the stub's task does by driving its
/// connection; tests must not race it.
async fn wait_registered(nodes: &Nodes, id: &str) {
    let id = id.parse::<NodeId>().unwrap();
    tokio::time::timeout(STEP, async {
        while nodes.get(id).is_none() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("node must register");
}

/// Wait until the relay has given `id` up: the reservation is gone,
/// not only the h2 handle. `within` is the reason the relay must have
/// noticed by then.
async fn wait_released(nodes: &Nodes, id: &str, within: Duration, why: &str) {
    let id = id.parse::<NodeId>().unwrap();
    tokio::time::timeout(within, async {
        while nodes.occupied(id) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect(why);
}

/// Send `CONNECT <authority>` (plus `extra` pipelined behind it) and
/// return the socket and the relay's HTTP status line.
async fn send_connect(
    connect_addr: SocketAddr,
    authority: &str,
    extra: &[u8],
) -> (TcpStream, String) {
    let mut stream = open_connect(connect_addr, authority, extra).await;
    let status = read_status(&mut stream, STEP)
        .await
        .expect("relay must answer the CONNECT");
    (stream, status)
}

async fn open_connect(connect_addr: SocketAddr, authority: &str, extra: &[u8]) -> TcpStream {
    let mut stream = TcpStream::connect(connect_addr).await.unwrap();
    let mut request =
        format!("CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\n\r\n").into_bytes();
    request.extend_from_slice(extra);
    stream.write_all(&request).await.unwrap();
    stream
}

async fn read_status(stream: &mut TcpStream, within: Duration) -> Option<String> {
    tokio::time::timeout(within, async {
        let mut head = Vec::new();
        let mut byte = [0u8; 1];
        while !head.ends_with(b"\r\n\r\n") {
            stream.read_exact(&mut byte).await.unwrap();
            head.push(byte[0]);
        }
        String::from_utf8(head)
            .unwrap()
            .lines()
            .next()
            .unwrap()
            .to_string()
    })
    .await
    .ok()
}

#[tokio::test(flavor = "multi_thread")]
async fn end_to_end_echo() {
    let (node_addr, connect_addr, nodes) = start_relay().await;
    let _node = stub_node(node_addr, TEST_ID).await.unwrap();
    wait_registered(&nodes, TEST_ID).await;

    let (mut stream, status) = send_connect(connect_addr, &format!("{TEST_ID}:80"), b"").await;
    assert_eq!(status, "HTTP/1.1 200 Connection established");

    stream.write_all(b"hello relay").await.unwrap();
    let mut got = [0u8; 11];
    tokio::time::timeout(STEP, stream.read_exact(&mut got))
        .await
        .expect("echo must come back")
        .unwrap();
    assert_eq!(&got, b"hello relay");
}

#[tokio::test(flavor = "multi_thread")]
async fn early_data_behind_the_connect_is_not_lost() {
    let (node_addr, connect_addr, nodes) = start_relay().await;
    let _node = stub_node(node_addr, TEST_ID).await.unwrap();
    wait_registered(&nodes, TEST_ID).await;

    let (mut stream, status) = send_connect(connect_addr, TEST_ID, b"early").await;
    assert_eq!(status, "HTTP/1.1 200 Connection established");
    let mut got = [0u8; 5];
    tokio::time::timeout(STEP, stream.read_exact(&mut got))
        .await
        .expect("pipelined bytes must be echoed")
        .unwrap();
    assert_eq!(&got, b"early");
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_node_gets_502() {
    let (_node_addr, connect_addr, _nodes) = start_relay().await;
    let (_stream, status) = send_connect(connect_addr, OTHER_ID, b"").await;
    assert_eq!(status, "HTTP/1.1 502 Bad Gateway");
}

#[tokio::test(flavor = "multi_thread")]
async fn malformed_connect_gets_400() {
    let (_node_addr, connect_addr, _nodes) = start_relay().await;
    let (_stream, status) = send_connect(connect_addr, "not-a-node-id", b"").await;
    assert_eq!(status, "HTTP/1.1 400 Bad Request");
}

/// A colliding claim is not a collision until the holder proves it is
/// alive, so what the registry does at claim time is wake the holder's
/// probe; the holder then reports the first collision and, since a
/// misconfigured node retries forever and one relay serves a fleet of
/// them, stays quiet about the retries.
#[test]
fn repeated_duplicate_claims_are_only_reported_once() {
    use futures_util::FutureExt as _;

    let nodes = Nodes::default();
    let id = TEST_ID.parse::<NodeId>().unwrap();
    let holder = nodes.reserve(id).expect("first claim wins");
    assert!(
        holder.reservation.probe.notified().now_or_never().is_none(),
        "nothing to probe before anyone claims the id"
    );

    assert!(
        nodes.reserve(id).is_err(),
        "the second claim must be refused"
    );
    assert!(
        holder.reservation.probe.notified().now_or_never().is_some(),
        "the claim must wake the holder's probe"
    );
    // the probe was answered: this is a live duplicate
    assert!(
        nodes.claim_is_news(&holder.reservation),
        "the first collision on a live holder is worth a warning"
    );
    for attempt in 0..5 {
        assert!(nodes.reserve(id).is_err(), "the claim must stay refused");
        assert!(
            !nodes.claim_is_news(&holder.reservation),
            "retry {attempt} must stay quiet"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn duplicate_claim_is_rejected_first_wins() {
    let (node_addr, _connect_addr, nodes) = start_relay().await;
    let _node = stub_node(node_addr, TEST_ID).await.unwrap();
    wait_registered(&nodes, TEST_ID).await;

    let err = stub_node(node_addr, TEST_ID)
        .await
        .expect_err("second claim must be rejected");
    let tokio_tungstenite::tungstenite::Error::Http(resp) = err else {
        panic!("expected an HTTP rejection, got {err:?}");
    };
    assert_eq!(resp.status(), 409);

    // the claim probed the holder; a live one answers and stays
    tokio::time::sleep(node::HEARTBEAT_TIMEOUT).await;
    assert!(
        nodes.get(TEST_ID.parse().unwrap()).is_some(),
        "a live holder must survive being probed"
    );
}

/// README.relayd.md: a node back after an abrupt reboot claims an id
/// its dead old connection still holds. The 409 it gets triggers a
/// probe of that holder, which goes unanswered, so the id frees within
/// one PING timeout rather than a whole heartbeat cycle and the redial
/// succeeds.
#[tokio::test(flavor = "multi_thread")]
async fn a_dead_holder_is_reaped_when_its_id_is_reclaimed() {
    let (node_addr, connect_addr, nodes) = start_relay().await;
    let (_dead, mute) = stub_node_that_goes_silent(node_addr, TEST_ID)
        .await
        .unwrap();
    wait_registered(&nodes, TEST_ID).await;
    mute.await;

    // the reboot's first redial: refused, but it wakes the probe
    let err = stub_node(node_addr, TEST_ID)
        .await
        .expect_err("the dead holder still holds the id");
    let tokio_tungstenite::tungstenite::Error::Http(resp) = err else {
        panic!("expected an HTTP rejection, got {err:?}");
    };
    assert_eq!(resp.status(), 409);
    wait_released(
        &nodes,
        TEST_ID,
        node::HEARTBEAT_TIMEOUT + STEP,
        "the probe must reap the dead holder well before the next heartbeat",
    )
    .await;

    let _node = stub_node(node_addr, TEST_ID).await.unwrap();
    wait_registered(&nodes, TEST_ID).await;
    let (_stream, status) = send_connect(connect_addr, TEST_ID, b"").await;
    assert_eq!(status, "HTTP/1.1 200 Connection established");
}

#[tokio::test(flavor = "multi_thread")]
async fn id_is_reclaimable_after_the_node_drops() {
    let (node_addr, connect_addr, nodes) = start_relay().await;
    let node = stub_node(node_addr, TEST_ID).await.unwrap();
    wait_registered(&nodes, TEST_ID).await;

    // aborting drops the stub's WebSocket; the relay's conn driver ends
    // and releases the id
    node.abort();
    wait_released(
        &nodes,
        TEST_ID,
        STEP,
        "id must be released when the connection dies",
    )
    .await;

    let _node = stub_node(node_addr, TEST_ID).await.unwrap();
    wait_registered(&nodes, TEST_ID).await;
    let (_stream, status) = send_connect(connect_addr, TEST_ID, b"").await;
    assert_eq!(status, "HTTP/1.1 200 Connection established");
}

/// A peer that completes the upgrade and then says nothing holds a
/// reservation but is not a node: h2's client handshake never waits for
/// the peer, so only its first pong proves anyone is listening. Callers
/// must not be routed at it, and it must be given up on.
#[tokio::test(flavor = "multi_thread")]
async fn a_silent_peer_never_becomes_a_node() {
    let (node_addr, connect_addr, nodes) = start_relay().await;
    let tcp = TcpStream::connect(node_addr).await.unwrap();
    let url = format!("ws://{node_addr}{TUNNEL_PATH}?node_id={TEST_ID}");
    // held open and never read: the h2 preface and PING land unanswered
    let (_ws, _response) = tokio_tungstenite::client_async(url, tcp).await.unwrap();
    let id = TEST_ID.parse::<NodeId>().unwrap();
    assert!(nodes.occupied(id), "the upgrade reserves the id");

    // long enough for the relay to have run its h2 handshake many times over
    tokio::time::sleep(node::HEARTBEAT_TIMEOUT / 4).await;
    assert!(
        nodes.get(id).is_none(),
        "a peer that never answered a PING must not be routable"
    );
    let (_stream, status) = send_connect(connect_addr, TEST_ID, b"").await;
    assert_eq!(status, "HTTP/1.1 502 Bad Gateway");

    wait_released(
        &nodes,
        TEST_ID,
        node::HEARTBEAT_TIMEOUT + STEP,
        "a peer that never answers the first PING must be given up on",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn upgrade_without_id_is_rejected() {
    let (node_addr, _connect_addr, _nodes) = start_relay().await;
    let tcp = TcpStream::connect(node_addr).await.unwrap();
    let err = tokio_tungstenite::client_async(format!("ws://{node_addr}{TUNNEL_PATH}"), tcp)
        .await
        .expect_err("upgrade without id must fail");
    let tokio_tungstenite::tungstenite::Error::Http(resp) = err else {
        panic!("expected an HTTP rejection, got {err:?}");
    };
    assert_eq!(resp.status(), 400);
}

/// Callers share one tunnel: their streams must run at the same time,
/// in both directions, not one after the other.
#[tokio::test(flavor = "multi_thread")]
async fn callers_are_multiplexed_onto_one_tunnel() {
    let (node_addr, connect_addr, nodes) = start_relay().await;
    let _node = stub_node(node_addr, TEST_ID).await.unwrap();
    wait_registered(&nodes, TEST_ID).await;

    let (mut a, status_a) = send_connect(connect_addr, TEST_ID, b"").await;
    let (mut b, status_b) = send_connect(connect_addr, TEST_ID, b"").await;
    assert_eq!(status_a, "HTTP/1.1 200 Connection established");
    assert_eq!(status_b, "HTTP/1.1 200 Connection established");

    for round in 0..4u8 {
        a.write_all(b"aaaa").await.unwrap();
        b.write_all(b"bbbb").await.unwrap();
        let mut got_a = [0u8; 4];
        let mut got_b = [0u8; 4];
        tokio::time::timeout(STEP, a.read_exact(&mut got_a))
            .await
            .unwrap_or_else(|_| panic!("caller a stalled in round {round}"))
            .unwrap();
        tokio::time::timeout(STEP, b.read_exact(&mut got_b))
            .await
            .unwrap_or_else(|_| panic!("caller b stalled in round {round}"))
            .unwrap();
        assert_eq!((&got_a, &got_b), (b"aaaa", b"bbbb"));
    }
}

/// Callers whose streams stopped draining -- a hung local service, a
/// caller that went away -- must not starve the other callers on the
/// same tunnel. That is what `CONNECTION_WINDOW` is sized for: with
/// h2's default of one stream window for the whole connection, the
/// wedged callers below own all of it and the healthy caller never sees
/// its echo.
#[tokio::test(flavor = "multi_thread")]
async fn hung_streams_do_not_starve_other_callers() {
    // few enough that a correctly sized connection window has room for
    // all of them and one more
    const HUNG_CALLERS: usize = 8;

    let (node_addr, connect_addr, nodes) = start_relay().await;
    let _node = stub_node_hanging_streams(node_addr, TEST_ID, HUNG_CALLERS)
        .await
        .unwrap();
    wait_registered(&nodes, TEST_ID).await;

    // the hung callers keep pushing for the rest of the test, so they
    // hold every byte of window they can claim and reclaim whatever
    // frees up, like a real hung service would
    let mut pushers = Vec::new();
    let mut pushed = Vec::new();
    for _ in 0..HUNG_CALLERS {
        let (mut hung, status) = send_connect(connect_addr, TEST_ID, b"").await;
        assert_eq!(status, "HTTP/1.1 200 Connection established");
        let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        pushed.push(Arc::clone(&counter));
        pushers.push(tokio::spawn(async move {
            let chunk = vec![0x41u8; STREAM_WINDOW as usize];
            while hung.write_all(&chunk).await.is_ok() {
                counter.fetch_add(chunk.len(), std::sync::atomic::Ordering::Relaxed);
            }
        }));
    }

    // wait until every hung caller is wedged and holding window
    let total = || -> usize {
        pushed
            .iter()
            .map(|c| c.load(std::sync::atomic::Ordering::Relaxed))
            .sum()
    };
    tokio::time::timeout(STEP, async {
        loop {
            let before = total();
            tokio::time::sleep(Duration::from_millis(300)).await;
            if total() == before && before > 0 {
                return;
            }
        }
    })
    .await
    .expect("the hung callers must wedge");

    // half a stream window, so leftover bytes of window will not do
    let ping = vec![0x2Au8; STREAM_WINDOW as usize / 2];
    let (mut healthy, status) = send_connect(connect_addr, TEST_ID, b"").await;
    assert_eq!(status, "HTTP/1.1 200 Connection established");
    let mut got = vec![0u8; ping.len()];
    let echo = tokio::time::timeout(STEP, async {
        healthy.write_all(&ping).await.unwrap();
        healthy.read_exact(&mut got).await.unwrap();
    })
    .await;
    for pusher in pushers {
        pusher.abort();
    }
    echo.expect("hung streams must not block a healthy caller");
    assert_eq!(got, ping);
}

/// The node advertises `MAX_TUNNEL_STREAMS`, so that many callers is
/// the point where the next one has to queue for a free slot instead of
/// being served right away -- and is served once one frees up.
#[tokio::test(flavor = "multi_thread")]
async fn a_caller_beyond_the_stream_limit_waits_for_a_slot() {
    let (node_addr, connect_addr, nodes) = start_relay().await;
    let _node = stub_node(node_addr, TEST_ID).await.unwrap();
    wait_registered(&nodes, TEST_ID).await;

    let mut held = Vec::new();
    for _ in 0..MAX_TUNNEL_STREAMS {
        let (stream, status) = send_connect(connect_addr, TEST_ID, b"").await;
        assert_eq!(status, "HTTP/1.1 200 Connection established");
        held.push(stream);
    }

    let mut queued = open_connect(connect_addr, TEST_ID, b"").await;
    assert_eq!(
        read_status(&mut queued, SLOT_TIMEOUT / 5).await,
        None,
        "a caller beyond the limit must not be served yet"
    );

    held.pop();
    assert_eq!(
        read_status(&mut queued, STEP).await.as_deref(),
        Some("HTTP/1.1 200 Connection established"),
    );
    queued.write_all(b"ping").await.unwrap();
    let mut got = [0u8; 4];
    tokio::time::timeout(STEP, queued.read_exact(&mut got))
        .await
        .expect("the queued caller must be spliced")
        .unwrap();
    assert_eq!(&got, b"ping");
}

/// A caller that never gets a slot must be told so, rather than left
/// hanging on a socket that will never answer.
#[tokio::test(flavor = "multi_thread")]
async fn a_caller_gets_503_when_the_node_stays_full() {
    let (node_addr, connect_addr, nodes) = start_relay().await;
    let _node = stub_node(node_addr, TEST_ID).await.unwrap();
    wait_registered(&nodes, TEST_ID).await;

    let mut held = Vec::new();
    for _ in 0..MAX_TUNNEL_STREAMS {
        let (stream, status) = send_connect(connect_addr, TEST_ID, b"").await;
        assert_eq!(status, "HTTP/1.1 200 Connection established");
        held.push(stream);
    }

    // more than one: each waits its turn in the relay's line for a slot,
    // and each must be told, not only the head of the line
    for caller in 0..3 {
        let (_stream, status) = send_connect(connect_addr, TEST_ID, b"").await;
        assert_eq!(
            status, "HTTP/1.1 503 Service Unavailable",
            "caller {caller} on a full node"
        );
    }

    // resetting the queued requests must not have taken the tunnel down
    held.pop();
    let (mut next, status) = send_connect(connect_addr, TEST_ID, b"").await;
    assert_eq!(status, "HTTP/1.1 200 Connection established");
    next.write_all(b"ping").await.unwrap();
    let mut got = [0u8; 4];
    tokio::time::timeout(STEP, next.read_exact(&mut got))
        .await
        .expect("the tunnel must have survived the rejected callers")
        .unwrap();
    assert_eq!(&got, b"ping");
}

/// The same wait, but with the node's slots free: that is not the relay
/// out of capacity, that is a node not answering, and the caller and the
/// log must not be told otherwise.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_that_does_not_answer_is_not_reported_as_full() {
    let (node_addr, connect_addr, nodes) = start_relay().await;
    let _node = stub_node_never_answering(node_addr, TEST_ID).await.unwrap();
    wait_registered(&nodes, TEST_ID).await;

    let (_stream, status) = send_connect(connect_addr, TEST_ID, b"").await;
    assert_eq!(status, "HTTP/1.1 502 Bad Gateway");

    // the tunnel itself is fine as far as the relay can tell, and the
    // slot the caller held while it waited on the node is back
    let (_h2, load) = nodes
        .get(TEST_ID.parse().unwrap())
        .expect("the node must still be registered");
    tokio::time::timeout(STEP, async {
        while load.active() != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the slot must be released with the caller");
    assert_eq!(load.queued(), 0);
}

/// A caller that gave up waiting must be gone for good: it holds no
/// slot, and the node never hears of it. Left to h2's own queue, an
/// abandoned request keeps its HEADERS, and the next slot to free would
/// be spent opening and resetting one phantom stream per waiter before
/// a live caller gets it.
#[tokio::test(flavor = "multi_thread")]
async fn a_timed_out_waiter_leaves_no_phantom_stream() {
    const GAVE_UP: usize = 3;
    let (node_addr, connect_addr, nodes) = start_relay().await;
    let (_node, seen) = stub_node_counting_streams(node_addr, TEST_ID, 0)
        .await
        .unwrap();
    wait_registered(&nodes, TEST_ID).await;
    let (_h2, load) = nodes.get(TEST_ID.parse().unwrap()).unwrap();

    let mut held = Vec::new();
    for _ in 0..MAX_TUNNEL_STREAMS {
        let (stream, status) = send_connect(connect_addr, TEST_ID, b"").await;
        assert_eq!(status, "HTTP/1.1 200 Connection established");
        held.push(stream);
    }
    let served = MAX_TUNNEL_STREAMS as usize;
    assert_eq!(seen.load(Ordering::Relaxed), served);

    let mut waiters = Vec::new();
    for _ in 0..GAVE_UP {
        waiters.push(open_connect(connect_addr, TEST_ID, b"").await);
    }
    for (n, waiter) in waiters.iter_mut().enumerate() {
        assert_eq!(
            read_status(waiter, STEP).await.as_deref(),
            Some("HTTP/1.1 503 Service Unavailable"),
            "waiter {n} must be turned away"
        );
    }
    // nothing of them is left: no slot held, nobody in line, and the
    // node was never asked
    assert_eq!(load.active(), MAX_TUNNEL_STREAMS);
    assert_eq!(load.queued(), 0);
    assert_eq!(
        seen.load(Ordering::Relaxed),
        served,
        "the node must not hear of a caller that gave up"
    );

    // the freed slot goes to a live caller, with no phantom ahead of it
    held.pop();
    let (mut next, status) = send_connect(connect_addr, TEST_ID, b"").await;
    assert_eq!(status, "HTTP/1.1 200 Connection established");
    assert_eq!(
        seen.load(Ordering::Relaxed),
        served + 1,
        "exactly one more stream must have reached the node"
    );
    next.write_all(b"ping").await.unwrap();
    let mut got = [0u8; 4];
    tokio::time::timeout(STEP, next.read_exact(&mut got))
        .await
        .expect("the live caller must be spliced")
        .unwrap();
    assert_eq!(&got, b"ping");
}
