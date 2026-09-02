// SPDX-License-Identifier: LGPL-2.1-or-later

use std::net::SocketAddr;
use std::sync::Arc;
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
    let tcp = TcpStream::connect(node_addr).await.unwrap();
    let url = format!("ws://{node_addr}{TUNNEL_PATH}?id={id}");
    let (ws, _response) = tokio_tungstenite::client_async(url, tcp).await?;
    Ok(tokio::spawn(async move {
        let mut conn = h2_server_builder()
            .handshake::<_, bytes::Bytes>(WsByteStream::new(ws))
            .await
            .unwrap();
        let mut streams = 0;
        // keeps the hung streams' local peers open without reading them
        let mut hung = Vec::new();
        while let Some(Ok((req, mut respond))) = conn.accept().await {
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
    }))
}

/// The relay attaches a node's h2 handle only after the h2 handshake,
/// which runs in the stub's task; tests must not race it.
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

/// A node that claims a taken id retries forever, and one relay serves
/// a fleet of them, so the complaint must not repeat per attempt.
#[test]
fn repeated_duplicate_claims_are_only_reported_once() {
    let nodes = Nodes::default();
    let id = TEST_ID.parse::<NodeId>().unwrap();
    let _holder = nodes.reserve(id).expect("first claim wins");

    let Err(first) = nodes.reserve(id) else {
        panic!("the second claim must be refused");
    };
    assert!(first.loud, "the first collision is worth a warning");
    for attempt in 0..5 {
        let Err(again) = nodes.reserve(id) else {
            panic!("the claim must stay refused");
        };
        assert!(!again.loud, "retry {attempt} must stay quiet");
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
}

#[tokio::test(flavor = "multi_thread")]
async fn id_is_reclaimable_after_the_node_drops() {
    let (node_addr, connect_addr, nodes) = start_relay().await;
    let node = stub_node(node_addr, TEST_ID).await.unwrap();
    wait_registered(&nodes, TEST_ID).await;

    // aborting drops the stub's WebSocket; the relay's conn driver ends
    // and releases the id
    node.abort();
    let id = TEST_ID.parse::<NodeId>().unwrap();
    tokio::time::timeout(STEP, async {
        while nodes.occupied(id) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("id must be released when the connection dies");

    let _node = stub_node(node_addr, TEST_ID).await.unwrap();
    wait_registered(&nodes, TEST_ID).await;
    let (_stream, status) = send_connect(connect_addr, TEST_ID, b"").await;
    assert_eq!(status, "HTTP/1.1 200 Connection established");
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

    // more than one: h2 queues the first request beyond the limit in the
    // connection and the next ones behind that queue, both must get the
    // 503
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
