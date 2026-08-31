// SPDX-License-Identifier: LGPL-2.1-or-later

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use varlink_http_bridge::tunnel::{NodeId, TUNNEL_PATH, WsByteStream, h2_server_builder, splice};

use crate::registry::Nodes;
use crate::{caller, node};

const TEST_ID: &str = "0123456789abcdef0123456789abcdef";
const OTHER_ID: &str = "fedcba9876543210fedcba9876543210";
const STEP: Duration = Duration::from_secs(5);

async fn start_relay() -> (SocketAddr, SocketAddr, Arc<Nodes>) {
    let node_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let connect_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let node_addr = node_listener.local_addr().unwrap();
    let connect_addr = connect_listener.local_addr().unwrap();
    let nodes = Arc::new(Nodes::default());
    tokio::spawn(node::serve(node_listener, None, Arc::clone(&nodes)));
    tokio::spawn(caller::serve(connect_listener, Arc::clone(&nodes)));
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
    let tcp = TcpStream::connect(node_addr).await.unwrap();
    let url = format!("ws://{node_addr}{TUNNEL_PATH}?id={id}");
    let (ws, _response) = tokio_tungstenite::client_async(url, tcp).await?;
    Ok(tokio::spawn(async move {
        let mut conn = h2_server_builder()
            .handshake::<_, bytes::Bytes>(WsByteStream::new(ws))
            .await
            .unwrap();
        while let Some(Ok((req, mut respond))) = conn.accept().await {
            let body = req.into_body();
            let send = respond
                .send_response(http::Response::new(()), false)
                .unwrap();
            tokio::spawn(async move {
                let (io, echo) = tokio::io::duplex(4096);
                tokio::spawn(async move {
                    let (mut rd, mut wr) = tokio::io::split(echo);
                    let _ = tokio::io::copy(&mut rd, &mut wr).await;
                    let _ = wr.shutdown().await;
                });
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
    let mut stream = TcpStream::connect(connect_addr).await.unwrap();
    let mut request =
        format!("CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\n\r\n").into_bytes();
    request.extend_from_slice(extra);
    stream.write_all(&request).await.unwrap();

    let status = tokio::time::timeout(STEP, async {
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
    .expect("relay must answer the CONNECT");
    (stream, status)
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
