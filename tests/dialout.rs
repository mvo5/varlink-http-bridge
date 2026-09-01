// SPDX-License-Identifier: LGPL-2.1-or-later

//! The real end-to-end path from README.relayd.md: a varlink-relayd
//! process, a varlink-httpd process dialing out to it, and a caller
//! reaching the bridge through `CONNECT <id>`.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use varlink_http_bridge::tunnel::MachineId;

const DEADLINE: Duration = Duration::from_secs(30);

/// Kills the child on drop so a failing test leaves no processes.
struct Daemon(Child);

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn_relayd() -> (Daemon, String, String) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_varlink-relayd"))
        .args([
            "--insecure",
            "--bind",
            "127.0.0.1:0",
            "--connect-bind",
            "127.0.0.1:0",
        ])
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawning varlink-relayd");
    // the startup line logs the resolved addresses:
    // "nodes dial in at 127.0.0.1:PORT/v1/tunnel, callers CONNECT to 127.0.0.1:PORT"
    let stderr = child.stderr.take().expect("piped stderr");
    let mut lines = BufReader::new(stderr).lines();
    let line = loop {
        let line = lines
            .next()
            .expect("relayd exited before logging its addresses")
            .expect("reading relayd stderr");
        if line.contains("nodes dial in at ") {
            break line;
        }
    };
    let node_addr = line
        .split("nodes dial in at ")
        .nth(1)
        .and_then(|s| s.split("/v1/tunnel").next())
        .expect("node address in startup line")
        .to_string();
    let connect_addr = line
        .split("callers CONNECT to ")
        .nth(1)
        .expect("caller address in startup line")
        .trim()
        .to_string();
    // drain the rest so the child never blocks on a full pipe
    std::thread::spawn(move || for _ in lines {});
    (Daemon(child), node_addr, connect_addr)
}

fn spawn_httpd(
    node_addr: &str,
    socket_dir: &std::path::Path,
    instance: Option<&str>,
    args: &[&str],
) -> Daemon {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_varlink-httpd"));
    cmd.args(args)
        .arg("--relay")
        .arg(format!("ws://{node_addr}"))
        .stderr(Stdio::null())
        .stdout(Stdio::null());
    if let Some(label) = instance {
        cmd.args(["--instance", label]);
    }
    cmd.arg(socket_dir);
    Daemon(cmd.spawn().expect("spawning varlink-httpd"))
}

/// CONNECT to `id` through the relay, retrying while the node has not
/// registered yet, then return the established stream.
fn connect_via_relay(connect_addr: &str, id: &str) -> TcpStream {
    let deadline = Instant::now() + DEADLINE;
    loop {
        let mut stream = TcpStream::connect(connect_addr).expect("connecting to relay");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        stream
            .write_all(format!("CONNECT {id}:80 HTTP/1.1\r\nHost: {id}\r\n\r\n").as_bytes())
            .unwrap();
        let mut head = Vec::new();
        let mut byte = [0u8; 1];
        while !head.ends_with(b"\r\n\r\n") {
            let mut r = &stream;
            r.read_exact(&mut byte).expect("reading CONNECT reply");
            head.push(byte[0]);
        }
        let status = String::from_utf8_lossy(&head);
        if status.starts_with("HTTP/1.1 200") {
            return stream;
        }
        assert!(
            Instant::now() < deadline,
            "node never registered, last relay answer: {}",
            status.lines().next().unwrap_or_default()
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// GET a path from the bridge through an established CONNECT tunnel.
fn get_via(mut stream: TcpStream, path: &str) -> String {
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: node\r\nConnection: close\r\n\r\n").as_bytes(),
        )
        .unwrap();
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("reading bridge response");
    response
}

#[test]
fn caller_reaches_the_bridge_through_the_relay() {
    // needs a usable machine id, same states test as machine-id(5)
    let Ok(text) = std::fs::read_to_string("/etc/machine-id") else {
        return;
    };
    let Ok(machine_id) = text.trim().parse::<MachineId>() else {
        return;
    };
    let default_id = machine_id.node_id().unwrap().to_string();
    let update_id = machine_id.instance_node_id("update").unwrap().to_string();

    let (_relayd, node_addr, connect_addr) = spawn_relayd();
    let sockets = tempfile::tempdir().unwrap();

    // the default instance and a second one on the same host
    let insecure = ["--insecure", "--bind", "127.0.0.1:0"];
    let _bridge = spawn_httpd(&node_addr, sockets.path(), None, &insecure);
    let _update_bridge = spawn_httpd(&node_addr, sockets.path(), Some("update"), &insecure);

    for id in [&default_id, &update_id] {
        let stream = connect_via_relay(&connect_addr, id);
        let response = get_via(stream, "/health");
        assert!(
            response.starts_with("HTTP/1.1 200"),
            "unexpected response for {id}: {}",
            response.lines().next().unwrap_or_default()
        );
    }

    // both instances stay independently reachable, distinct ids
    assert_ne!(default_id, update_id);
}

/// The update-trigger deployment shape: no local listener, TLS kept,
/// requests unauthenticated (`--bind=none --auth=none`).
#[test]
fn relay_only_instance_with_auth_none() {
    // needs a usable machine id, same states test as machine-id(5)
    let Ok(text) = std::fs::read_to_string("/etc/machine-id") else {
        return;
    };
    let Ok(machine_id) = text.trim().parse::<MachineId>() else {
        return;
    };
    let update_id = machine_id.instance_node_id("update").unwrap().to_string();

    let (_relayd, node_addr, connect_addr) = spawn_relayd();
    let sockets = tempfile::tempdir().unwrap();
    // the self-signed cert must not land in the real state directory
    let state = tempfile::tempdir().unwrap();
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_varlink-httpd"));
    cmd.env("STATE_DIRECTORY", state.path())
        .args(["--auth=none", "--bind=none", "--instance", "update"])
        .arg("--relay")
        .arg(format!("ws://{node_addr}"))
        .arg(sockets.path())
        .stderr(Stdio::null())
        .stdout(Stdio::null());
    let _bridge = Daemon(cmd.spawn().expect("spawning varlink-httpd"));

    let stream = connect_via_relay(&connect_addr, &update_id);

    // the caller's TLS terminates in the bridge; identity verification
    // is out of scope here, the test only needs the transport
    use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
    let mut builder = SslConnector::builder(SslMethod::tls_client()).unwrap();
    builder.set_verify(SslVerifyMode::NONE);
    let connector = builder.build();
    let mut config = connector.configure().unwrap();
    config.set_verify_hostname(false);
    let mut tls = config
        .connect(&update_id, stream)
        .expect("TLS handshake with the bridge through the tunnel");

    // /sockets sits behind the auth middleware, so a 200 proves
    // --auth=none actually authenticates the anonymous caller
    tls.write_all(b"GET /sockets HTTP/1.1\r\nHost: node\r\nConnection: close\r\n\r\n")
        .unwrap();
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        tls.read_exact(&mut byte).expect("reading bridge response");
        head.push(byte[0]);
    }
    let status = String::from_utf8_lossy(&head);
    assert!(
        status.starts_with("HTTP/1.1 200"),
        "unexpected response: {}",
        status.lines().next().unwrap_or_default()
    );
}
