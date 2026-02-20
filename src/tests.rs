use super::*;
use futures_util::{SinkExt, StreamExt};
use gethostname::gethostname;
use reqwest::Client;
use scopeguard::defer;
use std::os::fd::OwnedFd;
use tokio::task::JoinSet;
use tokio_tungstenite::tungstenite::Message as WsMsg;

/// Path to the varlinkctl-helper binary built by cargo alongside the test binary.
fn helper_binary() -> std::path::PathBuf {
    let test_exe = std::env::current_exe().expect("failed to get test exe path");
    // test binary is in target/debug/deps/, helper is in target/debug/
    test_exe
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("varlinkctl-helper")
}

async fn run_test_server(
    varlink_sockets_path: &str,
) -> (tokio::task::JoinHandle<()>, std::net::SocketAddr) {
    run_test_server_with_auth(varlink_sockets_path, Vec::new()).await
}

async fn run_test_server_with_auth(
    varlink_sockets_path: &str,
    authenticators: Vec<Box<dyn Authenticator>>,
) -> (tokio::task::JoinHandle<()>, std::net::SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind to random port failed");
    let local_addr = listener
        .local_addr()
        .expect("failed to extract local address");

    let varlink_sockets_path = varlink_sockets_path.to_string();
    let task_handle = tokio::spawn(async move {
        run_server(&varlink_sockets_path, listener, None, authenticators)
            .await
            .expect("server failed")
    });

    (task_handle, local_addr)
}

#[test_with::path(/run/systemd/io.systemd.Hostname)]
#[tokio::test]
async fn test_integration_real_systemd_hostname_post() {
    let (server, local_addr) = run_test_server("/run/systemd").await;
    defer! {
        server.abort();
    };

    let client = Client::new();
    let res = client
        .post(format!(
            "http://{}/call/io.systemd.Hostname.Describe",
            local_addr,
        ))
        .json(&json!({}))
        .send()
        .await
        .expect("failed to post to test server");
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.expect("varlink body invalid");
    assert!(body["Hostname"].as_str().is_some_and(|h| !h.is_empty()));
}

#[test_with::path(/run/systemd/io.systemd.Hostname)]
#[tokio::test]
async fn test_integration_real_systemd_socket_get() {
    let (server, local_addr) = run_test_server("/run/systemd").await;
    defer! {
        server.abort();
    };

    let client = Client::new();
    let res = client
        .get(format!("http://{}/sockets/io.systemd.Hostname", local_addr,))
        .send()
        .await
        .expect("failed to get from test server");
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.expect("varlink body invalid");
    assert_eq!(body["product"], "systemd (systemd-hostnamed)");
}

#[test_with::path(/run/systemd/io.systemd.Hostname)]
#[tokio::test]
async fn test_integration_real_systemd_sockets_get() {
    let (server, local_addr) = run_test_server("/run/systemd").await;
    defer! {
        server.abort();
    };

    let client = Client::new();
    let res = client
        .get(format!("http://{}/sockets", local_addr,))
        .send()
        .await
        .expect("failed to get from test server");
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.expect("varlink body invalid");
    assert!(
        body["sockets"]
            .as_array()
            .expect("sockets not an array")
            .contains(&json!("io.systemd.Hostname"))
    );
}

#[test_with::path(/run/systemd/io.systemd.Hostname)]
#[tokio::test]
async fn test_integration_real_systemd_socket_interface_get() {
    let (server, local_addr) = run_test_server("/run/systemd").await;
    defer! {
        server.abort();
    };

    let client = Client::new();
    let res = client
        .get(format!(
            "http://{}/sockets/io.systemd.Hostname/io.systemd.Hostname",
            local_addr,
        ))
        .send()
        .await
        .expect("failed to get from test server");
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.expect("varlink body invalid");
    assert_eq!(body.get("method_names").unwrap(), &json!(["Describe"]));
}

#[test_with::path(/run/systemd/io.systemd.Hostname)]
#[tokio::test]
async fn test_integration_real_systemd_hostname_parallel() {
    let (server, local_addr) = run_test_server("/run/systemd").await;
    defer! {
        server.abort();
    };

    let url = format!("http://{}/call/io.systemd.Hostname.Describe", local_addr);

    const NUM_TASKS: u32 = 10;
    let mut set = JoinSet::new();
    let client = Client::new();
    for _ in 0..NUM_TASKS {
        let client = client.clone();
        let target_url = url.clone();

        set.spawn(async move {
            let res = client
                .post(target_url)
                .json(&json!({}))
                .send()
                .await
                .expect("failed to post to test server");

            assert_eq!(res.status(), 200);
            let body: Value = res.json().await.expect("varlink body invalid");

            body["Hostname"].as_str().unwrap_or_default().to_string()
        });
    }
    let expected_hostname = gethostname().into_string().expect("failed to get hostname");

    let mut count = 0;
    while let Some(res) = set.join_next().await {
        let hostname = res.expect("client task to collect results panicked");
        assert_eq!(expected_hostname, hostname);
        count += 1;
    }
    assert_eq!(count, NUM_TASKS);
}

#[test_with::path(/run/systemd/io.systemd.Hostname)]
#[tokio::test]
async fn test_integration_real_systemd_socket_query_param() {
    let (server, local_addr) = run_test_server("/run/systemd").await;
    defer! {
        server.abort();
    };

    let client = Client::new();
    let res = client
        .post(format!(
            "http://{}/call/org.varlink.service.GetInfo?socket=io.systemd.Hostname",
            local_addr,
        ))
        .json(&json!({}))
        .send()
        .await
        .expect("failed to post to test server");
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.expect("varlink body invalid");
    assert_eq!(body["product"], "systemd (systemd-hostnamed)");
}

#[test_with::path(/run/systemd)]
#[tokio::test]
async fn test_error_bad_request_on_malformed_json() {
    let (server, local_addr) = run_test_server("/run/systemd").await;
    defer! {
        server.abort();
    };
    let client = Client::new();

    let res = client
        .post(format!(
            "http://{}/call/org.varlink.service.GetInfo",
            local_addr,
        ))
        .body("this is NOT valid json")
        .header("Content-Type", "application/json")
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[test_with::path(/run/systemd)]
#[tokio::test]
async fn test_error_unknown_varlink_address() {
    let (server, local_addr) = run_test_server("/run/systemd").await;
    defer! {
        server.abort();
    };
    let client = Client::new();

    let res = client
        .post(format!(
            "http://{}/call/no.such.address.SomeMethod",
            local_addr,
        ))
        .body("{}")
        .header("Content-Type", "application/json")
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::BAD_GATEWAY);
    let body: Value = res.json().await.expect("error body invalid");
    // TODO: see comment in impl From<varlink::Error>, would be great to improve this upstream
    assert_eq!(body["error"], "IO error");
}

#[test_with::path(/run/systemd/io.systemd.Hostname)]
#[tokio::test]
async fn test_error_404_for_missing_method() {
    let (server, local_addr) = run_test_server("/run/systemd").await;
    defer! {
        server.abort();
    };
    let client = Client::new();

    let res = client
        .post(format!(
            "http://{}/call/com.missing.Call?socket=io.systemd.Hostname",
            local_addr
        ))
        .json(&json!({}))
        .send()
        .await
        .expect("failed to post to test server");

    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    let body: Value = res.json().await.expect("error body invalid");
    assert_eq!(body["error"], "Method not found: 'com.missing.Call'");
}

#[test_with::path(/run/systemd)]
#[tokio::test]
async fn test_error_bad_request_for_unclean_address() {
    let (server, local_addr) = run_test_server("/run/systemd").await;
    defer! {
        server.abort();
    };
    let client = Client::new();

    let res = client
        .post(format!(
            // %2f is url encoding for "/" so socket param is ../io.systemd.Hostname
            "http://{}/call/com.missing.Call?socket=..%2fio.systemd.Hostname",
            local_addr
        ))
        .json(&json!({}))
        .send()
        .await
        .expect("failed to post to test server");

    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let body: Value = res.json().await.expect("error body invalid");
    assert_eq!(
        body["error"],
        "invalid socket name (must be a valid varlink interface name): ../io.systemd.Hostname"
    );
}

#[test_with::path(/run/systemd)]
#[tokio::test]
async fn test_error_bad_request_for_invalid_chars_in_address() {
    let (server, local_addr) = run_test_server("/run/systemd").await;
    defer! {
        server.abort();
    };
    let client = Client::new();

    let res = client
        .post(format!(
            // %0A is \n
            "http://{}/call/com.missing.Call?socket=io.systemd.Hostname%0Abad-msg",
            local_addr
        ))
        .json(&json!({}))
        .send()
        .await
        .expect("failed to post to test server");

    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let body: Value = res.json().await.expect("error body invalid");
    assert_eq!(
        body["error"],
        "invalid socket name (must be a valid varlink interface name): io.systemd.Hostname\nbad-msg"
    );
}

#[test_with::path(/run/systemd)]
#[tokio::test]
async fn test_error_bad_request_for_method_without_dots() {
    let (server, local_addr) = run_test_server("/run/systemd").await;
    defer! {
        server.abort();
    };
    let client = Client::new();

    let res = client
        .post(format!("http://{}/call/NoDots", local_addr))
        .json(&json!({}))
        .send()
        .await
        .expect("failed to post to test server");

    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let body: Value = res.json().await.expect("error body invalid");
    assert_eq!(
        body["error"],
        "cannot derive socket from method 'NoDots': no dots in name"
    );
}

#[test_with::path(/run/systemd)]
#[tokio::test]
async fn test_health_endpoint() {
    let (server, local_addr) = run_test_server("/run/systemd").await;
    defer! {
        server.abort();
    };

    let client = Client::new();
    let res = client
        .get(format!("http://{}/health", local_addr))
        .send()
        .await
        .expect("failed to get health endpoint");

    assert_eq!(res.status(), 200);
}

#[tokio::test]
async fn test_varlink_sockets_dir_or_file_missing() {
    let varlink_sockets_dir_or_file = "/does-not-exist".to_string();

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind to random port failed");
    let res = run_server(&varlink_sockets_dir_or_file, listener, None, Vec::new()).await;

    assert!(res.is_err());
    assert!(
        res.unwrap_err()
            .to_string()
            .contains("failed to stat /does-not-exist"),
    );
}

#[test_with::path(/run/systemd/io.systemd.Hostname)]
#[tokio::test]
async fn test_single_socket_post() {
    let (server, local_addr) = run_test_server("/run/systemd/io.systemd.Hostname").await;
    defer! {
        server.abort();
    };

    let client = Client::new();
    let res = client
        .post(format!(
            "http://{}/call/io.systemd.Hostname.Describe",
            local_addr,
        ))
        .json(&json!({}))
        .send()
        .await
        .expect("failed to post to test server");
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.expect("varlink body invalid");
    assert!(body["Hostname"].as_str().is_some_and(|h| !h.is_empty()));
}

#[test_with::path(/run/systemd/io.systemd.Hostname)]
#[tokio::test]
async fn test_single_socket_rejects_wrong_name() {
    let (server, local_addr) = run_test_server("/run/systemd/io.systemd.Hostname").await;
    defer! {
        server.abort();
    };

    let client = Client::new();
    let res = client
        .post(format!(
            "http://{}/call/io.systemd.Wrong.Describe",
            local_addr,
        ))
        .json(&json!({}))
        .send()
        .await
        .expect("failed to post to test server");
    assert_eq!(res.status(), StatusCode::BAD_GATEWAY);
    let body: Value = res.json().await.expect("error body invalid");
    assert_eq!(
        body["error"],
        "socket 'io.systemd.Wrong' not available (only 'io.systemd.Hostname' is available)"
    );
}

#[test_with::path(/run/systemd/io.systemd.Hostname)]
#[tokio::test]
async fn test_varlink_unix_sockets_in_follows_symlinks() {
    let tmpdir = tempfile::tempdir().expect("failed to create tempdir");
    let symlink_path = tmpdir.path().join("io.systemd.Hostname");

    std::os::unix::fs::symlink("/run/systemd/io.systemd.Hostname", &symlink_path)
        .expect("failed to create symlink");

    let dir_fd = OwnedFd::from(std::fs::File::open(tmpdir.path()).unwrap());
    let vs = VarlinkSockets::SocketDir { dirfd: dir_fd };
    let sockets = vs.list_sockets().await.expect("list_sockets failed");
    assert_eq!(sockets, vec!["io.systemd.Hostname"]);
}

#[test_with::path(/run/systemd/io.systemd.Hostname)]
#[tokio::test]
async fn test_varlink_unix_sockets_in_skips_dangling_symlinks() {
    let tmpdir = tempfile::tempdir().expect("failed to create tempdir");

    let good = tmpdir.path().join("io.systemd.Hostname");
    std::os::unix::fs::symlink("/run/systemd/io.systemd.Hostname", &good)
        .expect("failed to create symlink");

    let bad = tmpdir.path().join("io.example.Bad");
    std::os::unix::fs::symlink("/no/such/socket", &bad).expect("failed to create dangling symlink");

    let dir_fd = OwnedFd::from(std::fs::File::open(tmpdir.path()).unwrap());
    let vs = VarlinkSockets::SocketDir { dirfd: dir_fd };
    let sockets = vs
        .list_sockets()
        .await
        .expect("list_sockets should not fail on dangling symlinks");
    assert_eq!(sockets, vec!["io.systemd.Hostname"]);
}

#[test_with::path(/run/systemd/io.systemd.Hostname)]
#[tokio::test]
async fn test_ws_hostname_describe() {
    let (server, local_addr) = run_test_server("/run/systemd").await;
    defer! {
        server.abort();
    };

    let url = format!("ws://{local_addr}/ws/sockets/io.systemd.Hostname");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("WS connect failed");

    let mut msg = r#"{"method":"io.systemd.Hostname.Describe","parameters":{}}"#.to_string();
    msg.push('\0');
    ws.send(WsMsg::Text(msg.into()))
        .await
        .expect("WS send failed");

    // each varlink message arrives as one WS binary frame (with NUL delimiter)
    let msg = ws
        .next()
        .await
        .expect("no WS response")
        .expect("WS recv error");
    let data = msg.into_data();
    let json_bytes = data.strip_suffix(&[0]).unwrap_or(&data);
    let body: Value = serde_json::from_slice(json_bytes).expect("response not valid JSON");

    // raw varlink protocol wraps responses in "parameters"
    let expected_hostname = gethostname().into_string().expect("failed to get hostname");
    assert_eq!(body["parameters"]["Hostname"], expected_hostname);
}

#[test_with::path(/run/systemd/userdb/io.systemd.Multiplexer)]
#[tokio::test]
async fn test_ws_userdb_get_user_record_more() {
    let (server, local_addr) = run_test_server("/run/systemd/userdb").await;
    defer! {
        server.abort();
    };

    let url = format!("ws://{local_addr}/ws/sockets/io.systemd.Multiplexer");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("WS connect failed");

    // Send varlink "more" call as binary frame (NUL-delimited)
    let mut msg = r#"{
        "method": "io.systemd.UserDatabase.GetUserRecord",
        "parameters": {"service": "io.systemd.Multiplexer"},
        "more": true
    }"#
    .as_bytes()
    .to_vec();
    msg.push(0);
    ws.send(WsMsg::Binary(msg.into()))
        .await
        .expect("WS send failed");

    let mut users = Vec::new();
    loop {
        let Some(Ok(msg)) = ws.next().await else {
            break;
        };
        let data = msg.into_data();
        let json_bytes = data.strip_suffix(&[0]).unwrap_or(&data);
        if json_bytes.is_empty() {
            continue;
        }
        let body: Value = serde_json::from_slice(json_bytes).expect("response not valid JSON");

        if body.get("error").is_some() {
            break;
        }
        let name = body["parameters"]["record"]["userName"]
            .as_str()
            .expect("userName missing from user record");
        users.push(name.to_string());

        if !body
            .get("continues")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            break;
        }
    }

    // we expect at least root + current user
    assert!(
        users.len() >= 2,
        "expected at least 2 user records, got users {users:#?}"
    );
}

#[test_with::path(/usr/bin/varlinkctl)]
#[test_with::path(/run/systemd/io.systemd.Hostname)]
#[tokio::test]
async fn test_varlinkctl_helper_hostname_describe() {
    let (server, local_addr) = run_test_server("/run/systemd").await;
    defer! {
        server.abort();
    };

    let bridge_url = format!("http://{local_addr}/ws/sockets/io.systemd.Hostname");
    let output = tokio::process::Command::new("varlinkctl")
        .args([
            "call",
            "--json=short",
            &format!("exec:{}", helper_binary().display()),
            "io.systemd.Hostname.Describe",
            "{}",
        ])
        .env("VARLINK_BRIDGE_URL", &bridge_url)
        .output()
        .await
        .expect("failed to run varlinkctl");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "varlinkctl failed (stderr: {stderr})"
    );

    let stdout = String::from_utf8(output.stdout).expect("invalid UTF-8 in varlinkctl output");
    let line = stdout.trim().trim_start_matches('\x1e');
    let body: Value =
        serde_json::from_str(line).expect("varlinkctl output not valid JSON: {e}: {line:?}");

    let expected_hostname = gethostname().into_string().expect("failed to get hostname");
    assert_eq!(body["Hostname"], expected_hostname);
}

#[test_with::path(/usr/bin/varlinkctl)]
#[test_with::path(/run/systemd/userdb/io.systemd.Multiplexer)]
#[tokio::test]
async fn test_varlinkctl_helper_userdb_get_user_record() {
    let (server, local_addr) = run_test_server("/run/systemd/userdb").await;
    defer! {
        server.abort();
    };

    let bridge_url = format!("http://{local_addr}/ws/sockets/io.systemd.Multiplexer");
    let output = tokio::process::Command::new("varlinkctl")
        .args([
            "call",
            "--more",
            "--json=short",
            &format!("exec:{}", helper_binary().display()),
            "io.systemd.UserDatabase.GetUserRecord",
            r#"{"service":"io.systemd.Multiplexer"}"#,
        ])
        .env("VARLINK_BRIDGE_URL", &bridge_url)
        .output()
        .await
        .expect("failed to run varlinkctl");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "varlinkctl failed (stderr: {stderr})"
    );

    let stdout = String::from_utf8(output.stdout).expect("invalid UTF-8 in varlinkctl output");
    let mut users = Vec::new();
    for line in stdout.lines() {
        // varlinkctl uses JSON Text Sequences (RFC 7464): each record is
        // prefixed with U+001E (Record Separator)
        let line = line.trim().trim_start_matches('\x1e');
        if line.is_empty() {
            continue;
        }
        let body: Value =
            serde_json::from_str(line).expect("varlinkctl output not valid JSON: {e}: {line:?}");
        if let Some(name) = body["record"]["userName"].as_str() {
            users.push(name.to_string());
        }
    }

    // we expect at least root + current user
    assert!(
        users.len() >= 2,
        "expected at least 2 user records, got users {users:#?}"
    );
}

struct TestPki {
    _tmpdir: tempfile::TempDir,
    ca_cert_pem: Vec<u8>,
    server_cert_path: std::path::PathBuf,
    server_key_path: std::path::PathBuf,
    ca_cert_path: std::path::PathBuf,
    client_cert_path: std::path::PathBuf,
    client_key_path: std::path::PathBuf,
    client_cert_pem: Vec<u8>,
    client_key_pem: Vec<u8>,
}

#[rustfmt::skip]
fn make_test_pki() -> TestPki {
    use std::process::Command;

    let tmpdir = tempfile::tempdir().unwrap();
    let d = tmpdir.path();

    let openssl = |args: &[&str]| {
        let out = Command::new("openssl").args(args).output().unwrap();
        assert!(
            out.status.success(),
            "openssl {args:?} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    };

    // CA key + self-signed cert
    openssl(&[
        "req", "-x509", "-newkey", "rsa:2048", "-nodes",
        "-keyout", d.join("ca-key.pem").to_str().unwrap(),
        "-out",    d.join("ca.pem").to_str().unwrap(),
        "-subj",   "/CN=Test CA",
        "-days",   "365",
    ]);

    // Server key + CSR + cert signed by CA (with SAN)
    openssl(&[
        "req", "-newkey", "rsa:2048", "-nodes",
        "-keyout", d.join("server-key.pem").to_str().unwrap(),
        "-out",    d.join("server.csr").to_str().unwrap(),
        "-subj",   "/CN=localhost",
        "-addext", "subjectAltName=DNS:localhost",
    ]);
    openssl(&[
        "x509", "-req",
        "-in",      d.join("server.csr").to_str().unwrap(),
        "-CA",      d.join("ca.pem").to_str().unwrap(),
        "-CAkey",   d.join("ca-key.pem").to_str().unwrap(),
        "-CAcreateserial",
        "-out",     d.join("server-cert.pem").to_str().unwrap(),
        "-days",    "365",
        "-copy_extensions", "copy",
    ]);

    // Client key + CSR + cert signed by CA
    openssl(&[
        "req", "-newkey", "rsa:2048", "-nodes",
        "-keyout", d.join("client-key.pem").to_str().unwrap(),
        "-out",    d.join("client.csr").to_str().unwrap(),
        "-subj",   "/CN=test-client",
    ]);
    openssl(&[
        "x509", "-req",
        "-in",      d.join("client.csr").to_str().unwrap(),
        "-CA",      d.join("ca.pem").to_str().unwrap(),
        "-CAkey",   d.join("ca-key.pem").to_str().unwrap(),
        "-CAcreateserial",
        "-out",     d.join("client-cert.pem").to_str().unwrap(),
        "-days",    "365",
    ]);

    TestPki {
        ca_cert_pem:       std::fs::read(d.join("ca.pem")).unwrap(),
        server_cert_path:  d.join("server-cert.pem"),
        server_key_path:   d.join("server-key.pem"),
        ca_cert_path:      d.join("ca.pem"),
        client_cert_path:  d.join("client-cert.pem"),
        client_key_path:   d.join("client-key.pem"),
        client_cert_pem:   std::fs::read(d.join("client-cert.pem")).unwrap(),
        client_key_pem:    std::fs::read(d.join("client-key.pem")).unwrap(),
        _tmpdir: tmpdir,
    }
}

async fn run_test_tls_server(
    varlink_sockets_path: &str,
    tls_acceptor: openssl::ssl::SslAcceptor,
) -> (tokio::task::JoinHandle<()>, std::net::SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind to random port failed");
    let local_addr = listener
        .local_addr()
        .expect("failed to extract local address");

    let varlink_sockets_path = varlink_sockets_path.to_string();
    let task_handle = tokio::spawn(async move {
        run_server(
            &varlink_sockets_path,
            listener,
            Some(tls_acceptor),
            Vec::new(),
        )
        .await
        .expect("server failed")
    });

    (task_handle, local_addr)
}

#[tokio::test]
async fn test_tls_basic_connection() {
    let pki = make_test_pki();
    let varlink_dir = tempfile::tempdir().unwrap();

    let acceptor = load_tls_acceptor(
        pki.server_cert_path.to_str().unwrap(),
        pki.server_key_path.to_str().unwrap(),
        None,
    )
    .unwrap();

    let (server, local_addr) =
        run_test_tls_server(varlink_dir.path().to_str().unwrap(), acceptor).await;
    defer! {
        server.abort();
    };

    let ca_cert = reqwest::Certificate::from_pem(&pki.ca_cert_pem).unwrap();
    let client = Client::builder()
        .add_root_certificate(ca_cert)
        .resolve("localhost", local_addr)
        .build()
        .unwrap();

    let res = client
        .get(format!("https://localhost:{}/health", local_addr.port()))
        .send()
        .await
        .expect("TLS connection failed");
    assert_eq!(res.status(), 200);
}

#[tokio::test]
async fn test_mtls_accepts_client_cert_and_rejects_without() {
    let pki = make_test_pki();
    let varlink_dir = tempfile::tempdir().unwrap();

    let acceptor = load_tls_acceptor(
        pki.server_cert_path.to_str().unwrap(),
        pki.server_key_path.to_str().unwrap(),
        Some(pki.ca_cert_path.to_str().unwrap()),
    )
    .unwrap();

    let (server, local_addr) =
        run_test_tls_server(varlink_dir.path().to_str().unwrap(), acceptor).await;
    defer! {
        server.abort();
    };

    // Without a client certificate the TLS handshake is rejected
    let ca_cert = reqwest::Certificate::from_pem(&pki.ca_cert_pem).unwrap();
    let client_no_cert = Client::builder()
        .add_root_certificate(ca_cert)
        .resolve("localhost", local_addr)
        .build()
        .unwrap();

    let result = client_no_cert
        .get(format!("https://localhost:{}/health", local_addr.port()))
        .send()
        .await;
    assert!(
        result.is_err(),
        "connection without client cert should fail with mTLS"
    );

    // With a valid client certificate the request succeeds
    let ca_cert = reqwest::Certificate::from_pem(&pki.ca_cert_pem).unwrap();
    let identity =
        reqwest::Identity::from_pkcs8_pem(&pki.client_cert_pem, &pki.client_key_pem).unwrap();
    let client_with_cert = Client::builder()
        .add_root_certificate(ca_cert)
        .identity(identity)
        .resolve("localhost", local_addr)
        .build()
        .unwrap();

    let res = client_with_cert
        .get(format!("https://localhost:{}/health", local_addr.port()))
        .send()
        .await
        .expect("mTLS connection with client cert failed");
    assert_eq!(res.status(), 200);
}

#[tokio::test]
async fn test_tls_credentials_directory_fallback() {
    let pki = make_test_pki();

    // Set up a fake credentials directory with the well-known file names
    let creds_dir = tempfile::tempdir().unwrap();
    std::fs::copy(
        &pki.server_cert_path,
        creds_dir.path().join("tls-cert-file"),
    )
    .unwrap();
    std::fs::copy(
        &pki.server_key_path,
        creds_dir.path().join("tls-private-key-file"),
    )
    .unwrap();

    // No CLI flags — resolve_tls_acceptor should pick up creds from the directory
    let acceptor = resolve_tls_acceptor(None, None, None, Some(creds_dir.path()))
        .expect("credentials directory fallback failed")
        .expect("expected Some(acceptor) from credentials directory");

    let varlink_dir = tempfile::tempdir().unwrap();
    let (server, local_addr) =
        run_test_tls_server(varlink_dir.path().to_str().unwrap(), acceptor).await;
    defer! {
        server.abort();
    };

    let ca_cert = reqwest::Certificate::from_pem(&pki.ca_cert_pem).unwrap();
    let client = Client::builder()
        .add_root_certificate(ca_cert)
        .resolve("localhost", local_addr)
        .build()
        .unwrap();

    let res = client
        .get(format!("https://localhost:{}/health", local_addr.port()))
        .send()
        .await
        .expect("TLS via credentials directory failed");
    assert_eq!(res.status(), 200);
}

#[test_with::path(/usr/bin/varlinkctl)]
#[test_with::path(/run/systemd/io.systemd.Hostname)]
#[tokio::test]
async fn test_varlinkctl_helper_mtls_hostname_describe() {
    let pki = make_test_pki();

    let acceptor = load_tls_acceptor(
        pki.server_cert_path.to_str().unwrap(),
        pki.server_key_path.to_str().unwrap(),
        Some(pki.ca_cert_path.to_str().unwrap()),
    )
    .unwrap();

    let (server, local_addr) = run_test_tls_server("/run/systemd", acceptor).await;
    defer! {
        server.abort();
    };

    let fake_xdg_home = tempfile::tempdir().unwrap();
    let tls_dir = fake_xdg_home.path().join("varlink-http-bridge");
    std::fs::create_dir_all(&tls_dir).unwrap();
    std::fs::copy(&pki.client_cert_path, tls_dir.join("client-cert-file")).unwrap();
    std::fs::copy(&pki.client_key_path, tls_dir.join("client-key-file")).unwrap();
    std::fs::copy(&pki.ca_cert_path, tls_dir.join("server-ca-file")).unwrap();

    let bridge_url = format!(
        "https://localhost:{}/ws/sockets/io.systemd.Hostname",
        local_addr.port()
    );

    let output = tokio::process::Command::new("varlinkctl")
        .args([
            "call",
            "--json=short",
            &format!("exec:{}", helper_binary().display()),
            "io.systemd.Hostname.Describe",
            "{}",
        ])
        .env("VARLINK_BRIDGE_URL", &bridge_url)
        .env("XDG_CONFIG_HOME", fake_xdg_home.path())
        .output()
        .await
        .expect("failed to run varlinkctl");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "varlinkctl failed (stderr: {stderr})"
    );

    let stdout = String::from_utf8(output.stdout).expect("invalid UTF-8 in varlinkctl output");
    let line = stdout.trim().trim_start_matches('\x1e');
    let body: Value =
        serde_json::from_str(line).expect("varlinkctl output not valid JSON: {e}: {line:?}");

    let expected_hostname = gethostname().into_string().expect("failed to get hostname");
    assert_eq!(body["Hostname"], expected_hostname);
}

#[test_with::path(/usr/bin/varlinkctl)]
#[test_with::path(/run/systemd/io.systemd.Hostname)]
#[tokio::test]
async fn test_varlinkctl_helper_mtls_no_client_cert() {
    let pki = make_test_pki();

    let acceptor = load_tls_acceptor(
        pki.server_cert_path.to_str().unwrap(),
        pki.server_key_path.to_str().unwrap(),
        Some(pki.ca_cert_path.to_str().unwrap()),
    )
    .unwrap();

    let (server, local_addr) = run_test_tls_server("/run/systemd", acceptor).await;
    defer! {
        server.abort();
    };

    // Provide the server CA (so the client trusts the server) but NO client cert/key
    let fake_xdg_home = tempfile::tempdir().unwrap();
    let tls_dir = fake_xdg_home.path().join("varlink-http-bridge");
    std::fs::create_dir_all(&tls_dir).unwrap();
    std::fs::copy(&pki.ca_cert_path, tls_dir.join("server-ca-file")).unwrap();

    let bridge_url = format!(
        "https://localhost:{}/ws/sockets/io.systemd.Hostname",
        local_addr.port()
    );

    let output = tokio::process::Command::new("varlinkctl")
        .args([
            "call",
            "--json=short",
            &format!("exec:{}", helper_binary().display()),
            "io.systemd.Hostname.Describe",
            "{}",
        ])
        .env("VARLINK_BRIDGE_URL", &bridge_url)
        .env("XDG_CONFIG_HOME", fake_xdg_home.path())
        .output()
        .await
        .expect("failed to run varlinkctl");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "expected failure without client cert, but helper succeeded"
    );
    assert!(
        stderr.contains("handshake failed: check client cert if server requires mTLS"),
        "expected mTLS hint in error, got: {stderr}"
    );
}

#[test]
fn test_tls_credentials_directory_returns_none_without_creds() {
    let empty_dir = tempfile::tempdir().unwrap();
    let result = resolve_tls_acceptor(None, None, None, Some(empty_dir.path())).unwrap();
    assert!(
        result.is_none(),
        "empty credentials dir should yield no TLS"
    );
}

// --- SSH key auth tests ---

fn make_test_authorized_keys_file(pubkeys: &[&str]) -> tempfile::NamedTempFile {
    use std::io::Write;
    let mut f = tempfile::NamedTempFile::new().unwrap();
    for key in pubkeys {
        writeln!(f, "{key}").unwrap();
    }
    f
}

#[test]
fn test_ssh_auth_parse_authorized_keys_ed25519() {
    use openssl::pkey::PKey;

    let pkey = PKey::generate_ed25519().unwrap();
    let raw_pub = pkey.raw_public_key().unwrap();

    // Build SSH blob: string "ssh-ed25519" + string <32 bytes>
    let mut blob = Vec::new();
    let algo = b"ssh-ed25519";
    blob.extend_from_slice(&(algo.len() as u32).to_be_bytes());
    blob.extend_from_slice(algo);
    blob.extend_from_slice(&(raw_pub.len() as u32).to_be_bytes());
    blob.extend_from_slice(&raw_pub);

    let b64_blob = openssl::base64::encode_block(&blob);
    let line = format!("ssh-ed25519 {b64_blob} testkey@host");
    let ak_file = make_test_authorized_keys_file(&[&line]);
    let auth = SshKeyAuthenticator::new(ak_file.path().to_str().unwrap()).unwrap();

    assert_eq!(auth.key_count(), 1);
}

#[test]
fn test_ssh_auth_parse_authorized_keys_rsa() {
    // Use ssh-keygen to generate an RSA key for the test
    let tmpdir = tempfile::tempdir().unwrap();
    let key_path = tmpdir.path().join("test_rsa");
    let status = std::process::Command::new("ssh-keygen")
        .args(["-t", "rsa", "-b", "2048", "-f"])
        .arg(&key_path)
        .args(["-N", "", "-q"])
        .status()
        .expect("ssh-keygen failed to run");
    assert!(status.success(), "ssh-keygen failed");

    let pub_key_content = std::fs::read_to_string(key_path.with_extension("pub")).unwrap();
    let ak_file = make_test_authorized_keys_file(&[pub_key_content.trim()]);
    let result = SshKeyAuthenticator::new(ak_file.path().to_str().unwrap());
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("RSA is not supported"),
        "RSA-only authorized_keys should fail with clear message"
    );
}

#[test]
fn test_ssh_auth_rejects_garbage() {
    let ak_file = make_test_authorized_keys_file(&["not-a-real-key line", "# comment"]);
    let result = SshKeyAuthenticator::new(ak_file.path().to_str().unwrap());
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("no valid SSH public keys")
    );
}

#[tokio::test]
async fn test_ssh_auth_rejects_expired_timestamp() {
    let tmpdir = tempfile::tempdir().unwrap();
    let key_path = tmpdir.path().join("test_ed25519");
    let status = std::process::Command::new("ssh-keygen")
        .args(["-t", "ed25519", "-f"])
        .arg(&key_path)
        .args(["-N", "", "-q"])
        .status()
        .expect("ssh-keygen failed to run");
    assert!(status.success(), "ssh-keygen failed");

    let pubkey_line = std::fs::read_to_string(key_path.with_extension("pub")).unwrap();
    let ak_file = make_test_authorized_keys_file(&[pubkey_line.trim()]);
    let auth = SshKeyAuthenticator::new(ak_file.path().to_str().unwrap())
        .unwrap()
        .with_max_skew(0);

    let privkey_pem = std::fs::read_to_string(&key_path).unwrap();
    let privkey = ssh_key::PrivateKey::from_openssh(&privkey_pem).unwrap();

    let mut builder = sshauth::TokenSigner::using_private_key(privkey).unwrap();
    builder.include_fingerprint(true).magic_prefix(*b"vhbridge");
    let signer = builder.build().unwrap();

    let nonce = "test-nonce-expired";
    let mut tb = signer.sign_for();
    tb.action("method", "GET")
        .action("path", "/sockets")
        .action("nonce", nonce);
    let token = tb.sign().await.unwrap();

    // Wait for the token to become stale (max_skew is 0)
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let header = format!("Bearer {}", token.encode());
    let result = auth.check_request("GET", "/sockets", &header, Some(nonce));
    assert!(result.is_err(), "expired token should be rejected");
}

#[tokio::test]
async fn test_ssh_auth_rejects_unknown_fingerprint() {
    // Key A: in authorized_keys
    let tmpdir_a = tempfile::tempdir().unwrap();
    let key_path_a = tmpdir_a.path().join("key_a");
    let status = std::process::Command::new("ssh-keygen")
        .args(["-t", "ed25519", "-f"])
        .arg(&key_path_a)
        .args(["-N", "", "-q"])
        .status()
        .unwrap();
    assert!(status.success());

    let pubkey_a_line = std::fs::read_to_string(key_path_a.with_extension("pub")).unwrap();
    let ak_file = make_test_authorized_keys_file(&[pubkey_a_line.trim()]);
    let auth = SshKeyAuthenticator::new(ak_file.path().to_str().unwrap()).unwrap();

    // Key B: NOT in authorized_keys
    let tmpdir_b = tempfile::tempdir().unwrap();
    let key_path_b = tmpdir_b.path().join("key_b");
    let status = std::process::Command::new("ssh-keygen")
        .args(["-t", "ed25519", "-f"])
        .arg(&key_path_b)
        .args(["-N", "", "-q"])
        .status()
        .unwrap();
    assert!(status.success());

    let privkey_b_pem = std::fs::read_to_string(&key_path_b).unwrap();
    let privkey_b = ssh_key::PrivateKey::from_openssh(&privkey_b_pem).unwrap();

    let mut builder = sshauth::TokenSigner::using_private_key(privkey_b).unwrap();
    builder.include_fingerprint(true).magic_prefix(*b"vhbridge");
    let signer = builder.build().unwrap();

    let nonce = "test-nonce-unknown-fp";
    let mut tb = signer.sign_for();
    tb.action("method", "GET")
        .action("path", "/sockets")
        .action("nonce", nonce);
    let token = tb.sign().await.unwrap();

    let header = format!("Bearer {}", token.encode());
    let result = auth.check_request("GET", "/sockets", &header, Some(nonce));
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("unknown key fingerprint"));
}

#[tokio::test]
async fn test_ssh_auth_verify_ed25519() {
    let tmpdir = tempfile::tempdir().unwrap();
    let key_path = tmpdir.path().join("test_ed25519");
    let status = std::process::Command::new("ssh-keygen")
        .args(["-t", "ed25519", "-f"])
        .arg(&key_path)
        .args(["-N", "", "-q"])
        .status()
        .expect("ssh-keygen failed to run");
    assert!(status.success(), "ssh-keygen failed");

    let pubkey_line = std::fs::read_to_string(key_path.with_extension("pub")).unwrap();
    let ak_file = make_test_authorized_keys_file(&[pubkey_line.trim()]);
    let auth = SshKeyAuthenticator::new(ak_file.path().to_str().unwrap()).unwrap();

    let privkey_pem = std::fs::read_to_string(&key_path).unwrap();
    let privkey = ssh_key::PrivateKey::from_openssh(&privkey_pem).unwrap();

    let mut builder = sshauth::TokenSigner::using_private_key(privkey).unwrap();
    builder.include_fingerprint(true).magic_prefix(*b"vhbridge");
    let signer = builder.build().unwrap();

    let nonce = "test-nonce-verify";
    let mut tb = signer.sign_for();
    tb.action("method", "GET")
        .action("path", "/sockets")
        .action("nonce", nonce);
    let token = tb.sign().await.unwrap();

    let header = format!("Bearer {}", token.encode());
    auth.check_request("GET", "/sockets", &header, Some(nonce))
        .expect("valid ed25519 token should pass");
}

fn make_auth_test_router(authenticators: Vec<Box<dyn Authenticator>>) -> Router {
    let tmpdir = tempfile::tempdir().unwrap();
    // Keep tmpdir so it lives for the test duration (but gets cleaned up eventually)
    let path = tmpdir.keep();
    create_router(path.to_str().unwrap(), authenticators).unwrap()
}

#[tokio::test]
async fn test_ssh_auth_rejects_without_header() {
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    let pkey = openssl::pkey::PKey::generate_ed25519().unwrap();
    let raw_pub = pkey.raw_public_key().unwrap();

    let mut blob = Vec::new();
    let algo = b"ssh-ed25519";
    blob.extend_from_slice(&(algo.len() as u32).to_be_bytes());
    blob.extend_from_slice(algo);
    blob.extend_from_slice(&(raw_pub.len() as u32).to_be_bytes());
    blob.extend_from_slice(&raw_pub);

    let b64_blob = openssl::base64::encode_block(&blob);
    let line = format!("ssh-ed25519 {b64_blob} testkey@host");
    let ak_file = make_test_authorized_keys_file(&[&line]);
    let auth = SshKeyAuthenticator::new(ak_file.path().to_str().unwrap()).unwrap();

    let app = make_auth_test_router(vec![Box::new(auth)]);
    let response = app
        .oneshot(Request::get("/sockets").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_ssh_auth_health_always_open() {
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    let pkey = openssl::pkey::PKey::generate_ed25519().unwrap();
    let raw_pub = pkey.raw_public_key().unwrap();

    let mut blob = Vec::new();
    let algo = b"ssh-ed25519";
    blob.extend_from_slice(&(algo.len() as u32).to_be_bytes());
    blob.extend_from_slice(algo);
    blob.extend_from_slice(&(raw_pub.len() as u32).to_be_bytes());
    blob.extend_from_slice(&raw_pub);

    let b64_blob = openssl::base64::encode_block(&blob);
    let line = format!("ssh-ed25519 {b64_blob} testkey@host");
    let ak_file = make_test_authorized_keys_file(&[&line]);
    let auth = SshKeyAuthenticator::new(ak_file.path().to_str().unwrap()).unwrap();

    let app = make_auth_test_router(vec![Box::new(auth)]);
    let response = app
        .oneshot(Request::get("/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_ssh_auth_no_authenticators_allows_all() {
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    let app = make_auth_test_router(Vec::new());
    let response = app
        .oneshot(Request::get("/sockets").body(Body::empty()).unwrap())
        .await
        .unwrap();
    // No authenticators = open access; 200 even though the socket dir is empty
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_ssh_auth_rejects_replayed_nonce() {
    let tmpdir = tempfile::tempdir().unwrap();
    let key_path = tmpdir.path().join("test_ed25519");
    let status = std::process::Command::new("ssh-keygen")
        .args(["-t", "ed25519", "-f"])
        .arg(&key_path)
        .args(["-N", "", "-q"])
        .status()
        .expect("ssh-keygen failed to run");
    assert!(status.success(), "ssh-keygen failed");

    let pubkey_line = std::fs::read_to_string(key_path.with_extension("pub")).unwrap();
    let ak_file = make_test_authorized_keys_file(&[pubkey_line.trim()]);
    let auth = SshKeyAuthenticator::new(ak_file.path().to_str().unwrap()).unwrap();

    let privkey_pem = std::fs::read_to_string(&key_path).unwrap();
    let privkey = ssh_key::PrivateKey::from_openssh(&privkey_pem).unwrap();

    let mut builder = sshauth::TokenSigner::using_private_key(privkey).unwrap();
    builder.include_fingerprint(true).magic_prefix(*b"vhbridge");
    let signer = builder.build().unwrap();

    let nonce = "replay-me";
    let mut tb = signer.sign_for();
    tb.action("method", "GET")
        .action("path", "/sockets")
        .action("nonce", nonce);
    let token = tb.sign().await.unwrap();
    let header = format!("Bearer {}", token.encode());

    // First use should succeed
    auth.check_request("GET", "/sockets", &header, Some(nonce))
        .expect("first use of nonce should pass");

    // Replay with the same nonce should fail
    let result = auth.check_request("GET", "/sockets", &header, Some(nonce));
    assert!(result.is_err(), "replayed nonce should be rejected");
    assert!(
        result.unwrap_err().contains("nonce already used"),
        "error should mention nonce replay"
    );
}

#[tokio::test]
async fn test_ssh_auth_rejects_missing_nonce() {
    let tmpdir = tempfile::tempdir().unwrap();
    let key_path = tmpdir.path().join("test_ed25519");
    let status = std::process::Command::new("ssh-keygen")
        .args(["-t", "ed25519", "-f"])
        .arg(&key_path)
        .args(["-N", "", "-q"])
        .status()
        .expect("ssh-keygen failed to run");
    assert!(status.success(), "ssh-keygen failed");

    let pubkey_line = std::fs::read_to_string(key_path.with_extension("pub")).unwrap();
    let ak_file = make_test_authorized_keys_file(&[pubkey_line.trim()]);
    let auth = SshKeyAuthenticator::new(ak_file.path().to_str().unwrap()).unwrap();

    let privkey_pem = std::fs::read_to_string(&key_path).unwrap();
    let privkey = ssh_key::PrivateKey::from_openssh(&privkey_pem).unwrap();

    let mut builder = sshauth::TokenSigner::using_private_key(privkey).unwrap();
    builder.include_fingerprint(true).magic_prefix(*b"vhbridge");
    let signer = builder.build().unwrap();

    let mut tb = signer.sign_for();
    tb.action("method", "GET").action("path", "/sockets");
    let token = tb.sign().await.unwrap();
    let header = format!("Bearer {}", token.encode());

    // Without a nonce, the request should be rejected
    let result = auth.check_request("GET", "/sockets", &header, None);
    assert!(result.is_err(), "request without nonce should be rejected");
    assert!(result.unwrap_err().contains("missing nonce"));
}
