use super::*;
use futures_util::{SinkExt, StreamExt};
use gethostname::gethostname;
use reqwest::Client;
use scopeguard::defer;
use std::os::fd::OwnedFd;
use tokio::task::JoinSet;
use tokio_tungstenite::tungstenite::Message as WsMsg;

async fn run_test_server(
    varlink_sockets_path: &str,
) -> (tokio::task::JoinHandle<()>, std::net::SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind to random port failed");
    let local_addr = listener
        .local_addr()
        .expect("failed to extract local address");

    let varlink_sockets_path = varlink_sockets_path.to_string();
    let task_handle = tokio::spawn(async move {
        run_server(&varlink_sockets_path, listener)
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
    let res = run_server(&varlink_sockets_dir_or_file, listener).await;

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
