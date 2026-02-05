use anyhow::bail;
use argh::FromArgs;
use axum::{
    Router,
    extract::{
        DefaultBodyLimit, Path, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use log::{debug, error};
use serde_json::{Value, json};
use std::os::unix::fs::FileTypeExt;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::signal;
use varlink_parser::IDL;

#[derive(Debug)]
struct AppError {
    status: StatusCode,
    message: String,
}

impl AppError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn bad_gateway(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_GATEWAY,
            message: message.into(),
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        error!("{}", self.message);
        let body = axum::Json(json!({ "error": self.message }));
        (self.status, body).into_response()
    }
}

impl From<varlink::Error> for AppError {
    fn from(e: varlink::Error) -> Self {
        use varlink::error::ErrorKind::{
            ConnectionClosed, InvalidParameter, Io, MethodNotFound, MethodNotImplemented,
        };
        let status = match e.kind() {
            InvalidParameter { .. } => StatusCode::BAD_REQUEST,
            MethodNotFound { .. } => StatusCode::NOT_FOUND,
            MethodNotImplemented { .. } => StatusCode::NOT_IMPLEMENTED,
            ConnectionClosed | Io { .. } => StatusCode::BAD_GATEWAY,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        Self {
            status,
            message: e.to_string(),
        }
    }
}

impl From<serde_json::Error> for AppError {
    fn from(e: serde_json::Error) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: e.to_string(),
        }
    }
}

impl From<std::io::Error> for AppError {
    fn from(e: std::io::Error) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: e.to_string(),
        }
    }
}

fn validate_address(address: &str) -> Result<(), AppError> {
    let is_clean_path = std::path::Path::new(&address)
        .file_name()
        .and_then(|os_str| os_str.to_str())
        .is_some_and(|filename| filename == address);
    let is_valid_chars = address
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_');
    // varlink sockets use reverse-domain naming, never start with a dot
    let no_dot_prefix = !address.starts_with('.');

    if is_clean_path && is_valid_chars && no_dot_prefix {
        Ok(())
    } else {
        Err(AppError::bad_request(format!(
            "invalid socket address: {address}"
        )))
    }
}

async fn get_varlink_connection(
    address: &str,
    state: &AppState,
) -> Result<Arc<varlink::AsyncConnection>, AppError> {
    validate_address(address)?;

    let varlink_socket_path = format!("unix:{}/{}", state.varlink_sockets_dir, address);
    debug!("Creating varlink connection for: {varlink_socket_path}");

    let connection = varlink::AsyncConnection::with_address(varlink_socket_path).await?;
    Ok(connection)
}

#[derive(Clone)]
struct AppState {
    // this is cloned for each request so we could use Arc<str> here but its a tiny str
    // so the extra clone is fine
    varlink_sockets_dir: String,
}

async fn unix_sockets_in(varlink_sockets_dir: &str) -> Result<Vec<String>, AppError> {
    let mut socket_names = Vec::new();
    let mut entries = tokio::fs::read_dir(varlink_sockets_dir).await?;

    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        // we cannot reuse entry() here, we need fs::metadata() so
        // that it follows symlinks
        if tokio::fs::metadata(&path).await?.file_type().is_socket()
            && let Some(name) = path.file_name().and_then(|n| n.to_str())
        {
            // XXX: this is very crude, varlink sockets are reverse domain so we expect
            // at least a single ".". Once there is xattr for S_IFSOCK we could use this.
            if name.contains('.') {
                socket_names.push(name.to_string());
            }
        }
    }
    Ok(socket_names)
}

async fn route_info_get(State(state): State<AppState>) -> Result<axum::Json<Value>, AppError> {
    debug!("GET info");
    let all_sockets = unix_sockets_in(&state.varlink_sockets_dir).await?;
    Ok(axum::Json(json!({"sockets": all_sockets})))
}

async fn route_info_address_get(
    Path(address): Path<String>,
    State(state): State<AppState>,
) -> Result<axum::Json<Value>, AppError> {
    debug!("GET info for address: {address}");
    let connection = get_varlink_connection(&address, &state).await?;

    let mut call = varlink::AsyncMethodCall::<Value, Value, varlink::Error>::new(
        connection,
        "org.varlink.service.GetInfo",
        Value::Null,
    );
    let reply = call.call().await?;
    Ok(axum::Json(reply))
}

async fn route_info_address_interface_get(
    Path((address, interface)): Path<(String, String)>,
    State(state): State<AppState>,
) -> Result<axum::Json<Value>, AppError> {
    debug!("GET info for address: {address}, interface: {interface}");
    let connection = get_varlink_connection(&address, &state).await?;

    let mut call = varlink::AsyncMethodCall::<Value, Value, varlink::Error>::new(
        connection,
        "org.varlink.service.GetInterfaceDescription",
        json!({"interface": interface}),
    );
    let reply = call.call().await?;

    let description = reply
        .get("description")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AppError::bad_gateway("upstream response missing 'description' field"))?;

    let iface = IDL::try_from(description)
        .map_err(|e| AppError::bad_gateway(format!("upstream IDL parse error: {e}")))?;

    Ok(axum::Json(json!({"method_names": iface.method_keys})))
}

async fn route_call_post(
    Path((address, method)): Path<(String, String)>,
    State(state): State<AppState>,
    axum::Json(call_args): axum::Json<Value>,
) -> Result<axum::Json<Value>, AppError> {
    debug!("POST call for address: {address}, method: {method}");
    let connection = get_varlink_connection(&address, &state).await?;

    let mut call = varlink::AsyncMethodCall::<Value, Value, varlink::Error>::new(
        connection, method, call_args,
    );
    let reply = call.call().await?;

    Ok(axum::Json(reply))
}

/// WebSocket endpoint for streaming varlink calls.
/// Client sends: {"method": "Interface.Method", "params": {...}, "more": true}
/// Server responds with: {"response": {...}} for each reply, or {"error": "..."}
async fn route_ws(
    Path(address): Path<String>,
    State(state): State<AppState>,
    ws: WebSocketUpgrade,
) -> Result<Response, AppError> {
    validate_address(&address)?;
    let varlink_sockets_dir = state.varlink_sockets_dir.clone();

    Ok(ws.on_upgrade(move |socket| handle_ws(socket, address, varlink_sockets_dir)))
}

fn ws_msg(v: Value) -> Message {
    Message::Text(v.to_string().into())
}

async fn ws_varlink_stream(
    socket: &mut WebSocket,
    mut call: varlink::AsyncMethodCall<Value, Value, varlink::Error>,
) {
    loop {
        match call.recv().await {
            Ok(reply) => {
                let _ = socket.send(ws_msg(json!({"response": reply}))).await;
            }
            Err(e) => {
                use varlink::error::ErrorKind::ConnectionClosed;
                if !matches!(e.kind(), ConnectionClosed) {
                    let _ = socket.send(ws_msg(json!({"error": e.to_string()}))).await;
                }
                break;
            }
        }
    }
}

async fn handle_ws(mut socket: WebSocket, address: String, varlink_sockets_dir: String) {
    let varlink_socket_path = format!("unix:{}/{}", varlink_sockets_dir, address);

    while let Some(Ok(msg)) = socket.recv().await {
        let Message::Text(text) = msg else {
            continue;
        };

        let request: Value = match serde_json::from_str(&text) {
            Ok(v) => v,
            Err(e) => {
                let _ = socket.send(ws_msg(json!({"error": e.to_string()}))).await;
                continue;
            }
        };

        let method = match request.get("method").and_then(|v| v.as_str()) {
            Some(m) => m.to_string(),
            None => {
                let _ = socket.send(ws_msg(json!({"error": "missing 'method' field"}))).await;
                continue;
            }
        };

        let params = request.get("params").cloned().unwrap_or(Value::Object(Default::default()));
        let wants_more = request.get("more").and_then(|v| v.as_bool()).unwrap_or(false);

        let connection = match varlink::AsyncConnection::with_address(&varlink_socket_path).await {
            Ok(c) => c,
            Err(e) => {
                let _ = socket.send(ws_msg(json!({"error": e.to_string()}))).await;
                continue;
            }
        };

        let mut call = varlink::AsyncMethodCall::<Value, Value, varlink::Error>::new(
            connection, method, params,
        );

        if wants_more {
            if let Err(e) = call.more().await {
                let _ = socket.send(ws_msg(json!({"error": e.to_string()}))).await;
                continue;
            }
            ws_varlink_stream(&mut socket, call).await;
        } else {
            match call.call().await {
                Ok(reply) => { socket.send(ws_msg(json!({"response": reply}))).await.ok(); }
                Err(e) => { socket.send(ws_msg(json!({"error": e.to_string()}))).await.ok(); }
            }
        }
    }
}

fn create_router(varlink_sockets_dir: String) -> anyhow::Result<Router> {
    if !std::path::Path::new(&varlink_sockets_dir).is_dir() {
        bail!("path {varlink_sockets_dir} is not a directory");
    }
    let shared_state = AppState {
        varlink_sockets_dir,
    };

    // the /info endpoint is just "sugar", should we YAGNI it?
    let app = Router::new()
        .route("/health", get(|| async { StatusCode::OK }))
        .route("/info", get(route_info_get))
        .route("/info/{address}", get(route_info_address_get))
        .route(
            "/info/{address}/{interface}",
            get(route_info_address_interface_get),
        )
        .route("/call/{address}/{method}", post(route_call_post))
        .route("/ws/{address}", get(route_ws))
        // the limit is arbitrary - DO WE NEED IT?
        .layer(DefaultBodyLimit::max(4 * 1024 * 1024))
        .with_state(shared_state);

    Ok(app)
}

async fn shutdown_signal() {
    signal::ctrl_c().await.ok();
    println!("Shutdown signal received, stopping server...");
}

async fn run_server(varlink_sockets_dir: String, listener: TcpListener) -> anyhow::Result<()> {
    let app = create_router(varlink_sockets_dir)?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}

/// A proxy for Varlink sockets.
#[derive(FromArgs, Debug)]
struct Cli {
    /// address to bind HTTP server to (default: 127.0.0.1:8080)
    // XXX: use 0.0.0.0:8080 once we have a security story
    #[argh(option, default = "String::from(\"127.0.0.1:8080\")")]
    bind: String,

    /// varlink unix socket dir to proxy, contains the sockets or symlinks to sockets
    #[argh(positional)]
    varlink_sockets_dir: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // not using "tracing" crate here because its quite big (>1.2mb to the production build)
    env_logger::init();

    // not using "clap" crate as it adds 600kb even with minimal settings
    let cli: Cli = argh::from_env();

    let listener = TcpListener::bind(&cli.bind).await?;
    let local_addr = listener.local_addr()?;

    println!("🚀 Varlink proxy started");
    println!(
        "🔗 Forwarding HTTP {} -> Varlink dir: {}",
        local_addr, &cli.varlink_sockets_dir
    );
    run_server(cli.varlink_sockets_dir, listener).await
}

#[test_with::path(/run/systemd/io.systemd.Hostname)]
#[cfg(test)]
mod tests {
    use super::*;
    use gethostname::gethostname;
    use reqwest::Client;
    use scopeguard::defer;
    use tokio::task::JoinSet;

    async fn run_test_server() -> (tokio::task::JoinHandle<()>, std::net::SocketAddr) {
        let varlink_sockets_dir = "/run/systemd".to_string();

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind to random port failed");
        let local_addr = listener
            .local_addr()
            .expect("failed to extract local address");

        let task_handle = tokio::spawn(async {
            run_server(varlink_sockets_dir, listener)
                .await
                .expect("server failed")
        });

        (task_handle, local_addr)
    }

    #[tokio::test]
    async fn test_integration_real_systemd_hostname_post() {
        let (server, local_addr) = run_test_server().await;
        defer! {
            server.abort();
        };

        let client = Client::new();
        let res = client
            .post(format!(
                "http://{}/call/io.systemd.Hostname/org.varlink.service.GetInfo",
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

    #[tokio::test]
    async fn test_integration_real_systemd_info_address_get() {
        let (server, local_addr) = run_test_server().await;
        defer! {
            server.abort();
        };

        let client = Client::new();
        let res = client
            .get(format!("http://{}/info/io.systemd.Hostname", local_addr,))
            .json(&json!({}))
            .send()
            .await
            .expect("failed to post to test server");
        assert_eq!(res.status(), 200);
        let body: Value = res.json().await.expect("varlink body invalid");
        assert_eq!(body["product"], "systemd (systemd-hostnamed)");
    }

    #[tokio::test]
    async fn test_integration_real_systemd_info_get() {
        let (server, local_addr) = run_test_server().await;
        defer! {
            server.abort();
        };

        let client = Client::new();
        let res = client
            .get(format!("http://{}/info", local_addr,))
            .json(&json!({}))
            .send()
            .await
            .expect("failed to post to test server");
        assert_eq!(res.status(), 200);
        let body: Value = res.json().await.expect("varlink body invalid");
        assert!(
            body["sockets"]
                .as_array()
                .expect("sockets not an array")
                .contains(&json!("io.systemd.Hostname"))
        );
    }

    #[tokio::test]
    async fn test_integration_real_systemd_info_interface_get() {
        let (server, local_addr) = run_test_server().await;
        defer! {
            server.abort();
        };

        let client = Client::new();
        let res = client
            .get(format!(
                "http://{}/info/io.systemd.Hostname/io.systemd.Hostname",
                local_addr,
            ))
            .json(&json!({}))
            .send()
            .await
            .expect("failed to post to test server");
        assert_eq!(res.status(), 200);
        let body: Value = res.json().await.expect("varlink body invalid");
        assert_eq!(body.get("method_names").unwrap(), &json!(["Describe"]));
    }

    #[tokio::test]
    async fn test_integration_real_systemd_hostname_parallel() {
        let (server, local_addr) = run_test_server().await;
        defer! {
            server.abort();
        };

        let url = format!(
            "http://{}/call/io.systemd.Hostname/io.systemd.Hostname.Describe",
            local_addr
        );

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
            let hostname = res.expect("client task to collect results paniced");
            assert_eq!(expected_hostname, hostname);
            count += 1;
        }
        assert_eq!(count, NUM_TASKS);
    }

    #[tokio::test]
    async fn test_error_bad_request_on_malformed_json() {
        let (server, local_addr) = run_test_server().await;
        defer! {
            server.abort();
        };
        let client = Client::new();

        let res = client
            .post(format!(
                "http://{}/call/io.systemd.Hostname/org.varlink.service.GetInfo",
                local_addr,
            ))
            .body("this is NOT valid json")
            .header("Content-Type", "application/json")
            .send()
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_error_unknown_varlink_address() {
        let (server, local_addr) = run_test_server().await;
        defer! {
            server.abort();
        };
        let client = Client::new();

        let res = client
            .post(format!(
                "http://{}/call/no.such.address/org.varlink.service.GetInfo",
                local_addr,
            ))
            .body("{}")
            .header("Content-Type", "application/json")
            .send()
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::BAD_GATEWAY);
    }

    #[tokio::test]
    async fn test_error_404_for_missing_method() {
        let (server, local_addr) = run_test_server().await;
        defer! {
            server.abort();
        };
        let client = Client::new();

        let res = client
            .post(format!(
                "http://{}/call/io.systemd.Hostname/com.missing.Call",
                local_addr
            ))
            .json(&json!({}))
            .send()
            .await
            .expect("failed to post to test server");

        assert_eq!(res.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_error_bad_request_for_unclean_address() {
        let (server, local_addr) = run_test_server().await;
        defer! {
            server.abort();
        };
        let client = Client::new();

        let res = client
            .post(format!(
                // %2f is url encoding for "/" so this is ../io.systemd.Hostname
                "http://{}/call/..%2fio.systemd.Hostname/com.missing.Call",
                local_addr
            ))
            .json(&json!({}))
            .send()
            .await
            .expect("failed to post to test server");

        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_error_bad_request_for_invalid_chars_in_address() {
        let (server, local_addr) = run_test_server().await;
        defer! {
            server.abort();
        };
        let client = Client::new();

        let res = client
            .post(format!(
                // %0A is \n
                "http://{}/call/io.systemd.Hostname%0Abad-msg/com.missing.Call",
                local_addr
            ))
            .json(&json!({}))
            .send()
            .await
            .expect("failed to post to test server");

        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_health_endpoint() {
        let (server, local_addr) = run_test_server().await;
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
    async fn test_varlink_sockets_dir_missing() {
        let varlink_sockets_dir = "/does-not-exist".to_string();

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind to random port failed");
        let res = run_server(varlink_sockets_dir, listener).await;

        assert!(res.is_err());
        assert_eq!(
            res.unwrap_err().to_string(),
            "path /does-not-exist is not a directory"
        );
    }

    #[tokio::test]
    async fn test_unix_sockets_in_follows_symlinks() {
        let tmpdir = tempfile::tempdir().expect("failed to create tempdir");
        let symlink_path = tmpdir.path().join("io.systemd.Hostname");

        std::os::unix::fs::symlink("/run/systemd/io.systemd.Hostname", &symlink_path)
            .expect("failed to create symlink");

        let sockets = unix_sockets_in(tmpdir.path().to_str().unwrap())
            .await
            .expect("unix_sockets_in failed");

        assert!(
            sockets.contains(&"io.systemd.Hostname".to_string()),
            "symlinked socket not found, got: {:?}",
            sockets
        );
    }

    #[tokio::test]
    async fn test_websocket_single_call() {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::{connect_async, tungstenite::Message as WsMessage};

        let (server, local_addr) = run_test_server().await;
        defer! {
            server.abort();
        };

        let url = format!("ws://{}/ws/io.systemd.Hostname", local_addr);
        let (mut ws, _) = connect_async(&url).await.expect("failed to connect");

        // Send a single call request
        let request = json!({
            "method": "org.varlink.service.GetInfo",
            "params": {}
        });
        ws.send(WsMessage::Text(request.to_string().into()))
            .await
            .expect("failed to send");

        // Receive response
        let msg = ws.next().await.expect("no response").expect("ws error");
        let WsMessage::Text(text) = msg else {
            panic!("expected text message");
        };

        let response: Value = serde_json::from_str(&text).expect("invalid json");
        assert_eq!(response["response"]["product"], "systemd (systemd-hostnamed)");
    }
}
