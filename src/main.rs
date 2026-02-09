use anyhow::bail;
use argh::FromArgs;
use axum::{
    Router,
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    extract::{DefaultBodyLimit, Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use log::{debug, error, warn};
use regex_lite::Regex;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::os::unix::fs::FileTypeExt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UnixStream};
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
            // TODO: implement something like NotExists or NotFound in the upstream
            // varlink crate as IO error is extremly generic. Also add details upstream
            // to the error string (like what socket)
            ConnectionClosed | Io { .. } => StatusCode::BAD_GATEWAY,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        Self {
            status,
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

// see https://varlink.org/Interface-Definition (interface_name there)
fn varlink_interface_name_is_valid(name: &str) -> bool {
    static RE: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"^[A-Za-z]([-]*[A-Za-z0-9])*(\.[A-Za-z0-9]([-]*[A-Za-z0-9])*)+$").unwrap()
    });
    RE.is_match(name)
}

async fn get_varlink_connection(
    socket: &str,
    state: &AppState,
) -> Result<Arc<varlink::AsyncConnection>, AppError> {
    if !varlink_interface_name_is_valid(socket) {
        return Err(AppError::bad_request(format!(
            "invalid socket name (must be a valid varlink interface name): {socket}"
        )));
    }

    let varlink_socket_path = format!("unix:{}/{}", state.varlink_sockets_dir, socket);
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

async fn varlink_unix_sockets_in(varlink_sockets_dir: &str) -> Result<Vec<String>, AppError> {
    let mut socket_names = Vec::new();
    let mut entries = tokio::fs::read_dir(varlink_sockets_dir).await?;

    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        // we cannot reuse entry() here, we need fs::metadata() so
        // that it follows symlinks. Skip entries where metadata fails to avoid
        // a single bad entry bringing down the entire service.
        let Ok(metadata) = tokio::fs::metadata(&path).await else {
            continue;
        };
        if metadata.file_type().is_socket()
            && let Some(name) = path.file_name().and_then(|fname| fname.to_str())
            && varlink_interface_name_is_valid(name)
        {
            socket_names.push(name.to_string());
        }
    }
    socket_names.sort();
    Ok(socket_names)
}

async fn route_sockets_get(State(state): State<AppState>) -> Result<axum::Json<Value>, AppError> {
    debug!("GET sockets");
    let all_sockets = varlink_unix_sockets_in(&state.varlink_sockets_dir).await?;
    Ok(axum::Json(json!({"sockets": all_sockets})))
}

async fn route_socket_get(
    Path(socket): Path<String>,
    State(state): State<AppState>,
) -> Result<axum::Json<Value>, AppError> {
    debug!("GET socket: {socket}");
    let connection = get_varlink_connection(&socket, &state).await?;

    let mut call = varlink::AsyncMethodCall::<Value, Value, varlink::Error>::new(
        connection,
        "org.varlink.service.GetInfo",
        Value::Null,
    );
    let reply = call.call().await?;
    Ok(axum::Json(reply))
}

async fn route_socket_interface_get(
    Path((socket, interface)): Path<(String, String)>,
    State(state): State<AppState>,
) -> Result<axum::Json<Value>, AppError> {
    debug!("GET socket: {socket}, interface: {interface}");
    let connection = get_varlink_connection(&socket, &state).await?;

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
    Path(method): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    State(state): State<AppState>,
    axum::Json(call_args): axum::Json<Value>,
) -> Result<axum::Json<Value>, AppError> {
    debug!("POST call for method: {method}, params: {params:#?}");

    let socket = if let Some(socket) = params.get("socket") {
        socket.clone()
    } else {
        method
            .rsplit_once('.')
            .map(|x| x.0)
            .ok_or_else(|| {
                AppError::bad_request(format!(
                    "cannot derive socket from method '{method}': no dots in name"
                ))
            })?
            .to_string()
    };

    let connection = get_varlink_connection(&socket, &state).await?;

    let mut call = varlink::AsyncMethodCall::<Value, Value, varlink::Error>::new(
        connection, method, call_args,
    );
    let reply = call.call().await?;

    Ok(axum::Json(reply))
}

async fn route_ws(
    Path(varlink_socket): Path<String>,
    State(state): State<AppState>,
    ws: WebSocketUpgrade,
) -> Result<Response, AppError> {
    // Validate before upgrade so we can return a proper HTTP 400
    if !varlink_interface_name_is_valid(&varlink_socket) {
        return Err(AppError::bad_request(format!(
            "invalid socket name (must be a valid varlink interface name): {varlink_socket}"
        )));
    }

    let unix_path = format!("{}/{}", state.varlink_sockets_dir, varlink_socket);
    // Connect eagerly so connection failures return proper HTTP errors
    let varlink_socket = UnixStream::connect(&unix_path)
        .await
        .map_err(|e| AppError::bad_gateway(format!("cannot connect to {unix_path}: {e}")))?;

    Ok(ws.on_upgrade(move |ws_socket| handle_ws(ws_socket, varlink_socket)))
}

// Forwards raw bytes between the websocket and the varlink unix
// socket in both directions. Each NUL-delimited varlink message is
// sent as one WS binary frame. Once a protocol upgrade happens this is
// dropped and its just a raw byte stream.
async fn handle_ws(mut ws: WebSocket, unix: UnixStream) {
    let (unix_read, mut unix_write) = tokio::io::split(unix);
    let mut unix_reader = tokio::io::BufReader::new(unix_read);
    let (varlink_msg_tx, mut varlink_msg_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(32);
    // the complexity here is a bit ugly but without it the websocket is very hard
    // to use from tools like "websocat" which will add a \n or \0 after each "message"
    let varlink_connection_upgraded = Arc::new(AtomicBool::new(false));

    // read_until is not cancel-safe, so run it in a separate task and we need read_until
    // to ensure we keep the \0 boundaries and send these via a varlink_msg channel.
    //
    // After a varlink protocol upgrade the connection carries raw bytes without \0
    // delimiters, so the reader switches to plain read() once "upgraded" is set.
    let reader_task = tokio::spawn({
        let varlink_connection_upgraded = varlink_connection_upgraded.clone();
        async move {
            loop {
                let mut buf = Vec::new();
                let res = if varlink_connection_upgraded.load(Ordering::Relaxed) {
                    buf.reserve(8192);
                    unix_reader.read_buf(&mut buf).await
                } else {
                    unix_reader.read_until(0, &mut buf).await
                };
                match res {
                    Err(e) => {
                        warn!("varlink read error: {e}");
                        break;
                    }
                    Ok(0) => {
                        debug!("varlink socket closed (read returned 0)");
                        break;
                    }
                    Ok(_) => {
                        if varlink_msg_tx.send(buf).await.is_err() {
                            warn!("varlink_msg channel closed, ws gone?");
                            break;
                        }
                    }
                }
            }
        }
    });

    loop {
        tokio::select! {
            ws_msg = ws.recv() => {
                let Some(Ok(msg)) = ws_msg else {
                    debug!("ws.recv() returned None or error, client disconnected");
                    break;
                };
                let data = match msg {
                    Message::Binary(bin) => {
                        debug!("ws recv binary: {} bytes", bin.len());
                        bin.to_vec()
                    }
                    Message::Text(text) => {
                        debug!("ws recv text: {} bytes", text.len());
                        text.as_bytes().to_vec()
                    }
                    Message::Close(frame) => {
                        debug!("ws recv close frame: {frame:?}");
                        break;
                    }
                    other => {
                        debug!("ws recv other: {other:?}");
                        continue;
                    }
                };
                // Detect varlink protocol upgrade request
                if !varlink_connection_upgraded.load(Ordering::Relaxed) {
                    let json_bytes = data.strip_suffix(&[0]).unwrap_or(&data);
                    match serde_json::from_slice::<Value>(json_bytes) {
                        Ok(v) => {
                            if v.get("upgrade").and_then(Value::as_bool).unwrap_or(false) {
                                debug!("varlink protocol upgrade detected");
                                varlink_connection_upgraded.store(true, Ordering::Relaxed);
                            }
                        }
                        Err(e) => {
                            warn!("failed to parse ws message as JSON for upgrade detection: {e}");
                        }
                    }
                }
                if let Err(e) = unix_write.write_all(&data).await {
                    warn!("varlink write error: {e}");
                    break;
                }
            }
            Some(data) = varlink_msg_rx.recv() => {
                if let Err(e) = ws.send(Message::Binary(data.into())).await {
                    warn!("ws send error: {e}");
                    break;
                }
            }
            else => {
                debug!("select: all branches closed");
                break;
            }
        }
    }
    debug!("handle_ws loop exited");

    reader_task.abort();
}

fn create_router(varlink_sockets_dir: String) -> anyhow::Result<Router> {
    if !std::path::Path::new(&varlink_sockets_dir).is_dir() {
        bail!("path {varlink_sockets_dir} is not a directory");
    }
    let shared_state = AppState {
        varlink_sockets_dir,
    };

    let app = Router::new()
        .route("/health", get(|| async { StatusCode::OK }))
        .route("/sockets", get(route_sockets_get))
        .route("/sockets/{socket}", get(route_socket_get))
        .route(
            "/sockets/{socket}/{interface}",
            get(route_socket_interface_get),
        )
        .route("/call/{method}", post(route_call_post))
        .route("/ws/sockets/{socket}", get(route_ws))
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
    #[argh(positional, default = "String::from(\"/run/systemd/registry\")")]
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

#[cfg(test)]
mod tests;
