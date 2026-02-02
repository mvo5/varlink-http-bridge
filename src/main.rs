use anyhow::{Context, bail};
use argh::FromArgs;
use axum::{
    Router,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use listenfd::ListenFd;
use log::{debug, error};
use regex_lite::Regex;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::os::fd::{AsRawFd, OwnedFd};
use std::os::unix::fs::FileTypeExt;
use std::sync::{Arc, LazyLock};
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

async fn get_varlink_connection_with_validate_socket(
    socket: &str,
    state: &AppState,
) -> Result<Arc<varlink::AsyncConnection>, AppError> {
    if !varlink_interface_name_is_valid(socket) {
        return Err(AppError::bad_request(format!(
            "invalid socket name (must be a valid varlink interface name): {socket}"
        )));
    }

    let varlink_socket_path = format!(
        "unix:/proc/self/fd/{sockets_dir_fd}/{socket}",
        sockets_dir_fd = state.varlink_sockets_dirfd.as_raw_fd(),
    );
    debug!("Creating varlink connection for: {varlink_socket_path}");

    let connection = varlink::AsyncConnection::with_address(varlink_socket_path).await?;
    Ok(connection)
}

#[derive(Clone)]
struct AppState {
    varlink_sockets_dirfd: Arc<OwnedFd>,
}

async fn varlink_unix_sockets_in(varlink_sockets_dirfd: &OwnedFd) -> Result<Vec<String>, AppError> {
    let mut socket_names = Vec::new();
    let mut entries = tokio::fs::read_dir(format!(
        "/proc/self/fd/{}",
        varlink_sockets_dirfd.as_raw_fd()
    ))
    .await?;

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
    let all_sockets = varlink_unix_sockets_in(&state.varlink_sockets_dirfd).await?;
    Ok(axum::Json(json!({"sockets": all_sockets})))
}

async fn route_socket_get(
    Path(socket): Path<String>,
    State(state): State<AppState>,
) -> Result<axum::Json<Value>, AppError> {
    debug!("GET socket: {socket}");
    let connection = get_varlink_connection_with_validate_socket(&socket, &state).await?;

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
    let connection = get_varlink_connection_with_validate_socket(&socket, &state).await?;

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

    let connection = get_varlink_connection_with_validate_socket(&socket, &state).await?;

    let mut call = varlink::AsyncMethodCall::<Value, Value, varlink::Error>::new(
        connection, method, call_args,
    );
    // XXX: handle more and protocol switch
    // XXX2: switch to websocket right away(?)
    let reply = call.call().await?;
    // XXX: we need to check for "more" here in the reply and switch protocol

    Ok(axum::Json(reply))
}

fn create_router(varlink_sockets_dir: &str) -> anyhow::Result<Router> {
    let dir_file = std::fs::File::open(varlink_sockets_dir)
        .with_context(|| format!("failed to open {varlink_sockets_dir}"))?;
    if !dir_file.metadata()?.is_dir() {
        bail!("path {varlink_sockets_dir} is not a directory");
    }
    let shared_state = AppState {
        varlink_sockets_dirfd: Arc::new(dir_file.into()),
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
        // the limit is arbitrary - DO WE NEED IT?
        .layer(DefaultBodyLimit::max(4 * 1024 * 1024))
        .with_state(shared_state);

    Ok(app)
}

async fn shutdown_signal() {
    let ctrl_c = signal::ctrl_c();
    let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())
        .expect("failed to install SIGTERM handler");
    tokio::select! {
        _ = ctrl_c => {},
        _ = sigterm.recv() => {},
    }
    println!("Shutdown signal received, stopping server...");
}

async fn run_server(varlink_sockets_dir: &str, listener: TcpListener) -> anyhow::Result<()> {
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

    // run with e.g. "systemd-socket-activate -l 127.0.0.1:8080 -- varlink-http-bridge"
    let mut listenfd = ListenFd::from_env();
    let listener = if let Some(std_listener) = listenfd.take_tcp_listener(0)? {
        // needed or tokio panics, see https://github.com/mitsuhiko/listenfd/pull/23
        std_listener.set_nonblocking(true)?;
        TcpListener::from_std(std_listener)?
    } else {
        TcpListener::bind(&cli.bind).await?
    };
    let local_addr = listener.local_addr()?;

    eprintln!("Varlink proxy started");
    eprintln!(
        "Forwarding HTTP {local_addr} -> Varlink dir: {varlink_sockets_dir}",
        varlink_sockets_dir = &cli.varlink_sockets_dir
    );
    run_server(&cli.varlink_sockets_dir, listener).await
}

#[cfg(test)]
mod tests;
