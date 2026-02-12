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

enum VarlinkSockets {
    SocketDir { dirfd: OwnedFd },
    SingleSocket { dirfd: OwnedFd, name: String },
}

impl VarlinkSockets {
    fn from_socket_dir(dir_path: &str) -> anyhow::Result<Self> {
        let dir_file =
            std::fs::File::open(dir_path).with_context(|| format!("failed to open {dir_path}"))?;
        Ok(VarlinkSockets::SocketDir {
            dirfd: OwnedFd::from(dir_file),
        })
    }

    fn from_socket(socket_path: &str) -> anyhow::Result<Self> {
        let path = std::path::Path::new(socket_path);
        let socket_name = path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| anyhow::anyhow!("cannot extract socket name from {socket_path}"))?;
        let dir_path = path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("cannot extract parent directory from {socket_path}"))?;
        let dir_file = std::fs::File::open(dir_path)
            .with_context(|| format!("failed to open parent directory {}", dir_path.display()))?;

        Ok(VarlinkSockets::SingleSocket {
            dirfd: OwnedFd::from(dir_file),
            name: socket_name.to_string(),
        })
    }

    fn resolve_socket_with_validate(&self, name: &str) -> Result<String, AppError> {
        if !varlink_interface_name_is_valid(name) {
            return Err(AppError::bad_request(format!(
                "invalid socket name (must be a valid varlink interface name): {name}"
            )));
        }

        match self {
            VarlinkSockets::SocketDir { dirfd } => {
                Ok(format!("unix:/proc/self/fd/{}/{name}", dirfd.as_raw_fd()))
            }
            VarlinkSockets::SingleSocket {
                dirfd,
                name: expected,
            } => {
                if name == expected {
                    Ok(format!("unix:/proc/self/fd/{}/{name}", dirfd.as_raw_fd()))
                } else {
                    Err(AppError::bad_gateway(format!(
                        "socket '{name}' not available (only '{expected}' is available)"
                    )))
                }
            }
        }
    }

    async fn list_sockets(&self) -> Result<Vec<String>, AppError> {
        match self {
            VarlinkSockets::SocketDir { dirfd } => {
                let mut socket_names = Vec::new();
                let mut entries =
                    tokio::fs::read_dir(format!("/proc/self/fd/{}", dirfd.as_raw_fd())).await?;

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
            VarlinkSockets::SingleSocket { name, .. } => Ok(vec![name.clone()]),
        }
    }
}

async fn get_varlink_connection_with_validate_socket(
    socket: &str,
    state: &AppState,
) -> Result<Arc<varlink::AsyncConnection>, AppError> {
    let varlink_socket_path = state.varlink_sockets.resolve_socket_with_validate(socket)?;
    debug!("Creating varlink connection for: {varlink_socket_path}");

    let connection = varlink::AsyncConnection::with_address(varlink_socket_path).await?;
    Ok(connection)
}

#[derive(Clone)]
struct AppState {
    varlink_sockets: Arc<VarlinkSockets>,
}

async fn route_sockets_get(State(state): State<AppState>) -> Result<axum::Json<Value>, AppError> {
    debug!("GET sockets");
    let all_sockets = state.varlink_sockets.list_sockets().await?;
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

fn create_router(varlink_sockets_path: &str) -> anyhow::Result<Router> {
    let metadata = std::fs::metadata(varlink_sockets_path)
        .with_context(|| format!("failed to stat {varlink_sockets_path}"))?;

    let shared_state = AppState {
        varlink_sockets: Arc::new(if metadata.is_dir() {
            VarlinkSockets::from_socket_dir(varlink_sockets_path)?
        } else if metadata.file_type().is_socket() {
            VarlinkSockets::from_socket(varlink_sockets_path)?
        } else {
            bail!("path {varlink_sockets_path} is neither a directory nor a socket");
        }),
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

async fn run_server(varlink_sockets_path: &str, listener: TcpListener) -> anyhow::Result<()> {
    let app = create_router(varlink_sockets_path)?;
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

    /// varlink unix socket path to proxy: a directory of sockets/symlinks or a single socket
    #[argh(positional, default = "String::from(\"/run/systemd/registry\")")]
    varlink_sockets_path: String,
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
        "Forwarding HTTP {local_addr} -> Varlink: {varlink_sockets_path}",
        varlink_sockets_path = &cli.varlink_sockets_path
    );
    run_server(&cli.varlink_sockets_path, listener).await
}

#[cfg(test)]
mod tests;
