use anyhow::bail;
use argh::FromArgs;
use axum::{
    Router,
    extract::{ConnectInfo, DefaultBodyLimit, Path, Query, Request, State},
    extract::connect_info::Connected,
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
    routing::{get, post},
    serve::IncomingStream,
};
use log::{debug, error};
use openssl::nid::Nid;
use regex_lite::Regex;
use serde_json::{Value, json};
use std::collections::HashMap;
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

// -- Connection info & authentication ---

#[derive(Clone, Debug)]
struct ConnectionInfo {
    remote_addr: std::net::SocketAddr,
    /// DER-encoded peer certificate, None for plain TCP or no client cert
    peer_cert_der: Option<Vec<u8>>,
}

impl Connected<IncomingStream<'_, TlsListener>> for ConnectionInfo {
    fn connect_info(stream: IncomingStream<'_, TlsListener>) -> Self {
        let peer_cert_der = stream
            .io()
            .ssl()
            .peer_certificate()
            .and_then(|cert| cert.to_der().ok());
        ConnectionInfo {
            remote_addr: *stream.remote_addr(),
            peer_cert_der,
        }
    }
}

impl Connected<IncomingStream<'_, TcpListener>> for ConnectionInfo {
    fn connect_info(stream: IncomingStream<'_, TcpListener>) -> Self {
        ConnectionInfo {
            remote_addr: *stream.remote_addr(),
            peer_cert_der: None,
        }
    }
}

#[derive(Clone, Debug)]
struct Identity {
    subject_cn: String,
    #[allow(dead_code)]
    cert_der: Vec<u8>,
}

trait Authenticator: Send + Sync + 'static {
    /// Ok(Some(id)) = authenticated, Ok(None) = not applicable, Err = reject
    fn authenticate(
        &self,
        conn_info: &ConnectionInfo,
        headers: &axum::http::HeaderMap,
    ) -> Result<Option<Identity>, Response>;
}

/// Authenticates clients by extracting the CN from a peer certificate.
///
/// This authenticator does NOT verify the certificate signature — it relies on
/// OpenSSL having already validated the cert against the configured CA during the
/// TLS handshake (see `load_tls_acceptor`). It must only be used behind a TLS
/// listener with `set_ca_file` / `SslVerifyMode::PEER` configured.
struct ClientCertAuthenticator;

impl Authenticator for ClientCertAuthenticator {
    fn authenticate(
        &self,
        conn_info: &ConnectionInfo,
        _headers: &axum::http::HeaderMap,
    ) -> Result<Option<Identity>, Response> {
        let Some(cert_der) = &conn_info.peer_cert_der else {
            return Ok(None);
        };
        let cert = openssl::x509::X509::from_der(cert_der).map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                format!("invalid client certificate: {e}"),
            )
                .into_response()
        })?;
        let cn = cert
            .subject_name()
            .entries_by_nid(Nid::COMMONNAME)
            .next()
            .and_then(|entry| entry.data().as_utf8().ok())
            .map(|s| s.to_string())
            .unwrap_or_default();
        Ok(Some(Identity {
            subject_cn: cn,
            cert_der: cert_der.clone(),
        }))
    }
}

async fn auth_middleware(
    ConnectInfo(conn_info): ConnectInfo<ConnectionInfo>,
    State(state): State<AppState>,
    mut request: Request,
    next: Next,
) -> Result<Response, Response> {
    if state.authenticators.is_empty() {
        return Ok(next.run(request).await);
    }
    for auth in state.authenticators.iter() {
        match auth.authenticate(&conn_info, request.headers()) {
            Ok(Some(identity)) => {
                request.extensions_mut().insert(identity);
                return Ok(next.run(request).await);
            }
            Ok(None) => continue,
            Err(resp) => return Err(resp),
        }
    }
    Err((StatusCode::UNAUTHORIZED, "authentication required").into_response())
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
    authenticators: Arc<Vec<Box<dyn Authenticator>>>,
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
    // XXX: handle more and protocol switch
    // XXX2: switch to websocket right away(?)
    let reply = call.call().await?;
    // XXX: we need to check for "more" here in the reply and switch protocol

    Ok(axum::Json(reply))
}

struct TlsListener {
    inner: TcpListener,
    acceptor: openssl::ssl::SslAcceptor,
}

impl axum::serve::Listener for TlsListener {
    type Io = tokio_openssl::SslStream<tokio::net::TcpStream>;
    type Addr = std::net::SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            let (stream, addr) = match self.inner.accept().await {
                Ok(conn) => conn,
                Err(e) => {
                    debug!("TCP accept failed: {e}");
                    continue;
                }
            };
            let ssl = match openssl::ssl::Ssl::new(self.acceptor.context()) {
                Ok(ssl) => ssl,
                Err(e) => {
                    debug!("SSL context error: {e}");
                    continue;
                }
            };
            let mut tls_stream = match tokio_openssl::SslStream::new(ssl, stream) {
                Ok(s) => s,
                Err(e) => {
                    debug!("SSL stream creation failed: {e}");
                    continue;
                }
            };
            match std::pin::Pin::new(&mut tls_stream).accept().await {
                Ok(()) => return (tls_stream, addr),
                Err(e) => {
                    debug!("TLS handshake failed: {e}");
                }
            }
        }
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        self.inner.local_addr()
    }
}

fn load_tls_acceptor(
    cert_path: &str,
    key_path: &str,
    client_ca_path: Option<&str>,
) -> anyhow::Result<openssl::ssl::SslAcceptor> {
    use openssl::ssl::{SslAcceptor, SslFiletype, SslMethod, SslVerifyMode};

    let mut builder = SslAcceptor::mozilla_modern_v5(SslMethod::tls_server())?;
    builder.set_certificate_chain_file(cert_path)?;
    builder.set_private_key_file(key_path, SslFiletype::PEM)?;
    builder.check_private_key()?;

    if let Some(ca_path) = client_ca_path {
        builder.set_ca_file(ca_path)?;
        builder.set_verify(SslVerifyMode::PEER);
    }

    Ok(builder.build())
}

fn create_router(
    varlink_sockets_dir: String,
    authenticators: Vec<Box<dyn Authenticator>>,
) -> anyhow::Result<Router> {
    if !std::path::Path::new(&varlink_sockets_dir).is_dir() {
        bail!("path {varlink_sockets_dir} is not a directory");
    }
    let shared_state = AppState {
        varlink_sockets_dir,
        authenticators: Arc::new(authenticators),
    };

    let api_routes = Router::new()
        .route("/sockets", get(route_sockets_get))
        .route("/sockets/{socket}", get(route_socket_get))
        .route(
            "/sockets/{socket}/{interface}",
            get(route_socket_interface_get),
        )
        .route("/call/{method}", post(route_call_post))
        .route_layer(axum::middleware::from_fn_with_state(
            shared_state.clone(),
            auth_middleware,
        ));

    let app = Router::new()
        .route("/health", get(|| async { StatusCode::OK }))
        .merge(api_routes)
        // the limit is arbitrary - DO WE NEED IT?
        .layer(DefaultBodyLimit::max(4 * 1024 * 1024))
        .with_state(shared_state);

    Ok(app)
}

async fn shutdown_signal() {
    signal::ctrl_c().await.ok();
    println!("Shutdown signal received, stopping server...");
}

async fn run_server(
    varlink_sockets_dir: String,
    listener: TcpListener,
    tls_acceptor: Option<openssl::ssl::SslAcceptor>,
    authenticators: Vec<Box<dyn Authenticator>>,
) -> anyhow::Result<()> {
    let app = create_router(varlink_sockets_dir, authenticators)?;

    if let Some(acceptor) = tls_acceptor {
        let tls_listener = TlsListener {
            inner: listener,
            acceptor,
        };
        axum::serve(
            tls_listener,
            app.into_make_service_with_connect_info::<ConnectionInfo>(),
        )
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    } else {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<ConnectionInfo>(),
        )
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    }

    Ok(())
}

/// A proxy for Varlink sockets.
#[derive(FromArgs, Debug)]
struct Cli {
    /// address to bind HTTP server to (default: 127.0.0.1:8080)
    // XXX: use 0.0.0.0:8080 once we have a security story
    #[argh(option, default = "String::from(\"127.0.0.1:8080\")")]
    bind: String,

    /// path to TLS certificate PEM file
    #[argh(option)]
    tls_cert: Option<String>,

    /// path to TLS private key PEM file
    #[argh(option)]
    tls_key: Option<String>,

    /// path to CA certificate PEM file for client certificate verification (mTLS)
    #[argh(option)]
    tls_client_ca: Option<String>,

    /// allow running without any authentication (DANGEROUS)
    #[argh(switch)]
    insecure: bool,

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

    let tls_acceptor = match (cli.tls_cert.as_deref(), cli.tls_key.as_deref()) {
        (Some(cert), Some(key)) => {
            Some(load_tls_acceptor(cert, key, cli.tls_client_ca.as_deref())?)
        }
        (None, None) => {
            if cli.tls_client_ca.is_some() {
                bail!("--tls-client-ca requires --tls-cert and --tls-key");
            }
            None
        }
        _ => bail!("--tls-cert and --tls-key must be specified together"),
    };

    let mut authenticators: Vec<Box<dyn Authenticator>> = Vec::new();
    if cli.tls_client_ca.is_some() {
        authenticators.push(Box::new(ClientCertAuthenticator));
    }

    if authenticators.is_empty() && !cli.insecure {
        bail!(
            "no authenticator configured; use --tls-client-ca for mTLS \
             or --insecure to allow unauthenticated access (DANGEROUS)"
        );
    }
    if authenticators.is_empty() {
        eprintln!("WARNING: running without authentication (--insecure)");
    }

    let listener = TcpListener::bind(&cli.bind).await?;
    let local_addr = listener.local_addr()?;
    let scheme = if tls_acceptor.is_some() {
        "HTTPS"
    } else {
        "HTTP"
    };

    println!("🚀 Varlink proxy started");
    println!(
        "🔗 Forwarding {scheme} {local_addr} -> Varlink dir: {}",
        &cli.varlink_sockets_dir
    );
    run_server(
        cli.varlink_sockets_dir,
        listener,
        tls_acceptor,
        authenticators,
    )
    .await
}

#[cfg(test)]
mod tests;
