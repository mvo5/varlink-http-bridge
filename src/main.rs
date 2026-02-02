use anyhow::bail;
use argh::FromArgs;
use axum::{
    Json, Router,
    body::Bytes,
    extract::Path,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    routing::post,
};
use log::debug;
use serde_json::{Value, json};
use std::os::unix::fs::FileTypeExt;
use std::sync::Arc;
use tokio::net::TcpListener;
use varlink_parser::IDL;

pub enum AppError {
    Anyhow(anyhow::Error),
    Varlink(varlink::Error),
    SerdeJson(StatusCode, serde_json::Error),

    // errors with specific status code
    WithStatus(StatusCode, anyhow::Error),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            AppError::Anyhow(inner) => (StatusCode::INTERNAL_SERVER_ERROR, inner.to_string()),
            AppError::Varlink(inner) => {
                let error_message = inner.to_string();
                (map_varlink_error_to_http_status(inner), error_message)
            }
            AppError::SerdeJson(code, inner) => (code, inner.to_string()),
            AppError::WithStatus(code, inner) => (code, inner.to_string()),
        };

        let body = Json(json!({
            "error": error_message,
        }));

        (status, body).into_response()
    }
}

impl From<anyhow::Error> for AppError {
    fn from(inner: anyhow::Error) -> Self {
        AppError::Anyhow(inner)
    }
}

impl From<varlink::Error> for AppError {
    fn from(inner: varlink::Error) -> Self {
        AppError::Varlink(inner)
    }
}

impl From<serde_json::Error> for AppError {
    fn from(inner: serde_json::Error) -> Self {
        AppError::SerdeJson(StatusCode::BAD_REQUEST, inner)
    }
}

fn map_varlink_error_to_http_status(e: varlink::Error) -> StatusCode {
    use varlink::error::ErrorKind::*;
    match e.kind() {
        InvalidParameter { .. } => StatusCode::BAD_REQUEST,
        MethodNotFound { .. } => StatusCode::NOT_FOUND,
        MethodNotImplemented { .. } => StatusCode::NOT_IMPLEMENTED,
        ConnectionClosed { .. } => StatusCode::BAD_GATEWAY,
        // XXX: slightly debatble how this should be mapped, we get this e.g. when no socket is available
        Io { .. } => StatusCode::BAD_GATEWAY,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

fn validate_address(address: &str) -> anyhow::Result<()> {
    let path = std::path::Path::new(address);

    if path.components().count() != 1 {
        bail!("Address must be a single filename, no paths allowed");
    }
    let is_valid_chars = address
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_');

    if is_valid_chars {
        Ok(())
    } else {
        bail!("unclean socket address attempted: {}", address)
    }
}

async fn get_varlink_connection(
    address: &str,
    state: &AppState,
) -> Result<Arc<varlink::AsyncConnection>, AppError> {
    validate_address(address).map_err(|e| AppError::WithStatus(StatusCode::BAD_REQUEST, e))?;

    let varlink_socket_path = format!("unix:{}/{}", state.varlink_sockets_dir, address);
    debug!(
        "Creating varlink connection for socket path: {}",
        varlink_socket_path,
    );
    let connection = varlink::AsyncConnection::with_address(varlink_socket_path).await?;

    Ok(connection)
}

#[derive(Clone)]
struct AppState {
    varlink_sockets_dir: Arc<String>,
}

async fn unix_sockets_in(varlink_sockets_dir: &str) -> anyhow::Result<Vec<String>> {
    let mut socket_names = Vec::new();

    let mut entries = tokio::fs::read_dir(varlink_sockets_dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        // metadata() will follow symlinks
        let metadata = entry.metadata().await?;
        if metadata.file_type().is_socket() {
            if let Some(name) = entry.file_name().to_str() {
                // XXX: this is very crude, varlink sockets are reverse domain so we expect
                // at least a single ".". Once there is xattr for S_IFSOCK we could use this.
                if name.contains(".") {
                    socket_names.push(name.to_string());
                }
            }
        }
    }

    Ok(socket_names)
}

async fn route_info_get(State(state): State<AppState>) -> Result<Json<Value>, AppError> {
    debug!("GET info");

    let all_sockets = unix_sockets_in(&state.varlink_sockets_dir).await?;
    let reply = json!({"sockets": all_sockets});

    Ok(axum::Json(reply))
}

async fn route_info_address_get(
    Path(address): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<Value>, AppError> {
    debug!("GET info for address: {}", address);
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
    Path(path): Path<(String, String)>,
    State(state): State<AppState>,
) -> Result<Json<Value>, AppError> {
    let (address, interface) = path;
    debug!(
        "GET info for address: {}, interface: {}",
        address, interface
    );
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
        .ok_or_else(|| {
            return AppError::Anyhow(anyhow::anyhow!("failed to get description"));
        })?;

    let iface = IDL::try_from(description)
        .map_err(|e| AppError::Anyhow(anyhow::anyhow!("idl error: {}", e)))?;

    let reply = json!({"method_names": iface.method_keys});

    Ok(axum::Json(reply))
}

async fn route_call_post(
    Path(path): Path<(String, String)>,
    State(state): State<AppState>,
    body: Bytes,
) -> Result<Json<Value>, AppError> {
    let (address, method) = path;
    debug!("POST call for address: {}, method: {}", address, method);
    let connection = get_varlink_connection(&address, &state).await?;

    let call_args = serde_json::from_slice::<Value>(&body)?;

    let mut call = varlink::AsyncMethodCall::<Value, Value, varlink::Error>::new(
        connection, method, call_args,
    );
    // XXX: handle more and protocol switch
    // XXX2: switch to websocket right away(?)
    let reply = call.call().await?;
    // XXX: we need to check for "more" here in the reply and switch protocol

    Ok(axum::Json(reply))
}

fn create_router(varlink_sockets_dir: String) -> anyhow::Result<Router> {
    if !std::path::Path::new(&varlink_sockets_dir).is_dir() {
        bail!("path {} is not a directory", varlink_sockets_dir);
    }
    let shared_state = AppState {
        varlink_sockets_dir: Arc::new(varlink_sockets_dir),
    };

    // the /info endpoint is just "sugar", should we YAGNI it?
    let app = Router::new()
        .route("/info", get(route_info_get))
        .route("/info/{address}", get(route_info_address_get))
        .route(
            "/info/{address}/{interface}",
            get(route_info_address_interface_get),
        )
        .route("/call/{address}/{method}", post(route_call_post))
        .with_state(shared_state);

    Ok(app)
}

async fn run_server(varlink_sockets_dir: String, listener: TcpListener) -> anyhow::Result<()> {
    let app = create_router(varlink_sockets_dir)?;
    axum::serve(listener, app).await?;

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
        for _ in 0..NUM_TASKS {
            let client = Client::new();
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
        let client = reqwest::Client::new();

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
        let client = reqwest::Client::new();

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
        let client = reqwest::Client::new();

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
        let client = reqwest::Client::new();

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
        let client = reqwest::Client::new();

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
    async fn test_varlink_sockets_dir_missing() {
        let varlink_sockets_dir = "/does-not-exist".to_string();

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind to random port failed");
        let res = run_server(varlink_sockets_dir, listener).await;

        assert_eq!(res.is_err(), true);
        assert_eq!(
            res.unwrap_err().to_string(),
            "path /does-not-exist is not a directory"
        );
    }
}
