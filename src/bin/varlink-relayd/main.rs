// SPDX-License-Identifier: LGPL-2.1-or-later

//! varlink-relayd: makes a `varlink-httpd` reachable when it runs
//! behind NAT or a firewall that only lets HTTPS out (README.relayd.md).
//!
//! Nodes dial out to `--bind` with a WebSocket and stay connected; the
//! relay runs HTTP/2 over each of those connections as the h2 client.
//! A caller sends `CONNECT <id>` to `--connect-bind`; the relay opens
//! one h2 stream to that node and from then on splices opaque bytes,
//! never terminating the caller's TLS session to the node's bridge.
//!
//! The two faces live in [`node`] and [`caller`] and share nothing but
//! the [`registry::Nodes`] map: the node face writes it, the caller
//! face only reads it.

mod caller;
mod node;
mod registry;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use log::{info, warn};
use tokio::net::TcpListener;

use varlink_http_bridge::tunnel::TUNNEL_PATH;

use crate::registry::Nodes;

const DEFAULT_BIND: &str = "127.0.0.1:8443";
const DEFAULT_CONNECT_BIND: &str = "127.0.0.1:8444";
// bounds every prelude (TLS, upgrade, h2 handshake, CONNECT head), so a
// silent or trickling peer cannot pin a task and fd forever
pub(crate) const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const USAGE: &str = "\
usage: varlink-relayd --insecure [options]

options:
  --bind ADDR          node-facing WebSocket listener (default 127.0.0.1:8443)
  --connect-bind ADDR  caller-facing CONNECT listener (default 127.0.0.1:8444)
  --cert PEM           TLS certificate for the node listener
  --key PEM            TLS key for the node listener
  --insecure           L0 node auth: nodes just assert their id
";

struct Args {
    bind: String,
    connect_bind: String,
    tls: Option<openssl::ssl::SslAcceptor>,
}

fn parse_args() -> Result<Args> {
    use lexopt::prelude::*;

    let mut bind = DEFAULT_BIND.to_string();
    let mut connect_bind = DEFAULT_CONNECT_BIND.to_string();
    let mut cert = None;
    let mut key = None;
    let mut insecure = false;

    let mut parser = lexopt::Parser::from_env();
    while let Some(arg) = parser.next()? {
        match arg {
            Long("bind") => bind = parser.value()?.string()?,
            Long("connect-bind") => connect_bind = parser.value()?.string()?,
            Long("cert") => cert = Some(parser.value()?.string()?),
            Long("key") => key = Some(parser.value()?.string()?),
            Long("insecure") => insecure = true,
            Short('h') | Long("help") => {
                print!("{USAGE}");
                std::process::exit(0);
            }
            _ => return Err(arg.unexpected().into()),
        }
    }

    if !insecure {
        bail!(
            "device-key node authentication is not implemented yet; \
             --insecure (nodes just assert their id) is required for now"
        );
    }
    warn!("running with --insecure: anyone reaching {bind} can claim any node id");

    let tls = match (cert, key) {
        (Some(cert), Some(key)) => Some(varlink_http_bridge::tls_acceptor(&cert, &key, None)?),
        (None, None) => {
            warn!("no --cert/--key: nodes connect over plain ws://, testing only");
            None
        }
        _ => bail!("--cert and --key must be given together"),
    };
    Ok(Args {
        bind,
        connect_bind,
        tls,
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let args = parse_args()?;

    let node_listener = TcpListener::bind(&args.bind)
        .await
        .with_context(|| format!("binding node listener {}", args.bind))?;
    let connect_listener = TcpListener::bind(&args.connect_bind)
        .await
        .with_context(|| format!("binding caller listener {}", args.connect_bind))?;
    info!(
        "nodes dial in at {}{TUNNEL_PATH}, callers CONNECT to {}",
        node_listener
            .local_addr()
            .context("node listener address")?,
        connect_listener
            .local_addr()
            .context("caller listener address")?,
    );

    let nodes = Arc::new(Nodes::default());
    tokio::try_join!(
        node::serve(node_listener, args.tls, Arc::clone(&nodes)),
        caller::serve(connect_listener, nodes),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests;
