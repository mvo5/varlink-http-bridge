// SPDX-License-Identifier: LGPL-2.1-or-later

//! The caller-tag provider polkit calls to turn a tag back into who the
//! bridge authenticated. One method, NUL-framed JSON, no other varlink
//! machinery: the zlink server feature is deliberately not enabled.
//!
//! Security rests on polkit, not on anything in the request: before it
//! sends Resolve, polkit checks with SO_PEERPIDFD that whoever called
//! listen() on our socket is the subject (or shares its cgroup). So the
//! socket must be listened on by this process, see [`adopt_activated`].

use std::io;
use std::os::fd::OwnedFd;

use log::{debug, warn};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};

use crate::identity::IdentityTable;

/// Where polkit looks for us: its tmpfiles entry makes the directory
/// root-owned and setgid polkitd, so only root (or systemd via a socket
/// unit) can register a provider there.
pub(crate) const DEFAULT_SOCKET: &str =
    "/run/polkit-1/caller-tag-providers/io.systemd.VarlinkBridge";

const METHOD: &str = "io.systemd.VarlinkBridge.Resolve";

const MAX_MESSAGE: usize = 64 * 1024;

const BACKLOG: i32 = 64;

/// Bind the provider socket ourselves; needs write access to the directory,
/// i.e. root. A unit running as DynamicUser= gets the socket from systemd
/// instead, see [`adopt_activated`].
pub(crate) fn bind(path: &str) -> io::Result<UnixListener> {
    // a socket file left by a previous run makes bind() fail
    if let Err(e) = std::fs::remove_file(path)
        && e.kind() != io::ErrorKind::NotFound
    {
        return Err(e);
    }
    UnixListener::bind(path)
}

/// Take over the provider socket a `.socket` unit bound for us.
///
/// systemd called listen() on it, so the socket carries pid 1's credentials
/// and polkit's SO_PEERPIDFD check would see init, not us. A second listen()
/// restamps the credentials with this process, which is what the check
/// needs to see. Must run before the first client is served, or that
/// client's tag is checked against pid 1.
pub(crate) fn adopt_activated(fd: OwnedFd) -> io::Result<UnixListener> {
    rustix::net::listen(&fd, BACKLOG)?;
    let std = std::os::unix::net::UnixListener::from(fd);
    std.set_nonblocking(true)?;
    UnixListener::from_std(std)
}

pub(crate) async fn serve(listener: UnixListener, table: IdentityTable) {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let table = table.clone();
                tokio::spawn(async move {
                    if let Err(e) = serve_conn(stream, &table).await {
                        debug!("resolver: connection ended: {e}");
                    }
                });
            }
            Err(e) => warn!("resolver: accept failed: {e}"),
        }
    }
}

async fn serve_conn(mut stream: UnixStream, table: &IdentityTable) -> io::Result<()> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&chunk[..n]);
        while let Some(pos) = buf.iter().position(|&b| b == 0) {
            let message: Vec<u8> = buf.drain(..=pos).collect();
            let mut reply = serde_json::to_vec(&handle_resolve(&message[..pos], table))?;
            reply.push(0);
            stream.write_all(&reply).await?;
        }
        if buf.len() > MAX_MESSAGE {
            return Err(io::Error::other("resolver: request too large"));
        }
    }
}

/// "Unknown" is a reply with no identity rather than an error, so polkit
/// treats it like any other unprivileged subject.
fn handle_resolve(message: &[u8], table: &IdentityTable) -> Value {
    let Ok(call) = serde_json::from_slice::<Value>(message) else {
        return json!({"error": "org.varlink.service.InvalidParameter",
                      "parameters": {"parameter": "<message>"}});
    };
    if call["method"].as_str() != Some(METHOD) {
        return json!({"error": "org.varlink.service.MethodNotFound",
                      "parameters": {"method": call["method"]}});
    }

    // Anyone can read a tag from /proc/net/unix and rebind it once we
    // release it. That is polkit's problem to catch, and it does, by
    // verifying that the subject presenting the tag is us (see the module
    // doc). We only vouch for references we bound ourselves, and a lease
    // drops out of the table the moment its connection closes.
    let reference = call["parameters"]["reference"].as_str().unwrap_or("");
    match table.resolve(reference) {
        Some(identity) => json!({"parameters": {"identity": identity}}),
        None => json!({"parameters": {}}),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::Identity;
    use crate::socket_tag::ConnectionTag;

    fn table_with_one() -> (IdentityTable, crate::identity::Lease) {
        let table = IdentityTable::new();
        let lease = table.bind(
            ConnectionTag::generate().unwrap(),
            Identity::new("api-key", "ci"),
        );
        (table, lease)
    }

    /// What polkit sends: the method and the reference, nothing else.
    fn call(reference: &str) -> Vec<u8> {
        serde_json::to_vec(&json!({"method": METHOD, "parameters": {"reference": reference}}))
            .unwrap()
    }

    #[test]
    fn resolves_a_bound_tag() {
        let (table, lease) = table_with_one();
        let reply = handle_resolve(&call(lease.reference()), &table);
        assert_eq!(reply["parameters"]["identity"]["mechanism"], "api-key");
        assert_eq!(reply["parameters"]["identity"]["caller"], "ci");
    }

    /// A released name may be rebound by anyone; it must stop resolving.
    #[test]
    fn a_dropped_lease_no_longer_resolves() {
        let (table, lease) = table_with_one();
        let reference = lease.reference().to_string();
        drop(lease);
        assert_eq!(
            handle_resolve(&call(&reference), &table),
            json!({"parameters": {}})
        );
    }

    #[test]
    fn unknown_tag_and_bad_input() {
        let (table, _lease) = table_with_one();
        assert_eq!(
            handle_resolve(&call("nope"), &table),
            json!({"parameters": {}})
        );
        assert_eq!(
            handle_resolve(br#"{"method":"org.varlink.service.GetInfo"}"#, &table)["error"],
            "org.varlink.service.MethodNotFound"
        );
        assert_eq!(
            handle_resolve(b"{not json", &table)["error"],
            "org.varlink.service.InvalidParameter"
        );
    }

    /// An activated socket must stay usable after we listen() on it again.
    #[tokio::test]
    async fn adopted_socket_accepts() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s");
        let activated = std::os::unix::net::UnixListener::bind(&path).unwrap();

        let listener = adopt_activated(activated.into()).unwrap();
        let client = tokio::net::UnixStream::connect(&path).await.unwrap();
        listener.accept().await.unwrap();
        drop(client);
    }
}
