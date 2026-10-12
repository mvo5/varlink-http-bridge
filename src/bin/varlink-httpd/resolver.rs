// SPDX-License-Identifier: LGPL-2.1-or-later

//! The caller-tag provider polkit calls to turn a tag back into who the
//! bridge authenticated. One method, NUL-framed JSON, no other varlink
//! machinery: the zlink server feature is deliberately not enabled.

use std::io;

use log::{debug, warn};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};

use crate::identity::IdentityTable;

/// Where polkit looks for us (naming TBD with polkit).
pub(crate) const DEFAULT_SOCKET: &str =
    "/run/polkit/trusted-caller-tag-providers/io.systemd.VarlinkBridge";

const METHOD: &str = "io.systemd.VarlinkBridge.Resolve";

const MAX_MESSAGE: usize = 64 * 1024;

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
    // release it. What they cannot do is be our pid, so a tag only resolves
    // for a subject that is this process.
    //
    // FIXME: this takes a plain pid, the RFC says pidfd. A pid number is
    // what varlinkctl can send from a shell, which is all the PoC needs,
    // but it is not what polkit will pass: polkit holds the subject as a
    // pidfd and should hand that over (SCM_RIGHTS), and this check then
    // becomes "does the pidfd refer to our process" via /proc/self/fdinfo
    // or pidfd_getpid(). Alternatively polkit verifies it on its side with
    // SO_PEERCRED on its connection to us and we drop the argument. Which
    // of the two is a polkit decision; until it is made, do not build on
    // the pid integer being here.
    let pid = call["parameters"]["pid"].as_u64();
    if pid != Some(u64::from(std::process::id())) {
        debug!("resolver: refusing tag for subject pid {pid:?}");
        return json!({"parameters": {}});
    }

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

    fn call(reference: &str, pid: u64) -> Vec<u8> {
        serde_json::to_vec(&json!({"method": METHOD,
                                   "parameters": {"reference": reference, "pid": pid}}))
        .unwrap()
    }

    #[test]
    fn resolves_our_own_tag() {
        let (table, lease) = table_with_one();
        let reply = handle_resolve(&call(lease.reference(), std::process::id().into()), &table);
        assert_eq!(reply["parameters"]["identity"]["mechanism"], "api-key");
        assert_eq!(reply["parameters"]["identity"]["caller"], "ci");
    }

    /// The rebind race: a valid tag presented by a process that is not us.
    #[test]
    fn refuses_another_pid() {
        let (table, lease) = table_with_one();
        let reply = handle_resolve(
            &call(lease.reference(), u64::from(std::process::id()) + 1),
            &table,
        );
        assert_eq!(reply, json!({"parameters": {}}));
    }

    #[test]
    fn unknown_tag_and_bad_input() {
        let (table, _lease) = table_with_one();
        assert_eq!(
            handle_resolve(&call("nope", std::process::id().into()), &table),
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
}
