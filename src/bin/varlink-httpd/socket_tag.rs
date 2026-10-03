// SPDX-License-Identifier: LGPL-2.1-or-later

//! Give each varlink connection a kernel "socket-tag" by creating an
//! abstract unix socket with a "varlink-httpd/$random" name and use
//! that socket to connect to the actual varlink socket path. This
//! means that every varlink connection by the bridge to the varlink
//! service is now uniquely identifiable via (pid, tag).
//!
//! It must be random because anyone can create/observe/reuse the
//! abstract socket names. This also means we always need the tuple of
//! (pid, tag) to identify a connection.

use std::io;
use std::os::unix::net::UnixStream as StdUnixStream;

use data_encoding::HEXLOWER;
use log::debug;
use rustix::net::{AddressFamily, SocketAddrUnix, SocketFlags, SocketType};

/// Identifies the tags as ours to whoever reads one off a socket.
pub(crate) const PROXY_NAME: &str = "varlink-httpd";

const REFERENCE_BYTES: usize = 16;

/// Why [`connect_tagged`] failed.
///
/// Worth separating because the two mean different things to whoever is
/// waiting on the request: one is the service's problem, the other is ours.
#[derive(Debug)]
pub(crate) enum ConnectError {
    /// The service could not be reached.
    Unreachable(io::Error),
    /// Setting up our end failed, e.g. the process is out of descriptors.
    Local(io::Error),
}

impl std::fmt::Display for ConnectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unreachable(e) => write!(f, "cannot reach the service: {e}"),
            Self::Local(e) => write!(f, "cannot set up the client socket: {e}"),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ConnectionTag(String);

impl ConnectionTag {
    fn generate() -> io::Result<Self> {
        let mut bytes = [0u8; REFERENCE_BYTES];
        let n = rustix::rand::getrandom(&mut bytes[..], rustix::rand::GetRandomFlags::empty())?;
        // a full fill is guaranteed below 256 bytes, so this only guards the constant growing
        if n != REFERENCE_BYTES {
            return Err(io::Error::other("short read from getrandom"));
        }
        Ok(Self(HEXLOWER.encode(&bytes)))
    }

    pub(crate) fn reference(&self) -> &str {
        &self.0
    }

    pub(crate) fn abstract_name(&self) -> String {
        format!("{PROXY_NAME}/{}", self.0)
    }
}

/// Connect to the varlink socket at `path`, naming this end so the service
/// can tell this connection apart from the others we have open.
///
/// Use this for every varlink connection the bridge makes. A plain
/// `UnixStream::connect` leaves the end unnamed, and the service then has no
/// way to tell which of our connections it is talking to.
///
/// The name stays bound for as long as the returned stream is open.
pub(crate) fn connect_tagged(
    path: &str,
) -> Result<(tokio::net::UnixStream, ConnectionTag), ConnectError> {
    use ConnectError::{Local, Unreachable};

    let tag = ConnectionTag::generate().map_err(Local)?;
    let name = tag.abstract_name();
    let tag_addr =
        SocketAddrUnix::new_abstract_name(name.as_bytes()).map_err(|e| Local(e.into()))?;
    let service_addr = SocketAddrUnix::new(path).map_err(|e| Unreachable(e.into()))?;

    let fd = rustix::net::socket_with(
        AddressFamily::UNIX,
        SocketType::STREAM,
        SocketFlags::NONBLOCK | SocketFlags::CLOEXEC,
        None,
    )
    .map_err(|e| Local(e.into()))?;
    rustix::net::bind(&fd, &tag_addr).map_err(|e| Local(e.into()))?;

    // AF_UNIX connect() completes synchronously; a full listen backlog surfaces
    // as EAGAIN, never EINPROGRESS (connect(2)).
    rustix::net::connect(&fd, &service_addr).map_err(|e| Unreachable(e.into()))?;

    let stream = tokio::net::UnixStream::from_std(StdUnixStream::from(fd)).map_err(Local)?;
    // zlink's own connect() sets this; keep parity so varlink tooling still
    // recognises the socket as a client
    zlink::unix_utils::tag_socket(&stream, zlink::unix_utils::SocketRole::Client);

    debug!("connected to {path} as @{name}");
    Ok((stream, tag))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tags_are_unique_and_namespaced() {
        let a = ConnectionTag::generate().unwrap();
        let b = ConnectionTag::generate().unwrap();

        assert_ne!(a, b);
        assert_eq!(a.reference().len(), REFERENCE_BYTES * 2);
        assert!(a.abstract_name().starts_with("varlink-httpd/"));
    }

    /// Overflowing `sun_path` fails at runtime, not at compile time.
    #[test]
    fn abstract_name_fits_in_sun_path() {
        let tag = ConnectionTag::generate().unwrap();
        assert!(tag.abstract_name().len() <= 107);
    }

    /// The whole design rests on this, so exercise the real syscalls.
    #[tokio::test]
    async fn connect_tagged_makes_the_name_visible_to_the_server() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s");
        let listener = tokio::net::UnixListener::bind(&path).unwrap();

        let (client, tag) = connect_tagged(path.to_str().unwrap()).unwrap();
        let expected = tag.abstract_name();
        let (server_side, _) = listener.accept().await.unwrap();

        let peer = server_side.peer_addr().unwrap();
        assert_eq!(peer.as_abstract_name(), Some(expected.as_bytes()));
        drop(client);
    }

    /// How a server tells "no tag" apart from one it should act on.
    #[tokio::test]
    async fn an_unbound_client_shows_no_name() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s");
        let listener = tokio::net::UnixListener::bind(&path).unwrap();

        let client = tokio::net::UnixStream::connect(&path).await.unwrap();
        let (server_side, _) = listener.accept().await.unwrap();

        assert!(server_side.peer_addr().unwrap().is_unnamed());
        drop(client);
    }
}
