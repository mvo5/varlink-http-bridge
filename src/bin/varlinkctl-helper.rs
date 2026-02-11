use std::io::{Read, Write};
use std::net::TcpStream;
use std::os::fd::{FromRawFd, OwnedFd};
use std::os::unix::net::UnixStream;

use anyhow::{Context, Result, bail};
use rustix::event::{PollFd, PollFlags, poll};
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{Message, WebSocket};

type Ws = WebSocket<MaybeTlsStream<TcpStream>>;

fn ws_url(url: &str) -> String {
    if let Some(rest) = url.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = url.strip_prefix("http://") {
        format!("ws://{rest}")
    } else {
        url.to_string()
    }
}

fn ws_tcp_stream(ws: &Ws) -> Result<&TcpStream> {
    match ws.get_ref() {
        MaybeTlsStream::Plain(s) => Ok(s),
        MaybeTlsStream::NativeTls(s) => Ok(s.get_ref()),
        _ => bail!("unsupported TLS stream type {ws:?}"),
    }
}

fn run() -> Result<()> {
    let listen_fds: i32 = std::env::var("LISTEN_FDS")
        .context("LISTEN_FDS is not set")?
        .parse()
        .context("LISTEN_FDS is not a valid integer")?;
    if listen_fds != 1 {
        bail!("LISTEN_FDS must be 1, got {listen_fds}");
    }

    // XXX: once https://github.com/systemd/systemd/issues/40640 is implemented
    // we can remove the env_url and this confusing mach
    let env_url = std::env::var("VARLINK_BRIDGE_URL").ok();
    let arg_url = std::env::args().nth(1);
    let bridge_url = match (env_url, arg_url) {
        (Some(_), Some(_)) => bail!("cannot set both VARLINK_BRIDGE_URL and argv[1]"),
        (None, None) => bail!("bridge URL required via VARLINK_BRIDGE_URL or argv[1]"),
        (Some(url), None) | (None, Some(url)) => url,
    };
    let ws_url = ws_url(&bridge_url);

    // Safety: fd 3 is passed to us via the sd_listen_fds() protocol.
    let fd3 = unsafe { OwnedFd::from_raw_fd(3) };
    rustix::io::fcntl_getfd(&fd3).context("fd 3 is not valid (LISTEN_FDS protocol error?)")?;
    let mut fd3 = UnixStream::from(fd3);

    let (mut ws, _) = tungstenite::connect(&ws_url)
        .with_context(|| format!("WebSocket connect to {ws_url} failed"))?;

    // Set non-blocking so that we deal with incomplete websocket
    // frames in ws.read() - they return WouldBlock now and we can
    // continue when waking up from PollFd next time.
    ws_tcp_stream(&ws)?.set_nonblocking(true)?;

    let mut buf = vec![0u8; 8192];
    loop {
        let mut pollfds = [
            PollFd::new(&fd3, PollFlags::IN),
            PollFd::new(ws_tcp_stream(&ws)?, PollFlags::IN),
        ];
        poll(&mut pollfds, None)?;
        let fd3_revents = pollfds[0].revents();
        let ws_revents = pollfds[1].revents();

        if fd3_revents.contains(PollFlags::IN) {
            let n = fd3.read(&mut buf).context("fd3 read")?;
            if n == 0 {
                return Ok(());
            }
            ws.send(Message::Binary(buf[..n].to_vec().into()))
                .context("ws send")?;
        }

        if ws_revents.contains(PollFlags::IN) {
            loop {
                match ws.read() {
                    Ok(Message::Binary(data)) => fd3.write_all(&data).context("fd3 write")?,
                    Ok(Message::Text(_)) => bail!("unexpected text WebSocket frame"),
                    Ok(Message::Close(_)) => return Ok(()),
                    Ok(_) => {}
                    Err(tungstenite::Error::Io(ref e))
                        if e.kind() == std::io::ErrorKind::WouldBlock =>
                    {
                        break;
                    }
                    Err(e) => return Err(e).context("ws read"),
                }
            }
        }

        if fd3_revents.contains(PollFlags::HUP) {
            return Ok(());
        }
    }
}

fn main() {
    if let Err(e) = run() {
        eprintln!("Error: {e:#}");
        std::process::exit(1);
    }
}
