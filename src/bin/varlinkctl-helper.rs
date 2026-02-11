use std::os::unix::io::FromRawFd;

use anyhow::{Context, Result, bail};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::UnixStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

fn ws_url(url: &str) -> String {
    if let Some(rest) = url.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = url.strip_prefix("http://") {
        format!("ws://{rest}")
    } else {
        url.to_string()
    }
}

async fn fd3_to_ws(
    fd3: &mut ReadHalf<UnixStream>,
    ws: &mut SplitSink<WsStream, Message>,
) -> Result<()> {
    let mut buf = vec![0u8; 8192];
    loop {
        let n = fd3.read(&mut buf).await?;
        if n == 0 {
            return Ok(());
        }
        ws.send(Message::Binary(buf[..n].to_vec().into())).await?;
    }
}

async fn ws_to_fd3(ws: &mut SplitStream<WsStream>, fd3: &mut WriteHalf<UnixStream>) -> Result<()> {
    while let Some(msg) = ws.next().await {
        match msg? {
            Message::Binary(data) => fd3.write_all(&data).await?,
            Message::Text(_) => bail!("unexpected text WebSocket frame"),
            Message::Close(_) => return Ok(()),
            _ => {}
        }
    }
    Ok(())
}

async fn run() -> Result<()> {
    let listen_fds: i32 = std::env::var("LISTEN_FDS")
        .context("LISTEN_FDS is not set")?
        .parse()
        .context("LISTEN_FDS is not a valid integer")?;
    if listen_fds != 1 {
        bail!("LISTEN_FDS must be 1, got {listen_fds}");
    }

    // XXX: we need a better way, varlinkctl needs to pass this, see
    // https://github.com/systemd/systemd/issues/40640
    let bridge_url =
        std::env::var("VARLINK_BRIDGE_URL").context("VARLINK_BRIDGE_URL is not set")?;
    let ws_url = ws_url(&bridge_url);

    // we need to open raw fd 3 here, we could
    // UnixStream::connect("/proc/self/fd/3") to avoid the unsafe{}
    // but that relies on having /proc
    let std_stream = unsafe { std::os::unix::net::UnixStream::from_raw_fd(3) };
    std_stream
        .set_nonblocking(true)
        .context("failed to set fd 3 non-blocking")?;
    let fd3 =
        tokio::net::UnixStream::from_std(std_stream).context("failed to get fd 3 into tokio")?;
    let (mut fd3_read, mut fd3_write) = tokio::io::split(fd3);

    let (ws_stream, _) = tokio_tungstenite::connect_async(&ws_url)
        .await
        .with_context(|| format!("WebSocket connect to {ws_url} failed"))?;
    let (mut ws_write, mut ws_read) = ws_stream.split();

    tokio::select! {
        res = fd3_to_ws(&mut fd3_read, &mut ws_write) => res.context("fd3→ws")?,
        res = ws_to_fd3(&mut ws_read, &mut fd3_write) => res.context("ws→fd3")?,
    }
    Ok(())
}

// avoid multi_thread overhead, we just have a single select!, see
// https://docs.rs/tokio/latest/tokio/attr.main.html#current-thread,
#[tokio::main(flavor = "current_thread")]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("Error: {e:#}");
        std::process::exit(1);
    }
}
