// SPDX-License-Identifier: LGPL-2.1-or-later

//! A WebSocket as an `AsyncRead`/`AsyncWrite` byte stream, so code that
//! only moves bytes, such as `io::copy` or an h2 connection, can run over
//! a WebSocket without knowing about frames.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use bytes::{Buf, Bytes};
use futures_util::{Sink, Stream};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::error::{Error as WsError, ProtocolError};

/// Binary payloads are the stream, a close frame is EOF and a text
/// frame is an error, since only a confused peer or middlebox sends
/// one. Shutdown sends a close frame so the peer sees EOF rather than
/// a dropped connection.
pub struct WsByteStream<S> {
    ws: WebSocketStream<S>,
    readbuf: Bytes,
    abrupt_close_is_eof: bool,
}

impl<S> WsByteStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// A peer that vanishes without a close handshake is an error, as
    /// a reset is on any socket.
    pub fn new(ws: WebSocketStream<S>) -> Self {
        Self {
            ws,
            readbuf: Bytes::new(),
            abrupt_close_is_eof: false,
        }
    }

    /// Treat a peer that vanishes without a close handshake as EOF, for
    /// long-lived connections through middleboxes that reap them without
    /// ever sending a close frame.
    #[must_use]
    pub fn abrupt_close_as_eof(mut self) -> Self {
        self.abrupt_close_is_eof = true;
        self
    }
}

impl<S> AsyncRead for WsByteStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        loop {
            if !this.readbuf.is_empty() {
                let n = this.readbuf.len().min(buf.remaining());
                buf.put_slice(&this.readbuf[..n]);
                this.readbuf.advance(n);
                return Poll::Ready(Ok(()));
            }
            let abrupt = match ready!(Pin::new(&mut this.ws).poll_next(cx)) {
                Some(Ok(Message::Binary(data))) => {
                    this.readbuf = data;
                    continue;
                }
                Some(Ok(Message::Text(_))) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unexpected text WebSocket frame",
                    )));
                }
                // tungstenite answers pings on its read path, with retry
                // on a blocked socket; flushing here would bypass that
                // retry and could strand the pong
                Some(Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_))) => continue,
                Some(Ok(Message::Close(_))) | None => return Poll::Ready(Ok(())),
                Some(Err(
                    e @ (WsError::Protocol(ProtocolError::ResetWithoutClosingHandshake)
                    | WsError::ConnectionClosed),
                )) => io::Error::new(io::ErrorKind::UnexpectedEof, e),
                Some(Err(WsError::Io(e)))
                    if matches!(
                        e.kind(),
                        io::ErrorKind::ConnectionReset | io::ErrorKind::UnexpectedEof
                    ) =>
                {
                    e
                }
                Some(Err(e)) => return Poll::Ready(Err(io::Error::other(e))),
            };
            return Poll::Ready(if this.abrupt_close_is_eof {
                Ok(())
            } else {
                Err(abrupt)
            });
        }
    }
}

impl<S> AsyncWrite for WsByteStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        ready!(Pin::new(&mut this.ws).poll_ready(cx)).map_err(io::Error::other)?;
        Pin::new(&mut this.ws)
            .start_send(Message::Binary(Bytes::copy_from_slice(buf)))
            .map_err(io::Error::other)?;
        Poll::Ready(Ok(buf.len()))
    }

    // without this a vectored writer such as h2 turns every slice into
    // its own masked WebSocket message
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let total = bufs.iter().map(|b| b.len()).sum();
        if total == 0 {
            return Poll::Ready(Ok(0));
        }
        let this = self.get_mut();
        ready!(Pin::new(&mut this.ws).poll_ready(cx)).map_err(io::Error::other)?;
        let mut data = bytes::BytesMut::with_capacity(total);
        for buf in bufs {
            data.extend_from_slice(buf);
        }
        Pin::new(&mut this.ws)
            .start_send(Message::Binary(data.freeze()))
            .map_err(io::Error::other)?;
        Poll::Ready(Ok(total))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().ws)
            .poll_flush(cx)
            .map_err(io::Error::other)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match ready!(Pin::new(&mut self.get_mut().ws).poll_close(cx)) {
            // the peer closed first and tungstenite already answered
            Ok(()) | Err(WsError::ConnectionClosed | WsError::AlreadyClosed) => Poll::Ready(Ok(())),
            Err(e) => Poll::Ready(Err(io::Error::other(e))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::SinkExt as _;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream, duplex};
    use tokio_tungstenite::tungstenite::protocol::Role;

    async fn raw_pair() -> (WebSocketStream<DuplexStream>, WebSocketStream<DuplexStream>) {
        let (a, b) = duplex(4096);
        tokio::join!(
            WebSocketStream::from_raw_socket(a, Role::Client, None),
            WebSocketStream::from_raw_socket(b, Role::Server, None),
        )
    }

    async fn ws_pair() -> (WsByteStream<DuplexStream>, WsByteStream<DuplexStream>) {
        let (client, server) = raw_pair().await;
        (WsByteStream::new(client), WsByteStream::new(server))
    }

    #[tokio::test]
    async fn text_frames_are_an_error() {
        let (mut client, server) = raw_pair().await;
        let mut server = WsByteStream::new(server);
        client.send(Message::Text("nope".into())).await.unwrap();
        let mut buf = [0u8; 4];
        let err = server.read_exact(&mut buf).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn roundtrip_and_eof() {
        let (mut client, mut server) = ws_pair().await;

        client.write_all(b"ping").await.unwrap();
        client.flush().await.unwrap();
        let mut small = [0u8; 4];
        server.read_exact(&mut small).await.unwrap();
        assert_eq!(&small, b"ping");
        server.write_all(b"pong").await.unwrap();
        server.flush().await.unwrap();
        client.read_exact(&mut small).await.unwrap();
        assert_eq!(&small, b"pong");

        // larger than every buffer in the path
        let data: Vec<u8> = (0..256 * 1024)
            .map(|i| u8::try_from(i % 251).unwrap())
            .collect();
        let expected = data.clone();
        let writer = tokio::spawn(async move {
            client.write_all(&data).await.unwrap();
            client.shutdown().await.unwrap();
        });
        let mut got = Vec::new();
        server.read_to_end(&mut got).await.unwrap();
        writer.await.unwrap();
        assert_eq!(got, expected);
    }

    #[tokio::test]
    async fn shutdown_after_the_peer_closed_is_not_an_error() {
        let (mut client, mut server) = ws_pair().await;
        client.shutdown().await.unwrap();
        let mut got = Vec::new();
        server.read_to_end(&mut got).await.unwrap();
        assert_eq!(got, b"");
        server.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn a_vanished_peer_is_an_error_unless_asked_for() {
        let (client, server) = raw_pair().await;
        let mut server = WsByteStream::new(server);
        drop(client);
        let mut buf = [0u8; 1];
        let err = server.read(&mut buf).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);

        let (client, server) = raw_pair().await;
        let mut server = WsByteStream::new(server).abrupt_close_as_eof();
        drop(client);
        assert_eq!(server.read(&mut buf).await.unwrap(), 0);
    }
}
