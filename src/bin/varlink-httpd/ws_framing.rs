// SPDX-License-Identifier: LGPL-2.1-or-later

//! Framing and upgrade detection for the WebSocket <-> varlink byte
//! bridge, kept free of I/O so it can be unit tested without sockets.

use log::{debug, warn};
use serde_json::Value;

/// Before a varlink protocol upgrade each NUL-delimited varlink message
/// becomes one WebSocket frame (delimiter included), which makes the
/// websocket easy to use from tools like "websocat". After an upgrade
/// the connection is a raw byte stream and everything passes through
/// unsplit.
pub(crate) struct VarlinkFramer {
    buf: Vec<u8>,
    upgraded: bool,
}

impl VarlinkFramer {
    pub(crate) fn new() -> Self {
        Self {
            buf: Vec::new(),
            upgraded: false,
        }
    }

    /// Detect a varlink protocol upgrade request (`{"upgrade": true}`)
    /// in a client->varlink message.
    pub(crate) fn detect_protocol_upgrade_request(&mut self, data: &[u8]) {
        if self.upgraded {
            return;
        }
        // tools like "websocat" add a \n or \0 after each "message";
        // the \0 must be stripped, trailing \n is fine for the parser
        let json_bytes = data.strip_suffix(&[0]).unwrap_or(data);
        match serde_json::from_slice::<Value>(json_bytes) {
            Ok(v) => {
                if v.get("upgrade").and_then(Value::as_bool).unwrap_or(false) {
                    debug!("varlink protocol upgrade detected");
                    self.upgraded = true;
                }
            }
            Err(e) => {
                warn!("failed to parse ws message as JSON for upgrade detection: {e}");
            }
        }
    }

    /// Feed bytes read from the varlink socket, returning the frames
    /// ready to be sent to the WebSocket.
    pub(crate) fn push_varlink_bytes(&mut self, data: &[u8]) -> Vec<Vec<u8>> {
        self.buf.extend_from_slice(data);

        if self.upgraded {
            if self.buf.is_empty() {
                return Vec::new();
            }
            return vec![std::mem::take(&mut self.buf)];
        }

        let mut frames = Vec::new();
        while let Some(pos) = self.buf.iter().position(|&b| b == 0) {
            let rest = self.buf.split_off(pos + 1);
            frames.push(std::mem::replace(&mut self.buf, rest));
        }
        frames
    }

    /// Return any buffered partial message so nothing is lost when the
    /// varlink socket hits EOF.
    pub(crate) fn finish(&mut self) -> Option<Vec<u8>> {
        if self.buf.is_empty() {
            None
        } else {
            Some(std::mem::take(&mut self.buf))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_one_message_per_frame_including_delimiter() {
        let mut f = VarlinkFramer::new();
        assert_eq!(
            f.push_varlink_bytes(b"{\"a\":1}\0{\"b\":2}\0"),
            vec![b"{\"a\":1}\0".to_vec(), b"{\"b\":2}\0".to_vec()]
        );
        assert_eq!(f.finish(), None);
    }

    #[test]
    fn test_message_split_across_reads() {
        let mut f = VarlinkFramer::new();
        assert!(f.push_varlink_bytes(b"{\"met").is_empty());
        assert!(f.push_varlink_bytes(b"hod\":\"x\"}").is_empty());
        assert_eq!(
            f.push_varlink_bytes(b"\0{\"next").as_slice(),
            &[b"{\"method\":\"x\"}\0".to_vec()]
        );
        assert_eq!(f.finish(), Some(b"{\"next".to_vec()));
        assert_eq!(f.finish(), None);
    }

    #[test]
    fn test_empty_push_yields_no_frames() {
        let mut f = VarlinkFramer::new();
        assert!(f.push_varlink_bytes(b"").is_empty());
        assert_eq!(f.finish(), None);
    }

    #[test]
    fn test_upgrade_detection() {
        let mut f = VarlinkFramer::new();

        f.detect_protocol_upgrade_request(b"{\"method\":\"io.systemd.Hostname.Describe\"}\0");
        assert!(!f.upgraded);
        f.detect_protocol_upgrade_request(b"{\"upgrade\":false}\0");
        assert!(!f.upgraded);
        f.detect_protocol_upgrade_request(b"not json at all");
        assert!(!f.upgraded);

        f.detect_protocol_upgrade_request(b"{\"method\":\"m\",\"upgrade\":true}\0");
        assert!(f.upgraded);
    }

    #[test]
    fn test_upgrade_detection_trailing_whitespace() {
        // websocat appends \n after each message
        let mut f = VarlinkFramer::new();
        f.detect_protocol_upgrade_request(b"{\"upgrade\":true}\n");
        assert!(f.upgraded);
    }

    #[test]
    fn test_raw_passthrough_after_upgrade() {
        let mut f = VarlinkFramer::new();
        f.detect_protocol_upgrade_request(b"{\"upgrade\":true}\0");

        assert_eq!(
            f.push_varlink_bytes(b"raw\0bytes\0no framing"),
            vec![b"raw\0bytes\0no framing".to_vec()]
        );
        assert!(f.push_varlink_bytes(b"").is_empty());

        // post-upgrade client bytes must not be parsed (or warned about)
        f.detect_protocol_upgrade_request(b"\xff\xfe raw upload data");
        assert!(f.upgraded);
    }

    #[test]
    fn test_buffered_partial_flushed_on_upgrade() {
        let mut f = VarlinkFramer::new();
        assert!(f.push_varlink_bytes(b"{\"reply\"").is_empty());
        f.detect_protocol_upgrade_request(b"{\"upgrade\":true}\0");
        assert_eq!(
            f.push_varlink_bytes(b":42}\0raw"),
            vec![b"{\"reply\":42}\0raw".to_vec()]
        );
    }
}
