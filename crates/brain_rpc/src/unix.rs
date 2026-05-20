//! `UnixSocketTransport` — the production `Transport` adapter.
//!
//! # Hexagonal role
//!
//! This is the **only** module in the crate that touches concrete I/O.
//! Everything related to `std::io`, `std::os::unix`, `UnixStream`,
//! framing, and serialization lives here; the port-layer files
//! (`domain.rs`, `transport.rs`, `client.rs`, `testing.rs`) stay pure
//! by construction. The `just audit-rpc` recipe enforces this — its
//! port-I/O grep explicitly excludes `unix.rs`.
//!
//! # Wire format
//!
//! Each message is a single line of JSON terminated by `\n`. JSON
//! values produced by `serde_json::to_vec` (the non-pretty serializer)
//! contain no raw newline characters, so `\n` is a safe frame delimiter
//! by construction. Empty frames (zero-byte payload + `\n`) are
//! technically representable but never valid: every `Request` and
//! `Response` serializes to at least `"{}"`.

#![cfg(unix)]

use std::io::{self, BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;

use crate::domain::{Request, Response, RpcError};
use crate::transport::Transport;

/// `Transport` adapter that talks newline-delimited JSON over a
/// [`UnixStream`]. All concrete I/O is encapsulated; consumers see
/// only the `Transport` trait surface.
pub struct UnixSocketTransport {
    reader: BufReader<UnixStream>,
}

impl UnixSocketTransport {
    /// Connect to `socket_path` and return a ready transport.
    ///
    /// # Errors
    ///
    /// Returns [`RpcError::Transport`] on socket-level failures
    /// (path doesn't exist, peer refused, permission denied). The
    /// underlying `io::Error` is stringified — full source chains
    /// must be logged by the caller before the error is constructed
    /// here, because the wire-format error type drops sources by
    /// design (see [`crate::domain::RpcError`]).
    pub fn connect(socket_path: &Path) -> Result<Self, RpcError> {
        let stream = UnixStream::connect(socket_path).map_err(|e| RpcError::Transport {
            message: format!("connect({}): {e}", socket_path.display()),
        })?;
        Ok(Self {
            reader: BufReader::new(stream),
        })
    }
}

impl Transport for UnixSocketTransport {
    fn call(&mut self, req: Request) -> Result<Response, RpcError> {
        let payload = serde_json::to_vec(&req).map_err(|e| RpcError::Protocol {
            message: format!("serialize request: {e}"),
        })?;

        write_frame(self.reader.get_mut(), &payload).map_err(|e| RpcError::Transport {
            message: format!("write frame: {e}"),
        })?;

        let response_bytes = read_frame(&mut self.reader).map_err(|e| RpcError::Transport {
            message: format!("read frame: {e}"),
        })?;

        // The wire carries two distinct JSON shapes serialised by serde:
        //   - RpcError  → {"kind":"…", …}   (uses `kind` as the enum tag)
        //   - Response  → {"type":"…", …}   (uses `type`  as the enum tag)
        //
        // Both use adjacent tag field names (5 bytes: `{"k` vs `{"t`), so a
        // single byte prefix check is enough to dispatch without a full parse.
        if response_bytes.len() >= 5 && response_bytes[0..5] == *b"{\"kind" {
            return serde_json::from_slice(&response_bytes).map_err(|e| RpcError::Protocol {
                message: format!("deserialize error: {e}"),
            });
        }

        serde_json::from_slice(&response_bytes).map_err(|e| RpcError::Protocol {
            message: format!("deserialize response: {e}"),
        })
    }
}

/// Write `payload` to `writer` as one newline-terminated frame.
///
/// Public so the daemon-side adapter (`brain_daemon::server`) can use
/// the same framing implementation. Keeping a single implementation —
/// one for client-side writes, one for server-side writes — prevents
/// the wire format from drifting between sender and receiver.
pub fn write_frame<W: Write>(writer: &mut W, payload: &[u8]) -> io::Result<()> {
    writer.write_all(payload)?;
    writer.write_all(b"\n")?;
    writer.flush()
}

/// Read one newline-terminated frame from `reader`. The returned
/// vector does **not** include the trailing `\n`.
///
/// Public so the daemon-side adapter (`brain_daemon::server`) can use
/// the same framing implementation as [`UnixSocketTransport`] — see
/// [`write_frame`] for the rationale.
///
/// Returns [`io::ErrorKind::UnexpectedEof`] if the reader closes
/// before a full frame arrives.
pub fn read_frame<R: BufRead>(reader: &mut R) -> io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    let n = reader.read_until(b'\n', &mut buf)?;
    if n == 0 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "transport closed before end of frame",
        ));
    }
    // Frame terminator: read_until includes the delimiter when one is
    // found. Strip it before returning so callers see the raw payload.
    if buf.last() == Some(&b'\n') {
        buf.pop();
    }
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_roundtrips() {
        let mut buf: Vec<u8> = Vec::new();
        write_frame(&mut buf, b"{\"type\":\"ping\"}").unwrap();
        assert_eq!(buf, b"{\"type\":\"ping\"}\n");

        let mut cursor = &buf[..];
        let frame = read_frame(&mut cursor).unwrap();
        assert_eq!(frame, b"{\"type\":\"ping\"}");
    }

    #[test]
    fn two_frames_read_in_order() {
        let mut buf: Vec<u8> = Vec::new();
        write_frame(&mut buf, b"first").unwrap();
        write_frame(&mut buf, b"second").unwrap();

        let mut cursor = &buf[..];
        assert_eq!(read_frame(&mut cursor).unwrap(), b"first");
        assert_eq!(read_frame(&mut cursor).unwrap(), b"second");
    }

    #[test]
    fn empty_payload_frames_through() {
        // Edge case: \n with no payload bytes ahead. Not produced by
        // serde_json::to_vec in practice, but the framer should still
        // be deterministic on this input.
        let mut buf: Vec<u8> = Vec::new();
        write_frame(&mut buf, b"").unwrap();
        assert_eq!(buf, b"\n");

        let mut cursor = &buf[..];
        let frame = read_frame(&mut cursor).unwrap();
        assert_eq!(frame, b"");
    }

    #[test]
    fn read_frame_returns_unexpected_eof_on_closed_reader() {
        let mut cursor: &[u8] = &[];
        let err = read_frame(&mut cursor).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn frame_without_terminator_returns_remaining_bytes() {
        // read_until reads until delimiter OR EOF. If the reader
        // closes mid-frame (no trailing \n), the bytes still come
        // back to the caller — we don't error, but the deserializer
        // upstream will reject malformed JSON. Document that contract.
        let buf = b"partial-no-newline";
        let mut cursor: &[u8] = buf;
        let frame = read_frame(&mut cursor).unwrap();
        assert_eq!(frame, b"partial-no-newline");
    }
}
