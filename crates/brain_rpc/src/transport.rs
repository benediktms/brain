//! `Transport` port — the abstract send-and-receive boundary.
//!
//! # Hexagonal role
//!
//! `Transport` is a port in the strict sense: a framework-free trait that
//! describes *what* the client needs (send a [`Request`], get a [`Response`])
//! without saying *how* (sockets? in-memory channels? stdio pipes?). Concrete
//! adapters implement this trait at the edge:
//!
//! - [`crate::unix::UnixSocketTransport`] (US-006) — the production adapter,
//!   newline-JSON framing over `UnixStream`.
//! - [`crate::testing::InMemoryTransport`] (US-004) — the test adapter, in-
//!   process function dispatch with zero I/O.
//!
//! Because the trait signature contains zero I/O types, `DaemonClient<T:
//! Transport>` is fully testable without any real socket — the swap-in for
//! integration tests is a one-line change.
//!
//! # Why call-shaped, not frame-shaped
//!
//! An alternative design exposes `send_frame(&[u8])` / `recv_frame() ->
//! Vec<u8>` at the port boundary and keeps serialization above. We
//! deliberately keep serialization *below* the port (inside each adapter)
//! because:
//!
//! 1. The wire format is an *adapter* choice. Different transports can use
//!    different framings (newline-JSON for Unix sockets today, length-prefix
//!    or protobuf for a future remote daemon) without changing the port.
//! 2. `InMemoryTransport` never has to serialize — it just maps
//!    `Request` -> `Response` in memory. Frame-level ports force a useless
//!    serialize/deserialize round trip in tests.
//!
//! # Example
//!
//! ```
//! use brain_rpc::{Request, Response, RpcError, Transport};
//!
//! struct AlwaysPong;
//! impl Transport for AlwaysPong {
//!     fn call(&mut self, req: Request) -> Result<Response, RpcError> {
//!         match req {
//!             Request::Ping => Ok(Response::Pong),
//!             Request::Handshake { .. } => Ok(Response::HandshakeOk {
//!                 server_version: brain_rpc::PROTOCOL_VERSION,
//!             }),
//!         }
//!     }
//! }
//! ```

use crate::domain::{Request, Response, RpcError};

/// Abstract transport for sending [`Request`]s and receiving [`Response`]s.
///
/// Implementations may be in-process (e.g. a test mock that maps
/// `Request -> Response` directly) or over a real wire (e.g. a Unix socket).
/// All concrete I/O — sockets, framing, serialization — lives inside the
/// implementor; the trait surface is intentionally I/O-free.
///
/// # Concurrency
///
/// The trait does not require `Send` or `Sync`. Callers needing concurrent
/// access should wrap the transport in their own synchronization (e.g.
/// `Mutex<Box<dyn Transport>>`). Adding marker bounds here would force every
/// implementor to pay for them.
///
/// # Errors
///
/// Implementors return [`RpcError`] for every failure mode. The error type is
/// serializable and source-chain-free by design (see [`RpcError`]); adapters
/// stringify any underlying `io::Error` / `serde_json::Error` before
/// constructing the [`RpcError::Transport`] / [`RpcError::Protocol`] variants.
pub trait Transport {
    /// Send `req` and block until a matching `Response` (or `RpcError`) is
    /// available. The exact framing, serialization, and I/O strategy are
    /// implementation details.
    fn call(&mut self, req: Request) -> Result<Response, RpcError>;
}
