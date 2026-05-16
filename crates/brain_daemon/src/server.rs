//! `UnixSocketServer` — the production server adapter.
//!
//! # Hexagonal role
//!
//! This is the only module in the crate that touches concrete I/O —
//! `UnixListener`, `UnixStream`, thread spawning, framing reads/writes.
//! It wraps a [`Dispatcher`] (the port) with an accept loop, per-
//! connection request/response loop, and a shutdown handle. The
//! dispatcher itself sees only [`Request`] → [`Response`] / [`RpcError`].
//!
//! # Concurrency model
//!
//! One thread per accepted connection. Cheap on Unix; the daemon's
//! expected fan-out is small (a handful of long-lived clients —
//! `brain` CLI invocations, `brain-mcp` per Claude Code session).
//! Higher-scale designs (a worker pool, async runtime) can be swapped
//! in later by replacing this adapter; the dispatcher trait surface
//! stays the same.
//!
//! # Shutdown
//!
//! The accept loop polls a non-blocking listener every 50 ms and
//! exits when the shutdown flag is set. Per-connection threads check
//! the same flag between requests. This is a best-effort soft shutdown
//! — a connection blocked mid-`read_frame` waits for the client to
//! close. Production-grade signal handling (`SIGTERM` forcing close)
//! is deferred to a follow-up ticket.

#![cfg(unix)]

use std::io::BufReader;
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use brain_rpc::{Request, RpcError, read_frame, write_frame};

use crate::config::DaemonConfig;
use crate::dispatcher::Dispatcher;

/// Polling interval for the non-blocking accept loop. 50 ms strikes
/// a balance between shutdown responsiveness and CPU usage when idle.
const ACCEPT_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Handle that can request the server to shut down. Cheap to clone —
/// internally an [`Arc<AtomicBool>`]. Held by tests and (later) the
/// signal handler so they can ask the accept loop to exit cleanly.
#[derive(Clone)]
pub struct ShutdownHandle {
    flag: Arc<AtomicBool>,
}

impl ShutdownHandle {
    /// Signal the server to stop accepting new connections and exit
    /// its run loop. Existing per-connection threads finish their
    /// current request, then exit.
    pub fn request(&self) {
        self.flag.store(true, Ordering::SeqCst);
    }
}

/// Generic server adapter: takes a [`Dispatcher`], binds a Unix socket,
/// and runs an accept loop.
///
/// Construction: [`UnixSocketServer::bind`]. Get a shutdown handle
/// before moving the server into a thread via
/// [`UnixSocketServer::shutdown_handle`], then call
/// [`UnixSocketServer::run`] to drive the accept loop.
pub struct UnixSocketServer<D: Dispatcher + Send + Sync + 'static> {
    listener: UnixListener,
    dispatcher: Arc<D>,
    shutdown: Arc<AtomicBool>,
}

impl<D: Dispatcher + Send + Sync + 'static> UnixSocketServer<D> {
    /// Bind a Unix socket at `config.socket_path` and return a ready
    /// server. The listener is set non-blocking so `run` can poll the
    /// shutdown flag.
    pub fn bind(config: &DaemonConfig, dispatcher: D) -> Result<Self, RpcError> {
        let listener =
            UnixListener::bind(&config.socket_path).map_err(|e| RpcError::Transport {
                message: format!("bind({}): {e}", config.socket_path.display()),
            })?;
        listener
            .set_nonblocking(true)
            .map_err(|e| RpcError::Transport {
                message: format!("set_nonblocking: {e}"),
            })?;
        Ok(Self {
            listener,
            dispatcher: Arc::new(dispatcher),
            shutdown: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Return a cheaply-cloneable handle for triggering shutdown.
    pub fn shutdown_handle(&self) -> ShutdownHandle {
        ShutdownHandle {
            flag: Arc::clone(&self.shutdown),
        }
    }

    /// Run the accept loop until the shutdown flag is set.
    ///
    /// Per-connection work happens on spawned threads — accept() never
    /// blocks request handling. Errors on individual accepts are
    /// logged and the loop continues; one misbehaving client doesn't
    /// take the daemon down.
    pub fn run(&self) -> Result<(), RpcError> {
        while !self.shutdown.load(Ordering::SeqCst) {
            match self.listener.accept() {
                Ok((stream, _addr)) => {
                    let dispatcher = Arc::clone(&self.dispatcher);
                    let shutdown = Arc::clone(&self.shutdown);
                    thread::spawn(move || {
                        handle_connection(stream, dispatcher.as_ref(), &shutdown);
                    });
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(ACCEPT_POLL_INTERVAL);
                }
                Err(e) => {
                    // Log and continue — one bad accept shouldn't crash
                    // the daemon. Production deployments should pipe
                    // stderr to a log file.
                    eprintln!("brain-daemon: accept error (continuing): {e}");
                }
            }
        }
        Ok(())
    }
}

/// Per-connection loop. Reads requests, dispatches, writes responses
/// until EOF or shutdown.
fn handle_connection<D: Dispatcher + ?Sized>(
    stream: UnixStream,
    dispatcher: &D,
    shutdown: &Arc<AtomicBool>,
) {
    // The listener is non-blocking so the accept loop can poll the
    // shutdown flag. Accepted streams *inherit* that flag, but per-
    // connection reads must block — otherwise read_frame returns
    // EAGAIN on every quiet moment, the handler bails, and the client
    // sees a broken pipe on its next request. Reset to blocking here.
    if let Err(e) = stream.set_nonblocking(false) {
        eprintln!("brain-daemon: set_nonblocking(false) on accepted stream: {e}");
        return;
    }

    let reader_stream = match stream.try_clone() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("brain-daemon: try_clone failed: {e}");
            return;
        }
    };
    let mut reader = BufReader::new(reader_stream);
    let mut writer = stream;

    while !shutdown.load(Ordering::SeqCst) {
        // Read the next frame. EOF is the normal exit path — the
        // client closed the connection.
        let frame_bytes = match read_frame(&mut reader) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return;
            }
            Err(e) => {
                eprintln!("brain-daemon: read_frame error: {e}");
                return;
            }
        };

        let req: Request = match serde_json::from_slice(&frame_bytes) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("brain-daemon: deserialize request: {e}");
                return;
            }
        };

        match dispatcher.dispatch(req) {
            Ok(resp) => {
                let payload = match serde_json::to_vec(&resp) {
                    Ok(b) => b,
                    Err(e) => {
                        eprintln!("brain-daemon: serialize response: {e}");
                        return;
                    }
                };
                if let Err(e) = write_frame(&mut writer, &payload) {
                    eprintln!("brain-daemon: write_frame error: {e}");
                    return;
                }
            }
            Err(err) => {
                // The MVP wire protocol has no error envelope yet —
                // dispatcher errors aren't expected for the only two
                // current Request variants (Ping, Handshake), and a
                // future Response::Error variant or wire-level error
                // envelope is in scope for a follow-up ticket. For
                // now, log and close the connection so the client
                // sees an EOF instead of a wedged read.
                eprintln!("brain-daemon: dispatcher error (closing connection): {err}");
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shutdown_handle_sets_atomic_flag() {
        let flag = Arc::new(AtomicBool::new(false));
        let h = ShutdownHandle {
            flag: Arc::clone(&flag),
        };
        assert!(!flag.load(Ordering::SeqCst));
        h.request();
        assert!(flag.load(Ordering::SeqCst));
    }
}
