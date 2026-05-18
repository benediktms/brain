//! Dispatch transport selection.
//!
//! Production always uses [`DispatchMode::Daemon`], backed by
//! `UnixSocketTransport`. A `Local` variant gated behind
//! `cfg(any(test, feature = "test-utils"))` lands with the Phase E
//! test-harness migration and will hold an in-memory transport for
//! in-process unit testing. Until then this module exists primarily to
//! anchor the type so future migration can extend it without churning
//! call sites in `handle` / `server`.

use std::sync::Arc;

use brain_rpc::{DaemonClient, UnixSocketTransport};
use tokio::sync::{Mutex, RwLock};

/// Dispatch mode for the MCP server's `tools/call` path.
pub enum DispatchMode {
    /// Production: forward every typed request through the daemon over
    /// a Unix socket.
    Daemon {
        client: Arc<Mutex<DaemonClient<UnixSocketTransport>>>,
        session_brain_name: Arc<RwLock<String>>,
    },
    // A `Local { client: Arc<Mutex<DaemonClient<InMemoryTransport>>>, .. }`
    // variant gated on `cfg(any(test, feature = "test-utils"))` lands with
    // the Phase E test-harness migration. Keeping the enum in place now
    // avoids churning every call site at that point.
}
