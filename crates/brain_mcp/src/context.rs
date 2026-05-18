//! Reshaped MCP context — a thin client of `brain-daemon`.
//!
//! Holds nothing but the [`DaemonClient`] handle and the per-session
//! brain name. Storage and search live behind the wire in the daemon;
//! adding any owning resource here (a `BrainStores`, a `Store`, an
//! `Embedder`) is the anti-pattern the brain_mcp / brain_rpc
//! architecture ratchet prevents.

use std::sync::Arc;

use brain_rpc::{DaemonClient, RpcError, UnixSocketTransport};
use tokio::sync::{Mutex, RwLock};

/// Shared context for MCP tool handlers.
///
/// Production tool bodies access the daemon via [`McpContext::with_client`]
/// to obtain a mutable [`DaemonClient`] for the duration of one wire
/// request. The mutex serialises requests on the single underlying
/// `UnixSocketTransport`; one MCP harness session is inherently
/// sequential, so the mutex is not a bottleneck.
pub struct McpContext {
    pub client: Arc<Mutex<DaemonClient<UnixSocketTransport>>>,
    pub session_brain_name: Arc<RwLock<String>>,
}

impl McpContext {
    /// Construct a context from a connected [`DaemonClient`].
    pub fn new(client: DaemonClient<UnixSocketTransport>, startup_brain_name: String) -> Self {
        Self {
            client: Arc::new(Mutex::new(client)),
            session_brain_name: Arc::new(RwLock::new(startup_brain_name)),
        }
    }

    /// Run `f` against a locked [`DaemonClient`].
    ///
    /// The closure pattern keeps the lock scope minimal and avoids
    /// lock-across-await footguns — [`DaemonClient`] calls are sync.
    pub async fn with_client<F, T>(&self, f: F) -> Result<T, RpcError>
    where
        F: FnOnce(&mut DaemonClient<UnixSocketTransport>) -> Result<T, RpcError>,
    {
        let mut client = self.client.lock().await;
        f(&mut client)
    }

    /// Current session brain name (resolved from MCP `initialize`
    /// roots, or set at startup).
    pub async fn brain_name(&self) -> String {
        self.session_brain_name.read().await.clone()
    }

    /// Replace the session brain name.
    pub async fn set_brain_name(&self, name: String) {
        *self.session_brain_name.write().await = name;
    }
}
