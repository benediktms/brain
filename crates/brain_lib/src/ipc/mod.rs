/// IPC server for the brain daemon.
///
/// Provides a Unix Domain Socket (UDS) JSON-RPC 2.0 server that allows
/// multiple clients to communicate with the daemon concurrently.
pub mod client;
pub mod router;
pub mod server;
