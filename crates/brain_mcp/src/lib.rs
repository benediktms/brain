//! MCP-protocol-facing crate.
//!
//! `brain_mcp` is the JSON-RPC server that Claude Code / Codex MCP
//! harnesses talk to. It is a thin client of [`brain_rpc::DaemonClient`]:
//! tool bodies parse params, dispatch a typed RPC call to the daemon,
//! and shape the JSON envelope.
//!
//! No SQLite, no LanceDB, no embedder. The daemon is the sole owner of
//! every storage and search resource. This crate's `Cargo.toml` is
//! intentionally minimal — adding `rusqlite` or any `brain_<domain>`
//! crate here is the anti-pattern the saga-5df architecture gate
//! prevents.
//!
//! ## Module map
//!
//! - [`protocol`] — JSON-RPC 2.0 + MCP envelope types (framework-free).
//! - [`saga_validation`] — pure saga-arg validation helpers.
//! - [`context`] — reshaped [`McpContext`] (client + session brain).
//! - [`dispatch`] — [`DispatchMode`] (production `Daemon` mode now,
//!   `Local` test-only variant lands with Phase E).
//! - [`server`] — stdio loop ([`run_server`]).
//! - [`handle`] — per-request dispatcher (initialize / tools/list /
//!   tools/call).
//! - [`entry`] — library [`run_cli`] entry point for the future
//!   `brain-mcp` binary.
//!
//! ## Status
//!
//! Phase B/C scaffold landed. The 51 tool bodies, the in-memory test
//! harness, the production binary, and the deletion of
//! `brain_lib::mcp` follow in Phases D / E / F / G.

pub mod context;
pub mod dispatch;
pub mod entry;
pub mod handle;
pub mod protocol;
pub mod saga_validation;
pub mod server;

pub use context::McpContext;
pub use dispatch::DispatchMode;
pub use entry::run_cli;
pub use server::run_server;
