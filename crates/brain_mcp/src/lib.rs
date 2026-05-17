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
//! ## Status
//!
//! Foundation move in progress. The framework-free modules — protocol
//! and saga_validation — have landed; the 51 MCP tool bodies and the
//! McpContext reshape follow in later commits as the migration
//! progresses.

pub mod protocol;
pub mod saga_validation;
