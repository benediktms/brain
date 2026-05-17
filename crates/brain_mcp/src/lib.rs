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
//! Scaffold-only: this is the foundation for the brain_mcp extraction
//! (PR2 of brn-2fe.32 / brn-2fe.10). Module bodies land in subsequent
//! commits as the 51 MCP tool bodies migrate from `brain_lib::mcp::tools`
//! to a `DaemonClient`-backed implementation here.

// Empty for now — module declarations land alongside their content as
// the migration progresses.
