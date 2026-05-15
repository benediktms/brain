//! brain_daemon — the singleton state-owner of the centralized-writer
//! architecture.
//!
//! # Role
//!
//! `brain_daemon` runs the long-lived RPC server that owns the writer
//! connection to SQLite (and, eventually, the file watcher, job
//! scheduler, and other persistent services). Thin clients (the `brain`
//! CLI, the `brain-mcp` MCP server) talk to it over a Unix socket using
//! the wire protocol defined by [`brain_rpc`].
//!
//! # MVP scope (this ticket)
//!
//! The MVP is intentionally small: a `brain-daemon` binary that listens
//! on a Unix socket and answers [`brain_rpc::Request::Ping`] +
//! [`brain_rpc::Request::Handshake`]. No DB access. No file watcher.
//! No signal handling. No real handlers backed by [`brain_lib`].
//!
//! See [`NOT_YET_IMPLEMENTED`] for the explicit list of deferred work
//! and which ticket each item lives in.
//!
//! # Hexagonal shape
//!
//! Mirroring [`brain_rpc`]'s structure:
//!
//! - `config` — [`DaemonConfig`] (pure data, no I/O).
//! - `dispatcher` — `Dispatcher` port: framework-free trait mapping
//!   [`brain_rpc::Request`] to [`brain_rpc::Response`]. Server-side
//!   counterpart of [`brain_rpc::Transport`] on the client side.
//! - `server` — `UnixSocketServer<D: Dispatcher>` adapter: concrete I/O,
//!   accept loop, per-connection request/response loop. Reuses
//!   [`brain_rpc::read_frame`] / [`brain_rpc::write_frame`] so the wire
//!   framing has exactly one implementation across both sides.
//!
//! # Architectural ratchet
//!
//! This crate's `[dependencies]` is locked to
//! `{brain-rpc, serde, serde_json, anyhow, thiserror}` until the
//! migration tickets land. NO `rusqlite`, `lancedb`, `candle`, or
//! `brain_*` domain crate.
//!
//! The port-layer files (`config.rs`, `dispatcher.rs`) must remain free
//! of concrete I/O imports (`std::io`, `std::os`, `std::process`,
//! `std::net`). Adapter files (`server.rs`, `main.rs`) are EXEMPT.
//!
//! These rules are enforced two ways:
//!
//! 1. `tests/architecture.rs` — programmatic gate running with
//!    `cargo test`.
//! 2. `just audit-daemon` — sub-second shell pipeline for pre-commit
//!    feedback.
//!
//! # NOT YET IMPLEMENTED — deferred to follow-up tickets
//!
//! - Migrating the existing daemon lifecycle code (~491 LOC) from
//!   `crates/cli/src/commands/daemon.rs`.
//! - Migrating launchd / systemd integration (~407 LOC) from
//!   `crates/cli/src/commands/daemon_service.rs`.
//! - Migrating the file watcher (~1,428 LOC) from
//!   `crates/cli/src/commands/watch.rs` — to land inside a future
//!   `services/watch` module here.
//! - Signal handling (`SIGTERM` graceful shutdown, `SIGHUP` config
//!   reload — see `brn-aba` for the unconditional-write-on-SIGHUP
//!   issue this will fix).
//! - Real `Request` handlers backed by `BrainStores` (depends on the
//!   existing daemon code migration landing first).
//! - Job scheduler (consolidation worker, etc.).
//! - Daemon startup / detachment / log rotation.
//! - `just install` recipe update — currently still builds the
//!   monolithic `brain` binary and runs the old daemon code path.
//!   Updated when `brn-2fe.27` makes the CLI actually shell out to
//!   `brain-daemon`.

// Module declarations are added story-by-story per the PRD at .omc/prd.json.
pub mod config;
pub mod dispatcher;
pub mod entry;

#[cfg(unix)]
pub mod server;

pub use config::DaemonConfig;
pub use dispatcher::{DefaultDispatcher, Dispatcher};
pub use entry::run_cli;

#[cfg(unix)]
pub use server::{ShutdownHandle, UnixSocketServer};
