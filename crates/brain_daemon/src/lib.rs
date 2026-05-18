//! brain_daemon — the new RPC server for the centralized-writer
//! architecture.
//!
//! # Role
//!
//! `brain_daemon` runs the long-lived RPC server that owns the writer
//! connection to SQLite, the file watcher, and (eventually) the job
//! scheduler. Thin clients (the `brain` CLI, the `brain-mcp` MCP
//! server) talk to it over a Unix socket using the wire protocol
//! defined by [`brain_rpc`].
//!
//! # Coexistence with the legacy `brain_lib::ipc::IpcServer`
//!
//! There is already a daemon in the workspace: `brain_lib::ipc::IpcServer`
//! is a JSON-RPC 2.0 server (routed via `BrainRouter`) that binds
//! `~/.brain/brain.sock` and is started by `brain watch`. The new
//! brain_daemon speaks a different wire format (newline-delimited
//! `brain_rpc::Request`), so the two daemons must NOT share a socket
//! path. Defaults:
//!
//! - Legacy IpcServer:   `~/.brain/brain.sock`        (JSON-RPC 2.0)
//! - brain_daemon (this): `~/.brain/brain-rpc.sock`   (newline-JSON)
//!
//! The migration plan is to retire the legacy IpcServer in a follow-up
//! ticket once enough commands have migrated to the new wire to make
//! it the obvious replacement. Until then, both daemons can run
//! side-by-side on their own sockets.
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
//! `brain-rpc` stays the wire contract and must never take on storage
//! or domain-crate deps. `brain_daemon`'s handler layer (`handlers.rs`)
//! is allowed to depend on `brain-lib`, `brain-persistence`, and domain
//! crates — but `rusqlite`, `lancedb`, and `candle` must NOT appear as
//! direct deps (they enter only transitively through `brain-lib`).
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
//! - Migrating the existing daemon lifecycle code from the cli crate's
//!   `daemon` command module.
//! - Migrating launchd / systemd integration from the cli crate's
//!   `daemon_service` command module.
//! - Signal handling (`SIGTERM` graceful shutdown, `SIGHUP` config
//!   reload — see the open daemon-bookkeeping-write issue this will
//!   fix).
//! - Full `Request` coverage. The first real handler (`TasksList` via
//!   `BrainStoresDispatcher`) lands here; other command families
//!   (records, sagas, memory, …) come in follow-up tickets, one
//!   vertical slice per ticket.
//! - Job scheduler (consolidation worker, etc.).
//! - Daemon startup / detachment / log rotation.
//! - `just install` recipe update — currently still builds the
//!   monolithic `brain` binary and runs the old in-process daemon
//!   code path. Updated when the CLI migrates to actually shell out
//!   to `brain-daemon`.

pub mod config;
pub mod dispatcher;
pub mod entry;
pub mod handlers;

#[cfg(unix)]
pub mod server;

#[cfg(feature = "embed")]
#[cfg(feature = "embed")]
pub mod watcher;

pub use config::DaemonConfig;
pub use dispatcher::{DefaultDispatcher, Dispatcher};
pub use entry::run_cli;
pub use handlers::BrainStoresDispatcher;

#[cfg(unix)]
pub use server::{ShutdownHandle, UnixSocketServer};
