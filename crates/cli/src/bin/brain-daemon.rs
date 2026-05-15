//! `brain-daemon` binary — forwarder.
//!
//! The implementation lives in `brain_daemon::run_cli`. This file
//! exists so the `brain` cli crate produces a `brain-daemon` binary
//! alongside its main `brain` binary, which is what makes a single
//! `brew install benediktms/brain/brain` formula deliver both
//! executables.
//!
//! See `crates/brain_daemon/src/entry.rs` for the real entry point
//! and `crates/cli/Cargo.toml` for the `[[bin]]` declaration that
//! points cargo at this file.

fn main() -> std::process::ExitCode {
    brain_daemon::run_cli()
}
