//! `brain-mcp` binary — forwarder.
//!
//! The implementation lives in `brain_mcp::run_cli`. This file
//! exists so the `brain` cli crate produces a `brain-mcp` binary
//! alongside its main `brain` and `brain-daemon` binaries, which is
//! what makes a single `brew install benediktms/brain/brain` formula
//! deliver all three executables.
//!
//! See `crates/brain_mcp/src/entry.rs` for the real entry point and
//! `crates/cli/Cargo.toml` for the `[[bin]]` declaration that points
//! cargo at this file.

fn main() -> std::process::ExitCode {
    brain_mcp::run_cli()
}
