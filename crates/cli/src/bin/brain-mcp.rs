//! `brain-mcp` binary — forwarder.
//!
//! The implementation lives in [`brain_mcp::run_cli`]. This file
//! exists so the `brain` cli crate produces a `brain-mcp` binary
//! alongside `brain` and `brain-daemon`, delivering all three from a
//! single `brew install benediktms/brain/brain` formula. The matching
//! `dist = false` opt-out in the `brain-mcp` crate's manifest keeps
//! cargo-dist from shipping the binary twice.

fn main() -> std::process::ExitCode {
    brain_mcp::run_cli()
}
