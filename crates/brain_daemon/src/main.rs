//! `brain-daemon` binary entry point.
//!
//! The actual entry-point logic lives in [`brain_daemon::run_cli`] so
//! the same binary can be produced from two places:
//!
//! - This crate's own `[[bin]]` target — used during development and
//!   for `cargo build -p brain-daemon`.
//! - The `brain` (cli) crate's sibling `[[bin]]` target — what
//!   cargo-dist actually ships in the Homebrew formula so users get
//!   both binaries in a single `brew install brain` invocation.
//!
//! Both paths route to the same function. See `cli/src/bin/brain-daemon.rs`
//! for the forwarder.

fn main() -> std::process::ExitCode {
    brain_daemon::run_cli()
}
