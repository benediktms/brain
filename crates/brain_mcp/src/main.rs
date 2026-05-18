//! `brain-mcp` binary entry point.
//!
//! The actual entry-point logic lives in [`brain_mcp::run_cli`] so
//! the same binary can be produced from two places:
//!
//! - This crate's own `[[bin]]` target — used during development and
//!   for `cargo build -p brain-mcp`.
//! - The `brain` (cli) crate's sibling `[[bin]]` target — what
//!   cargo-dist actually ships in the Homebrew formula so users get
//!   all three binaries (brain, brain-daemon, brain-mcp) from a
//!   single `brew install brain` invocation.
//!
//! Both paths route to the same function.

fn main() -> std::process::ExitCode {
    brain_mcp::run_cli()
}
