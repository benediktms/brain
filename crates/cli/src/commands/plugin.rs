//! `brain plugin install/uninstall` — deprecation stubs.
//!
//! Plugin distribution moved to Claude Code's built-in marketplace flow.
//! Both subcommands now print a redirect hint and exit non-zero. The
//! `--target` flag is still parsed (`claude` is the only remaining value;
//! `codex` was dropped) so existing scripts surface a clear deprecation
//! error rather than a clap parse failure.
//!
//! TODO: remove the `brain plugin` subcommand surface entirely in v0.6.0
//! once the deprecation window has elapsed. Drop `PluginAction`,
//! `PluginTarget`, and the dispatcher arm at that point.

use anyhow::{Result, bail};

use crate::cli::{GITHUB_SOURCE, PluginTarget};

pub fn install(_target: PluginTarget) -> Result<()> {
    eprintln!("The brain plugin is now distributed via Claude Code's plugin marketplace.");
    eprintln!();
    eprintln!("Install it from inside Claude Code:");
    eprintln!("  /plugin marketplace add {GITHUB_SOURCE}");
    eprintln!("  /plugin install brain@brain");
    eprintln!();
    eprintln!("For local development against a brain checkout, point at the repo path:");
    eprintln!("  /plugin marketplace add /abs/path/to/brain");
    eprintln!("  /plugin install brain@brain");
    bail!("`brain plugin install` is deprecated; install via Claude Code");
}

pub fn uninstall(_target: PluginTarget) -> Result<()> {
    eprintln!("The brain plugin is now managed via Claude Code's plugin marketplace.");
    eprintln!();
    eprintln!("Remove it from inside Claude Code:");
    eprintln!("  /plugin uninstall brain@brain");
    eprintln!("  /plugin marketplace remove brain");
    bail!("`brain plugin uninstall` is deprecated; uninstall via Claude Code");
}
