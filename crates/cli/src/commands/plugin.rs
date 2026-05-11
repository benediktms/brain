//! `brain plugin install/uninstall` — deprecation stubs.
//!
//! Plugin distribution moved to Claude Code's built-in marketplace flow.
//! Both subcommands now print a redirect hint and exit non-zero. The
//! `--target` flag is still parsed (`claude` is the only remaining value;
//! `codex` was dropped) so existing scripts surface a clear deprecation
//! error rather than a clap parse failure.

use anyhow::{Result, bail};

use crate::cli::PluginTarget;

const INSTALL_REDIRECT: &str = "\
The brain plugin is now distributed via Claude Code's plugin marketplace.

Install it from inside Claude Code:
  /plugin marketplace add benediktms/brain
  /plugin install brain@brain

For local development against a brain checkout, point at the repo path:
  /plugin marketplace add /abs/path/to/brain
  /plugin install brain@brain";

const UNINSTALL_REDIRECT: &str = "\
The brain plugin is now managed via Claude Code's plugin marketplace.

Remove it from inside Claude Code:
  /plugin uninstall brain@brain
  /plugin marketplace remove brain";

pub fn install(_target: PluginTarget, _dry_run: bool) -> Result<()> {
    eprintln!("{INSTALL_REDIRECT}");
    bail!("`brain plugin install` is deprecated; install via Claude Code");
}

pub fn uninstall(_target: PluginTarget) -> Result<()> {
    eprintln!("{UNINSTALL_REDIRECT}");
    bail!("`brain plugin uninstall` is deprecated; uninstall via Claude Code");
}
