use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;

use crate::cli::*;
#[cfg(feature = "embed")]
use crate::commands::daemon::Daemon;

mod cli;
mod commands;
mod dispatch;
pub mod hooks;
pub mod markdown_table;

// Generated at build time by build.rs — contains git SHA when
// BRAIN_VERSION env var is set (i.e. via `just install`).
include!(concat!(env!("OUT_DIR"), "/version.rs"));

// ── config resolution ───────────────────────────────────────

/// If the user didn't pass explicit `--model-dir` / `--lance-db` / `--sqlite-db`
/// flags (i.e. all three are still at their clap defaults), resolve them from
/// `BRAIN_HOME` (or `~/.brain`).
///
/// Per the unified-DB invariant (single shared SQLite at `BRAIN_HOME/brain.db`),
/// the relative clap default `./.brain/...` is wrong for every command — it
/// would silently materialize an empty project-local DB whenever the subprocess
/// cwd lacked a `.brain/brain.toml` marker. The previous version gated this
/// resolution on a marker discovered by walking up from cwd, which made
/// registry commands (`list`, `alias`, `remove`) cwd-dependent: they hit the
/// global registry only when invoked from inside a brain-managed tree.
///
/// Resolution is now unconditional: if the user did not pass explicit flags,
/// the canonical home-relative paths are used.
fn resolve_defaults(cli: &mut Cli) {
    let default_model = PathBuf::from("./.brain/models/bge-small-en-v1.5");
    let default_lance = PathBuf::from("./.brain/lancedb");
    let default_sqlite = PathBuf::from("./.brain/brain.db");

    if cli.model_dir != default_model
        || cli.lance_db != default_lance
        || cli.sqlite_db != default_sqlite
    {
        return;
    }

    if let Ok(home) = brain_lib::config::brain_home() {
        cli.model_dir = home.join("models").join("bge-small-en-v1.5");
        cli.lance_db = home.join("lancedb");
        cli.sqlite_db = home.join("brain.db");
    }
}

// ── entry point ─────────────────────────────────────────────

fn main() -> Result<()> {
    let mut cli = Cli::parse();
    resolve_defaults(&mut cli);

    // Gated on `embed`: the forked `brain-daemon` child runs the watcher
    // supervisor, which only exists in embed builds. In `--no-default-features`
    // builds we fall through to `dispatch::async_main`, where the
    // `DaemonAction::Start` arm already bails with a clear "rebuild with
    // `embed`" message.
    #[cfg(feature = "embed")]
    if let Command::Daemon {
        action: DaemonAction::Start { notes_path, .. },
    } = &cli.command
    {
        let socket_path = crate::commands::rpc_client::default_socket_path()?;
        let daemon = Daemon::new()?;
        daemon.start(
            notes_path.as_deref(),
            &cli.sqlite_db,
            &cli.lance_db,
            &socket_path,
        )?;
        // The child process never reaches here — exec() replaces it with
        // brain-daemon. The parent exits via std::process::exit(0) inside
        // start(). This line is only reachable if start() returns an Err
        // (e.g. notes_path bail, lock contention), which propagates up.
    }

    // Fork must happen before the tokio runtime is created (forking a
    // multi-threaded process is undefined behaviour), so we use a plain
    // main function and build the runtime manually after the fork.
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(dispatch::async_main(cli))
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "embed")]
    use std::path::Path;

    use super::*;
    use clap::Parser;

    // ── Subcommand parsing ──────────────────────────────────────────

    #[cfg(feature = "embed")]
    #[test]
    fn parse_index() {
        let cli = Cli::try_parse_from(["brain", "index", "./notes"]).unwrap();
        assert!(
            matches!(cli.command, Command::Index { notes_path } if notes_path == Path::new("./notes"))
        );
    }

    #[cfg(feature = "embed")]
    #[test]
    fn parse_watch() {
        let cli = Cli::try_parse_from(["brain", "watch", "./notes"]).unwrap();
        assert!(
            matches!(cli.command, Command::Watch { notes_path } if notes_path == Some(PathBuf::from("./notes")))
        );
    }

    #[cfg(feature = "embed")]
    #[test]
    fn parse_watch_no_path() {
        let cli = Cli::try_parse_from(["brain", "watch"]).unwrap();
        assert!(matches!(cli.command, Command::Watch { notes_path: None }));
    }

    #[test]
    fn parse_daemon_start_no_args() {
        let cli = Cli::try_parse_from(["brain", "daemon", "start"]).unwrap();
        match cli.command {
            Command::Daemon {
                action: DaemonAction::Start { notes_path, .. },
            } => {
                assert!(
                    notes_path.is_none(),
                    "notes_path should be None when not provided"
                );
            }
            _ => panic!("expected Daemon Start"),
        }
    }

    #[test]
    fn parse_daemon_start_with_path() {
        let cli = Cli::try_parse_from(["brain", "daemon", "start", "./notes"]).unwrap();
        match cli.command {
            Command::Daemon {
                action: DaemonAction::Start { notes_path, .. },
            } => {
                assert_eq!(
                    notes_path,
                    Some(PathBuf::from("./notes")),
                    "notes_path should be Some when provided"
                );
            }
            _ => panic!("expected Daemon Start"),
        }
    }

    #[test]
    fn parse_daemon_stop() {
        let cli = Cli::try_parse_from(["brain", "daemon", "stop"]).unwrap();
        assert!(matches!(
            cli.command,
            Command::Daemon {
                action: DaemonAction::Stop
            }
        ));
    }

    #[test]
    fn parse_daemon_status() {
        let cli = Cli::try_parse_from(["brain", "daemon", "status"]).unwrap();
        assert!(matches!(
            cli.command,
            Command::Daemon {
                action: DaemonAction::Status
            }
        ));
    }

    #[test]
    fn parse_daemon_install() {
        let cli = Cli::try_parse_from(["brain", "daemon", "install"]).unwrap();
        match cli.command {
            Command::Daemon {
                action:
                    DaemonAction::Install {
                        brain_root,
                        dry_run,
                    },
            } => {
                assert!(brain_root.is_none());
                assert!(!dry_run);
            }
            _ => panic!("expected Daemon Install"),
        }
    }

    #[test]
    fn parse_daemon_install_dry_run() {
        let cli = Cli::try_parse_from(["brain", "daemon", "install", "--dry-run"]).unwrap();
        match cli.command {
            Command::Daemon {
                action: DaemonAction::Install { dry_run, .. },
            } => {
                assert!(dry_run);
            }
            _ => panic!("expected Daemon Install"),
        }
    }

    #[test]
    fn parse_daemon_install_with_root() {
        let cli = Cli::try_parse_from([
            "brain",
            "daemon",
            "install",
            "--brain-root",
            "/tmp/myproject",
        ])
        .unwrap();
        match cli.command {
            Command::Daemon {
                action: DaemonAction::Install { brain_root, .. },
            } => {
                assert_eq!(brain_root, Some(PathBuf::from("/tmp/myproject")));
            }
            _ => panic!("expected Daemon Install"),
        }
    }

    #[test]
    fn parse_daemon_uninstall() {
        let cli = Cli::try_parse_from(["brain", "daemon", "uninstall"]).unwrap();
        match cli.command {
            Command::Daemon {
                action: DaemonAction::Uninstall { brain_root },
            } => {
                assert!(brain_root.is_none());
            }
            _ => panic!("expected Daemon Uninstall"),
        }
    }

    #[test]
    fn parse_daemon_uninstall_with_root() {
        let cli = Cli::try_parse_from([
            "brain",
            "daemon",
            "uninstall",
            "--brain-root",
            "/tmp/myproject",
        ])
        .unwrap();
        match cli.command {
            Command::Daemon {
                action: DaemonAction::Uninstall { brain_root },
            } => {
                assert_eq!(brain_root, Some(PathBuf::from("/tmp/myproject")));
            }
            _ => panic!("expected Daemon Uninstall"),
        }
    }

    #[test]
    fn parse_mcp() {
        let cli = Cli::try_parse_from(["brain", "mcp"]).unwrap();
        assert!(matches!(cli.command, Command::Mcp { action: None }));
    }

    #[test]
    fn parse_mcp_setup_claude() {
        let cli = Cli::try_parse_from(["brain", "mcp", "setup", "claude"]).unwrap();
        assert!(matches!(
            cli.command,
            Command::Mcp {
                action: Some(McpAction::Setup { dry_run: false, .. })
            }
        ));
    }

    #[test]
    fn parse_mcp_setup_cursor_dry_run() {
        let cli = Cli::try_parse_from(["brain", "mcp", "setup", "cursor", "--dry-run"]).unwrap();
        assert!(matches!(
            cli.command,
            Command::Mcp {
                action: Some(McpAction::Setup { dry_run: true, .. })
            }
        ));
    }

    #[test]
    fn parse_mcp_setup_vscode() {
        let cli = Cli::try_parse_from(["brain", "mcp", "setup", "vscode"]).unwrap();
        assert!(matches!(
            cli.command,
            Command::Mcp {
                action: Some(McpAction::Setup { dry_run: false, .. })
            }
        ));
    }

    #[test]
    fn parse_plugin_install_defaults_to_claude() {
        let cli = Cli::try_parse_from(["brain", "plugin", "install"]).unwrap();
        assert!(matches!(
            cli.command,
            Command::Plugin {
                action: PluginAction::Install {
                    target: PluginTarget::Claude,
                }
            }
        ));
    }

    #[test]
    fn parse_plugin_install_target_claude_explicit() {
        // The explicit `--target claude` form must continue to parse so
        // existing scripts keep reaching the deprecation hint rather than
        // hitting a clap parse error. `let-else` is used instead of
        // `.expect` because `Cli` does not implement `Debug`.
        let Ok(cli) = Cli::try_parse_from(["brain", "plugin", "install", "--target", "claude"])
        else {
            panic!("--target claude must parse");
        };
        assert!(matches!(
            cli.command,
            Command::Plugin {
                action: PluginAction::Install {
                    target: PluginTarget::Claude,
                }
            }
        ));
    }

    #[test]
    fn parse_plugin_install_codex_target_rejected() {
        // The Codex variant was removed when Codex distribution was dropped;
        // anyone still passing `--target codex` should get a clap parse error
        // (specifically InvalidValue) rather than a silent fallback.
        let result = Cli::try_parse_from(["brain", "plugin", "install", "--target", "codex"]);
        let err = result
            .err()
            .expect("--target codex must be rejected at parse time");
        assert_eq!(err.kind(), clap::error::ErrorKind::InvalidValue);
    }

    #[test]
    fn parse_plugin_install_dry_run_flag_rejected() {
        // `--dry-run` was meaningful for the legacy file-copy installer.
        // Now that the command is a deprecation stub, the flag is removed
        // entirely so users aren't misled into thinking a preview exists.
        let result = Cli::try_parse_from(["brain", "plugin", "install", "--dry-run"]);
        let err = result
            .err()
            .expect("--dry-run on the deprecation stub must be rejected");
        assert!(matches!(
            err.kind(),
            clap::error::ErrorKind::UnknownArgument | clap::error::ErrorKind::TooManyValues
        ));
    }

    // ── Alias parsing ───────────────────────────────────────────────

    #[cfg(feature = "embed")]
    #[test]
    fn alias_idx() {
        let cli = Cli::try_parse_from(["brain", "idx", "./notes"]).unwrap();
        assert!(matches!(cli.command, Command::Index { .. }));
    }

    #[cfg(feature = "embed")]
    #[test]
    fn alias_w() {
        let cli = Cli::try_parse_from(["brain", "w", "./notes"]).unwrap();
        assert!(matches!(cli.command, Command::Watch { .. }));
    }

    #[test]
    fn alias_d() {
        let cli = Cli::try_parse_from(["brain", "d", "start"]).unwrap();
        assert!(matches!(
            cli.command,
            Command::Daemon {
                action: DaemonAction::Start { .. }
            }
        ));
    }

    // ── Global args ─────────────────────────────────────────────────

    #[test]
    fn global_args_override_defaults() {
        let cli = Cli::try_parse_from([
            "brain",
            "--model-dir",
            "/m",
            "--lance-db",
            "/l",
            "--sqlite-db",
            "/s",
            "mcp",
        ])
        .unwrap();
        assert_eq!(cli.model_dir, PathBuf::from("/m"));
        assert_eq!(cli.lance_db, PathBuf::from("/l"));
        assert_eq!(cli.sqlite_db, PathBuf::from("/s"));
    }

    #[test]
    fn global_args_have_defaults() {
        // Env vars (e.g. BRAIN_MODEL_DIR from justfile) may override clap
        // defaults, so we only assert the path suffixes are correct.
        let cli = Cli::try_parse_from(["brain", "mcp"]).unwrap();
        let model_str = cli.model_dir.to_string_lossy();
        let lance_str = cli.lance_db.to_string_lossy();
        let sqlite_str = cli.sqlite_db.to_string_lossy();
        assert!(
            model_str.ends_with("models/bge-small-en-v1.5"),
            "unexpected model_dir: {model_str}"
        );
        assert!(
            lance_str.ends_with("lancedb"),
            "unexpected lance_db: {lance_str}"
        );
        assert!(
            sqlite_str.ends_with("brain.db"),
            "unexpected sqlite_db: {sqlite_str}"
        );
    }

    // ── New command parsing ─────────────────────────────────────────

    #[cfg(feature = "embed")]
    #[test]
    fn parse_reindex_full() {
        let cli = Cli::try_parse_from(["brain", "reindex", "--full", "./notes"]).unwrap();
        match cli.command {
            Command::Reindex { full, file } => {
                assert_eq!(full, Some(PathBuf::from("./notes")));
                assert!(file.is_none());
            }
            _ => panic!("expected Reindex"),
        }
    }

    #[cfg(feature = "embed")]
    #[test]
    fn parse_reindex_file() {
        let cli = Cli::try_parse_from(["brain", "reindex", "--file", "test.md"]).unwrap();
        match cli.command {
            Command::Reindex { full, file } => {
                assert!(full.is_none());
                assert_eq!(file, Some(PathBuf::from("test.md")));
            }
            _ => panic!("expected Reindex"),
        }
    }

    #[cfg(feature = "embed")]
    #[test]
    fn parse_vacuum_defaults() {
        let cli = Cli::try_parse_from(["brain", "vacuum"]).unwrap();
        match cli.command {
            Command::Vacuum { older_than } => {
                assert_eq!(older_than, 30);
            }
            _ => panic!("expected Vacuum"),
        }
    }

    #[cfg(feature = "embed")]
    #[test]
    fn parse_vacuum_custom() {
        let cli = Cli::try_parse_from(["brain", "vacuum", "--older-than", "7"]).unwrap();
        match cli.command {
            Command::Vacuum { older_than } => {
                assert_eq!(older_than, 7);
            }
            _ => panic!("expected Vacuum"),
        }
    }

    #[cfg(feature = "embed")]
    #[test]
    fn parse_doctor() {
        let cli = Cli::try_parse_from(["brain", "doctor", "./notes"]).unwrap();
        match cli.command {
            Command::Doctor { notes_path } => {
                assert_eq!(notes_path, PathBuf::from("./notes"));
            }
            _ => panic!("expected Doctor"),
        }
    }

    #[cfg(feature = "embed")]
    #[test]
    fn parse_doctor_default_path() {
        let cli = Cli::try_parse_from(["brain", "doctor"]).unwrap();
        match cli.command {
            Command::Doctor { notes_path } => {
                assert_eq!(notes_path, PathBuf::from("."));
            }
            _ => panic!("expected Doctor"),
        }
    }

    // ── Convenience command parsing ────────────────────────────────

    #[test]
    fn parse_tasks_close() {
        let cli = Cli::try_parse_from([
            "brain",
            "tasks",
            "close",
            "--brain",
            "test-brain",
            "t1",
            "t2",
        ])
        .unwrap();
        match cli.command {
            Command::Tasks {
                action: TasksAction::Close { ids, .. },
                ..
            } => {
                assert_eq!(ids, vec!["t1", "t2"]);
            }
            _ => panic!("expected Tasks Close"),
        }
    }

    #[test]
    fn parse_tasks_close_requires_id() {
        assert!(Cli::try_parse_from(["brain", "tasks", "close", "--brain", "test-brain"]).is_err());
    }

    #[test]
    fn parse_tasks_ready() {
        let cli = Cli::try_parse_from(["brain", "tasks", "ready"]).unwrap();
        assert!(matches!(
            cli.command,
            Command::Tasks {
                action: TasksAction::Ready,
                ..
            }
        ));
    }

    #[test]
    fn parse_tasks_blocked() {
        let cli = Cli::try_parse_from(["brain", "tasks", "blocked"]).unwrap();
        assert!(matches!(
            cli.command,
            Command::Tasks {
                action: TasksAction::Blocked,
                ..
            }
        ));
    }

    #[test]
    fn parse_tasks_stats() {
        let cli = Cli::try_parse_from(["brain", "tasks", "stats"]).unwrap();
        assert!(matches!(
            cli.command,
            Command::Tasks {
                action: TasksAction::Stats,
                ..
            }
        ));
    }

    // ── Cross-brain task create parsing ────────────────────────────

    #[test]
    fn parse_tasks_create_with_brain() {
        let cli = Cli::try_parse_from([
            "brain", "tasks", "create", "--brain", "infra", "--title", "Fix CI",
        ])
        .unwrap();
        match cli.command {
            Command::Tasks {
                action: TasksAction::Create { brain, title, .. },
                ..
            } => {
                assert_eq!(brain, "infra");
                assert_eq!(title, "Fix CI");
            }
            _ => panic!("expected Tasks Create"),
        }
    }

    // ── Edge cases ──────────────────────────────────────────────────

    #[test]
    fn no_subcommand_is_error() {
        assert!(Cli::try_parse_from(["brain"]).is_err());
    }

    #[test]
    fn version_flag_triggers_error() {
        // clap reports --version as a DisplayVersion error
        match Cli::try_parse_from(["brain", "-v"]) {
            Err(e) => assert_eq!(e.kind(), clap::error::ErrorKind::DisplayVersion),
            Ok(_) => panic!("expected DisplayVersion error"),
        }
    }
}
