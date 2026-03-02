use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand, ValueHint};
use tracing_subscriber::EnvFilter;

use crate::commands::daemon::Daemon;

mod commands;

#[derive(Parser)]
#[command(
    name = "brain",
    version, disable_version_flag = true,
    about = "A personal knowledge base with semantic search and task management",
    long_about = "A personal knowledge base with semantic search and task management.\n\n\
        brain indexes your Markdown notes into a vector database (LanceDB) using \
        BGE embeddings, then lets you search them with natural-language queries. \
        It also includes an event-sourced task system and an MCP server for \
        integration with AI coding agents.\n\n\
        Data is stored under .brain/ in the current directory by default:\n  \
        - .brain/lancedb/        Vector database\n  \
        - .brain/brain.db        SQLite control-plane & task store\n  \
        - .brain/models/         Embedding model weights",
    after_help = "EXAMPLES:\n  \
        brain index ./notes          Index a notes directory\n  \
        brain query \"async patterns\" Search for notes about async patterns\n  \
        brain watch ./notes          Watch and re-index on changes\n  \
        brain daemon start           Start background watcher\n  \
        brain mcp                    Start MCP server for agent integration\n\n\
        Use `brain <command> --help` for more details on each command."
)]
struct Cli {
    /// Path to a local BGE model directory (run scripts/setup-model.sh to download)
    #[arg(
        long,
        global = true,
        env = "BRAIN_MODEL_DIR",
        default_value = "./.brain/models/bge-small-en-v1.5",
        value_hint = ValueHint::DirPath,
    )]
    model_dir: PathBuf,

    /// Path to the LanceDB database directory
    #[arg(
        long,
        global = true,
        env = "BRAIN_DB",
        default_value = "./.brain/lancedb",
        value_hint = ValueHint::DirPath,
    )]
    lance_db: PathBuf,

    /// Path to the SQLite control-plane database
    #[arg(
        long,
        global = true,
        env = "BRAIN_SQLITE_DB",
        default_value = "./.brain/brain.db",
        value_hint = ValueHint::FilePath,
    )]
    sqlite_db: PathBuf,

    /// Print version
    #[arg(short = 'v', long = "version", action = clap::ArgAction::Version)]
    version: (),

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Index Markdown files from a directory into the vector database
    #[command(
        visible_alias = "idx",
        long_about = "Index Markdown files from a directory into the vector database.\n\n\
            Performs a full scan of the given directory, splits each Markdown file \
            into chunks, computes BGE embeddings, and upserts them into LanceDB. \
            Files that haven't changed since the last run (detected via content \
            hash) are skipped automatically."
    )]
    Index {
        /// Path to the notes directory
        #[arg(default_value = ".", value_hint = ValueHint::DirPath)]
        notes_path: PathBuf,
    },

    /// Query indexed notes using semantic search
    #[command(
        visible_alias = "q",
        long_about = "Query indexed notes using semantic search.\n\n\
            Embeds your query with the same BGE model used for indexing, retrieves \
            the top-k most similar chunks from LanceDB, and prints them enriched \
            with their source file path and nearest Markdown heading.",
        after_help = "EXAMPLES:\n  \
            brain query \"how does authentication work\"\n  \
            brain query \"async error handling\" -k 10\n  \
            brain query \"database migration steps\" -k 3"
    )]
    Query {
        /// Natural-language search query
        query: String,

        /// Maximum number of results to return
        #[arg(short, long, default_value = "5")]
        k: usize,
    },

    /// Watch a directory for changes and re-index incrementally
    #[command(
        visible_alias = "w",
        long_about = "Watch a directory for changes and re-index incrementally.\n\n\
            Performs an initial full index scan, then enters a filesystem event \
            loop that re-indexes only the files that changed. Press Ctrl+C to stop."
    )]
    Watch {
        /// Path to the notes directory
        #[arg(default_value = ".", value_hint = ValueHint::DirPath)]
        notes_path: PathBuf,
    },

    /// Manage the brain daemon (background watcher)
    #[command(
        visible_alias = "d",
        long_about = "Manage the brain daemon (background watcher).\n\n\
            The daemon runs the watcher as a background process using fork/setsid. \
            State is tracked via a PID file at ~/.brain/brain.pid and logs are \
            written to ~/.brain/brain.log."
    )]
    Daemon {
        #[command(subcommand)]
        action: DaemonAction,
    },

    /// Start the MCP server (stdin/stdout JSON-RPC for agent integration)
    #[command(long_about = "Start the MCP server (stdin/stdout JSON-RPC for agent integration).\n\n\
        Exposes brain's capabilities as MCP tools over stdin/stdout for use by \
        AI coding agents. Available tools:\n  \
        - memory_search_minimal  Search notes and return compact stubs\n  \
        - memory_expand          Expand memory stubs to full content\n  \
        - memory_write_episode   Record a goal/actions/outcome episode\n  \
        - memory_reflect         Retrieve source material for reflection\n  \
        - tasks_apply_event      Create or update tasks via event sourcing\n  \
        - tasks_next             Get the next highest-priority ready task(s)")]
    Mcp,
}

#[derive(Subcommand)]
enum DaemonAction {
    /// Start the daemon in the background
    #[command(long_about = "Start the daemon in the background.\n\n\
        Forks a child process, detaches it from the terminal via setsid, and \
        writes its PID to ~/.brain/brain.pid. The child process runs the watcher \
        loop, logging to ~/.brain/brain.log.")]
    Start {
        /// Path to the notes directory
        #[arg(default_value = ".", value_hint = ValueHint::DirPath)]
        notes_path: PathBuf,
    },
    /// Stop the running daemon
    #[command(long_about = "Stop the running daemon.\n\n\
        Reads the PID from ~/.brain/brain.pid, sends SIGTERM, and waits up to \
        5 seconds for the process to exit. Cleans up the PID file afterward.")]
    Stop,
    /// Check if the daemon is running
    #[command(long_about = "Check if the daemon is running.\n\n\
        Reads the PID from ~/.brain/brain.pid and checks whether the process is \
        alive. Cleans up stale PID files if the process is no longer running.")]
    Status,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    if let Command::Daemon {
        action: DaemonAction::Start { .. },
    } = &cli.command
    {
        let daemon = Daemon::new()?;
        daemon.start()?;
        // Only the child process reaches here — parent called exit(0).
    }

    // Fork must happen before the tokio runtime is created (forking a
    // multi-threaded process is undefined behaviour), so we use a plain
    // main function and build the runtime manually after the fork.
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async_main(cli))
}

async fn async_main(cli: Cli) -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .with_writer(std::io::stderr)
        .init();

    match cli.command {
        Command::Index { notes_path } => {
            commands::index::run(notes_path, cli.model_dir, cli.lance_db, cli.sqlite_db).await?
        }
        Command::Query { query, k } => {
            commands::query::run(query, k, cli.model_dir, cli.lance_db, cli.sqlite_db).await?
        }
        Command::Watch { notes_path } => {
            commands::watch::run(notes_path, cli.model_dir, cli.lance_db, cli.sqlite_db).await?
        }
        Command::Daemon { action } => {
            let daemon = Daemon::new()?;
            match action {
                DaemonAction::Start { notes_path } => {
                    // Child process after fork — run watch directly.
                    commands::watch::run(notes_path, cli.model_dir, cli.lance_db, cli.sqlite_db)
                        .await?;
                }
                DaemonAction::Stop => daemon.stop()?,
                DaemonAction::Status => daemon.status()?,
            }
        }
        Command::Mcp => {
            commands::mcp::run(cli.model_dir, cli.lance_db, cli.sqlite_db).await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    // ── Subcommand parsing ──────────────────────────────────────────

    #[test]
    fn parse_index() {
        let cli = Cli::try_parse_from(["brain", "index", "./notes"]).unwrap();
        assert!(matches!(cli.command, Command::Index { notes_path } if notes_path == PathBuf::from("./notes")));
    }

    #[test]
    fn parse_query_default_k() {
        let cli = Cli::try_parse_from(["brain", "query", "hello"]).unwrap();
        match cli.command {
            Command::Query { query, k } => {
                assert_eq!(query, "hello");
                assert_eq!(k, 5);
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn parse_query_custom_k() {
        let cli = Cli::try_parse_from(["brain", "query", "hello", "-k", "10"]).unwrap();
        match cli.command {
            Command::Query { query, k } => {
                assert_eq!(query, "hello");
                assert_eq!(k, 10);
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn parse_watch() {
        let cli = Cli::try_parse_from(["brain", "watch", "./notes"]).unwrap();
        assert!(matches!(cli.command, Command::Watch { notes_path } if notes_path == PathBuf::from("./notes")));
    }

    #[test]
    fn parse_daemon_start() {
        let cli = Cli::try_parse_from(["brain", "daemon", "start"]).unwrap();
        assert!(matches!(
            cli.command,
            Command::Daemon { action: DaemonAction::Start { .. } }
        ));
    }

    #[test]
    fn parse_daemon_stop() {
        let cli = Cli::try_parse_from(["brain", "daemon", "stop"]).unwrap();
        assert!(matches!(
            cli.command,
            Command::Daemon { action: DaemonAction::Stop }
        ));
    }

    #[test]
    fn parse_daemon_status() {
        let cli = Cli::try_parse_from(["brain", "daemon", "status"]).unwrap();
        assert!(matches!(
            cli.command,
            Command::Daemon { action: DaemonAction::Status }
        ));
    }

    #[test]
    fn parse_mcp() {
        let cli = Cli::try_parse_from(["brain", "mcp"]).unwrap();
        assert!(matches!(cli.command, Command::Mcp));
    }

    // ── Alias parsing ───────────────────────────────────────────────

    #[test]
    fn alias_idx() {
        let cli = Cli::try_parse_from(["brain", "idx", "./notes"]).unwrap();
        assert!(matches!(cli.command, Command::Index { .. }));
    }

    #[test]
    fn alias_q() {
        let cli = Cli::try_parse_from(["brain", "q", "hello"]).unwrap();
        assert!(matches!(cli.command, Command::Query { .. }));
    }

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
            Command::Daemon { action: DaemonAction::Start { .. } }
        ));
    }

    // ── Global args ─────────────────────────────────────────────────

    #[test]
    fn global_args_override_defaults() {
        let cli = Cli::try_parse_from([
            "brain",
            "--model-dir", "/m",
            "--lance-db", "/l",
            "--sqlite-db", "/s",
            "query", "x",
        ])
        .unwrap();
        assert_eq!(cli.model_dir, PathBuf::from("/m"));
        assert_eq!(cli.lance_db, PathBuf::from("/l"));
        assert_eq!(cli.sqlite_db, PathBuf::from("/s"));
    }

    #[test]
    fn global_args_have_defaults() {
        let cli = Cli::try_parse_from(["brain", "mcp"]).unwrap();
        assert_eq!(cli.model_dir, PathBuf::from("./.brain/models/bge-small-en-v1.5"));
        assert_eq!(cli.lance_db, PathBuf::from("./.brain/lancedb"));
        assert_eq!(cli.sqlite_db, PathBuf::from("./.brain/brain.db"));
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
