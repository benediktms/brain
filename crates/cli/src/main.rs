use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

use crate::commands::daemon::Daemon;

mod commands;

#[derive(Parser)]
#[command(name = "brain", about = "Semantic search over your Markdown notes")]
struct Cli {
    /// Path to a local BGE model directory (run scripts/setup-model.sh to download)
    #[arg(
        long,
        global = true,
        env = "BRAIN_MODEL_DIR",
        default_value = "./.brain/models/bge-small-en-v1.5"
    )]
    model_dir: PathBuf,

    /// Path to the LanceDB database directory
    #[arg(
        long,
        global = true,
        env = "BRAIN_DB",
        default_value = "./.brain/lancedb"
    )]
    lance_db: PathBuf,

    /// Path to the SQLite control-plane database
    #[arg(
        long,
        global = true,
        env = "BRAIN_SQLITE_DB",
        default_value = "./.brain/brain.db"
    )]
    sqlite_db: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Index Markdown files from a directory into LanceDB
    Index {
        /// Path to the notes directory
        #[arg(default_value = ".")]
        notes_path: PathBuf,
    },

    /// Query indexed notes semantically
    Query {
        /// Natural language query
        query: String,

        /// Number of results to return
        #[arg(short, long, default_value = "5")]
        k: usize,
    },

    /// Watch a directory for changes and re-index incrementally
    Watch {
        /// Path to the notes directory
        #[arg(default_value = ".")]
        notes_path: PathBuf,
    },

    /// Manage the brain daemon (background watcher)
    Daemon {
        #[command(subcommand)]
        action: DaemonAction,
    },
}

#[derive(Subcommand)]
enum DaemonAction {
    /// Start the daemon in the background
    Start {
        /// Path to the notes directory
        #[arg(default_value = ".")]
        notes_path: PathBuf,
    },
    /// Stop the running daemon
    Stop,
    /// Check if the daemon is running
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
            commands::query::run(query, k, cli.model_dir, cli.lance_db).await?
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
    }

    Ok(())
}
