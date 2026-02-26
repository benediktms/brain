use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

mod commands;

#[derive(Parser)]
#[command(name = "brain", about = "Semantic search over your Markdown notes")]
struct Cli {
    /// Path to a local BGE model directory (run scripts/setup-model.sh to download)
    #[arg(long, global = true, env = "BRAIN_MODEL_DIR", default_value = "./models/bge-small-en-v1.5")]
    model_dir: PathBuf,

    /// Path to the LanceDB database directory
    #[arg(long, global = true, env = "BRAIN_DB", default_value = "./brain_lancedb")]
    db: PathBuf,

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
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();

    match cli.command {
        Command::Index { notes_path } => {
            commands::index::run(notes_path, cli.model_dir, cli.db).await?
        }
        Command::Query { query, k } => {
            commands::query::run(query, k, cli.model_dir, cli.db).await?
        }
    }

    Ok(())
}
