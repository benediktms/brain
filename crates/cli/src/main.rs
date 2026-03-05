use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum, ValueHint};
use tracing_subscriber::EnvFilter;

use crate::commands::daemon::Daemon;

mod commands;
pub mod markdown_table;

/// Ranking intent profiles for hybrid search.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Intent {
    /// Equal weights across all signals
    Auto,
    /// Keyword-heavy (40% BM25) for exact matches
    Lookup,
    /// Recency + links for project planning queries
    Planning,
    /// Recency-heavy for journal/reflection queries
    Reflection,
    /// Vector-heavy (40%) for semantic similarity
    Synthesis,
}

impl Intent {
    fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Lookup => "lookup",
            Self::Planning => "planning",
            Self::Reflection => "reflection",
            Self::Synthesis => "synthesis",
        }
    }
}

#[derive(Parser)]
#[command(
    name = "brain",
    version,
    disable_version_flag = true,
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

    /// Query indexed notes using hybrid search (vector + keyword + ranking)
    #[command(
        visible_alias = "q",
        long_about = "Query indexed notes using hybrid search.\n\n\
            Combines vector similarity (BGE embeddings) with BM25 keyword matching \
            and a 6-signal ranking engine (vector, keyword, recency, backlinks, \
            tags, importance). Use --intent to tune ranking for your retrieval goal.\n\n\
            Weight profiles:\n  \
            - auto      Equal weights across all signals (default)\n  \
            - lookup    Keyword-heavy (40% BM25) for exact matches\n  \
            - planning  Recency + links for project planning queries\n  \
            - reflection Recency-heavy for journal/reflection queries\n  \
            - synthesis  Vector-heavy (40%) for semantic similarity",
        after_help = "EXAMPLES:\n  \
            brain query \"how does authentication work\"\n  \
            brain query \"async error handling\" -k 10\n  \
            brain query -i lookup \"database migration steps\"\n  \
            brain query -i synthesis \"ownership and borrowing\" --verbose"
    )]
    Query {
        /// Natural-language search query
        query: String,

        /// Maximum number of results to return
        #[arg(short, long, default_value = "5")]
        k: usize,

        /// Ranking intent profile
        #[arg(short, long, default_value = "auto")]
        intent: Intent,

        /// Token budget for result packing
        #[arg(short, long, default_value = "800")]
        budget: usize,

        /// Show per-signal score breakdown for each result
        #[arg(short = 'V', long)]
        verbose: bool,
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
    #[command(
        long_about = "Start the MCP server (stdin/stdout JSON-RPC for agent integration).\n\n\
        Exposes brain's capabilities as MCP tools over stdin/stdout for use by \
        AI coding agents. Available tools:\n  \
        - memory_search_minimal  Search notes and return compact stubs\n  \
        - memory_expand          Expand memory stubs to full content\n  \
        - memory_write_episode   Record a goal/actions/outcome episode\n  \
        - memory_reflect         Retrieve source material for reflection\n  \
        - tasks_apply_event      Create or update tasks via event sourcing\n  \
        - tasks_next             Get the next highest-priority ready task(s)"
    )]
    Mcp,

    /// Force re-index files (clears content hashes, re-embeds everything)
    Reindex {
        /// Re-index all files in this directory
        #[arg(long, value_hint = ValueHint::DirPath)]
        full: Option<PathBuf>,

        /// Re-index a single file
        #[arg(long, value_hint = ValueHint::FilePath)]
        file: Option<PathBuf>,
    },

    /// Compact and reclaim space (SQLite VACUUM + LanceDB optimize + purge deleted)
    Vacuum {
        /// Purge soft-deleted files older than this many days
        #[arg(long, default_value = "30")]
        older_than: u32,
    },

    /// Run health checks on the index
    Doctor {
        /// Path to the notes directory
        #[arg(default_value = ".", value_hint = ValueHint::DirPath)]
        notes_path: PathBuf,
    },

    /// Import beads issues into the brain task system
    ImportBeads {
        /// Path to beads issues.jsonl (auto-discovers .beads/issues.jsonl if omitted)
        #[arg(long)]
        path: Option<PathBuf>,
        /// Preview without writing
        #[arg(long)]
        dry_run: bool,
    },

    /// Get or set brain configuration
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },

    /// Manage brain tasks
    #[command(visible_alias = "task")]
    Tasks {
        /// Output as JSON instead of human-readable text
        #[arg(long, global = true)]
        json: bool,

        /// Output as a markdown table (default for human-readable output)
        #[arg(long, global = true)]
        markdown: bool,

        #[command(subcommand)]
        action: TasksAction,
    },
}

#[derive(Subcommand)]
enum TasksAction {
    /// Create a new task
    Create {
        /// Task title
        #[arg(long)]
        title: String,

        /// Task description
        #[arg(long)]
        description: Option<String>,

        /// Priority (0=critical, 1=high, 2=medium, 3=low, 4=backlog)
        #[arg(long, default_value = "2")]
        priority: i32,

        /// Task type (e.g. task, bug, feature)
        #[arg(long, value_name = "TYPE", default_value = "task")]
        task_type: String,

        /// Assignee
        #[arg(long)]
        assignee: Option<String>,

        /// Parent task ID
        #[arg(long)]
        parent: Option<String>,
    },

    /// List tasks with optional filters
    List {
        /// Filter by status (open, in_progress, blocked, done, cancelled)
        #[arg(long)]
        status: Option<String>,

        /// Filter by priority (0-4)
        #[arg(long)]
        priority: Option<i32>,

        /// Filter by task type
        #[arg(long, value_name = "TYPE")]
        task_type: Option<String>,

        /// Filter by assignee
        #[arg(long)]
        assignee: Option<String>,

        /// Show only ready tasks (no blockers)
        #[arg(long)]
        ready: bool,

        /// Show only blocked tasks
        #[arg(long)]
        blocked: bool,
    },

    /// Show details for a specific task
    Show {
        /// Task ID
        id: String,
    },

    /// Update a task's fields or status
    Update {
        /// Task ID
        id: String,

        /// New title
        #[arg(long)]
        title: Option<String>,

        /// New description
        #[arg(long)]
        description: Option<String>,

        /// New status (open, in_progress, blocked, done, cancelled)
        #[arg(long)]
        status: Option<String>,

        /// New priority (0-4)
        #[arg(long)]
        priority: Option<i32>,

        /// New task type
        #[arg(long, value_name = "TYPE")]
        task_type: Option<String>,

        /// New assignee
        #[arg(long)]
        assignee: Option<String>,

        /// Set blocked reason
        #[arg(long)]
        blocked_reason: Option<String>,
    },

    /// Manage task dependencies
    Dep {
        #[command(subcommand)]
        action: DepAction,
    },

    /// Link a note (chunk) to a task
    Link {
        /// Task ID
        task_id: String,

        /// Chunk ID to link
        chunk_id: String,
    },

    /// Unlink a note (chunk) from a task
    Unlink {
        /// Task ID
        task_id: String,

        /// Chunk ID to unlink
        chunk_id: String,
    },

    /// Add a comment to a task
    Comment {
        /// Task ID
        task_id: String,

        /// Comment body
        body: String,
    },

    /// Manage task labels
    Label {
        #[command(subcommand)]
        action: LabelAction,
    },

    /// Export tasks to a file format (defaults to markdown)
    Export {
        /// Output format (currently only "markdown" is supported)
        #[arg(default_value = "markdown")]
        format: String,

        /// Output directory
        #[arg(long, default_value = ".brain/tasks/projections")]
        dir: PathBuf,
    },

    /// Close one or more tasks (shorthand for update --status done)
    Close {
        /// Task IDs to close
        #[arg(required = true)]
        ids: Vec<String>,
    },

    /// Show ready tasks (no blockers)
    Ready,

    /// Show blocked tasks
    Blocked,

    /// Show project task statistics
    Stats,
}

#[derive(Subcommand)]
enum DepAction {
    /// Add a dependency (task depends on another)
    Add {
        /// Task that has the dependency
        task_id: String,

        /// Task it depends on
        depends_on: String,
    },

    /// Remove a dependency
    Remove {
        /// Task that has the dependency
        task_id: String,

        /// Task it depended on
        depends_on: String,
    },
}

#[derive(Subcommand)]
enum LabelAction {
    /// Add a label to a task
    Add {
        /// Task ID
        task_id: String,

        /// Label to add
        label: String,
    },

    /// Remove a label from a task
    Remove {
        /// Task ID
        task_id: String,

        /// Label to remove
        label: String,
    },
}

#[derive(Subcommand)]
enum ConfigAction {
    /// Set a configuration value
    Set {
        /// Configuration key (e.g. "prefix")
        key: String,

        /// Value to set
        value: String,
    },

    /// Get a configuration value
    Get {
        /// Configuration key (e.g. "prefix")
        key: String,
    },
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
    let env_filter = EnvFilter::from_default_env().add_directive("info".parse()?);
    let use_json = std::env::var("BRAIN_LOG_FORMAT")
        .map(|v| v.eq_ignore_ascii_case("json"))
        .unwrap_or(false);

    if use_json {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_writer(std::io::stderr)
            .json()
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_writer(std::io::stderr)
            .init();
    }

    match cli.command {
        Command::Index { notes_path } => {
            commands::index::run(notes_path, cli.model_dir, cli.lance_db, cli.sqlite_db).await?
        }
        Command::Query {
            query,
            k,
            intent,
            budget,
            verbose,
        } => {
            commands::query::run(commands::query::QueryParams {
                query,
                top_k: k,
                intent: intent.as_str().to_string(),
                budget,
                verbose,
                model_dir: cli.model_dir,
                db_path: cli.lance_db,
                sqlite_path: cli.sqlite_db,
            })
            .await?
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
        Command::Reindex { full, file } => match (full, file) {
            (Some(notes_path), None) => {
                commands::reindex::run_full(notes_path, cli.model_dir, cli.lance_db, cli.sqlite_db)
                    .await?
            }
            (None, Some(file_path)) => {
                commands::reindex::run_file(file_path, cli.model_dir, cli.lance_db, cli.sqlite_db)
                    .await?
            }
            (Some(_), Some(_)) => {
                anyhow::bail!("Cannot specify both --full and --file");
            }
            (None, None) => {
                anyhow::bail!("Must specify either --full <path> or --file <path>");
            }
        },
        Command::Vacuum { older_than } => {
            commands::vacuum::run(cli.model_dir, cli.lance_db, cli.sqlite_db, older_than).await?
        }
        Command::Doctor { notes_path } => {
            commands::doctor::run(notes_path, cli.model_dir, cli.lance_db, cli.sqlite_db).await?
        }
        Command::Mcp => {
            commands::mcp::run(cli.model_dir, cli.lance_db, cli.sqlite_db).await?;
        }
        Command::ImportBeads { path, dry_run } => {
            commands::import_beads::run(path, cli.sqlite_db, dry_run)?;
        }
        Command::Config { action } => {
            let db = brain_lib::db::Db::open(&cli.sqlite_db)?;
            db.with_write_conn(|conn| match action {
                ConfigAction::Set { key, value } => match key.as_str() {
                    "prefix" => {
                        let upper = value.to_ascii_uppercase();
                        if upper.len() != 3 || !upper.chars().all(|c| c.is_ascii_uppercase()) {
                            return Err(brain_lib::error::BrainCoreError::Config(format!(
                                "prefix must be exactly 3 uppercase ASCII letters, got: {value}"
                            )));
                        }
                        brain_lib::db::meta::set_meta(conn, "project_prefix", &upper)?;
                        println!("Set project prefix to {upper}");
                        Ok(())
                    }
                    other => Err(brain_lib::error::BrainCoreError::Config(format!(
                        "unknown config key: {other}. Known keys: prefix"
                    ))),
                },
                ConfigAction::Get { key } => match key.as_str() {
                    "prefix" => {
                        let brain_dir = cli.sqlite_db.parent().unwrap_or(std::path::Path::new("."));
                        let prefix =
                            brain_lib::db::meta::get_or_init_project_prefix(conn, brain_dir)?;
                        println!("{prefix}");
                        Ok(())
                    }
                    other => Err(brain_lib::error::BrainCoreError::Config(format!(
                        "unknown config key: {other}. Known keys: prefix"
                    ))),
                },
            })?;
        }
        Command::Tasks {
            json,
            markdown: _,
            action,
        } => {
            use commands::tasks::run::{CreateParams, ListParams, TaskCtx, UpdateParams};
            let ctx = TaskCtx::new(&cli.sqlite_db, json)?;

            match action {
                TasksAction::Create {
                    title,
                    description,
                    priority,
                    task_type,
                    assignee,
                    parent,
                } => {
                    commands::tasks::run::create(
                        &ctx,
                        CreateParams {
                            title,
                            description,
                            priority,
                            task_type,
                            assignee,
                            parent,
                        },
                    )?;
                }
                TasksAction::List {
                    status,
                    priority,
                    task_type,
                    assignee,
                    ready,
                    blocked,
                } => {
                    commands::tasks::run::list(
                        &ctx,
                        &ListParams {
                            status,
                            priority,
                            task_type,
                            assignee,
                            ready,
                            blocked,
                        },
                    )?;
                }
                TasksAction::Show { id } => {
                    commands::tasks::run::show(&ctx, &id)?;
                }
                TasksAction::Update {
                    id,
                    title,
                    description,
                    status,
                    priority,
                    task_type,
                    assignee,
                    blocked_reason,
                } => {
                    commands::tasks::run::update(
                        &ctx,
                        UpdateParams {
                            id,
                            title,
                            description,
                            status,
                            priority,
                            task_type,
                            assignee,
                            blocked_reason,
                        },
                    )?;
                }
                TasksAction::Dep { action } => match action {
                    DepAction::Add {
                        task_id,
                        depends_on,
                    } => {
                        commands::tasks::run::dep_add(&ctx, &task_id, &depends_on)?;
                    }
                    DepAction::Remove {
                        task_id,
                        depends_on,
                    } => {
                        commands::tasks::run::dep_remove(&ctx, &task_id, &depends_on)?;
                    }
                },
                TasksAction::Link { task_id, chunk_id } => {
                    commands::tasks::run::link(&ctx, &task_id, &chunk_id)?;
                }
                TasksAction::Unlink { task_id, chunk_id } => {
                    commands::tasks::run::unlink(&ctx, &task_id, &chunk_id)?;
                }
                TasksAction::Comment { task_id, body } => {
                    commands::tasks::run::comment(&ctx, &task_id, &body)?;
                }
                TasksAction::Label { action } => match action {
                    LabelAction::Add { task_id, label } => {
                        commands::tasks::run::label_add(&ctx, &task_id, &label)?;
                    }
                    LabelAction::Remove { task_id, label } => {
                        commands::tasks::run::label_remove(&ctx, &task_id, &label)?;
                    }
                },
                TasksAction::Export { format, dir } => match format.as_str() {
                    "markdown" | "md" => {
                        commands::tasks::export_markdown::run(dir, cli.sqlite_db)?;
                    }
                    other => {
                        anyhow::bail!("Unknown export format: {other}. Supported: markdown");
                    }
                },
                TasksAction::Close { ids } => {
                    commands::tasks::run::close(&ctx, &ids)?;
                }
                TasksAction::Ready => {
                    commands::tasks::run::list(
                        &ctx,
                        &ListParams {
                            status: None,
                            priority: None,
                            task_type: None,
                            assignee: None,
                            ready: true,
                            blocked: false,
                        },
                    )?;
                }
                TasksAction::Blocked => {
                    commands::tasks::run::list(
                        &ctx,
                        &ListParams {
                            status: None,
                            priority: None,
                            task_type: None,
                            assignee: None,
                            ready: false,
                            blocked: true,
                        },
                    )?;
                }
                TasksAction::Stats => {
                    commands::tasks::run::stats(&ctx)?;
                }
            }
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
        assert!(
            matches!(cli.command, Command::Index { notes_path } if notes_path == PathBuf::from("./notes"))
        );
    }

    #[test]
    fn parse_query_default_k() {
        let cli = Cli::try_parse_from(["brain", "query", "hello"]).unwrap();
        match cli.command {
            Command::Query {
                query,
                k,
                intent,
                budget,
                verbose,
            } => {
                assert_eq!(query, "hello");
                assert_eq!(k, 5);
                assert_eq!(intent, Intent::Auto);
                assert_eq!(budget, 800);
                assert!(!verbose);
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn parse_query_custom_k() {
        let cli = Cli::try_parse_from(["brain", "query", "hello", "-k", "10"]).unwrap();
        match cli.command {
            Command::Query { query, k, .. } => {
                assert_eq!(query, "hello");
                assert_eq!(k, 10);
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn parse_query_with_intent() {
        let cli = Cli::try_parse_from(["brain", "query", "-i", "lookup", "hello"]).unwrap();
        match cli.command {
            Command::Query { query, intent, .. } => {
                assert_eq!(query, "hello");
                assert_eq!(intent, Intent::Lookup);
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn parse_query_invalid_intent_rejected() {
        let result = Cli::try_parse_from(["brain", "query", "-i", "bogus", "hello"]);
        assert!(result.is_err(), "invalid intent should be rejected");
    }

    #[test]
    fn parse_watch() {
        let cli = Cli::try_parse_from(["brain", "watch", "./notes"]).unwrap();
        assert!(
            matches!(cli.command, Command::Watch { notes_path } if notes_path == PathBuf::from("./notes"))
        );
    }

    #[test]
    fn parse_daemon_start() {
        let cli = Cli::try_parse_from(["brain", "daemon", "start"]).unwrap();
        assert!(matches!(
            cli.command,
            Command::Daemon {
                action: DaemonAction::Start { .. }
            }
        ));
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
            "query",
            "x",
        ])
        .unwrap();
        assert_eq!(cli.model_dir, PathBuf::from("/m"));
        assert_eq!(cli.lance_db, PathBuf::from("/l"));
        assert_eq!(cli.sqlite_db, PathBuf::from("/s"));
    }

    #[test]
    fn global_args_have_defaults() {
        let cli = Cli::try_parse_from(["brain", "mcp"]).unwrap();
        assert_eq!(
            cli.model_dir,
            PathBuf::from("./.brain/models/bge-small-en-v1.5")
        );
        assert_eq!(cli.lance_db, PathBuf::from("./.brain/lancedb"));
        assert_eq!(cli.sqlite_db, PathBuf::from("./.brain/brain.db"));
    }

    // ── Edge cases ──────────────────────────────────────────────────

    // ── New command parsing ─────────────────────────────────────────

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
        let cli = Cli::try_parse_from(["brain", "tasks", "close", "t1", "t2"]).unwrap();
        match cli.command {
            Command::Tasks {
                action: TasksAction::Close { ids },
                ..
            } => {
                assert_eq!(ids, vec!["t1", "t2"]);
            }
            _ => panic!("expected Tasks Close"),
        }
    }

    #[test]
    fn parse_tasks_close_requires_id() {
        assert!(Cli::try_parse_from(["brain", "tasks", "close"]).is_err());
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
