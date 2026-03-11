mod artifacts;
mod record_common;
mod records;
mod snapshots;
mod tasks;

use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum, ValueHint};

pub(crate) use artifacts::*;
pub(crate) use record_common::*;
pub(crate) use records::*;
pub(crate) use snapshots::*;
pub(crate) use tasks::*;

// ── value-enum helpers ──────────────────────────────────────

/// Valid task types for CLI arguments.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub(crate) enum TaskTypeArg {
    Task,
    Bug,
    Feature,
    Epic,
    Spike,
}

impl From<TaskTypeArg> for brain_lib::tasks::events::TaskType {
    fn from(arg: TaskTypeArg) -> Self {
        match arg {
            TaskTypeArg::Task => Self::Task,
            TaskTypeArg::Bug => Self::Bug,
            TaskTypeArg::Feature => Self::Feature,
            TaskTypeArg::Epic => Self::Epic,
            TaskTypeArg::Spike => Self::Spike,
        }
    }
}

/// Ranking intent profiles for hybrid search.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum Intent {
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
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Lookup => "lookup",
            Self::Planning => "planning",
            Self::Reflection => "reflection",
            Self::Synthesis => "synthesis",
        }
    }
}

// ── top-level CLI struct ────────────────────────────────────

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
        brain daemon install         Install as login service (launchd/systemd)\n  \
        brain mcp                    Start MCP server for agent integration\n\n\
        Use `brain <command> --help` for more details on each command."
)]
pub(crate) struct Cli {
    /// Path to a local BGE model directory (see README for download instructions)
    #[arg(
        long,
        global = true,
        env = "BRAIN_MODEL_DIR",
        default_value = "./.brain/models/bge-small-en-v1.5",
        value_hint = ValueHint::DirPath,
    )]
    pub(crate) model_dir: PathBuf,

    /// Path to the LanceDB database directory
    #[arg(
        long,
        global = true,
        env = "BRAIN_DB",
        default_value = "./.brain/lancedb",
        value_hint = ValueHint::DirPath,
    )]
    pub(crate) lance_db: PathBuf,

    /// Path to the SQLite control-plane database
    #[arg(
        long,
        global = true,
        env = "BRAIN_SQLITE_DB",
        default_value = "./.brain/brain.db",
        value_hint = ValueHint::FilePath,
    )]
    pub(crate) sqlite_db: PathBuf,

    /// Print version
    #[arg(short = 'v', long = "version", action = clap::ArgAction::Version)]
    version: (),

    #[command(subcommand)]
    pub(crate) command: Command,
}

// ── subcommands ─────────────────────────────────────────────

#[derive(Subcommand)]
pub(crate) enum Command {
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
            The daemon runs the watcher as a background process. It can be started \
            directly via fork/setsid (`start`/`stop`) or installed as a platform-native \
            service that auto-starts on login (`install`/`uninstall`).\n\n\
            State is tracked via a PID file at ~/.brain/brain.pid and logs are \
            written to ~/.brain/brain.log.",
        after_help = "EXAMPLES:\n  \
            brain daemon start           Start daemon in background\n  \
            brain daemon stop            Stop the running daemon\n  \
            brain daemon status          Check daemon and service status\n  \
            brain daemon install         Install as login service (launchd/systemd)\n  \
            brain daemon install --dry-run  Preview the service definition\n  \
            brain daemon uninstall       Remove the login service"
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
    Mcp {
        #[command(subcommand)]
        action: Option<McpAction>,
    },

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

    /// Regenerate AGENTS.md and bridge CLAUDE.md from the current brain config
    Docs,

    /// Show the brain ID for the current project (generates one if missing)
    Id,

    /// Agent utilities
    Agent {
        #[command(subcommand)]
        action: AgentAction,
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

    /// Initialize a new brain in the current directory
    Init {
        /// Brain name (defaults to directory name)
        #[arg(long)]
        name: Option<String>,

        /// Note directories to index (defaults to current directory)
        #[arg(long)]
        notes: Vec<PathBuf>,

        /// Skip generating AGENTS.md
        #[arg(long)]
        no_agents_md: bool,
    },

    /// List all registered brains
    #[command(visible_alias = "ls")]
    List,

    /// Remove a registered brain
    #[command(visible_alias = "rm")]
    Remove {
        /// Brain name to remove
        name: String,
        /// Also delete derived data (~/.brain/brains/<name>/)
        #[arg(long)]
        purge: bool,
    },

    /// Get or set brain configuration
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },

    /// Manage Claude Code hook integration
    Hooks {
        #[command(subcommand)]
        action: HooksAction,
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

    /// Manage snapshots (opaque state bundles)
    #[command(visible_alias = "snap")]
    Snapshots {
        /// Output as JSON
        #[arg(long, global = true)]
        json: bool,

        #[command(subcommand)]
        action: SnapshotsAction,
    },

    /// Manage artifacts (durable work products)
    #[command(visible_alias = "art")]
    Artifacts {
        /// Output as JSON
        #[arg(long, global = true)]
        json: bool,

        #[command(subcommand)]
        action: ArtifactsAction,
    },

    /// Manage records storage (verify, gc, evict, pin)
    Records {
        /// Output as JSON
        #[arg(long, global = true)]
        json: bool,

        #[command(subcommand)]
        action: RecordsAction,
    },
}

// ── non-task subcommand enums ───────────────────────────────

#[derive(Subcommand)]
pub(crate) enum ConfigAction {
    /// Set a configuration value
    Set {
        /// Configuration key (e.g. "prefix")
        key: String,

        /// Value to set (omit to auto-derive for prefix)
        value: Option<String>,
    },

    /// Get a configuration value
    Get {
        /// Configuration key (e.g. "prefix")
        key: String,
    },
}

#[derive(Subcommand)]
pub enum McpAction {
    /// Configure brain as an MCP server for an editor or agent framework
    Setup {
        /// Target to configure (claude, cursor, vscode)
        target: McpTarget,
        /// Print the config without writing it
        #[arg(long)]
        dry_run: bool,
    },
}

#[derive(Debug, Clone, ValueEnum)]
pub enum McpTarget {
    /// Claude Code (~/.claude/settings.json)
    Claude,
    /// Cursor (~/.cursor/mcp.json)
    Cursor,
    /// VS Code (.vscode/settings.json)
    Vscode,
}

#[derive(Subcommand)]
pub(crate) enum AgentAction {
    /// Output JSON Schema for all MCP tool definitions
    #[command(
        long_about = "Output the full JSON Schema for all MCP tool definitions.\n\n\
            Useful for understanding exact payload formats and validating MCP tool calls. \
            By default outputs all tools as a compact JSON array. Use --tool to filter to \
            a single tool and --pretty for human-readable formatting."
    )]
    Schema {
        /// Filter output to a single tool by name (e.g. "tasks.apply_event")
        #[arg(long)]
        tool: Option<String>,
        /// Pretty-print the JSON output
        #[arg(long)]
        pretty: bool,
    },
}

#[derive(Subcommand)]
pub(crate) enum HooksAction {
    /// Install brain hooks into Claude Code settings
    Install {
        /// Print the hook config without writing it
        #[arg(long)]
        dry_run: bool,
    },
    /// Show current hook status
    Status,
}

#[derive(Subcommand)]
pub(crate) enum DaemonAction {
    /// Start the daemon in the background
    #[command(long_about = "Start the daemon in the background.\n\n\
        Forks a child process, detaches it from the terminal via setsid, and \
        writes its PID to ~/.brain/brain.pid. The child process runs the watcher \
        loop, logging to ~/.brain/brain.log.\n\n\
        When no path is given, watches all brain projects registered in \
        ~/.brain/config.toml. When a path is given, watches only that directory \
        (legacy single-brain mode).")]
    Start {
        /// Path to notes directory. When omitted, watches all registered brains from the global registry.
        #[arg(value_hint = ValueHint::DirPath)]
        notes_path: Option<PathBuf>,
    },
    /// Stop the running daemon
    #[command(long_about = "Stop the running daemon.\n\n\
        Reads the PID from ~/.brain/brain.pid, sends SIGTERM, and waits up to \
        5 seconds for the process to exit. Cleans up the PID file afterward.")]
    Stop,
    /// Check if the daemon is running
    #[command(long_about = "Check daemon and service status.\n\n\
        Shows the PID-based daemon status and, if a platform service is \
        installed (launchd on macOS, systemd on Linux), its status as well.")]
    Status,
    /// Install as a login service (auto-starts on login)
    #[command(
        long_about = "Install the brain watcher as a platform-native login service.\n\n\
        On macOS: generates a launchd plist in ~/Library/LaunchAgents/ and loads it.\n\
        On Linux: generates a systemd user unit in ~/.config/systemd/user/ and enables it.\n\n\
        The service runs `brain watch` directly — the OS service manager handles \
        process lifecycle, restart-on-failure, and startup-on-login.\n\n\
        Must be run from a directory containing a .brain/brain.toml marker \
        (or use --brain-root to specify the brain project)."
    )]
    Install {
        /// Brain project root (defaults to discovering .brain/brain.toml from cwd)
        #[arg(long, value_hint = ValueHint::DirPath)]
        brain_root: Option<PathBuf>,
        /// Print the service definition without installing
        #[arg(long)]
        dry_run: bool,
    },
    /// Remove the login service
    #[command(long_about = "Uninstall the platform-native login service.\n\n\
        Stops and removes the launchd plist (macOS) or systemd unit (Linux) \
        that was created by `brain daemon install`.")]
    Uninstall {
        /// Brain project root (defaults to discovering .brain/brain.toml from cwd)
        #[arg(long, value_hint = ValueHint::DirPath)]
        brain_root: Option<PathBuf>,
    },
}
