use std::path::PathBuf;

use clap::Subcommand;

use super::TaskTypeArg;

// ── task subcommands ────────────────────────────────────────

#[derive(Subcommand)]
pub(crate) enum TasksAction {
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

        /// Task type (task, bug, feature, epic, spike)
        #[arg(long, value_name = "TYPE", default_value = "task")]
        task_type: TaskTypeArg,

        /// Assignee
        #[arg(long)]
        assignee: Option<String>,

        /// Parent task ID
        #[arg(long)]
        parent: Option<String>,

        /// Target brain name or ID
        #[arg(long)]
        brain: Option<String>,
    },

    /// List tasks with optional filters
    List {
        /// Filter by status (open, in_progress, blocked, done, cancelled)
        #[arg(long)]
        status: Option<String>,

        /// Filter by priority (0-4)
        #[arg(long)]
        priority: Option<i32>,

        /// Filter by task type (task, bug, feature, epic, spike)
        #[arg(long, value_name = "TYPE")]
        task_type: Option<TaskTypeArg>,

        /// Filter by assignee
        #[arg(long)]
        assignee: Option<String>,

        /// Filter by label (exact match)
        #[arg(long)]
        label: Option<String>,

        /// Full-text search on title and description
        #[arg(long)]
        search: Option<String>,

        /// Show only ready tasks (no blockers)
        #[arg(long)]
        ready: bool,

        /// Show only blocked tasks
        #[arg(long)]
        blocked: bool,

        /// Include task descriptions in JSON output (omitted by default)
        #[arg(long)]
        include_description: bool,

        /// Group output by a field (currently supports: label)
        #[arg(long)]
        group_by: Option<String>,

        /// Target brain name or ID (lists tasks from that brain)
        #[arg(long)]
        brain: Option<String>,
    },

    /// Show details for a specific task
    Show {
        /// Task ID
        id: String,

        /// Target brain name or ID (fetches from that brain instead of locally)
        #[arg(long)]
        brain: Option<String>,
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

        /// New task type (task, bug, feature, epic, spike)
        #[arg(long, value_name = "TYPE")]
        task_type: Option<TaskTypeArg>,

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

    /// Manage external references (GitHub issues, URLs, etc.)
    ExtLink {
        #[command(subcommand)]
        action: ExtLinkAction,
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

        /// Target brain name or ID (closes tasks in that brain instead of locally)
        #[arg(long)]
        brain: Option<String>,
    },

    /// Show ready tasks (no blockers)
    Ready,

    /// Get the next highest-priority actionable tasks
    #[command(
        long_about = "Get the next highest-priority actionable tasks.\n\n\
            Returns tasks with no unresolved dependencies, sorted by:\n  \
            1. Status (in-progress tasks promoted to top)\n  \
            2. Priority (0=critical first)\n  \
            3. Due date (earliest first)\n\n\
            Epics are excluded — only leaf tasks are shown. Results are \
            grouped by parent epic when applicable.\n\n\
            Use this for \"what should I work on next?\" queries.",
        after_help = "EXAMPLES:\n  \
            brain tasks next          # Top 5 actionable tasks\n  \
            brain tasks next -k 10    # Top 10 actionable tasks"
    )]
    Next {
        /// Maximum number of tasks to show
        #[arg(short, long, default_value = "5")]
        k: usize,
    },

    /// Show blocked tasks
    Blocked,

    /// Show project task statistics
    Stats,

    /// List all labels with counts
    Labels,

    /// Transfer a task to a different brain
    Transfer {
        /// Task ID to transfer (full ID or short hash)
        task_id: String,

        /// Target brain (name, brain_id, or alias)
        #[arg(long, required = true, value_name = "BRAIN")]
        to: String,

        /// Print what would happen without making any changes
        #[arg(long)]
        dry_run: bool,
    },
}

// ── dependency subcommands ──────────────────────────────────

#[derive(Subcommand)]
pub(crate) enum DepAction {
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

    /// Create a sequential dependency chain (each task depends on the previous)
    AddChain {
        /// Task IDs in order (at least 2)
        #[arg(required = true)]
        task_ids: Vec<String>,
    },

    /// Make multiple tasks depend on a single source task
    AddFan {
        /// Source task (the one others depend on)
        source: String,

        /// Tasks that depend on the source (comma-separated)
        #[arg(required = true, value_delimiter = ',')]
        dependents: Vec<String>,
    },

    /// Remove all dependencies for a task
    Clear {
        /// Task ID
        task_id: String,
    },
}

// ── label subcommands ───────────────────────────────────────

#[derive(Subcommand)]
pub(crate) enum LabelAction {
    /// Add a label to a task
    Add {
        /// Task ID
        task_id: String,

        /// Label to add
        label: String,

        /// Target brain name or ID (adds label in that brain instead of locally)
        #[arg(long)]
        brain: Option<String>,
    },

    /// Remove a label from a task
    Remove {
        /// Task ID
        task_id: String,

        /// Label to remove
        label: String,

        /// Target brain name or ID (removes label in that brain instead of locally)
        #[arg(long)]
        brain: Option<String>,
    },

    /// Add a label to multiple tasks at once
    BatchAdd {
        /// Comma-separated task IDs
        #[arg(long, value_delimiter = ',')]
        tasks: Vec<String>,

        /// Label to add
        label: String,

        /// Target brain name or ID (adds labels in that brain instead of locally)
        #[arg(long)]
        brain: Option<String>,
    },

    /// Remove a label from multiple tasks at once
    BatchRemove {
        /// Comma-separated task IDs
        #[arg(long, value_delimiter = ',')]
        tasks: Vec<String>,

        /// Label to remove
        label: String,

        /// Target brain name or ID (removes labels in that brain instead of locally)
        #[arg(long)]
        brain: Option<String>,
    },

    /// Rename a label across all tasks
    Rename {
        /// Current label name
        old_label: String,

        /// New label name
        new_label: String,
    },

    /// Remove a label from all tasks
    Purge {
        /// Label to purge
        label: String,
    },
}

// ── ext-link subcommands ─────────────────────────────────────

#[derive(Subcommand)]
pub(crate) enum ExtLinkAction {
    /// Add an external reference to a task
    Add {
        /// Task ID
        task_id: String,

        /// Source system (e.g. "github", "jira", "linear")
        #[arg(long)]
        source: String,

        /// External identifier (e.g. issue number, ticket ID)
        #[arg(long)]
        id: String,

        /// URL to the external resource
        #[arg(long)]
        url: Option<String>,
    },

    /// Remove an external reference from a task
    Remove {
        /// Task ID
        task_id: String,

        /// Source system
        #[arg(long)]
        source: String,

        /// External identifier
        #[arg(long)]
        id: String,
    },

    /// List external references for a task
    List {
        /// Task ID
        task_id: String,
    },
}
