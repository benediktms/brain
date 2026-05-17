use clap::Subcommand;

#[derive(Subcommand)]
pub(crate) enum SagasAction {
    /// Create a new saga in 'planning' status
    Create {
        /// Saga title
        #[arg(long)]
        title: String,

        /// Optional description
        #[arg(long)]
        description: Option<String>,

        /// Route operation through brain-daemon over local Unix socket
        #[arg(long)]
        remote: bool,
    },

    /// Show a saga by its saga_id
    Show {
        /// Saga ID (bare 26-char ULID)
        saga_id: String,

        /// Route operation through brain-daemon over local Unix socket
        #[arg(long)]
        remote: bool,
    },

    /// List sagas (default: planning and open only)
    List {
        /// Include closed sagas
        #[arg(long)]
        include_closed: bool,

        /// Include cancelled sagas
        #[arg(long)]
        include_cancelled: bool,

        /// Include all sagas regardless of status
        #[arg(long)]
        all: bool,

        /// Filter by brain_id (not brain name). Only sagas with at least one
        /// live member task in this brain are returned.
        #[arg(long)]
        containing_brain: Option<String>,

        /// Route operation through brain-daemon over local Unix socket
        #[arg(long)]
        remote: bool,
    },

    /// Update a saga's title and/or description
    Update {
        /// Saga ID (bare 26-char ULID)
        saga_id: String,

        /// New title (must not be empty)
        #[arg(long)]
        title: Option<String>,

        /// New description
        #[arg(long, conflicts_with = "clear_description")]
        description: Option<String>,

        /// Clear the description (set it to null)
        #[arg(long)]
        clear_description: bool,

        /// Route operation through brain-daemon over local Unix socket
        #[arg(long)]
        remote: bool,
    },

    /// Add one or more tasks to a saga (atomic batch)
    Add {
        /// Saga ID (bare 26-char ULID)
        saga_id: String,

        /// Task IDs to add (full IDs or short hashes, cross-brain aware)
        #[arg(required = true)]
        task_ids: Vec<String>,

        /// Also add every transitive descendant of each input task (via parent_of)
        #[arg(long)]
        cascade: bool,

        /// Route operation through brain-daemon over local Unix socket
        #[arg(long)]
        remote: bool,
    },

    /// Start a saga (planning → open)
    Start {
        /// Saga ID (bare 26-char ULID)
        saga_id: String,

        /// Route operation through brain-daemon over local Unix socket
        #[arg(long)]
        remote: bool,
    },

    /// Close a saga (must be in 'open' status)
    Close {
        /// Saga ID (bare 26-char ULID)
        saga_id: String,

        /// Also close all member tasks
        #[arg(long)]
        cascade: bool,

        /// Route operation through brain-daemon over local Unix socket
        #[arg(long)]
        remote: bool,
    },

    /// Remove tasks from a saga (idempotent)
    Remove {
        /// Saga ID (bare 26-char ULID)
        saga_id: String,

        /// One or more task IDs to remove
        #[arg(required = true)]
        task_ids: Vec<String>,

        /// Also remove every transitive descendant of each input task (via parent_of) currently in the saga
        #[arg(long)]
        cascade: bool,

        /// Route operation through brain-daemon over local Unix socket
        #[arg(long)]
        remote: bool,
    },

    /// Reopen a closed or cancelled saga (status → open)
    Reopen {
        /// Saga ID (bare 26-char ULID)
        saga_id: String,

        /// Route operation through brain-daemon over local Unix socket
        #[arg(long)]
        remote: bool,
    },

    /// Show ready-actionable tasks in a saga (same rules as tasks next)
    Frontier {
        /// Saga ID (bare 26-char ULID)
        saga_id: String,

        /// Route operation through brain-daemon over local Unix socket
        #[arg(long)]
        remote: bool,
    },

    /// Aggregate statistics for a saga's member tasks
    Stats {
        /// Saga ID (bare 26-char ULID)
        saga_id: String,

        /// Route operation through brain-daemon over local Unix socket
        #[arg(long)]
        remote: bool,
    },

    /// Cancel a saga (optionally cascade-cancels member tasks)
    Cancel {
        /// Saga ID (bare 26-char ULID)
        saga_id: String,

        /// Also cancel non-terminal member tasks
        #[arg(long)]
        cascade: bool,

        /// Route operation through brain-daemon over local Unix socket
        #[arg(long)]
        remote: bool,
    },
}
