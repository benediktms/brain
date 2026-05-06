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
    },

    /// Show a saga by its saga_id
    Show {
        /// Saga ID (bare 26-char ULID)
        saga_id: String,
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

        /// Only show sagas containing a task in this brain (brain_id)
        #[arg(long)]
        containing_brain: Option<String>,
    },

    /// Update a saga's title and/or description
    Update {
        /// Saga ID (bare 26-char ULID)
        saga_id: String,

        /// New title (must not be empty)
        #[arg(long)]
        title: Option<String>,

        /// New description
        #[arg(long)]
        description: Option<String>,
    },
}
