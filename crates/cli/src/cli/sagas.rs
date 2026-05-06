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
}
