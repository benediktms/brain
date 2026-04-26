use clap::Subcommand;

#[derive(Subcommand)]
pub(crate) enum DocumentsAction {
    /// Create a new document
    Create {
        /// Document title
        #[arg(long)]
        title: String,

        /// Path to the payload file
        #[arg(long)]
        file: Option<std::path::PathBuf>,

        /// Read payload from stdin
        #[arg(long)]
        stdin: bool,

        /// Inline text payload
        #[arg(long)]
        text: Option<String>,

        /// Optional description
        #[arg(long)]
        description: Option<String>,

        /// Link to a task
        #[arg(long)]
        task: Option<String>,

        /// Tags to add
        #[arg(long)]
        tag: Vec<String>,

        /// Media type (e.g. text/plain, application/json)
        #[arg(long)]
        media_type: Option<String>,

        /// Target brain name or ID (writes to current brain if omitted)
        #[arg(long)]
        brain: Option<String>,
    },
}
