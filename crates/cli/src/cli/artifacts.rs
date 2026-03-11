use clap::Subcommand;

use super::record_common::{RecordLinkAction, RecordTagAction};

#[derive(Subcommand)]
pub(crate) enum ArtifactsAction {
    /// Create a new artifact
    Create {
        /// Artifact title
        #[arg(long)]
        title: String,

        /// Record kind (report, diff, export, analysis, document)
        #[arg(long, default_value = "document")]
        kind: String,

        /// Path to the payload file
        #[arg(long)]
        file: Option<std::path::PathBuf>,

        /// Read payload from stdin
        #[arg(long)]
        stdin: bool,

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
    },

    /// List artifacts
    List {
        /// Filter by kind
        #[arg(long)]
        kind: Option<String>,

        /// Filter by tag
        #[arg(long)]
        tag: Option<String>,

        /// Filter by status (active, archived)
        #[arg(long, default_value = "active")]
        status: String,

        /// Maximum results
        #[arg(long, default_value = "50")]
        limit: usize,
    },

    /// Show details for a specific artifact
    Get {
        /// Record ID (full or prefix)
        id: String,
    },

    /// Archive an artifact
    Archive {
        /// Record ID (full or prefix)
        id: String,

        /// Reason for archiving
        #[arg(long)]
        reason: Option<String>,
    },

    /// Add or remove tags on an artifact
    Tag {
        #[command(subcommand)]
        action: RecordTagAction,
    },

    /// Add or remove links on an artifact
    Link {
        #[command(subcommand)]
        action: RecordLinkAction,
    },
}
