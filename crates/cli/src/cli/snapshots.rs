use clap::Subcommand;

use super::record_common::{RecordLinkAction, RecordTagAction};

#[derive(Subcommand)]
pub(crate) enum SnapshotsAction {
    /// Save a new snapshot (opaque state bundle)
    Save {
        /// Snapshot title
        #[arg(long)]
        title: String,

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

        /// Media type (default: application/octet-stream)
        #[arg(long)]
        media_type: Option<String>,
    },

    /// List snapshots
    List {
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

    /// Show details for a specific snapshot
    Get {
        /// Record ID (full or prefix)
        id: String,
    },

    /// Restore a snapshot's content to a file or stdout
    Restore {
        /// Record ID (full or prefix)
        id: String,

        /// Output file path (defaults to stdout)
        #[arg(long, short)]
        output: Option<std::path::PathBuf>,
    },

    /// Archive a snapshot
    Archive {
        /// Record ID (full or prefix)
        id: String,

        /// Reason for archiving
        #[arg(long)]
        reason: Option<String>,
    },

    /// Add or remove tags on a snapshot
    Tag {
        #[command(subcommand)]
        action: RecordTagAction,
    },

    /// Add or remove links on a snapshot
    Link {
        #[command(subcommand)]
        action: RecordLinkAction,
    },
}
