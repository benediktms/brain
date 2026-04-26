use clap::Subcommand;

use super::record_common::{RecordLinkAction, RecordTagAction};

#[derive(Subcommand)]
pub(crate) enum ArtifactsAction {
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

    /// Restore an artifact's content to a file or stdout
    #[command(
        long_about = "Fetch an artifact's raw content and write it to a file or stdout.\n\n\
            Text content (text/*, application/json, application/toml, application/yaml) \
            is auto-decompressed. Binary content is written as raw bytes.\n\n\
            When no --output is specified, content is written to stdout — useful \
            for piping to other commands.",
        after_help = "EXAMPLES:\n  \
            brain artifacts restore BRN-01JPH                  # Print to stdout\n  \
            brain artifacts restore BRN-01JPH -o report.md     # Write to file\n  \
            brain artifacts restore BRN-01JPH | jq .           # Pipe JSON artifact"
    )]
    Restore {
        /// Record ID (full or prefix)
        id: String,
        /// Output file path (defaults to stdout)
        #[arg(long, short)]
        output: Option<std::path::PathBuf>,
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
