use clap::Subcommand;

#[derive(Subcommand)]
pub(crate) enum RecordTagAction {
    /// Add a tag to a record
    Add {
        /// Record ID (full or prefix)
        id: String,
        /// Tag to add
        tag: String,
    },
    /// Remove a tag from a record
    Remove {
        /// Record ID (full or prefix)
        id: String,
        /// Tag to remove
        tag: String,
    },
}

#[derive(Subcommand)]
pub(crate) enum RecordLinkAction {
    /// Add a link to a record
    Add {
        /// Record ID (full or prefix)
        id: String,
        /// Task ID to link
        #[arg(long)]
        task: Option<String>,
        /// Chunk ID to link
        #[arg(long)]
        chunk: Option<String>,
    },
    /// Remove a link from a record
    Remove {
        /// Record ID (full or prefix)
        id: String,
        /// Task ID to unlink
        #[arg(long)]
        task: Option<String>,
        /// Chunk ID to unlink
        #[arg(long)]
        chunk: Option<String>,
    },
}
