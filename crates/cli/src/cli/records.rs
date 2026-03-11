use clap::Subcommand;

#[derive(Subcommand)]
pub(crate) enum RecordsAction {
    /// Verify integrity of the records object store
    Verify {
        /// Show detailed findings
        #[arg(long)]
        verbose: bool,
    },

    /// Remove orphan blobs from the object store
    Gc {
        /// Preview without deleting
        #[arg(long)]
        dry_run: bool,
    },

    /// Evict a record's payload from the object store
    Evict {
        /// Record ID (full or prefix)
        id: String,

        /// Reason for eviction
        #[arg(long)]
        reason: Option<String>,
    },

    /// Pin a record (prevent payload eviction)
    Pin {
        /// Record ID (full or prefix)
        id: String,
    },

    /// Unpin a record (allow payload eviction)
    Unpin {
        /// Record ID (full or prefix)
        id: String,
    },
}
