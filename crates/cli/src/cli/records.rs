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

    /// Search records using semantic + keyword hybrid retrieval
    Search {
        /// Natural-language search query
        query: String,

        /// Maximum number of results
        #[arg(short, long, default_value = "10")]
        k: usize,

        /// Token budget
        #[arg(short, long, default_value = "800")]
        budget: usize,

        /// Tags to filter (comma-delimited)
        #[arg(long, value_delimiter = ',')]
        tags: Vec<String>,

        /// Search across specific brains (repeatable). Use 'all' for all registered brains.
        #[arg(long = "brain", value_name = "NAME_OR_ID", num_args = 1)]
        brains: Vec<String>,
    },
}
