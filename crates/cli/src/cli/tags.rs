use clap::Subcommand;

#[derive(Subcommand)]
pub(crate) enum TagsAction {
    /// Run synonym clustering over the current brain's raw tags
    Recluster {
        /// Cosine similarity threshold for cluster edges
        #[arg(long, default_value = "0.85")]
        threshold: f32,
    },

    /// Inspect the current brain's tag_aliases table
    Aliases {
        #[command(subcommand)]
        action: AliasesAction,
    },

    /// Show health summary for the synonym-clustering subsystem
    Status,
}

#[derive(Subcommand)]
pub(crate) enum AliasesAction {
    /// List tag_aliases rows with optional filtering
    List {
        /// Filter to rows whose canonical_tag equals this value
        #[arg(long)]
        canonical: Option<String>,

        /// Filter to rows in the given cluster_id
        #[arg(long = "cluster-id")]
        cluster_id: Option<String>,

        /// Maximum rows to return
        #[arg(long, default_value = "50")]
        limit: i64,

        /// Row offset for pagination
        #[arg(long, default_value = "0")]
        offset: i64,
    },
}
