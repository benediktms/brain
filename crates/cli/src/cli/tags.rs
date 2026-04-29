use clap::Subcommand;

/// Parse and bounds-check a `threshold` argument. Values outside `[0.0,
/// 1.0]` would produce all-singleton clusters silently, so we reject them
/// at the CLI boundary rather than letting the recluster job complete and
/// write a misleading "successful" run row.
fn parse_threshold(s: &str) -> Result<f32, String> {
    let v: f32 = s.parse().map_err(|e| format!("invalid f32 '{s}': {e}"))?;
    if !(0.0..=1.0).contains(&v) {
        return Err(format!("threshold must be in [0.0, 1.0], got {v}"));
    }
    Ok(v)
}

#[derive(Subcommand)]
pub(crate) enum TagsAction {
    /// Run synonym clustering over the current brain's raw tags
    Recluster {
        /// Cosine similarity threshold for cluster edges. Must be in [0.0, 1.0].
        #[arg(long, default_value = "0.85", value_parser = parse_threshold)]
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
