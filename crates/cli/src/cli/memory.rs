use clap::Subcommand;

/// Subcommands for `brain memory`.
#[derive(Subcommand)]
pub(crate) enum MemoryAction {
    /// Record a goal/actions/outcome episode to the knowledge base
    #[command(
        name = "write-episode",
        visible_alias = "we",
        long_about = "Record an episode (goal, actions, outcome) to the knowledge base.\n\n\
            Episodes are stored in SQLite and best-effort embedded into the vector store \
            for semantic retrieval. Use `brain memory reflect` to synthesize episodes \
            into reflections.",
        after_help = "EXAMPLES:\n  \
            brain memory write-episode \\\n    \
                --goal \"Fix authentication bug\" \\\n    \
                --actions \"Traced JWT validation, patched expiry check\" \\\n    \
                --outcome \"Bug resolved, tests pass\"\n  \
            brain memory we \\\n    \
                --goal \"Refactor DB layer\" \\\n    \
                --actions \"Extracted repository trait\" \\\n    \
                --outcome \"Cleaner separation of concerns\" \\\n    \
                --tags refactoring,database --importance 0.8"
    )]
    WriteEpisode {
        /// What was the goal
        #[arg(long, required = true)]
        goal: String,

        /// What actions were taken
        #[arg(long, required = true)]
        actions: String,

        /// What was the outcome
        #[arg(long, required = true)]
        outcome: String,

        /// Tags for categorization (comma-delimited, e.g. debugging,auth)
        #[arg(long, value_delimiter = ',')]
        tags: Vec<String>,

        /// Importance score (0.0 to 1.0)
        #[arg(long, default_value = "1.0")]
        importance: f64,
    },

    /// Store a step-by-step procedure to the knowledge base
    #[command(
        name = "write-procedure",
        visible_alias = "wp",
        long_about = "Store a procedure (title + steps) to the knowledge base.\n\n\
            Procedures are stored in SQLite and best-effort embedded into the vector store \
            for semantic retrieval. Use for repeatable processes, runbooks, and how-tos.",
        after_help = "EXAMPLES:\n  \
            brain memory write-procedure \\\n    \
                --title \"Deploy to production\" \\\n    \
                --steps \"1. Run tests\\n2. Build image\\n3. Push to registry\\n4. Update manifests\"\n  \
            brain memory wp \\\n    \
                --title \"Debug auth failures\" \\\n    \
                --steps \"1. Check JWT expiry\\n2. Verify signing key\" \\\n    \
                --tags auth,debugging --importance 0.8"
    )]
    WriteProcedure {
        /// Title of the procedure
        #[arg(long, required = true)]
        title: String,

        /// Step-by-step content of the procedure
        #[arg(long, required = true)]
        steps: String,

        /// Tags for categorization (comma-delimited, e.g. ops,deployment)
        #[arg(long, value_delimiter = ',')]
        tags: Vec<String>,

        /// Importance score (0.0 to 1.0)
        #[arg(long, default_value = "0.9")]
        importance: f64,
    },

    /// Group recent episodes by temporal proximity into consolidation clusters
    #[command(
        name = "consolidate",
        visible_alias = "co",
        long_about = "Group recent episodes by temporal proximity into consolidation clusters.\n\n\
            Returns clusters of temporally proximate episodes with suggested titles and summaries, \
            ordered newest-first. Use the output to decide which episodes to synthesize into a \
            reflection via `brain memory reflect --commit`.",
        after_help = "EXAMPLES:\n  \
            brain memory consolidate\n  \
            brain memory consolidate --limit 100 --gap-seconds 7200\n  \
            brain memory co"
    )]
    Consolidate {
        /// Maximum number of recent episodes to consider
        #[arg(long, default_value = "50")]
        limit: usize,

        /// Gap in seconds between episodes that triggers a cluster boundary
        #[arg(long, default_value = "3600")]
        gap_seconds: i64,

        /// Enqueue async LLM synthesis jobs for returned clusters
        #[arg(long)]
        auto_summarize: bool,
    },

    /// Generate or retrieve a scope summary for a directory or tag
    #[command(
        name = "summarize-scope",
        visible_alias = "ss",
        long_about = "Generate or retrieve a derived summary for a directory or tag scope.\n\n\
            Collects all chunk content matching the scope and produces an extractive summary. \
            Use --regenerate to force a fresh summary even if one already exists.\n\n\
            Scope types:\n  \
            - directory   Summarize all chunks under a directory path\n  \
            - tag         Summarize all chunks with a given tag",
        after_help = "EXAMPLES:\n  \
            brain memory summarize-scope --scope-type directory --scope-value src/auth\n  \
            brain memory summarize-scope --scope-type tag --scope-value rust\n  \
            brain memory summarize-scope --scope-type directory --scope-value src/auth --regenerate\n  \
            brain memory ss --scope-type tag --scope-value debugging --regenerate\n  \
            brain memory ss --scope-type directory --scope-value src/auth --no-async-llm"
    )]
    SummarizeScope {
        /// Scope type: "directory" or "tag"
        #[arg(long, required = true)]
        scope_type: String,

        /// Scope value: directory path or tag name
        #[arg(long, required = true)]
        scope_value: String,

        /// Force regeneration of the summary even if one exists
        #[arg(long)]
        regenerate: bool,

        /// Disable async LLM refresh and keep the extractive placeholder only
        #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
        async_llm: bool,
    },

    /// Retrieve memory chunks at a requested level of detail (LOD)
    #[command(
        visible_alias = "r",
        long_about = "Retrieve memory chunks at a requested level of detail (LOD).\n\n\
            Supports two modes:\n  \
            - Query mode (default): semantic search ranked by relevance.\n  \
            - URI mode: direct access by synapse:// address.\n\n\
            LOD levels:\n  \
            - L0   Extractive abstract (~100 tokens each, default)\n  \
            - L1   LLM-summarized content (~2000 tokens each)\n  \
            - L2   Full source passthrough\n\n\
            Strategy (ranking weight profile):\n  \
            - auto       Equal weights across all signals (default)\n  \
            - lookup     Keyword-heavy (40% BM25) for exact matches\n  \
            - planning   Recency + links for project planning queries\n  \
            - reflection Recency-heavy for journal/reflection queries\n  \
            - synthesis  Vector-heavy (40%) for semantic similarity\n\n\
            Exactly one of QUERY or --uri must be provided.",
        after_help = "EXAMPLES:\n  \
            brain memory retrieve \"how does authentication work\"\n  \
            brain memory retrieve \"async error handling\" -k 5 --lod L1\n  \
            brain memory r \"rust ownership\" --strategy lookup\n  \
            brain memory retrieve --uri synapse://brain/memory/chunk-abc123\n  \
            brain memory retrieve \"design patterns\" --brain work --brain personal\n  \
            brain memory retrieve \"recent changes\" --time-scope 7d\n  \
            brain memory retrieve \"auth\" --kinds note,episode --explain"
    )]
    Retrieve {
        /// Natural-language search query (provide this or --uri, not both)
        query: Option<String>,

        /// Direct access by synapse:// URI (provide this or QUERY, not both)
        #[arg(long)]
        uri: Option<String>,

        /// Level of detail: L0 (extractive abstract), L1 (LLM summary), L2 (full source)
        #[arg(long, default_value = "L0")]
        lod: String,

        /// Maximum number of results to return
        #[arg(short = 'k', long = "count", default_value = "10")]
        count: u64,

        /// Ranking strategy (auto, lookup, planning, reflection, synthesis)
        #[arg(short = 's', long, default_value = "auto")]
        strategy: String,

        /// Search across specific brains (repeatable). Use 'all' for all registered brains.
        #[arg(long = "brain", value_name = "NAME_OR_ID", num_args = 1)]
        brains: Vec<String>,

        /// Relative time window, e.g. "7d", "30d", "24h"
        #[arg(long)]
        time_scope: Option<String>,

        /// Only results created/modified after this Unix timestamp
        #[arg(long)]
        time_after: Option<i64>,

        /// Only results created/modified before this Unix timestamp
        #[arg(long)]
        time_before: Option<i64>,

        /// Tags to boost via Jaccard similarity (comma-delimited)
        #[arg(long, value_delimiter = ',')]
        tags: Vec<String>,

        /// Tags that all must match (AND filter, comma-delimited)
        #[arg(long = "tags-require", value_delimiter = ',')]
        tags_require: Vec<String>,

        /// Tags whose presence excludes a result (NOR filter, comma-delimited)
        #[arg(long = "tags-exclude", value_delimiter = ',')]
        tags_exclude: Vec<String>,

        /// Filter by kind (comma-delimited: note,episode,reflection,procedure,task,task-outcome,record)
        #[arg(long, value_delimiter = ',')]
        kinds: Vec<String>,

        /// Include per-result signal score breakdowns
        #[arg(long)]
        explain: bool,
    },

    /// Retrieve source material for reflection (prepare) or store a reflection (commit)
    #[command(
        long_about = "Two-phase episodic reflection.\n\n\
            PREPARE mode (default): Retrieve recent episodes and semantically related chunks \
            that you can synthesize into a reflection. Requires --topic.\n\n\
            COMMIT mode: Store a completed reflection linked to its source episodes. \
            Requires --title, --content, and --source-ids.\n\n\
            Typical workflow:\n  \
            1. brain memory reflect --topic \"project architecture\"\n  \
            2. [synthesize the returned episodes + chunks into a reflection]\n  \
            3. brain memory reflect --commit \\\n       \
                   --title \"Architecture notes\" \\\n       \
                   --content \"...\" \\\n       \
                   --source-ids ep-abc,ep-def",
        after_help = "EXAMPLES:\n  \
            brain memory reflect --topic \"async patterns in the codebase\"\n  \
            brain memory reflect --topic \"debugging sessions\" --budget 4000\n  \
            brain memory reflect --commit \\\n    \
                --title \"Weekly reflection\" \\\n    \
                --content \"Key insight: ...\" \\\n    \
                --source-ids ep-abc123,ep-def456\n  \
            brain memory reflect --commit \\\n    \
                --title \"Architecture notes\" \\\n    \
                --content \"...\" \\\n    \
                --source-ids ep-abc123 \\\n    \
                --tags architecture --importance 0.9"
    )]
    Reflect {
        /// Switch to commit mode (store a completed reflection)
        #[arg(long)]
        commit: bool,

        // --- prepare fields ---
        /// (prepare) Topic to reflect on
        #[arg(long)]
        topic: Option<String>,

        /// (prepare) Token budget for source material
        #[arg(long, default_value = "2000")]
        budget: usize,

        /// (prepare) Brain names/IDs to include (repeatable). Use 'all' for all brains.
        #[arg(long = "brain", value_name = "NAME_OR_ID", num_args = 1)]
        brains: Vec<String>,

        // --- commit fields ---
        /// (commit) Title of the reflection
        #[arg(long)]
        title: Option<String>,

        /// (commit) Synthesized reflection content
        #[arg(long)]
        content: Option<String>,

        /// (commit) summary_ids of source episodes (comma-delimited)
        #[arg(long = "source-ids", value_delimiter = ',')]
        source_ids: Vec<String>,

        /// Tags for the reflection (comma-delimited)
        #[arg(long, value_delimiter = ',')]
        tags: Vec<String>,

        /// (commit) Importance score (0.0 to 1.0)
        #[arg(long)]
        importance: Option<f64>,
    },
}
