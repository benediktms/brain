use clap::Subcommand;

use super::Intent;

/// Subcommands for `brain memory`.
#[derive(Subcommand)]
pub(crate) enum MemoryAction {
    /// Search the knowledge base and return compact memory stubs
    #[command(
        visible_alias = "s",
        long_about = "Search the knowledge base using hybrid retrieval (vector + BM25 + ranking).\n\n\
            Returns compact memory stubs within a token budget. Results span note chunks, \
            task capsules, episodes, and reflections. Use --intent to tune the ranking \
            weight profile for your retrieval goal.\n\n\
            Weight profiles:\n  \
            - auto       Equal weights across all signals (default)\n  \
            - lookup     Keyword-heavy (40% BM25) for exact matches\n  \
            - planning   Recency + links for project planning queries\n  \
            - reflection Recency-heavy for journal/reflection queries\n  \
            - synthesis  Vector-heavy (40%) for semantic similarity",
        after_help = "EXAMPLES:\n  \
            brain memory search \"how does authentication work\"\n  \
            brain memory search \"async error handling\" -k 10\n  \
            brain memory s -i lookup \"database migration steps\"\n  \
            brain memory search \"rust ownership\" --tags rust,memory\n  \
            brain memory search \"design patterns\" --brain work --brain personal"
    )]
    Search {
        /// Natural-language search query
        query: String,

        /// Maximum number of results to return
        #[arg(short, long, default_value = "5")]
        k: usize,

        /// Ranking intent profile
        #[arg(short, long, default_value = "auto")]
        intent: Intent,

        /// Token budget for result packing
        #[arg(short, long, default_value = "800")]
        budget: usize,

        /// Tags to boost (comma-delimited, e.g. rust,memory)
        #[arg(long, value_delimiter = ',')]
        tags: Vec<String>,

        /// Search across specific brains (repeatable). Use 'all' for all registered brains.
        #[arg(long = "brain", value_name = "NAME_OR_ID", num_args = 1)]
        brains: Vec<String>,
    },

    /// Expand memory stubs to full content
    #[command(
        visible_alias = "e",
        long_about = "Expand memory stubs to full content.\n\n\
            Pass memory_ids obtained from `brain memory search` results. Returns the \
            full chunk content for each ID within the token budget.",
        after_help = "EXAMPLES:\n  \
            brain memory expand chunk-abc123 chunk-def456\n  \
            brain memory e chunk-abc123 --budget 4000"
    )]
    Expand {
        /// Memory IDs to expand (from search results)
        #[arg(required = true)]
        memory_ids: Vec<String>,

        /// Token budget for expanded content
        #[arg(short, long, default_value = "2000")]
        budget: usize,
    },

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
        #[arg(long, default_value = "0.5")]
        importance: f64,
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
