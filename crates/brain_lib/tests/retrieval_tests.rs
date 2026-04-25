//! Phase 2 integration tests: hybrid retrieval, FTS5, links, ranking, MCP round-trip.
//!
//! Uses MockEmbedder (deterministic hash-based vectors) and in-memory/tempdir
//! databases. Tests validate the full pipeline from indexing through retrieval.

use std::sync::Arc;

use serde_json::{Value, json};
use tempfile::TempDir;

use brain_lib::embedder::{Embed, MockEmbedder};
use brain_lib::error::BrainCoreError;
use brain_lib::links::{LinkType, extract_links};
use brain_lib::mcp::McpContext;
use brain_lib::mcp::tools::ToolRegistry;
use brain_lib::parser::parse_document;
use brain_lib::pipeline::IndexPipeline;
use brain_lib::ranking::{
    CandidateSignals, RankedResult, SignalScores, WeightProfile, Weights, rank_candidates,
};
use brain_lib::retrieval::{expand_results, pack_minimal};
use brain_lib::tokens::estimate_tokens;
use brain_persistence::db::Db;
use brain_persistence::db::chunks::get_chunks_by_ids;
use brain_persistence::db::fts::search_fts;
use brain_persistence::db::links::count_backlinks;
use brain_persistence::db::summaries::{Episode, get_summary, list_episodes, store_episode};
use brain_persistence::store::Store;

// ─── Helpers ─────────────────────────────────────────────────────

async fn setup() -> (IndexPipeline, TempDir) {
    let tmp = TempDir::new().unwrap();
    let sqlite_path = tmp.path().join("brain.db");
    let lance_path = tmp.path().join("brain_lancedb");

    let db = Db::open(&sqlite_path).unwrap();
    let store = Store::open_or_create(&lance_path).await.unwrap();
    let embedder = Arc::new(MockEmbedder);

    let pipeline = IndexPipeline::with_embedder(db, store, embedder)
        .await
        .unwrap();
    (pipeline, tmp)
}

async fn setup_mcp() -> (McpContext, TempDir) {
    let tmp = TempDir::new().unwrap();
    let brain_home = tmp.path().to_path_buf();
    let sqlite_path = tmp.path().join("brain.db");
    let lance_path = tmp.path().join("brain_lancedb");
    let _tasks_dir = tmp.path().join("tasks");

    let db = Db::open(&sqlite_path).unwrap();
    let store = Store::open_or_create(&lance_path).await.unwrap();
    let store_reader = brain_persistence::store::StoreReader::from_store(&store);
    let embedder: Arc<dyn Embed> = Arc::new(MockEmbedder);

    let stores =
        brain_lib::stores::BrainStores::from_dbs(db, "", &brain_home, &brain_home).unwrap();

    let ctx = McpContext {
        stores,
        search: Some(brain_lib::search_service::SearchService {
            store: store_reader,
            embedder,
        }),
        writable_store: Some(store),
        metrics: Arc::new(brain_lib::metrics::Metrics::new()),
    };
    (ctx, tmp)
}

fn write_md(dir: &std::path::Path, name: &str, content: &str) -> std::path::PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, content).unwrap();
    path
}

// ─── 1. Parser integration ──────────────────────────────────────

#[test]
fn test_parser_frontmatter_extracted() {
    let text = "---\ntitle: My Note\ntags:\n  - rust\n  - brain\n---\n\n# Heading\n\nBody text.";
    let doc = parse_document(text);

    assert!(!doc.frontmatter.is_empty());
    assert_eq!(
        doc.frontmatter.get("title").and_then(|v| v.as_str()),
        Some("My Note")
    );
}

#[test]
fn test_parser_heading_paths() {
    let text = "# Top\n\nIntro.\n\n## Sub A\n\nA content.\n\n### Deep\n\nDeep content.\n\n## Sub B\n\nB content.";
    let doc = parse_document(text);

    let paths: Vec<&str> = doc
        .sections
        .iter()
        .map(|s| s.heading_path.as_str())
        .collect();
    assert_eq!(
        paths,
        vec![
            "# Top",
            "# Top > ## Sub A",
            "# Top > ## Sub A > ### Deep",
            "# Top > ## Sub B"
        ]
    );
}

#[test]
fn test_parser_code_blocks_preserved() {
    let text = "# Code\n\n```rust\nfn main() {\n    println!(\"hello\");\n}\n```\n\nAfter code.";
    let doc = parse_document(text);

    // Code block should be in the section content, not parsed as headings
    let content = &doc.sections[0].content;
    assert!(
        content.contains("fn main()"),
        "code block should be preserved in content"
    );
    assert!(
        content.contains("println!"),
        "code block body should be preserved"
    );
}

// ─── 2. Link extraction and backlinks ────────────────────────────

#[test]
fn test_links_wiki_and_markdown_extracted() {
    let text = "See [[project-ideas]] and [[design|Design Doc]] for details.\n\nAlso check [the RFC](rfc.md) and [external](https://example.com).";
    let links = extract_links(text);

    assert_eq!(links.len(), 4);

    let wiki_targets: Vec<&str> = links
        .iter()
        .filter(|l| l.link_type == LinkType::Wiki)
        .map(|l| l.target.as_str())
        .collect();
    assert!(wiki_targets.contains(&"project-ideas"));
    assert!(wiki_targets.contains(&"design"));

    let md_links: Vec<&str> = links
        .iter()
        .filter(|l| l.link_type == LinkType::Markdown)
        .map(|l| l.target.as_str())
        .collect();
    assert!(md_links.contains(&"rfc.md"));

    let external: Vec<&str> = links
        .iter()
        .filter(|l| l.link_type == LinkType::External)
        .map(|l| l.target.as_str())
        .collect();
    assert!(external.contains(&"https://example.com"));
}

#[tokio::test]
async fn test_links_stored_and_backlinks_queryable() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    // File A links to File B via wiki-link
    write_md(&notes_dir, "a.md", "# File A\n\nSee [[b]] for more info.");
    write_md(&notes_dir, "b.md", "# File B\n\nThis is the target file.");

    pipeline.full_scan(&[notes_dir]).await.unwrap();

    // Query backlinks to b.md's path
    let backlinks = pipeline
        .db_for_tests()
        .with_read_conn(|conn| count_backlinks(conn, "b"))
        .unwrap();
    assert_eq!(backlinks, 1, "b.md should have 1 backlink from a.md");
}

// ─── 3. FTS5 via pipeline ────────────────────────────────────────

#[tokio::test]
async fn test_fts_keyword_search_after_indexing() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    write_md(
        &notes_dir,
        "rust.md",
        "# Rust\n\nRust is a systems programming language focused on safety and performance.",
    );
    write_md(
        &notes_dir,
        "python.md",
        "# Python\n\nPython is great for data science and machine learning.",
    );
    write_md(
        &notes_dir,
        "cooking.md",
        "# Cooking\n\nBoil water and add pasta for eight minutes.",
    );

    pipeline.full_scan(&[notes_dir]).await.unwrap();

    // FTS search for "rust"
    let results = pipeline
        .db_for_tests()
        .with_read_conn(|conn| search_fts(conn, "rust", 10, None))
        .unwrap();
    assert!(!results.is_empty(), "FTS should find 'rust'");
    // The result should be from the rust.md chunk
    assert!(
        results[0].chunk_id.contains(":"),
        "chunk_id should be file_id:ord format"
    );

    // FTS search for "machine learning"
    let results = pipeline
        .db_for_tests()
        .with_read_conn(|conn| search_fts(conn, "\"machine learning\"", 10, None))
        .unwrap();
    assert_eq!(
        results.len(),
        1,
        "phrase query should find exactly 1 result"
    );

    // FTS search for something not present
    let results = pipeline
        .db_for_tests()
        .with_read_conn(|conn| search_fts(conn, "javascript", 10, None))
        .unwrap();
    assert!(results.is_empty(), "javascript not in any file");
}

#[tokio::test]
async fn test_fts_consistent_after_file_update() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    let path = write_md(
        &notes_dir,
        "topic.md",
        "# Topic\n\nOriginal content about databases.",
    );
    pipeline
        .full_scan(std::slice::from_ref(&notes_dir))
        .await
        .unwrap();

    let results = pipeline
        .db_for_tests()
        .with_read_conn(|conn| search_fts(conn, "databases", 10, None))
        .unwrap();
    assert_eq!(results.len(), 1);

    // Update content — remove "databases", add "networking"
    std::fs::write(&path, "# Topic\n\nUpdated content about networking.").unwrap();
    pipeline.full_scan(&[notes_dir]).await.unwrap();

    let results = pipeline
        .db_for_tests()
        .with_read_conn(|conn| search_fts(conn, "databases", 10, None))
        .unwrap();
    assert!(
        results.is_empty(),
        "old keyword should not be found after update"
    );

    let results = pipeline
        .db_for_tests()
        .with_read_conn(|conn| search_fts(conn, "networking", 10, None))
        .unwrap();
    assert_eq!(results.len(), 1, "new keyword should be found");
}

// ─── 4. Hybrid ranking ──────────────────────────────────────────

#[test]
fn test_hybrid_ranking_signal_combination() {
    // Create candidates with known signals
    let candidates = vec![
        CandidateSignals {
            chunk_id: "keyword_champ".into(),
            sim_vector: 0.2,
            bm25: 1.0,
            age_seconds: 86400.0 * 60.0, // 60 days old
            pagerank_score: 0.0,
            tags: vec![],
            importance: 1.0,
            file_path: "/a.md".into(),
            heading_path: String::new(),
            content: "keyword content".into(),
            token_estimate: 10,
            byte_start: 0,
            byte_end: 0,
            summary_kind: None,
        },
        CandidateSignals {
            chunk_id: "vector_champ".into(),
            sim_vector: 0.95,
            bm25: 0.1,
            age_seconds: 86400.0 * 60.0,
            pagerank_score: 0.0,
            tags: vec![],
            importance: 1.0,
            file_path: "/b.md".into(),
            heading_path: String::new(),
            content: "vector content".into(),
            token_estimate: 10,
            byte_start: 0,
            byte_end: 0,
            summary_kind: None,
        },
        CandidateSignals {
            chunk_id: "fresh_one".into(),
            sim_vector: 0.3,
            bm25: 0.3,
            age_seconds: 60.0, // 1 minute old
            pagerank_score: 0.2,
            tags: vec![],
            importance: 1.0,
            file_path: "/c.md".into(),
            heading_path: String::new(),
            content: "fresh content".into(),
            token_estimate: 10,
            byte_start: 0,
            byte_end: 0,
            summary_kind: None,
        },
    ];

    // Lookup profile: should prefer keyword_champ
    let lookup = rank_candidates(
        &candidates,
        &Weights::from_profile(WeightProfile::Lookup),
        &[],
    );
    assert_eq!(
        lookup[0].chunk_id, "keyword_champ",
        "Lookup should favor BM25"
    );

    // Synthesis profile: should prefer vector_champ
    let synthesis = rank_candidates(
        &candidates,
        &Weights::from_profile(WeightProfile::Synthesis),
        &[],
    );
    assert_eq!(
        synthesis[0].chunk_id, "vector_champ",
        "Synthesis should favor vector similarity"
    );

    // Planning profile: should prefer fresh_one (recency + links)
    let planning = rank_candidates(
        &candidates,
        &Weights::from_profile(WeightProfile::Planning),
        &[],
    );
    assert_eq!(
        planning[0].chunk_id, "fresh_one",
        "Planning should favor recency + links"
    );
}

// ─── 5. Token budgeting ─────────────────────────────────────────

#[test]
fn test_search_minimal_budget_compliance() {
    let ranked: Vec<RankedResult> = (0..20)
        .map(|i| {
            let content = format!("This is chunk number {i} with some content to fill tokens.");
            let tokens = estimate_tokens(&content);
            RankedResult {
                chunk_id: format!("chunk:{i}"),
                hybrid_score: 1.0 - (i as f64 * 0.05),
                scores: SignalScores {
                    vector: 0.5,
                    keyword: 0.5,
                    recency: 0.5,
                    links: 0.0,
                    tag_match: 0.0,
                    importance: 1.0,
                },
                file_path: format!("/notes/{i}.md"),
                heading_path: format!("## Section {i}"),
                content,
                token_estimate: tokens,
                byte_start: 0,
                byte_end: 0,
                summary_kind: None,
            }
        })
        .collect();

    // Tight budget: 100 tokens
    let result = pack_minimal(&ranked, 100, 20, false, &std::collections::HashMap::new());
    assert!(
        result.used_tokens_est <= 100,
        "must not exceed budget: {} > 100",
        result.used_tokens_est
    );
    assert!(result.num_results > 0, "should return at least 1 result");
    assert_eq!(result.total_available, 20);

    // Generous budget
    let result = pack_minimal(&ranked, 10000, 20, false, &std::collections::HashMap::new());
    assert_eq!(
        result.num_results, 20,
        "generous budget should fit all results"
    );
}

#[test]
fn test_expand_truncates_correctly() {
    let long_content = "A medium length sentence for testing. ".repeat(30);
    let ranked = vec![RankedResult {
        chunk_id: "long".into(),
        hybrid_score: 1.0,
        scores: SignalScores {
            vector: 1.0,
            keyword: 0.0,
            recency: 0.0,
            links: 0.0,
            tag_match: 0.0,
            importance: 1.0,
        },
        file_path: "/notes/long.md".into(),
        heading_path: "## Long".into(),
        content: long_content.clone(),
        token_estimate: estimate_tokens(&long_content),
        byte_start: 0,
        byte_end: 0,
        summary_kind: None,
    }];

    // Budget smaller than the content
    let result = expand_results(&ranked, 50);
    assert_eq!(result.memories.len(), 1);
    assert!(result.memories[0].truncated, "should be truncated");
    assert!(
        result.memories[0].content.contains("[truncated]"),
        "should have truncation marker"
    );
    assert!(result.used_tokens_est <= 50, "must not exceed budget");
}

// ─── 6. Chunk lookup ─────────────────────────────────────────────

#[tokio::test]
async fn test_chunk_lookup_by_ids() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    write_md(
        &notes_dir,
        "lookup.md",
        "## First\n\nContent of first section.\n\n## Second\n\nContent of second section.",
    );
    pipeline.full_scan(&[notes_dir]).await.unwrap();

    // Get all chunk IDs from the database
    let chunk_ids: Vec<String> = pipeline
        .db_for_tests()
        .with_read_conn(|conn| {
            let mut stmt = conn.prepare("SELECT chunk_id FROM chunks ORDER BY chunk_ord")?;
            let rows = stmt.query_map([], |row| row.get(0))?;
            let mut ids = Vec::new();
            for row in rows {
                ids.push(row?);
            }
            Ok(ids)
        })
        .unwrap();

    assert_eq!(
        chunk_ids.len(),
        2,
        "should have 2 chunks (2 heading sections)"
    );

    // Look up by IDs
    let rows = pipeline
        .db_for_tests()
        .with_read_conn(|conn| get_chunks_by_ids(conn, &chunk_ids))
        .unwrap();

    assert_eq!(rows.len(), 2);
    assert!(rows[0].content.contains("first section") || rows[1].content.contains("first section"));
    assert!(
        !rows[0].file_path.is_empty(),
        "file_path should be populated from join"
    );
}

// ─── 7. MCP tool round-trip ─────────────────────────────────────

#[tokio::test]
async fn test_mcp_write_episode_and_retrieve() {
    let (ctx, _tmp) = setup_mcp().await;

    // Write an episode
    let write_params = json!({
        "goal": "Implement hybrid search",
        "actions": "Built FTS5 + vector search + ranking engine",
        "outcome": "Hybrid retrieval working end-to-end",
        "tags": ["search", "retrieval"],
        "importance": 0.9
    });

    let registry = ToolRegistry::new();
    let result = registry
        .dispatch("memory.write_episode", write_params.clone(), &ctx)
        .await;
    assert!(result.is_error.is_none(), "write_episode should succeed");

    let text = &result.content[0].text;
    let parsed: Value = serde_json::from_str(text).unwrap();
    assert_eq!(parsed["status"], "stored");
    let summary_id = parsed["summary_id"].as_str().unwrap();

    // Verify episode is in the database
    let episode = ctx
        .stores
        .db_for_tests()
        .with_read_conn(|conn| get_summary(conn, summary_id))
        .unwrap();
    assert!(episode.is_some(), "episode should be retrievable");
    let ep = episode.unwrap();
    assert_eq!(ep.kind, "episode");
    assert!(ep.content.contains("hybrid search"));
}

#[tokio::test]
async fn test_mcp_search_minimal_returns_results() {
    let tmp = TempDir::new().unwrap();
    let sqlite_path = tmp.path().join("brain.db");
    let lance_path = tmp.path().join("brain_lancedb");
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    // Index via pipeline first
    let db = Db::open(&sqlite_path).unwrap();
    let store = Store::open_or_create(&lance_path).await.unwrap();
    let embedder: Arc<dyn Embed> = Arc::new(MockEmbedder);
    let pipeline = IndexPipeline::with_embedder(db, store, embedder)
        .await
        .unwrap();

    write_md(
        &notes_dir,
        "rust.md",
        "## Rust\n\nRust is a systems programming language.",
    );
    write_md(
        &notes_dir,
        "python.md",
        "## Python\n\nPython is used for machine learning.",
    );

    pipeline.full_scan(&[notes_dir]).await.unwrap();
    drop(pipeline);

    // Create fresh McpContext that sees the indexed data
    let store2 = Store::open_or_create(&lance_path).await.unwrap();
    let store2_reader = brain_persistence::store::StoreReader::from_store(&store2);
    let ctx_db = Db::open(&sqlite_path).unwrap();
    let stores2 =
        brain_lib::stores::BrainStores::from_dbs(ctx_db, "", tmp.path(), tmp.path()).unwrap();
    let ctx = McpContext {
        stores: stores2,
        search: Some(brain_lib::search_service::SearchService {
            store: store2_reader,
            embedder: Arc::new(MockEmbedder),
        }),
        writable_store: Some(store2),
        metrics: Arc::new(brain_lib::metrics::Metrics::new()),
    };

    // Search — MockEmbedder won't give semantic results, but pipeline should not error.
    // Use exact chunk text for identity matching with MockEmbedder.
    let params = json!({
        "query": "Rust is a systems programming language.",
        "intent": "lookup",
        "budget_tokens": 500,
        "k": 5
    });

    let registry = ToolRegistry::new();
    let result = registry
        .dispatch("memory.search_minimal", params.clone(), &ctx)
        .await;
    assert!(result.is_error.is_none(), "search should not error");

    let text = &result.content[0].text;
    let parsed: Value = serde_json::from_str(text).unwrap();
    assert_eq!(parsed["intent_resolved"], "Lookup");

    // Should have results from both vector and FTS
    let count = parsed["result_count"].as_u64().unwrap_or(0);
    assert!(
        count > 0,
        "should find at least one result, got response: {text}"
    );
}

#[tokio::test]
async fn test_mcp_expand_returns_full_content() {
    let tmp = TempDir::new().unwrap();
    let sqlite_path = tmp.path().join("brain.db");
    let lance_path = tmp.path().join("brain_lancedb");
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    // Index via pipeline first
    let db = Db::open(&sqlite_path).unwrap();
    let store = Store::open_or_create(&lance_path).await.unwrap();
    let embedder: Arc<dyn Embed> = Arc::new(MockEmbedder);
    let pipeline = IndexPipeline::with_embedder(db, store, embedder)
        .await
        .unwrap();

    write_md(
        &notes_dir,
        "expand.md",
        "## Expandable\n\nThis is content that will be expanded via the MCP tool.",
    );
    pipeline.full_scan(&[notes_dir]).await.unwrap();
    drop(pipeline);

    // Create fresh McpContext that sees the indexed data
    let store3 = Store::open_or_create(&lance_path).await.unwrap();
    let store3_reader = brain_persistence::store::StoreReader::from_store(&store3);
    let ctx_db3 = Db::open(&sqlite_path).unwrap();
    let stores3 =
        brain_lib::stores::BrainStores::from_dbs(ctx_db3, "", tmp.path(), tmp.path()).unwrap();
    let ctx = McpContext {
        stores: stores3,
        search: Some(brain_lib::search_service::SearchService {
            store: store3_reader,
            embedder: Arc::new(MockEmbedder),
        }),
        writable_store: Some(store3),
        metrics: Arc::new(brain_lib::metrics::Metrics::new()),
    };

    // Get the chunk_id
    let chunk_ids: Vec<String> = ctx
        .stores
        .db_for_tests()
        .with_read_conn(|conn: &rusqlite::Connection| {
            let mut stmt = conn.prepare("SELECT chunk_id FROM chunks")?;
            let rows = stmt.query_map([], |row| row.get(0))?;
            let mut ids = Vec::new();
            for row in rows {
                ids.push(row?);
            }
            Ok(ids)
        })
        .unwrap();

    assert!(!chunk_ids.is_empty());

    // Expand via MCP
    let params = json!({
        "memory_ids": chunk_ids,
        "budget_tokens": 2000
    });

    let registry = ToolRegistry::new();
    let result = registry
        .dispatch("memory.expand", params.clone(), &ctx)
        .await;
    assert!(result.is_error.is_none(), "expand should not error");

    let text = &result.content[0].text;
    let parsed: Value = serde_json::from_str(text).unwrap();

    let memories = parsed["memories"].as_array().unwrap();
    assert_eq!(memories.len(), 1);
    assert!(
        memories[0]["content"]
            .as_str()
            .unwrap()
            .contains("expanded via the MCP tool"),
        "expanded content should contain the original text"
    );
}

#[tokio::test]
async fn test_mcp_reflect_returns_sources() {
    let (ctx, _tmp) = setup_mcp().await;

    let registry = ToolRegistry::new();

    // Write some episodes first
    for i in 0..3 {
        let params = json!({
            "goal": format!("Goal {i}"),
            "actions": format!("Actions {i}"),
            "outcome": format!("Outcome {i}"),
            "tags": ["test"],
            "importance": 0.8
        });
        registry
            .dispatch("memory.write_episode", params.clone(), &ctx)
            .await;
    }

    // Reflect
    let params = json!({ "topic": "test progress", "budget_tokens": 2000 });
    let result = registry
        .dispatch("memory.reflect", params.clone(), &ctx)
        .await;
    assert!(result.is_error.is_none(), "reflect should not error");

    let text = &result.content[0].text;
    let parsed: Value = serde_json::from_str(text).unwrap();
    assert_eq!(parsed["topic"], "test progress");

    let episodes = parsed["episodes"].as_array().unwrap();
    assert_eq!(episodes.len(), 3, "should return all 3 episodes");
}

// ─── 8. Episodes round-trip ──────────────────────────────────────

#[test]
fn test_episode_store_and_list() {
    let db = Db::open_in_memory().unwrap();

    let ep1_id = db
        .with_write_conn(|conn| {
            store_episode(
                conn,
                &Episode {
                    brain_id: "brain-test".into(),
                    goal: "Build indexer".into(),
                    actions: "Wrote chunker and embedder".into(),
                    outcome: "Indexing works".into(),
                    tags: vec!["indexing".into()],
                    importance: 0.9,
                },
            )
        })
        .unwrap();

    let _ep2_id = db
        .with_write_conn(|conn| {
            store_episode(
                conn,
                &Episode {
                    brain_id: "brain-test".into(),
                    goal: "Add FTS5".into(),
                    actions: "Created virtual table and triggers".into(),
                    outcome: "Keyword search works".into(),
                    tags: vec!["search".into()],
                    importance: 0.8,
                },
            )
        })
        .unwrap();

    // List episodes
    let episodes = db
        .with_read_conn(|conn| list_episodes(conn, 10, ""))
        .unwrap();
    assert_eq!(episodes.len(), 2);

    // Get specific episode
    let ep = db
        .with_read_conn(|conn| get_summary(conn, &ep1_id))
        .unwrap()
        .unwrap();
    assert_eq!(ep.kind, "episode");
    assert!(ep.content.contains("Build indexer"));
    assert_eq!(ep.tags, vec!["indexing"]);
}

// ─── 10. Procedure kind surfaces in search_minimal ──────────────

/// Verify that a procedure stored in summaries (kind='procedure') is
/// returned with kind="procedure" by memory.search_minimal.
#[tokio::test]
async fn test_procedure_surfaces_in_search_minimal_with_kind_procedure() {
    use brain_lib::embedder::embed_batch_async;

    let tmp = TempDir::new().unwrap();
    let sqlite_path = tmp.path().join("brain.db");
    let lance_path = tmp.path().join("brain_lancedb");

    // 1. Open DB and store
    let db = Db::open(&sqlite_path).unwrap();
    let store = Store::open_or_create(&lance_path).await.unwrap();

    // 2. Insert a procedure row directly into summaries
    let procedure_content = "Step 1: do this. Step 2: do that. Procedure for quarterly review.";
    let summary_id: String = db
        .with_write_conn(|conn| {
            let id = ulid::Ulid::new().to_string();
            let now = brain_lib::utils::now_ts();
            conn.execute(
                "INSERT INTO summaries (summary_id, kind, title, content, tags, importance, brain_id, valid_from, created_at, updated_at)
                 VALUES (?1, 'procedure', ?2, ?3, '[]', 1.0, '', ?4, ?4, ?4)",
                rusqlite::params![id, "Quarterly Review Procedure", procedure_content, now],
            )?;
            Ok(id)
        })
        .unwrap();

    // 3. Embed it into LanceDB using MockEmbedder
    let embedder: Arc<dyn brain_lib::embedder::Embed> = Arc::new(brain_lib::embedder::MockEmbedder);
    let embedder_arc = Arc::clone(&embedder);
    let vecs = embed_batch_async(&embedder_arc, vec![procedure_content.to_string()])
        .await
        .unwrap();
    let vec = vecs.into_iter().next().unwrap();
    store
        .upsert_summary(&summary_id, procedure_content, "", &vec)
        .await
        .unwrap();

    drop(store);

    // 4. Build McpContext over the same DBs
    let store2 = Store::open_or_create(&lance_path).await.unwrap();
    let store2_reader = brain_persistence::store::StoreReader::from_store(&store2);
    let ctx_db = Db::open(&sqlite_path).unwrap();
    let stores2 =
        brain_lib::stores::BrainStores::from_dbs(ctx_db, "", tmp.path(), tmp.path()).unwrap();
    let ctx = brain_lib::mcp::McpContext {
        stores: stores2,
        search: Some(brain_lib::search_service::SearchService {
            store: store2_reader,
            embedder: Arc::new(brain_lib::embedder::MockEmbedder),
        }),
        writable_store: Some(store2),
        metrics: Arc::new(brain_lib::metrics::Metrics::new()),
    };

    // 5. Search — use the procedure's exact content so MockEmbedder yields a
    //    near-perfect vector match (same hash-based embedding).
    let params = serde_json::json!({
        "query": procedure_content,
        "intent": "lookup",
        "budget_tokens": 500,
        "k": 5
    });

    let registry = brain_lib::mcp::tools::ToolRegistry::new();
    let result = registry
        .dispatch("memory.search_minimal", params, &ctx)
        .await;
    assert!(
        result.is_error.is_none(),
        "search should not error: {:?}",
        result.content
    );

    let text = &result.content[0].text;
    let parsed: serde_json::Value = serde_json::from_str(text).unwrap();

    // 6. Assert at least one result has kind="procedure"
    let results = parsed["results"].as_array().expect("results array present");
    assert!(
        !results.is_empty(),
        "expected at least one search result, got: {text}"
    );

    let procedure_result = results
        .iter()
        .find(|r| r["kind"].as_str() == Some("procedure"));

    assert!(
        procedure_result.is_some(),
        "expected a result with kind='procedure', got kinds: {:?}",
        results
            .iter()
            .map(|r| r["kind"].as_str().unwrap_or("?"))
            .collect::<Vec<_>>()
    );
}

// ─── 9. Full pipeline with fixtures ──────────────────────────────

#[tokio::test]
async fn test_fixtures_fts_and_chunks_consistent() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    // Copy fixture files
    let fixtures_dir =
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures");

    for entry in std::fs::read_dir(&fixtures_dir).unwrap() {
        let entry = entry.unwrap();
        let src = entry.path();
        if src.extension().is_some_and(|e| e == "md") {
            let dst = notes_dir.join(entry.file_name());
            std::fs::copy(&src, &dst).unwrap();
        }
    }

    let stats = pipeline.full_scan(&[notes_dir]).await.unwrap();
    assert!(stats.indexed > 0, "should index fixture files");
    assert_eq!(stats.errors, 0, "no errors during fixture indexing");

    // Verify FTS has entries
    let fts_results = pipeline
        .db_for_tests()
        .with_read_conn(|conn| search_fts(conn, "vector", 10, None))
        .unwrap();
    assert!(
        !fts_results.is_empty(),
        "FTS should find 'vector' in fixtures"
    );

    // Verify chunks exist
    let chunk_count: i64 = pipeline
        .db_for_tests()
        .with_read_conn(|conn| {
            conn.query_row("SELECT COUNT(*) FROM chunks", [], |row| row.get(0))
                .map_err(|e| BrainCoreError::Database(e.to_string()))
        })
        .unwrap();
    assert!(
        chunk_count > 10,
        "should have many chunks from fixtures, got {chunk_count}"
    );

    // Verify FTS count matches chunk count (external-content sync)
    let fts_count: i64 = pipeline
        .db_for_tests()
        .with_read_conn(|conn| {
            conn.query_row("SELECT COUNT(*) FROM fts_chunks", [], |row| row.get(0))
                .map_err(|e| BrainCoreError::Database(e.to_string()))
        })
        .unwrap();
    assert_eq!(
        fts_count, chunk_count,
        "FTS index should match chunks table"
    );
}
