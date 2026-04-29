//! Acceptance tests for the `memory.retrieve` MCP tool.
//!
//! Run: cargo test -p brain-lib --test retrieve_acceptance_tests
//! Golden regen: cargo test -p brain-lib --test retrieve_acceptance_tests -- golden_generate --ignored --nocapture

mod mcp_test_harness;

use std::path::PathBuf;
use std::sync::Arc;

use brain_lib::embedder::{Embed, MockEmbedder};
use brain_lib::error::Result as BrainResult;
use brain_lib::mcp::McpContext;
use brain_lib::mcp::tools::ToolRegistry;
use brain_lib::metrics::Metrics;
use brain_lib::pipeline::IndexPipeline;
use brain_lib::query_pipeline::{FederatedPipeline, SearchParams};
use brain_lib::search_service::SearchService;
use brain_lib::stores::BrainStores;
use brain_persistence::store::{Store, StoreReader};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tempfile::TempDir;

// ─── Two-phase context builder ────────────────────────────────────────────────
//
// Phase 1: create the temp dir, open the Db, register the brain, index notes.
// Phase 2: open a fresh StoreReader from the now-populated LanceDB table and
//          build McpContext reusing the same Db (so resolve_brain succeeds).
//
// This order guarantees the StoreReader sees all indexed chunks (LanceDB
// snapshot isolation: a reader opened before writes cannot see those writes).

struct RetrieveCtx {
    _tmp: TempDir,
    pub ctx: McpContext,
    pub registry: ToolRegistry,
}

/// Seed notes, then build an McpContext over the populated lance store.
/// `embedder` is used for both indexing and query-time embedding.
async fn make_ctx_with_notes(notes: &[(&str, &str)], embedder: Arc<dyn Embed>) -> RetrieveCtx {
    let tmp = TempDir::new().expect("create tempdir");
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    let db_path = tmp.path().join("brain.db");
    let lance_path = tmp.path().join("lancedb");

    // Open DB and register the brain (so resolve_brain("test-brain") succeeds).
    let db = brain_persistence::db::Db::open(&db_path).expect("open db");
    db.upsert_brain(&brain_persistence::db::schema::BrainUpsert {
        brain_id: "test-brain-id",
        name: "test-brain",
        prefix: "TST",
        roots_json: "[]",
        notes_json: "[]",
        aliases_json: "[]",
        archived: false,
    })
    .expect("register brain");

    // Write note files.
    for (name, content) in notes {
        std::fs::write(notes_dir.join(name), content).unwrap();
    }

    // Phase 1: index all notes into the DB + lance store.
    {
        let store = Store::open_or_create(&lance_path)
            .await
            .expect("open lance");
        let mut pipeline = IndexPipeline::with_embedder(db.clone(), store, Arc::clone(&embedder))
            .await
            .expect("build pipeline");
        pipeline.set_brain_id("test-brain-id".to_string());
        let stats = pipeline.full_scan(&[notes_dir]).await.expect("full_scan");
        assert_eq!(stats.errors, 0, "indexing had errors");
        assert!(
            stats.indexed >= notes.len(),
            "expected ≥{} chunks indexed, got {}",
            notes.len(),
            stats.indexed
        );
    }

    // Phase 2: build BrainStores from the same Db (brain already registered),
    // open a fresh StoreReader from the now-populated lance table.
    let brain_data_dir = tmp.path().join("brains").join("test-brain");
    std::fs::create_dir_all(&brain_data_dir).unwrap();
    let stores = BrainStores::from_dbs(db, "test-brain-id", &brain_data_dir, tmp.path())
        .expect("build BrainStores from real Db");

    let writable_store = Store::open_or_create(&lance_path)
        .await
        .expect("open writable LanceDB");
    let store_reader = StoreReader::from_store(&writable_store);
    let search = SearchService {
        store: store_reader,
        embedder: Arc::clone(&embedder),
    };

    let ctx = McpContext {
        stores,
        search: Some(search),
        writable_store: Some(writable_store),
        metrics: Arc::new(Metrics::new()),
    };

    RetrieveCtx {
        _tmp: tmp,
        ctx,
        registry: ToolRegistry::new(),
    }
}

async fn make_ctx_with_notes_mock(notes: &[(&str, &str)]) -> RetrieveCtx {
    make_ctx_with_notes(notes, Arc::new(MockEmbedder) as Arc<dyn Embed>).await
}

// ─── Standard 5-chunk seed corpus ────────────────────────────────────────────

fn five_chunks() -> Vec<(&'static str, &'static str)> {
    vec![
        (
            "chunk_a.md",
            "# Alpha\n\nAlpha is about memory retrieval and semantic indexing.",
        ),
        (
            "chunk_b.md",
            "# Beta\n\nBeta covers vector embeddings and nearest-neighbour search.",
        ),
        (
            "chunk_c.md",
            "# Gamma\n\nGamma discusses hybrid search combining BM25 and cosine similarity.",
        ),
        (
            "chunk_d.md",
            "# Delta\n\nDelta explains ranking signals: recency, importance, backlinks.",
        ),
        (
            "chunk_e.md",
            "# Epsilon\n\nEpsilon is about query pipelines and LOD resolution.",
        ),
    ]
}

// ─── Golden helpers ───────────────────────────────────────────────────────────

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures")
        .join(name)
}

fn load_golden<T: for<'de> Deserialize<'de>>(name: &str) -> T {
    let text = std::fs::read_to_string(fixture_path(name))
        .unwrap_or_else(|e| panic!("golden fixture {name} missing: {e}"));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("golden fixture {name} parse error: {e}"))
}

fn save_golden<T: Serialize>(name: &str, value: &T) {
    let json = serde_json::to_string_pretty(value).unwrap();
    std::fs::write(fixture_path(name), json).unwrap();
    println!("  wrote {name}");
}

/// Strip volatile fields before golden comparison.
///
/// Stripped top-level: `query_time_ms`.
/// Stripped per-result: `generated_at`, `uri`, `source_uri` (contain ULID-based
/// chunk_ids that differ across test runs because each run uses a fresh TempDir
/// and fresh ULID generation), and `score` (f32 cosine-similarity reductions
/// drift in the last 6–7 decimal places across separate process invocations
/// even though they are deterministic *within* a single process — which is
/// what tests 5 and 6 already pin. The structural assertions of this golden
/// test — result count, ordering, `lod_plan_slot`, `expansion_reason`, `kind`
/// — are byte-stable and remain in the comparison).
fn strip_volatile_fields(mut v: Value) -> Value {
    if let Some(obj) = v.as_object_mut() {
        obj.remove("query_time_ms");
        if let Some(results) = obj.get_mut("results") {
            if let Some(arr) = results.as_array_mut() {
                for item in arr.iter_mut() {
                    if let Some(o) = item.as_object_mut() {
                        o.remove("generated_at");
                        o.remove("uri");
                        o.remove("source_uri");
                        o.remove("score");
                    }
                }
            }
        }
    }
    v
}

fn parse_tool_json(result: &brain_lib::mcp::protocol::ToolCallResult) -> Value {
    assert_ne!(
        result.is_error,
        Some(true),
        "tool error: {}",
        result.content[0].text
    );
    serde_json::from_str(&result.content[0].text).expect("tool result should be valid JSON")
}

// ─── Embedder for expansion-reason classification ─────────────────────────────

/// One-hot 384-dim embedder: distinct slots for VECONLY / KWONLY / HYBRIDTOK
/// markers so we can engineer exact vector-only, keyword-only, and hybrid hits.
struct ExpansionReasonEmbedder;

impl Embed for ExpansionReasonEmbedder {
    fn embed_batch(&self, texts: &[&str]) -> BrainResult<Vec<Vec<f32>>> {
        Ok(texts.iter().map(|t| Self::vec_for(t)).collect())
    }

    fn hidden_size(&self) -> usize {
        384
    }

    fn version(&self) -> &str {
        "expansion-reason-test-v0"
    }
}

impl ExpansionReasonEmbedder {
    fn vec_for(text: &str) -> Vec<f32> {
        let mut v = vec![0f32; 384];
        if text.contains("VECONLY") {
            v[0] = 1.0; // same slot as query → high cosine sim
        } else if text.contains("HYBRIDTOK") {
            v[0] = 0.707; // partial vector match
            v[1] = 0.707;
        } else {
            // KWONLY or unknown: orthogonal to query vector
            v[2] = 1.0;
        }
        // L2-normalise
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            for x in &mut v {
                *x /= norm;
            }
        }
        v
    }
}

// ─── 1. query_mode_returns_expected_results ───────────────────────────────────

#[tokio::test]
async fn query_mode_returns_expected_results() {
    let ctx = make_ctx_with_notes_mock(&five_chunks()).await;

    let result = ctx
        .registry
        .dispatch(
            "memory.retrieve",
            json!({ "query": "memory retrieval semantic", "lod": "L0", "count": 5 }),
            &ctx.ctx,
        )
        .await;

    let parsed = parse_tool_json(&result);
    let count = parsed["result_count"].as_u64().expect("result_count");
    assert_eq!(count, 5, "expected 5 results, got {count}");

    let results = parsed["results"].as_array().expect("results array");
    assert_eq!(results.len(), 5);
    for item in results {
        assert!(item["uri"].is_string(), "missing uri: {item}");
        assert!(!item["uri"].as_str().unwrap().is_empty(), "empty uri");
        assert!(item["score"].is_number(), "missing score: {item}");
        assert!(
            item["expansion_reason"].is_string(),
            "missing expansion_reason: {item}"
        );
        assert!(
            item["lod_plan_slot"].is_number(),
            "missing lod_plan_slot: {item}"
        );
    }
}

// ─── 2. query_mode_explainability_fields_populated ────────────────────────────

#[tokio::test]
async fn query_mode_explainability_fields_populated() {
    let ctx = make_ctx_with_notes_mock(&five_chunks()).await;

    let result = ctx
        .registry
        .dispatch(
            "memory.retrieve",
            json!({
                "query": "vector embeddings hybrid search",
                "lod": "L0",
                "count": 5,
                "explain": true
            }),
            &ctx.ctx,
        )
        .await;

    let parsed = parse_tool_json(&result);
    let results = parsed["results"].as_array().expect("results array");
    assert!(!results.is_empty(), "expected at least one result");

    for (i, item) in results.iter().enumerate() {
        // signals must be present and non-null when explain=true
        assert!(
            item.get("signals").is_some() && !item["signals"].is_null(),
            "result {i} missing signals: {item}"
        );
        // expansion_reason must be non-empty string
        let reason = item["expansion_reason"]
            .as_str()
            .expect("expansion_reason string");
        assert!(!reason.is_empty(), "result {i} has empty expansion_reason");
        // lod_plan_slot must equal the 0-based index
        let slot = item["lod_plan_slot"].as_u64().expect("lod_plan_slot");
        assert_eq!(slot, i as u64, "lod_plan_slot mismatch at index {i}");
    }
}

// ─── 3. query_mode_expansion_reason_classifies_correctly ─────────────────────

#[tokio::test]
async fn query_mode_expansion_reason_classifies_correctly() {
    let embedder = Arc::new(ExpansionReasonEmbedder) as Arc<dyn Embed>;

    // Notes engineered for specific expansion paths:
    // - vec_only.md:  VECONLY → vector slot 0 matches query; no BM25 keyword.
    // - kw_only.md:   KWONLY  → slot 2 (orthogonal to query); keyword "KWONLY" in query.
    // - hybrid.md:    VECONLY + KWONLY → vector AND keyword match.
    let ctx = make_ctx_with_notes(
        &[
            (
                "vec_only.md",
                "# VecOnly\n\nThis document has VECONLY signal for vector retrieval only.",
            ),
            (
                "kw_only.md",
                "# KwOnly\n\nThis document has KWONLY signal for keyword search only.",
            ),
            (
                "hybrid.md",
                "# Hybrid\n\nThis document has VECONLY signal and KWONLY signal both.",
            ),
        ],
        Arc::clone(&embedder),
    )
    .await;

    // Query contains KWONLY so BM25 fires on KWONLY docs.
    // Query vector (slot 0) matches VECONLY docs.
    let result = ctx
        .registry
        .dispatch(
            "memory.retrieve",
            json!({
                "query": "VECONLY KWONLY",
                "lod": "L0",
                "count": 10,
                "explain": true
            }),
            &ctx.ctx,
        )
        .await;

    let parsed = parse_tool_json(&result);
    let results = parsed["results"].as_array().expect("results array");
    assert!(!results.is_empty(), "expected at least one result");

    for item in results {
        let reason = item["expansion_reason"].as_str().unwrap_or("<missing>");
        assert!(
            matches!(reason, "vector_only" | "keyword_only" | "hybrid"),
            "unexpected expansion_reason {reason:?}: {item}"
        );
    }
}

// ─── 4. uri_mode_returns_uri_direct ──────────────────────────────────────────

#[tokio::test]
async fn uri_mode_returns_uri_direct() {
    let ctx =
        make_ctx_with_notes_mock(&[("single.md", "# Single\n\nA single chunk for URI test.")])
            .await;

    // Get the chunk_id from the DB (the brain_id scope matches "test-brain-id").
    let brain_id = ctx.ctx.brain_id().to_string();
    let chunk_id = ctx
        .ctx
        .stores
        .db_for_tests()
        .with_read_conn(|conn| {
            let mut stmt =
                conn.prepare("SELECT chunk_id FROM chunks WHERE brain_id = ?1 LIMIT 1")?;
            let id: String = stmt.query_row([&brain_id], |row| row.get(0))?;
            Ok(id)
        })
        .expect("get chunk_id from DB");

    // Build the synapse:// URI with the memory domain: synapse://<brain>/memory/<chunk_id>
    let uri = format!("synapse://test-brain/memory/{chunk_id}");

    let result = ctx
        .registry
        .dispatch("memory.retrieve", json!({ "uri": uri }), &ctx.ctx)
        .await;

    let parsed = parse_tool_json(&result);
    let results = parsed["results"].as_array().expect("results array");
    assert_eq!(results.len(), 1, "URI mode must return exactly 1 result");

    let item = &results[0];
    assert_eq!(
        item["expansion_reason"].as_str().unwrap_or(""),
        "uri_direct",
        "URI mode must have expansion_reason=uri_direct: {item}"
    );
    assert_eq!(
        item["lod_plan_slot"].as_u64().unwrap_or(99),
        0,
        "URI mode lod_plan_slot must be 0: {item}"
    );
}

// ─── 5. tied_scores_are_lexicographically_ordered ────────────────────────────

#[tokio::test]
async fn tied_scores_are_lexicographically_ordered() {
    // Two files with identical content produce the same MockEmbedder vector
    // (same hash → same vector) and the same BM25 score, so hybrid_score ties.
    // The ranking engine (ranking.rs:395-402) breaks ties by lex chunk_id order.
    let content = "# Note\n\nThis note is about memory retrieval semantic search topics.";
    let ctx =
        make_ctx_with_notes_mock(&[("aaaa_tied.md", content), ("zzzz_tied.md", content)]).await;

    let mut first_serialized: Option<String> = None;

    for run in 0..5 {
        let result = ctx
            .registry
            .dispatch(
                "memory.retrieve",
                json!({
                    "query": "memory retrieval semantic search",
                    "lod": "L0",
                    "count": 10
                }),
                &ctx.ctx,
            )
            .await;

        let parsed = parse_tool_json(&result);
        let results = parsed["results"].as_array().expect("results array");

        // Strip generated_at for byte-stable comparison.
        let stable: Vec<Value> = results
            .iter()
            .map(|item| {
                let mut o = item.clone();
                if let Some(obj) = o.as_object_mut() {
                    obj.remove("generated_at");
                }
                o
            })
            .collect();
        let serialized = serde_json::to_string(&stable).unwrap();

        if let Some(ref first) = first_serialized {
            assert_eq!(
                &serialized, first,
                "Run {run} results differ from run 0 — non-deterministic output"
            );
        } else {
            first_serialized = Some(serialized);
        }

        // Verify lex ordering for tied-score adjacent pairs.
        for window in results.windows(2) {
            let score0 = window[0]["score"].as_f64().unwrap_or(0.0);
            let score1 = window[1]["score"].as_f64().unwrap_or(0.0);
            if (score0 - score1).abs() < 1e-10 {
                let uri0 = window[0]["uri"].as_str().unwrap_or("");
                let uri1 = window[1]["uri"].as_str().unwrap_or("");
                assert!(
                    uri0 <= uri1,
                    "run {run}: tied scores not in lex uri order: {uri0} > {uri1}"
                );
            }
        }
    }
}

// ─── 6. federated_dedup_prefers_lex_smaller_brain_on_tie ─────────────────────

/// Minimal ChunkSearcher that always returns one pre-configured QueryResult.
/// Used to inject a known chunk_id into FederatedPipeline without real LanceDB.
struct SingleChunkSearcher {
    chunk_id: String,
    file_id: String,
    file_path: String,
    content: String,
}

impl brain_lib::ports::ChunkSearcher for SingleChunkSearcher {
    fn query<'a>(
        &'a self,
        _embedding: &'a [f32],
        _top_k: usize,
        _nprobes: usize,
        _mode: brain_persistence::store::VectorSearchMode,
        _brain_id: Option<&'a str>,
    ) -> impl std::future::Future<
        Output = brain_lib::error::Result<Vec<brain_persistence::store::QueryResult>>,
    > + Send
    + 'a {
        let result = brain_persistence::store::QueryResult {
            chunk_id: self.chunk_id.clone(),
            file_id: self.file_id.clone(),
            file_path: self.file_path.clone(),
            brain_id: String::new(),
            chunk_ord: 0,
            content: self.content.clone(),
            score: Some(0.05),
        };
        async move { Ok(vec![result]) }
    }
}

#[tokio::test]
async fn federated_dedup_prefers_lex_smaller_brain_on_tie() {
    // Uses FederatedPipeline with a mock ChunkSearcher so both brains return
    // the SAME chunk_id with identical scores. The dedup logic (query_pipeline.rs)
    // must attribute the result to the lex-smaller brain name.
    //
    // The MCP `memory.retrieve` with `brains:` uses `open_remote_search_context`
    // which requires on-disk brain configs, so we test at the pipeline level.

    const CHUNK_ID: &str = "shared-chunk:0";
    const FILE_ID: &str = "shared-file";
    const FILE_PATH: &str = "/shared/note.md";
    const CONTENT: &str = "This content is identical in both brains for tie testing.";

    // Shared DB — both brains share the same chunk_id (unified storage model).
    let tmp = TempDir::new().unwrap();
    let db = brain_persistence::db::Db::open(&tmp.path().join("shared.db")).unwrap();
    db.ensure_brain_registered("aaa-brain", "aaa-brain")
        .unwrap();
    db.ensure_brain_registered("zzz-brain", "zzz-brain")
        .unwrap();

    // Seed the shared chunk for both brains in the DB.
    db.with_write_conn(|conn| {
        conn.execute(
            "INSERT INTO files (file_id, path, indexing_state, brain_id)
             VALUES (?1, ?2, 'idle', 'aaa-brain')",
            rusqlite::params![FILE_ID, FILE_PATH],
        )?;
        conn.execute(
            "INSERT INTO chunks (chunk_id, file_id, chunk_ord, chunk_hash, content)
             VALUES (?1, ?2, 0, 'h0', ?3)",
            rusqlite::params![CHUNK_ID, FILE_ID, CONTENT],
        )?;
        Ok(())
    })
    .unwrap();

    let make_searcher = || SingleChunkSearcher {
        chunk_id: CHUNK_ID.to_string(),
        file_id: FILE_ID.to_string(),
        file_path: FILE_PATH.to_string(),
        content: CONTENT.to_string(),
    };

    let embedder: Arc<dyn Embed> = Arc::new(MockEmbedder);
    let metrics = Arc::new(Metrics::new());

    for run in 0..5 {
        let pipeline = FederatedPipeline {
            db: &db,
            brains: vec![
                (
                    "aaa-brain".to_string(),
                    "aaa-brain".to_string(),
                    Some(make_searcher()),
                ),
                (
                    "zzz-brain".to_string(),
                    "zzz-brain".to_string(),
                    Some(make_searcher()),
                ),
            ],
            embedder: &embedder,
            metrics: &metrics,
        };

        let params = SearchParams::new("identical content", "auto", 0, 10, &[]);
        let result = pipeline.search_ranked_federated(&params).await.unwrap();

        // The shared chunk must be attributed to "aaa-brain" (lex-smaller) on every run.
        assert!(
            !result.chunk_brain.is_empty(),
            "run {run}: expected at least one result in chunk_brain"
        );
        for (chunk_id, brain_name) in &result.chunk_brain {
            assert_eq!(
                brain_name, "aaa-brain",
                "run {run}: federated dedup must prefer lex-smaller brain for {chunk_id:?}, got {brain_name:?}"
            );
        }
    }
}

// ─── 7. golden_query_response_byte_stable ────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
struct GoldenRetrieve {
    description: String,
    query: String,
    /// Full response with volatile fields stripped.
    response: Value,
}

fn golden_notes() -> Vec<(&'static str, &'static str)> {
    vec![
        (
            "golden_a.md",
            "# Memory Retrieval\n\nSemantic memory retrieval uses vector embeddings.",
        ),
        (
            "golden_b.md",
            "# Search Pipeline\n\nThe search pipeline combines BM25 and cosine similarity.",
        ),
        (
            "golden_c.md",
            "# LOD Resolution\n\nLevel-of-detail resolution produces extractive summaries.",
        ),
    ]
}

#[tokio::test]
async fn golden_query_response_byte_stable() {
    let ctx = make_ctx_with_notes_mock(&golden_notes()).await;

    let raw = ctx
        .registry
        .dispatch(
            "memory.retrieve",
            json!({
                "query": "semantic memory retrieval",
                "lod": "L0",
                "count": 3
            }),
            &ctx.ctx,
        )
        .await;

    let parsed = parse_tool_json(&raw);
    // Strip volatile wall-clock fields before comparing:
    //   - query_time_ms: varies per run
    //   - generated_at (per result): set during LOD resolution
    let stable = strip_volatile_fields(parsed);

    let expected: GoldenRetrieve = load_golden("golden/retrieve_query_default.json");
    assert_eq!(
        serde_json::to_string_pretty(&stable).unwrap(),
        serde_json::to_string_pretty(&expected.response).unwrap(),
        "golden fixture mismatch — regenerate with:\n  cargo test -p brain-lib --test retrieve_acceptance_tests -- golden_generate --ignored --nocapture"
    );
}

/// Regenerate the golden fixture (run with `--ignored`).
///
/// Volatile fields stripped before saving: `query_time_ms`, `generated_at`.
#[test]
#[ignore]
fn golden_generate() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let ctx = make_ctx_with_notes_mock(&golden_notes()).await;

        let raw = ctx
            .registry
            .dispatch(
                "memory.retrieve",
                json!({
                    "query": "semantic memory retrieval",
                    "lod": "L0",
                    "count": 3
                }),
                &ctx.ctx,
            )
            .await;

        let parsed = parse_tool_json(&raw);
        let stable = strip_volatile_fields(parsed);

        let fixture = GoldenRetrieve {
            description: "Golden fixture for memory.retrieve query mode. \
                Volatile fields stripped before saving: query_time_ms (top-level), \
                generated_at (per-result)."
                .into(),
            query: "semantic memory retrieval".into(),
            response: stable,
        };

        std::fs::create_dir_all(fixture_path("golden")).unwrap();
        save_golden("golden/retrieve_query_default.json", &fixture);
        println!("Golden fixture written.");
    });
}
