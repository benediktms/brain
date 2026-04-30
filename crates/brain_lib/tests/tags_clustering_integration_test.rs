//! End-to-end correctness gate for the synonym-clustering pipeline (`brn-83a.7.2.6`).
//!
//! Pins the contract `[seed → recluster → search via canonical → get aliased
//! hits, no row mutation]` across the real `BrainStores`, `IndexPipeline`,
//! `run_recluster`, and the `memory.retrieve` MCP handler. A sibling
//! task (`brn-83a.7.2.5`) will add MCP/CLI surface for `tags.recluster`; until
//! then this test calls `run_recluster` directly.

mod mcp_test_harness;

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

use brain_lib::embedder::{Embed, MockEmbedder};
use brain_lib::mcp::McpContext;
use brain_lib::mcp::tools::ToolRegistry;
use brain_lib::metrics::Metrics;
use brain_lib::pipeline::IndexPipeline;
use brain_lib::search_service::SearchService;
use brain_lib::stores::BrainStores;
use brain_lib::{ClusterParams, run_recluster};
use brain_persistence::db::tag_aliases as ta;
use brain_persistence::store::{Store, StoreReader};
use mcp_test_harness::ControlledEmbedder;
use serde_json::{Value, json};

#[tokio::test]
async fn tag_clustering_end_to_end() {
    // 1. Boot the harness — in-memory stores, writable LanceDB, MCP context.
    let (tmp, stores) =
        BrainStores::in_memory_with_brain("e2e-brain", "e2e-brain", "TST").expect("boot stores");
    let lance_path = tmp.path().join("lance");
    let writable_store = Store::open_or_create(&lance_path)
        .await
        .expect("open writable LanceDB");
    let store_reader = StoreReader::from_store(&writable_store);
    let search = SearchService {
        store: store_reader,
        embedder: Arc::new(MockEmbedder),
    };
    let ctx = McpContext {
        stores,
        search: Some(search),
        writable_store: Some(writable_store),
        metrics: Arc::new(Metrics::new()),
    };
    let registry = ToolRegistry::new();
    let brain_id = ctx.stores.brain_id.clone();

    // 2. Seed records and tasks across the eight raw tags. Records exercise
    //    `record_tags`; tasks exercise `task_labels` — both feed
    //    `collect_raw_tags`.
    {
        let bid = brain_id.clone();
        ctx.stores
            .db_for_tests()
            .with_write_conn(move |conn| {
                ta::seed_record_with_tags(conn, "r1", &bid, 1000, &["bug"])?;
                ta::seed_record_with_tags(conn, "r2", &bid, 2000, &["bugs"])?;
                ta::seed_record_with_tags(conn, "r3", &bid, 3000, &["defect"])?;
                ta::seed_record_with_tags(conn, "r4", &bid, 4000, &["performance"])?;
                ta::seed_record_with_tags(conn, "r5", &bid, 5000, &["perf"])?;
                ta::seed_record_with_tags(conn, "r6", &bid, 6000, &["docs"])?;
                ta::seed_task_with_labels(conn, "t1", &bid, 7000, &["documentation"])?;
                ta::seed_task_with_labels(conn, "t2", &bid, 8000, &["chore"])?;
                Ok(())
            })
            .expect("seed records and tasks");
    }

    // 3. Capture pre-test full row contents for the audit invariant in step 9.
    //    Snapshot the (parent_id, tag) tuples — strictly stronger than a row
    //    count because it catches an UPDATE that rewrites a tag value while
    //    preserving the row count (no such code path exists today, but the
    //    invariant should fail-loud regardless).
    let (rt_before, tl_before) = snapshot_tag_rows(&ctx);
    assert_eq!(rt_before.len(), 6, "pre: 6 record_tags rows");
    assert_eq!(tl_before.len(), 2, "pre: 2 task_labels rows");

    // 4. Run recluster with the ControlledEmbedder. Pin threshold explicitly
    //    so a future PR bumping the default doesn't silently break us.
    let embedder: Arc<dyn Embed> = Arc::new(ControlledEmbedder);
    let params = ClusterParams {
        cosine_threshold: 0.85,
    };
    let report = run_recluster(&ctx.stores, &embedder, params)
        .await
        .expect("first recluster ok");

    assert_eq!(report.source_count, 8, "8 distinct raw tags");
    assert_eq!(
        report.cluster_count, 4,
        "expected 4 clusters: bug/bugs/defect, performance/perf, docs/documentation, chore"
    );
    assert_eq!(report.new_aliases, 8, "every raw tag becomes an alias row");
    assert_eq!(report.updated_aliases, 0);
    assert_eq!(report.stale_aliases, 0);

    // 5. Verify cluster topology via the alias snapshot.
    let snapshot = ctx
        .stores
        .db_for_tests()
        .with_read_conn(|conn| ta::read_alias_snapshot(conn, &brain_id))
        .expect("alias snapshot");
    let canonical = |t: &str| {
        snapshot
            .get(t)
            .unwrap_or_else(|| panic!("missing alias row for {t}"))
            .canonical_tag
            .clone()
    };

    assert_eq!(canonical("bug"), canonical("bugs"));
    assert_eq!(canonical("bug"), canonical("defect"));
    assert_eq!(canonical("performance"), canonical("perf"));
    assert_eq!(canonical("docs"), canonical("documentation"));

    let canons: HashSet<String> = [
        canonical("bug"),
        canonical("performance"),
        canonical("docs"),
        canonical("chore"),
    ]
    .into_iter()
    .collect();
    assert_eq!(canons.len(), 4, "four distinct canonical_tag values");

    // 6. Seed three indexed notes for the search filter to match. The notes'
    //    raw tags (`bug`, `bugs`, `defect`) must already exist in the alias
    //    table from step 4 for canonical→raw expansion to fire.
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).expect("create notes dir");
    for (file, raw_tag) in [
        ("note_bug.md", "bug"),
        ("note_bugs.md", "bugs"),
        ("note_defect.md", "defect"),
    ] {
        let content =
            format!("---\ntags:\n  - {raw_tag}\n---\n\n# {raw_tag} note\n\nFixture body.\n");
        std::fs::write(notes_dir.join(file), content).expect("write note markdown");
    }

    // 7. Index notes via IndexPipeline. Use the same brain_id the MCP context
    //    queries under; use `MockEmbedder` for note text (the search filter
    //    only touches `chunks.tags`, so vector quality doesn't gate this test).
    let db_for_idx = ctx.stores.db_for_tests().clone();
    let store_for_idx = Store::open_or_create(&lance_path)
        .await
        .expect("reopen LanceDB for indexing");
    let mut pipeline =
        IndexPipeline::with_embedder(db_for_idx, store_for_idx, Arc::new(MockEmbedder))
            .await
            .expect("build IndexPipeline");
    pipeline.set_brain_id(brain_id.clone());
    let stats = pipeline
        .full_scan(std::slice::from_ref(&notes_dir))
        .await
        .expect("index note fixtures");
    assert_eq!(stats.errors, 0, "no indexing errors");
    assert!(stats.indexed >= 3, "at least one chunk per note");

    // 8. Run memory.retrieve via the MCP harness with
    //    tags_require=["bug"]. The canonical→raw expansion (`brn-83a.7.2.4.3`)
    //    should widen the filter to {bug, bugs, defect}, surfacing all three
    //    notes.
    let result = registry
        .dispatch(
            "memory.retrieve",
            json!({ "query": "fixture body", "tags_require": ["bug"], "lod": "L0", "count": 10 }),
            &ctx,
        )
        .await;
    assert_ne!(
        result.is_error,
        Some(true),
        "search must succeed: {}",
        result.content[0].text
    );
    let parsed: Value =
        serde_json::from_str(&result.content[0].text).expect("response is valid JSON");
    let titles: Vec<String> = parsed["results"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|r| r["title"].as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();
    let combined = titles.join(" | ");

    assert!(
        combined.contains("bug note"),
        "literal match for 'bug' missing: {combined}",
    );
    assert!(
        combined.contains("bugs note"),
        "alias-expanded 'bugs' missing — `expand_tags_via_aliases` regression? results: {combined}",
    );
    assert!(
        combined.contains("defect note"),
        "alias-expanded 'defect' missing — `expand_tags_via_aliases` regression? results: {combined}",
    );

    // 9. Audit invariant: search must not mutate raw-tag rows. Compare full
    //    (parent_id, tag) tuple sets — catches inserts, deletes, AND in-place
    //    value mutations.
    let (rt_after, tl_after) = snapshot_tag_rows(&ctx);
    assert_eq!(
        rt_before, rt_after,
        "record_tags content must not change during search",
    );
    assert_eq!(
        tl_before, tl_after,
        "task_labels content must not change during search",
    );

    // 10. Idempotence: a re-run on identical data must produce zero
    //     upserts.
    let report2 = run_recluster(&ctx.stores, &embedder, params)
        .await
        .expect("second recluster ok");
    assert_eq!(report2.new_aliases, 0, "rerun produced no new aliases");
    assert_eq!(
        report2.updated_aliases, 0,
        "rerun produced no alias updates",
    );
    assert_eq!(report2.cluster_count, 4, "rerun: same four clusters");

    // CLI surface coverage note: the `brain memory retrieve --tags-require` /
    // `--tags-exclude` flags share the same `query_pipeline::search` code path
    // exercised above (the dispatcher feeds `tags_require`/`tags_exclude` into
    // the same `SearchParams` builder). A subprocess test from this crate
    // can't see the `brain` binary (different cargo package, no
    // `CARGO_BIN_EXE_brain`) — flag-wiring smoke belongs in
    // `crates/cli/tests/` if we add it, but the manual `cargo run -- memory
    // retrieve --help` smoke during implementation already validates the flags
    // are exposed and documented.
}

/// Snapshot `record_tags` and `task_labels` as sorted tuple sets so the test
/// can assert exact content equality (not just row counts) before/after
/// search. `BTreeSet` gives deterministic iteration if a future failure
/// message wants to render the diff.
fn snapshot_tag_rows(ctx: &McpContext) -> (BTreeSet<(String, String)>, BTreeSet<(String, String)>) {
    ctx.stores
        .db_for_tests()
        .with_read_conn(|conn| {
            let mut rt = BTreeSet::new();
            let mut stmt = conn.prepare("SELECT record_id, tag FROM record_tags")?;
            let rows = stmt.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            for row in rows {
                rt.insert(row?);
            }

            let mut tl = BTreeSet::new();
            let mut stmt = conn.prepare("SELECT task_id, label FROM task_labels")?;
            let rows = stmt.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            for row in rows {
                tl.insert(row?);
            }

            Ok((rt, tl))
        })
        .expect("snapshot tag rows")
}
