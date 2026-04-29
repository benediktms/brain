//! End-to-end coverage for the `tags.*` MCP surface (`brn-83a.7.2.5`).
//!
//! Drives `tags.recluster` against an in-memory brain seeded with a few
//! synonym-pair tags, then asserts that the report counts match what
//! `tags.aliases_list` and `tags.aliases_status` see afterward. Pins the
//! contract that the three tools agree on the shape of the brain after a
//! recluster pass.

use std::sync::Arc;

use brain_lib::embedder::MockEmbedder;
use brain_lib::mcp::McpContext;
use brain_lib::mcp::tools::ToolRegistry;
use brain_lib::metrics::Metrics;
use brain_lib::search_service::SearchService;
use brain_lib::stores::BrainStores;
use brain_persistence::db::tag_aliases as ta;
use brain_persistence::store::{Store, StoreReader};
use serde_json::{Value, json};

async fn build_ctx() -> (tempfile::TempDir, McpContext) {
    let (tmp, stores) =
        BrainStores::in_memory_with_brain("tags-mcp", "tags-mcp", "TST").expect("boot stores");
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
    (tmp, ctx)
}

#[tokio::test]
async fn tags_mcp_surface_round_trip() {
    let (_tmp, ctx) = build_ctx().await;
    let registry = ToolRegistry::new();
    let brain_id = ctx.stores.brain_id.clone();

    // Seed three records and one task across distinct raw tags.
    {
        let bid = brain_id.clone();
        ctx.stores
            .db_for_tests()
            .with_write_conn(move |conn| {
                ta::seed_record_with_tags(conn, "r1", &bid, 1000, &["bug", "perf"])?;
                ta::seed_record_with_tags(conn, "r2", &bid, 2000, &["bug"])?;
                ta::seed_record_with_tags(conn, "r3", &bid, 1500, &["docs"])?;
                ta::seed_task_with_labels(conn, "t1", &bid, 4000, &["docs"])?;
                Ok(())
            })
            .unwrap();
    }

    // 1. Status before any recluster: empty.
    let pre_status = registry
        .dispatch("tags.aliases_status", json!({}), &ctx)
        .await;
    let pre: Value = serde_json::from_str(&pre_status.content[0].text).expect("valid json");
    assert!(pre["last_run"].is_null());
    assert_eq!(pre["total_aliases"], 0);
    assert_eq!(pre["total_clusters"], 0);

    // 2. Trigger tags.recluster.
    let recluster_result = registry.dispatch("tags.recluster", json!({}), &ctx).await;
    assert!(
        recluster_result.is_error.is_none(),
        "tags.recluster should not error: {}",
        recluster_result.content[0].text
    );
    let report: Value =
        serde_json::from_str(&recluster_result.content[0].text).expect("valid json");
    assert_eq!(
        report["source_count"], 3,
        "expected 3 raw tags after dedupe, got {report}"
    );
    let new_aliases = report["new_aliases"].as_u64().expect("number");
    assert_eq!(new_aliases, 3, "expected 3 new alias rows: {report}");
    let cluster_count = report["cluster_count"].as_u64().expect("number");

    // 3. tags.aliases_list returns the rows we just persisted.
    let list_result = registry
        .dispatch("tags.aliases_list", json!({}), &ctx)
        .await;
    assert!(
        list_result.is_error.is_none(),
        "{}",
        list_result.content[0].text
    );
    let list: Value = serde_json::from_str(&list_result.content[0].text).expect("valid json");
    let aliases = list["aliases"].as_array().expect("aliases array");
    assert_eq!(
        aliases.len() as u64,
        new_aliases,
        "alias count from list must match the report"
    );
    let raw_tags: std::collections::HashSet<&str> = aliases
        .iter()
        .map(|a| a["raw_tag"].as_str().expect("raw_tag string"))
        .collect();
    for tag in ["bug", "perf", "docs"] {
        assert!(raw_tags.contains(tag), "missing {tag} in {raw_tags:?}");
    }

    // 4. Filter by canonical tag — picks one cluster's worth of rows.
    let canonical = aliases[0]["canonical_tag"]
        .as_str()
        .expect("canonical_tag string")
        .to_string();
    let filtered = registry
        .dispatch("tags.aliases_list", json!({ "canonical": canonical }), &ctx)
        .await;
    assert!(filtered.is_error.is_none(), "{}", filtered.content[0].text);
    let filtered: Value = serde_json::from_str(&filtered.content[0].text).expect("valid json");
    let filtered_aliases = filtered["aliases"].as_array().expect("array");
    assert!(
        !filtered_aliases.is_empty(),
        "filter by canonical={canonical} should match at least one row"
    );
    for a in filtered_aliases {
        assert_eq!(a["canonical_tag"], json!(canonical));
    }

    // 5. tags.aliases_status agrees with the report.
    let post_status = registry
        .dispatch("tags.aliases_status", json!({}), &ctx)
        .await;
    let post: Value = serde_json::from_str(&post_status.content[0].text).expect("valid json");
    assert!(!post["last_run"].is_null(), "last_run must be populated");
    assert_eq!(post["total_aliases"], new_aliases);
    assert_eq!(post["total_clusters"], cluster_count);
    assert_eq!(post["current_embedder_version"], "mock-v1");
    let coverage = &post["alias_coverage"];
    assert_eq!(coverage["raw_count"], new_aliases);
    let canonical_count = coverage["canonical_count"].as_u64().expect("number");
    assert!(canonical_count >= 1);
}
