//! Property-based tests using proptest for invariant verification.
//!
//! Pure tests use the default 256 cases; async/DB tests use reduced counts.

use std::collections::HashSet;
use std::sync::Arc;

use brain_lib::chunker::chunk_document;
use brain_lib::embedder::MockEmbedder;
use brain_lib::parser::parse_document;
use brain_lib::prelude::*;
use brain_lib::ranking::{RankedResult, SignalScores};
use brain_lib::retrieval::{expand_results, pack_minimal};
use brain_lib::tasks::events::*;
use brain_lib::tasks::projections::apply_event;
use brain_lib::tasks::queries::{list_blocked, list_open, list_ready};
use brain_persistence::db::Db;
use proptest::prelude::*;
use rusqlite::Connection;
use tempfile::TempDir;

// ─── Helpers ─────────────────────────────────────────────────────

fn markdown_strategy() -> impl Strategy<Value = String> {
    prop::collection::vec(
        prop_oneof![
            Just("# Heading\n\nA paragraph of text here.\n\n".to_string()),
            Just("## Subheading\n\nAnother paragraph with more words.\n\n".to_string()),
            Just("### Deep heading\n\nDeep content goes here.\n\n".to_string()),
            Just("Plain text without headings.\n\n".to_string()),
            "[a-zA-Z ]{20,200}\n\n",
        ],
        1..8,
    )
    .prop_map(|parts| parts.join(""))
}

fn make_ranked(id: &str, score: f64, content: &str) -> RankedResult {
    RankedResult {
        chunk_id: id.to_string(),
        hybrid_score: score,
        scores: SignalScores {
            vector: score,
            keyword: 0.0,
            recency: 0.0,
            links: 0.0,
            tag_match: 0.0,
            importance: 1.0,
        },
        file_path: format!("/notes/{id}.md"),
        heading_path: format!("## {id}"),
        content: content.to_string(),
        token_estimate: estimate_tokens(content),
        byte_start: 0,
        byte_end: 0,
        summary_kind: None,
    }
}

fn make_event(
    evt_num: u32,
    ts: i64,
    task_id: &str,
    event_type: EventType,
    payload: serde_json::Value,
) -> TaskEvent {
    TaskEvent {
        event_id: format!("evt-{evt_num}"),
        task_id: task_id.to_string(),
        timestamp: ts,
        actor: "test".to_string(),
        event_type,
        event_version: CURRENT_EVENT_VERSION,
        payload,
    }
}

async fn setup_pipeline() -> (IndexPipeline, TempDir) {
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

// ─── Property #1: Upsert safety ─────────────────────────────────
//
// Modifying and re-indexing file A must not corrupt file B's chunks.

proptest! {
    #![proptest_config(ProptestConfig::with_cases(20))]
    #[test]
    fn prop_upsert_safety(
        content_a in "[a-zA-Z ]{10,100}",
        content_b in "[a-zA-Z ]{10,100}",
        modified_a in "[a-zA-Z ]{10,100}",
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (pipeline, tmp) = setup_pipeline().await;
            let notes = tmp.path().join("notes");
            std::fs::create_dir_all(&notes).unwrap();

            std::fs::write(notes.join("a.md"), format!("# A\n\n{content_a}\n")).unwrap();
            std::fs::write(notes.join("b.md"), format!("# B\n\n{content_b}\n")).unwrap();
            pipeline.full_scan(std::slice::from_ref(&notes)).await.unwrap();

            // Capture B's chunk hashes via a direct connection
            let db_path = tmp.path().join("brain.db");
            let conn = Connection::open(&db_path).unwrap();
            let before: Vec<String> = conn
                .prepare(
                    "SELECT chunk_hash FROM chunks c \
                     JOIN files f ON c.file_id = f.file_id \
                     WHERE f.path LIKE '%b.md' ORDER BY c.chunk_ord",
                )
                .unwrap()
                .query_map([], |row| row.get(0))
                .unwrap()
                .map(|r| r.unwrap())
                .collect();
            drop(conn);

            // Modify A and re-index
            std::fs::write(notes.join("a.md"), format!("# A\n\n{modified_a}\n")).unwrap();
            pipeline.full_scan(&[notes]).await.unwrap();

            // Verify B unchanged
            let conn = Connection::open(&db_path).unwrap();
            let after: Vec<String> = conn
                .prepare(
                    "SELECT chunk_hash FROM chunks c \
                     JOIN files f ON c.file_id = f.file_id \
                     WHERE f.path LIKE '%b.md' ORDER BY c.chunk_ord",
                )
                .unwrap()
                .query_map([], |row| row.get(0))
                .unwrap()
                .map(|r| r.unwrap())
                .collect();

            assert_eq!(before, after, "B's chunks changed after re-indexing A");
        });
    }
}

// ─── Property #2: Hash determinism ──────────────────────────────
//
// content_hash must return identical results for identical input.

proptest! {
    #[test]
    fn prop_hash_determinism(s in "\\PC{0,500}") {
        let h1 = content_hash(&s);
        let h2 = content_hash(&s);
        prop_assert_eq!(h1, h2);
    }
}

// ─── Property #3: Chunk completeness ────────────────────────────
//
// Every word from parsed sections must appear in at least one chunk.

proptest! {
    #[test]
    fn prop_chunk_completeness(text in markdown_strategy()) {
        let doc = parse_document(&text);
        let chunks = chunk_document(&doc);

        let section_words: HashSet<&str> = doc
            .sections
            .iter()
            .filter(|s| !s.content.trim().is_empty())
            .flat_map(|s| s.content.split_whitespace())
            .collect();

        let chunk_words: HashSet<&str> = chunks
            .iter()
            .flat_map(|c| c.content.split_whitespace())
            .collect();

        for word in &section_words {
            prop_assert!(
                chunk_words.contains(word),
                "word '{}' from sections not found in any chunk",
                word
            );
        }
    }
}

// ─── Property #4: Roundtrip ─────────────────────────────────────
//
// Index a random markdown file, query by exact first-chunk content,
// verify the file appears in results.

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10))]
    #[test]
    fn prop_roundtrip(text in markdown_strategy()) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (pipeline, tmp) = setup_pipeline().await;
            let notes = tmp.path().join("notes");
            std::fs::create_dir_all(&notes).unwrap();

            let md = format!("# Test\n\n{text}\n");
            std::fs::write(notes.join("test.md"), &md).unwrap();
            let stats = pipeline.full_scan(&[notes]).await.unwrap();
            assert_eq!(stats.errors, 0);

            if stats.indexed > 0 {
                let chunks = chunk_text(&md);
                if let Some(first) = chunks.first() {
                    let embedder = MockEmbedder;
                    let query_vec = embedder
                        .embed_batch(&[first.content.as_str()])
                        .unwrap();
                    let store = Store::open_or_create(&tmp.path().join("brain_lancedb"))
                        .await
                        .unwrap();
                    let results = store.query(&query_vec[0], 3, 20, Default::default()).await.unwrap();
                    assert!(
                        results.iter().any(|r| r.file_path.ends_with("test.md")),
                        "indexed file should appear in query results"
                    );
                }
            }
        });
    }
}

// ─── Property #5: Budget compliance ─────────────────────────────
//
// pack_minimal: first result always included; for 2+ results budget is respected.
// expand_results: similar invariant with truncation tolerance.

proptest! {
    #[test]
    fn prop_budget_compliance_pack(
        budget in 1usize..5000,
        n in 1usize..20,
    ) {
        let ranked: Vec<RankedResult> = (0..n)
            .map(|i| {
                make_ranked(
                    &format!("c{i}"),
                    1.0 - (i as f64 * 0.05),
                    &"word ".repeat(10 + i),
                )
            })
            .collect();

        let result = pack_minimal(&ranked, budget, n, false, &std::collections::HashMap::new());

        // First result always included even if over budget
        prop_assert!(
            result.num_results <= 1 || result.used_tokens_est <= budget,
            "pack_minimal budget violated: used={}, budget={}, num={}",
            result.used_tokens_est,
            budget,
            result.num_results
        );
    }

    #[test]
    fn prop_budget_compliance_expand(
        budget in 1usize..5000,
        n in 1usize..20,
    ) {
        let ranked: Vec<RankedResult> = (0..n)
            .map(|i| {
                make_ranked(
                    &format!("c{i}"),
                    1.0 - (i as f64 * 0.05),
                    &"word ".repeat(10 + i),
                )
            })
            .collect();

        let result = expand_results(&ranked, budget);

        // At most as many results as input
        prop_assert!(result.memories.len() <= n);

        // If we have more than one non-truncated memory, the non-truncated portion
        // should be within budget
        let non_truncated: Vec<_> = result.memories.iter().filter(|m| !m.truncated).collect();
        if non_truncated.len() > 1 {
            let non_trunc_tokens: usize = non_truncated
                .iter()
                .map(|m| estimate_tokens(&m.content))
                .sum();
            prop_assert!(
                non_trunc_tokens <= budget,
                "expand non-truncated tokens {} exceed budget {}",
                non_trunc_tokens,
                budget
            );
        }
    }
}

// ─── Property #6: Task event ordering ───────────────────────────
//
// Creating N tasks + random status changes must never lose a task.

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    #[test]
    fn prop_task_event_ordering(
        n in 1usize..10,
        do_status_changes in prop::collection::vec(prop::bool::ANY, 1..10),
    ) {
        let db = Db::open_in_memory().unwrap();
        let count = db.with_write_conn(|conn| {
            let mut ts = 1700000000i64;
            let mut evt_num = 0u32;

            for i in 0..n {
                evt_num += 1;
                ts += 1;
                let event = make_event(
                    evt_num,
                    ts,
                    &format!("T{i}"),
                    EventType::TaskCreated,
                    serde_json::to_value(TaskCreatedPayload {
                        title: format!("Task {i}"),
                        description: None,
                        priority: 2,
                        status: TaskStatus::Open,
                        due_ts: None,
                        task_type: None,
                        assignee: None,
                        defer_until: None,
                        parent_task_id: None,
                        display_id: None,
                    })
                    .unwrap(),
                );
                apply_event(conn, &event, "")?;
            }

            for (i, &do_change) in do_status_changes.iter().enumerate() {
                if !do_change || n == 0 {
                    continue;
                }
                let task_idx = i % n;
                evt_num += 1;
                ts += 1;
                let event = make_event(
                    evt_num,
                    ts,
                    &format!("T{task_idx}"),
                    EventType::StatusChanged,
                    serde_json::to_value(StatusChangedPayload {
                        new_status: TaskStatus::InProgress,
                    })
                    .unwrap(),
                );
                let _ = apply_event(conn, &event, "");
            }

            let count: i64 =
                conn.query_row("SELECT COUNT(*) FROM tasks", [], |row| row.get(0))?;
            Ok(count)
        }).unwrap();

        prop_assert_eq!(count as usize, n);
    }
}

// ─── Property #7: Ready/blocked consistency ─────────────────────
//
// Invariant 1: ready ∩ blocked = ∅
// Invariant 2: ready ∪ blocked covers all non-terminal tasks

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    #[test]
    fn prop_ready_blocked_consistency(
        n in 2usize..8,
        edges in prop::collection::vec((0usize..8, 0usize..8), 0..10),
    ) {
        let db = Db::open_in_memory().unwrap();
        let (ready_ids, blocked_ids, open_ids) = db.with_write_conn(|conn| {
            let mut ts = 1700000000i64;
            let mut evt_num = 0u32;

            for i in 0..n {
                evt_num += 1;
                ts += 1;
                let event = make_event(
                    evt_num,
                    ts,
                    &format!("T{i}"),
                    EventType::TaskCreated,
                    serde_json::to_value(TaskCreatedPayload {
                        title: format!("Task {i}"),
                        description: None,
                        priority: 2,
                        status: TaskStatus::Open,
                        due_ts: None,
                        task_type: None,
                        assignee: None,
                        defer_until: None,
                        parent_task_id: None,
                        display_id: None,
                    })
                    .unwrap(),
                );
                apply_event(conn, &event, "")?;
            }

            for (from, to) in &edges {
                if *from >= n || *to >= n || from == to {
                    continue;
                }
                evt_num += 1;
                ts += 1;
                let event = make_event(
                    evt_num,
                    ts,
                    &format!("T{from}"),
                    EventType::DependencyAdded,
                    serde_json::to_value(DependencyPayload {
                        depends_on_task_id: format!("T{to}"),
                    })
                    .unwrap(),
                );
                // Gracefully skip duplicate/cycle errors
                let _ = apply_event(conn, &event, "");
            }

            let ready = list_ready(conn, None)?;
            let blocked = list_blocked(conn, None)?;
            let open = list_open(conn, None)?;

            let ready_ids: HashSet<String> = ready.iter().map(|r| r.task_id.clone()).collect();
            let blocked_ids: HashSet<String> =
                blocked.iter().map(|r| r.task_id.clone()).collect();
            let open_ids: HashSet<String> = open.iter().map(|r| r.task_id.clone()).collect();

            Ok((ready_ids, blocked_ids, open_ids))
        }).unwrap();

        // Invariant 1: ready ∩ blocked = ∅
        prop_assert!(
            ready_ids.is_disjoint(&blocked_ids),
            "ready and blocked overlap: ready={ready_ids:?}, blocked={blocked_ids:?}"
        );

        // Invariant 2: ready ∪ blocked covers all non-terminal tasks
        let union: HashSet<String> = ready_ids.union(&blocked_ids).cloned().collect();
        prop_assert_eq!(&union, &open_ids);
    }
}
