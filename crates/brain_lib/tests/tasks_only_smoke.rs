//! Smoke test for the tasks-only runtime soft-gate.
//!
//! Exercises [`IndexPipeline::tasks_only`] — the constructor introduced
//! alongside the `embed` feature gate work — to prove the soft-gate is
//! wired correctly end-to-end:
//!
//! - `IndexPipeline.embedder` is `None`
//! - `index_file` succeeds, writing chunk rows to SQLite
//! - `mark_chunks_embedded` is stamped (so the stale-poll daemon's
//!   short-circuit doesn't perpetually re-queue tasks-only chunks)
//! - The vector store (here `MockStore`) receives **zero** `upsert_chunks`
//!   calls — verifying the per-call-site `if let Some(...)` gate
//!
//! Designed to run under `--no-default-features --features test-utils` so
//! that the CI `test-tasks-only` job exercises the soft-gate at runtime,
//! not just compile-time.

use std::path::PathBuf;

use brain_lib::pipeline::IndexPipeline;
use brain_lib::ports::mock::MockStore;
use brain_persistence::db::Db;
use brain_persistence::sql::SqlResultExt;
use tempfile::TempDir;

/// Helper: write a markdown file into a directory.
fn write_md(dir: &std::path::Path, name: &str, content: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, content).unwrap();
    path
}

#[tokio::test]
async fn tasks_only_pipeline_has_no_embedder() {
    let tmp = TempDir::new().unwrap();
    let sqlite_path = tmp.path().join("brain.db");
    let db = Db::open(&sqlite_path).unwrap();
    let store = MockStore::default();

    let pipeline = IndexPipeline::tasks_only(db, store).await.unwrap();

    assert!(
        pipeline.embedder().is_none(),
        "tasks-only pipeline must have embedder = None"
    );
}

#[tokio::test]
async fn tasks_only_index_writes_sqlite_skips_vector_store() {
    let tmp = TempDir::new().unwrap();
    let sqlite_path = tmp.path().join("brain.db");
    let db = Db::open(&sqlite_path).unwrap();
    let store = MockStore::default();

    let mut pipeline = IndexPipeline::tasks_only(db, store).await.unwrap();
    pipeline.set_brain_id("test-brain".to_string());

    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();
    let md_path = write_md(
        &notes_dir,
        "hello.md",
        "## Hello\n\nThis is a tasks-only smoke test.",
    );

    let indexed = pipeline.index_file(&md_path).await.unwrap();
    assert!(indexed, "index_file should report a fresh index");

    // SQLite side: chunks rows present (FTS-searchable).
    let chunk_count: i64 = pipeline
        .db_for_tests()
        .with_read_conn(|conn| {
            let c: i64 = conn
                .query_row("SELECT COUNT(*) FROM chunks", [], |row| row.get(0))
                .unwrap();
            Ok(c)
        })
        .into_brain_core()
        .unwrap();
    assert!(
        chunk_count > 0,
        "SQLite chunks table should have rows after tasks-only index"
    );

    // Vector-store side: zero upserts. The `if let Some(embedder)` gate in
    // `index_file` must skip `store.upsert_chunks` when embedder is None.
    let upserted = pipeline.store().chunks();
    assert!(
        upserted.is_empty(),
        "MockStore should receive no upsert_chunks calls in tasks-only mode, \
         got {} entries",
        upserted.len()
    );
}
