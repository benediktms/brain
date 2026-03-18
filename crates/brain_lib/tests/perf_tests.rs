//! Performance & concurrency integration tests.
//!
//! Tests correctness under load: batch equivalence, backpressure, event
//! coalescing, concurrent read/write, optimize scheduling, and startup
//! scan/watcher race idempotency.
//!
//! Uses MockEmbedder (deterministic, BLAKE3-based) — no model weights needed.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use brain_lib::db::Db;
use brain_lib::db::files;
use brain_lib::embedder::MockEmbedder;
use brain_lib::pipeline::IndexPipeline;
use brain_lib::store::{Store, StoreReader};
use brain_lib::watcher::{FileEvent, coalesce_events};

use tempfile::TempDir;

// ─── Helpers ─────────────────────────────────────────────────────

/// Create a pipeline with mock embedder in a temp directory.
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

/// Write a markdown file into a directory.
fn write_md(dir: &Path, name: &str, content: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, content).unwrap();
    path
}

/// Generate `count` markdown files, each with 2 heading sections (= 2 chunks).
fn generate_brain(dir: &Path, count: usize) -> Vec<PathBuf> {
    (0..count)
        .map(|i| {
            write_md(
                dir,
                &format!("file_{i:04}.md"),
                &format!(
                    "## Section A of {i}\n\nContent alpha for file {i}.\n\n\
                     ## Section B of {i}\n\nContent beta for file {i}."
                ),
            )
        })
        .collect()
}

// ─── Test 1: Batch embedding equivalence ─────────────────────────

#[tokio::test]
#[ignore] // fd-heavy — run via `just test-perf`
async fn test_batch_vs_single_equivalence() {
    let (pipeline_a, tmp_a) = setup().await;
    let (pipeline_b, tmp_b) = setup().await;

    let dir_a = tmp_a.path().join("notes");
    let dir_b = tmp_b.path().join("notes");
    std::fs::create_dir_all(&dir_a).unwrap();
    std::fs::create_dir_all(&dir_b).unwrap();

    // Create identical 20-file brains in both directories
    let paths_a = generate_brain(&dir_a, 20);
    let mut paths_b = Vec::new();
    for (i, pa) in paths_a.iter().enumerate() {
        let content = std::fs::read_to_string(pa).unwrap();
        paths_b.push(write_md(&dir_b, &format!("file_{i:04}.md"), &content));
    }

    // Pipeline A: batch index
    let stats_a = pipeline_a.index_files_batch(&paths_a).await.unwrap();

    // Pipeline B: single-file loop
    let mut indexed_b = 0usize;
    for path in &paths_b {
        if pipeline_b.index_file(path).await.unwrap() {
            indexed_b += 1;
        }
    }

    assert_eq!(stats_a.indexed, 20);
    assert_eq!(indexed_b, 20);

    // Both should have the same chunk count in SQLite
    let count_a: i64 = pipeline_a
        .db()
        .with_read_conn(|conn| {
            let c: i64 = conn
                .query_row("SELECT COUNT(*) FROM chunks", [], |row| row.get(0))
                .unwrap();
            Ok(c)
        })
        .unwrap();

    let count_b: i64 = pipeline_b
        .db()
        .with_read_conn(|conn| {
            let c: i64 = conn
                .query_row("SELECT COUNT(*) FROM chunks", [], |row| row.get(0))
                .unwrap();
            Ok(c)
        })
        .unwrap();

    assert_eq!(count_a, count_b);
    // 20 files × 2 heading sections = 40 chunks
    assert_eq!(count_a, 40);
}

// ─── Test 2: Backpressure — no dropped events ────────────────────

#[tokio::test]
#[ignore] // fd-heavy — run via `just test-perf`
async fn test_backpressure_no_dropped_events() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    let file_paths = generate_brain(&notes_dir, 150);

    // Send 150 Created events through a bounded channel from a std thread.
    // Channel capacity 16 forces the producer to block (backpressure).
    let (tx, mut rx) = tokio::sync::mpsc::channel::<FileEvent>(16);
    let tx_paths = file_paths.clone();

    std::thread::spawn(move || {
        for path in tx_paths {
            tx.blocking_send(FileEvent::Created(path)).unwrap();
        }
        // tx dropped → channel closes
    });

    // Consumer drains events via handle_event
    let mut processed = 0usize;
    while let Some(event) = rx.recv().await {
        pipeline.handle_event(event).await.unwrap();
        processed += 1;
    }

    assert_eq!(processed, 150);

    // Verify SQLite: 150 active files, 300 chunks
    let active_count = pipeline
        .db()
        .with_read_conn(|conn| {
            let paths = files::get_all_active_paths(conn)?;
            Ok(paths.len())
        })
        .unwrap();
    assert_eq!(active_count, 150);

    let chunk_count: i64 = pipeline
        .db()
        .with_read_conn(|conn| {
            let c: i64 = conn
                .query_row("SELECT COUNT(*) FROM chunks", [], |row| row.get(0))
                .unwrap();
            Ok(c)
        })
        .unwrap();
    assert_eq!(chunk_count, 300);
}

// ─── Test 3: Coalescing — 10 events, 1 index pass ───────────────

#[tokio::test]
#[ignore] // fd-heavy — run via `just test-perf`
async fn test_coalescing_deduplicates() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    let path = write_md(&notes_dir, "target.md", "## Initial\n\nOriginal content.");

    // ── Unit level: 10 Changed events for the same path → 1 index path
    let events: Vec<FileEvent> = (0..10).map(|_| FileEvent::Changed(path.clone())).collect();
    let (_renames, index_paths, _delete_paths) = coalesce_events(events);
    assert_eq!(index_paths.len(), 1);

    // ── Integration level: index, then modify + coalesce, verify 1 pass
    pipeline.index_file(&path).await.unwrap();
    let count_before = pipeline.metrics().indexing_latency.count();

    // Modify the file (so hash gate allows re-index)
    std::fs::write(&path, "## Modified\n\nUpdated content after coalescing.").unwrap();

    // Process the coalesced single path via batch
    let stats = pipeline.index_files_batch(&index_paths).await.unwrap();
    assert_eq!(stats.indexed, 1);

    let count_after = pipeline.metrics().indexing_latency.count();
    assert_eq!(count_after - count_before, 1, "exactly 1 indexing pass");
}

// ─── Test 4: Concurrent read/write — no deadlocks ───────────────

#[tokio::test]
#[ignore] // fd-heavy — run via `just test-perf`
async fn test_concurrent_read_write_no_deadlocks() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    // Seed 20 files via full_scan
    let seed_paths = generate_brain(&notes_dir, 20);
    let _ = seed_paths; // files exist on disk
    let stats = pipeline
        .full_scan(std::slice::from_ref(&notes_dir))
        .await
        .unwrap();
    assert_eq!(stats.indexed, 20);

    // Create a query embedding for readers
    let embedder = pipeline.embedder().clone();
    let query_vec = embedder.embed_batch(&["search query"]).unwrap()[0].clone();

    // Create StoreReader clones for concurrent queries
    let reader = StoreReader::from_store(pipeline.store());

    // Writer: index 30 new files sequentially
    let writer_dir = tmp.path().join("notes_new");
    std::fs::create_dir_all(&writer_dir).unwrap();
    let new_paths = generate_brain(&writer_dir, 30);

    // Spawn 4 reader tasks
    let mut reader_handles = Vec::new();
    for task_id in 0..4 {
        let r = reader.clone();
        let qv = query_vec.clone();
        reader_handles.push(tokio::spawn(async move {
            for i in 0..25 {
                let results = r.query(&qv, 5, 20).await;
                assert!(
                    results.is_ok(),
                    "reader {task_id} query {i} failed: {:?}",
                    results.err()
                );
                let results = results.unwrap();
                // Seeded data should always return results; empty would
                // indicate a concurrency bug (e.g. writes clearing the table).
                assert!(
                    !results.is_empty(),
                    "reader {task_id} query {i} returned no results"
                );
                for r in &results {
                    assert!(!r.chunk_id.is_empty());
                    assert!(!r.file_path.is_empty());
                }
                tokio::task::yield_now().await;
            }
        }));
    }

    // Writer runs on main task
    for path in &new_paths {
        pipeline.index_file(path).await.unwrap();
    }

    // Wait for all readers
    for handle in reader_handles {
        handle.await.expect("reader task panicked");
    }

    // All 50 files should be indexed
    let total_files = pipeline
        .db()
        .with_read_conn(|conn| {
            let paths = files::get_all_active_paths(conn)?;
            Ok(paths.len())
        })
        .unwrap();
    assert_eq!(total_files, 50);
}

// ─── Test 5: Optimize scheduling — row threshold ─────────────────

#[tokio::test]
#[ignore] // fd-heavy — run via `just test-perf`
async fn test_optimize_row_threshold() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    // Generate 120 files × 2 chunks = 240 chunks > DEFAULT_ROW_THRESHOLD (200)
    let paths = generate_brain(&notes_dir, 120);
    let stats = pipeline.index_files_batch(&paths).await.unwrap();
    assert_eq!(stats.indexed, 120);

    // Each chunk counts as one pending mutation: 120 files × 2 chunks = 240
    let pending = pipeline.store().optimizer().pending_count();
    assert_eq!(pending, 240, "expected exactly 240 pending mutations");

    // Trigger optimize
    pipeline.store().optimizer().maybe_optimize().await;

    // After optimize, pending should be 0
    assert_eq!(pipeline.store().optimizer().pending_count(), 0);

    // Queries should still work after optimize
    let embedder = pipeline.embedder().clone();
    let query_vec = embedder.embed_batch(&["test query"]).unwrap()[0].clone();
    let results = pipeline.store().query(&query_vec, 5, 20).await.unwrap();
    assert!(
        !results.is_empty(),
        "queries should return results after optimize"
    );
}

// ─── Test 6: Optimize scheduling — time threshold ────────────────

#[tokio::test(start_paused = true)]
#[ignore] // fd-heavy — run via `just test-perf`
async fn test_optimize_time_threshold() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    // Index 1 file — well below the row threshold (200)
    let path = write_md(
        &notes_dir,
        "solo.md",
        "## Solo Section\n\nJust one file, not enough for row threshold.",
    );
    pipeline.index_file(&path).await.unwrap();

    let pending_before = pipeline.store().optimizer().pending_count();
    assert!(pending_before > 0, "should have pending mutations");

    // maybe_optimize should NOT fire (below both thresholds at t=0).
    // Note: start_paused controls tokio::time::Instant used by the scheduler;
    // LanceDB I/O still uses real wall-clock time.
    pipeline.store().optimizer().maybe_optimize().await;
    assert!(
        pipeline.store().optimizer().pending_count() > 0,
        "should still have pending mutations (below both thresholds)"
    );

    // Advance past the 300s time threshold
    tokio::time::advance(std::time::Duration::from_secs(301)).await;

    // Now maybe_optimize should fire (time threshold exceeded)
    pipeline.store().optimizer().maybe_optimize().await;
    assert_eq!(
        pipeline.store().optimizer().pending_count(),
        0,
        "pending should be 0 after time-triggered optimize"
    );
}

// ─── Test 7: Startup scan + watcher race — no duplicates ─────────

#[tokio::test]
#[ignore] // fd-heavy — run via `just test-perf`
async fn test_scan_watcher_race_idempotent() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    let file_paths = generate_brain(&notes_dir, 30);

    // Full scan indexes all 30 files
    let stats = pipeline
        .full_scan(std::slice::from_ref(&notes_dir))
        .await
        .unwrap();
    assert_eq!(stats.indexed, 30);

    let stale_before = pipeline
        .metrics()
        .stale_hashes_prevented
        .load(Ordering::Relaxed);

    // Simulate watcher: send Created events for all 30 already-scanned files
    for path in &file_paths {
        pipeline
            .handle_event(FileEvent::Created(path.clone()))
            .await
            .unwrap();
    }

    // Still exactly 30 files (no duplicates)
    let active_count = pipeline
        .db()
        .with_read_conn(|conn| {
            let paths = files::get_all_active_paths(conn)?;
            Ok(paths.len())
        })
        .unwrap();
    assert_eq!(active_count, 30);

    // Still exactly 60 chunks (30 files × 2 chunks)
    let chunk_count: i64 = pipeline
        .db()
        .with_read_conn(|conn| {
            let c: i64 = conn
                .query_row("SELECT COUNT(*) FROM chunks", [], |row| row.get(0))
                .unwrap();
            Ok(c)
        })
        .unwrap();
    assert_eq!(chunk_count, 60);

    // Hash gate prevented all 30 stale re-indexes
    let stale_after = pipeline
        .metrics()
        .stale_hashes_prevented
        .load(Ordering::Relaxed);
    assert!(
        stale_after - stale_before >= 30,
        "expected >= 30 stale hashes prevented, got {}",
        stale_after - stale_before
    );

    // ── Complementary: modify a file after scan → verify re-indexing occurs
    let latency_before = pipeline.metrics().indexing_latency.count();
    std::fs::write(
        &file_paths[0],
        "## Changed A\n\nModified content.\n\n## Changed B\n\nAlso modified.",
    )
    .unwrap();
    pipeline
        .handle_event(FileEvent::Changed(file_paths[0].clone()))
        .await
        .unwrap();
    let latency_after = pipeline.metrics().indexing_latency.count();
    assert!(
        latency_after > latency_before,
        "modified file should be re-indexed"
    );
}

// ─── Test 8: IVF-PQ index creation ──────────────────────────────

#[tokio::test]
#[ignore] // fd-heavy — run via `just test-perf`
async fn test_ivf_pq_index_creation() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    // Need >256 rows for index creation (128 files × 2 chunks = 256)
    let paths = generate_brain(&notes_dir, 130);
    let stats = pipeline.index_files_batch(&paths).await.unwrap();
    assert_eq!(stats.indexed, 130);

    // Create index explicitly
    let config = brain_lib::store::IvfPqConfig::default();
    pipeline.store().create_vector_index(&config).await.unwrap();

    // Verify index exists via list_indices
    let indices = pipeline.store().table().list_indices().await.unwrap();
    let has_embedding_index = indices
        .iter()
        .any(|i| i.columns.contains(&"embedding".to_string()));
    assert!(
        has_embedding_index,
        "should have a vector index on embedding column"
    );
}

// ─── Test 9: IVF-PQ query returns results ────────────────────────

#[tokio::test]
#[ignore] // fd-heavy — run via `just test-perf`
async fn test_ivf_pq_query_returns_results() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    let paths = generate_brain(&notes_dir, 130);
    pipeline.index_files_batch(&paths).await.unwrap();

    // Create index
    let config = brain_lib::store::IvfPqConfig::default();
    pipeline.store().create_vector_index(&config).await.unwrap();

    // Query with nprobes
    let embedder = pipeline.embedder().clone();
    let query_vec = embedder.embed_batch(&["search query"]).unwrap()[0].clone();
    let results = pipeline.store().query(&query_vec, 10, 20).await.unwrap();

    assert!(
        !results.is_empty(),
        "query should return results after index creation"
    );
    for r in &results {
        assert!(!r.chunk_id.is_empty());
        assert!(!r.file_path.is_empty());
    }
}

// ─── Test 10: Auto-index on optimize ─────────────────────────────

#[tokio::test]
#[ignore] // fd-heavy — run via `just test-perf`
async fn test_auto_index_on_optimize() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    // Generate enough files to exceed both row threshold and MIN_ROWS_FOR_INDEX
    // 150 files × 2 chunks = 300 > 256 (MIN_ROWS_FOR_INDEX)
    let paths = generate_brain(&notes_dir, 150);
    pipeline.index_files_batch(&paths).await.unwrap();

    // Force optimize — should also auto-create index
    pipeline.store().optimizer().force_optimize().await;

    // Verify index was auto-created
    let indices = pipeline.store().table().list_indices().await.unwrap();
    let has_embedding_index = indices
        .iter()
        .any(|i| i.columns.contains(&"embedding".to_string()));
    assert!(
        has_embedding_index,
        "optimize should auto-create IVF-PQ index when row count >= 256"
    );

    // Queries should still work with the index
    let embedder = pipeline.embedder().clone();
    let query_vec = embedder.embed_batch(&["test query"]).unwrap()[0].clone();
    let results = pipeline.store().query(&query_vec, 5, 20).await.unwrap();
    assert!(
        !results.is_empty(),
        "queries should return results after auto-index"
    );
}
