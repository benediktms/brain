//! Integration tests: incremental indexing with SQLite control plane.
//!
//! Uses MockEmbedder (deterministic hash-based 384-dim vectors) to avoid
//! requiring model weights in CI.

use std::path::PathBuf;
use std::sync::Arc;

use brain_persistence::db::Db;
use brain_persistence::db::files;
use brain_persistence::db::meta;
use brain_lib::embedder::MockEmbedder;
use brain_lib::pipeline::IndexPipeline;
use brain_lib::ports::FileMetaWriter;
use brain_persistence::store::{LANCE_SCHEMA_VERSION, Store};
use brain_lib::utils::content_hash;
use brain_lib::watcher::FileEvent;

use tempfile::TempDir;

/// Helper: create a pipeline with mock embedder in a temp directory.
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

/// Helper: write a markdown file into a directory.
fn write_md(dir: &std::path::Path, name: &str, content: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, content).unwrap();
    path
}

// ─── Hash gate tests ──────────────────────────────────────────────

#[tokio::test]
async fn test_hash_gate_skip_unchanged() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    write_md(&notes_dir, "test.md", "# Hello\n\nSome content here.");

    // First index
    let stats = pipeline
        .full_scan(std::slice::from_ref(&notes_dir))
        .await
        .unwrap();
    assert_eq!(stats.indexed, 1);
    assert_eq!(stats.skipped, 0);

    // Second index — unchanged, should be skipped
    let stats = pipeline.full_scan(&[notes_dir]).await.unwrap();
    assert_eq!(stats.indexed, 0);
    assert_eq!(stats.skipped, 1);
}

#[tokio::test]
async fn test_hash_gate_trigger_on_modification() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    let path = write_md(&notes_dir, "test.md", "# Original\n\nOriginal content.");

    // First index
    let stats = pipeline
        .full_scan(std::slice::from_ref(&notes_dir))
        .await
        .unwrap();
    assert_eq!(stats.indexed, 1);

    // Modify content
    std::fs::write(&path, "# Modified\n\nDifferent content now.").unwrap();

    // Second index — should re-index
    let stats = pipeline
        .full_scan(std::slice::from_ref(&notes_dir))
        .await
        .unwrap();
    assert_eq!(stats.indexed, 1);
    assert_eq!(stats.skipped, 0);
}

// ─── Cross-file safety ──────────────────────────────────────────

#[tokio::test]
async fn test_cross_file_safety() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    write_md(&notes_dir, "a.md", "# File A\n\nContent of file A.");
    write_md(&notes_dir, "b.md", "# File B\n\nContent of file B.");

    // Index both
    let stats = pipeline
        .full_scan(std::slice::from_ref(&notes_dir))
        .await
        .unwrap();
    assert_eq!(stats.indexed, 2);

    // Modify only A
    write_md(&notes_dir, "a.md", "# File A\n\nUpdated content of A.");

    // Re-index — only A should be indexed, B skipped
    let stats = pipeline
        .full_scan(std::slice::from_ref(&notes_dir))
        .await
        .unwrap();
    assert_eq!(stats.indexed, 1);
    assert_eq!(stats.skipped, 1);
}

// ─── File deletion ──────────────────────────────────────────────

#[tokio::test]
async fn test_file_deletion_removes_from_index() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    let path = write_md(
        &notes_dir,
        "ephemeral.md",
        "# Ephemeral\n\nWill be deleted.",
    );

    // Index
    let stats = pipeline
        .full_scan(std::slice::from_ref(&notes_dir))
        .await
        .unwrap();
    assert_eq!(stats.indexed, 1);

    // Delete the file
    std::fs::remove_file(&path).unwrap();

    // Re-scan — full_scan no longer detects deletions (cross-brain safety:
    // deletion is handled by the file watcher or explicit handle_delete).
    // Deletion stats are 0; the file remains in SQLite until explicitly deleted.
    let stats = pipeline
        .full_scan(std::slice::from_ref(&notes_dir))
        .await
        .unwrap();
    assert_eq!(stats.deleted, 0);
    assert_eq!(stats.indexed, 0);
}

// ─── Startup recovery ───────────────────────────────────────────

#[tokio::test]
async fn test_startup_recovery_reindexes_stuck_files() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    write_md(&notes_dir, "stuck.md", "# Stuck\n\nThis file got stuck.");

    // Index normally first
    let stats = pipeline
        .full_scan(std::slice::from_ref(&notes_dir))
        .await
        .unwrap();
    assert_eq!(stats.indexed, 1);

    // Simulate a crash: set indexing_state to 'indexing_started'
    pipeline
        .db()
        .with_write_conn(|conn| {
            let paths = files::get_all_active_paths(conn)?;
            assert_eq!(paths.len(), 1);
            files::set_indexing_state(conn, &paths[0].0, "indexing_started")?;
            Ok(())
        })
        .unwrap();

    // Re-scan — should recover stuck file and re-index
    let stats = pipeline
        .full_scan(std::slice::from_ref(&notes_dir))
        .await
        .unwrap();
    assert_eq!(stats.stuck_recovered, 1);
    assert_eq!(stats.indexed, 1); // re-indexed after recovery
}

// ─── Whitespace normalization ───────────────────────────────────

#[tokio::test]
async fn test_whitespace_normalization_same_hash() {
    // Different trailing whitespace should produce the same hash
    let h1 = content_hash("hello   \nworld\n");
    let h2 = content_hash("hello\nworld\n");
    assert_eq!(h1, h2);

    let h3 = content_hash("hello\r\nworld\r\n");
    assert_eq!(h1, h3);
}

// ─── Empty file handling ────────────────────────────────────────

#[tokio::test]
async fn test_empty_file_indexed_without_error() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    write_md(&notes_dir, "empty.md", "");
    write_md(&notes_dir, "whitespace.md", "   \n  \n  ");

    let stats = pipeline
        .full_scan(std::slice::from_ref(&notes_dir))
        .await
        .unwrap();
    // Empty files get indexed (state tracked) even if no chunks produced
    assert_eq!(stats.indexed, 2);
    assert_eq!(stats.errors, 0);
}

// ─── Multiple scan consistency ──────────────────────────────────

#[tokio::test]
async fn test_multiple_scans_no_duplicates() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    write_md(
        &notes_dir,
        "test.md",
        "# Test\n\nParagraph one.\n\nParagraph two.",
    );

    // Index three times
    pipeline
        .full_scan(std::slice::from_ref(&notes_dir))
        .await
        .unwrap();
    pipeline
        .full_scan(std::slice::from_ref(&notes_dir))
        .await
        .unwrap();
    pipeline
        .full_scan(std::slice::from_ref(&notes_dir))
        .await
        .unwrap();

    // Verify only one file tracked in SQLite
    let active = pipeline
        .db()
        .with_read_conn(files::get_all_active_paths)
        .unwrap();
    assert_eq!(active.len(), 1);
}

// ─── File rename tests ──────────────────────────────────────────

#[tokio::test]
async fn test_file_rename_preserves_identity() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    let old_path = write_md(&notes_dir, "a.md", "# Alpha\n\nContent of file alpha.");

    // Index
    let stats = pipeline
        .full_scan(std::slice::from_ref(&notes_dir))
        .await
        .unwrap();
    assert_eq!(stats.indexed, 1);

    // Grab the original file_id
    let original_id = pipeline
        .db()
        .with_read_conn(|conn| {
            let paths = files::get_all_active_paths(conn)?;
            Ok(paths[0].0.clone())
        })
        .unwrap();

    // Rename on disk and in the pipeline
    let new_path = notes_dir.join("b.md");
    std::fs::rename(&old_path, &new_path).unwrap();
    pipeline.rename_file(&old_path, &new_path).await.unwrap();

    // Verify: same file_id, updated path
    let active = pipeline
        .db()
        .with_read_conn(files::get_all_active_paths)
        .unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].0, original_id);
    assert!(active[0].1.ends_with("b.md"));

    // Re-scan should not create duplicates
    let stats = pipeline
        .full_scan(std::slice::from_ref(&notes_dir))
        .await
        .unwrap();
    assert_eq!(stats.indexed, 0);
    assert_eq!(stats.skipped, 1);
}

// ─── Handle event tests ─────────────────────────────────────────

#[tokio::test]
async fn test_handle_event_created() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    let path = write_md(&notes_dir, "new.md", "# New\n\nCreated via event.");

    pipeline
        .handle_event(FileEvent::Created(path))
        .await
        .unwrap();

    // Verify: file appears in SQLite with state=indexed
    let active = pipeline
        .db()
        .with_read_conn(|conn| {
            let paths = files::get_all_active_paths(conn)?;
            assert_eq!(paths.len(), 1);
            let state: String = conn
                .query_row(
                    "SELECT indexing_state FROM files WHERE file_id = ?1",
                    [&paths[0].0],
                    |row| row.get(0),
                )
                .unwrap();
            Ok((paths[0].1.clone(), state))
        })
        .unwrap();
    assert!(active.0.ends_with("new.md"));
    assert_eq!(active.1, "indexed");
}

#[tokio::test]
async fn test_handle_event_deleted() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    let path = write_md(
        &notes_dir,
        "doomed.md",
        "# Doomed\n\nWill be deleted via event.",
    );

    // Index first
    pipeline
        .full_scan(std::slice::from_ref(&notes_dir))
        .await
        .unwrap();

    // Delete via event
    pipeline
        .handle_event(FileEvent::Deleted(path))
        .await
        .unwrap();

    // Verify: soft-deleted (no active paths)
    let active = pipeline
        .db()
        .with_read_conn(files::get_all_active_paths)
        .unwrap();
    assert!(active.is_empty());
}

#[tokio::test]
async fn test_handle_event_renamed() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    let old_path = write_md(&notes_dir, "before.md", "# Before\n\nContent.");

    // Index first
    pipeline
        .full_scan(std::slice::from_ref(&notes_dir))
        .await
        .unwrap();

    let (original_id, original_hash) = pipeline
        .db()
        .with_read_conn(|conn| {
            let paths = files::get_all_active_paths(conn)?;
            let hash = files::get_content_hash(conn, &paths[0].0)?;
            Ok((paths[0].0.clone(), hash))
        })
        .unwrap();

    // Rename on disk + via event
    let new_path = notes_dir.join("after.md");
    std::fs::rename(&old_path, &new_path).unwrap();
    pipeline
        .handle_event(FileEvent::Renamed {
            from: old_path,
            to: new_path,
        })
        .await
        .unwrap();

    // Verify: path updated, same file_id, same content_hash (no re-embedding)
    let (found_id, found_path, found_hash) = pipeline
        .db()
        .with_read_conn(|conn| {
            let paths = files::get_all_active_paths(conn)?;
            assert_eq!(paths.len(), 1);
            let hash = files::get_content_hash(conn, &paths[0].0)?;
            Ok((paths[0].0.clone(), paths[0].1.clone(), hash))
        })
        .unwrap();
    assert_eq!(found_id, original_id);
    assert!(found_path.ends_with("after.md"));
    assert_eq!(found_hash, original_hash);
}

// ─── Delete/recreate tests ──────────────────────────────────────

#[tokio::test]
async fn test_delete_then_recreate_resurrects_id() {
    // get_or_create_file_id resurrects soft-deleted rows at the same path,
    // so a recreated file keeps its original file_id.
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    let path = write_md(&notes_dir, "a.md", "# Alpha\n\nOriginal content.");

    // Index
    pipeline
        .full_scan(std::slice::from_ref(&notes_dir))
        .await
        .unwrap();
    let original_id = pipeline
        .db()
        .with_read_conn(|conn| {
            let paths = files::get_all_active_paths(conn)?;
            Ok(paths[0].0.clone())
        })
        .unwrap();

    // Delete on disk — explicit handle_delete (full_scan no longer sweeps deletions)
    std::fs::remove_file(&path).unwrap();
    let deleted = pipeline
        .db()
        .handle_delete(&path.to_string_lossy())
        .unwrap();
    assert!(deleted.is_some(), "handle_delete should return the file_id");

    // Recreate with different content (same path)
    write_md(
        &notes_dir,
        "a.md",
        "# Alpha\n\nNew content after recreation.",
    );

    // Full scan picks it up
    let stats = pipeline
        .full_scan(std::slice::from_ref(&notes_dir))
        .await
        .unwrap();
    assert_eq!(stats.indexed, 1);

    // Verify: resurrected with the same file_id
    let resurrected_id = pipeline
        .db()
        .with_read_conn(|conn| {
            let paths = files::get_all_active_paths(conn)?;
            assert_eq!(paths.len(), 1);
            Ok(paths[0].0.clone())
        })
        .unwrap();
    assert_eq!(resurrected_id, original_id);
}

// ─── Chunk upsert tests ─────────────────────────────────────────

#[tokio::test]
async fn test_upsert_add_paragraph_adds_chunk() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    let path = write_md(
        &notes_dir,
        "growing.md",
        "## One\n\nPara one.\n\n## Two\n\nPara two.",
    );

    // Index — should produce 2 chunks (one per heading)
    pipeline
        .full_scan(std::slice::from_ref(&notes_dir))
        .await
        .unwrap();

    let (file_id, count) = pipeline
        .db()
        .with_read_conn(|conn| {
            let paths = files::get_all_active_paths(conn)?;
            let fid = paths[0].0.clone();
            let c: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM chunks WHERE file_id = ?1",
                    [&fid],
                    |row| row.get(0),
                )
                .unwrap();
            Ok((fid, c))
        })
        .unwrap();
    assert_eq!(count, 2);

    // Add a 3rd section
    std::fs::write(
        &path,
        "## One\n\nPara one.\n\n## Two\n\nPara two.\n\n## Three\n\nPara three.",
    )
    .unwrap();

    // Re-index
    pipeline
        .full_scan(std::slice::from_ref(&notes_dir))
        .await
        .unwrap();

    let new_count: i64 = pipeline
        .db()
        .with_read_conn(|conn| {
            let c: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM chunks WHERE file_id = ?1",
                    [&file_id],
                    |row| row.get(0),
                )
                .unwrap();
            Ok(c)
        })
        .unwrap();
    assert_eq!(new_count, 3);
}

#[tokio::test]
async fn test_upsert_remove_paragraph_removes_chunk() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    let path = write_md(
        &notes_dir,
        "shrinking.md",
        "## One\n\nPara one.\n\n## Two\n\nPara two.\n\n## Three\n\nPara three.",
    );

    // Index — 3 chunks (one per heading)
    pipeline
        .full_scan(std::slice::from_ref(&notes_dir))
        .await
        .unwrap();

    let (file_id, count) = pipeline
        .db()
        .with_read_conn(|conn| {
            let paths = files::get_all_active_paths(conn)?;
            let fid = paths[0].0.clone();
            let c: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM chunks WHERE file_id = ?1",
                    [&fid],
                    |row| row.get(0),
                )
                .unwrap();
            Ok((fid, c))
        })
        .unwrap();
    assert_eq!(count, 3);

    // Remove middle section
    std::fs::write(&path, "## One\n\nPara one.\n\n## Three\n\nPara three.").unwrap();

    // Re-index
    pipeline.full_scan(&[notes_dir]).await.unwrap();

    let new_count: i64 = pipeline
        .db()
        .with_read_conn(|conn| {
            let c: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM chunks WHERE file_id = ?1",
                    [&file_id],
                    |row| row.get(0),
                )
                .unwrap();
            Ok(c)
        })
        .unwrap();
    assert_eq!(new_count, 2);
}

// ─── Batch indexing tests ────────────────────────────────────────

#[tokio::test]
async fn test_batch_index_multiple_files() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    let paths: Vec<PathBuf> = (0..5)
        .map(|i| {
            write_md(
                &notes_dir,
                &format!("file_{i}.md"),
                &format!("# File {i}\n\nContent {i}."),
            )
        })
        .collect();

    let stats = pipeline.index_files_batch(&paths).await.unwrap();
    assert_eq!(stats.indexed, 5);
    assert_eq!(stats.skipped, 0);
    assert_eq!(stats.errors, 0);
}

#[tokio::test]
async fn test_batch_index_skips_unchanged() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    let paths: Vec<PathBuf> = (0..3)
        .map(|i| {
            write_md(
                &notes_dir,
                &format!("file_{i}.md"),
                &format!("# File {i}\n\nContent {i}."),
            )
        })
        .collect();

    let stats = pipeline.index_files_batch(&paths).await.unwrap();
    assert_eq!(stats.indexed, 3);

    let stats = pipeline.index_files_batch(&paths).await.unwrap();
    assert_eq!(stats.indexed, 0);
    assert_eq!(stats.skipped, 3);
}

#[tokio::test]
async fn test_batch_index_mixed_changed_unchanged() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    let paths: Vec<PathBuf> = (0..3)
        .map(|i| {
            write_md(
                &notes_dir,
                &format!("file_{i}.md"),
                &format!("# File {i}\n\nContent {i}."),
            )
        })
        .collect();

    pipeline.index_files_batch(&paths).await.unwrap();

    // Modify only file_1
    std::fs::write(&paths[1], "# File 1\n\nUpdated content.").unwrap();

    let stats = pipeline.index_files_batch(&paths).await.unwrap();
    assert_eq!(stats.indexed, 1);
    assert_eq!(stats.skipped, 2);
}

#[tokio::test]
async fn test_batch_index_nonexistent_file_continues() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    let good = write_md(&notes_dir, "good.md", "# Good\n\nValid file.");
    let bad: PathBuf = notes_dir.join("nonexistent.md");

    let stats = pipeline.index_files_batch(&[good, bad]).await.unwrap();
    assert_eq!(stats.indexed, 1);
    assert_eq!(stats.errors, 1);
}

#[tokio::test]
async fn test_batch_index_empty_file() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    let empty_path = write_md(&notes_dir, "empty.md", "");
    let normal_path = write_md(&notes_dir, "normal.md", "# Normal\n\nHas content.");

    let stats = pipeline
        .index_files_batch(&[empty_path, normal_path])
        .await
        .unwrap();
    assert_eq!(stats.indexed, 2);
    assert_eq!(stats.errors, 0);
}

// ─── Schema versioning tests ────────────────────────────────────

#[tokio::test]
async fn test_schema_version_mismatch_triggers_rebuild() {
    let tmp = TempDir::new().unwrap();
    let sqlite_path = tmp.path().join("brain.db");
    let lance_path = tmp.path().join("brain_lancedb");

    // Create DB and store, index a file
    let db = Db::open(&sqlite_path).unwrap();
    let store = Store::open_or_create(&lance_path).await.unwrap();
    let embedder = Arc::new(MockEmbedder);

    // Stamp a stale version (0) to simulate an outdated schema
    db.with_write_conn(|conn| meta::set_meta(conn, "lancedb_schema_version", "0"))
        .unwrap();

    // Insert a file so we can verify the hash gets cleared
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();
    let path = notes_dir.join("test.md");
    std::fs::write(&path, "# Test\n\nSome content.").unwrap();

    let pipeline = IndexPipeline::with_embedder(db, store, embedder)
        .await
        .unwrap();

    // with_embedder should have detected the mismatch, rebuilt, and stamped the new version
    let stored: Option<String> = pipeline
        .db()
        .with_read_conn(|conn| meta::get_meta(conn, "lancedb_schema_version"))
        .unwrap();
    assert_eq!(
        stored,
        Some(LANCE_SCHEMA_VERSION.to_string()),
        "version should be stamped after rebuild"
    );

    // Table should be empty (rebuilt)
    let ids = pipeline.store().get_file_ids_with_chunks().await.unwrap();
    assert!(ids.is_empty(), "table should be empty after rebuild");

    // Pipeline should still be functional — index a file
    assert!(pipeline.index_file(&path).await.unwrap());
    let ids = pipeline.store().get_file_ids_with_chunks().await.unwrap();
    assert!(!ids.is_empty(), "should be able to index after rebuild");
}

#[tokio::test]
async fn test_schema_version_match_skips_rebuild() {
    let tmp = TempDir::new().unwrap();
    let sqlite_path = tmp.path().join("brain.db");
    let lance_path = tmp.path().join("brain_lancedb");

    // First pipeline: creates store, stamps version
    let db = Db::open(&sqlite_path).unwrap();
    let store = Store::open_or_create(&lance_path).await.unwrap();
    let embedder = Arc::new(MockEmbedder);
    let pipeline = IndexPipeline::with_embedder(db, store, embedder)
        .await
        .unwrap();

    // Index a file
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();
    let path = notes_dir.join("test.md");
    std::fs::write(&path, "# Test\n\nSome content.").unwrap();
    pipeline.index_file(&path).await.unwrap();

    let ids_before = pipeline.store().get_file_ids_with_chunks().await.unwrap();
    assert!(!ids_before.is_empty());
    drop(pipeline);

    // Second pipeline: opens the same store — version matches, no rebuild
    let db2 = Db::open(&sqlite_path).unwrap();
    let store2 = Store::open_or_create(&lance_path).await.unwrap();
    let embedder2 = Arc::new(MockEmbedder);
    let pipeline2 = IndexPipeline::with_embedder(db2, store2, embedder2)
        .await
        .unwrap();

    // Data should still be present (no rebuild occurred)
    let ids_after = pipeline2.store().get_file_ids_with_chunks().await.unwrap();
    assert_eq!(
        ids_before, ids_after,
        "data should survive when version matches"
    );
}

#[tokio::test]
async fn test_first_run_stamps_without_rebuild() {
    // First run (no version stored): should stamp the version WITHOUT rebuilding
    // the table — data inserted before pipeline construction must survive.
    let tmp = TempDir::new().unwrap();
    let sqlite_path = tmp.path().join("brain.db");
    let lance_path = tmp.path().join("brain_lancedb");

    let db = Db::open(&sqlite_path).unwrap();
    let store = Store::open_or_create(&lance_path).await.unwrap();
    let embedder = Arc::new(MockEmbedder);

    // Construct pipeline — no version stored, first run
    let pipeline = IndexPipeline::with_embedder(db, store, embedder)
        .await
        .unwrap();

    // Version should be stamped
    let stored: Option<String> = pipeline
        .db()
        .with_read_conn(|conn| meta::get_meta(conn, "lancedb_schema_version"))
        .unwrap();
    assert_eq!(
        stored,
        Some(LANCE_SCHEMA_VERSION.to_string()),
        "version should be stamped on first run"
    );

    // Table should still be functional (not wiped) — index a file to prove it
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();
    let path = notes_dir.join("test.md");
    std::fs::write(&path, "# Test\n\nSome content.").unwrap();
    assert!(pipeline.index_file(&path).await.unwrap());
}

#[tokio::test]
async fn test_unparseable_version_triggers_rebuild() {
    // When the stored version is not a valid u32, treat as mismatch and rebuild.
    let tmp = TempDir::new().unwrap();
    let sqlite_path = tmp.path().join("brain.db");
    let lance_path = tmp.path().join("brain_lancedb");

    let db = Db::open(&sqlite_path).unwrap();
    let store = Store::open_or_create(&lance_path).await.unwrap();
    let embedder = Arc::new(MockEmbedder);

    // Set an unparseable version
    db.with_write_conn(|conn| meta::set_meta(conn, "lancedb_schema_version", "not_a_number"))
        .unwrap();

    let pipeline = IndexPipeline::with_embedder(db, store, embedder)
        .await
        .unwrap();

    // Version should be stamped to current
    let stored: Option<String> = pipeline
        .db()
        .with_read_conn(|conn| meta::get_meta(conn, "lancedb_schema_version"))
        .unwrap();
    assert_eq!(
        stored,
        Some(LANCE_SCHEMA_VERSION.to_string()),
        "version should be stamped after rebuild"
    );

    // Table should be empty (rebuilt)
    let ids = pipeline.store().get_file_ids_with_chunks().await.unwrap();
    assert!(ids.is_empty(), "table should be empty after rebuild");
}
