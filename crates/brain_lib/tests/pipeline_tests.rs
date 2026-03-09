//! Integration tests for `IndexPipeline` public API.
//!
//! Covers `index_file`, `full_scan`, `vacuum`, and `doctor` using `MockEmbedder`
//! so no model weights are required in CI.

use std::path::PathBuf;
use std::sync::Arc;

use brain_lib::db::Db;
use brain_lib::db::files;
use brain_lib::doctor::CheckStatus;
use brain_lib::embedder::MockEmbedder;
use brain_lib::pipeline::IndexPipeline;
use brain_lib::store::Store;

use tempfile::TempDir;

// ─── Helpers ────────────────────────────────────────────────────────────────

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

fn write_md(dir: &std::path::Path, name: &str, content: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, content).unwrap();
    path
}

// ─── index_file ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_index_file_stores_chunks_in_sqlite() {
    let (pipeline, tmp) = setup().await;
    let path = write_md(
        tmp.path(),
        "note.md",
        "# Hello\n\nSome content here.\n\n## Section Two\n\nMore content.",
    );

    let indexed = pipeline.index_file(&path).await.unwrap();
    assert!(indexed, "first index should return true");

    // Verify chunks are stored in SQLite
    let chunk_count: i64 = pipeline
        .db()
        .with_read_conn(|conn| {
            let c = conn.query_row("SELECT COUNT(*) FROM chunks", [], |row| row.get(0))?;
            Ok(c)
        })
        .unwrap();
    assert!(
        chunk_count > 0,
        "expected chunks in SQLite after index_file"
    );
}

#[tokio::test]
async fn test_index_file_stores_chunks_in_lancedb() {
    let (pipeline, tmp) = setup().await;
    let path = write_md(tmp.path(), "note.md", "# Hello\n\nSome content here.");

    pipeline.index_file(&path).await.unwrap();

    let file_ids = pipeline.store().get_file_ids_with_chunks().await.unwrap();
    assert_eq!(file_ids.len(), 1, "expected one file_id in LanceDB");
}

#[tokio::test]
async fn test_index_file_second_call_skipped_by_hash_gate() {
    let (pipeline, tmp) = setup().await;
    let path = write_md(tmp.path(), "note.md", "# Same\n\nUnchanged content.");

    assert!(pipeline.index_file(&path).await.unwrap());
    // Content unchanged — hash gate should skip
    assert!(
        !pipeline.index_file(&path).await.unwrap(),
        "second call with identical content should be skipped (returns false)"
    );
}

#[tokio::test]
async fn test_index_file_utf8_multibyte_content() {
    let (pipeline, tmp) = setup().await;
    // Include emoji, CJK, and combining characters
    let content =
        "# 日本語テスト\n\nHello 🌍 world. Ñoño café naïve résumé.\n\n## 한국어\n\nContent here.";
    let path = write_md(tmp.path(), "utf8.md", content);

    let result = pipeline.index_file(&path).await;
    assert!(
        result.is_ok(),
        "index_file should not error on UTF-8 content"
    );
    assert!(result.unwrap(), "UTF-8 file should be indexed");

    let chunk_count: i64 = pipeline
        .db()
        .with_read_conn(|conn| {
            let c = conn.query_row("SELECT COUNT(*) FROM chunks", [], |row| row.get(0))?;
            Ok(c)
        })
        .unwrap();
    assert!(chunk_count > 0, "UTF-8 file should produce chunks");
}

#[tokio::test]
async fn test_index_file_empty_file_returns_true_no_chunks() {
    let (pipeline, tmp) = setup().await;
    let path = write_md(tmp.path(), "empty.md", "");

    let indexed = pipeline.index_file(&path).await.unwrap();
    assert!(indexed, "empty file returns true (state was updated)");

    let chunk_count: i64 = pipeline
        .db()
        .with_read_conn(|conn| {
            let c = conn.query_row("SELECT COUNT(*) FROM chunks", [], |row| row.get(0))?;
            Ok(c)
        })
        .unwrap();
    assert_eq!(chunk_count, 0, "empty file should produce zero chunks");
}

#[tokio::test]
async fn test_index_file_sqlite_and_lancedb_agree_on_file_id() {
    let (pipeline, tmp) = setup().await;
    let path = write_md(
        tmp.path(),
        "agree.md",
        "# Agreement\n\nBoth stores should use the same file_id.",
    );

    pipeline.index_file(&path).await.unwrap();

    let sqlite_ids: Vec<String> = pipeline
        .db()
        .with_read_conn(|conn| {
            let pairs = files::get_all_active_paths(conn)?;
            Ok(pairs.into_iter().map(|(fid, _)| fid).collect())
        })
        .unwrap();
    assert_eq!(sqlite_ids.len(), 1);

    let lance_ids = pipeline.store().get_file_ids_with_chunks().await.unwrap();
    assert_eq!(lance_ids.len(), 1);
    assert_eq!(
        sqlite_ids[0],
        *lance_ids.iter().next().unwrap(),
        "file_id must match between SQLite and LanceDB"
    );
}

// ─── full_scan ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_full_scan_indexes_multiple_files() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    for i in 0..4 {
        write_md(
            &notes_dir,
            &format!("file{i}.md"),
            &format!("# File {i}\n\nContent of file {i}."),
        );
    }

    let stats = pipeline.full_scan(&[notes_dir]).await.unwrap();
    assert_eq!(stats.indexed, 4);
    assert_eq!(stats.errors, 0);
}

#[tokio::test]
async fn test_full_scan_idempotent_no_duplicate_chunks() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    write_md(
        &notes_dir,
        "a.md",
        "# Alpha\n\nFirst section.\n\n## Beta\n\nSecond section.",
    );
    write_md(&notes_dir, "b.md", "# Gamma\n\nAnother file.");

    // First scan
    let stats1 = pipeline.full_scan(&[notes_dir.clone()]).await.unwrap();
    assert_eq!(stats1.indexed, 2);

    let chunk_count_after_first: i64 = pipeline
        .db()
        .with_read_conn(|conn| {
            let c = conn.query_row("SELECT COUNT(*) FROM chunks", [], |row| row.get(0))?;
            Ok(c)
        })
        .unwrap();

    // Second scan — identical content, nothing should change
    let stats2 = pipeline.full_scan(&[notes_dir]).await.unwrap();
    assert_eq!(stats2.indexed, 0, "nothing re-indexed on second scan");
    assert_eq!(stats2.skipped, 2, "both files should be skipped");

    let chunk_count_after_second: i64 = pipeline
        .db()
        .with_read_conn(|conn| {
            let c = conn.query_row("SELECT COUNT(*) FROM chunks", [], |row| row.get(0))?;
            Ok(c)
        })
        .unwrap();
    assert_eq!(
        chunk_count_after_first, chunk_count_after_second,
        "no duplicate chunks after second full_scan"
    );

    let lance_ids = pipeline.store().get_file_ids_with_chunks().await.unwrap();
    assert_eq!(lance_ids.len(), 2, "LanceDB should have exactly 2 file_ids");
}

#[tokio::test]
async fn test_full_scan_detects_deletion() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    let path = write_md(&notes_dir, "doomed.md", "# Doomed\n\nWill be removed.");

    let stats = pipeline.full_scan(&[notes_dir.clone()]).await.unwrap();
    assert_eq!(stats.indexed, 1);

    std::fs::remove_file(&path).unwrap();

    let stats = pipeline.full_scan(&[notes_dir]).await.unwrap();
    assert_eq!(stats.deleted, 1);

    let active = pipeline
        .db()
        .with_read_conn(files::get_all_active_paths)
        .unwrap();
    assert!(active.is_empty(), "soft-deleted file should not be active");
}

// ─── vacuum ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_vacuum_completes_without_error_on_empty_index() {
    let (pipeline, _tmp) = setup().await;
    // Vacuum with a 0-day threshold purges everything older than now
    let stats = pipeline.vacuum(0).await.unwrap();
    assert_eq!(stats.purged_files, 0, "nothing to purge on empty index");
}

#[tokio::test]
async fn test_vacuum_purges_soft_deleted_files() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    let path = write_md(&notes_dir, "old.md", "# Old\n\nWill be deleted.");

    // Index and then delete
    pipeline.full_scan(&[notes_dir.clone()]).await.unwrap();
    std::fs::remove_file(&path).unwrap();
    pipeline.full_scan(&[notes_dir]).await.unwrap();

    // Verify it's soft-deleted (not active) but the row still exists
    let active = pipeline
        .db()
        .with_read_conn(files::get_all_active_paths)
        .unwrap();
    assert!(
        active.is_empty(),
        "file should be soft-deleted before vacuum"
    );

    // Back-date deleted_at to 2 days ago so vacuum(1) picks it up.
    // vacuum(N) purges files where deleted_at < (now - N*86400), so the
    // deletion must be strictly in the past relative to the cutoff.
    let two_days_ago = brain_lib::utils::now_ts() - 2 * 86400;
    pipeline
        .db()
        .with_write_conn(|conn| {
            conn.execute(
                "UPDATE files SET deleted_at = ?1 WHERE deleted_at IS NOT NULL",
                rusqlite::params![two_days_ago],
            )?;
            Ok(())
        })
        .unwrap();

    let stats = pipeline.vacuum(1).await.unwrap();
    assert_eq!(
        stats.purged_files, 1,
        "should purge the one soft-deleted file"
    );

    // LanceDB should also have no chunks for this file
    let lance_ids = pipeline.store().get_file_ids_with_chunks().await.unwrap();
    assert!(
        lance_ids.is_empty(),
        "LanceDB should have no chunks after vacuum"
    );
}

#[tokio::test]
async fn test_vacuum_does_not_purge_active_files() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    write_md(&notes_dir, "keep.md", "# Keep\n\nThis file stays.");
    pipeline.full_scan(&[notes_dir]).await.unwrap();

    // Vacuum should not touch active (non-deleted) files
    let stats = pipeline.vacuum(0).await.unwrap();
    assert_eq!(stats.purged_files, 0, "active file should not be purged");

    let active = pipeline
        .db()
        .with_read_conn(files::get_all_active_paths)
        .unwrap();
    assert_eq!(active.len(), 1, "active file should still be present");
}

// ─── doctor ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_doctor_healthy_after_full_index() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    write_md(&notes_dir, "a.md", "# Alpha\n\nContent of alpha.");
    write_md(&notes_dir, "b.md", "# Beta\n\nContent of beta.");

    pipeline.full_scan(&[notes_dir.clone()]).await.unwrap();

    let report = pipeline.doctor(&[notes_dir]).await.unwrap();

    assert!(
        !report.checks.is_empty(),
        "doctor should produce at least one check"
    );
    assert_eq!(
        report.problem_count(),
        0,
        "no problems expected after clean index\n{report}"
    );
}

#[tokio::test]
async fn test_doctor_empty_index_no_problems() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    let report = pipeline.doctor(&[notes_dir]).await.unwrap();

    assert_eq!(
        report.problem_count(),
        0,
        "empty index should report no problems\n{report}"
    );
}

#[tokio::test]
async fn test_doctor_returns_expected_check_names() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    write_md(&notes_dir, "x.md", "# X\n\nSome content.");
    pipeline.full_scan(&[notes_dir.clone()]).await.unwrap();

    let report = pipeline.doctor(&[notes_dir]).await.unwrap();

    let check_names: Vec<&str> = report.checks.iter().map(|c| c.name.as_str()).collect();
    for expected in &[
        "Orphan chunks",
        "Missing chunks",
        "Content hashes",
        "FTS5 consistency",
        "Stuck files",
        "Index coverage",
    ] {
        assert!(
            check_names.contains(expected),
            "expected check '{expected}' in doctor report; got: {check_names:?}"
        );
    }
}

#[tokio::test]
async fn test_doctor_fts5_consistency_ok_after_index() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    write_md(
        &notes_dir,
        "fts.md",
        "# FTS Test\n\nThis content should appear in the FTS5 index.",
    );
    pipeline.full_scan(&[notes_dir.clone()]).await.unwrap();

    let report = pipeline.doctor(&[notes_dir]).await.unwrap();

    let fts_check = report
        .checks
        .iter()
        .find(|c| c.name == "FTS5 consistency")
        .expect("FTS5 consistency check should exist");
    assert_eq!(
        fts_check.status,
        CheckStatus::Ok,
        "FTS5 should be consistent after indexing: {}",
        fts_check.detail
    );
}
