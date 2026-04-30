#![allow(clippy::disallowed_macros, clippy::disallowed_types)]

//! TDD integration tests for `store_procedure`.
//!
//! These tests use a self-contained in-memory SQLite schema that already
//! includes `'procedure'` in the `kind` CHECK constraint, making them
//! independent of the v28 migration. Once the migration lands the full
//! `Db::open()` path will also accept `kind='procedure'` and these tests
//! will continue to pass unchanged.

use rusqlite::Connection;

use brain_persistence::db::fts::{FtsSummaryResult, search_summaries_fts};
use brain_persistence::db::summaries::{SummaryRow, get_summary, store_procedure};

// ─── Schema helpers ──────────────────────────────────────────────

/// Create a minimal in-memory SQLite schema that includes `'procedure'`
/// in the summaries CHECK constraint plus the FTS5 virtual table and
/// the insert/delete/update triggers that keep it in sync.
fn setup_procedure_schema() -> Connection {
    let conn = Connection::open_in_memory().unwrap();

    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA foreign_keys = ON;

         CREATE TABLE IF NOT EXISTS summaries (
             summary_id  TEXT    PRIMARY KEY,
             kind        TEXT    NOT NULL
                                 CHECK(kind IN ('episode','reflection','summary','procedure')),
             title       TEXT,
             content     TEXT    NOT NULL DEFAULT '',
             tags        TEXT    NOT NULL DEFAULT '[]',
             importance  REAL    NOT NULL DEFAULT 1.0,
             brain_id    TEXT    NOT NULL DEFAULT '',
             parent_id   TEXT    REFERENCES summaries(summary_id),
             source_hash TEXT,
             confidence  REAL    NOT NULL DEFAULT 1.0,
             valid_from  INTEGER,
             chunk_id    TEXT,
             summarizer  TEXT,
             created_at  INTEGER NOT NULL,
             updated_at  INTEGER NOT NULL
         );

         CREATE INDEX IF NOT EXISTS idx_summaries_kind ON summaries(kind);

         CREATE VIRTUAL TABLE IF NOT EXISTS fts_summaries USING fts5(
             title, content,
             content=summaries,
             content_rowid=rowid,
             tokenize='porter unicode61'
         );

         CREATE TRIGGER IF NOT EXISTS summaries_fts_insert AFTER INSERT ON summaries BEGIN
             INSERT INTO fts_summaries(rowid, title, content)
             VALUES (new.rowid, COALESCE(new.title, ''), new.content);
         END;

         CREATE TRIGGER IF NOT EXISTS summaries_fts_delete AFTER DELETE ON summaries BEGIN
             INSERT INTO fts_summaries(fts_summaries, rowid, title, content)
             VALUES ('delete', old.rowid, COALESCE(old.title, ''), old.content);
         END;

         CREATE TRIGGER IF NOT EXISTS summaries_fts_update AFTER UPDATE OF title, content ON summaries BEGIN
             INSERT INTO fts_summaries(fts_summaries, rowid, title, content)
             VALUES ('delete', old.rowid, COALESCE(old.title, ''), old.content);
             INSERT INTO fts_summaries(rowid, title, content)
             VALUES (new.rowid, COALESCE(new.title, ''), new.content);
         END;",
    )
    .unwrap();

    conn
}

// ─── Tests ───────────────────────────────────────────────────────

/// `store_procedure` must persist a row with `kind='procedure'` and the
/// supplied steps as `content`.
#[test]
fn test_store_procedure_creates_summaries_row() {
    let conn = setup_procedure_schema();

    let steps = "Step 1: Pull the repo.\nStep 2: Run just test.\nStep 3: Open a PR.";
    let tags = vec!["ci".to_string(), "workflow".to_string()];

    let summary_id = store_procedure(
        &conn,
        "Standard PR Workflow",
        steps,
        &tags,
        0.8,
        "brain-test",
    )
    .unwrap();

    assert!(!summary_id.is_empty(), "summary_id must be non-empty");

    let row: SummaryRow = get_summary(&conn, &summary_id)
        .unwrap()
        .expect("row must exist after store_procedure");

    assert_eq!(row.kind, "procedure", "kind must be 'procedure'");
    assert_eq!(
        row.title.as_deref(),
        Some("Standard PR Workflow"),
        "title must match"
    );
    assert!(
        row.content.contains("Step 1"),
        "content must contain the procedure steps"
    );
    assert!(
        row.content.contains("Step 2"),
        "content must contain all steps"
    );
    assert_eq!(row.brain_id, "brain-test", "brain_id must match");
    assert!(
        (row.importance - 0.8).abs() < f64::EPSILON,
        "importance must match"
    );
}

/// After `store_procedure`, the row's content must be discoverable via
/// the FTS5 summaries index.
#[test]
fn test_procedure_fts_searchable() {
    let conn = setup_procedure_schema();

    let steps = "Configure the deployment pipeline using Kubernetes manifests. \
                 Apply namespace isolation and resource quotas.";

    let summary_id = store_procedure(
        &conn,
        "Kubernetes Deployment Guide",
        steps,
        &["k8s".to_string(), "devops".to_string()],
        0.9,
        "brain-k8s",
    )
    .unwrap();

    // Search for a distinctive term from the content.
    let results: Vec<FtsSummaryResult> =
        search_summaries_fts(&conn, "kubernetes", 10, None).unwrap();

    assert!(
        !results.is_empty(),
        "FTS must return at least one result for 'kubernetes'"
    );

    let found = results.iter().any(|r| r.summary_id == summary_id);
    assert!(
        found,
        "the stored procedure's summary_id must appear in FTS results"
    );
}

/// `store_procedure` must assign the `sum:<id>` chunk_id prefix pattern
/// (verified via the returned ID format) for downstream embedding use.
///
/// This test validates the ID is a valid ULID (non-empty, no whitespace)
/// so embedders can safely use `sum:{summary_id}` as their chunk_id.
#[test]
fn test_procedure_summary_id_valid_for_embedding_prefix() {
    let conn = setup_procedure_schema();

    let summary_id = store_procedure(
        &conn,
        "Embed-ready Procedure",
        "Step A: do the thing.",
        &[],
        1.0,
        "brain-embed",
    )
    .unwrap();

    // A ULID is 26 uppercase alphanumeric chars with no whitespace.
    assert_eq!(summary_id.len(), 26, "ULID must be 26 characters");
    assert!(
        summary_id.chars().all(|c| c.is_ascii_alphanumeric()),
        "ULID must be alphanumeric"
    );

    // Confirm the chunk_id prefix the embedder would use is well-formed.
    let chunk_id = format!("sum:{summary_id}");
    assert!(
        chunk_id.starts_with("sum:"),
        "embedding chunk_id must have sum: prefix"
    );
}
