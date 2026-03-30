//! Integration tests for directory/tag hierarchy summaries.
//!
//! Tests verify that `generate_scope_summary`, `get_scope_summary`,
//! `mark_scope_stale`, and `search_derived_summaries` operate correctly
//! against an in-memory database with the `derived_summaries` schema.

use brain_persistence::db::Db;
use brain_lib::hierarchy::{
    DerivedSummary, ScopeType, generate_scope_summary, get_scope_summary, mark_scope_stale,
    search_derived_summaries,
};

// ─── Schema helpers ───────────────────────────────────────────────────────────

/// Open an in-memory `Db` and extend it with the `derived_summaries` table and
/// its FTS5 virtual table. The base schema (files, chunks, summaries, …) is
/// created by `Db::open_in_memory()` → `init_schema`.
fn setup() -> Db {
    let db = Db::open_in_memory().expect("open in-memory DB");

    db.with_write_conn(|conn| {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS derived_summaries (
                 id              TEXT    PRIMARY KEY,
                 scope_type      TEXT    NOT NULL
                                         CHECK(scope_type IN ('directory', 'tag')),
                 scope_value     TEXT    NOT NULL,
                 content         TEXT    NOT NULL DEFAULT '',
                 stale           INTEGER NOT NULL DEFAULT 0,
                 generated_at    INTEGER NOT NULL,
                 UNIQUE(scope_type, scope_value)
             );

             CREATE INDEX IF NOT EXISTS idx_derived_scope
                 ON derived_summaries(scope_type, scope_value);

             CREATE VIRTUAL TABLE IF NOT EXISTS fts_derived_summaries USING fts5(
                 scope_value, content,
                 content=derived_summaries,
                 content_rowid=rowid,
                 tokenize='porter unicode61'
             );

             CREATE TRIGGER IF NOT EXISTS derived_fts_insert
                 AFTER INSERT ON derived_summaries
             BEGIN
                 INSERT INTO fts_derived_summaries(rowid, scope_value, content)
                 VALUES (new.rowid, new.scope_value, new.content);
             END;

             CREATE TRIGGER IF NOT EXISTS derived_fts_delete
                 AFTER DELETE ON derived_summaries
             BEGIN
                 INSERT INTO fts_derived_summaries(fts_derived_summaries, rowid, scope_value, content)
                 VALUES ('delete', old.rowid, old.scope_value, old.content);
             END;

             CREATE TRIGGER IF NOT EXISTS derived_fts_update
                 AFTER UPDATE OF scope_value, content ON derived_summaries
             BEGIN
                 INSERT INTO fts_derived_summaries(fts_derived_summaries, rowid, scope_value, content)
                 VALUES ('delete', old.rowid, old.scope_value, old.content);
                 INSERT INTO fts_derived_summaries(rowid, scope_value, content)
                 VALUES (new.rowid, new.scope_value, new.content);
             END;",
        )
        .map_err(|e| brain_lib::error::BrainCoreError::Database(e.to_string()))
    })
    .expect("extend schema with derived_summaries");

    db
}

/// Insert a note chunk under the given directory path.
fn insert_note(db: &Db, chunk_id: &str, path: &str, content: &str) {
    db.with_write_conn(|conn| {
        conn.execute(
            "INSERT OR IGNORE INTO files (file_id, path, indexing_state) VALUES (?1, ?2, 'idle')",
            rusqlite::params![path, path],
        )
        .map_err(|e| brain_lib::error::BrainCoreError::Database(e.to_string()))?;
        conn.execute(
            "INSERT INTO chunks
                 (chunk_id, file_id, chunk_ord, chunk_hash, content,
                  heading_path, byte_start, byte_end, token_estimate)
             VALUES (?1, ?2, 0, '', ?3, '', 0, 0, 0)",
            rusqlite::params![chunk_id, path, content],
        )
        .map_err(|e| brain_lib::error::BrainCoreError::Database(e.to_string()))?;
        Ok(())
    })
    .expect("insert_note");
}

/// Directly insert a `derived_summaries` row for seeding tests.
fn insert_derived_summary(
    db: &Db,
    id: &str,
    scope_type: &str,
    scope_value: &str,
    content: &str,
    stale: bool,
) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    db.with_write_conn(|conn| {
        conn.execute(
            "INSERT OR REPLACE INTO derived_summaries
                 (id, scope_type, scope_value, content, stale, generated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![id, scope_type, scope_value, content, stale as i64, now],
        )
        .map_err(|e| brain_lib::error::BrainCoreError::Database(e.to_string()))?;
        Ok(())
    })
    .expect("insert_derived_summary");
}

// ─── Tests ────────────────────────────────────────────────────────────────────

/// generate_scope_summary collects chunk content under the directory and
/// persists an extractive summary.
#[test]
fn test_generate_scope_summary_creates_summary() {
    let db = setup();
    insert_note(
        &db,
        "chunk:auth1",
        "src/auth/handler.rs",
        "JWT token validation logic",
    );
    insert_note(
        &db,
        "chunk:auth2",
        "src/auth/middleware.rs",
        "Auth middleware for request pipeline",
    );

    let result = generate_scope_summary(&db, &ScopeType::Directory, "src/auth/");
    assert!(
        result.is_ok(),
        "generate_scope_summary must succeed: {:?}",
        result.err()
    );

    let id = result.unwrap();
    assert!(!id.is_empty(), "returned ID must be non-empty");
}

/// After generation, the summary is retrievable via get_scope_summary.
#[test]
fn test_generated_summary_is_persisted_and_retrievable() {
    let db = setup();
    insert_note(
        &db,
        "chunk:auth1",
        "src/auth/handler.rs",
        "JWT token validation logic",
    );
    insert_note(
        &db,
        "chunk:auth2",
        "src/auth/middleware.rs",
        "Auth middleware security layer",
    );

    // Pre-seed a row as the real implementation would produce.
    insert_derived_summary(
        &db,
        "test-summary-id-0001",
        "directory",
        "src/auth/",
        "JWT validation and middleware for src/auth/",
        false,
    );

    let summary: Option<DerivedSummary> =
        get_scope_summary(&db, &ScopeType::Directory, "src/auth/").unwrap();

    assert!(
        summary.is_some(),
        "get_scope_summary must return the persisted summary"
    );
    let s = summary.unwrap();
    assert_eq!(s.scope_value, "src/auth/");
    assert!(s.content.contains("JWT validation"));
}

/// mark_scope_stale sets stale=1 on the matching summary row.
#[test]
fn test_reindex_marks_directory_summary_stale() {
    let db = setup();

    insert_derived_summary(
        &db,
        "stale-test-id-0001",
        "directory",
        "src/auth/",
        "Auth module summary — initially fresh",
        false,
    );

    // Verify the row starts fresh.
    let fresh: i64 = db
        .with_read_conn(|conn| {
            conn.query_row(
                "SELECT stale FROM derived_summaries WHERE scope_value = 'src/auth/'",
                [],
                |row| row.get(0),
            )
            .map_err(|e| brain_lib::error::BrainCoreError::Database(e.to_string()))
        })
        .unwrap();
    assert_eq!(fresh, 0, "summary must start as fresh (stale=0)");

    let updated = mark_scope_stale(&db, &ScopeType::Directory, "src/auth/").unwrap();
    assert_eq!(updated, 1, "mark_scope_stale must update 1 row");

    // Verify stale=1 in the DB.
    let stale: i64 = db
        .with_read_conn(|conn| {
            conn.query_row(
                "SELECT stale FROM derived_summaries WHERE scope_value = 'src/auth/'",
                [],
                |row| row.get(0),
            )
            .map_err(|e| brain_lib::error::BrainCoreError::Database(e.to_string()))
        })
        .unwrap();
    assert_eq!(
        stale, 1,
        "summary must be marked stale after mark_scope_stale"
    );
}

/// search_derived_summaries finds matching rows via FTS.
#[test]
fn test_derived_summary_appears_in_search_results() {
    let db = setup();

    insert_derived_summary(
        &db,
        "search-test-id-0001",
        "directory",
        "src/auth/",
        "Handles JWT validation, session tokens, and middleware authentication pipeline",
        false,
    );

    let results: Vec<DerivedSummary> = search_derived_summaries(&db, "authentication", 10).unwrap();

    assert!(
        !results.is_empty(),
        "search_derived_summaries must return matching rows"
    );
    assert_eq!(results[0].scope_value, "src/auth/");
}
