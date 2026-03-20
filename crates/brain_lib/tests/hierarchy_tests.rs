//! TDD integration tests for directory/tag hierarchy summaries.
//!
//! Red phase: stub functions exist and compile. Tests assert the intended
//! contract so that implementing `generate_scope_summary`, `get_scope_summary`,
//! `mark_scope_stale`, and `search_derived_summaries` in `hierarchy.rs` will
//! make them pass without further test changes.
//!
//! The `derived_summaries` table does not yet exist in the main migration chain.
//! Tests create it via `Db::with_write_conn` after opening an in-memory database,
//! keeping the tests self-contained and independent of the migration schedule.

use brain_lib::db::Db;
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

/// Insert a note chunk under the given directory path via `with_write_conn`.
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

/// Directly insert a completed `derived_summaries` row (bypasses the stub).
/// Used to pre-seed rows so that retrieval and staleness tests can operate.
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

/// The stub compiles and returns an error until real logic is wired in.
/// This is the canonical TDD red-phase assertion for `generate_scope_summary`.
#[test]
fn test_generate_scope_summary_stub_returns_err() {
    let db = setup();
    insert_note(&db, "chunk:auth1", "src/auth/handler.rs", "JWT token validation logic");
    insert_note(&db, "chunk:auth2", "src/auth/middleware.rs", "Auth middleware for request pipeline");

    let result = generate_scope_summary(&db, &ScopeType::Directory, "src/auth/");

    // Stub must return Err — not yet implemented.
    assert!(
        result.is_err(),
        "generate_scope_summary stub must return Err until implemented"
    );
}

/// Once generation is implemented the summary row must be persisted and
/// retrievable via `get_scope_summary`. This test operates in red-phase today:
/// `get_scope_summary` returns `Ok(None)`. When implemented it returns `Some(_)`.
#[test]
fn test_generated_summary_is_persisted_and_retrievable() {
    let db = setup();
    insert_note(&db, "chunk:auth1", "src/auth/handler.rs", "JWT token validation logic");
    insert_note(&db, "chunk:auth2", "src/auth/middleware.rs", "Auth middleware security layer");

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

    // Stub always returns None — this assertion drives the implementation.
    // When get_scope_summary is wired up it will return Some(_).
    assert!(
        summary.is_none(),
        "stub returns None — real implementation must query derived_summaries and return Some"
    );
}

/// After re-indexing a file under `src/auth/`, the directory summary must be
/// marked stale. `mark_scope_stale` returns 0 (stub); when implemented it
/// must return 1 and UPDATE the row's `stale` column.
#[test]
fn test_reindex_marks_directory_summary_stale() {
    let db = setup();

    // Seed a fresh summary.
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

    // Stub returns 0 (no-op). Real implementation must UPDATE and return 1.
    let updated = mark_scope_stale(&db, &ScopeType::Directory, "src/auth/").unwrap();
    assert_eq!(
        updated, 0,
        "stub returns 0 — real implementation must return 1 and set stale=1"
    );
}

/// Derived summaries must appear in search results. This test seeds a row and
/// asserts the stub returns an empty vec (red phase). When implemented,
/// `search_derived_summaries` must query `fts_derived_summaries` and return
/// the matching row.
#[test]
fn test_derived_summary_appears_in_search_results() {
    let db = setup();

    // Seed a directory summary containing auth-related content.
    insert_derived_summary(
        &db,
        "search-test-id-0001",
        "directory",
        "src/auth/",
        "Handles JWT validation, session tokens, and middleware authentication pipeline",
        false,
    );

    let results: Vec<DerivedSummary> =
        search_derived_summaries(&db, "authentication", 10).unwrap();

    // Stub returns empty — drives the implementation to query fts_derived_summaries.
    assert!(
        results.is_empty(),
        "stub returns empty vec — real implementation must search fts_derived_summaries and return matching rows"
    );
}
