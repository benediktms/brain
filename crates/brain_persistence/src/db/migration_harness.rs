//! Migration test harness: loads schema snapshots at historical versions,
//! runs migrations, and validates post-migration invariants.
//!
//! Each test creates a database frozen at an older schema version, seeds it
//! with representative data, then calls `init_schema` to bring it to the
//! current version. Assertions verify:
//!
//! - Schema version is stamped correctly
//! - All expected tables and indexes exist
//! - Seeded data survives the migration intact
//! - FTS5 virtual table and triggers are present

use rusqlite::Connection;

use super::migrations::{
    migrate_v0_to_v1, migrate_v1_to_v2, migrate_v2_to_v3, migrate_v3_to_v4, migrate_v4_to_v5,
    migrate_v5_to_v6, migrate_v6_to_v7, migrate_v7_to_v8, migrate_v8_to_v9, migrate_v9_to_v10,
    migrate_v10_to_v11, migrate_v11_to_v12, migrate_v12_to_v13, migrate_v13_to_v14,
    migrate_v14_to_v15, migrate_v15_to_v16, migrate_v16_to_v17, migrate_v17_to_v18,
    migrate_v18_to_v19, migrate_v19_to_v20, migrate_v20_to_v21, migrate_v21_to_v22,
    migrate_v22_to_v23, migrate_v23_to_v24, migrate_v24_to_v25, migrate_v25_to_v26,
    migrate_v26_to_v27, migrate_v27_to_v28, migrate_v28_to_v29, migrate_v29_to_v30,
    migrate_v30_to_v31, migrate_v31_to_v32, migrate_v32_to_v33, migrate_v33_to_v34,
    migrate_v34_to_v35, migrate_v35_to_v36, migrate_v36_to_v37,
};
use super::schema::{SCHEMA_VERSION, init_schema};

// ─── Snapshot helpers ────────────────────────────────────────────

/// Create an in-memory database frozen at a specific schema version.
///
/// Runs migrations 0..version sequentially, producing a database that
/// looks exactly like one created when that version was current.
fn snapshot_at_version(version: i32) -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    conn.pragma_update(None, "journal_mode", "WAL").unwrap();
    conn.pragma_update(None, "foreign_keys", "ON").unwrap();

    for v in 0..version {
        match v {
            0 => migrate_v0_to_v1(&conn).unwrap(),
            1 => migrate_v1_to_v2(&conn).unwrap(),
            2 => migrate_v2_to_v3(&conn).unwrap(),
            3 => migrate_v3_to_v4(&conn).unwrap(),
            4 => migrate_v4_to_v5(&conn).unwrap(),
            5 => migrate_v5_to_v6(&conn).unwrap(),
            6 => migrate_v6_to_v7(&conn).unwrap(),
            7 => migrate_v7_to_v8(&conn).unwrap(),
            8 => migrate_v8_to_v9(&conn).unwrap(),
            9 => migrate_v9_to_v10(&conn).unwrap(),
            10 => migrate_v10_to_v11(&conn).unwrap(),
            11 => migrate_v11_to_v12(&conn).unwrap(),
            12 => migrate_v12_to_v13(&conn).unwrap(),
            13 => migrate_v13_to_v14(&conn).unwrap(),
            14 => migrate_v14_to_v15(&conn).unwrap(),
            15 => migrate_v15_to_v16(&conn).unwrap(),
            16 => migrate_v16_to_v17(&conn).unwrap(),
            17 => migrate_v17_to_v18(&conn).unwrap(),
            18 => migrate_v18_to_v19(&conn).unwrap(),
            19 => migrate_v19_to_v20(&conn).unwrap(),
            20 => migrate_v20_to_v21(&conn).unwrap(),
            21 => migrate_v21_to_v22(&conn).unwrap(),
            22 => migrate_v22_to_v23(&conn).unwrap(),
            23 => migrate_v23_to_v24(&conn).unwrap(),
            24 => migrate_v24_to_v25(&conn).unwrap(),
            25 => migrate_v25_to_v26(&conn).unwrap(),
            26 => migrate_v26_to_v27(&conn).unwrap(),
            27 => migrate_v27_to_v28(&conn).unwrap(),
            28 => migrate_v28_to_v29(&conn).unwrap(),
            29 => migrate_v29_to_v30(&conn).unwrap(),
            30 => migrate_v30_to_v31(&conn).unwrap(),
            31 => migrate_v31_to_v32(&conn).unwrap(),
            32 => migrate_v32_to_v33(&conn).unwrap(),
            33 => migrate_v33_to_v34(&conn).unwrap(),
            34 => migrate_v34_to_v35(&conn).unwrap(),
            35 => migrate_v35_to_v36(&conn).unwrap(),
            36 => migrate_v36_to_v37(&conn).unwrap(),
            _ => panic!("no snapshot migration for version {v}"),
        }
    }
    conn
}

/// Seed core tables (files + chunks) available from v1 onward.
fn seed_v1_data(conn: &Connection) {
    conn.execute(
        "INSERT INTO files (file_id, path, content_hash, indexing_state)
         VALUES ('f1', '/notes/hello.md', 'abc123', 'indexed')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO chunks (chunk_id, file_id, chunk_ord, chunk_hash, content)
         VALUES ('f1:0', 'f1', 0, 'h0', 'hello world')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO chunks (chunk_id, file_id, chunk_ord, chunk_hash, content)
         VALUES ('f1:1', 'f1', 1, 'h1', 'second chunk')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO links (link_id, source_file_id, target_path, link_type)
         VALUES ('l1', 'f1', '/notes/other.md', 'wiki')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO summaries (summary_id, kind, content, created_at, updated_at)
         VALUES ('s1', 'episode', 'a summary', 1000, 1000)",
        [],
    )
    .unwrap();
}

/// Seed task tables available from v2 onward.
fn seed_v2_data(conn: &Connection) {
    conn.execute(
        "INSERT INTO tasks (task_id, title, status, priority, created_at, updated_at)
         VALUES ('t1', 'Fix bug', 'open', 1, 1000, 1000)",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO tasks (task_id, title, status, priority, created_at, updated_at)
         VALUES ('t2', 'Add feature', 'in_progress', 2, 1001, 1001)",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO task_deps (task_id, depends_on) VALUES ('t2', 't1')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO task_events (event_id, task_id, event_type, timestamp, actor)
         VALUES ('e1', 't1', 'created', 1000, 'user')",
        [],
    )
    .unwrap();
}

/// Seed v3-specific data (labels, comments).
fn seed_v3_data(conn: &Connection) {
    conn.execute(
        "INSERT INTO task_labels (task_id, label) VALUES ('t1', 'bug')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO task_comments (comment_id, task_id, author, body, created_at)
         VALUES ('c1', 't1', 'alice', 'looks good', 2000)",
        [],
    )
    .unwrap();
}

// ─── Invariant assertions ────────────────────────────────────────

/// All tables that must exist at the current schema version.
const EXPECTED_TABLES: &[&str] = &[
    "files",
    "chunks",
    "links",
    "summaries",
    "reflection_sources",
    "tasks",
    "task_deps",
    "task_note_links",
    "task_events",
    "task_labels",
    "task_comments",
    "brain_meta",
    "task_external_ids",
    "records",
    "record_tags",
    "record_links",
    "record_events",
    "brains",
    "jobs",
    "providers",
    "summary_sources",
];

/// All named indexes that must exist at the current schema version.
const EXPECTED_INDEXES: &[&str] = &[
    "idx_chunks_file_id",
    "idx_links_source",
    "idx_links_target",
    "idx_summaries_kind",
    "idx_tasks_parent",
    "idx_tasks_status",
    "idx_tasks_defer_until",
    "idx_task_deps_depends_on",
    "idx_task_comments_task_id",
    "idx_task_events_task_id",
    "idx_external_lookup",
    "idx_tasks_status_priority",
    "idx_tasks_brain_status",
    "idx_tasks_brain_priority",
    "idx_records_brain",
    "idx_records_brain_status",
    "idx_summaries_brain_id",
    "idx_tasks_brain_display_id",
    "idx_jobs_poll",
    "idx_jobs_kind",
    "idx_summary_sources_source",
];

/// FTS5 triggers that must exist.
const EXPECTED_TRIGGERS: &[&str] = &[
    "chunks_fts_delete",
    "chunks_fts_insert",
    "chunks_fts_update",
    "tasks_fts_delete",
    "tasks_fts_insert",
    "tasks_fts_update",
];

fn assert_schema_version(conn: &Connection, expected: i32) {
    let version: i32 = conn
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    assert_eq!(version, expected, "schema version mismatch");
}

fn assert_tables_exist(conn: &Connection) {
    let tables: Vec<String> = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .filter_map(|r| r.ok())
        .collect();

    for expected in EXPECTED_TABLES {
        assert!(
            tables.contains(&expected.to_string()),
            "missing table: {expected} (found: {tables:?})"
        );
    }
}

fn assert_indexes_exist(conn: &Connection) {
    let indexes: Vec<String> = conn
        .prepare(
            "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%' ORDER BY name",
        )
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .filter_map(|r| r.ok())
        .collect();

    for expected in EXPECTED_INDEXES {
        assert!(
            indexes.contains(&expected.to_string()),
            "missing index: {expected} (found: {indexes:?})"
        );
    }
}

fn assert_fts5_exists(conn: &Connection) {
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE name = 'fts_chunks'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(count > 0, "fts_chunks virtual table should exist");

    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE name = 'fts_tasks'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(count > 0, "fts_tasks virtual table should exist");

    let triggers: Vec<String> = conn
        .prepare(
            "SELECT name FROM sqlite_master WHERE type='trigger' AND (name LIKE 'chunks_fts_%' OR name LIKE 'tasks_fts_%') ORDER BY name",
        )
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .filter_map(|r| r.ok())
        .collect();

    for expected in EXPECTED_TRIGGERS {
        assert!(
            triggers.contains(&expected.to_string()),
            "missing trigger: {expected} (found: {triggers:?})"
        );
    }
}

fn assert_row_count(conn: &Connection, table: &str, expected: i64) {
    let count: i64 = conn
        .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(
        count, expected,
        "row count mismatch for {table}: got {count}, expected {expected}"
    );
}

/// Assert all post-migration structural invariants.
fn assert_full_invariants(conn: &Connection) {
    assert_schema_version(conn, SCHEMA_VERSION);
    assert_tables_exist(conn);
    assert_indexes_exist(conn);
    assert_fts5_exists(conn);
}

// ─── Migration tests from each historical version ────────────────

#[test]
fn migrate_from_v0_to_current() {
    let conn = snapshot_at_version(0);
    init_schema(&conn).unwrap();
    assert_full_invariants(&conn);
}

#[test]
fn migrate_from_v1_to_current() {
    let conn = snapshot_at_version(1);
    seed_v1_data(&conn);

    init_schema(&conn).unwrap();
    assert_full_invariants(&conn);

    assert_row_count(&conn, "files", 1);
    assert_row_count(&conn, "chunks", 2);
    assert_row_count(&conn, "links", 1);
    assert_row_count(&conn, "summaries", 1);
}

#[test]
fn migrate_from_v2_to_current() {
    let conn = snapshot_at_version(2);
    seed_v1_data(&conn);
    seed_v2_data(&conn);

    init_schema(&conn).unwrap();
    assert_full_invariants(&conn);

    assert_row_count(&conn, "files", 1);
    assert_row_count(&conn, "chunks", 2);
    assert_row_count(&conn, "tasks", 2);
    assert_row_count(&conn, "task_deps", 1);
    assert_row_count(&conn, "task_events", 1);
}

#[test]
fn migrate_from_v3_to_current() {
    let conn = snapshot_at_version(3);
    seed_v1_data(&conn);
    seed_v2_data(&conn);
    seed_v3_data(&conn);

    init_schema(&conn).unwrap();
    assert_full_invariants(&conn);

    assert_row_count(&conn, "files", 1);
    assert_row_count(&conn, "chunks", 2);
    assert_row_count(&conn, "tasks", 2);
    assert_row_count(&conn, "task_labels", 1);
    assert_row_count(&conn, "task_comments", 1);

    // v3 added task_type column — verify default value survived
    let task_type: String = conn
        .query_row(
            "SELECT task_type FROM tasks WHERE task_id = 't1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(task_type, "task");
}

#[test]
fn migrate_from_v4_to_current() {
    let conn = snapshot_at_version(4);
    seed_v1_data(&conn);
    seed_v2_data(&conn);
    seed_v3_data(&conn);

    // v4 adds parent_task_id — set a parent relationship
    conn.execute(
        "UPDATE tasks SET parent_task_id = 't1' WHERE task_id = 't2'",
        [],
    )
    .unwrap();

    init_schema(&conn).unwrap();
    assert_full_invariants(&conn);

    // Verify parent relationship survived
    let parent: Option<String> = conn
        .query_row(
            "SELECT parent_task_id FROM tasks WHERE task_id = 't2'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(parent, Some("t1".to_string()));
}

#[test]
fn migrate_from_v5_to_current() {
    let conn = snapshot_at_version(5);
    seed_v1_data(&conn);
    seed_v2_data(&conn);
    seed_v3_data(&conn);

    init_schema(&conn).unwrap();
    assert_full_invariants(&conn);

    // v5→v6 adds brain_meta table — verify it's functional
    conn.execute(
        "INSERT INTO brain_meta (key, value) VALUES ('test_key', 'test_value')",
        [],
    )
    .unwrap();
    let val: String = conn
        .query_row(
            "SELECT value FROM brain_meta WHERE key = 'test_key'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(val, "test_value");
}

#[test]
fn migrate_from_v6_to_current() {
    let conn = snapshot_at_version(6);
    seed_v1_data(&conn);
    seed_v2_data(&conn);
    seed_v3_data(&conn);

    init_schema(&conn).unwrap();
    assert_full_invariants(&conn);

    // v6→v7 adds task_external_ids table — verify it's functional
    conn.execute(
        "INSERT INTO task_external_ids (task_id, source, external_id, imported_at)
         VALUES ('t1', 'github', 'GH-42', 3000)",
        [],
    )
    .unwrap();
    let ext_id: String = conn
        .query_row(
            "SELECT external_id FROM task_external_ids WHERE task_id = 't1' AND source = 'github'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(ext_id, "GH-42");
}

#[test]
fn migrate_from_v7_to_current() {
    let conn = snapshot_at_version(7);
    seed_v1_data(&conn);
    seed_v2_data(&conn);
    seed_v3_data(&conn);

    init_schema(&conn).unwrap();
    assert_full_invariants(&conn);

    // v7→v8 adds child_seq column — verify it exists and is nullable
    let child_seq: Option<i64> = conn
        .query_row(
            "SELECT child_seq FROM tasks WHERE task_id = 't1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(child_seq, None);

    // Set a child_seq value
    conn.execute("UPDATE tasks SET child_seq = 1 WHERE task_id = 't2'", [])
        .unwrap();
    let child_seq: Option<i64> = conn
        .query_row(
            "SELECT child_seq FROM tasks WHERE task_id = 't2'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(child_seq, Some(1));
}

#[test]
fn migrate_from_v8_to_current() {
    let conn = snapshot_at_version(8);
    seed_v1_data(&conn);
    seed_v2_data(&conn);
    seed_v3_data(&conn);

    init_schema(&conn).unwrap();
    assert_full_invariants(&conn);

    // v8→v9 adds chunker_version column — verify it exists and is nullable
    let chunker_version: Option<u32> = conn
        .query_row(
            "SELECT chunker_version FROM files WHERE file_id = 'f1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(chunker_version, None);

    // Set a chunker_version value
    conn.execute(
        "UPDATE files SET chunker_version = 2 WHERE file_id = 'f1'",
        [],
    )
    .unwrap();
    let chunker_version: Option<u32> = conn
        .query_row(
            "SELECT chunker_version FROM files WHERE file_id = 'f1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(chunker_version, Some(2));
}

#[test]
fn migrate_from_v9_to_current() {
    let conn = snapshot_at_version(9);
    seed_v1_data(&conn);
    seed_v2_data(&conn);
    seed_v3_data(&conn);

    init_schema(&conn).unwrap();
    assert_full_invariants(&conn);

    // v9→v10 adds composite index — verify it exists
    let has_index: bool = conn
        .query_row(
            "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='index' AND name = 'idx_tasks_status_priority'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(
        has_index,
        "idx_tasks_status_priority should exist after v9→v10"
    );
}

#[test]
fn migrate_from_v10_to_current() {
    let conn = snapshot_at_version(10);
    seed_v1_data(&conn);
    seed_v2_data(&conn);
    seed_v3_data(&conn);

    init_schema(&conn).unwrap();
    assert_full_invariants(&conn);

    // v10→v11 adds fts_tasks — verify it exists and indexes existing tasks
    // Rebuild FTS content since triggers only fire on new inserts
    conn.execute("INSERT INTO fts_tasks(fts_tasks) VALUES('rebuild')", [])
        .unwrap();

    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM fts_tasks WHERE fts_tasks MATCH 'bug'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 1, "FTS5 should find 'Fix bug' task title");
}

#[test]
fn fts_tasks_triggers_insert_update_delete() {
    let conn = snapshot_at_version(SCHEMA_VERSION);
    init_schema(&conn).unwrap();

    // Insert a task — trigger should populate fts_tasks
    conn.execute(
        "INSERT INTO tasks (task_id, title, description, status, priority, task_type, created_at, updated_at)
         VALUES ('ft1', 'Search filtering', 'implement FTS5', 'open', 2, 'task', 1000, 1000)",
        [],
    )
    .unwrap();

    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM fts_tasks WHERE fts_tasks MATCH 'filtering'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 1, "insert trigger should index the task");

    // Update title — trigger should update fts_tasks
    conn.execute(
        "UPDATE tasks SET title = 'Updated title' WHERE task_id = 'ft1'",
        [],
    )
    .unwrap();

    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM fts_tasks WHERE fts_tasks MATCH 'filtering'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 0, "old title should be removed from FTS");

    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM fts_tasks WHERE fts_tasks MATCH 'Updated'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 1, "new title should be in FTS");

    // Delete the task — trigger should clean up fts_tasks
    conn.execute("DELETE FROM tasks WHERE task_id = 'ft1'", [])
        .unwrap();

    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM fts_tasks WHERE fts_tasks MATCH 'Updated'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 0, "deleted task should be removed from FTS");
}

// ─── Snapshot fixture on disk ────────────────────────────────────

#[test]
fn migrate_on_disk_snapshot_from_v3() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db_path = tmp.path().join("snapshot_v3.db");

    // Create a v3 database on disk with real data
    {
        let conn = Connection::open(&db_path).unwrap();
        conn.pragma_update(None, "journal_mode", "WAL").unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();
        migrate_v0_to_v1(&conn).unwrap();
        migrate_v1_to_v2(&conn).unwrap();
        migrate_v2_to_v3(&conn).unwrap();
        seed_v1_data(&conn);
        seed_v2_data(&conn);
        seed_v3_data(&conn);
    }

    // Reopen with Db::open — triggers init_schema which runs v3→v4→v5→v6
    let db = crate::db::Db::open(&db_path).unwrap();

    db.with_read_conn(|conn| {
        assert_full_invariants(conn);
        assert_row_count(conn, "files", 1);
        assert_row_count(conn, "chunks", 2);
        assert_row_count(conn, "tasks", 2);
        assert_row_count(conn, "task_labels", 1);
        assert_row_count(conn, "task_comments", 1);
        Ok(())
    })
    .unwrap();
}

// ─── Edge cases ──────────────────────────────────────────────────

#[test]
fn migrate_idempotent_from_current() {
    let conn = snapshot_at_version(SCHEMA_VERSION);
    seed_v1_data(&conn);
    seed_v2_data(&conn);

    // Running init_schema on an already-current DB should be a no-op
    init_schema(&conn).unwrap();
    init_schema(&conn).unwrap();

    assert_full_invariants(&conn);
    assert_row_count(&conn, "files", 1);
    assert_row_count(&conn, "tasks", 2);
}

#[test]
fn migrate_preserves_foreign_keys() {
    let conn = snapshot_at_version(1);
    seed_v1_data(&conn);

    init_schema(&conn).unwrap();

    // Rebuild FTS5 so the delete trigger can find the rows it needs to remove
    conn.execute("INSERT INTO fts_chunks(fts_chunks) VALUES('rebuild')", [])
        .unwrap();

    // Deleting a file should cascade-delete its chunks and links
    conn.execute("DELETE FROM files WHERE file_id = 'f1'", [])
        .unwrap();

    assert_row_count(&conn, "chunks", 0);
    assert_row_count(&conn, "links", 0);
}

#[test]
fn migrate_fts5_works_after_migration() {
    let conn = snapshot_at_version(1);
    seed_v1_data(&conn);

    init_schema(&conn).unwrap();

    // FTS5 should index the chunks that existed before migration
    // Since ensure_fts5 recreates triggers, we need to rebuild FTS content
    conn.execute("INSERT INTO fts_chunks(fts_chunks) VALUES('rebuild')", [])
        .unwrap();

    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM fts_chunks WHERE fts_chunks MATCH 'hello'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 1, "FTS5 should find seeded chunk content");
}

#[test]
fn migrate_v2_tasks_gain_v3_columns() {
    let conn = snapshot_at_version(2);
    seed_v1_data(&conn);
    seed_v2_data(&conn);

    init_schema(&conn).unwrap();

    // Tasks created at v2 should now have v3 columns with defaults
    let row: (String, Option<String>, Option<i64>) = conn
        .query_row(
            "SELECT task_type, assignee, defer_until FROM tasks WHERE task_id = 't1'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(row.0, "task");
    assert_eq!(row.1, None);
    assert_eq!(row.2, None);

    // v4 column: parent_task_id should also exist with NULL default
    let parent: Option<String> = conn
        .query_row(
            "SELECT parent_task_id FROM tasks WHERE task_id = 't1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(parent, None);
}
