use brain_persistence::db::schema::init_schema;
use rusqlite::Connection;

/// Build a v50 fixture: initialise the current schema (v51) and downgrade
/// user_version to 50 so a subsequent `init_schema` exercises v50→v51.
///
/// We insert representative rows into `tasks`, `task_deps`, and `record_links`
/// AFTER downgrading the version so the backfill has data to walk.
fn snapshot_at_v50_with_data() -> Connection {
    let conn = Connection::open_in_memory().expect("open in-memory sqlite");
    conn.pragma_update(None, "journal_mode", "WAL").unwrap();
    conn.pragma_update(None, "foreign_keys", "OFF").unwrap();

    // Bring up to current schema (v51), then rewind to 50.
    init_schema(&conn).expect("initialize current schema");

    // Clear any entity_links rows that init_schema may have produced.
    conn.execute("DELETE FROM entity_links", []).unwrap();

    // Insert tasks: two with parent_task_id, one root.
    conn.execute_batch(
        "INSERT INTO tasks (task_id, brain_id, title, status, priority, task_type, created_at, updated_at)
             VALUES ('t-root', '', 'Root task', 'open', 4, 'task', 1000, 1001);
         INSERT INTO tasks (task_id, brain_id, title, status, priority, task_type, parent_task_id, created_at, updated_at)
             VALUES ('t-child1', '', 'Child 1', 'open', 4, 'task', 't-root', 1002, 1003);
         INSERT INTO tasks (task_id, brain_id, title, status, priority, task_type, parent_task_id, created_at, updated_at)
             VALUES ('t-child2', '', 'Child 2', 'open', 4, 'task', 't-root', 1004, 1005);",
    )
    .unwrap();

    // Insert task_deps: one dep edge (t-child2 blocks t-child1).
    conn.execute_batch(
        "INSERT INTO task_deps (task_id, depends_on) VALUES ('t-child1', 't-child2');",
    )
    .unwrap();

    // Insert records referenced by record_links.
    conn.execute_batch(
        "INSERT INTO records (record_id, brain_id, title, kind, status, content_hash, content_size, actor, created_at, updated_at)
             VALUES ('rec-1', '', 'Rec 1', 'snapshot', 'active', 'hash1', 100, 'test', 2000, 2001);
         INSERT INTO records (record_id, brain_id, title, kind, status, content_hash, content_size, actor, created_at, updated_at)
             VALUES ('rec-2', '', 'Rec 2', 'snapshot', 'active', 'hash2', 200, 'test', 2002, 2003);",
    )
    .unwrap();

    // Insert chunks referenced by record_links.
    conn.execute_batch(
        "INSERT INTO files (file_id, path, indexing_state) VALUES ('f-1', '/test.md', 'idle');
         INSERT INTO chunks (chunk_id, file_id, chunk_ord, chunk_hash, content)
             VALUES ('chunk-1', 'f-1', 0, 'chash1', 'content1');",
    )
    .unwrap();

    // record_links: rec-1 → t-root (covers task), rec-2 → chunk-1 (covers chunk).
    conn.execute_batch(
        "INSERT INTO record_links (record_id, task_id, chunk_id, created_at)
             VALUES ('rec-1', 't-root', NULL, 3000);
         INSERT INTO record_links (record_id, task_id, chunk_id, created_at)
             VALUES ('rec-2', NULL, 'chunk-1', 3001);",
    )
    .unwrap();

    // Stamp user_version back to 50 so init_schema runs v50→v51.
    conn.pragma_update(None, "user_version", 50i32)
        .expect("downgrade user_version to v50 fixture");

    conn
}

fn count(conn: &Connection, sql: &str) -> i64 {
    conn.query_row(sql, [], |row| row.get(0)).unwrap()
}

/// After v50→v51, parent_of edge count matches tasks with parent_task_id.
#[test]
fn test_migration_v51_parent_of_cardinality() {
    let conn = snapshot_at_v50_with_data();

    let expected = count(
        &conn,
        "SELECT COUNT(*) FROM tasks WHERE parent_task_id IS NOT NULL",
    );

    init_schema(&conn).unwrap();

    let actual = count(
        &conn,
        "SELECT COUNT(*) FROM entity_links WHERE edge_kind = 'parent_of'",
    );
    assert_eq!(
        actual, expected,
        "parent_of edge count mismatch: got {actual}, expected {expected}"
    );
}

/// After v50→v51, blocks edge count matches task_deps rows.
#[test]
fn test_migration_v51_blocks_cardinality() {
    let conn = snapshot_at_v50_with_data();

    let expected = count(&conn, "SELECT COUNT(*) FROM task_deps");

    init_schema(&conn).unwrap();

    let actual = count(
        &conn,
        "SELECT COUNT(*) FROM entity_links WHERE edge_kind = 'blocks'",
    );
    assert_eq!(
        actual, expected,
        "blocks edge count mismatch: got {actual}, expected {expected}"
    );
}

/// After v50→v51, covers-task edge count matches record_links with task_id.
#[test]
fn test_migration_v51_covers_task_cardinality() {
    let conn = snapshot_at_v50_with_data();

    let expected = count(
        &conn,
        "SELECT COUNT(*) FROM record_links WHERE task_id IS NOT NULL",
    );

    init_schema(&conn).unwrap();

    let actual = count(
        &conn,
        "SELECT COUNT(*) FROM entity_links WHERE edge_kind = 'covers' AND to_type = 'TASK'",
    );
    assert_eq!(
        actual, expected,
        "covers-task edge count mismatch: got {actual}, expected {expected}"
    );
}

/// After v50→v51, covers-chunk edge count matches record_links with chunk_id.
#[test]
fn test_migration_v51_covers_chunk_cardinality() {
    let conn = snapshot_at_v50_with_data();

    let expected = count(
        &conn,
        "SELECT COUNT(*) FROM record_links WHERE chunk_id IS NOT NULL",
    );

    init_schema(&conn).unwrap();

    let actual = count(
        &conn,
        "SELECT COUNT(*) FROM entity_links WHERE edge_kind = 'covers' AND to_type = 'CHUNK'",
    );
    assert_eq!(
        actual, expected,
        "covers-chunk edge count mismatch: got {actual}, expected {expected}"
    );
}

/// Re-running the migration on a post-v51 DB produces zero new rows (idempotency).
///
/// Strategy: after reaching v51, stamp user_version back to 50 and call
/// init_schema again. It re-runs v50→v51; INSERT OR IGNORE skips all existing
/// tuples. Row count must not change.
#[test]
fn test_migration_v51_idempotent() {
    let conn = snapshot_at_v50_with_data();
    init_schema(&conn).unwrap();

    let count_before: i64 = count(&conn, "SELECT COUNT(*) FROM entity_links");

    // Stamp back to 50 so init_schema re-executes v50→v51.
    conn.pragma_update(None, "user_version", 50i32).unwrap();
    init_schema(&conn).unwrap();

    let count_after: i64 = count(&conn, "SELECT COUNT(*) FROM entity_links");
    assert_eq!(
        count_before, count_after,
        "idempotency violated: re-run added rows ({count_before} → {count_after})"
    );
}

/// PRAGMA user_version returns 51 after migration.
#[test]
fn test_migration_v51_user_version_stamped() {
    let conn = snapshot_at_v50_with_data();
    init_schema(&conn).unwrap();

    let version: i32 = conn
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    assert_eq!(
        version, 51,
        "user_version must be 51 after v50→v51 migration"
    );
}

/// Backfilled rows have NULL brain_scope (audit carve-out: source tables lack scope).
#[test]
fn test_migration_v51_brain_scope_is_null() {
    let conn = snapshot_at_v50_with_data();
    init_schema(&conn).unwrap();

    let non_null_scope: i64 = count(
        &conn,
        "SELECT COUNT(*) FROM entity_links WHERE brain_scope IS NOT NULL",
    );
    assert_eq!(
        non_null_scope, 0,
        "all backfilled entity_links rows must have brain_scope = NULL"
    );
}
