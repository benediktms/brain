use brain_persistence::db::schema::init_schema;
use rusqlite::Connection;

/// Build a v51 fixture: initialize the current schema (v52) and downgrade
/// user_version to 51 so a subsequent `init_schema` exercises v51→v52.
///
/// The v51→v52 migration adds the `updated_at` column to `task_comments`
/// and backfills `updated_at = created_at` for existing rows.
fn snapshot_at_v51_with_data() -> Connection {
    let conn = Connection::open_in_memory().expect("open in-memory sqlite");
    conn.pragma_update(None, "journal_mode", "WAL").unwrap();
    conn.pragma_update(None, "foreign_keys", "OFF").unwrap();

    // Bring up to current schema (v52), then rewind to 51.
    init_schema(&conn).expect("initialize current schema");

    // Insert a task with a comment so the backfill has data to work with.
    conn.execute_batch(
        "INSERT INTO tasks (task_id, brain_id, title, status, priority, task_type, created_at, updated_at)
             VALUES ('t1', '', 'Test task', 'open', 4, 'task', 1000, 1001);
         INSERT INTO task_comments (comment_id, task_id, author, body, created_at)
             VALUES ('c1', 't1', 'alice', 'hello', 1000);
         INSERT INTO task_comments (comment_id, task_id, author, body, created_at)
             VALUES ('c2', 't1', 'bob', 'world', 2000);",
    )
    .unwrap();

    // Stamp user_version back to 51 so init_schema runs v51→v52.
    conn.pragma_update(None, "user_version", 51i32)
        .expect("downgrade user_version to v51 fixture");

    conn
}

fn count(conn: &Connection, sql: &str) -> i64 {
    conn.query_row(sql, [], |row| row.get(0)).unwrap()
}

/// After v51→v52, the `updated_at` column exists on `task_comments`.
#[test]
fn test_migration_v52_updated_at_column_exists() {
    let conn = snapshot_at_v51_with_data();
    init_schema(&conn).unwrap();

    let exists: bool = conn
        .query_row(
            "SELECT 1 FROM pragma_table_xinfo('task_comments') WHERE name = 'updated_at' LIMIT 1",
            [],
            |_row| Ok(true),
        )
        .ok()
        .unwrap_or(false);
    assert!(
        exists,
        "updated_at column must exist on task_comments after v51→v52"
    );
}

/// After v51→v52, existing comments have `updated_at = created_at`.
#[test]
fn test_migration_v52_backfills_updated_at() {
    let conn = snapshot_at_v51_with_data();
    init_schema(&conn).unwrap();

    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM task_comments WHERE updated_at = created_at",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        count, 2,
        "all existing comments must have updated_at = created_at (got {count})"
    );
}

/// PRAGMA user_version returns 52 after migration.
#[test]
fn test_migration_v52_user_version_stamped() {
    let conn = snapshot_at_v51_with_data();
    init_schema(&conn).unwrap();

    let version: i32 = conn
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    assert_eq!(
        version, 52,
        "user_version must be 52 after v51→v52 migration"
    );
}

/// Re-running the migration on a post-v52 DB produces no changes (idempotency).
///
/// Strategy: after reaching v52, stamp user_version back to 51 and call
/// init_schema again. It re-runs v51→v52; the column-existence guard prevents
/// a double-add. Row count must not change.
#[test]
fn test_migration_v52_idempotent() {
    let conn = snapshot_at_v51_with_data();
    init_schema(&conn).unwrap();

    let count_before: i64 = count(&conn, "SELECT COUNT(*) FROM task_comments");

    // Stamp back to 51 so init_schema re-executes v51→v52.
    conn.pragma_update(None, "user_version", 51i32).unwrap();
    init_schema(&conn).unwrap();

    let count_after: i64 = count(&conn, "SELECT COUNT(*) FROM task_comments");
    assert_eq!(
        count_before, count_after,
        "idempotency violated: re-run changed row count ({count_before} → {count_after})"
    );
}
