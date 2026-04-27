use brain_persistence::db::schema::init_schema;
use rusqlite::{Connection, params};

/// Build a v42 fixture by initialising the current schema, downgrading
/// `user_version` to 42, and dropping the v43 tables. The drop step is
/// essential — without it `init_schema` finds the v43-shaped tables created
/// by the first call and the rerun's `CREATE TABLE IF NOT EXISTS` is a no-op,
/// so the test would not actually exercise the migration.
fn snapshot_at_version(version: i32) -> Connection {
    assert_eq!(version, 42, "this fixture only supports v42 setup");

    let conn = Connection::open_in_memory().expect("open in-memory sqlite");
    init_schema(&conn).expect("initialize current schema");
    conn.pragma_update(None, "user_version", 42)
        .expect("downgrade user_version to v42 fixture");

    // Drop tag_aliases first — it holds the FK to tag_cluster_runs and
    // foreign_keys = ON would reject dropping the parent first.
    conn.execute_batch(
        "DROP TABLE IF EXISTS tag_aliases;
         DROP TABLE IF EXISTS tag_cluster_runs;",
    )
    .expect("drop v43 tables for fixture");

    conn
}

fn insert_record(conn: &Connection, record_id: &str) {
    conn.execute(
        "INSERT INTO records (
            record_id, title, kind, status, description, content_hash,
            content_size, media_type, task_id, actor, created_at, updated_at,
            retention_class, pinned, payload_available, content_encoding,
            original_size, brain_id, searchable, embedded_at
        ) VALUES (
            ?1, ?2, 'document', 'active', NULL, 'hash',
            4, 'text/plain', NULL, 'test-agent', 1000, 1000,
            NULL, 0, 1, 'identity',
            NULL, '', 1, NULL
        )",
        params![record_id, format!("record {record_id}")],
    )
    .unwrap();
}

fn insert_task(conn: &Connection, task_id: &str) {
    conn.execute(
        "INSERT INTO tasks (task_id, title, status, priority, task_type, created_at, updated_at)
         VALUES (?1, ?2, 'open', 1, 'task', 1000, 1000)",
        params![task_id, format!("task {task_id}")],
    )
    .unwrap();
}

fn count(conn: &Connection, table: &str) -> i64 {
    conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
        row.get(0)
    })
    .unwrap()
}

fn table_exists(conn: &Connection, name: &str) -> bool {
    let n: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1",
            [name],
            |row| row.get(0),
        )
        .unwrap();
    n == 1
}

fn index_exists(conn: &Connection, name: &str) -> bool {
    let n: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name=?1",
            [name],
            |row| row.get(0),
        )
        .unwrap();
    n == 1
}

/// Return `(name, declared_type_uppercase, notnull)` for each column in `table`.
fn column_info(conn: &Connection, table: &str) -> Vec<(String, String, bool)> {
    let mut stmt = conn
        .prepare(&format!("PRAGMA table_info({table})"))
        .unwrap();
    stmt.query_map([], |row| {
        let name: String = row.get(1)?;
        let ty: String = row.get(2)?;
        let notnull: i32 = row.get(3)?;
        Ok((name, ty.to_uppercase(), notnull != 0))
    })
    .unwrap()
    .map(|r| r.unwrap())
    .collect()
}

fn assert_column(
    cols: &[(String, String, bool)],
    name: &str,
    expected_type: &str,
    expected_notnull: bool,
) {
    let actual = cols
        .iter()
        .find(|(n, _, _)| n == name)
        .unwrap_or_else(|| panic!("column {name} missing; got {cols:?}"));
    assert_eq!(
        actual.1, expected_type,
        "column {name}: type mismatch (got {:?}, expected {expected_type})",
        actual.1
    );
    assert_eq!(
        actual.2, expected_notnull,
        "column {name}: NOT NULL mismatch (got {}, expected {expected_notnull})",
        actual.2
    );
}

#[test]
fn test_migration_v43_creates_new_tables_and_indexes() {
    let conn = snapshot_at_version(42);

    assert!(!table_exists(&conn, "tag_aliases"));
    assert!(!table_exists(&conn, "tag_cluster_runs"));

    init_schema(&conn).unwrap();

    let version: i32 = conn
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    assert_eq!(version, 43);

    assert!(table_exists(&conn, "tag_aliases"));
    assert!(table_exists(&conn, "tag_cluster_runs"));
    assert!(index_exists(&conn, "idx_tag_aliases_canonical"));
    assert!(index_exists(&conn, "idx_tag_aliases_cluster"));

    assert_eq!(count(&conn, "tag_aliases"), 0);
    assert_eq!(count(&conn, "tag_cluster_runs"), 0);
}

#[test]
fn test_migration_v43_preserves_existing_tag_and_label_rows() {
    let conn = snapshot_at_version(42);

    insert_record(&conn, "r1");
    insert_task(&conn, "t1");
    conn.execute(
        "INSERT INTO record_tags (record_id, tag) VALUES ('r1', 'auth')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO task_labels (task_id, label) VALUES ('t1', 'auth')",
        [],
    )
    .unwrap();

    let tags_before = count(&conn, "record_tags");
    let labels_before = count(&conn, "task_labels");
    let records_before = count(&conn, "records");
    let tasks_before = count(&conn, "tasks");

    init_schema(&conn).unwrap();

    assert_eq!(count(&conn, "record_tags"), tags_before);
    assert_eq!(count(&conn, "task_labels"), labels_before);
    assert_eq!(count(&conn, "records"), records_before);
    assert_eq!(count(&conn, "tasks"), tasks_before);
}

#[test]
fn test_migration_v43_tag_aliases_columns_match_spec() {
    let conn = snapshot_at_version(42);
    init_schema(&conn).unwrap();

    let cols = column_info(&conn, "tag_aliases");
    // raw_tag / run_id are TEXT PRIMARY KEY: per SQLite quirk, non-INTEGER
    // PKs are nullable (notnull=0) unless declared NOT NULL explicitly.
    // We don't add NOT NULL to PK columns since the PK uniqueness already
    // forbids meaningful NULL use.
    assert_column(&cols, "raw_tag", "TEXT", false);
    assert_column(&cols, "canonical_tag", "TEXT", true);
    assert_column(&cols, "cluster_id", "TEXT", true);
    assert_column(&cols, "last_run_id", "TEXT", true);
    assert_column(&cols, "embedding", "BLOB", false);
    assert_column(&cols, "embedder_version", "TEXT", false);
    assert_column(&cols, "updated_at", "TEXT", true);
    assert_eq!(cols.len(), 7, "unexpected extra columns: {cols:?}");
}

#[test]
fn test_migration_v43_tag_cluster_runs_columns_match_spec() {
    let conn = snapshot_at_version(42);
    init_schema(&conn).unwrap();

    let cols = column_info(&conn, "tag_cluster_runs");
    assert_column(&cols, "run_id", "TEXT", false);
    assert_column(&cols, "started_at", "TEXT", true);
    assert_column(&cols, "finished_at", "TEXT", false);
    assert_column(&cols, "source_count", "INTEGER", false);
    assert_column(&cols, "cluster_count", "INTEGER", false);
    assert_column(&cols, "embedder_version", "TEXT", true);
    assert_column(&cols, "threshold", "REAL", true);
    assert_column(&cols, "triggered_by", "TEXT", true);
    assert_column(&cols, "notes", "TEXT", false);
    assert_eq!(cols.len(), 9, "unexpected extra columns: {cols:?}");
}
