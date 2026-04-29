use brain_persistence::db::schema::init_schema;
use rusqlite::{Connection, params};

/// Build a v43 fixture: initialise the current schema (v44) and downgrade
/// the user_version to 43, dropping the v44-shape `tag_*` tables and
/// recreating them in v43 shape so a subsequent `init_schema` exercises the
/// real v43→v44 migration. Mirrors the snapshot pattern from the legacy
/// `migration_v43_fixture_test.rs` (now superseded by this file).
fn snapshot_at_v43() -> Connection {
    let conn = Connection::open_in_memory().expect("open in-memory sqlite");
    conn.pragma_update(None, "journal_mode", "WAL").unwrap();
    conn.pragma_update(None, "foreign_keys", "ON").unwrap();

    // Bring the DB up to current SCHEMA_VERSION (v44), then rewind the v44
    // tag_* tables to v43 shape and stamp user_version back to 43.
    init_schema(&conn).expect("initialize current schema");

    conn.execute_batch(
        "DROP INDEX IF EXISTS idx_tag_aliases_canonical;
         DROP INDEX IF EXISTS idx_tag_aliases_cluster;
         DROP TABLE IF EXISTS tag_aliases;
         DROP TABLE IF EXISTS tag_cluster_runs;

         -- v43 shape: no brain_id, raw_tag PK on tag_aliases.
         CREATE TABLE tag_cluster_runs (
             run_id           TEXT PRIMARY KEY,
             started_at       TEXT NOT NULL,
             finished_at      TEXT,
             source_count     INTEGER,
             cluster_count    INTEGER,
             embedder_version TEXT NOT NULL,
             threshold        REAL NOT NULL,
             triggered_by     TEXT NOT NULL,
             notes            TEXT
         );
         CREATE TABLE tag_aliases (
             raw_tag          TEXT PRIMARY KEY,
             canonical_tag    TEXT NOT NULL,
             cluster_id       TEXT NOT NULL,
             last_run_id      TEXT NOT NULL REFERENCES tag_cluster_runs(run_id),
             embedding        BLOB,
             embedder_version TEXT,
             updated_at       TEXT NOT NULL
         );
         CREATE INDEX idx_tag_aliases_canonical ON tag_aliases(canonical_tag);
         CREATE INDEX idx_tag_aliases_cluster   ON tag_aliases(cluster_id);",
    )
    .expect("rebuild v43-shape tag_* tables");

    conn.pragma_update(None, "user_version", 43)
        .expect("downgrade user_version to v43 fixture");

    conn
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
fn test_migration_v44_recreates_tables_and_indexes() {
    let conn = snapshot_at_v43();

    // Pre-condition: v43 shape is in place.
    assert!(table_exists(&conn, "tag_aliases"));
    assert!(table_exists(&conn, "tag_cluster_runs"));
    let v43_alias_cols = column_info(&conn, "tag_aliases");
    assert!(
        v43_alias_cols.iter().all(|(n, _, _)| n != "brain_id"),
        "v43 should not have brain_id"
    );

    init_schema(&conn).unwrap();

    let version: i32 = conn
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    // init_schema runs all migrations forward; v44 is no longer the tip
    // (v44→v45 added external-blocker columns). Assert ≥44 so this test
    // continues to verify the v43→v44 recreate landed without coupling
    // to whatever later migrations exist.
    assert!(
        version >= 44,
        "init_schema should land at >= v44 (got {version})"
    );

    assert!(table_exists(&conn, "tag_aliases"));
    assert!(table_exists(&conn, "tag_cluster_runs"));
    assert!(index_exists(&conn, "idx_tag_aliases_canonical"));
    assert!(index_exists(&conn, "idx_tag_aliases_cluster"));
}

#[test]
fn test_migration_v44_tag_aliases_columns_match_spec() {
    let conn = snapshot_at_v43();
    init_schema(&conn).unwrap();

    let cols = column_info(&conn, "tag_aliases");
    // Composite PK columns are explicitly declared NOT NULL, so PRAGMA
    // reports notnull=1 — distinct from the v43 quirk where a TEXT PK
    // column reported notnull=0.
    assert_column(&cols, "brain_id", "TEXT", true);
    assert_column(&cols, "raw_tag", "TEXT", true);
    assert_column(&cols, "canonical_tag", "TEXT", true);
    assert_column(&cols, "cluster_id", "TEXT", true);
    assert_column(&cols, "last_run_id", "TEXT", true);
    assert_column(&cols, "embedding", "BLOB", false);
    assert_column(&cols, "embedder_version", "TEXT", false);
    assert_column(&cols, "updated_at", "TEXT", true);
    assert_eq!(cols.len(), 8, "unexpected extra columns: {cols:?}");
}

#[test]
fn test_migration_v44_tag_cluster_runs_columns_match_spec() {
    let conn = snapshot_at_v43();
    init_schema(&conn).unwrap();

    let cols = column_info(&conn, "tag_cluster_runs");
    assert_column(&cols, "run_id", "TEXT", false); // TEXT PK quirk: notnull=0
    assert_column(&cols, "brain_id", "TEXT", true);
    assert_column(&cols, "started_at", "TEXT", true);
    assert_column(&cols, "finished_at", "TEXT", false);
    assert_column(&cols, "source_count", "INTEGER", false);
    assert_column(&cols, "cluster_count", "INTEGER", false);
    assert_column(&cols, "embedder_version", "TEXT", true);
    assert_column(&cols, "threshold", "REAL", true);
    assert_column(&cols, "triggered_by", "TEXT", true);
    assert_column(&cols, "notes", "TEXT", false);
    assert_eq!(cols.len(), 10, "unexpected extra columns: {cols:?}");
}

#[test]
fn test_migration_v44_fk_enforced() {
    let conn = snapshot_at_v43();
    init_schema(&conn).unwrap();

    // tag_aliases.last_run_id references tag_cluster_runs.run_id.
    // A bogus reference must error.
    let result = conn.execute(
        "INSERT INTO tag_aliases
             (brain_id, raw_tag, canonical_tag, cluster_id, last_run_id, updated_at)
         VALUES ('brain-a', 'auth', 'auth', 'c1', 'no-such-run', '2026-04-28T00:00:00Z')",
        [],
    );
    assert!(result.is_err(), "FK to tag_cluster_runs should be enforced");
}

#[test]
fn test_migration_v44_composite_pk_allows_same_tag_across_brains() {
    let conn = snapshot_at_v43();
    init_schema(&conn).unwrap();

    // Seed a run for the FK to resolve.
    conn.execute(
        "INSERT INTO tag_cluster_runs
             (run_id, brain_id, started_at, embedder_version, threshold, triggered_by)
         VALUES ('r1', 'brain-a', '2026-04-28T00:00:00Z', 'bge', 0.85, 'manual')",
        [],
    )
    .unwrap();

    let insert = |brain_id: &str, raw_tag: &str| {
        conn.execute(
            "INSERT INTO tag_aliases
                 (brain_id, raw_tag, canonical_tag, cluster_id, last_run_id, updated_at)
             VALUES (?1, ?2, ?2, 'c1', 'r1', '2026-04-28T00:00:00Z')",
            params![brain_id, raw_tag],
        )
    };

    // Two brains using the same raw tag must succeed under composite PK.
    insert("brain-a", "shared").unwrap();
    insert("brain-b", "shared").unwrap();
    // Re-inserting (brain-a, "shared") fails — composite PK collision.
    assert!(insert("brain-a", "shared").is_err());
}
