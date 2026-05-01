use brain_persistence::db::schema::init_schema;
use rusqlite::Connection;

/// Build a v48 fixture: initialise the current schema (v49) and downgrade
/// the user_version to 48, dropping the v49-shape `entity_links` table and
/// indexes so a subsequent `init_schema` exercises the real v48→v49 migration.
/// Mirrors the snapshot pattern from `migration_v44_fixture_test.rs`.
fn snapshot_at_v48() -> Connection {
    let conn = Connection::open_in_memory().expect("open in-memory sqlite");
    conn.pragma_update(None, "journal_mode", "WAL").unwrap();
    conn.pragma_update(None, "foreign_keys", "ON").unwrap();

    // Bring the DB up to current SCHEMA_VERSION (v49), then remove the v49
    // `entity_links` table and stamp user_version back to 48.
    init_schema(&conn).expect("initialize current schema");

    conn.execute_batch(
        "DROP INDEX IF EXISTS idx_entity_links_parent_of_partial;
         DROP INDEX IF EXISTS idx_entity_links_blocks_partial;
         DROP INDEX IF EXISTS idx_entity_links_incoming;
         DROP INDEX IF EXISTS idx_entity_links_outgoing;
         DROP INDEX IF EXISTS idx_entity_links_unique;
         DROP TABLE IF EXISTS entity_links;",
    )
    .expect("remove v49-shape entity_links table and indexes");

    conn.pragma_update(None, "user_version", 48)
        .expect("downgrade user_version to v48 fixture");

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
fn test_migration_v49_creates_entity_links_table_and_indexes() {
    let conn = snapshot_at_v48();

    // Pre-condition: entity_links table absent at v48 fixture.
    assert!(
        !table_exists(&conn, "entity_links"),
        "entity_links table must be absent in v48 fixture"
    );

    init_schema(&conn).unwrap();

    let version: i32 = conn
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    // init_schema runs all migrations forward. v49 is the current tip.
    assert!(
        version >= 49,
        "init_schema should land at >= v49 (got {version})"
    );

    assert!(
        table_exists(&conn, "entity_links"),
        "entity_links table must exist after migration"
    );

    for idx in &[
        "idx_entity_links_unique",
        "idx_entity_links_outgoing",
        "idx_entity_links_incoming",
        "idx_entity_links_blocks_partial",
        "idx_entity_links_parent_of_partial",
    ] {
        assert!(
            index_exists(&conn, idx),
            "index {idx} must exist after migration"
        );
    }
}

#[test]
fn test_migration_v49_entity_links_columns_match_spec() {
    let conn = snapshot_at_v48();
    init_schema(&conn).unwrap();

    let cols = column_info(&conn, "entity_links");
    // TEXT PRIMARY KEY has notnull=0 in PRAGMA table_info (SQLite quirk).
    assert_column(&cols, "id", "TEXT", false);
    assert_column(&cols, "from_type", "TEXT", true);
    assert_column(&cols, "from_id", "TEXT", true);
    assert_column(&cols, "to_type", "TEXT", true);
    assert_column(&cols, "to_id", "TEXT", true);
    assert_column(&cols, "edge_kind", "TEXT", true);
    assert_column(&cols, "created_at", "TEXT", true);
    assert_column(&cols, "brain_scope", "TEXT", false);
    assert_eq!(cols.len(), 8, "unexpected extra columns: {cols:?}");
}

#[test]
fn test_migration_v49_partial_indexes_exist_via_pragma() {
    let conn = snapshot_at_v48();
    init_schema(&conn).unwrap();

    // Verify partial indexes via PRAGMA index_list(entity_links) as required by spec.
    let mut stmt = conn.prepare("PRAGMA index_list(entity_links)").unwrap();
    let index_names: Vec<String> = stmt
        .query_map([], |row| {
            let name: String = row.get(1)?;
            Ok(name)
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    assert!(
        index_names.contains(&"idx_entity_links_blocks_partial".to_string()),
        "idx_entity_links_blocks_partial must appear in PRAGMA index_list(entity_links); got {index_names:?}"
    );
    assert!(
        index_names.contains(&"idx_entity_links_parent_of_partial".to_string()),
        "idx_entity_links_parent_of_partial must appear in PRAGMA index_list(entity_links); got {index_names:?}"
    );
}

#[test]
fn test_migration_v49_self_loop_check_constraint() {
    let conn = snapshot_at_v48();
    init_schema(&conn).unwrap();

    // Self-referential edge (same from_type+from_id = to_type+to_id) must be rejected by CHECK.
    let result = conn.execute(
        "INSERT INTO entity_links (id, from_type, from_id, to_type, to_id, edge_kind, created_at)
         VALUES ('01JSELF', 'TASK', 'task-1', 'TASK', 'task-1', 'blocks', '2026-05-01T00:00:00Z')",
        [],
    );
    assert!(
        result.is_err(),
        "self-loop CHECK (NOT (from_type = to_type AND from_id = to_id)) must fire"
    );
}

#[test]
fn test_migration_v49_unique_index_prevents_duplicate_edges() {
    let conn = snapshot_at_v48();
    init_schema(&conn).unwrap();

    conn.execute(
        "INSERT INTO entity_links (id, from_type, from_id, to_type, to_id, edge_kind, created_at)
         VALUES ('01JEDGE1', 'TASK', 'task-a', 'TASK', 'task-b', 'blocks', '2026-05-01T00:00:00Z')",
        [],
    )
    .unwrap();

    // Duplicate edge (different id, same semantic tuple) must be rejected.
    let result = conn.execute(
        "INSERT INTO entity_links (id, from_type, from_id, to_type, to_id, edge_kind, created_at)
         VALUES ('01JEDGE2', 'TASK', 'task-a', 'TASK', 'task-b', 'blocks', '2026-05-01T00:00:00Z')",
        [],
    );
    assert!(
        result.is_err(),
        "idx_entity_links_unique must reject duplicate (from_type, from_id, to_type, to_id, edge_kind)"
    );
}

#[test]
fn test_migration_v49_cross_type_edge_allowed() {
    let conn = snapshot_at_v48();
    init_schema(&conn).unwrap();

    // TASK→RECORD with same IDs but different types: CHECK allows it.
    let result = conn.execute(
        "INSERT INTO entity_links (id, from_type, from_id, to_type, to_id, edge_kind, created_at)
         VALUES ('01JCROSS', 'TASK', 'shared-id', 'RECORD', 'shared-id', 'covers', '2026-05-01T00:00:00Z')",
        [],
    );
    assert!(
        result.is_ok(),
        "cross-type edge with matching IDs must be allowed: {result:?}"
    );
}
