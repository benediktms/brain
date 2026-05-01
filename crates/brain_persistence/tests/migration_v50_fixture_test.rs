use brain_persistence::db::schema::init_schema;
use rusqlite::Connection;

/// Build a v49 fixture: initialise the current schema (v50) and downgrade the
/// user_version to 49, restoring the two partial indexes that v50 drops.
/// A subsequent `init_schema` exercises the real v49→v50 migration.
///
/// We recreate the partial indexes explicitly because init_schema (v50) already
/// removed them — we need them present to verify that v49→v50 removes them.
fn snapshot_at_v49() -> Connection {
    let conn = Connection::open_in_memory().expect("open in-memory sqlite");
    conn.pragma_update(None, "journal_mode", "WAL").unwrap();
    conn.pragma_update(None, "foreign_keys", "ON").unwrap();

    // Bring the DB up to current SCHEMA_VERSION (v50).
    init_schema(&conn).expect("initialize current schema");

    // Re-create the two partial indexes that v50 dropped, so the fixture
    // accurately represents a v49 database before v49→v50 migration runs.
    conn.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_entity_links_blocks_partial
             ON entity_links(from_id, to_id)
             WHERE from_type = 'TASK' AND to_type = 'TASK' AND edge_kind = 'blocks';

         CREATE INDEX IF NOT EXISTS idx_entity_links_parent_of_partial
             ON entity_links(from_id, to_id)
             WHERE from_type = 'TASK' AND to_type = 'TASK' AND edge_kind = 'parent_of';",
    )
    .expect("recreate v49 partial indexes");

    // Stamp user_version back to 49 so init_schema will run v49→v50.
    conn.pragma_update(None, "user_version", 49)
        .expect("downgrade user_version to v49 fixture");

    conn
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

/// After v49→v50, both dead partial indexes must be gone.
#[test]
fn test_migration_v50_drops_partial_indexes() {
    let conn = snapshot_at_v49();

    // Pre-condition: both partial indexes exist in the v49 fixture.
    assert!(
        index_exists(&conn, "idx_entity_links_blocks_partial"),
        "idx_entity_links_blocks_partial must exist in v49 fixture before migration"
    );
    assert!(
        index_exists(&conn, "idx_entity_links_parent_of_partial"),
        "idx_entity_links_parent_of_partial must exist in v49 fixture before migration"
    );

    init_schema(&conn).unwrap();

    // Post-condition: both dropped.
    assert!(
        !index_exists(&conn, "idx_entity_links_blocks_partial"),
        "idx_entity_links_blocks_partial must be absent after v49→v50 migration"
    );
    assert!(
        !index_exists(&conn, "idx_entity_links_parent_of_partial"),
        "idx_entity_links_parent_of_partial must be absent after v49→v50 migration"
    );
}

/// After v49→v50, the three surviving entity_links indexes must still exist.
#[test]
fn test_migration_v50_surviving_indexes_intact() {
    let conn = snapshot_at_v49();
    init_schema(&conn).unwrap();

    for idx in &[
        "idx_entity_links_unique",
        "idx_entity_links_outgoing",
        "idx_entity_links_incoming",
    ] {
        assert!(
            index_exists(&conn, idx),
            "index {idx} must still exist after v49→v50 migration"
        );
    }
}

/// After v49→v50, the `entity_links` table itself must be unchanged.
#[test]
fn test_migration_v50_entity_links_table_unchanged() {
    let conn = snapshot_at_v49();
    init_schema(&conn).unwrap();

    assert!(
        table_exists(&conn, "entity_links"),
        "entity_links table must still exist after v49→v50 migration"
    );

    // Verify columns are intact by inserting a valid non-self-loop row.
    let result = conn.execute(
        "INSERT INTO entity_links (id, from_type, from_id, to_type, to_id, edge_kind, created_at)
         VALUES ('01JV50A', 'TASK', 'task-x', 'TASK', 'task-y', 'blocks', '2026-05-01T00:00:00Z')",
        [],
    );
    assert!(
        result.is_ok(),
        "valid insert into entity_links must succeed after v49→v50: {result:?}"
    );
}

/// After v49→v50, PRAGMA user_version must return 50.
#[test]
fn test_migration_v50_user_version_stamped() {
    let conn = snapshot_at_v49();
    init_schema(&conn).unwrap();

    let version: i32 = conn
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    assert_eq!(
        version, 50,
        "user_version must be 50 after v49→v50 migration"
    );
}
