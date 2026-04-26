use brain_persistence::db::schema::init_schema;
use rusqlite::{Connection, params};

fn snapshot_at_version(version: i32) -> Connection {
    assert_eq!(version, 41, "this fixture only supports v41 setup");

    let conn = Connection::open_in_memory().expect("open in-memory sqlite");
    init_schema(&conn).expect("initialize current schema");
    conn.pragma_update(None, "user_version", 41)
        .expect("downgrade user_version to v41 fixture");

    conn
}

fn insert_record(conn: &Connection, record_id: &str, kind: &str, searchable: i32) {
    conn.execute(
        "INSERT INTO records (
            record_id, title, kind, status, description, content_hash,
            content_size, media_type, task_id, actor, created_at, updated_at,
            retention_class, pinned, payload_available, content_encoding,
            original_size, brain_id, searchable, embedded_at
        ) VALUES (
            ?1, ?2, ?3, 'active', NULL, 'hash',
            4, 'text/plain', NULL, 'test-agent', 1000, 1000,
            NULL, 0, 1, 'identity',
            NULL, '', ?4, NULL
        )",
        params![record_id, format!("record {record_id}"), kind, searchable],
    )
    .unwrap();
}

fn record_state(conn: &Connection, record_id: &str) -> (String, i32) {
    conn.query_row(
        "SELECT kind, searchable FROM records WHERE record_id = ?1",
        [record_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .unwrap()
}

fn all_record_states(conn: &Connection) -> Vec<(String, String, i32)> {
    let mut stmt = conn
        .prepare("SELECT record_id, kind, searchable FROM records ORDER BY record_id")
        .unwrap();
    stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .map(|row| row.unwrap())
        .collect()
}

fn tag_count(conn: &Connection) -> i64 {
    conn.query_row("SELECT COUNT(*) FROM record_tags", [], |row| row.get(0))
        .unwrap()
}

fn link_count(conn: &Connection) -> i64 {
    conn.query_row("SELECT COUNT(*) FROM record_links", [], |row| row.get(0))
        .unwrap()
}

#[test]
fn test_migration_v42_on_fixture_db() {
    let conn = snapshot_at_version(41);

    insert_record(&conn, "r-snapshot", "snapshot", 0);
    insert_record(&conn, "r-implementation", "implementation", 1);
    insert_record(&conn, "r-review", "review", 1);
    insert_record(&conn, "r-plan", "plan", 1);
    insert_record(&conn, "r-analysis", "analysis", 1);
    insert_record(&conn, "r-document", "document", 1);
    insert_record(&conn, "r-dispatch-brief", "dispatch-brief", 0);
    insert_record(&conn, "r-conversation", "conversation", 1);
    insert_record(&conn, "r-summary", "summary", 1);
    insert_record(&conn, "r-report", "report", 0);

    init_schema(&conn).unwrap();

    assert_eq!(record_state(&conn, "r-snapshot"), ("snapshot".into(), 0));
    assert_eq!(
        record_state(&conn, "r-implementation"),
        ("implementation".into(), 1)
    );
    assert_eq!(record_state(&conn, "r-review"), ("review".into(), 1));
    assert_eq!(record_state(&conn, "r-plan"), ("plan".into(), 1));
    assert_eq!(record_state(&conn, "r-analysis"), ("analysis".into(), 1));
    assert_eq!(record_state(&conn, "r-document"), ("document".into(), 1));
    assert_eq!(
        record_state(&conn, "r-dispatch-brief"),
        ("document".into(), 1)
    );
    assert_eq!(
        record_state(&conn, "r-conversation"),
        ("snapshot".into(), 0)
    );
    assert_eq!(record_state(&conn, "r-summary"), ("summary".into(), 1));
    assert_eq!(record_state(&conn, "r-report"), ("document".into(), 1));
}

#[test]
fn test_migration_idempotent_re_run() {
    let conn = snapshot_at_version(41);

    insert_record(&conn, "r-dispatch-brief", "dispatch-brief", 0);
    insert_record(&conn, "r-conversation", "conversation", 1);
    insert_record(&conn, "r-report", "report", 0);
    insert_record(&conn, "r-plan", "plan", 1);

    init_schema(&conn).unwrap();
    let first_pass = all_record_states(&conn);

    init_schema(&conn).unwrap();
    let second_pass = all_record_states(&conn);

    assert_eq!(first_pass, second_pass);
}

#[test]
fn test_migration_preserves_record_relationships() {
    let conn = snapshot_at_version(41);

    insert_record(&conn, "r-dispatch-brief", "dispatch-brief", 0);
    insert_record(&conn, "r-conversation", "conversation", 1);

    conn.execute(
        "INSERT INTO record_tags (record_id, tag) VALUES (?1, ?2), (?1, ?3), (?4, ?5)",
        params![
            "r-dispatch-brief",
            "ops",
            "taxonomy",
            "r-conversation",
            "chat"
        ],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO record_links (record_id, task_id, chunk_id, created_at)
         VALUES (?1, ?2, NULL, 1000), (?3, NULL, ?4, 1001)",
        params!["r-dispatch-brief", "TASK-1", "r-conversation", "chunk-1"],
    )
    .unwrap();

    let tags_before = tag_count(&conn);
    let links_before = link_count(&conn);

    init_schema(&conn).unwrap();

    assert_eq!(tag_count(&conn), tags_before);
    assert_eq!(link_count(&conn), links_before);
}

#[test]
fn test_migration_searchable_redrived_for_dispatch_brief() {
    let conn = snapshot_at_version(41);

    insert_record(&conn, "r-dispatch-brief", "dispatch-brief", 0);

    init_schema(&conn).unwrap();

    assert_eq!(
        record_state(&conn, "r-dispatch-brief"),
        ("document".into(), 1)
    );
}
