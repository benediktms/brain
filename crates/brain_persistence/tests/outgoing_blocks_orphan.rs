//! Verifies that `outgoing_blocks` tolerates orphan entity_links rows
//! (foreign-keys-off insert pointing at a non-existent task). The cycle
//! detection in `brain_tasks::cycle` assumes neighbor lookups never panic
//! on such rows, so this test pins the contract at the persistence
//! boundary rather than in the domain crate.

use brain_persistence::db::schema::init_schema;
use brain_persistence::db::tasks::queries::outgoing_blocks;
use rusqlite::Connection;

#[test]
fn outgoing_blocks_tolerates_orphan_to_id() {
    let conn = Connection::open_in_memory().expect("open in-memory sqlite");
    init_schema(&conn).expect("init schema");

    // Insert one real task — this is the source of the orphan edge.
    conn.execute(
        "INSERT INTO tasks (task_id, brain_id, title, status, priority, task_type, \
                            created_at, updated_at, display_id) \
         VALUES ('t2', '', 't2', 'open', 2, 'task', \
                 strftime('%s','now'), strftime('%s','now'), 't2')",
        [],
    )
    .expect("insert real task");

    // Insert an orphan entity_links 'blocks' edge: t2 → ghost-9999, where
    // ghost-9999 has no corresponding row in `tasks`. FK enforcement is
    // disabled around the insert because the schema declares a FK on
    // entity_links.to_id when both endpoints are TASKs in real use.
    conn.execute_batch("PRAGMA foreign_keys = OFF").unwrap();
    conn.execute(
        "INSERT OR IGNORE INTO entity_links \
         (id, from_type, from_id, to_type, to_id, edge_kind, created_at, brain_scope) \
         VALUES \
         (lower(hex(randomblob(16))), 'TASK', 't2', 'TASK', 'ghost-9999', 'blocks', \
          strftime('%Y-%m-%dT%H:%M:%SZ', 'now'), NULL)",
        [],
    )
    .expect("insert orphan blocks edge");
    conn.execute_batch("PRAGMA foreign_keys = ON").unwrap();

    // outgoing_blocks must return the ghost id verbatim — no panic, no FK error.
    let neighbors = outgoing_blocks(&conn, "t2").expect("outgoing_blocks succeeds");
    assert_eq!(neighbors, vec!["ghost-9999".to_string()]);
}
