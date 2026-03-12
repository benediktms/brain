//! Integration tests for the records domain.
//!
//! Covers:
//! 1. Rebuild correctness — create records with tags and links, rebuild, verify SQLite state.
//! 2. Rebuild idempotency — rebuild twice produces the same result.
//! 3. Crash recovery — events logged but not projected; rebuild recovers correct state.
//! 4. Partial write recovery — truncated last line in JSONL; rebuild skips malformed line.
//! 5. Projection consistency — incremental apply matches bulk rebuild.
//! 6. Status transitions — active → archived.
//! 7. Tag lifecycle — add + remove leaves no tag.
//! 8. Link lifecycle — add + remove leaves no link.
//! 9. Object store integration — store blob, create record referencing it, read back.

use std::fs::OpenOptions;
use std::io::Write as IoWrite;

use brain_lib::db::Db;
use brain_lib::records::events::{
    ContentRefPayload, LinkPayload, RecordArchivedPayload, RecordCreatedPayload, RecordEvent,
    RecordEventType, RecordUpdatedPayload, TagPayload, append_event, new_record_id,
    read_all_events,
};
use brain_lib::records::objects::ObjectStore;
use brain_lib::records::projections::{apply_event, rebuild};
use brain_lib::records::{RecordStatus, RecordStore};
use tempfile::TempDir;

// ─── Helpers ─────────────────────────────────────────────────────

fn make_created_event(record_id: &str, title: &str, kind: &str) -> RecordEvent {
    RecordEvent::from_payload(
        record_id,
        "test-agent",
        RecordCreatedPayload {
            title: title.to_string(),
            kind: kind.to_string(),
            content_ref: ContentRefPayload::new(
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".to_string(),
                42,
                Some("text/plain".to_string()),
            ),
            description: None,
            task_id: None,
            tags: vec![],
            scope_type: None,
            scope_id: None,
            retention_class: None,
            producer: None,
        },
    )
}

fn make_created_event_with_tags(
    record_id: &str,
    title: &str,
    kind: &str,
    tags: Vec<String>,
) -> RecordEvent {
    RecordEvent::from_payload(
        record_id,
        "test-agent",
        RecordCreatedPayload {
            title: title.to_string(),
            kind: kind.to_string(),
            content_ref: ContentRefPayload::new(
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".to_string(),
                42,
                None,
            ),
            description: Some("A test record".to_string()),
            task_id: None,
            tags,
            scope_type: None,
            scope_id: None,
            retention_class: None,
            producer: None,
        },
    )
}

// ─── DB query helpers using Db::with_read_conn ───────────────────

fn count_records(db: &Db) -> i64 {
    db.with_read_conn(|conn| {
        conn.query_row("SELECT COUNT(*) FROM records", [], |row| row.get(0))
            .map_err(Into::into)
    })
    .unwrap()
}

fn count_tags_for(db: &Db, record_id: &str) -> i64 {
    db.with_read_conn(|conn| {
        conn.query_row(
            "SELECT COUNT(*) FROM record_tags WHERE record_id = ?1",
            rusqlite::params![record_id],
            |row| row.get(0),
        )
        .map_err(Into::into)
    })
    .unwrap()
}

fn count_links_for(db: &Db, record_id: &str) -> i64 {
    db.with_read_conn(|conn| {
        conn.query_row(
            "SELECT COUNT(*) FROM record_links WHERE record_id = ?1",
            rusqlite::params![record_id],
            |row| row.get(0),
        )
        .map_err(Into::into)
    })
    .unwrap()
}

fn get_status(db: &Db, record_id: &str) -> String {
    db.with_read_conn(|conn| {
        conn.query_row(
            "SELECT status FROM records WHERE record_id = ?1",
            rusqlite::params![record_id],
            |row| row.get::<_, String>(0),
        )
        .map_err(Into::into)
    })
    .unwrap()
}

fn get_title(db: &Db, record_id: &str) -> String {
    db.with_read_conn(|conn| {
        conn.query_row(
            "SELECT title FROM records WHERE record_id = ?1",
            rusqlite::params![record_id],
            |row| row.get::<_, String>(0),
        )
        .map_err(Into::into)
    })
    .unwrap()
}

fn tag_exists(db: &Db, record_id: &str, tag: &str) -> bool {
    let count: i64 = db
        .with_read_conn(|conn| {
            conn.query_row(
                "SELECT COUNT(*) FROM record_tags WHERE record_id = ?1 AND tag = ?2",
                rusqlite::params![record_id, tag],
                |row| row.get(0),
            )
            .map_err(Into::into)
        })
        .unwrap();
    count > 0
}

fn link_task_exists(db: &Db, record_id: &str, task_id: &str) -> bool {
    let count: i64 = db
        .with_read_conn(|conn| {
            conn.query_row(
                "SELECT COUNT(*) FROM record_links WHERE record_id = ?1 AND task_id = ?2",
                rusqlite::params![record_id, task_id],
                |row| row.get(0),
            )
            .map_err(Into::into)
        })
        .unwrap();
    count > 0
}

fn get_content_hash(db: &Db, record_id: &str) -> String {
    db.with_read_conn(|conn| {
        conn.query_row(
            "SELECT content_hash FROM records WHERE record_id = ?1",
            rusqlite::params![record_id],
            |row| row.get::<_, String>(0),
        )
        .map_err(Into::into)
    })
    .unwrap()
}

// ─── 1. Rebuild correctness ───────────────────────────────────────

#[test]
fn test_rebuild_creates_all_records_with_tags_and_links() {
    let dir = TempDir::new().unwrap();
    let events_path = dir.path().join("events.jsonl");
    let db = Db::open_in_memory().unwrap();

    // Create an artifact with inline tags
    let artifact_id = new_record_id("BRN");
    append_event(
        &events_path,
        &make_created_event_with_tags(
            &artifact_id,
            "My Artifact",
            "report",
            vec!["alpha".to_string(), "beta".to_string()],
        ),
    )
    .unwrap();

    // Create a snapshot
    let snapshot_id = new_record_id("BRN");
    append_event(
        &events_path,
        &make_created_event(&snapshot_id, "My Snapshot", "snapshot"),
    )
    .unwrap();

    // Add a tag to the snapshot via separate event
    append_event(
        &events_path,
        &RecordEvent::new(
            &snapshot_id,
            "agent",
            RecordEventType::TagAdded,
            &TagPayload {
                tag: "critical".to_string(),
            },
        ),
    )
    .unwrap();

    // Add a link to the artifact
    append_event(
        &events_path,
        &RecordEvent::new(
            &artifact_id,
            "agent",
            RecordEventType::LinkAdded,
            &LinkPayload {
                task_id: Some("BRN-TASK-01".to_string()),
                chunk_id: None,
            },
        ),
    )
    .unwrap();

    let event_count = db
        .with_write_conn(|conn| rebuild(conn, &events_path))
        .unwrap();
    assert_eq!(event_count, 4);

    // Both records exist
    assert_eq!(count_records(&db), 2);

    // Artifact has 2 inline tags
    assert_eq!(count_tags_for(&db, &artifact_id), 2);
    assert!(tag_exists(&db, &artifact_id, "alpha"));
    assert!(tag_exists(&db, &artifact_id, "beta"));

    // Snapshot has 1 tag added via event
    assert_eq!(count_tags_for(&db, &snapshot_id), 1);
    assert!(tag_exists(&db, &snapshot_id, "critical"));

    // Artifact has 1 link
    assert_eq!(count_links_for(&db, &artifact_id), 1);
    assert!(link_task_exists(&db, &artifact_id, "BRN-TASK-01"));
}

#[test]
fn test_rebuild_event_count_matches() {
    let dir = TempDir::new().unwrap();
    let events_path = dir.path().join("events.jsonl");
    let db = Db::open_in_memory().unwrap();

    let n_records = 5;
    for i in 0..n_records {
        append_event(
            &events_path,
            &make_created_event(&format!("r{i}"), &format!("Record {i}"), "report"),
        )
        .unwrap();
    }

    // Add extra events (tag, update)
    append_event(
        &events_path,
        &RecordEvent::new(
            "r0",
            "agent",
            RecordEventType::TagAdded,
            &TagPayload {
                tag: "important".to_string(),
            },
        ),
    )
    .unwrap();
    append_event(
        &events_path,
        &RecordEvent::from_payload(
            "r1",
            "agent",
            RecordUpdatedPayload {
                title: Some("Updated".to_string()),
                description: None,
            },
        ),
    )
    .unwrap();

    let total_events = read_all_events(&events_path).unwrap().len();
    let rebuild_count = db
        .with_write_conn(|conn| rebuild(conn, &events_path))
        .unwrap();

    assert_eq!(rebuild_count, total_events);
    assert_eq!(rebuild_count, n_records + 2);
}

// ─── 2. Rebuild idempotency ───────────────────────────────────────

#[test]
fn test_rebuild_twice_is_idempotent() {
    let dir = TempDir::new().unwrap();
    let events_path = dir.path().join("events.jsonl");
    let db = Db::open_in_memory().unwrap();

    let r1 = new_record_id("BRN");
    let r2 = new_record_id("BRN");
    append_event(
        &events_path,
        &make_created_event_with_tags(&r1, "Artifact One", "diff", vec!["tag-a".to_string()]),
    )
    .unwrap();
    append_event(
        &events_path,
        &make_created_event(&r2, "Artifact Two", "analysis"),
    )
    .unwrap();
    append_event(
        &events_path,
        &RecordEvent::new(
            &r1,
            "agent",
            RecordEventType::LinkAdded,
            &LinkPayload {
                task_id: Some("TASK-X".to_string()),
                chunk_id: None,
            },
        ),
    )
    .unwrap();

    let count1 = db
        .with_write_conn(|conn| rebuild(conn, &events_path))
        .unwrap();
    let records_after_first = count_records(&db);
    let tags_after_first = count_tags_for(&db, &r1);
    let links_after_first = count_links_for(&db, &r1);
    let title_after_first = get_title(&db, &r1);

    let count2 = db
        .with_write_conn(|conn| rebuild(conn, &events_path))
        .unwrap();
    let records_after_second = count_records(&db);
    let tags_after_second = count_tags_for(&db, &r1);
    let links_after_second = count_links_for(&db, &r1);
    let title_after_second = get_title(&db, &r1);

    assert_eq!(count1, count2, "event counts should match across rebuilds");
    assert_eq!(
        records_after_first, records_after_second,
        "record count unchanged"
    );
    assert_eq!(tags_after_first, tags_after_second, "tag count unchanged");
    assert_eq!(
        links_after_first, links_after_second,
        "link count unchanged"
    );
    assert_eq!(title_after_first, title_after_second, "title unchanged");
}

// ─── 3. Crash recovery ────────────────────────────────────────────

#[test]
fn test_rebuild_recovers_when_sqlite_is_behind_event_log() {
    let dir = TempDir::new().unwrap();
    let events_path = dir.path().join("events.jsonl");
    let db = Db::open_in_memory().unwrap();

    let r1 = new_record_id("BRN");
    let r2 = new_record_id("BRN");

    // Apply r1 to SQLite and log (simulating r1 was fully projected)
    let ev1 = make_created_event(&r1, "Record One", "report");
    append_event(&events_path, &ev1).unwrap();
    db.with_write_conn(|conn| apply_event(conn, &ev1, "")).unwrap();

    // Write r2 to the log but DO NOT project it (simulates crash before projection)
    let ev2 = make_created_event(&r2, "Record Two", "export");
    append_event(&events_path, &ev2).unwrap();

    // SQLite is behind: only r1 is projected
    assert_eq!(count_records(&db), 1);

    // Rebuild recovers to full consistent state
    let recovered = db
        .with_write_conn(|conn| rebuild(conn, &events_path))
        .unwrap();
    assert_eq!(recovered, 2);
    assert_eq!(count_records(&db), 2);
    assert_eq!(get_status(&db, &r1), "active");
    assert_eq!(get_status(&db, &r2), "active");
}

#[test]
fn test_rebuild_recovers_when_more_events_logged_than_projected() {
    let dir = TempDir::new().unwrap();
    let events_path = dir.path().join("events.jsonl");
    let db = Db::open_in_memory().unwrap();

    let rid = new_record_id("BRN");

    // Project only the create event
    let ev_create = make_created_event(&rid, "My Record", "document");
    append_event(&events_path, &ev_create).unwrap();
    db.with_write_conn(|conn| apply_event(conn, &ev_create, ""))
        .unwrap();

    // Log additional events but don't project them (simulates crash mid-sequence)
    append_event(
        &events_path,
        &RecordEvent::new(
            &rid,
            "agent",
            RecordEventType::TagAdded,
            &TagPayload {
                tag: "urgent".to_string(),
            },
        ),
    )
    .unwrap();
    append_event(
        &events_path,
        &RecordEvent::from_payload(&rid, "agent", RecordArchivedPayload { reason: None }),
    )
    .unwrap();

    // SQLite shows: active, no tags
    assert_eq!(get_status(&db, &rid), "active");
    assert_eq!(count_tags_for(&db, &rid), 0);

    // Rebuild recovers the full state
    db.with_write_conn(|conn| rebuild(conn, &events_path))
        .unwrap();

    assert_eq!(get_status(&db, &rid), "archived");
    assert_eq!(count_tags_for(&db, &rid), 1);
    assert!(tag_exists(&db, &rid, "urgent"));
}

// ─── 4. Partial write (truncated last line) ───────────────────────

#[test]
fn test_rebuild_skips_truncated_last_line_and_recovers() {
    let dir = TempDir::new().unwrap();
    let events_path = dir.path().join("events.jsonl");
    let db = Db::open_in_memory().unwrap();

    // Write 2 good events
    let r1 = new_record_id("BRN");
    let r2 = new_record_id("BRN");
    append_event(&events_path, &make_created_event(&r1, "Good One", "report")).unwrap();
    append_event(&events_path, &make_created_event(&r2, "Good Two", "diff")).unwrap();

    // Simulate a partial (truncated) write: append an incomplete JSON line
    {
        let mut file = OpenOptions::new().append(true).open(&events_path).unwrap();
        // Incomplete JSON — no closing brace
        file.write_all(b"{\"event_id\":\"truncated\",\"record_id\":\"r3\"")
            .unwrap();
        file.flush().unwrap();
    }

    // read_all_events skips malformed lines — only 2 valid events remain
    let events = read_all_events(&events_path).unwrap();
    assert_eq!(events.len(), 2, "truncated line should be skipped");

    // rebuild should succeed on the 2 valid events
    let count = db
        .with_write_conn(|conn| rebuild(conn, &events_path))
        .unwrap();
    assert_eq!(count, 2);
    assert_eq!(count_records(&db), 2);
    assert_eq!(get_title(&db, &r1), "Good One");
    assert_eq!(get_title(&db, &r2), "Good Two");
}

// ─── 5. Projection consistency: incremental vs bulk rebuild ───────

#[test]
fn test_incremental_apply_matches_bulk_rebuild() {
    let dir = TempDir::new().unwrap();
    let events_path = dir.path().join("events.jsonl");
    let db_incremental = Db::open_in_memory().unwrap();
    let db_bulk = Db::open_in_memory().unwrap();

    let r1 = new_record_id("BRN");
    let r2 = new_record_id("BRN");

    let events_to_apply: Vec<RecordEvent> = vec![
        make_created_event_with_tags(&r1, "Alpha", "report", vec!["x".to_string()]),
        make_created_event(&r2, "Beta", "snapshot"),
        RecordEvent::new(
            &r1,
            "agent",
            RecordEventType::LinkAdded,
            &LinkPayload {
                task_id: Some("TASK-1".to_string()),
                chunk_id: None,
            },
        ),
        RecordEvent::from_payload(
            &r2,
            "agent",
            RecordArchivedPayload {
                reason: Some("outdated".to_string()),
            },
        ),
        RecordEvent::new(
            &r1,
            "agent",
            RecordEventType::TagAdded,
            &TagPayload {
                tag: "high-priority".to_string(),
            },
        ),
    ];

    // Apply events incrementally and append to log
    for ev in &events_to_apply {
        db_incremental
            .with_write_conn(|conn| apply_event(conn, ev, ""))
            .unwrap();
        append_event(&events_path, ev).unwrap();
    }

    // Rebuild from log in bulk
    db_bulk
        .with_write_conn(|conn| rebuild(conn, &events_path))
        .unwrap();

    // Compare final state
    assert_eq!(
        count_records(&db_incremental),
        count_records(&db_bulk),
        "record counts differ"
    );

    for id in [&r1, &r2] {
        let inc_status = get_status(&db_incremental, id);
        let bulk_status = get_status(&db_bulk, id);
        assert_eq!(
            inc_status, bulk_status,
            "status differs for {id}: inc={inc_status} bulk={bulk_status}"
        );

        let inc_tags = count_tags_for(&db_incremental, id);
        let bulk_tags = count_tags_for(&db_bulk, id);
        assert_eq!(
            inc_tags, bulk_tags,
            "tag count differs for {id}: inc={inc_tags} bulk={bulk_tags}"
        );

        let inc_links = count_links_for(&db_incremental, id);
        let bulk_links = count_links_for(&db_bulk, id);
        assert_eq!(
            inc_links, bulk_links,
            "link count differs for {id}: inc={inc_links} bulk={bulk_links}"
        );
    }
}

#[test]
fn test_incremental_state_after_each_event() {
    let db = Db::open_in_memory().unwrap();
    let rid = new_record_id("BRN");

    // Event 1: create → record active, no tags
    let ev1 = make_created_event(&rid, "Initial", "analysis");
    db.with_write_conn(|conn| apply_event(conn, &ev1, "")).unwrap();
    assert_eq!(count_records(&db), 1);
    assert_eq!(get_status(&db, &rid), "active");
    assert_eq!(count_tags_for(&db, &rid), 0);

    // Event 2: add tag → 1 tag
    let ev2 = RecordEvent::new(
        &rid,
        "agent",
        RecordEventType::TagAdded,
        &TagPayload {
            tag: "new-tag".to_string(),
        },
    );
    db.with_write_conn(|conn| apply_event(conn, &ev2, "")).unwrap();
    assert_eq!(count_tags_for(&db, &rid), 1);
    assert!(tag_exists(&db, &rid, "new-tag"));

    // Event 3: update title
    let ev3 = RecordEvent::from_payload(
        &rid,
        "agent",
        RecordUpdatedPayload {
            title: Some("Updated Title".to_string()),
            description: None,
        },
    );
    db.with_write_conn(|conn| apply_event(conn, &ev3, "")).unwrap();
    assert_eq!(get_title(&db, &rid), "Updated Title");

    // Event 4: archive
    let ev4 = RecordEvent::from_payload(&rid, "agent", RecordArchivedPayload { reason: None });
    db.with_write_conn(|conn| apply_event(conn, &ev4, "")).unwrap();
    assert_eq!(get_status(&db, &rid), "archived");
}

// ─── 6. Status transition: active → archived ─────────────────────

#[test]
fn test_status_transition_active_to_archived() {
    let db = Db::open_in_memory().unwrap();
    let rid = new_record_id("BRN");

    db.with_write_conn(|conn| apply_event(conn, &make_created_event(&rid, "My Record", "export"), ""))
        .unwrap();
    assert_eq!(get_status(&db, &rid), "active");

    db.with_write_conn(|conn| {
        apply_event(
            conn,
            &RecordEvent::from_payload(
                &rid,
                "agent",
                RecordArchivedPayload {
                    reason: Some("superseded".to_string()),
                },
            ),
            "",
        )
    })
    .unwrap();
    assert_eq!(get_status(&db, &rid), "archived");
}

#[test]
fn test_multiple_records_have_independent_status() {
    let db = Db::open_in_memory().unwrap();
    let r1 = new_record_id("BRN");
    let r2 = new_record_id("BRN");

    db.with_write_conn(|conn| apply_event(conn, &make_created_event(&r1, "Active One", "report"), ""))
        .unwrap();
    db.with_write_conn(|conn| apply_event(conn, &make_created_event(&r2, "Active Two", "diff"), ""))
        .unwrap();

    // Archive only r2
    db.with_write_conn(|conn| {
        apply_event(
            conn,
            &RecordEvent::from_payload(&r2, "agent", RecordArchivedPayload { reason: None }),
            "",
        )
    })
    .unwrap();

    assert_eq!(get_status(&db, &r1), "active");
    assert_eq!(get_status(&db, &r2), "archived");
}

// ─── 7. Tag lifecycle: add + remove ──────────────────────────────

#[test]
fn test_tag_add_then_remove_leaves_no_tag() {
    let db = Db::open_in_memory().unwrap();
    let rid = new_record_id("BRN");

    db.with_write_conn(|conn| {
        apply_event(conn, &make_created_event(&rid, "Tagged Record", "report"), "")
    })
    .unwrap();

    db.with_write_conn(|conn| {
        apply_event(
            conn,
            &RecordEvent::new(
                &rid,
                "agent",
                RecordEventType::TagAdded,
                &TagPayload {
                    tag: "temporary".to_string(),
                },
            ),
            "",
        )
    })
    .unwrap();
    assert!(tag_exists(&db, &rid, "temporary"));

    db.with_write_conn(|conn| {
        apply_event(
            conn,
            &RecordEvent::new(
                &rid,
                "agent",
                RecordEventType::TagRemoved,
                &TagPayload {
                    tag: "temporary".to_string(),
                },
            ),
            "",
        )
    })
    .unwrap();

    assert!(
        !tag_exists(&db, &rid, "temporary"),
        "tag should not appear after removal"
    );
    assert_eq!(count_tags_for(&db, &rid), 0);
}

#[test]
fn test_multiple_tags_add_remove_selectively() {
    let db = Db::open_in_memory().unwrap();
    let rid = new_record_id("BRN");

    db.with_write_conn(|conn| {
        apply_event(
            conn,
            &make_created_event_with_tags(
                &rid,
                "Multi-Tag",
                "analysis",
                vec![
                    "keep-a".to_string(),
                    "remove-b".to_string(),
                    "keep-c".to_string(),
                ],
            ),
            "",
        )
    })
    .unwrap();

    db.with_write_conn(|conn| {
        apply_event(
            conn,
            &RecordEvent::new(
                &rid,
                "agent",
                RecordEventType::TagRemoved,
                &TagPayload {
                    tag: "remove-b".to_string(),
                },
            ),
            "",
        )
    })
    .unwrap();

    assert!(tag_exists(&db, &rid, "keep-a"));
    assert!(
        !tag_exists(&db, &rid, "remove-b"),
        "removed tag should not appear"
    );
    assert!(tag_exists(&db, &rid, "keep-c"));
    assert_eq!(count_tags_for(&db, &rid), 2);
}

#[test]
fn test_tag_add_remove_then_rebuild_is_consistent() {
    let dir = TempDir::new().unwrap();
    let events_path = dir.path().join("events.jsonl");
    let db = Db::open_in_memory().unwrap();

    let rid = new_record_id("BRN");
    append_event(&events_path, &make_created_event(&rid, "T", "report")).unwrap();
    append_event(
        &events_path,
        &RecordEvent::new(
            &rid,
            "agent",
            RecordEventType::TagAdded,
            &TagPayload {
                tag: "gone".to_string(),
            },
        ),
    )
    .unwrap();
    append_event(
        &events_path,
        &RecordEvent::new(
            &rid,
            "agent",
            RecordEventType::TagRemoved,
            &TagPayload {
                tag: "gone".to_string(),
            },
        ),
    )
    .unwrap();

    db.with_write_conn(|conn| rebuild(conn, &events_path))
        .unwrap();
    assert_eq!(
        count_tags_for(&db, &rid),
        0,
        "tag should not appear after rebuild"
    );
}

// ─── 8. Link lifecycle: add + remove ─────────────────────────────

#[test]
fn test_link_add_then_remove_leaves_no_link() {
    let db = Db::open_in_memory().unwrap();
    let rid = new_record_id("BRN");

    db.with_write_conn(|conn| {
        apply_event(conn, &make_created_event(&rid, "Linked Record", "diff"), "")
    })
    .unwrap();

    db.with_write_conn(|conn| {
        apply_event(
            conn,
            &RecordEvent::new(
                &rid,
                "agent",
                RecordEventType::LinkAdded,
                &LinkPayload {
                    task_id: Some("TASK-99".to_string()),
                    chunk_id: None,
                },
            ),
            "",
        )
    })
    .unwrap();
    assert!(link_task_exists(&db, &rid, "TASK-99"));

    db.with_write_conn(|conn| {
        apply_event(
            conn,
            &RecordEvent::new(
                &rid,
                "agent",
                RecordEventType::LinkRemoved,
                &LinkPayload {
                    task_id: Some("TASK-99".to_string()),
                    chunk_id: None,
                },
            ),
            "",
        )
    })
    .unwrap();

    assert!(
        !link_task_exists(&db, &rid, "TASK-99"),
        "link should not appear after removal"
    );
    assert_eq!(count_links_for(&db, &rid), 0);
}

#[test]
fn test_link_add_remove_then_rebuild_is_consistent() {
    let dir = TempDir::new().unwrap();
    let events_path = dir.path().join("events.jsonl");
    let db = Db::open_in_memory().unwrap();

    let rid = new_record_id("BRN");
    append_event(&events_path, &make_created_event(&rid, "T", "report")).unwrap();
    append_event(
        &events_path,
        &RecordEvent::new(
            &rid,
            "agent",
            RecordEventType::LinkAdded,
            &LinkPayload {
                task_id: Some("TASK-GONE".to_string()),
                chunk_id: None,
            },
        ),
    )
    .unwrap();
    append_event(
        &events_path,
        &RecordEvent::new(
            &rid,
            "agent",
            RecordEventType::LinkRemoved,
            &LinkPayload {
                task_id: Some("TASK-GONE".to_string()),
                chunk_id: None,
            },
        ),
    )
    .unwrap();

    db.with_write_conn(|conn| rebuild(conn, &events_path))
        .unwrap();
    assert_eq!(
        count_links_for(&db, &rid),
        0,
        "link should not appear after rebuild"
    );
}

// ─── 9. Object store integration ─────────────────────────────────

#[test]
fn test_object_store_write_and_read_blob() {
    let dir = TempDir::new().unwrap();
    let store = ObjectStore::new(dir.path().join("objects")).unwrap();

    let payload = b"Hello from the object store!";
    let content_ref = store.write(payload).unwrap();

    // Size matches
    assert_eq!(content_ref.size, payload.len() as u64);
    // Hash is a valid 64-char hex string
    assert_eq!(content_ref.hash.len(), 64);

    // Read back and verify content
    let read_back = store.read(&content_ref.hash).unwrap();
    assert_eq!(read_back, payload);
}

#[test]
fn test_object_store_content_hash_matches_record() {
    let dir = TempDir::new().unwrap();
    let store = ObjectStore::new(dir.path().join("objects")).unwrap();
    let events_path = dir.path().join("events.jsonl");
    let db = Db::open_in_memory().unwrap();

    // Store the blob
    let payload = b"{\"result\": \"analysis complete\"}";
    let content_ref = store
        .write_with_media_type(payload, Some("application/json".to_string()))
        .unwrap();

    // Create a record referencing the stored blob
    let rid = new_record_id("BRN");
    let ev = RecordEvent::from_payload(
        &rid,
        "test-agent",
        RecordCreatedPayload {
            title: "Analysis Result".to_string(),
            kind: "analysis".to_string(),
            content_ref: ContentRefPayload::new(
                content_ref.hash.clone(),
                content_ref.size,
                content_ref.media_type.clone(),
            ),
            description: None,
            task_id: None,
            tags: vec![],
            scope_type: None,
            scope_id: None,
            retention_class: None,
            producer: None,
        },
    );

    append_event(&events_path, &ev).unwrap();
    db.with_write_conn(|conn| rebuild(conn, &events_path))
        .unwrap();

    // Verify content_hash in SQLite matches the stored blob's hash
    let stored_hash = get_content_hash(&db, &rid);
    assert_eq!(stored_hash, content_ref.hash);

    // Read blob back and verify content
    let read_back = store.read(&stored_hash).unwrap();
    assert_eq!(read_back, payload);
}

#[test]
fn test_object_store_deduplication_with_multiple_records() {
    let dir = TempDir::new().unwrap();
    let store = ObjectStore::new(dir.path().join("objects")).unwrap();
    let events_path = dir.path().join("events.jsonl");
    let db = Db::open_in_memory().unwrap();

    // Write the same payload twice — only one blob on disk (deduplication)
    let payload = b"shared content";
    let ref1 = store.write(payload).unwrap();
    let ref2 = store.write(payload).unwrap();
    assert_eq!(
        ref1.hash, ref2.hash,
        "duplicate blob should produce same hash"
    );

    // Two different records reference the same blob
    let rids = [new_record_id("BRN"), new_record_id("BRN")];
    for (i, rid) in rids.iter().enumerate() {
        let ev = RecordEvent::from_payload(
            rid,
            "agent",
            RecordCreatedPayload {
                title: format!("Record {i}"),
                kind: "report".to_string(),
                content_ref: ContentRefPayload::new(ref1.hash.clone(), ref1.size, None),
                description: None,
                task_id: None,
                tags: vec![],
                scope_type: None,
                scope_id: None,
                retention_class: None,
                producer: None,
            },
        );
        append_event(&events_path, &ev).unwrap();
    }

    db.with_write_conn(|conn| rebuild(conn, &events_path))
        .unwrap();
    assert_eq!(count_records(&db), 2);

    // Both records share the same content hash
    let hashes: Vec<String> = {
        db.with_read_conn(|conn| {
            let mut stmt = conn
                .prepare("SELECT content_hash FROM records ORDER BY record_id")
                .unwrap();
            let v: Vec<String> = stmt
                .query_map([], |row| row.get(0))
                .unwrap()
                .map(|r| r.unwrap())
                .collect();
            Ok(v)
        })
        .unwrap()
    };
    assert_eq!(hashes.len(), 2);
    assert_eq!(
        hashes[0], hashes[1],
        "both records should reference the same blob hash"
    );
}

// ─── 10. RecordStore high-level API ──────────────────────────────

#[test]
fn test_record_store_apply_and_append_then_rebuild() {
    let dir = TempDir::new().unwrap();
    let records_dir = dir.path().join("records");
    let db = Db::open_in_memory().unwrap();
    let store = RecordStore::new(&records_dir, db).unwrap();

    let rid = new_record_id("BRN");
    let ev = make_created_event(&rid, "Store Record", "document");

    // Use high-level API: apply_and_append
    store.apply_and_append(&ev).unwrap();

    // Verify the event log has the event
    let events = store.read_all_events().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].record_id, rid);

    // Rebuild using the high-level API
    let count = store.rebuild_projections().unwrap();
    assert_eq!(count, 1);
}

#[test]
fn test_record_store_append_then_rebuild_recovers_projection() {
    let dir = TempDir::new().unwrap();
    let records_dir = dir.path().join("records");
    let db = Db::open_in_memory().unwrap();
    let store = RecordStore::new(&records_dir, db).unwrap();

    let r1 = new_record_id("BRN");
    let r2 = new_record_id("BRN");

    // Append events to log only (no projection applied)
    store
        .append(&make_created_event(&r1, "One", "report"))
        .unwrap();
    store
        .append(&make_created_event(&r2, "Two", "diff"))
        .unwrap();
    store
        .append(&RecordEvent::new(
            &r1,
            "agent",
            RecordEventType::TagAdded,
            &TagPayload {
                tag: "rebuild-test".to_string(),
            },
        ))
        .unwrap();

    // Rebuild from log
    let count = store.rebuild_projections().unwrap();
    assert_eq!(count, 3);

    // Verify projected state via DB
    let total: i64 = store
        .db()
        .with_read_conn(|conn| {
            conn.query_row("SELECT COUNT(*) FROM records", [], |row| row.get(0))
                .map_err(Into::into)
        })
        .unwrap();
    assert_eq!(total, 2);

    let tag_count: i64 = store
        .db()
        .with_read_conn(|conn| {
            conn.query_row(
                "SELECT COUNT(*) FROM record_tags WHERE record_id = ?1",
                rusqlite::params![r1],
                |row| row.get(0),
            )
            .map_err(Into::into)
        })
        .unwrap();
    assert_eq!(tag_count, 1);
}

// ─── 11. RecordStatus type ────────────────────────────────────────

#[test]
fn test_record_status_from_str() {
    use std::str::FromStr;
    assert_eq!(
        RecordStatus::from_str("active").unwrap(),
        RecordStatus::Active
    );
    assert_eq!(
        RecordStatus::from_str("archived").unwrap(),
        RecordStatus::Archived
    );
    assert!(RecordStatus::from_str("invalid").is_err());
}

// ─── 12. B2: SQLite-first write semantics ────────────────────────

#[test]
fn test_record_sqlite_write_succeeds_when_jsonl_dir_is_read_only() {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    let dir = TempDir::new().unwrap();
    let records_dir = dir.path().join("records");
    let db = Db::open_in_memory().unwrap();
    let store = RecordStore::new(&records_dir, db).unwrap();

    // Make the records directory read-only so JSONL append will fail
    let perms = fs::Permissions::from_mode(0o555);
    fs::set_permissions(&records_dir, perms).unwrap();

    let rid = new_record_id("BRN");
    let ev = make_created_event(&rid, "Read-only Test", "report");

    // apply_and_append must succeed even though JSONL dir is read-only
    let result = store.apply_and_append(&ev);
    assert!(
        result.is_ok(),
        "apply_and_append must succeed when JSONL dir is read-only"
    );

    // Record exists in SQLite
    let record = store.get_record(&rid).unwrap();
    assert!(record.is_some(), "record must be in SQLite after write");
    assert_eq!(record.unwrap().title, "Read-only Test");

    // Restore permissions so TempDir cleanup works
    let perms = fs::Permissions::from_mode(0o755);
    fs::set_permissions(&records_dir, perms).unwrap();
}

#[test]
fn test_record_jsonl_audit_trail_populated_on_success() {
    use std::fs;

    let dir = TempDir::new().unwrap();
    let records_dir = dir.path().join("records");
    let db = Db::open_in_memory().unwrap();
    let store = RecordStore::new(&records_dir, db).unwrap();

    let rid = new_record_id("BRN");
    let ev = make_created_event(&rid, "Audit Record", "document");
    store.apply_and_append(&ev).unwrap();

    // SQLite has the record
    assert!(store.get_record(&rid).unwrap().is_some());

    // JSONL also has the event
    let jsonl_path = records_dir.join("events.jsonl");
    assert!(
        jsonl_path.exists(),
        "events.jsonl must exist after successful write"
    );
    let content = fs::read_to_string(&jsonl_path).unwrap();
    assert!(!content.is_empty(), "events.jsonl must contain the event");
    assert!(
        content.contains(rid.as_str()),
        "events.jsonl must reference the record id"
    );
}

#[test]
fn test_record_rebuild_from_jsonl_recovers_projections() {
    let dir = TempDir::new().unwrap();
    let records_dir = dir.path().join("records");
    let db = Db::open_in_memory().unwrap();
    let store = RecordStore::new(&records_dir, db).unwrap();

    let r1 = new_record_id("BRN");
    let r2 = new_record_id("BRN");
    store
        .apply_and_append(&make_created_event(&r1, "One", "report"))
        .unwrap();
    store
        .apply_and_append(&make_created_event(&r2, "Two", "diff"))
        .unwrap();

    // Verify both in SQLite
    assert_eq!(count_records(store.db()), 2);

    // Wipe SQLite projections manually
    store
        .db()
        .with_write_conn(|conn| {
            conn.execute_batch("DELETE FROM records")
                .map_err(Into::into)
        })
        .unwrap();
    assert_eq!(count_records(store.db()), 0, "SQLite wiped");

    // Rebuild from JSONL
    let event_count = store.rebuild_projections().unwrap();
    assert_eq!(event_count, 2);

    // Both records restored
    assert_eq!(count_records(store.db()), 2);
    assert!(store.get_record(&r1).unwrap().is_some());
    let r2_row = store.get_record(&r2).unwrap().unwrap();
    assert_eq!(r2_row.title, "Two");
}
