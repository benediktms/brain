//! Read-side helper for the synonym-clustering pipeline (parent task brn-83a.7.2).
//!
//! `collect_raw_tags()` enumerates the universe of raw tag strings currently
//! referenced by `record_tags` and `task_labels`, with reference counts and
//! last-seen timestamps. Downstream tasks consume this to seed clustering;
//! this module is read-only and does not write to `tag_aliases`.

use rusqlite::{Connection, params};

use crate::db::collect_rows;
use crate::error::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TagSource {
    Records,
    Tasks,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawTag {
    pub tag: String,
    pub source: TagSource,
    pub reference_count: i64,
    /// Unix seconds, sourced from `records.updated_at` / `tasks.updated_at`.
    pub last_seen_at: i64,
}

/// Enumerate every distinct (tag, source) pair across `record_tags` and
/// `task_labels`, with a reference count and the most recent `updated_at`
/// of any referencing row.
///
/// Both join tables have FKs to their parent (`record_tags → records`,
/// `task_labels → tasks ON DELETE CASCADE`), so orphan rows cannot exist
/// and INNER JOIN is sufficient on both sides.
///
/// `brain_id` filters: `Some(b)` adds `WHERE r.brain_id = b` /
/// `WHERE t.brain_id = b`. **Production callers MUST pass `Some(brain_id)`**
/// — `None` returns globally-unfiltered rows and is intended for tests
/// only. Per-brain consolidation is a non-negotiable invariant of the brain
/// data model (see `feedback_per_brain_consolidation.md`).
pub fn collect_raw_tags(conn: &Connection, brain_id: Option<&str>) -> Result<Vec<RawTag>> {
    let mut out = Vec::new();

    let (records_sql, tasks_sql) = match brain_id {
        Some(_) => (
            "SELECT rt.tag, COUNT(*), COALESCE(MAX(r.updated_at), 0)
             FROM record_tags rt
             JOIN records r ON r.record_id = rt.record_id
             WHERE r.brain_id = ?1
             GROUP BY rt.tag",
            "SELECT tl.label, COUNT(*), COALESCE(MAX(t.updated_at), 0)
             FROM task_labels tl
             JOIN tasks t ON t.task_id = tl.task_id
             WHERE t.brain_id = ?1
             GROUP BY tl.label",
        ),
        None => (
            "SELECT rt.tag, COUNT(*), COALESCE(MAX(r.updated_at), 0)
             FROM record_tags rt
             JOIN records r ON r.record_id = rt.record_id
             GROUP BY rt.tag",
            "SELECT tl.label, COUNT(*), COALESCE(MAX(t.updated_at), 0)
             FROM task_labels tl
             JOIN tasks t ON t.task_id = tl.task_id
             GROUP BY tl.label",
        ),
    };

    {
        let mut stmt = conn.prepare(records_sql)?;
        let map_row = |row: &rusqlite::Row<'_>| {
            Ok(RawTag {
                tag: row.get(0)?,
                source: TagSource::Records,
                reference_count: row.get(1)?,
                last_seen_at: row.get(2)?,
            })
        };
        let rows = match brain_id {
            Some(b) => stmt.query_map(params![b], map_row)?,
            None => stmt.query_map([], map_row)?,
        };
        out.extend(collect_rows(rows)?);
    }

    {
        let mut stmt = conn.prepare(tasks_sql)?;
        let map_row = |row: &rusqlite::Row<'_>| {
            Ok(RawTag {
                tag: row.get(0)?,
                source: TagSource::Tasks,
                reference_count: row.get(1)?,
                last_seen_at: row.get(2)?,
            })
        };
        let rows = match brain_id {
            Some(b) => stmt.query_map(params![b], map_row)?,
            None => stmt.query_map([], map_row)?,
        };
        out.extend(collect_rows(rows)?);
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::schema::init_schema;
    use rusqlite::params;

    fn setup() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();
        conn
    }

    fn insert_record(conn: &Connection, record_id: &str, updated_at: i64) {
        conn.execute(
            "INSERT INTO records (
                record_id, title, kind, status, description, content_hash,
                content_size, media_type, task_id, actor, created_at, updated_at,
                retention_class, pinned, payload_available, content_encoding,
                original_size, brain_id, searchable, embedded_at
            ) VALUES (
                ?1, ?2, 'document', 'active', NULL, 'hash',
                4, 'text/plain', NULL, 'test-agent', ?3, ?3,
                NULL, 0, 1, 'identity',
                NULL, '', 1, NULL
            )",
            params![record_id, format!("record {record_id}"), updated_at],
        )
        .unwrap();
    }

    fn insert_record_tag(conn: &Connection, record_id: &str, tag: &str) {
        conn.execute(
            "INSERT INTO record_tags (record_id, tag) VALUES (?1, ?2)",
            params![record_id, tag],
        )
        .unwrap();
    }

    fn insert_task(conn: &Connection, task_id: &str, updated_at: i64) {
        conn.execute(
            "INSERT INTO tasks (task_id, title, status, priority, task_type, created_at, updated_at)
             VALUES (?1, ?2, 'open', 1, 'task', ?3, ?3)",
            params![task_id, format!("task {task_id}"), updated_at],
        )
        .unwrap();
    }

    fn insert_task_label(conn: &Connection, task_id: &str, label: &str) {
        conn.execute(
            "INSERT INTO task_labels (task_id, label) VALUES (?1, ?2)",
            params![task_id, label],
        )
        .unwrap();
    }

    fn find(out: &[RawTag], tag: &str, source: TagSource) -> RawTag {
        out.iter()
            .find(|r| r.tag == tag && r.source == source)
            .unwrap_or_else(|| panic!("missing {tag} from {source:?} in {out:?}"))
            .clone()
    }

    #[test]
    fn empty_db_returns_empty_vec() {
        let conn = setup();
        let out = collect_raw_tags(&conn, None).unwrap();
        assert!(out.is_empty(), "expected no rows, got {out:?}");
    }

    #[test]
    fn aggregates_overlapping_tags_with_distinct_sources() {
        let conn = setup();

        insert_record(&conn, "r1", 1000);
        insert_record(&conn, "r2", 2000);
        insert_record(&conn, "r3", 1500);
        insert_record_tag(&conn, "r1", "shared");
        insert_record_tag(&conn, "r2", "shared");
        insert_record_tag(&conn, "r3", "records-only");

        insert_task(&conn, "t1", 3000);
        insert_task(&conn, "t2", 500);
        insert_task_label(&conn, "t1", "shared");
        insert_task_label(&conn, "t2", "tasks-only");

        let out = collect_raw_tags(&conn, None).unwrap();
        assert_eq!(out.len(), 4, "expected 4 rows, got {out:?}");

        let shared_records = find(&out, "shared", TagSource::Records);
        assert_eq!(shared_records.reference_count, 2);
        assert_eq!(shared_records.last_seen_at, 2000);

        let shared_tasks = find(&out, "shared", TagSource::Tasks);
        assert_eq!(shared_tasks.reference_count, 1);
        assert_eq!(shared_tasks.last_seen_at, 3000);

        let records_only = find(&out, "records-only", TagSource::Records);
        assert_eq!(records_only.reference_count, 1);
        assert_eq!(records_only.last_seen_at, 1500);

        let tasks_only = find(&out, "tasks-only", TagSource::Tasks);
        assert_eq!(tasks_only.reference_count, 1);
        assert_eq!(tasks_only.last_seen_at, 500);
    }

    #[test]
    fn deleted_task_cascades_label_removal() {
        // Sanity check that the production FK behaviour matches our INNER JOIN
        // assumption: deleting a task auto-removes its labels (ON DELETE CASCADE),
        // so collect_raw_tags never has to handle an orphan label.
        let conn = setup();

        insert_task(&conn, "t-tmp", 1000);
        insert_task_label(&conn, "t-tmp", "transient");

        let before = collect_raw_tags(&conn, None).unwrap();
        assert!(before.iter().any(|r| r.tag == "transient"));

        conn.execute("DELETE FROM tasks WHERE task_id = 't-tmp'", [])
            .unwrap();

        let after = collect_raw_tags(&conn, None).unwrap();
        assert!(
            after.iter().all(|r| r.tag != "transient"),
            "label should be cascade-deleted, got {after:?}"
        );
    }
}
