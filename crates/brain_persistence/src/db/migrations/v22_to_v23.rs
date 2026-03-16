use rusqlite::Connection;

use crate::db::meta::generate_prefix;
use crate::error::Result;

/// v22 → v23: Add `prefix` column to `brains` table + rewrite stale BRX- IDs.
///
/// 1. `ALTER TABLE brains ADD COLUMN prefix TEXT`
/// 2. Backfill `prefix` from `generate_prefix(name)` for every brain row.
/// 3. Rewrite `BRX-` prefixed IDs to the correct brain-specific prefix.
///    Brains whose name legitimately derives to `BRX` keep their IDs unchanged.
/// 4. Unscoped rows (brain_id = '') are left untouched.
pub fn migrate_v22_to_v23(conn: &Connection) -> Result<()> {
    // ── Phase 1: add prefix column ──────────────────────────────────────────
    conn.execute_batch("ALTER TABLE brains ADD COLUMN prefix TEXT")?;

    // ── Phase 2: backfill prefixes ──────────────────────────────────────────
    let brains: Vec<(String, String)> = {
        let mut stmt = conn.prepare("SELECT brain_id, name FROM brains")?;
        let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()?
    };

    for (brain_id, name) in &brains {
        let prefix = generate_prefix(name);
        conn.execute(
            "UPDATE brains SET prefix = ?1 WHERE brain_id = ?2",
            rusqlite::params![prefix, brain_id],
        )?;
    }

    // ── Phase 3: rewrite stale BRX- IDs ─────────────────────────────────────
    //
    // For each brain whose correct prefix is NOT 'BRX', find all tasks/records
    // with BRX- prefix belonging to that brain_id and rewrite them.

    let brains_to_rewrite: Vec<(String, String)> = {
        let mut stmt =
            conn.prepare("SELECT brain_id, prefix FROM brains WHERE prefix IS NOT NULL AND prefix != 'BRX' AND brain_id != ''")?;
        let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()?
    };

    if brains_to_rewrite.is_empty() {
        // No rewrites needed — stamp version and exit.
        conn.execute_batch("PRAGMA user_version = 23")?;
        return Ok(());
    }

    // Disable FK checks for PK updates.
    conn.execute_batch("PRAGMA foreign_keys = OFF")?;

    for (brain_id, correct_prefix) in &brains_to_rewrite {
        rewrite_brain_ids(conn, brain_id, correct_prefix)?;
    }

    conn.execute_batch(
        "PRAGMA user_version = 23;
         PRAGMA foreign_keys = ON;",
    )?;

    Ok(())
}

/// Rewrite all BRX- prefixed IDs for a single brain to its correct prefix.
fn rewrite_brain_ids(conn: &Connection, brain_id: &str, correct_prefix: &str) -> Result<()> {
    let new_prefix = format!("{correct_prefix}-");
    let old_prefix = "BRX-";

    // ── Tasks ────────────────────────────────────────────────────────────────

    let task_ids: Vec<String> = {
        let mut stmt =
            conn.prepare("SELECT task_id FROM tasks WHERE brain_id = ?1 AND task_id LIKE 'BRX-%'")?;
        let rows = stmt.query_map([brain_id], |row| row.get(0))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()?
    };

    for old_id in &task_ids {
        let new_id = format!("{new_prefix}{}", &old_id[old_prefix.len()..]);

        // tasks.task_id (PK)
        conn.execute(
            "UPDATE tasks SET task_id = ?1 WHERE task_id = ?2",
            rusqlite::params![new_id, old_id],
        )?;

        // tasks.parent_task_id (FK) — other tasks referencing the old ID as parent
        conn.execute(
            "UPDATE tasks SET parent_task_id = ?1 WHERE parent_task_id = ?2",
            rusqlite::params![new_id, old_id],
        )?;

        // task_events.task_id (FK)
        conn.execute(
            "UPDATE task_events SET task_id = ?1 WHERE task_id = ?2",
            rusqlite::params![new_id, old_id],
        )?;

        // task_events.payload (JSON) — replace BRX- references in JSON payloads
        conn.execute(
            "UPDATE task_events SET payload = REPLACE(payload, ?1, ?2) WHERE task_id = ?3 AND payload LIKE '%' || ?1 || '%'",
            rusqlite::params![old_id, new_id, new_id],
        )?;

        // task_deps.task_id
        conn.execute(
            "UPDATE task_deps SET task_id = ?1 WHERE task_id = ?2",
            rusqlite::params![new_id, old_id],
        )?;

        // task_deps.depends_on
        conn.execute(
            "UPDATE task_deps SET depends_on = ?1 WHERE depends_on = ?2",
            rusqlite::params![new_id, old_id],
        )?;

        // task_labels.task_id
        conn.execute(
            "UPDATE task_labels SET task_id = ?1 WHERE task_id = ?2",
            rusqlite::params![new_id, old_id],
        )?;

        // task_comments.task_id
        conn.execute(
            "UPDATE task_comments SET task_id = ?1 WHERE task_id = ?2",
            rusqlite::params![new_id, old_id],
        )?;

        // task_note_links.task_id
        conn.execute(
            "UPDATE task_note_links SET task_id = ?1 WHERE task_id = ?2",
            rusqlite::params![new_id, old_id],
        )?;

        // task_external_ids.task_id
        conn.execute(
            "UPDATE task_external_ids SET task_id = ?1 WHERE task_id = ?2",
            rusqlite::params![new_id, old_id],
        )?;
    }

    // ── Records ──────────────────────────────────────────────────────────────

    let record_ids: Vec<String> = {
        let mut stmt = conn.prepare(
            "SELECT record_id FROM records WHERE brain_id = ?1 AND record_id LIKE 'BRX-%'",
        )?;
        let rows = stmt.query_map([brain_id], |row| row.get(0))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()?
    };

    for old_id in &record_ids {
        let new_id = format!("{new_prefix}{}", &old_id[old_prefix.len()..]);

        // records.record_id (PK)
        conn.execute(
            "UPDATE records SET record_id = ?1 WHERE record_id = ?2",
            rusqlite::params![new_id, old_id],
        )?;

        // record_events.record_id (FK)
        conn.execute(
            "UPDATE record_events SET record_id = ?1 WHERE record_id = ?2",
            rusqlite::params![new_id, old_id],
        )?;

        // record_events.payload (JSON)
        conn.execute(
            "UPDATE record_events SET payload = REPLACE(payload, ?1, ?2) WHERE record_id = ?3 AND payload LIKE '%' || ?1 || '%'",
            rusqlite::params![old_id, new_id, new_id],
        )?;

        // record_tags.record_id
        conn.execute(
            "UPDATE record_tags SET record_id = ?1 WHERE record_id = ?2",
            rusqlite::params![new_id, old_id],
        )?;

        // record_links.record_id
        conn.execute(
            "UPDATE record_links SET record_id = ?1 WHERE record_id = ?2",
            rusqlite::params![new_id, old_id],
        )?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a v22 schema with the tables touched by this migration.
    fn setup_v22(conn: &Connection) {
        conn.execute_batch(
            "PRAGMA foreign_keys = OFF;

             CREATE TABLE brains (
                 brain_id   TEXT PRIMARY KEY,
                 name       TEXT NOT NULL UNIQUE,
                 created_at INTEGER NOT NULL
             );

             CREATE TABLE tasks (
                 task_id        TEXT PRIMARY KEY,
                 title          TEXT NOT NULL,
                 description    TEXT,
                 status         TEXT NOT NULL DEFAULT 'open',
                 priority       INTEGER NOT NULL DEFAULT 4,
                 blocked_reason TEXT,
                 due_ts         INTEGER,
                 task_type      TEXT NOT NULL DEFAULT 'task',
                 assignee       TEXT,
                 defer_until    INTEGER,
                 parent_task_id TEXT,
                 child_seq      INTEGER,
                 created_at     INTEGER NOT NULL,
                 updated_at     INTEGER NOT NULL,
                 brain_id       TEXT NOT NULL DEFAULT '' REFERENCES brains(brain_id),
                 embedded_at    INTEGER
             );
             CREATE INDEX idx_tasks_parent ON tasks(parent_task_id);

             CREATE TABLE task_events (
                 event_id   TEXT PRIMARY KEY,
                 task_id    TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
                 event_type TEXT NOT NULL,
                 timestamp  INTEGER NOT NULL,
                 actor      TEXT NOT NULL,
                 payload    TEXT NOT NULL DEFAULT '{}'
             );
             CREATE INDEX idx_task_events_task_id ON task_events(task_id);

             CREATE TABLE task_deps (
                 task_id    TEXT NOT NULL,
                 depends_on TEXT NOT NULL,
                 PRIMARY KEY (task_id, depends_on)
             );

             CREATE TABLE task_labels (
                 task_id TEXT NOT NULL,
                 label   TEXT NOT NULL,
                 PRIMARY KEY (task_id, label)
             );

             CREATE TABLE task_comments (
                 comment_id TEXT PRIMARY KEY,
                 task_id    TEXT NOT NULL,
                 author     TEXT NOT NULL,
                 body       TEXT NOT NULL,
                 created_at INTEGER NOT NULL
             );

             CREATE TABLE task_note_links (
                 task_id  TEXT NOT NULL,
                 chunk_id TEXT NOT NULL,
                 PRIMARY KEY (task_id, chunk_id)
             );

             CREATE TABLE task_external_ids (
                 task_id     TEXT NOT NULL,
                 source      TEXT NOT NULL,
                 external_id TEXT NOT NULL,
                 imported_at INTEGER NOT NULL,
                 PRIMARY KEY (task_id, source)
             );

             CREATE TABLE records (
                 record_id         TEXT PRIMARY KEY,
                 title             TEXT NOT NULL,
                 kind              TEXT NOT NULL,
                 status            TEXT NOT NULL DEFAULT 'active',
                 description       TEXT,
                 content_hash      TEXT NOT NULL,
                 content_size      INTEGER NOT NULL,
                 media_type        TEXT,
                 task_id           TEXT,
                 actor             TEXT NOT NULL,
                 created_at        INTEGER NOT NULL,
                 updated_at        INTEGER NOT NULL,
                 retention_class   TEXT,
                 pinned            INTEGER NOT NULL DEFAULT 0,
                 payload_available INTEGER NOT NULL DEFAULT 1,
                 content_encoding  TEXT NOT NULL DEFAULT 'identity',
                 original_size     INTEGER,
                 brain_id          TEXT NOT NULL DEFAULT '' REFERENCES brains(brain_id)
             );

             CREATE TABLE record_events (
                 event_id   TEXT PRIMARY KEY,
                 record_id  TEXT NOT NULL REFERENCES records(record_id) ON DELETE CASCADE,
                 event_type TEXT NOT NULL,
                 timestamp  INTEGER NOT NULL,
                 actor      TEXT NOT NULL,
                 payload    TEXT NOT NULL DEFAULT '{}',
                 brain_id   TEXT NOT NULL DEFAULT '' REFERENCES brains(brain_id)
             );

             CREATE TABLE record_tags (
                 record_id TEXT NOT NULL REFERENCES records(record_id),
                 tag       TEXT NOT NULL,
                 PRIMARY KEY (record_id, tag)
             );

             CREATE TABLE record_links (
                 record_id  TEXT NOT NULL REFERENCES records(record_id),
                 task_id    TEXT,
                 chunk_id   TEXT,
                 created_at INTEGER NOT NULL,
                 CHECK (task_id IS NOT NULL OR chunk_id IS NOT NULL)
             );

             CREATE TABLE brain_meta (
                 key   TEXT PRIMARY KEY,
                 value TEXT NOT NULL
             );

             PRAGMA user_version = 22;
             PRAGMA foreign_keys = ON;",
        )
        .unwrap();
    }

    /// Seed brains, tasks, and records with BRX- prefixes across multiple brains.
    fn seed_multi_brain_data(conn: &Connection) {
        // Two brains — neither legitimately derives BRX
        conn.execute(
            "INSERT INTO brains (brain_id, name, created_at) VALUES ('b-app', 'my-app', 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO brains (brain_id, name, created_at) VALUES ('b-lib', 'my-lib', 0)",
            [],
        )
        .unwrap();
        // Sentinel for unscoped rows
        conn.execute(
            "INSERT INTO brains (brain_id, name, created_at) VALUES ('', '(unscoped)', 0)",
            [],
        )
        .unwrap();

        // Tasks for b-app (should become MAP-)
        conn.execute(
            "INSERT INTO tasks (task_id, title, status, brain_id, created_at, updated_at)
             VALUES ('BRX-01AAA', 'App task 1', 'open', 'b-app', 0, 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO tasks (task_id, title, status, brain_id, parent_task_id, child_seq, created_at, updated_at)
             VALUES ('BRX-01AAB', 'App subtask', 'open', 'b-app', 'BRX-01AAA', 1, 0, 0)",
            [],
        )
        .unwrap();

        // Tasks for b-lib (should become MLB-)
        conn.execute(
            "INSERT INTO tasks (task_id, title, status, brain_id, created_at, updated_at)
             VALUES ('BRX-01BBB', 'Lib task', 'in_progress', 'b-lib', 0, 0)",
            [],
        )
        .unwrap();

        // Task deps: subtask depends on lib task
        conn.execute(
            "INSERT INTO task_deps (task_id, depends_on) VALUES ('BRX-01AAB', 'BRX-01BBB')",
            [],
        )
        .unwrap();

        // Task events with payload containing BRX- IDs
        conn.execute(
            "INSERT INTO task_events (event_id, task_id, event_type, timestamp, actor, payload)
             VALUES ('e1', 'BRX-01AAA', 'created', 0, 'agent', '{\"task_id\":\"BRX-01AAA\"}')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO task_events (event_id, task_id, event_type, timestamp, actor, payload)
             VALUES ('e2', 'BRX-01AAB', 'parent_set', 0, 'agent', '{\"parent_task_id\":\"BRX-01AAA\"}')",
            [],
        )
        .unwrap();

        // Records for b-app (should become MAP-)
        conn.execute(
            "INSERT INTO records (record_id, title, kind, content_hash, content_size, actor, brain_id, created_at, updated_at)
             VALUES ('BRX-01REC', 'App record', 'snapshot', 'hash1', 100, 'agent', 'b-app', 0, 0)",
            [],
        )
        .unwrap();

        // Record events with payload containing BRX- ID
        conn.execute(
            "INSERT INTO record_events (event_id, record_id, event_type, timestamp, actor, payload, brain_id)
             VALUES ('re1', 'BRX-01REC', 'created', 0, 'agent', '{\"record_id\":\"BRX-01REC\"}', 'b-app')",
            [],
        )
        .unwrap();

        // Record tags
        conn.execute(
            "INSERT INTO record_tags (record_id, tag) VALUES ('BRX-01REC', 'test-tag')",
            [],
        )
        .unwrap();

        // Unscoped task — should NOT be rewritten
        conn.execute(
            "INSERT INTO tasks (task_id, title, status, brain_id, created_at, updated_at)
             VALUES ('BRX-01UNS', 'Unscoped task', 'open', '', 0, 0)",
            [],
        )
        .unwrap();
    }

    #[test]
    fn test_version_stamped() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v22(&conn);

        conn.execute(
            "INSERT INTO brains (brain_id, name, created_at) VALUES ('b1', 'test', 0)",
            [],
        )
        .unwrap();

        migrate_v22_to_v23(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 23);
    }

    #[test]
    fn test_prefix_column_backfilled() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v22(&conn);
        seed_multi_brain_data(&conn);

        migrate_v22_to_v23(&conn).unwrap();

        // my-app → MAP
        let prefix: String = conn
            .query_row(
                "SELECT prefix FROM brains WHERE brain_id = 'b-app'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(prefix, generate_prefix("my-app"));

        // my-lib → MLB
        let prefix: String = conn
            .query_row(
                "SELECT prefix FROM brains WHERE brain_id = 'b-lib'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(prefix, generate_prefix("my-lib"));
    }

    #[test]
    fn test_task_ids_rewritten() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v22(&conn);
        seed_multi_brain_data(&conn);

        let app_prefix = generate_prefix("my-app");
        let lib_prefix = generate_prefix("my-lib");

        migrate_v22_to_v23(&conn).unwrap();

        // b-app tasks: BRX- → MAP- (or whatever my-app generates)
        let task_id: String = conn
            .query_row(
                "SELECT task_id FROM tasks WHERE title = 'App task 1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            task_id.starts_with(&format!("{app_prefix}-")),
            "expected {app_prefix}-01AAA, got {task_id}"
        );
        assert_eq!(task_id, format!("{app_prefix}-01AAA"));

        // b-lib tasks
        let task_id: String = conn
            .query_row(
                "SELECT task_id FROM tasks WHERE title = 'Lib task'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(task_id, format!("{lib_prefix}-01BBB"));

        // No BRX- tasks remain for scoped brains
        let brx_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM tasks WHERE task_id LIKE 'BRX-%' AND brain_id != ''",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            brx_count, 0,
            "no BRX- tasks should remain for scoped brains"
        );
    }

    #[test]
    fn test_parent_task_id_rewritten() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v22(&conn);
        seed_multi_brain_data(&conn);

        let app_prefix = generate_prefix("my-app");

        migrate_v22_to_v23(&conn).unwrap();

        let parent: Option<String> = conn
            .query_row(
                "SELECT parent_task_id FROM tasks WHERE title = 'App subtask'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(parent, Some(format!("{app_prefix}-01AAA")));
    }

    #[test]
    fn test_task_deps_rewritten() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v22(&conn);
        seed_multi_brain_data(&conn);

        let app_prefix = generate_prefix("my-app");
        let lib_prefix = generate_prefix("my-lib");

        migrate_v22_to_v23(&conn).unwrap();

        let (dep_task, dep_on): (String, String) = conn
            .query_row("SELECT task_id, depends_on FROM task_deps", [], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .unwrap();
        assert_eq!(dep_task, format!("{app_prefix}-01AAB"));
        assert_eq!(dep_on, format!("{lib_prefix}-01BBB"));
    }

    #[test]
    fn test_task_event_payload_rewritten() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v22(&conn);
        seed_multi_brain_data(&conn);

        let app_prefix = generate_prefix("my-app");

        migrate_v22_to_v23(&conn).unwrap();

        let payload: String = conn
            .query_row(
                "SELECT payload FROM task_events WHERE event_id = 'e1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            payload.contains(&format!("{app_prefix}-01AAA")),
            "payload should contain rewritten ID, got: {payload}"
        );
        assert!(
            !payload.contains("BRX-01AAA"),
            "payload should not contain old BRX- ID, got: {payload}"
        );
    }

    #[test]
    fn test_record_ids_rewritten() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v22(&conn);
        seed_multi_brain_data(&conn);

        let app_prefix = generate_prefix("my-app");

        migrate_v22_to_v23(&conn).unwrap();

        let record_id: String = conn
            .query_row(
                "SELECT record_id FROM records WHERE title = 'App record'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(record_id, format!("{app_prefix}-01REC"));

        // No BRX- records remain for scoped brains
        let brx_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM records WHERE record_id LIKE 'BRX-%' AND brain_id != ''",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(brx_count, 0);
    }

    #[test]
    fn test_record_events_rewritten() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v22(&conn);
        seed_multi_brain_data(&conn);

        let app_prefix = generate_prefix("my-app");

        migrate_v22_to_v23(&conn).unwrap();

        let (record_id, payload): (String, String) = conn
            .query_row(
                "SELECT record_id, payload FROM record_events WHERE event_id = 're1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(record_id, format!("{app_prefix}-01REC"));
        assert!(payload.contains(&format!("{app_prefix}-01REC")));
        assert!(!payload.contains("BRX-01REC"));
    }

    #[test]
    fn test_record_tags_rewritten() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v22(&conn);
        seed_multi_brain_data(&conn);

        let app_prefix = generate_prefix("my-app");

        migrate_v22_to_v23(&conn).unwrap();

        let record_id: String = conn
            .query_row(
                "SELECT record_id FROM record_tags WHERE tag = 'test-tag'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(record_id, format!("{app_prefix}-01REC"));
    }

    #[test]
    fn test_unscoped_rows_untouched() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v22(&conn);
        seed_multi_brain_data(&conn);

        migrate_v22_to_v23(&conn).unwrap();

        // Unscoped task keeps BRX- prefix
        let task_id: String = conn
            .query_row(
                "SELECT task_id FROM tasks WHERE title = 'Unscoped task'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(task_id, "BRX-01UNS");
    }

    #[test]
    fn test_legitimate_brx_brain_unchanged() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v22(&conn);

        // "brx" legitimately generates BRX (single word: B + R + X consonants)
        conn.execute(
            "INSERT INTO brains (brain_id, name, created_at) VALUES ('b-brx', 'brx', 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO brains (brain_id, name, created_at) VALUES ('', '(unscoped)', 0)",
            [],
        )
        .unwrap();

        assert_eq!(generate_prefix("brx"), "BRX");

        conn.execute(
            "INSERT INTO tasks (task_id, title, status, brain_id, created_at, updated_at)
             VALUES ('BRX-01LEGIT', 'Legit BRX task', 'open', 'b-brx', 0, 0)",
            [],
        )
        .unwrap();

        migrate_v22_to_v23(&conn).unwrap();

        // BRX is the correct prefix — ID should be unchanged
        let task_id: String = conn
            .query_row(
                "SELECT task_id FROM tasks WHERE title = 'Legit BRX task'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(task_id, "BRX-01LEGIT");
    }

    #[test]
    fn test_empty_tables_migrate_cleanly() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v22(&conn);

        migrate_v22_to_v23(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 23);
    }

    #[test]
    fn test_no_brx_ids_no_rewrite_needed() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v22(&conn);

        conn.execute(
            "INSERT INTO brains (brain_id, name, created_at) VALUES ('b1', 'my-app', 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO brains (brain_id, name, created_at) VALUES ('', '(unscoped)', 0)",
            [],
        )
        .unwrap();

        // Task with correct prefix already
        let app_prefix = generate_prefix("my-app");
        conn.execute(
            &format!(
                "INSERT INTO tasks (task_id, title, status, brain_id, created_at, updated_at)
                 VALUES ('{app_prefix}-01AAA', 'Already correct', 'open', 'b1', 0, 0)"
            ),
            [],
        )
        .unwrap();

        migrate_v22_to_v23(&conn).unwrap();

        let task_id: String = conn
            .query_row(
                "SELECT task_id FROM tasks WHERE title = 'Already correct'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(task_id, format!("{app_prefix}-01AAA"));
    }
}
