use rusqlite::Connection;

use crate::error::Result;

/// Add `brain_id` column to `files` and `chunks` tables for FTS scoping.
///
/// Files and chunks were previously workspace-global — FTS returned results
/// from all brains regardless of query scope.  This migration adds `brain_id`
/// so that FTS queries can optionally filter by brain, consistent with how
/// tasks, records, and summaries are already scoped.
///
/// Backfill strategy:
/// 1. Match file paths against registered brain roots (from `brains` table).
/// 2. Propagate file brain_id to chunks.
/// 3. Match synthetic task/record files to their owning brain.
pub fn migrate_v36_to_v37(conn: &Connection) -> Result<()> {
    // -- DDL: add columns + indexes ------------------------------------------
    conn.execute_batch(
        "ALTER TABLE files ADD COLUMN brain_id TEXT NOT NULL DEFAULT '';
         ALTER TABLE chunks ADD COLUMN brain_id TEXT NOT NULL DEFAULT '';
         CREATE INDEX IF NOT EXISTS idx_files_brain_id ON files(brain_id);
         CREATE INDEX IF NOT EXISTS idx_chunks_brain_id ON chunks(brain_id);",
    )?;

    // -- Backfill files from brain roots -------------------------------------
    backfill_files_from_brain_roots(conn)?;

    // -- Backfill synthetic task files ---------------------------------------
    conn.execute_batch(
        "UPDATE files SET brain_id = (
             SELECT t.brain_id FROM tasks t
             WHERE files.file_id = 'task:' || t.task_id
         )
         WHERE file_id LIKE 'task:%' AND brain_id = ''
           AND EXISTS (
             SELECT 1 FROM tasks t WHERE files.file_id = 'task:' || t.task_id
         );",
    )?;

    // -- Backfill synthetic task-outcome files --------------------------------
    conn.execute_batch(
        "UPDATE files SET brain_id = (
             SELECT t.brain_id FROM tasks t
             WHERE files.file_id = 'task-outcome:' || t.task_id
         )
         WHERE file_id LIKE 'task-outcome:%' AND brain_id = ''
           AND EXISTS (
             SELECT 1 FROM tasks t WHERE files.file_id = 'task-outcome:' || t.task_id
         );",
    )?;

    // -- Backfill synthetic record files -------------------------------------
    conn.execute_batch(
        "UPDATE files SET brain_id = (
             SELECT r.brain_id FROM records r
             WHERE files.file_id = 'record:' || r.record_id
         )
         WHERE file_id LIKE 'record:%' AND brain_id = ''
           AND EXISTS (
             SELECT 1 FROM records r WHERE files.file_id = 'record:' || r.record_id
         );",
    )?;

    // -- Propagate to chunks -------------------------------------------------
    conn.execute_batch(
        "UPDATE chunks SET brain_id = (
             SELECT f.brain_id FROM files f WHERE f.file_id = chunks.file_id
         )
         WHERE brain_id = '';",
    )?;

    // -- Stamp version -------------------------------------------------------
    conn.execute_batch("PRAGMA user_version = 37;")?;

    Ok(())
}

/// Match file paths against each brain's registered root paths.
///
/// For each brain row with non-empty roots JSON, parse the roots array and
/// update files whose path starts with that root.  Uses longest-prefix-wins
/// ordering (longer roots matched first) to handle nested roots correctly.
fn backfill_files_from_brain_roots(conn: &Connection) -> Result<()> {
    // Load all brains with their roots.  Each entry is (brain_id, single_root).
    let mut stmt = conn
        .prepare("SELECT brain_id, roots FROM brains WHERE brain_id != '' AND roots IS NOT NULL")?;
    let mut brain_roots: Vec<(String, String)> = Vec::new();
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let brain_id: String = row.get(0)?;
        let roots_json: String = row.get(1)?;
        if let Ok(roots) = serde_json::from_str::<Vec<String>>(&roots_json) {
            for root in roots {
                brain_roots.push((brain_id.clone(), root));
            }
        }
    }

    // Sort by root path length descending (longest prefix first).
    brain_roots.sort_by(|a, b| b.1.len().cmp(&a.1.len()));

    // Update files for each root, skipping already-assigned files.
    let mut update_stmt = conn.prepare(
        "UPDATE files SET brain_id = ?1
         WHERE path LIKE ?2 AND brain_id = ''
           AND file_id NOT LIKE 'task:%'
           AND file_id NOT LIKE 'record:%'",
    )?;

    for (brain_id, root) in &brain_roots {
        // Ensure the LIKE pattern matches paths under this root.
        let pattern = if root.ends_with('/') {
            format!("{root}%")
        } else {
            format!("{root}/%")
        };
        update_stmt.execute(rusqlite::params![brain_id, pattern])?;

        // Also match the root directory itself (exact path match).
        conn.execute(
            "UPDATE files SET brain_id = ?1 WHERE path = ?2 AND brain_id = ''",
            rusqlite::params![brain_id, root],
        )?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Bootstrap a v36 database with the tables needed by this migration:
    /// files, chunks, brains, tasks, records.
    fn setup_v36(conn: &Connection) {
        conn.execute_batch(
            "PRAGMA foreign_keys = OFF;

             CREATE TABLE files (
                 file_id        TEXT PRIMARY KEY,
                 path           TEXT UNIQUE NOT NULL,
                 content_hash   TEXT,
                 last_indexed_at INTEGER,
                 deleted_at     INTEGER,
                 indexing_state  TEXT NOT NULL DEFAULT 'idle',
                 chunker_version INTEGER,
                 pagerank_score  REAL
             );

             CREATE TABLE chunks (
                 chunk_id        TEXT PRIMARY KEY,
                 file_id         TEXT NOT NULL REFERENCES files(file_id) ON DELETE CASCADE,
                 chunk_ord       INTEGER NOT NULL,
                 chunk_hash      TEXT NOT NULL,
                 content         TEXT NOT NULL DEFAULT '',
                 chunker_version INTEGER NOT NULL DEFAULT 1,
                 heading_path    TEXT NOT NULL DEFAULT '',
                 byte_start      INTEGER NOT NULL DEFAULT 0,
                 byte_end        INTEGER NOT NULL DEFAULT 0,
                 token_estimate  INTEGER NOT NULL DEFAULT 0
             );

             CREATE TABLE brains (
                 brain_id   TEXT PRIMARY KEY,
                 name       TEXT NOT NULL UNIQUE,
                 prefix     TEXT,
                 created_at INTEGER NOT NULL DEFAULT 0,
                 archived   INTEGER NOT NULL DEFAULT 0,
                 roots      TEXT,
                 aliases    TEXT,
                 notes      TEXT,
                 projected  INTEGER NOT NULL DEFAULT 0
             );

             CREATE TABLE tasks (
                 task_id    TEXT PRIMARY KEY,
                 brain_id   TEXT NOT NULL DEFAULT '',
                 title      TEXT NOT NULL,
                 status     TEXT NOT NULL DEFAULT 'open',
                 priority   INTEGER NOT NULL DEFAULT 4,
                 task_type  TEXT NOT NULL DEFAULT 'task',
                 created_at INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL
             );

             CREATE TABLE records (
                 record_id  TEXT PRIMARY KEY,
                 brain_id   TEXT NOT NULL DEFAULT '',
                 kind       TEXT NOT NULL DEFAULT 'artifact',
                 title      TEXT NOT NULL DEFAULT '',
                 status     TEXT NOT NULL DEFAULT 'active',
                 created_at INTEGER NOT NULL DEFAULT 0,
                 updated_at INTEGER NOT NULL DEFAULT 0
             );

             PRAGMA user_version = 36;
             PRAGMA foreign_keys = ON;",
        )
        .unwrap();
    }

    #[test]
    fn test_version_stamped() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v36(&conn);

        migrate_v36_to_v37(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 37);
    }

    #[test]
    fn test_brain_id_columns_exist() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v36(&conn);

        migrate_v36_to_v37(&conn).unwrap();

        conn.execute(
            "INSERT INTO files (file_id, path, brain_id) VALUES ('f1', '/test/file.md', 'brain-a')",
            [],
        )
        .unwrap();

        conn.execute(
            "INSERT INTO chunks (chunk_id, file_id, chunk_ord, chunk_hash, content, brain_id)
             VALUES ('c1', 'f1', 0, 'h1', 'test content', 'brain-a')",
            [],
        )
        .unwrap();

        let brain_id: String = conn
            .query_row(
                "SELECT brain_id FROM files WHERE file_id = 'f1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(brain_id, "brain-a");

        let chunk_brain_id: String = conn
            .query_row(
                "SELECT brain_id FROM chunks WHERE chunk_id = 'c1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(chunk_brain_id, "brain-a");
    }

    #[test]
    fn test_backfill_from_brain_roots() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v36(&conn);

        // Register a brain with roots before migration.
        conn.execute(
            "INSERT INTO brains (brain_id, name, prefix, roots)
             VALUES ('bid-1', 'project-a', 'PA', ?1)",
            [serde_json::json!(["/home/user/project-a"]).to_string()],
        )
        .unwrap();

        // Insert files: one under the root, one unrelated.
        conn.execute(
            "INSERT INTO files (file_id, path) VALUES ('f1', '/home/user/project-a/notes/readme.md')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO files (file_id, path) VALUES ('f2', '/home/user/other/unrelated.md')",
            [],
        )
        .unwrap();

        // Insert a chunk for f1.
        conn.execute(
            "INSERT INTO chunks (chunk_id, file_id, chunk_ord, chunk_hash, content)
             VALUES ('c1', 'f1', 0, 'h1', 'hello')",
            [],
        )
        .unwrap();

        migrate_v36_to_v37(&conn).unwrap();

        let f1_brain: String = conn
            .query_row(
                "SELECT brain_id FROM files WHERE file_id = 'f1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(f1_brain, "bid-1");

        let f2_brain: String = conn
            .query_row(
                "SELECT brain_id FROM files WHERE file_id = 'f2'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(f2_brain, "");

        let c1_brain: String = conn
            .query_row(
                "SELECT brain_id FROM chunks WHERE chunk_id = 'c1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(c1_brain, "bid-1");
    }

    #[test]
    fn test_backfill_synthetic_task_files() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v36(&conn);

        conn.execute(
            "INSERT INTO brains (brain_id, name, prefix) VALUES ('bid-1', 'proj', 'PR')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO tasks (task_id, brain_id, title, status, priority, task_type, created_at, updated_at)
             VALUES ('PR-01ABC', 'bid-1', 'Test task', 'open', 2, 'task', 1000, 1000)",
            [],
        )
        .unwrap();

        // Synthetic file for that task.
        conn.execute(
            "INSERT INTO files (file_id, path) VALUES ('task:PR-01ABC', 'task:PR-01ABC')",
            [],
        )
        .unwrap();

        migrate_v36_to_v37(&conn).unwrap();

        let brain: String = conn
            .query_row(
                "SELECT brain_id FROM files WHERE file_id = 'task:PR-01ABC'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(brain, "bid-1");
    }

    #[test]
    fn test_backfill_synthetic_task_outcome_files() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v36(&conn);

        conn.execute(
            "INSERT INTO brains (brain_id, name, prefix) VALUES ('bid-1', 'proj', 'PR')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO tasks (task_id, brain_id, title, status, priority, task_type, created_at, updated_at)
             VALUES ('PR-01ABC', 'bid-1', 'Done task', 'done', 2, 'task', 1000, 1000)",
            [],
        )
        .unwrap();

        // Synthetic outcome file for that task.
        conn.execute(
            "INSERT INTO files (file_id, path) VALUES ('task-outcome:PR-01ABC', 'task-outcome:PR-01ABC')",
            [],
        )
        .unwrap();

        migrate_v36_to_v37(&conn).unwrap();

        let brain: String = conn
            .query_row(
                "SELECT brain_id FROM files WHERE file_id = 'task-outcome:PR-01ABC'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(brain, "bid-1");
    }
}
