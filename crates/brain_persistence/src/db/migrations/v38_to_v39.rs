use rusqlite::Connection;

use crate::error::Result;

/// v38 → v39: Add `disk_modified_at` column to the `files` table.
///
/// Stores the file's OS-level modification timestamp (Unix seconds) captured
/// during indexing.  This is the *real* edit time — as opposed to
/// `last_indexed_at` which records when Brain processed the file.
///
/// The column is nullable: existing rows default to NULL and the query
/// pipeline falls back to `last_indexed_at` when `disk_modified_at` is NULL.
pub fn migrate_v38_to_v39(conn: &Connection) -> Result<()> {
    conn.execute("ALTER TABLE files ADD COLUMN disk_modified_at INTEGER", [])?;

    conn.execute("PRAGMA user_version = 39", [])?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_v38(conn: &Connection) {
        conn.execute_batch(
            "CREATE TABLE files (
                 file_id        TEXT PRIMARY KEY,
                 path           TEXT NOT NULL UNIQUE,
                 content_hash   TEXT,
                 last_indexed_at INTEGER,
                 deleted_at     INTEGER,
                 indexing_state  TEXT NOT NULL DEFAULT 'idle',
                 chunker_version INTEGER,
                 pagerank_score  REAL,
                 brain_id       TEXT NOT NULL DEFAULT ''
             );

             PRAGMA user_version = 38;",
        )
        .unwrap();
    }

    #[test]
    fn test_version_stamped() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v38(&conn);

        migrate_v38_to_v39(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 39);
    }

    #[test]
    fn test_column_exists_after_migration() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v38(&conn);

        migrate_v38_to_v39(&conn).unwrap();

        // Insert a row with the new column
        conn.execute(
            "INSERT INTO files (file_id, path, disk_modified_at) VALUES ('f1', '/test.md', 1700000000)",
            [],
        )
        .unwrap();

        let mtime: Option<i64> = conn
            .query_row(
                "SELECT disk_modified_at FROM files WHERE file_id = 'f1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(mtime, Some(1700000000));
    }

    #[test]
    fn test_existing_rows_get_null() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v38(&conn);

        conn.execute(
            "INSERT INTO files (file_id, path, last_indexed_at) VALUES ('f1', '/old.md', 1600000000)",
            [],
        )
        .unwrap();

        migrate_v38_to_v39(&conn).unwrap();

        let mtime: Option<i64> = conn
            .query_row(
                "SELECT disk_modified_at FROM files WHERE file_id = 'f1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            mtime, None,
            "existing rows should have NULL disk_modified_at"
        );
    }
}
