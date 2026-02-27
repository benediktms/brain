use rusqlite::Connection;

use crate::error::Result;

const SCHEMA_VERSION: i64 = 1;

/// Initialize the database schema: WAL mode, foreign keys, and all tables.
pub fn init_schema(conn: &Connection) -> Result<()> {
    // Enable WAL mode for concurrent reads during writes
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "foreign_keys", "ON")?;

    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS metadata (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS files (
            file_id         TEXT PRIMARY KEY,
            path            TEXT UNIQUE NOT NULL,
            content_hash    TEXT,
            mtime           INTEGER,
            size            INTEGER,
            last_indexed_at INTEGER,
            deleted_at      INTEGER,
            indexing_state  TEXT NOT NULL DEFAULT 'idle'
        );

        CREATE TABLE IF NOT EXISTS chunks (
            chunk_id   TEXT PRIMARY KEY,
            file_id    TEXT NOT NULL REFERENCES files(file_id) ON DELETE CASCADE,
            chunk_ord  INTEGER NOT NULL,
            chunk_hash TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_chunks_file_id ON chunks(file_id);
        ",
    )?;

    // Set schema version if not already present
    let existing: Option<String> = conn
        .query_row(
            "SELECT value FROM metadata WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .ok();

    if existing.is_none() {
        conn.execute(
            "INSERT INTO metadata (key, value) VALUES ('schema_version', ?1)",
            [SCHEMA_VERSION.to_string()],
        )?;
    }

    Ok(())
}

/// Returns the current schema version, or None if the database is uninitialized.
pub fn schema_version(conn: &Connection) -> Result<Option<i64>> {
    // Check if metadata table exists first
    let table_exists: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='metadata')",
        [],
        |row| row.get(0),
    )?;

    if !table_exists {
        return Ok(None);
    }

    let version: Option<String> = conn
        .query_row(
            "SELECT value FROM metadata WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .ok();

    Ok(version.and_then(|v| v.parse().ok()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    #[test]
    fn test_init_schema_creates_tables() {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();

        // Verify WAL mode
        let journal_mode: String = conn
            .pragma_query_value(None, "journal_mode", |row| row.get(0))
            .unwrap();
        // In-memory databases report "memory" for journal_mode
        assert!(
            journal_mode == "wal" || journal_mode == "memory",
            "expected wal or memory, got {journal_mode}"
        );

        // Verify tables exist
        let tables: Vec<String> = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();

        assert!(tables.contains(&"metadata".to_string()));
        assert!(tables.contains(&"files".to_string()));
        assert!(tables.contains(&"chunks".to_string()));

        // Verify schema version
        let version = schema_version(&conn).unwrap();
        assert_eq!(version, Some(1));
    }

    #[test]
    fn test_init_schema_idempotent() {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();
        init_schema(&conn).unwrap(); // second call should not fail

        let version = schema_version(&conn).unwrap();
        assert_eq!(version, Some(1));
    }

    #[test]
    fn test_schema_version_uninitialized() {
        let conn = Connection::open_in_memory().unwrap();
        let version = schema_version(&conn).unwrap();
        assert_eq!(version, None);
    }
}
