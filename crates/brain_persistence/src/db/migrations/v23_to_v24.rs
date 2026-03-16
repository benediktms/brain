use rusqlite::Connection;

use crate::error::Result;

/// v23 → v24: Add `archived` column to `brains` table.
///
/// `ALTER TABLE brains ADD COLUMN archived INTEGER NOT NULL DEFAULT 0`
///
/// The column stores a boolean flag (0 = active, 1 = archived) indicating
/// whether the brain has been archived. SQLite's ALTER TABLE requires a
/// DEFAULT value for NOT NULL columns added to existing tables.
pub fn migrate_v23_to_v24(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "ALTER TABLE brains ADD COLUMN archived INTEGER NOT NULL DEFAULT 0;
         PRAGMA user_version = 24;",
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a v23 schema — brains table with prefix column but no archived column.
    fn setup_v23(conn: &Connection) {
        conn.execute_batch(
            "PRAGMA foreign_keys = OFF;

             CREATE TABLE brains (
                 brain_id   TEXT PRIMARY KEY,
                 name       TEXT NOT NULL UNIQUE,
                 prefix     TEXT,
                 created_at INTEGER NOT NULL
             );

             PRAGMA user_version = 23;
             PRAGMA foreign_keys = ON;",
        )
        .unwrap();
    }

    #[test]
    fn test_version_stamped() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v23(&conn);

        migrate_v23_to_v24(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 24);
    }

    #[test]
    fn test_archived_column_exists_and_defaults_to_zero() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v23(&conn);

        conn.execute(
            "INSERT INTO brains (brain_id, name, prefix, created_at) VALUES ('b1', 'test', 'TST', 0)",
            [],
        )
        .unwrap();

        migrate_v23_to_v24(&conn).unwrap();

        // Row inserted before migration should default archived = 0
        let archived: i64 = conn
            .query_row(
                "SELECT archived FROM brains WHERE brain_id = 'b1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(archived, 0, "existing row should default archived to 0");
    }

    #[test]
    fn test_archived_column_settable() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v23(&conn);

        conn.execute(
            "INSERT INTO brains (brain_id, name, prefix, created_at) VALUES ('b2', 'another', 'ANO', 0)",
            [],
        )
        .unwrap();

        migrate_v23_to_v24(&conn).unwrap();

        // After migration we can set archived = 1
        conn.execute("UPDATE brains SET archived = 1 WHERE brain_id = 'b2'", [])
            .unwrap();

        let archived: i64 = conn
            .query_row(
                "SELECT archived FROM brains WHERE brain_id = 'b2'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(archived, 1, "archived flag should be settable to 1");
    }

    #[test]
    fn test_empty_table_migrates_cleanly() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v23(&conn);

        migrate_v23_to_v24(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 24);
    }
}
