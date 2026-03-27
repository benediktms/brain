use rusqlite::Connection;

use crate::error::Result;

/// v25 → v26: Add `roots`, `aliases`, `notes`, and `projected` columns to `brains` table.
///
/// New columns:
/// - `roots TEXT` — JSON array of root paths (nullable); populated by `project_config_to_brains`
/// - `aliases TEXT` — JSON array of alias strings (nullable); populated by `project_config_to_brains`
/// - `notes TEXT` — JSON array of note dir paths (nullable); populated by `project_config_to_brains`
/// - `projected INTEGER NOT NULL DEFAULT 0` — 1 = row is current in state projection; 0 = stale/ensure_brain_registered only
///
/// All new columns are nullable except `projected` (which defaults to 0 for existing rows).
/// `ensure_brain_registered` does not populate roots/aliases/notes/projected.
pub fn migrate_v25_to_v26(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "ALTER TABLE brains ADD COLUMN roots TEXT;
         ALTER TABLE brains ADD COLUMN aliases TEXT;
         ALTER TABLE brains ADD COLUMN notes TEXT;
         ALTER TABLE brains ADD COLUMN projected INTEGER NOT NULL DEFAULT 0;
         PRAGMA user_version = 26;",
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a v25 schema — brains table with archived column but no roots/aliases/notes/projected.
    fn setup_v25(conn: &Connection) {
        conn.execute_batch(
            "PRAGMA foreign_keys = OFF;

             CREATE TABLE brains (
                 brain_id   TEXT PRIMARY KEY,
                 name       TEXT NOT NULL UNIQUE,
                 prefix     TEXT,
                 created_at INTEGER NOT NULL,
                 archived   INTEGER NOT NULL DEFAULT 0
             );

             PRAGMA user_version = 25;
             PRAGMA foreign_keys = ON;",
        )
        .unwrap();
    }

    #[test]
    fn test_version_stamped() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v25(&conn);

        migrate_v25_to_v26(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 26);
    }

    #[test]
    fn test_new_columns_exist_with_correct_defaults() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v25(&conn);

        // Insert a row before migration to verify backfill defaults
        conn.execute(
            "INSERT INTO brains (brain_id, name, prefix, created_at) VALUES ('b1', 'test', 'TST', 1000)",
            [],
        )
        .unwrap();

        migrate_v25_to_v26(&conn).unwrap();

        let (roots, aliases, notes, projected): (
            Option<String>,
            Option<String>,
            Option<String>,
            i32,
        ) = conn
            .query_row(
                "SELECT roots, aliases, notes, projected FROM brains WHERE brain_id = 'b1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();

        assert!(roots.is_none(), "existing row roots should default to NULL");
        assert!(
            aliases.is_none(),
            "existing row aliases should default to NULL"
        );
        assert!(notes.is_none(), "existing row notes should default to NULL");
        assert_eq!(projected, 0, "existing row projected should default to 0");
    }

    #[test]
    fn test_new_columns_accept_json_values() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v25(&conn);
        migrate_v25_to_v26(&conn).unwrap();

        conn.execute(
            "INSERT INTO brains (brain_id, name, prefix, created_at, roots, aliases, notes, projected)
             VALUES ('b2', 'mybrain', 'MYB', 2000, '[\"/home/user\"]', '[\"mb\"]', '[\"/notes\"]', 1)",
            [],
        )
        .unwrap();

        let (roots, aliases, notes, projected): (String, String, String, i32) = conn
            .query_row(
                "SELECT roots, aliases, notes, projected FROM brains WHERE brain_id = 'b2'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();

        assert_eq!(roots, "[\"/home/user\"]");
        assert_eq!(aliases, "[\"mb\"]");
        assert_eq!(notes, "[\"/notes\"]");
        assert_eq!(projected, 1);
    }

    #[test]
    fn test_empty_table_migrates_cleanly() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v25(&conn);
        migrate_v25_to_v26(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 26);
    }
}
