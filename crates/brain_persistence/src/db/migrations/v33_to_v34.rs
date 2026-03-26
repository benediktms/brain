use rusqlite::Connection;

use crate::error::Result;

/// Migration v33 → v34: add `providers` table for LLM API key storage.
///
/// Stores encrypted API keys with a blake3 hash for uniqueness enforcement.
/// The `api_key` column holds AES-256-GCM encrypted bytes (base64-encoded).
/// The `api_key_hash` column holds a blake3 hash of the plaintext key for
/// deduplication without exposing the key.
pub fn migrate_v33_to_v34(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        PRAGMA foreign_keys = OFF;
        BEGIN;

        CREATE TABLE providers (
            id              TEXT PRIMARY KEY,
            name            TEXT NOT NULL CHECK (name IN ('anthropic', 'openai')),
            api_key         TEXT NOT NULL,
            api_key_hash    TEXT NOT NULL,
            created_at      INTEGER NOT NULL,
            updated_at      INTEGER NOT NULL
        );

        CREATE UNIQUE INDEX idx_providers_name_key ON providers(name, api_key_hash);

        COMMIT;
        PRAGMA foreign_keys = ON;
        PRAGMA user_version = 34;
        ",
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use super::*;

    fn setup_v33() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA user_version = 33;").unwrap();
        conn
    }

    #[test]
    fn test_migration_stamps_version_34() {
        let conn = setup_v33();
        migrate_v33_to_v34(&conn).unwrap();
        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 34);
    }

    #[test]
    fn test_providers_table_exists() {
        let conn = setup_v33();
        migrate_v33_to_v34(&conn).unwrap();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='providers'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_unique_index_prevents_duplicate_key() {
        let conn = setup_v33();
        migrate_v33_to_v34(&conn).unwrap();

        conn.execute(
            "INSERT INTO providers (id, name, api_key, api_key_hash, created_at, updated_at)
             VALUES ('p1', 'anthropic', 'enc1', 'hash1', 1000, 1000)",
            [],
        )
        .unwrap();

        let result = conn.execute(
            "INSERT INTO providers (id, name, api_key, api_key_hash, created_at, updated_at)
             VALUES ('p2', 'anthropic', 'enc2', 'hash1', 1000, 1000)",
            [],
        );
        assert!(
            result.is_err(),
            "duplicate (name, api_key_hash) should fail"
        );
    }

    #[test]
    fn test_same_key_different_provider_ok() {
        let conn = setup_v33();
        migrate_v33_to_v34(&conn).unwrap();

        conn.execute(
            "INSERT INTO providers (id, name, api_key, api_key_hash, created_at, updated_at)
             VALUES ('p1', 'anthropic', 'enc1', 'hash1', 1000, 1000)",
            [],
        )
        .unwrap();

        // Same hash but different provider name is allowed
        conn.execute(
            "INSERT INTO providers (id, name, api_key, api_key_hash, created_at, updated_at)
             VALUES ('p2', 'openai', 'enc2', 'hash1', 1000, 1000)",
            [],
        )
        .unwrap();
    }

    #[test]
    fn test_name_check_constraint() {
        let conn = setup_v33();
        migrate_v33_to_v34(&conn).unwrap();

        let result = conn.execute(
            "INSERT INTO providers (id, name, api_key, api_key_hash, created_at, updated_at)
             VALUES ('p1', 'invalid_provider', 'enc1', 'hash1', 1000, 1000)",
            [],
        );
        assert!(result.is_err(), "invalid provider name should fail CHECK");
    }
}
