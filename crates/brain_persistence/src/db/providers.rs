//! CRUD operations for the `providers` table.

use rusqlite::{Connection, params};

use crate::error::Result;

/// A provider row as returned from queries.
#[derive(Debug, Clone)]
pub struct ProviderRow {
    pub id: String,
    pub name: String,
    /// AES-256-GCM encrypted API key (base64-encoded nonce || ciphertext).
    pub api_key: String,
    pub api_key_hash: String,
    pub created_at: i64,
    pub updated_at: i64,
}

/// Input for inserting a new provider.
pub struct InsertProvider<'a> {
    pub name: &'a str,
    /// Already-encrypted API key (base64).
    pub api_key_encrypted: &'a str,
    /// blake3 hash of the plaintext key.
    pub api_key_hash: &'a str,
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before UNIX epoch")
        .as_secs() as i64
}

/// Insert a new provider. Returns the generated ID.
pub fn insert_provider(conn: &Connection, input: &InsertProvider) -> Result<String> {
    let id = ulid::Ulid::new().to_string();
    let now = now_secs();
    conn.execute(
        "INSERT INTO providers (id, name, api_key, api_key_hash, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?5)",
        params![
            id,
            input.name,
            input.api_key_encrypted,
            input.api_key_hash,
            now
        ],
    )?;
    Ok(id)
}

/// Get a provider by ID.
pub fn get_provider(conn: &Connection, id: &str) -> Result<Option<ProviderRow>> {
    let mut stmt = conn.prepare(
        "SELECT id, name, api_key, api_key_hash, created_at, updated_at
         FROM providers WHERE id = ?1",
    )?;
    let mut rows = stmt.query_map(params![id], row_to_provider)?;
    match rows.next() {
        Some(row) => Ok(Some(row?)),
        None => Ok(None),
    }
}

/// Get the first provider matching a given name (e.g. 'anthropic').
/// Returns the most recently updated entry if multiple exist.
pub fn get_provider_by_name(conn: &Connection, name: &str) -> Result<Option<ProviderRow>> {
    let mut stmt = conn.prepare(
        "SELECT id, name, api_key, api_key_hash, created_at, updated_at
         FROM providers WHERE name = ?1
         ORDER BY updated_at DESC
         LIMIT 1",
    )?;
    let mut rows = stmt.query_map(params![name], row_to_provider)?;
    match rows.next() {
        Some(row) => Ok(Some(row?)),
        None => Ok(None),
    }
}

/// List all providers (id and name only — keys are not included in the output
/// for safety; use `get_provider` to fetch the encrypted key when needed).
pub fn list_providers(conn: &Connection) -> Result<Vec<ProviderRow>> {
    let mut stmt = conn.prepare(
        "SELECT id, name, api_key, api_key_hash, created_at, updated_at
         FROM providers ORDER BY name, updated_at DESC",
    )?;
    let rows = stmt
        .query_map([], row_to_provider)?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(rows)
}

/// Delete a provider by ID. Returns true if a row was deleted.
pub fn delete_provider(conn: &Connection, id: &str) -> Result<bool> {
    let rows = conn.execute("DELETE FROM providers WHERE id = ?1", params![id])?;
    Ok(rows > 0)
}

/// Delete a provider by name. Returns the number of rows deleted.
pub fn delete_provider_by_name(conn: &Connection, name: &str) -> Result<usize> {
    let rows = conn.execute("DELETE FROM providers WHERE name = ?1", params![name])?;
    Ok(rows)
}

/// Check if a provider with the given name and key hash already exists.
pub fn provider_exists(conn: &Connection, name: &str, api_key_hash: &str) -> Result<bool> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM providers WHERE name = ?1 AND api_key_hash = ?2",
        params![name, api_key_hash],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

/// Update the encrypted API key for a provider by ID. Returns true if updated.
pub fn update_provider_key(
    conn: &Connection,
    id: &str,
    api_key_encrypted: &str,
    api_key_hash: &str,
) -> Result<bool> {
    let now = now_secs();
    let rows = conn.execute(
        "UPDATE providers SET api_key = ?1, api_key_hash = ?2, updated_at = ?3
         WHERE id = ?4",
        params![api_key_encrypted, api_key_hash, now, id],
    )?;
    Ok(rows > 0)
}

fn row_to_provider(row: &rusqlite::Row) -> rusqlite::Result<ProviderRow> {
    Ok(ProviderRow {
        id: row.get(0)?,
        name: row.get(1)?,
        api_key: row.get(2)?,
        api_key_hash: row.get(3)?,
        created_at: row.get(4)?,
        updated_at: row.get(5)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE providers (
                id              TEXT PRIMARY KEY,
                name            TEXT NOT NULL CHECK (name IN ('anthropic', 'openai')),
                api_key         TEXT NOT NULL,
                api_key_hash    TEXT NOT NULL,
                created_at      INTEGER NOT NULL,
                updated_at      INTEGER NOT NULL
            );
            CREATE UNIQUE INDEX idx_providers_name_key ON providers(name, api_key_hash);",
        )
        .unwrap();
        conn
    }

    #[test]
    fn test_insert_and_get() {
        let conn = setup_db();
        let id = insert_provider(
            &conn,
            &InsertProvider {
                name: "anthropic",
                api_key_encrypted: "enc-data",
                api_key_hash: "hash123",
            },
        )
        .unwrap();

        let row = get_provider(&conn, &id).unwrap().unwrap();
        assert_eq!(row.name, "anthropic");
        assert_eq!(row.api_key, "enc-data");
        assert_eq!(row.api_key_hash, "hash123");
    }

    #[test]
    fn test_get_by_name() {
        let conn = setup_db();
        insert_provider(
            &conn,
            &InsertProvider {
                name: "openai",
                api_key_encrypted: "enc-key",
                api_key_hash: "hash-oa",
            },
        )
        .unwrap();

        let row = get_provider_by_name(&conn, "openai").unwrap().unwrap();
        assert_eq!(row.name, "openai");

        assert!(get_provider_by_name(&conn, "anthropic").unwrap().is_none());
    }

    #[test]
    fn test_list_providers() {
        let conn = setup_db();
        insert_provider(
            &conn,
            &InsertProvider {
                name: "anthropic",
                api_key_encrypted: "enc1",
                api_key_hash: "h1",
            },
        )
        .unwrap();
        insert_provider(
            &conn,
            &InsertProvider {
                name: "openai",
                api_key_encrypted: "enc2",
                api_key_hash: "h2",
            },
        )
        .unwrap();

        let list = list_providers(&conn).unwrap();
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn test_delete_provider() {
        let conn = setup_db();
        let id = insert_provider(
            &conn,
            &InsertProvider {
                name: "anthropic",
                api_key_encrypted: "enc",
                api_key_hash: "h",
            },
        )
        .unwrap();

        assert!(delete_provider(&conn, &id).unwrap());
        assert!(get_provider(&conn, &id).unwrap().is_none());
    }

    #[test]
    fn test_delete_by_name() {
        let conn = setup_db();
        insert_provider(
            &conn,
            &InsertProvider {
                name: "openai",
                api_key_encrypted: "enc1",
                api_key_hash: "h1",
            },
        )
        .unwrap();
        insert_provider(
            &conn,
            &InsertProvider {
                name: "openai",
                api_key_encrypted: "enc2",
                api_key_hash: "h2",
            },
        )
        .unwrap();

        let deleted = delete_provider_by_name(&conn, "openai").unwrap();
        assert_eq!(deleted, 2);
    }

    #[test]
    fn test_provider_exists() {
        let conn = setup_db();
        insert_provider(
            &conn,
            &InsertProvider {
                name: "anthropic",
                api_key_encrypted: "enc",
                api_key_hash: "hash-x",
            },
        )
        .unwrap();

        assert!(provider_exists(&conn, "anthropic", "hash-x").unwrap());
        assert!(!provider_exists(&conn, "anthropic", "other-hash").unwrap());
        assert!(!provider_exists(&conn, "openai", "hash-x").unwrap());
    }

    #[test]
    fn test_update_provider_key() {
        let conn = setup_db();
        let id = insert_provider(
            &conn,
            &InsertProvider {
                name: "anthropic",
                api_key_encrypted: "old-enc",
                api_key_hash: "old-hash",
            },
        )
        .unwrap();

        assert!(update_provider_key(&conn, &id, "new-enc", "new-hash").unwrap());

        let row = get_provider(&conn, &id).unwrap().unwrap();
        assert_eq!(row.api_key, "new-enc");
        assert_eq!(row.api_key_hash, "new-hash");
    }

    #[test]
    fn test_duplicate_name_and_hash_rejected() {
        let conn = setup_db();
        insert_provider(
            &conn,
            &InsertProvider {
                name: "anthropic",
                api_key_encrypted: "enc1",
                api_key_hash: "same-hash",
            },
        )
        .unwrap();

        let result = insert_provider(
            &conn,
            &InsertProvider {
                name: "anthropic",
                api_key_encrypted: "enc2",
                api_key_hash: "same-hash",
            },
        );
        assert!(result.is_err());
    }
}
