//! Saga short-ID generation and resolution.
//!
//! Wraps the shared [`crate::db::short_id`] module (BLAKE3 + collision-extend)
//! with saga-specific concerns: the `saga-` prefix on the user-facing form,
//! and a resolver that accepts either the bare 26-char ULID or the short
//! `saga-<hex>` form.

use std::collections::HashMap;

use rusqlite::{Connection, OptionalExtension};

use crate::error::BrainCoreError;
use crate::sql::SqlResult;

/// Format a saga `display_id` for user-facing emission as `saga-<hex>`.
///
/// Unlike tasks, the saga MCP wire format retains the field NAME `saga_id`
/// even though the value form changes from a bare 26-char ULID to `saga-<hex>`.
/// Tasks renamed `display_id` → `id` on the wire via serde; sagas do not,
/// because saga IDs are global (no brain prefix) and the field name
/// `saga_id` is already type-clarifying enough. Future maintainers: do
/// not unify the two patterns blindly.
pub fn compact_saga_id(display_id: &str) -> String {
    format!("saga-{display_id}")
}

/// Strip the `saga-` prefix from `input` if present; return `None` otherwise.
pub fn parse_short_form(input: &str) -> Option<&str> {
    input.strip_prefix("saga-")
}

/// Resolve a saga reference to its canonical 26-char ULID `saga_id`.
///
/// Accepts exactly two forms:
/// 1. The bare 26-char ULID (matches `saga_id` directly).
/// 2. The `saga-<lowercase hex>` short form (matches `display_id`).
///
/// Partial-prefix matching is intentionally absent: `display_id` values
/// are forced unique by the insert-time collision walk, so `saga-abcd`
/// can only exist if `saga-abc` was already taken at the time it was
/// inserted. Combined with the fact that sagas are never physically
/// deleted (only status-transitioned), there is no realistic state where
/// a partial prefix would resolve unambiguously to a single saga without
/// also matching its predecessor exactly. The simpler two-case resolver
/// is therefore complete.
pub fn resolve_saga_id(conn: &Connection, input: &str) -> SqlResult<String> {
    if input.is_empty() {
        return Err(BrainCoreError::Parse("empty saga id".into()));
    }

    // 1. Exact ULID match.
    if let Some(id) = conn
        .query_row(
            "SELECT saga_id FROM sagas WHERE saga_id = ?1",
            [input],
            |row| row.get::<_, String>(0),
        )
        .optional()?
    {
        return Ok(id);
    }

    // 2. Exact display_id match — input must be `saga-<lowercase hex>`.
    let hex =
        parse_short_form(input).ok_or_else(|| BrainCoreError::SagaNotFound(input.to_string()))?;
    if hex.is_empty()
        || !hex
            .chars()
            .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c))
    {
        return Err(BrainCoreError::Parse(format!(
            "saga short id must be `saga-<lowercase hex>`, got `{input}`"
        ))
        .into());
    }
    conn.query_row(
        "SELECT saga_id FROM sagas WHERE display_id = ?1",
        [hex],
        |row| row.get::<_, String>(0),
    )
    .optional()?
    .ok_or_else(|| BrainCoreError::SagaNotFound(input.to_string()))
}

/// Batch-load short IDs for all sagas. Returns a `saga_id (ULID) → saga-<hex>`
/// map suitable for CLI table rendering where many rows need their short
/// form without a round-trip per row.
pub fn compact_saga_ids(conn: &Connection) -> SqlResult<HashMap<String, String>> {
    let mut stmt = conn.prepare("SELECT saga_id, display_id FROM sagas")?;
    let mut out = HashMap::new();
    for row in stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })? {
        let (saga_id, display_id) = row?;
        out.insert(saga_id, compact_saga_id(&display_id));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrations::{migrate_v52_to_v53, migrate_v53_to_v54};

    fn fresh_v54(conn: &Connection) {
        conn.execute_batch(
            "PRAGMA foreign_keys = ON;
             PRAGMA user_version = 52;",
        )
        .unwrap();
        migrate_v52_to_v53(conn).unwrap();
        migrate_v53_to_v54(conn).unwrap();
    }

    fn insert_saga_with_display_id(conn: &Connection, saga_id: &str, display_id: &str) {
        conn.execute(
            "INSERT INTO sagas (saga_id, title, status, created_at, updated_at, display_id)
             VALUES (?1, ?2, 'planning', 1000, 1000, ?3)",
            rusqlite::params![saga_id, format!("title for {saga_id}"), display_id],
        )
        .unwrap();
    }

    #[test]
    fn compact_formats_with_saga_prefix() {
        assert_eq!(compact_saga_id("abc"), "saga-abc");
        assert_eq!(compact_saga_id("123def"), "saga-123def");
    }

    #[test]
    fn parse_short_form_strips_prefix() {
        assert_eq!(parse_short_form("saga-abc"), Some("abc"));
        assert_eq!(parse_short_form("01KR16ZJRDVNF5D463QMVD9PH0"), None);
        assert_eq!(parse_short_form(""), None);
    }

    #[test]
    fn resolve_accepts_bare_ulid() {
        let conn = Connection::open_in_memory().unwrap();
        fresh_v54(&conn);
        insert_saga_with_display_id(&conn, "01KR16ZJRDVNF5D463QMVD9PH0", "abc");

        let resolved = resolve_saga_id(&conn, "01KR16ZJRDVNF5D463QMVD9PH0").unwrap();
        assert_eq!(resolved, "01KR16ZJRDVNF5D463QMVD9PH0");
    }

    #[test]
    fn resolve_accepts_short_form() {
        let conn = Connection::open_in_memory().unwrap();
        fresh_v54(&conn);
        insert_saga_with_display_id(&conn, "01KR16ZJRDVNF5D463QMVD9PH0", "abc");

        let resolved = resolve_saga_id(&conn, "saga-abc").unwrap();
        assert_eq!(resolved, "01KR16ZJRDVNF5D463QMVD9PH0");
    }

    #[test]
    fn resolve_unknown_returns_saga_not_found() {
        let conn = Connection::open_in_memory().unwrap();
        fresh_v54(&conn);

        let err = resolve_saga_id(&conn, "saga-deadbeef").unwrap_err();
        assert!(matches!(err, BrainCoreError::SagaNotFound(_)));
    }

    #[test]
    fn resolve_malformed_short_form_errors() {
        let conn = Connection::open_in_memory().unwrap();
        fresh_v54(&conn);

        // Uppercase is not valid hex per our convention.
        let err = resolve_saga_id(&conn, "saga-ABC").unwrap_err();
        assert!(matches!(err, BrainCoreError::Parse(_)));

        // Non-hex characters rejected.
        let err = resolve_saga_id(&conn, "saga-xyz").unwrap_err();
        assert!(matches!(err, BrainCoreError::Parse(_)));

        // Empty after prefix.
        let err = resolve_saga_id(&conn, "saga-").unwrap_err();
        assert!(matches!(err, BrainCoreError::Parse(_)));
    }

    #[test]
    fn compact_saga_ids_returns_short_form_map() {
        let conn = Connection::open_in_memory().unwrap();
        fresh_v54(&conn);
        insert_saga_with_display_id(&conn, "01KR16ZJRDVNF5D463QMVD9PH0", "abc");
        insert_saga_with_display_id(&conn, "01KR16ZJRDVNF5D463QMVD9PH1", "def");

        let map = compact_saga_ids(&conn).unwrap();
        assert_eq!(
            map.get("01KR16ZJRDVNF5D463QMVD9PH0"),
            Some(&"saga-abc".to_string())
        );
        assert_eq!(
            map.get("01KR16ZJRDVNF5D463QMVD9PH1"),
            Some(&"saga-def".to_string())
        );
    }
}
