//! Hierarchy summaries: directory and tag scope aggregation.
//!
//! This module provides types and functions for generating and querying
//! derived summaries scoped to a directory path or tag. Summaries are
//! extractive aggregations of chunk content for a given scope, stored in the
//! `derived_summaries` table and indexed for full-text search.

use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::params;
use ulid::Ulid;

use brain_persistence::db::Db;
use brain_persistence::error::{BrainCoreError, Result};

// ─── Types ────────────────────────────────────────────────────────────────────

/// Scope discriminant for a derived summary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScopeType {
    /// A directory path scope, e.g. `src/auth/`.
    Directory,
    /// A tag scope, e.g. `rust`.
    Tag,
}

impl ScopeType {
    /// Canonical string representation stored in the DB.
    pub fn as_str(&self) -> &'static str {
        match self {
            ScopeType::Directory => "directory",
            ScopeType::Tag => "tag",
        }
    }
}

/// A derived summary row returned from the `derived_summaries` table.
#[derive(Debug, Clone)]
pub struct DerivedSummary {
    /// Auto-assigned row identifier (ULID string).
    pub id: String,
    /// Scope discriminant: "directory" or "tag".
    pub scope_type: String,
    /// The directory path or tag name this summary describes.
    pub scope_value: String,
    /// Extractive summary text derived from chunks matching the scope.
    pub content: String,
    /// When `true`, the summary is out of date and must be regenerated.
    pub stale: bool,
    /// Unix timestamp (seconds) when this summary was generated.
    pub generated_at: i64,
}

// ─── Implementation ───────────────────────────────────────────────────────────

/// Generate and persist a derived summary for the given scope.
///
/// Collects all chunk content matching the scope, truncates each chunk to
/// 200 characters, joins them with newlines, and persists the result as an
/// extractive summary via `INSERT OR REPLACE` into `derived_summaries`.
///
/// # Arguments
/// * `db`          — open database handle
/// * `scope_type`  — whether the scope is a directory or tag
/// * `scope_value` — the concrete path or tag name
///
/// # Returns
/// The newly assigned summary `id` on success.
pub fn generate_scope_summary(
    db: &Db,
    scope_type: &ScopeType,
    scope_value: &str,
) -> Result<String> {
    let scope_type_str = scope_type.as_str();
    let scope_value_owned = scope_value.to_string();

    // Collect chunk content matching the scope.
    let contents: Vec<String> = db.with_read_conn(|conn| {
        let rows: Vec<String> = match scope_type {
            ScopeType::Directory => {
                let pattern = format!("{}%", scope_value_owned);
                let mut stmt = conn.prepare(
                    "SELECT c.content
                     FROM chunks c
                     JOIN files f ON c.file_id = f.file_id
                     WHERE f.path LIKE ?1
                     ORDER BY f.path, c.chunk_ord",
                )?;
                stmt.query_map(params![pattern], |row| row.get(0))
                    .map_err(|e| BrainCoreError::Database(e.to_string()))?
                    .collect::<std::result::Result<Vec<String>, _>>()
                    .map_err(|e| BrainCoreError::Database(e.to_string()))?
            }
            ScopeType::Tag => {
                let pattern = format!("%{}%", scope_value_owned);
                let mut stmt = conn.prepare(
                    "SELECT content
                     FROM summaries
                     WHERE tags LIKE ?1
                     ORDER BY created_at",
                )?;
                stmt.query_map(params![pattern], |row| row.get(0))
                    .map_err(|e| BrainCoreError::Database(e.to_string()))?
                    .collect::<std::result::Result<Vec<String>, _>>()
                    .map_err(|e| BrainCoreError::Database(e.to_string()))?
            }
        };
        Ok(rows)
    })?;

    // Extractive summary: truncate each chunk to 200 chars, join with newlines.
    let summary_content: String = contents
        .iter()
        .map(|c| {
            if c.len() > 200 {
                &c[..200]
            } else {
                c.as_str()
            }
        })
        .collect::<Vec<&str>>()
        .join("\n");

    let id = Ulid::new().to_string();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);

    let id_clone = id.clone();
    let content_clone = summary_content.clone();
    let scope_value_clone = scope_value.to_string();

    db.with_write_conn(|conn| {
        conn.execute(
            "INSERT OR REPLACE INTO derived_summaries
                 (id, scope_type, scope_value, content, stale, generated_at)
             VALUES (?1, ?2, ?3, ?4, 0, ?5)",
            params![id_clone, scope_type_str, scope_value_clone, content_clone, now],
        )
        .map_err(|e| BrainCoreError::Database(e.to_string()))?;
        Ok(())
    })?;

    Ok(id)
}

/// Retrieve an existing derived summary for the given scope.
///
/// Returns `Ok(None)` if no summary row exists for the given scope.
pub fn get_scope_summary(
    db: &Db,
    scope_type: &ScopeType,
    scope_value: &str,
) -> Result<Option<DerivedSummary>> {
    let scope_type_str = scope_type.as_str().to_string();
    let scope_value_owned = scope_value.to_string();

    db.with_read_conn(|conn| {
        let mut stmt = conn.prepare(
            "SELECT id, scope_type, scope_value, content, stale, generated_at
             FROM derived_summaries
             WHERE scope_type = ?1 AND scope_value = ?2",
        )?;

        let mut rows = stmt
            .query_map(params![scope_type_str, scope_value_owned], |row| {
                Ok(DerivedSummary {
                    id: row.get(0)?,
                    scope_type: row.get(1)?,
                    scope_value: row.get(2)?,
                    content: row.get(3)?,
                    stale: row.get::<_, i64>(4)? != 0,
                    generated_at: row.get(5)?,
                })
            })
            .map_err(|e| BrainCoreError::Database(e.to_string()))?;

        match rows.next() {
            Some(Ok(summary)) => Ok(Some(summary)),
            Some(Err(e)) => Err(BrainCoreError::Database(e.to_string())),
            None => Ok(None),
        }
    })
}

/// Mark any existing derived summary for the given scope as stale.
///
/// Called when a file inside a directory is re-indexed so that the
/// directory summary is queued for regeneration.
///
/// # Returns
/// Number of rows updated (0 or 1).
pub fn mark_scope_stale(
    db: &Db,
    scope_type: &ScopeType,
    scope_value: &str,
) -> Result<usize> {
    let scope_type_str = scope_type.as_str().to_string();
    let scope_value_owned = scope_value.to_string();

    db.with_write_conn(|conn| {
        let n = conn
            .execute(
                "UPDATE derived_summaries SET stale = 1
                 WHERE scope_type = ?1 AND scope_value = ?2",
                params![scope_type_str, scope_value_owned],
            )
            .map_err(|e| BrainCoreError::Database(e.to_string()))?;
        Ok(n)
    })
}

/// Search derived summaries by keyword across all scopes.
///
/// Queries the `fts_derived_summaries` FTS5 virtual table when it exists,
/// falling back to a LIKE search on `derived_summaries.content`.
///
/// Returns matching summaries ordered by relevance.
///
/// TODO: Integrate derived summaries into `memory.search_minimal` results.
/// Embedding-based search requires indexing summaries into LanceDB, which is
/// deferred. Until then, use the `memory.summarize_scope` MCP tool for direct
/// scope-based access to derived summaries.
pub fn search_derived_summaries(
    db: &Db,
    query: &str,
    limit: usize,
) -> Result<Vec<DerivedSummary>> {
    let query_owned = query.to_string();
    let limit_i64 = limit as i64;

    db.with_read_conn(|conn| {
        // Check whether the FTS5 virtual table exists.
        let fts_exists: bool = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type='table' AND name='fts_derived_summaries'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|n| n > 0)
            .unwrap_or(false);

        if fts_exists {
            let mut stmt = conn.prepare(
                "SELECT ds.id, ds.scope_type, ds.scope_value, ds.content,
                        ds.stale, ds.generated_at
                 FROM fts_derived_summaries fts
                 JOIN derived_summaries ds ON ds.rowid = fts.rowid
                 WHERE fts_derived_summaries MATCH ?1
                 LIMIT ?2",
            )?;
            let summaries = stmt
                .query_map(params![query_owned, limit_i64], |row| {
                    Ok(DerivedSummary {
                        id: row.get(0)?,
                        scope_type: row.get(1)?,
                        scope_value: row.get(2)?,
                        content: row.get(3)?,
                        stale: row.get::<_, i64>(4)? != 0,
                        generated_at: row.get(5)?,
                    })
                })
                .map_err(|e| BrainCoreError::Database(e.to_string()))?
                .collect::<std::result::Result<Vec<DerivedSummary>, _>>()
                .map_err(|e| BrainCoreError::Database(e.to_string()))?;
            Ok(summaries)
        } else {
            // Fallback: LIKE search on content.
            let pattern = format!("%{}%", query_owned);
            let mut stmt = conn.prepare(
                "SELECT id, scope_type, scope_value, content, stale, generated_at
                 FROM derived_summaries
                 WHERE content LIKE ?1
                 LIMIT ?2",
            )?;
            let summaries = stmt
                .query_map(params![pattern, limit_i64], |row| {
                    Ok(DerivedSummary {
                        id: row.get(0)?,
                        scope_type: row.get(1)?,
                        scope_value: row.get(2)?,
                        content: row.get(3)?,
                        stale: row.get::<_, i64>(4)? != 0,
                        generated_at: row.get(5)?,
                    })
                })
                .map_err(|e| BrainCoreError::Database(e.to_string()))?
                .collect::<std::result::Result<Vec<DerivedSummary>, _>>()
                .map_err(|e| BrainCoreError::Database(e.to_string()))?;
            Ok(summaries)
        }
    })
}
