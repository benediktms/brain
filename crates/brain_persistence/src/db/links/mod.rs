//! Polymorphic edge graph types.
//!
//! Backing SQL table: `entity_links` (created in v48→v49 migration).
//! The legacy `links` table (note/wiki linking, used by `pagerank.rs`) is a
//! separate domain and is not touched by this module.
//!
//! The Rust module path is `crate::db::links`; the SQL table is `entity_links`.
//! This asymmetry exists to avoid a name collision with the legacy wiki-link
//! table while keeping the module name concise.

pub mod projections;
pub use projections::{LinkEvent, apply_link_event};

use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::error::Result;
use crate::links::Link;

// ── Polymorphic edge graph types ───────────────────────────────────────────

/// Discriminates the kind of entity participating in a polymorphic edge.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum EntityType {
    Task,
    Record,
    Episode,
    Procedure,
    Chunk,
    Note,
}

/// A typed reference to any entity in the system.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EntityRef {
    pub kind: EntityType,
    pub id: String,
}

/// Error returned by the validating [`EntityRef`] constructors.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum EntityRefError {
    #[error("EntityRef id must not be empty")]
    EmptyId,
}

impl EntityRef {
    /// Ergonomic, validating constructor. Rejects an empty `id`.
    ///
    /// Direct struct construction (`EntityRef { kind, id }`) remains valid for
    /// callers that have already validated the id.
    pub fn new(
        kind: EntityType,
        id: impl Into<String>,
    ) -> std::result::Result<Self, EntityRefError> {
        let id = id.into();
        if id.is_empty() {
            return Err(EntityRefError::EmptyId);
        }
        Ok(Self { kind, id })
    }

    /// Shorthand: `EntityRef { kind: EntityType::Task, id }`.
    pub fn task(id: impl Into<String>) -> std::result::Result<Self, EntityRefError> {
        Self::new(EntityType::Task, id)
    }

    /// Shorthand: `EntityRef { kind: EntityType::Record, id }`.
    pub fn record(id: impl Into<String>) -> std::result::Result<Self, EntityRefError> {
        Self::new(EntityType::Record, id)
    }

    /// Shorthand: `EntityRef { kind: EntityType::Episode, id }`.
    pub fn episode(id: impl Into<String>) -> std::result::Result<Self, EntityRefError> {
        Self::new(EntityType::Episode, id)
    }

    /// Shorthand: `EntityRef { kind: EntityType::Procedure, id }`.
    pub fn procedure(id: impl Into<String>) -> std::result::Result<Self, EntityRefError> {
        Self::new(EntityType::Procedure, id)
    }

    /// Shorthand: `EntityRef { kind: EntityType::Chunk, id }`.
    pub fn chunk(id: impl Into<String>) -> std::result::Result<Self, EntityRefError> {
        Self::new(EntityType::Chunk, id)
    }

    /// Shorthand: `EntityRef { kind: EntityType::Note, id }`.
    pub fn note(id: impl Into<String>) -> std::result::Result<Self, EntityRefError> {
        Self::new(EntityType::Note, id)
    }
}

/// The semantic relationship expressed by a directed edge.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeKind {
    ParentOf,
    Blocks,
    Covers,
    RelatesTo,
    SeeAlso,
    Supersedes,
    Contradicts,
}

impl EdgeKind {
    /// Returns `true` for edge kinds that must form a directed acyclic graph.
    ///
    /// Cycle-prevention logic MUST be applied before inserting edges of these
    /// kinds:
    ///
    /// - `ParentOf` — hierarchy edges cannot be cyclic (A is parent of B is parent of A
    ///   implies A is its own ancestor).
    /// - `Blocks` — circular blocking is a deadlock by definition.
    /// - `Supersedes` — A→B→A means "A supersedes B supersedes A", which is semantically
    ///   incoherent; cycles must be rejected.
    pub fn requires_dag(&self) -> bool {
        matches!(self, Self::ParentOf | Self::Blocks | Self::Supersedes)
    }
}

/// Payload written to the event log when a polymorphic edge is created.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinkCreatedPayload {
    pub from: EntityRef,
    pub to: EntityRef,
    pub edge_kind: EdgeKind,
}

/// Payload written to the event log when a polymorphic edge is removed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinkRemovedPayload {
    pub from: EntityRef,
    pub to: EntityRef,
    pub edge_kind: EdgeKind,
}

// ── Obsidian-style file link resolution ───────────────────────────────────

/// Resolve a link's `target_path` to a `file_id` using Obsidian-style disambiguation.
///
/// Resolution order (for wiki/markdown links):
/// 1. Exact `path` match (handles absolute paths stored as-is)
/// 2. Path ends with `/<target>.md` (wiki bare stems, e.g. "headings" → .../headings.md)
/// 3. Path ends with `/<target>` (markdown links that already carry an extension)
///
/// When multiple files match the same rule, the shortest path wins (nearest-match
/// semantics, mimicking Obsidian). Returns `None` for external links or no match.
pub(crate) fn resolve_target_file_id(
    conn: &Connection,
    target_path: &str,
    link_type: &str,
) -> Option<String> {
    if link_type == "external" {
        return None;
    }

    // Collect all candidate (file_id, path) rows that match any of the three strategies.
    let suffix_with_md = format!("/{}.md", target_path);
    let suffix_bare = format!("/{}", target_path);

    let mut stmt = conn
        .prepare_cached(
            "SELECT file_id, path FROM files
              WHERE path = ?1
                 OR path LIKE '%' || ?2
                 OR path LIKE '%' || ?3",
        )
        .ok()?;

    let candidates: Vec<(String, String)> = stmt
        .query_map(
            rusqlite::params![target_path, suffix_with_md, suffix_bare],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .ok()?
        .filter_map(|r| r.ok())
        .collect();

    // Pick the candidate with the shortest path (nearest-match).
    candidates
        .into_iter()
        .min_by_key(|(_, path)| path.len())
        .map(|(file_id, _)| file_id)
}

/// Returns true if the `links.target_file_id` column exists (i.e. v15+ schema).
fn has_target_file_id_column(conn: &Connection) -> bool {
    conn.query_row(
        "SELECT COUNT(*) FROM pragma_table_info('links') WHERE name = 'target_file_id'",
        [],
        |row| row.get::<_, i64>(0),
    )
    .map(|n| n > 0)
    .unwrap_or(false)
}

/// Atomically replace all links for a file.
///
/// Deletes existing links for the `source_file_id`, then inserts the new set.
/// When the schema is at v15+, resolves `target_file_id` for wiki/markdown links
/// at insert time. Falls back to the v14 INSERT when the column is absent.
pub fn replace_links(conn: &Connection, source_file_id: &str, links: &[Link]) -> Result<()> {
    let tx = conn.unchecked_transaction()?;

    tx.execute(
        "DELETE FROM links WHERE source_file_id = ?1",
        [source_file_id],
    )?;

    let with_target_file_id = has_target_file_id_column(&tx);

    if with_target_file_id {
        let mut stmt = tx.prepare_cached(
            "INSERT INTO links (link_id, source_file_id, target_path, link_text, link_type, target_file_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        )?;
        for link in links {
            let target_file_id = resolve_target_file_id(&tx, &link.target, link.link_type.as_str());
            stmt.execute(rusqlite::params![
                Ulid::new().to_string(),
                source_file_id,
                link.target,
                link.link_text,
                link.link_type.as_str(),
                target_file_id,
            ])?;
        }
    } else {
        let mut stmt = tx.prepare_cached(
            "INSERT INTO links (link_id, source_file_id, target_path, link_text, link_type)
             VALUES (?1, ?2, ?3, ?4, ?5)",
        )?;
        for link in links {
            stmt.execute(rusqlite::params![
                Ulid::new().to_string(),
                source_file_id,
                link.target,
                link.link_text,
                link.link_type.as_str(),
            ])?;
        }
    }

    tx.commit()?;
    Ok(())
}

/// Get all files that link to the given target path.
///
/// Returns `(source_file_id, link_text)` pairs.
pub fn get_backlinks(conn: &Connection, target_path: &str) -> Result<Vec<(String, String)>> {
    let mut stmt =
        conn.prepare("SELECT source_file_id, link_text FROM links WHERE target_path = ?1")?;
    let rows = stmt.query_map([target_path], |row| Ok((row.get(0)?, row.get(1)?)))?;

    super::collect_rows(rows)
}

/// Get all file_ids that `source_file_id` links to (outgoing 1-hop neighbours).
///
/// Returns resolved `target_file_id` values. When `target_file_id` is already
/// set on the link row, it is used directly. For wiki/markdown links where
/// `target_file_id` is NULL (e.g. because the target file was indexed after
/// the source file), the `target_path` is resolved against the `files` table
/// using Obsidian-style nearest-match logic.
///
/// External links and links whose target cannot be resolved are excluded.
pub fn get_outlinks(conn: &Connection, source_file_id: &str) -> Result<Vec<String>> {
    // Collect all outgoing links for this source file. We first gather all rows
    // into memory, then resolve target_file_id for unresolved entries afterwards.
    // This avoids re-entering the connection while a cursor is still open.
    let link_rows: Vec<(Option<String>, String, String)> = {
        let mut stmt = conn.prepare_cached(
            "SELECT l.target_file_id, l.target_path, l.link_type
             FROM links l
             WHERE l.source_file_id = ?1 AND l.link_type != 'external'",
        )?;
        let rows = stmt.query_map([source_file_id], |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        super::collect_rows(rows)?
    };

    let mut result: Vec<String> = Vec::new();
    for (target_file_id, target_path, link_type) in link_rows {
        let resolved_fid = if let Some(fid) = target_file_id {
            Some(fid)
        } else {
            // Attempt runtime resolution via the files table.
            // Safe: the cursor from the SELECT above is fully consumed before this.
            resolve_target_file_id(conn, &target_path, &link_type)
        };
        if let Some(fid) = resolved_fid
            && !result.contains(&fid)
        {
            result.push(fid);
        }
    }
    Ok(result)
}

/// Count backlinks for a given target path.
pub fn count_backlinks(conn: &Connection, target_path: &str) -> Result<usize> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM links WHERE target_path = ?1",
        [target_path],
        |row| row.get(0),
    )?;
    Ok(count as usize)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Polymorphic type tests ─────────────────────────────────────────────

    #[test]
    fn entity_type_serde_round_trip() {
        let cases = [
            (EntityType::Task, "\"TASK\""),
            (EntityType::Record, "\"RECORD\""),
            (EntityType::Episode, "\"EPISODE\""),
            (EntityType::Procedure, "\"PROCEDURE\""),
            (EntityType::Chunk, "\"CHUNK\""),
            (EntityType::Note, "\"NOTE\""),
        ];
        for (variant, expected_json) in cases {
            let serialized = serde_json::to_string(&variant).unwrap();
            assert_eq!(serialized, expected_json, "serialize {variant:?}");
            let deserialized: EntityType = serde_json::from_str(&serialized).unwrap();
            assert_eq!(deserialized, variant, "deserialize {variant:?}");
        }
    }

    #[test]
    fn edge_kind_serde_round_trip() {
        let cases = [
            (EdgeKind::ParentOf, "\"parent_of\""),
            (EdgeKind::Blocks, "\"blocks\""),
            (EdgeKind::Covers, "\"covers\""),
            (EdgeKind::RelatesTo, "\"relates_to\""),
            (EdgeKind::SeeAlso, "\"see_also\""),
            (EdgeKind::Supersedes, "\"supersedes\""),
            (EdgeKind::Contradicts, "\"contradicts\""),
        ];
        for (variant, expected_json) in cases {
            let serialized = serde_json::to_string(&variant).unwrap();
            assert_eq!(serialized, expected_json, "serialize {variant:?}");
            let deserialized: EdgeKind = serde_json::from_str(&serialized).unwrap();
            assert_eq!(deserialized, variant, "deserialize {variant:?}");
        }
    }

    #[test]
    fn edge_kind_requires_dag_truth_table() {
        assert!(EdgeKind::ParentOf.requires_dag(), "ParentOf must be DAG");
        assert!(EdgeKind::Blocks.requires_dag(), "Blocks must be DAG");
        assert!(
            EdgeKind::Supersedes.requires_dag(),
            "Supersedes must be DAG — A→B→A is semantically incoherent"
        );
        assert!(!EdgeKind::Covers.requires_dag(), "Covers is not DAG");
        assert!(!EdgeKind::RelatesTo.requires_dag(), "RelatesTo is not DAG");
        assert!(!EdgeKind::SeeAlso.requires_dag(), "SeeAlso is not DAG");
        assert!(
            !EdgeKind::Contradicts.requires_dag(),
            "Contradicts is not DAG"
        );
    }

    #[test]
    fn link_created_payload_serde_round_trip() {
        let payload = LinkCreatedPayload {
            from: EntityRef {
                kind: EntityType::Task,
                id: "task-001".to_string(),
            },
            to: EntityRef {
                kind: EntityType::Record,
                id: "rec-002".to_string(),
            },
            edge_kind: EdgeKind::Covers,
        };
        let json = serde_json::to_string(&payload).unwrap();
        let decoded: LinkCreatedPayload = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, payload);
    }

    #[test]
    fn link_removed_payload_serde_round_trip() {
        let payload = LinkRemovedPayload {
            from: EntityRef {
                kind: EntityType::Episode,
                id: "ep-abc".to_string(),
            },
            to: EntityRef {
                kind: EntityType::Note,
                id: "note-xyz".to_string(),
            },
            edge_kind: EdgeKind::SeeAlso,
        };
        let json = serde_json::to_string(&payload).unwrap();
        let decoded: LinkRemovedPayload = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, payload);
    }

    #[test]
    fn entity_ref_serde_round_trip_pins_wire_shape() {
        let r = EntityRef {
            kind: EntityType::Task,
            id: "task-123".to_string(),
        };
        let json = serde_json::to_string(&r).unwrap();
        assert_eq!(json, r#"{"kind":"TASK","id":"task-123"}"#);
        let decoded: EntityRef = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, r);
    }

    /// Pins the serde wire-string values used for `EntityType` and `EdgeKind`
    /// against the literal strings stored in SQL (notably the partial-index
    /// predicates `from_type = 'TASK'` and `edge_kind = 'parent_of'` /
    /// `'blocks'` in the v48→v49 migration). A rename of any enum variant
    /// would break the partial-index hot path silently — this test forces a
    /// compile-time review whenever the wire shape moves.
    #[test]
    fn serde_strings_match_sql_partial_index_predicates() {
        assert_eq!(
            serde_json::to_string(&EntityType::Task).unwrap(),
            "\"TASK\""
        );
        assert_eq!(
            serde_json::to_string(&EntityType::Record).unwrap(),
            "\"RECORD\""
        );
        assert_eq!(
            serde_json::to_string(&EdgeKind::ParentOf).unwrap(),
            "\"parent_of\""
        );
        assert_eq!(
            serde_json::to_string(&EdgeKind::Blocks).unwrap(),
            "\"blocks\""
        );
        assert_eq!(
            serde_json::to_string(&EdgeKind::Covers).unwrap(),
            "\"covers\""
        );
    }

    // ── EntityRef constructor tests (Finding 8) ────────────────────────────

    #[test]
    fn entity_ref_constructor_rejects_empty_id() {
        assert_eq!(
            EntityRef::new(EntityType::Task, ""),
            Err(EntityRefError::EmptyId)
        );
        assert_eq!(EntityRef::task(""), Err(EntityRefError::EmptyId));
        assert_eq!(EntityRef::record(""), Err(EntityRefError::EmptyId));
        assert_eq!(EntityRef::episode(""), Err(EntityRefError::EmptyId));
        assert_eq!(EntityRef::procedure(""), Err(EntityRefError::EmptyId));
        assert_eq!(EntityRef::chunk(""), Err(EntityRefError::EmptyId));
        assert_eq!(EntityRef::note(""), Err(EntityRefError::EmptyId));
    }

    #[test]
    fn entity_ref_task_helper_round_trips() {
        let r = EntityRef::task("task-123").unwrap();
        assert_eq!(r.kind, EntityType::Task);
        assert_eq!(r.id, "task-123");

        // Remaining helpers smoke-tested.
        assert_eq!(EntityRef::record("rec-1").unwrap().kind, EntityType::Record);
        assert_eq!(
            EntityRef::episode("ep-1").unwrap().kind,
            EntityType::Episode
        );
        assert_eq!(
            EntityRef::procedure("proc-1").unwrap().kind,
            EntityType::Procedure
        );
        assert_eq!(EntityRef::chunk("chunk-1").unwrap().kind, EntityType::Chunk);
        assert_eq!(EntityRef::note("note-1").unwrap().kind, EntityType::Note);
    }

    // ── DRY mirror tests: entity_type_str / edge_kind_str vs. serde (Finding 9) ──
    //
    // `entity_type_str` and `edge_kind_str` in projections.rs duplicate what
    // serde already produces. We intentionally keep the static functions (no
    // runtime JSON overhead, no quote stripping) but pin them against serde so
    // the duplication is a verified mirror, not drift-prone dead code.

    #[test]
    fn entity_type_str_matches_serde() {
        use crate::db::links::projections::entity_type_str_for_test as entity_type_str;
        let variants = [
            EntityType::Task,
            EntityType::Record,
            EntityType::Episode,
            EntityType::Procedure,
            EntityType::Chunk,
            EntityType::Note,
        ];
        for variant in variants {
            let from_fn = entity_type_str(variant);
            let from_serde = serde_json::to_value(variant)
                .unwrap()
                .as_str()
                .unwrap()
                .to_string();
            assert_eq!(
                from_fn, from_serde,
                "entity_type_str({variant:?}) diverges from serde"
            );
        }
    }

    #[test]
    fn edge_kind_str_matches_serde() {
        use crate::db::links::projections::edge_kind_str_for_test as edge_kind_str;
        let variants = [
            EdgeKind::ParentOf,
            EdgeKind::Blocks,
            EdgeKind::Covers,
            EdgeKind::RelatesTo,
            EdgeKind::SeeAlso,
            EdgeKind::Supersedes,
            EdgeKind::Contradicts,
        ];
        for variant in variants {
            let from_fn = edge_kind_str(variant);
            let from_serde = serde_json::to_value(variant)
                .unwrap()
                .as_str()
                .unwrap()
                .to_string();
            assert_eq!(
                from_fn, from_serde,
                "edge_kind_str({variant:?}) diverges from serde"
            );
        }
    }
}
