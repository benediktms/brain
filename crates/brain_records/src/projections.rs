// Re-export all record projection functions from brain_persistence.
pub use brain_persistence::db::records::projections::*;

use brain_persistence::sql::SqlResultExt;

/// Rebuild all records projection tables from the JSONL event log.
///
/// Reads events from `events_path`, then delegates to `rebuild_from_events`.
/// This wrapper stays in brain_lib because it depends on file I/O
/// (`read_all_events` lives here, not in brain_persistence).
pub fn rebuild(
    db: &brain_persistence::db::Db,
    events_path: &std::path::Path,
) -> brain_core::error::Result<usize> {
    let events = crate::events::read_all_events(events_path)?;
    db.with_write_conn(|conn| rebuild_from_events(conn, &events))
        .into_brain_core()
}
