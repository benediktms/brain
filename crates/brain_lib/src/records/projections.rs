// Re-export all record projection functions from brain_persistence.
pub use brain_persistence::db::records::projections::*;

/// Rebuild all records projection tables from the JSONL event log.
///
/// Reads events from `events_path`, then delegates to `rebuild_from_events`.
/// This wrapper stays in brain_lib because it depends on file I/O
/// (`read_all_events` lives here, not in brain_persistence).
pub fn rebuild(
    conn: &rusqlite::Connection,
    events_path: &std::path::Path,
) -> crate::error::Result<usize> {
    let events = super::events::read_all_events(events_path)?;
    rebuild_from_events(conn, &events)
}
