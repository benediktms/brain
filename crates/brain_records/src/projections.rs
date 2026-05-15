//! Projection-layer re-exports.
//!
//! Same narrowing as [`crate::queries`] and [`crate::events`]: the wildcard
//! re-export this module used to carry leaked every projection function from
//! `brain_persistence`. The only externally-needed entry point is
//! `apply_event` (used by integration tests for raw event replay) plus the
//! local `rebuild()` wrapper. `rebuild_from_events` is consumed only inside
//! this module and stays private.

// `apply_event` is the persistence-layer projection step. External callers
// no longer need it — they reach the same operations via typed `RecordStore`
// methods. Crate-private so internal store code keeps using `crate::projections::apply_event`.
pub(crate) use brain_persistence::db::records::projections::apply_event;

use brain_persistence::db::records::projections::rebuild_from_events;
use brain_persistence::sql::SqlResultExt;

/// Rebuild all records projection tables from the JSONL event log.
///
/// Reads events from `events_path`, then delegates to `rebuild_from_events`.
/// This wrapper depends on file I/O — `read_all_events` lives in
/// [`crate::events`], not in brain_persistence.
pub fn rebuild(
    db: &brain_persistence::db::Db,
    events_path: &std::path::Path,
) -> brain_core::error::Result<usize> {
    let events = crate::events::read_all_events(events_path)?;
    db.with_write_conn(|conn| rebuild_from_events(conn, &events))
        .into_brain_core()
}
