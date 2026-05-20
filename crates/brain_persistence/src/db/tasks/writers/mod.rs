//! SQL-execution writers for the task projection tables.
//!
//! # Role
//!
//! Writers are the SQL-execution half of the task projection. A future step
//! will move the dispatcher (the `match event.event_type { ... }` logic) into
//! `brain_tasks::projections`, which will own the domain-level decision of
//! *which* writer to call. Writers themselves remain here in `brain_persistence`
//! and contain no domain logic — only the SQL.
//!
//! # Primitive-scalars-only contract
//!
//! Every writer accepts primitive scalars only (`&Connection`, `&str`, `i64`,
//! `Option<&str>`, `Option<i64>`, `i32`). No `TaskEvent`, `TaskStatus`,
//! `EventType`, or payload structs are imported into any writer module. The
//! caller (`apply_event_inner`, currently in `projections.rs`) extracts
//! primitives from the typed payload and passes them in. This keeps
//! `brain_persistence` domain-type-free at its internal SQL boundary.
//!
//! # Read helpers
//!
//! Pure-read query helpers (`task_exists`, `next_child_seq`, `blake3_short_hex`)
//! live in `crate::db::tasks::queries` and are the **read** half of the same
//! boundary. `brain_tasks` crates may call both writers and pure-read helpers;
//! that does not violate the "no SQL outside persistence" rule because the SQL
//! execution stays here in `brain_persistence`.

mod comments;
mod deps;
mod events;
mod external_ids;
mod labels;
mod notes;
mod rebuild;
mod tasks;

pub use comments::{add_comment, update_comment};
pub use deps::{add_dependency, add_orphan_blocks_edge, remove_dependency};
pub use events::{
    append_status_changed_event, append_task_event_log, append_task_transferred_event,
};
pub use external_ids::{
    ExternalBlockerResolveOutcome, add_external_blocker, add_external_id, remove_external_id,
    resolve_external_blocker,
};
pub use labels::{add_label, remove_label};
pub use notes::{link_note, unlink_note};
pub use rebuild::{drop_fts_triggers, rebuild_clear_all, rebuild_fts_index};
#[allow(deprecated)]
pub use tasks::update_task;
pub use tasks::{
    TaskUpdateFields, change_description, change_priority, change_task_type, change_title,
    claim_task, clear_due_date, clear_task_parent, defer_task, insert_task_row, set_blocked_reason,
    set_due_date, set_task_parent, set_task_status, transfer_task, unclaim_task, undefer_task,
};
