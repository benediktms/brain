/// Cross-brain task transfer — transactional core.
///
/// All SQL for the preserve-ID transfer lives here so that `brain_lib` never
/// touches `rusqlite` directly (enforced by `brain_lib/clippy.toml`).
use rusqlite::{Connection, OptionalExtension, params};

use crate::db::tasks::display_id::compute_display_id_for_target;
use crate::db::tasks::events::{TaskEvent, TaskTransferredPayload};
use crate::error::{BrainCoreError, Result};

/// Result of a successful task transfer.
#[derive(Debug, Clone)]
pub struct TaskTransferResult {
    pub task_id: String,
    pub from_brain_id: String,
    pub to_brain_id: String,
    pub from_display_id: String,
    pub to_display_id: String,
    /// `true` when source and target brain are the same — no writes occurred.
    pub was_no_op: bool,
    /// `true` when the LanceDB vector rows were also re-stamped to `to_brain_id`.
    /// `false` when no LanceDB store was provided or the update failed (non-fatal).
    pub lance_synced: bool,
}

/// Execute the transfer inside a single `BEGIN IMMEDIATE` transaction.
///
/// Called by `TaskStore::transfer_task` via `db.with_write_conn`. The caller
/// is responsible for providing the write-connection handle; this function
/// manages the explicit transaction inside it.
///
/// Steps (all-or-nothing):
/// 1. Read current `(brain_id, display_id)` for the task.
/// 2. Same-brain no-op check.
/// 3. Compute collision-safe `display_id` for target brain.
/// 4. CAS `UPDATE tasks` — rolls back if 0 rows affected.
/// 5. Propagate `brain_id` to `chunks` and `files` tables.
/// 6. Propagate `brain_id` to `records` table.
/// 7. Insert `task_transferred` event row.
pub fn transfer_task_inner(
    conn: &Connection,
    task_id: &str,
    target_brain_id: &str,
) -> Result<TaskTransferResult> {
    // 1. Read current state.
    let current: Option<(String, Option<String>)> = conn
        .query_row(
            "SELECT brain_id, display_id FROM tasks WHERE task_id = ?1",
            params![task_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;

    let (from_brain_id, from_display_id_opt) =
        current.ok_or_else(|| BrainCoreError::TaskNotFound(task_id.to_string()))?;

    let from_display_id = from_display_id_opt.unwrap_or_default();

    // 2. Same-brain no-op.
    if from_brain_id == target_brain_id {
        return Ok(TaskTransferResult {
            task_id: task_id.to_string(),
            from_brain_id: from_brain_id.clone(),
            to_brain_id: from_brain_id,
            from_display_id: from_display_id.clone(),
            to_display_id: from_display_id,
            was_no_op: true,
            lance_synced: false,
        });
    }

    // Pre-check: verify target brain exists before opening a write lock.
    // This surfaces a clear BrainNotFound error instead of a generic FK violation.
    let target_exists: bool = conn
        .query_row(
            "SELECT 1 FROM brains WHERE brain_id = ?1",
            params![target_brain_id],
            |_| Ok(true),
        )
        .optional()?
        .unwrap_or(false);
    if !target_exists {
        return Err(BrainCoreError::BrainNotFound(target_brain_id.to_string()));
    }

    conn.execute_batch("BEGIN IMMEDIATE")?;

    let result = (|| -> Result<TaskTransferResult> {
        // 3. Compute collision-safe display_id for target brain.
        let to_display_id = compute_display_id_for_target(conn, task_id, target_brain_id)?;

        // 4. CAS UPDATE on tasks.
        let rows = conn.execute(
            "UPDATE tasks \
             SET brain_id = ?1, display_id = ?2, updated_at = strftime('%s','now') \
             WHERE task_id = ?3 AND brain_id = ?4",
            params![target_brain_id, to_display_id, task_id, from_brain_id],
        )?;
        if rows != 1 {
            return Err(BrainCoreError::TaskTransferCasFailed(format!(
                "task {task_id}: concurrent transfer detected — retry"
            )));
        }

        // 5. Update SQLite chunks and files.
        let task_file_id = format!("task:{task_id}");
        let outcome_file_id = format!("task-outcome:{task_id}");
        conn.execute(
            "UPDATE chunks SET brain_id = ?1 WHERE file_id IN (?2, ?3)",
            params![target_brain_id, task_file_id, outcome_file_id],
        )?;
        conn.execute(
            "UPDATE files SET brain_id = ?1 WHERE path IN (?2, ?3)",
            params![target_brain_id, task_file_id, outcome_file_id],
        )?;

        // 6. Records follow the task (FK-less, explicit update required).
        conn.execute(
            "UPDATE records SET brain_id = ?1 WHERE task_id = ?2",
            params![target_brain_id, task_id],
        )?;

        // 7. Insert task_transferred event.
        let ev = TaskEvent::from_payload(
            task_id,
            "system",
            TaskTransferredPayload {
                from_brain_id: from_brain_id.clone(),
                to_brain_id: target_brain_id.to_string(),
                from_display_id: from_display_id.clone(),
                to_display_id: to_display_id.clone(),
            },
        );
        // ev.payload is already a serde_json::Value; serializing it to a JSON
        // string is infallible in practice, but we propagate errors explicitly
        // rather than silently corrupting the event row with "{}".
        let payload_json = serde_json::to_string(&ev.payload)
            .map_err(|e| BrainCoreError::TaskEvent(format!("payload serialize failed: {e}")))?;
        conn.execute(
            "INSERT INTO task_events \
             (event_id, task_id, event_type, timestamp, actor, payload) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                ev.event_id,
                ev.task_id,
                format!("{:?}", ev.event_type),
                ev.timestamp,
                ev.actor,
                payload_json,
            ],
        )?;

        Ok(TaskTransferResult {
            task_id: task_id.to_string(),
            from_brain_id,
            to_brain_id: target_brain_id.to_string(),
            from_display_id,
            to_display_id,
            was_no_op: false,
            lance_synced: false, // Populated by the async caller after SQLite commits.
        })
    })();

    match result {
        Ok(r) => {
            conn.execute_batch("COMMIT")?;
            Ok(r)
        }
        Err(e) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(e)
        }
    }
}
