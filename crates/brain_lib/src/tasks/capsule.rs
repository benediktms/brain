//! Task capsule generation and storage.
//!
//! Builds short text capsules from task metadata and stores them into
//! SQLite for BM25/FTS keyword search alongside note chunks.

use crate::db::Db;
use crate::error::Result;

/// Build a capsule string from a task's current state.
///
/// Format: "{title}. {desc_summary}. Tags: {labels}. Priority: {priority}."
/// - `desc_summary`: first sentence (find `. ` or `\n`, max 200 chars); omit if None/empty
/// - Tags segment omitted if labels is empty
/// - Priority segment always included
pub fn build_task_capsule(
    title: &str,
    description: Option<&str>,
    labels: &[String],
    priority: i32,
) -> String {
    let mut parts = vec![title.to_string()];

    if let Some(desc) = description {
        let desc = desc.trim();
        if !desc.is_empty() {
            // Take first sentence (up to ". " or "\n"), max 200 chars
            let end = desc
                .find(". ")
                .map(|p| p + 1)
                .or_else(|| desc.find('\n'))
                .unwrap_or(desc.len());
            let summary = &desc[..end.min(200)];
            if !summary.is_empty() {
                parts.push(summary.to_string());
            }
        }
    }

    if !labels.is_empty() {
        parts.push(format!("Tags: {}", labels.join(", ")));
    }

    parts.push(format!("Priority: {}", priority_label(priority)));

    parts.join(". ") + "."
}

/// Build an outcome capsule for a closed task.
///
/// Format:
/// - With reason: "{title}. Outcome: {reason}."
/// - Without: "{title}. Completed."
pub fn build_outcome_capsule(title: &str, completion_reason: Option<&str>) -> String {
    match completion_reason {
        Some(reason) => {
            let reason = reason.trim();
            if reason.is_empty() {
                format!("{title}. Completed.")
            } else {
                format!("{title}. Outcome: {reason}.")
            }
        }
        None => format!("{title}. Completed."),
    }
}

/// Upsert a task capsule into SQLite for BM25/FTS indexing.
///
/// This is a low-level helper called after task mutations. Best-effort:
/// errors propagate to the caller.
pub fn store_task_capsule(
    db: &Db,
    file_id: &str,  // e.g. "task:BRN-01ABC" or "task-outcome:BRN-01ABC"
    capsule_text: &str,
) -> Result<()> {
    db.with_write_conn(|conn| {
        crate::db::chunks::upsert_task_chunk(conn, file_id, capsule_text)
    })
}

fn priority_label(p: i32) -> &'static str {
    match p {
        0 => "critical",
        1 => "high",
        2 => "medium",
        3 => "low",
        _ => "backlog",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_task_capsule_basic() {
        let capsule = build_task_capsule("Fix the bug", None, &[], 1);
        assert!(capsule.contains("Fix the bug"));
        assert!(capsule.contains("Priority: high"));
        assert!(!capsule.contains("Tags:"));
    }

    #[test]
    fn test_build_task_capsule_with_labels() {
        let labels = vec!["area:memory".to_string(), "type:feature".to_string()];
        let capsule = build_task_capsule(
            "Add search",
            Some("Implement semantic search for tasks."),
            &labels,
            2,
        );
        assert!(capsule.contains("Add search"));
        assert!(capsule.contains("Implement semantic search for tasks"));
        assert!(capsule.contains("Tags: area:memory, type:feature"));
        assert!(capsule.contains("Priority: medium"));
    }

    #[test]
    fn test_build_task_capsule_no_description() {
        let capsule = build_task_capsule("Simple task", None, &[], 3);
        assert!(capsule.contains("Simple task"));
        assert!(capsule.contains("Priority: low"));
        // Should not have a description segment
        assert_eq!(capsule, "Simple task. Priority: low.");
    }

    #[test]
    fn test_build_task_capsule_long_description_truncated() {
        let long_desc = "A".repeat(300);
        let capsule = build_task_capsule("Long desc task", Some(&long_desc), &[], 4);
        // Description should be truncated to ~200 chars
        assert!(capsule.len() < 400);
    }

    #[test]
    fn test_build_task_capsule_empty_labels() {
        let capsule = build_task_capsule("Task", None, &[], 2);
        assert!(!capsule.contains("Tags:"));
    }

    #[test]
    fn test_build_outcome_capsule_with_reason() {
        let capsule = build_outcome_capsule("Deploy v2", Some("Deployed successfully"));
        assert!(capsule.contains("Deploy v2"));
        assert!(capsule.contains("Outcome: Deployed successfully"));
    }

    #[test]
    fn test_build_outcome_capsule_no_reason() {
        let capsule = build_outcome_capsule("Simple task", None);
        assert!(capsule.contains("Simple task"));
        assert!(capsule.contains("Completed"));
        assert_eq!(capsule, "Simple task. Completed.");
    }

    #[test]
    fn test_build_outcome_capsule_empty_reason() {
        let capsule = build_outcome_capsule("Task", Some("  "));
        assert_eq!(capsule, "Task. Completed.");
    }

    #[test]
    fn test_priority_labels() {
        assert_eq!(priority_label(0), "critical");
        assert_eq!(priority_label(1), "high");
        assert_eq!(priority_label(2), "medium");
        assert_eq!(priority_label(3), "low");
        assert_eq!(priority_label(4), "backlog");
        assert_eq!(priority_label(5), "backlog");
    }

    #[test]
    fn test_build_task_capsule_description_first_sentence() {
        let desc = "First sentence. Second sentence.";
        let capsule = build_task_capsule("Title", Some(desc), &[], 2);
        // Should include "First sentence." but not necessarily the rest
        assert!(capsule.contains("First sentence"));
        // Should not include "Second sentence" since we only take up to ". "
        assert!(!capsule.contains("Second sentence"));
    }

    #[test]
    fn test_build_task_capsule_description_newline_split() {
        let desc = "First line\nSecond line";
        let capsule = build_task_capsule("Title", Some(desc), &[], 2);
        assert!(capsule.contains("First line"));
        assert!(!capsule.contains("Second line"));
    }
}
