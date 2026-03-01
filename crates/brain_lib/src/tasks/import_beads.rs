use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::path::Path;

use serde::Deserialize;

use crate::error::{BrainCoreError, Result};

use super::TaskStore;
use super::events::{
    CommentPayload, DependencyPayload, EventType, LabelPayload, ParentSetPayload,
    StatusChangedPayload, TaskCreatedPayload, TaskEvent, new_event_id,
};

/// Summary of an import run.
#[derive(Debug, Default)]
pub struct ImportReport {
    pub issues_imported: usize,
    pub events_generated: usize,
    pub deps_imported: usize,
    pub deps_skipped: usize,
    pub labels_imported: usize,
    pub comments_imported: usize,
    pub parent_links_imported: usize,
}

impl std::fmt::Display for ImportReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Import report:")?;
        writeln!(f, "  Issues imported:      {}", self.issues_imported)?;
        writeln!(f, "  Events generated:     {}", self.events_generated)?;
        writeln!(f, "  Block deps imported:  {}", self.deps_imported)?;
        writeln!(f, "  Parent links:         {}", self.parent_links_imported)?;
        writeln!(f, "  Deps skipped:         {}", self.deps_skipped)?;
        writeln!(f, "  Labels imported:      {}", self.labels_imported)?;
        writeln!(f, "  Comments imported:    {}", self.comments_imported)?;
        Ok(())
    }
}

// -- Beads JSONL schema --

#[derive(Debug, Deserialize)]
struct BeadsIssue {
    id: String,
    title: String,
    description: Option<String>,
    #[serde(default)]
    notes: Option<String>,
    #[serde(default)]
    design: Option<String>,
    #[serde(default)]
    acceptance_criteria: Option<String>,
    status: String,
    priority: i32,
    #[serde(default)]
    issue_type: Option<String>,
    #[serde(default)]
    owner: Option<String>,
    created_at: String,
    #[serde(default)]
    updated_at: Option<String>,
    #[serde(default)]
    closed_at: Option<String>,
    #[serde(default)]
    close_reason: Option<String>,
    #[serde(default)]
    labels: Vec<String>,
    #[serde(default)]
    dependencies: Vec<BeadsDependency>,
    #[serde(default)]
    comments: Vec<BeadsComment>,
}

#[derive(Debug, Deserialize)]
struct BeadsDependency {
    issue_id: String,
    depends_on_id: String,
    #[serde(rename = "type")]
    dep_type: String,
    #[serde(default)]
    created_at: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BeadsComment {
    #[serde(default)]
    author: Option<String>,
    text: String,
    #[serde(default)]
    created_at: Option<String>,
}

/// Parse an ISO-8601 timestamp into unix seconds.
fn parse_iso_ts(s: &str) -> i64 {
    // Try chrono-style parsing; fall back to now_ts on failure.
    // Format: "2026-02-25T16:12:27Z"
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        dt.timestamp()
    } else if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
        dt.and_utc().timestamp()
    } else {
        tracing::warn!("unparseable timestamp '{s}', using epoch");
        0
    }
}

/// Build the full description by appending notes/design/acceptance_criteria sections.
fn build_description(issue: &BeadsIssue) -> Option<String> {
    let mut parts = Vec::new();

    if let Some(desc) = &issue.description {
        if !desc.is_empty() {
            parts.push(desc.clone());
        }
    }
    if let Some(notes) = &issue.notes {
        if !notes.is_empty() {
            parts.push(format!("## Notes\n\n{notes}"));
        }
    }
    if let Some(design) = &issue.design {
        if !design.is_empty() {
            parts.push(format!("## Design\n\n{design}"));
        }
    }
    if let Some(ac) = &issue.acceptance_criteria {
        if !ac.is_empty() {
            parts.push(format!("## Acceptance Criteria\n\n{ac}"));
        }
    }

    if parts.is_empty() {
        None
    } else {
        Some(parts.join("\n\n"))
    }
}

/// Read and parse all beads issues from a JSONL file.
fn read_beads_issues(path: &Path) -> Result<Vec<BeadsIssue>> {
    let file = std::fs::File::open(path)
        .map_err(|e| BrainCoreError::TaskEvent(format!("open beads file: {e}")))?;
    let reader = BufReader::new(file);
    let mut issues = Vec::new();

    for (line_num, line) in reader.lines().enumerate() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        match serde_json::from_str::<BeadsIssue>(trimmed) {
            Ok(issue) => issues.push(issue),
            Err(e) => {
                tracing::warn!(
                    "skipping malformed beads issue at line {}: {e}",
                    line_num + 1
                );
            }
        }
    }

    Ok(issues)
}

/// Generate events from beads JSONL without applying them.
///
/// Uses a three-pass approach:
/// 1. Create all tasks (+ labels + comments)
/// 2. Wire up relationships (block deps + parent-child)
/// 3. Apply status changes (closed/in_progress)
///
/// Returns (events, report). Has no side effects.
pub fn generate_events_from_beads(jsonl_path: &Path) -> Result<(Vec<TaskEvent>, ImportReport)> {
    let issues = read_beads_issues(jsonl_path)?;
    let mut report = ImportReport::default();

    // Build a set of all issue IDs for relationship validation
    let issue_ids: HashMap<&str, &BeadsIssue> = issues.iter().map(|i| (i.id.as_str(), i)).collect();

    // Collect all events
    let mut all_events: Vec<TaskEvent> = Vec::new();

    // -- Pass 1: Create tasks + labels + comments --
    for issue in &issues {
        let created_ts = parse_iso_ts(&issue.created_at);

        let description = build_description(issue);

        let created_event = TaskEvent {
            event_id: new_event_id(),
            task_id: issue.id.clone(),
            timestamp: created_ts,
            actor: issue
                .owner
                .clone()
                .unwrap_or_else(|| "beads-import".to_string()),
            event_type: EventType::TaskCreated,
            payload: serde_json::to_value(TaskCreatedPayload {
                title: issue.title.clone(),
                description,
                priority: issue.priority,
                status: "open".to_string(),
                due_ts: None,
                task_type: issue.issue_type.clone(),
                assignee: issue.owner.clone(),
                defer_until: None,
                parent_task_id: None, // Set in pass 2
            })
            .unwrap(),
        };
        all_events.push(created_event);
        report.issues_imported += 1;

        // Labels
        for label in &issue.labels {
            let label_event = TaskEvent {
                event_id: new_event_id(),
                task_id: issue.id.clone(),
                timestamp: created_ts,
                actor: "beads-import".to_string(),
                event_type: EventType::LabelAdded,
                payload: serde_json::to_value(LabelPayload {
                    label: label.clone(),
                })
                .unwrap(),
            };
            all_events.push(label_event);
            report.labels_imported += 1;
        }

        // Comments
        for comment in &issue.comments {
            let comment_ts = comment
                .created_at
                .as_deref()
                .map(parse_iso_ts)
                .unwrap_or(created_ts);
            let comment_event = TaskEvent {
                event_id: new_event_id(),
                task_id: issue.id.clone(),
                timestamp: comment_ts,
                actor: comment
                    .author
                    .clone()
                    .unwrap_or_else(|| "beads-import".to_string()),
                event_type: EventType::CommentAdded,
                payload: serde_json::to_value(CommentPayload {
                    body: comment.text.clone(),
                })
                .unwrap(),
            };
            all_events.push(comment_event);
            report.comments_imported += 1;
        }
    }

    // -- Pass 2: Relationships --
    for issue in &issues {
        let dep_ts = issue
            .updated_at
            .as_deref()
            .map(parse_iso_ts)
            .unwrap_or_else(|| parse_iso_ts(&issue.created_at));

        for dep in &issue.dependencies {
            let dep_created_ts = dep
                .created_at
                .as_deref()
                .map(parse_iso_ts)
                .unwrap_or(dep_ts);

            match dep.dep_type.as_str() {
                "blocks" => {
                    // beads: issue_id blocks depends_on_id
                    // → brain: depends_on_id depends on issue_id
                    if !issue_ids.contains_key(dep.depends_on_id.as_str()) {
                        tracing::warn!(
                            "skipping block dep: {} blocks {} (target not in import set)",
                            dep.issue_id,
                            dep.depends_on_id
                        );
                        report.deps_skipped += 1;
                        continue;
                    }
                    if !issue_ids.contains_key(dep.issue_id.as_str()) {
                        tracing::warn!(
                            "skipping block dep: {} blocks {} (source not in import set)",
                            dep.issue_id,
                            dep.depends_on_id
                        );
                        report.deps_skipped += 1;
                        continue;
                    }
                    let dep_event = TaskEvent {
                        event_id: new_event_id(),
                        task_id: dep.depends_on_id.clone(),
                        timestamp: dep_created_ts,
                        actor: "beads-import".to_string(),
                        event_type: EventType::DependencyAdded,
                        payload: serde_json::to_value(DependencyPayload {
                            depends_on_task_id: dep.issue_id.clone(),
                        })
                        .unwrap(),
                    };
                    all_events.push(dep_event);
                    report.deps_imported += 1;
                }
                "parent-child" => {
                    // beads: issue_id is child, depends_on_id is parent
                    if !issue_ids.contains_key(dep.depends_on_id.as_str()) {
                        tracing::warn!(
                            "skipping parent-child: {} → parent {} (parent not in import set)",
                            dep.issue_id,
                            dep.depends_on_id
                        );
                        report.deps_skipped += 1;
                        continue;
                    }
                    if !issue_ids.contains_key(dep.issue_id.as_str()) {
                        tracing::warn!(
                            "skipping parent-child: {} → parent {} (child not in import set)",
                            dep.issue_id,
                            dep.depends_on_id
                        );
                        report.deps_skipped += 1;
                        continue;
                    }
                    let parent_event = TaskEvent {
                        event_id: new_event_id(),
                        task_id: dep.issue_id.clone(),
                        timestamp: dep_created_ts,
                        actor: "beads-import".to_string(),
                        event_type: EventType::ParentSet,
                        payload: serde_json::to_value(ParentSetPayload {
                            parent_task_id: Some(dep.depends_on_id.clone()),
                        })
                        .unwrap(),
                    };
                    all_events.push(parent_event);
                    report.parent_links_imported += 1;
                }
                other => {
                    tracing::warn!(
                        "skipping unknown dep type '{}': {} → {}",
                        other,
                        dep.issue_id,
                        dep.depends_on_id
                    );
                    report.deps_skipped += 1;
                }
            }
        }
    }

    // -- Pass 3: Status changes --
    for issue in &issues {
        match issue.status.as_str() {
            "closed" => {
                let closed_ts = issue
                    .closed_at
                    .as_deref()
                    .or(issue.updated_at.as_deref())
                    .map(parse_iso_ts)
                    .unwrap_or_else(|| parse_iso_ts(&issue.created_at));

                let status_event = TaskEvent {
                    event_id: new_event_id(),
                    task_id: issue.id.clone(),
                    timestamp: closed_ts,
                    actor: "beads-import".to_string(),
                    event_type: EventType::StatusChanged,
                    payload: serde_json::to_value(StatusChangedPayload {
                        new_status: "done".to_string(),
                    })
                    .unwrap(),
                };
                all_events.push(status_event);

                // Add close_reason as a comment if present
                if let Some(reason) = &issue.close_reason
                    && !reason.is_empty()
                {
                    let comment_event = TaskEvent {
                        event_id: new_event_id(),
                        task_id: issue.id.clone(),
                        timestamp: closed_ts,
                        actor: "beads-import".to_string(),
                        event_type: EventType::CommentAdded,
                        payload: serde_json::to_value(CommentPayload {
                            body: format!("[close_reason] {reason}"),
                        })
                        .unwrap(),
                    };
                    all_events.push(comment_event);
                }
            }
            "in_progress" => {
                let updated_ts = issue
                    .updated_at
                    .as_deref()
                    .map(parse_iso_ts)
                    .unwrap_or_else(|| parse_iso_ts(&issue.created_at));

                let status_event = TaskEvent {
                    event_id: new_event_id(),
                    task_id: issue.id.clone(),
                    timestamp: updated_ts,
                    actor: "beads-import".to_string(),
                    event_type: EventType::StatusChanged,
                    payload: serde_json::to_value(StatusChangedPayload {
                        new_status: "in_progress".to_string(),
                    })
                    .unwrap(),
                };
                all_events.push(status_event);
            }
            _ => {} // "open" is the default, no status change needed
        }
    }

    report.events_generated = all_events.len();
    Ok((all_events, report))
}

/// Import beads issues into the brain task system (one-time import).
///
/// Generates events from beads JSONL and appends them to brain's event log.
pub fn import_beads_issues(
    jsonl_path: &Path,
    task_store: &TaskStore,
    dry_run: bool,
) -> Result<ImportReport> {
    let (all_events, report) = generate_events_from_beads(jsonl_path)?;

    if dry_run {
        return Ok(report);
    }

    // Apply all events via append (validates + writes to JSONL + updates projection)
    for event in &all_events {
        task_store.append(event).map_err(|e| {
            BrainCoreError::TaskEvent(format!(
                "failed to apply event for task {}: {e}",
                event.task_id
            ))
        })?;
    }

    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Db;
    use std::io::Write;
    use tempfile::TempDir;

    fn setup() -> (TempDir, TaskStore) {
        let dir = TempDir::new().unwrap();
        let db = Db::open_in_memory().unwrap();
        let tasks_dir = dir.path().join("tasks");
        let store = TaskStore::new(&tasks_dir, db).unwrap();
        (dir, store)
    }

    fn write_jsonl(dir: &Path, issues: &[serde_json::Value]) -> std::path::PathBuf {
        let path = dir.join("issues.jsonl");
        let mut file = std::fs::File::create(&path).unwrap();
        for issue in issues {
            writeln!(file, "{}", serde_json::to_string(issue).unwrap()).unwrap();
        }
        path
    }

    fn make_issue(id: &str, title: &str, status: &str, priority: i32) -> serde_json::Value {
        serde_json::json!({
            "id": id,
            "title": title,
            "description": format!("Description for {title}"),
            "status": status,
            "priority": priority,
            "issue_type": "task",
            "created_at": "2026-02-25T10:00:00Z",
            "updated_at": "2026-02-25T12:00:00Z",
        })
    }

    #[test]
    fn test_import_basic_open_issues() {
        let (dir, store) = setup();
        let issues = vec![
            make_issue("t1", "Task 1", "open", 2),
            make_issue("t2", "Task 2", "open", 1),
        ];
        let path = write_jsonl(dir.path(), &issues);

        let report = import_beads_issues(&path, &store, false).unwrap();
        assert_eq!(report.issues_imported, 2);
        assert!(report.events_generated >= 2);

        let all = store.list_all().unwrap();
        assert_eq!(all.len(), 2);

        let t1 = store.get_task("t1").unwrap().unwrap();
        assert_eq!(t1.title, "Task 1");
        assert_eq!(t1.status, "open");
        assert_eq!(t1.priority, 2);
    }

    #[test]
    fn test_import_closed_maps_to_done() {
        let (dir, store) = setup();
        let mut issue = make_issue("t1", "Closed Task", "closed", 1);
        issue["closed_at"] = serde_json::json!("2026-02-26T10:00:00Z");
        issue["close_reason"] = serde_json::json!("All done");
        let path = write_jsonl(dir.path(), &[issue]);

        let report = import_beads_issues(&path, &store, false).unwrap();
        assert_eq!(report.issues_imported, 1);

        let t = store.get_task("t1").unwrap().unwrap();
        assert_eq!(t.status, "done");

        // close_reason should be a comment
        let comments = store.get_task_comments("t1").unwrap();
        assert!(comments.iter().any(|c| c.body.contains("[close_reason]")));
    }

    #[test]
    fn test_import_in_progress_status() {
        let (dir, store) = setup();
        let issue = make_issue("t1", "Active Task", "in_progress", 2);
        let path = write_jsonl(dir.path(), &[issue]);

        let report = import_beads_issues(&path, &store, false).unwrap();
        assert_eq!(report.issues_imported, 1);

        let t = store.get_task("t1").unwrap().unwrap();
        assert_eq!(t.status, "in_progress");
    }

    #[test]
    fn test_import_block_dependency_direction() {
        let (dir, store) = setup();
        // t1 blocks t2 → in brain: t2 depends_on t1
        let mut t1 = make_issue("t1", "Blocker", "open", 1);
        t1["dependencies"] = serde_json::json!([{
            "issue_id": "t1",
            "depends_on_id": "t2",
            "type": "blocks",
            "created_at": "2026-02-25T10:00:00Z"
        }]);
        let t2 = make_issue("t2", "Blocked", "open", 2);
        let path = write_jsonl(dir.path(), &[t1, t2]);

        let report = import_beads_issues(&path, &store, false).unwrap();
        assert_eq!(report.deps_imported, 1);

        // t1 should be ready, t2 should be blocked
        let ready = store.list_ready().unwrap();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].task_id, "t1");

        let blocked = store.list_blocked().unwrap();
        assert_eq!(blocked.len(), 1);
        assert_eq!(blocked[0].task_id, "t2");
    }

    #[test]
    fn test_import_parent_child_creates_parent_set() {
        let (dir, store) = setup();
        // child depends_on parent via parent-child type
        let mut child = make_issue("child-1", "Child Task", "open", 2);
        child["dependencies"] = serde_json::json!([{
            "issue_id": "child-1",
            "depends_on_id": "parent-1",
            "type": "parent-child",
            "created_at": "2026-02-25T10:00:00Z"
        }]);
        let parent = make_issue("parent-1", "Parent Epic", "open", 1);
        // parent must come first so it exists when child's ParentSet is applied
        let path = write_jsonl(dir.path(), &[parent, child]);

        let report = import_beads_issues(&path, &store, false).unwrap();
        assert_eq!(report.parent_links_imported, 1);
        assert_eq!(report.deps_imported, 0); // parent-child is NOT a block dep

        // Verify parent_task_id is set
        let child_row = store.get_task("child-1").unwrap().unwrap();
        assert_eq!(child_row.parent_task_id.as_deref(), Some("parent-1"));

        // Verify children query
        let children = store.get_children("parent-1").unwrap();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].task_id, "child-1");
    }

    #[test]
    fn test_parent_child_does_not_affect_readiness() {
        let (dir, store) = setup();
        let mut child = make_issue("child-1", "Child Task", "open", 2);
        child["dependencies"] = serde_json::json!([{
            "issue_id": "child-1",
            "depends_on_id": "parent-1",
            "type": "parent-child"
        }]);
        let parent = make_issue("parent-1", "Parent Epic", "open", 1);
        let path = write_jsonl(dir.path(), &[parent, child]);

        import_beads_issues(&path, &store, false).unwrap();

        // Both should be ready — parent-child is NOT a blocking relationship
        let ready = store.list_ready().unwrap();
        assert_eq!(ready.len(), 2);
    }

    #[test]
    fn test_import_labels() {
        let (dir, store) = setup();
        let mut issue = make_issue("t1", "Labeled", "open", 2);
        issue["labels"] = serde_json::json!(["urgent", "backend"]);
        let path = write_jsonl(dir.path(), &[issue]);

        let report = import_beads_issues(&path, &store, false).unwrap();
        assert_eq!(report.labels_imported, 2);

        let labels = store.get_task_labels("t1").unwrap();
        assert_eq!(labels.len(), 2);
        assert!(labels.contains(&"urgent".to_string()));
        assert!(labels.contains(&"backend".to_string()));
    }

    #[test]
    fn test_import_comments() {
        let (dir, store) = setup();
        let mut issue = make_issue("t1", "Commented", "open", 2);
        issue["comments"] = serde_json::json!([
            {"author": "alice", "text": "First comment", "created_at": "2026-02-25T11:00:00Z"},
            {"author": "bob", "text": "Second comment", "created_at": "2026-02-25T12:00:00Z"}
        ]);
        let path = write_jsonl(dir.path(), &[issue]);

        let report = import_beads_issues(&path, &store, false).unwrap();
        assert_eq!(report.comments_imported, 2);

        let comments = store.get_task_comments("t1").unwrap();
        assert_eq!(comments.len(), 2);
        assert_eq!(comments[0].body, "First comment");
        assert_eq!(comments[0].author, "alice");
        assert_eq!(comments[1].body, "Second comment");
    }

    #[test]
    fn test_import_notes_appended_to_description() {
        let (dir, store) = setup();
        let issue = serde_json::json!({
            "id": "t1",
            "title": "With notes",
            "description": "Main description",
            "notes": "Some implementation notes",
            "design": "Architecture decision",
            "status": "open",
            "priority": 2,
            "created_at": "2026-02-25T10:00:00Z"
        });
        let path = write_jsonl(dir.path(), &[issue]);

        import_beads_issues(&path, &store, false).unwrap();

        let t = store.get_task("t1").unwrap().unwrap();
        let desc = t.description.unwrap();
        assert!(desc.contains("Main description"));
        assert!(desc.contains("## Notes"));
        assert!(desc.contains("Some implementation notes"));
        assert!(desc.contains("## Design"));
        assert!(desc.contains("Architecture decision"));
    }

    #[test]
    fn test_import_skips_missing_dep_targets() {
        let (dir, store) = setup();
        let mut issue = make_issue("t1", "Has dep on missing", "open", 2);
        issue["dependencies"] = serde_json::json!([{
            "issue_id": "t1",
            "depends_on_id": "nonexistent",
            "type": "blocks"
        }]);
        let path = write_jsonl(dir.path(), &[issue]);

        let report = import_beads_issues(&path, &store, false).unwrap();
        assert_eq!(report.deps_skipped, 1);
        assert_eq!(report.deps_imported, 0);
    }

    #[test]
    fn test_dry_run_no_side_effects() {
        let (dir, store) = setup();
        let issues = vec![
            make_issue("t1", "Task 1", "open", 2),
            make_issue("t2", "Task 2", "closed", 1),
        ];
        let path = write_jsonl(dir.path(), &issues);

        let report = import_beads_issues(&path, &store, true).unwrap();
        assert_eq!(report.issues_imported, 2);
        assert!(report.events_generated > 0);

        // No tasks should exist in the store
        let all = store.list_all().unwrap();
        assert!(all.is_empty());
    }

    #[test]
    fn test_import_issue_type_preserved() {
        let (dir, store) = setup();
        let mut issue = make_issue("t1", "Epic", "open", 0);
        issue["issue_type"] = serde_json::json!("epic");
        let path = write_jsonl(dir.path(), &[issue]);

        import_beads_issues(&path, &store, false).unwrap();

        let t = store.get_task("t1").unwrap().unwrap();
        assert_eq!(t.task_type, "epic");
    }

    // -- sync tests (projection-only rebuild from beads) --

    #[test]
    fn test_sync_idempotent() {
        let (dir, store) = setup();
        let issues = vec![
            make_issue("t1", "Task 1", "open", 2),
            make_issue("t2", "Task 2", "closed", 1),
        ];
        let path = write_jsonl(dir.path(), &issues);

        let r1 = store.sync_from_beads(&path).unwrap();
        assert_eq!(r1.issues_imported, 2);

        let all1 = store.list_all().unwrap();
        assert_eq!(all1.len(), 2);

        // Sync again — same result
        let r2 = store.sync_from_beads(&path).unwrap();
        assert_eq!(r2.issues_imported, 2);

        let all2 = store.list_all().unwrap();
        assert_eq!(all2.len(), 2);

        // Verify data integrity after double sync
        let t1 = store.get_task("t1").unwrap().unwrap();
        assert_eq!(t1.title, "Task 1");
        assert_eq!(t1.status, "open");
        let t2 = store.get_task("t2").unwrap().unwrap();
        assert_eq!(t2.status, "done");
    }

    #[test]
    fn test_sync_picks_up_new_issue() {
        let (dir, store) = setup();
        let issues = vec![make_issue("t1", "Task 1", "open", 2)];
        let path = write_jsonl(dir.path(), &issues);

        store.sync_from_beads(&path).unwrap();
        assert_eq!(store.list_all().unwrap().len(), 1);

        // Add a second issue and re-sync
        let issues = vec![
            make_issue("t1", "Task 1", "open", 2),
            make_issue("t2", "Task 2", "open", 1),
        ];
        write_jsonl(dir.path(), &issues);

        store.sync_from_beads(&path).unwrap();
        assert_eq!(store.list_all().unwrap().len(), 2);
        assert!(store.get_task("t2").unwrap().is_some());
    }

    #[test]
    fn test_sync_picks_up_status_change() {
        let (dir, store) = setup();
        let issues = vec![make_issue("t1", "Task 1", "open", 2)];
        let path = write_jsonl(dir.path(), &issues);

        store.sync_from_beads(&path).unwrap();
        assert_eq!(store.get_task("t1").unwrap().unwrap().status, "open");

        // Change status to closed and re-sync
        let mut closed = make_issue("t1", "Task 1", "closed", 2);
        closed["closed_at"] = serde_json::json!("2026-02-26T10:00:00Z");
        write_jsonl(dir.path(), &[closed]);

        store.sync_from_beads(&path).unwrap();
        assert_eq!(store.get_task("t1").unwrap().unwrap().status, "done");
    }

    #[test]
    fn test_sync_removes_deleted_issue() {
        let (dir, store) = setup();
        let issues = vec![
            make_issue("t1", "Task 1", "open", 2),
            make_issue("t2", "Task 2", "open", 1),
        ];
        let path = write_jsonl(dir.path(), &issues);

        store.sync_from_beads(&path).unwrap();
        assert_eq!(store.list_all().unwrap().len(), 2);

        // Remove t2 from JSONL and re-sync
        let issues = vec![make_issue("t1", "Task 1", "open", 2)];
        write_jsonl(dir.path(), &issues);

        store.sync_from_beads(&path).unwrap();
        let all = store.list_all().unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].task_id, "t1");
        assert!(store.get_task("t2").unwrap().is_none());
    }

    #[test]
    fn test_generate_events_does_not_write() {
        let (dir, _store) = setup();
        let issues = vec![
            make_issue("t1", "Task 1", "open", 2),
            make_issue("t2", "Task 2", "closed", 1),
        ];
        let path = write_jsonl(dir.path(), &issues);

        let (events, report) = generate_events_from_beads(&path).unwrap();

        // Should have generated events without side effects
        assert_eq!(report.issues_imported, 2);
        assert!(!events.is_empty());

        // The store should still be empty (generate doesn't touch it)
        let store_all = _store.list_all().unwrap();
        assert!(store_all.is_empty());
    }
}
