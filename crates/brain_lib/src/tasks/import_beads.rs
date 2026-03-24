use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader};
use std::path::Path;

use serde::Deserialize;

use crate::error::{BrainCoreError, Result};

use super::TaskStore;
use super::events::{
    CommentPayload, DependencyPayload, EventType, ExternalIdPayload, LabelPayload,
    ParentSetPayload, StatusChangedPayload, TaskCreatedPayload, TaskEvent, TaskStatus, TaskType,
    TaskUpdatedPayload, new_task_id, now_ts,
};

/// Summary of an import run.
#[derive(Debug, Default)]
pub struct ImportReport {
    pub issues_imported: usize,
    pub issues_updated: usize,
    pub issues_skipped: usize,
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
        writeln!(f, "  Issues updated:       {}", self.issues_updated)?;
        writeln!(f, "  Issues skipped:       {}", self.issues_skipped)?;
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

    if let Some(desc) = &issue.description
        && !desc.is_empty()
    {
        parts.push(desc.clone());
    }

    if let Some(notes) = &issue.notes
        && !notes.is_empty()
    {
        parts.push(format!("## Notes\n\n{notes}"));
    }

    if let Some(design) = &issue.design
        && !design.is_empty()
    {
        parts.push(format!("## Design\n\n{design}"));
    }

    if let Some(ac) = &issue.acceptance_criteria
        && !ac.is_empty()
    {
        parts.push(format!("## Acceptance Criteria\n\n{ac}"));
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

        let created_event = TaskEvent::from_payload(
            issue.id.clone(),
            issue
                .owner
                .clone()
                .unwrap_or_else(|| "beads-import".to_string()),
            TaskCreatedPayload {
                title: issue.title.clone(),
                description,
                priority: issue.priority,
                status: TaskStatus::Open,
                due_ts: None,
                task_type: issue
                    .issue_type
                    .as_deref()
                    .and_then(|s| s.parse::<TaskType>().ok()),
                assignee: issue.owner.clone(),
                defer_until: None,
                parent_task_id: None, // Set in pass 2
                display_id: None,
            },
        )
        .with_timestamp(created_ts);
        all_events.push(created_event);
        report.issues_imported += 1;

        // Labels
        for label in &issue.labels {
            let label_event = TaskEvent::new(
                &issue.id,
                "beads-import",
                EventType::LabelAdded,
                &LabelPayload {
                    label: label.clone(),
                },
            )
            .with_timestamp(created_ts);
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
            let comment_event = TaskEvent::from_payload(
                issue.id.clone(),
                comment
                    .author
                    .clone()
                    .unwrap_or_else(|| "beads-import".to_string()),
                CommentPayload {
                    body: comment.text.clone(),
                },
            )
            .with_timestamp(comment_ts);
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
                    let dep_event = TaskEvent::new(
                        &dep.depends_on_id,
                        "beads-import",
                        EventType::DependencyAdded,
                        &DependencyPayload {
                            depends_on_task_id: dep.issue_id.clone(),
                        },
                    )
                    .with_timestamp(dep_created_ts);
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
                    let parent_event = TaskEvent::from_payload(
                        dep.issue_id.clone(),
                        "beads-import",
                        ParentSetPayload {
                            parent_task_id: Some(dep.depends_on_id.clone()),
                        },
                    )
                    .with_timestamp(dep_created_ts);
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

                all_events.push(
                    TaskEvent::from_payload(
                        &issue.id,
                        "beads-import",
                        StatusChangedPayload {
                            new_status: TaskStatus::Done,
                        },
                    )
                    .with_timestamp(closed_ts),
                );

                // Add close_reason as a comment if present
                if let Some(reason) = &issue.close_reason
                    && !reason.is_empty()
                {
                    all_events.push(
                        TaskEvent::from_payload(
                            &issue.id,
                            "beads-import",
                            CommentPayload {
                                body: format!("[close_reason] {reason}"),
                            },
                        )
                        .with_timestamp(closed_ts),
                    );
                }
            }
            "in_progress" => {
                let updated_ts = issue
                    .updated_at
                    .as_deref()
                    .map(parse_iso_ts)
                    .unwrap_or_else(|| parse_iso_ts(&issue.created_at));

                all_events.push(
                    TaskEvent::from_payload(
                        &issue.id,
                        "beads-import",
                        StatusChangedPayload {
                            new_status: TaskStatus::InProgress,
                        },
                    )
                    .with_timestamp(updated_ts),
                );
            }
            _ => {} // "open" is the default, no status change needed
        }
    }

    report.events_generated = all_events.len();
    Ok((all_events, report))
}

/// Import beads issues into the brain task system (idempotent).
///
/// - New issues: generates creation events and appends to brain's event log.
/// - Existing issues: detects field changes and generates delta events.
/// - Unchanged issues: skipped.
pub fn import_beads_issues(
    jsonl_path: &Path,
    task_store: &TaskStore,
    dry_run: bool,
) -> Result<ImportReport> {
    let issues = read_beads_issues(jsonl_path)?;
    let mut report = ImportReport::default();

    if issues.is_empty() {
        return Ok(report);
    }

    // Build beads_id → brain_id mapping:
    //   - Already-imported issues: resolve via external_ids table
    //   - New issues: generate a brain-native ID
    let prefix = task_store.get_project_prefix()?;
    let mut beads_to_brain: HashMap<String, String> = HashMap::new();
    for issue in &issues {
        if let Some(brain_id) = task_store.resolve_external_id("beads", &issue.id)? {
            beads_to_brain.insert(issue.id.clone(), brain_id);
        } else {
            beads_to_brain.insert(issue.id.clone(), new_task_id(&prefix));
        }
    }

    // Reverse lookup: brain_ids that came from beads (for dep diff filtering)
    let brain_ids_from_beads: HashSet<&str> = beads_to_brain.values().map(|s| s.as_str()).collect();

    // Load existing state for diffing (keyed by brain task_id)
    let existing_tasks: HashMap<String, super::queries::TaskRow> = task_store
        .list_all()?
        .into_iter()
        .map(|t| (t.task_id.clone(), t))
        .collect();

    let existing_labels: HashMap<String, HashSet<String>> = {
        let mut map: HashMap<String, HashSet<String>> = HashMap::new();
        for (task_id, label) in task_store.list_all_labels()? {
            map.entry(task_id).or_default().insert(label);
        }
        map
    };

    let existing_deps: HashMap<String, HashSet<String>> = {
        let mut map: HashMap<String, HashSet<String>> = HashMap::new();
        for dep in task_store.list_all_deps()? {
            map.entry(dep.task_id).or_default().insert(dep.depends_on);
        }
        map
    };

    // All beads IDs in this batch (for relationship validation)
    let beads_ids: HashSet<&str> = issues.iter().map(|i| i.id.as_str()).collect();

    // Build expected relationships using brain IDs
    // Beads deps reference beads IDs — translate through the mapping.
    let mut expected_deps: HashMap<String, HashSet<String>> = HashMap::new();
    let mut expected_parents: HashMap<String, String> = HashMap::new();

    for issue in &issues {
        for dep in &issue.dependencies {
            let has_both = beads_ids.contains(dep.depends_on_id.as_str())
                && beads_ids.contains(dep.issue_id.as_str());
            if !has_both {
                report.deps_skipped += 1;
                continue;
            }
            match dep.dep_type.as_str() {
                "blocks" => {
                    // "issue blocks depends_on" → in brain: depends_on depends_on issue
                    let brain_blocked = &beads_to_brain[&dep.depends_on_id];
                    let brain_blocker = &beads_to_brain[&dep.issue_id];
                    expected_deps
                        .entry(brain_blocked.clone())
                        .or_default()
                        .insert(brain_blocker.clone());
                }
                "parent-child" => {
                    let brain_child = &beads_to_brain[&dep.issue_id];
                    let brain_parent = &beads_to_brain[&dep.depends_on_id];
                    expected_parents.insert(brain_child.clone(), brain_parent.clone());
                }
                _ => {
                    report.deps_skipped += 1;
                }
            }
        }
    }

    // Collect events in 3 phases for correct ordering:
    // Phase 1: TaskCreated + labels + comments (new issues)
    // Phase 2: Field updates + status changes + label diffs (existing issues) + status for new
    // Phase 3: Relationship changes — deps + parents (all issues, needs all tasks to exist)
    let mut phase1: Vec<TaskEvent> = Vec::new();
    let mut phase2: Vec<TaskEvent> = Vec::new();
    let mut phase3: Vec<TaskEvent> = Vec::new();

    let now = now_ts();

    for issue in &issues {
        let brain_id = &beads_to_brain[&issue.id];

        if let Some(existing) = existing_tasks.get(brain_id) {
            // === EXISTING ISSUE — generate delta events ===
            let mut changed = false;

            // Field diffs → single TaskUpdated
            let new_desc = build_description(issue);
            let beads_task_type: TaskType = issue
                .issue_type
                .as_deref()
                .map(|s| {
                    s.parse().unwrap_or_else(|e| {
                        tracing::warn!(
                            beads_id = %issue.id,
                            raw_value = %s,
                            error = %e,
                            "invalid issue_type in beads import; defaulting to Task"
                        );
                        TaskType::Task
                    })
                })
                .unwrap_or(TaskType::Task);
            let mut upd = TaskUpdatedPayload {
                title: None,
                description: None,
                priority: None,
                due_ts: None,
                blocked_reason: None,
                task_type: None,
                assignee: None,
                defer_until: None,
            };
            let mut has_field = false;

            if issue.title != existing.title {
                upd.title = Some(issue.title.clone());
                has_field = true;
            }
            if new_desc != existing.description {
                upd.description = new_desc.or(Some(String::new()));
                has_field = true;
            }
            if issue.priority != existing.priority {
                upd.priority = Some(issue.priority);
                has_field = true;
            }
            if beads_task_type != existing.task_type {
                upd.task_type = Some(beads_task_type);
                has_field = true;
            }
            if issue.owner != existing.assignee {
                upd.assignee = Some(issue.owner.clone().unwrap_or_default());
                has_field = true;
            }

            if has_field {
                phase2.push(
                    TaskEvent::from_payload(brain_id, "beads-import", upd).with_timestamp(now),
                );
                changed = true;
            }

            // Status diff
            let brain_status = match issue.status.as_str() {
                "closed" => TaskStatus::Done,
                "in_progress" => TaskStatus::InProgress,
                _ => TaskStatus::Open,
            };
            if brain_status.as_ref() != existing.status {
                phase2.push(
                    TaskEvent::from_payload(
                        brain_id,
                        "beads-import",
                        StatusChangedPayload {
                            new_status: brain_status,
                        },
                    )
                    .with_timestamp(now),
                );
                changed = true;
            }

            // Label diffs
            let empty_labels = HashSet::new();
            let cur_labels = existing_labels.get(brain_id).unwrap_or(&empty_labels);
            let beads_labels: HashSet<&str> = issue.labels.iter().map(|l| l.as_str()).collect();
            let cur_labels_ref: HashSet<&str> = cur_labels.iter().map(|l| l.as_str()).collect();

            for label in beads_labels.difference(&cur_labels_ref) {
                phase2.push(
                    TaskEvent::new(
                        brain_id,
                        "beads-import",
                        EventType::LabelAdded,
                        &LabelPayload {
                            label: label.to_string(),
                        },
                    )
                    .with_timestamp(now),
                );
                changed = true;
            }
            for label in cur_labels_ref.difference(&beads_labels) {
                phase2.push(
                    TaskEvent::new(
                        brain_id,
                        "beads-import",
                        EventType::LabelRemoved,
                        &LabelPayload {
                            label: label.to_string(),
                        },
                    )
                    .with_timestamp(now),
                );
                changed = true;
            }

            // Dep diffs (only beads-originated deps)
            let empty_deps = HashSet::new();
            let cur_deps = existing_deps.get(brain_id).unwrap_or(&empty_deps);
            let cur_beads_deps: HashSet<&str> = cur_deps
                .iter()
                .filter(|d| brain_ids_from_beads.contains(d.as_str()))
                .map(|d| d.as_str())
                .collect();
            let exp_beads_deps: HashSet<&str> = expected_deps
                .get(brain_id)
                .map(|ds| ds.iter().map(|d| d.as_str()).collect())
                .unwrap_or_default();

            for dep in exp_beads_deps.difference(&cur_beads_deps) {
                phase3.push(
                    TaskEvent::new(
                        brain_id,
                        "beads-import",
                        EventType::DependencyAdded,
                        &DependencyPayload {
                            depends_on_task_id: dep.to_string(),
                        },
                    )
                    .with_timestamp(now),
                );
                changed = true;
            }
            for dep in cur_beads_deps.difference(&exp_beads_deps) {
                phase3.push(
                    TaskEvent::new(
                        brain_id,
                        "beads-import",
                        EventType::DependencyRemoved,
                        &DependencyPayload {
                            depends_on_task_id: dep.to_string(),
                        },
                    )
                    .with_timestamp(now),
                );
                changed = true;
            }

            // Parent diff (only when beads-related)
            let exp_parent = expected_parents.get(brain_id).map(|p| p.as_str());
            let cur_parent = existing.parent_task_id.as_deref();
            let exp_is_beads = exp_parent.is_some_and(|p| brain_ids_from_beads.contains(p));
            let cur_is_beads = cur_parent.is_some_and(|p| brain_ids_from_beads.contains(p));

            if (exp_is_beads || cur_is_beads) && exp_parent != cur_parent {
                phase3.push(
                    TaskEvent::from_payload(
                        brain_id,
                        "beads-import",
                        ParentSetPayload {
                            parent_task_id: exp_parent.map(|p| p.to_string()),
                        },
                    )
                    .with_timestamp(now),
                );
                changed = true;
            }

            if changed {
                report.issues_updated += 1;
            } else {
                report.issues_skipped += 1;
            }
        } else {
            // === NEW ISSUE — generate creation events ===
            let created_ts = parse_iso_ts(&issue.created_at);
            let description = build_description(issue);

            phase1.push(
                TaskEvent::from_payload(
                    brain_id,
                    issue
                        .owner
                        .clone()
                        .unwrap_or_else(|| "beads-import".to_string()),
                    TaskCreatedPayload {
                        title: issue.title.clone(),
                        description,
                        priority: issue.priority,
                        status: TaskStatus::Open,
                        due_ts: None,
                        task_type: issue
                            .issue_type
                            .as_deref()
                            .and_then(|s| s.parse::<TaskType>().ok()),
                        assignee: issue.owner.clone(),
                        defer_until: None,
                        parent_task_id: None,
                        display_id: None,
                    },
                )
                .with_timestamp(created_ts),
            );

            // Track beads origin as external ID
            phase1.push(
                TaskEvent::new(
                    brain_id,
                    "beads-import",
                    EventType::ExternalIdAdded,
                    &ExternalIdPayload {
                        source: "beads".to_string(),
                        external_id: issue.id.clone(),
                        external_url: None,
                    },
                )
                .with_timestamp(created_ts),
            );
            report.issues_imported += 1;

            // Labels
            for label in &issue.labels {
                phase1.push(
                    TaskEvent::new(
                        brain_id,
                        "beads-import",
                        EventType::LabelAdded,
                        &LabelPayload {
                            label: label.clone(),
                        },
                    )
                    .with_timestamp(created_ts),
                );
                report.labels_imported += 1;
            }

            // Comments
            for comment in &issue.comments {
                let comment_ts = comment
                    .created_at
                    .as_deref()
                    .map(parse_iso_ts)
                    .unwrap_or(created_ts);
                phase1.push(
                    TaskEvent::from_payload(
                        brain_id,
                        comment
                            .author
                            .clone()
                            .unwrap_or_else(|| "beads-import".to_string()),
                        CommentPayload {
                            body: comment.text.clone(),
                        },
                    )
                    .with_timestamp(comment_ts),
                );
                report.comments_imported += 1;
            }

            // Status changes (phase 2 — after all creates)
            match issue.status.as_str() {
                "closed" => {
                    let closed_ts = issue
                        .closed_at
                        .as_deref()
                        .or(issue.updated_at.as_deref())
                        .map(parse_iso_ts)
                        .unwrap_or(created_ts);

                    phase2.push(
                        TaskEvent::from_payload(
                            brain_id,
                            "beads-import",
                            StatusChangedPayload {
                                new_status: TaskStatus::Done,
                            },
                        )
                        .with_timestamp(closed_ts),
                    );

                    if let Some(reason) = &issue.close_reason
                        && !reason.is_empty()
                    {
                        phase2.push(
                            TaskEvent::from_payload(
                                brain_id,
                                "beads-import",
                                CommentPayload {
                                    body: format!("[close_reason] {reason}"),
                                },
                            )
                            .with_timestamp(closed_ts),
                        );
                    }
                }
                "in_progress" => {
                    let updated_ts = issue
                        .updated_at
                        .as_deref()
                        .map(parse_iso_ts)
                        .unwrap_or(created_ts);

                    phase2.push(
                        TaskEvent::from_payload(
                            brain_id,
                            "beads-import",
                            StatusChangedPayload {
                                new_status: TaskStatus::InProgress,
                            },
                        )
                        .with_timestamp(updated_ts),
                    );
                }
                _ => {}
            }

            // Relationships (phase 3 — after all tasks exist)
            if let Some(deps) = expected_deps.get(brain_id) {
                for dep_on in deps {
                    phase3.push(
                        TaskEvent::new(
                            brain_id,
                            "beads-import",
                            EventType::DependencyAdded,
                            &DependencyPayload {
                                depends_on_task_id: dep_on.clone(),
                            },
                        )
                        .with_timestamp(now),
                    );
                    report.deps_imported += 1;
                }
            }

            if let Some(parent) = expected_parents.get(brain_id) {
                phase3.push(
                    TaskEvent::from_payload(
                        brain_id,
                        "beads-import",
                        ParentSetPayload {
                            parent_task_id: Some(parent.clone()),
                        },
                    )
                    .with_timestamp(now),
                );
                report.parent_links_imported += 1;
            }
        }
    }

    report.events_generated = phase1.len() + phase2.len() + phase3.len();

    if dry_run {
        return Ok(report);
    }

    // Append events in order: creates → updates/status → relationships
    for event in phase1.iter().chain(phase2.iter()).chain(phase3.iter()) {
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
        let _tasks_dir = dir.path().join("tasks");
        let store = TaskStore::new(db);
        (dir, store)
    }

    /// Look up a brain task by its original beads ID via the external_ids table.
    fn get_by_beads_id(store: &TaskStore, beads_id: &str) -> super::super::queries::TaskRow {
        let brain_id = store
            .resolve_external_id("beads", beads_id)
            .unwrap()
            .unwrap_or_else(|| panic!("no brain task for beads ID '{beads_id}'"));
        store.get_task(&brain_id).unwrap().unwrap()
    }

    /// Resolve a beads ID to its brain task_id.
    fn brain_id(store: &TaskStore, beads_id: &str) -> String {
        store
            .resolve_external_id("beads", beads_id)
            .unwrap()
            .unwrap_or_else(|| panic!("no brain task for beads ID '{beads_id}'"))
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

        let t1 = get_by_beads_id(&store, "t1");
        assert_eq!(t1.title, "Task 1");
        assert_eq!(t1.status, "open");
        assert_eq!(t1.priority, 2);
        // Brain ID should NOT be the beads ID
        assert_ne!(t1.task_id, "t1");
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

        let t = get_by_beads_id(&store, "t1");
        assert_eq!(t.status, "done");

        // close_reason should be a comment
        let bid = brain_id(&store, "t1");
        let comments = store.get_task_comments(&bid).unwrap();
        assert!(comments.iter().any(|c| c.body.contains("[close_reason]")));
    }

    #[test]
    fn test_import_in_progress_status() {
        let (dir, store) = setup();
        let issue = make_issue("t1", "Active Task", "in_progress", 2);
        let path = write_jsonl(dir.path(), &[issue]);

        let report = import_beads_issues(&path, &store, false).unwrap();
        assert_eq!(report.issues_imported, 1);

        let t = get_by_beads_id(&store, "t1");
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

        let bid_t1 = brain_id(&store, "t1");
        let bid_t2 = brain_id(&store, "t2");

        // t1 should be ready, t2 should be blocked
        let ready = store.list_ready().unwrap();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].task_id, bid_t1);

        let blocked = store.list_blocked().unwrap();
        assert_eq!(blocked.len(), 1);
        assert_eq!(blocked[0].task_id, bid_t2);
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

        let bid_child = brain_id(&store, "child-1");
        let bid_parent = brain_id(&store, "parent-1");

        // Verify parent_task_id is set
        let child_row = store.get_task(&bid_child).unwrap().unwrap();
        assert_eq!(
            child_row.parent_task_id.as_deref(),
            Some(bid_parent.as_str())
        );

        // Verify children query
        let children = store.get_children(&bid_parent).unwrap();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].task_id, bid_child);
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

        let bid = brain_id(&store, "t1");
        let labels = store.get_task_labels(&bid).unwrap();
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

        let bid = brain_id(&store, "t1");
        let comments = store.get_task_comments(&bid).unwrap();
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

        let t = get_by_beads_id(&store, "t1");
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

        let t = get_by_beads_id(&store, "t1");
        assert_eq!(t.task_type, TaskType::Epic);
    }

    // -- idempotent import tests --

    #[test]
    fn test_import_idempotent() {
        let (dir, store) = setup();
        let issues = vec![
            make_issue("t1", "Task 1", "open", 2),
            make_issue("t2", "Task 2", "closed", 1),
        ];
        let path = write_jsonl(dir.path(), &issues);

        let r1 = import_beads_issues(&path, &store, false).unwrap();
        assert_eq!(r1.issues_imported, 2);
        assert_eq!(r1.issues_skipped, 0);
        assert_eq!(r1.issues_updated, 0);
        assert_eq!(store.list_all().unwrap().len(), 2);

        // Import again with same data — all skipped
        let r2 = import_beads_issues(&path, &store, false).unwrap();
        assert_eq!(r2.issues_imported, 0);
        assert_eq!(r2.issues_skipped, 2);
        assert_eq!(r2.issues_updated, 0);
        assert_eq!(r2.events_generated, 0);

        // Data unchanged
        let t1 = get_by_beads_id(&store, "t1");
        assert_eq!(t1.title, "Task 1");
        assert_eq!(t1.status, "open");
        let t2 = get_by_beads_id(&store, "t2");
        assert_eq!(t2.status, "done");
    }

    #[test]
    fn test_import_picks_up_new_issues() {
        let (dir, store) = setup();
        let issues = vec![
            make_issue("t1", "Task 1", "open", 2),
            make_issue("t2", "Task 2", "open", 1),
        ];
        let path = write_jsonl(dir.path(), &issues);

        let r1 = import_beads_issues(&path, &store, false).unwrap();
        assert_eq!(r1.issues_imported, 2);
        assert_eq!(store.list_all().unwrap().len(), 2);

        // Add a third issue, re-import
        let issues = vec![
            make_issue("t1", "Task 1", "open", 2),
            make_issue("t2", "Task 2", "open", 1),
            make_issue("t3", "Task 3", "open", 3),
        ];
        write_jsonl(dir.path(), &issues);

        let r2 = import_beads_issues(&path, &store, false).unwrap();
        assert_eq!(r2.issues_imported, 1);
        assert_eq!(r2.issues_skipped, 2);
        assert_eq!(r2.issues_updated, 0);
        assert_eq!(store.list_all().unwrap().len(), 3);
        // t3 should now have a brain ID
        assert!(store.resolve_external_id("beads", "t3").unwrap().is_some());
    }

    #[test]
    fn test_import_detects_title_update() {
        let (dir, store) = setup();
        let issues = vec![make_issue("t1", "Original Title", "open", 2)];
        let path = write_jsonl(dir.path(), &issues);
        import_beads_issues(&path, &store, false).unwrap();
        assert_eq!(get_by_beads_id(&store, "t1").title, "Original Title");

        // Change title in beads
        let issues = vec![make_issue("t1", "Updated Title", "open", 2)];
        write_jsonl(dir.path(), &issues);

        let r = import_beads_issues(&path, &store, false).unwrap();
        assert_eq!(r.issues_updated, 1);
        assert_eq!(r.issues_imported, 0);
        assert_eq!(r.issues_skipped, 0);

        let t = get_by_beads_id(&store, "t1");
        assert_eq!(t.title, "Updated Title");
    }

    #[test]
    fn test_import_detects_status_change() {
        let (dir, store) = setup();
        let issues = vec![make_issue("t1", "Task 1", "open", 2)];
        let path = write_jsonl(dir.path(), &issues);
        import_beads_issues(&path, &store, false).unwrap();
        assert_eq!(get_by_beads_id(&store, "t1").status, "open");

        // Close in beads
        let mut closed = make_issue("t1", "Task 1", "closed", 2);
        closed["closed_at"] = serde_json::json!("2026-02-26T10:00:00Z");
        write_jsonl(dir.path(), &[closed]);

        let r = import_beads_issues(&path, &store, false).unwrap();
        assert_eq!(r.issues_updated, 1);

        let t = get_by_beads_id(&store, "t1");
        assert_eq!(t.status, "done");
    }

    #[test]
    fn test_import_detects_label_changes() {
        let (dir, store) = setup();
        let mut issue = make_issue("t1", "Labeled", "open", 2);
        issue["labels"] = serde_json::json!(["urgent", "backend"]);
        let path = write_jsonl(dir.path(), &[issue]);
        import_beads_issues(&path, &store, false).unwrap();

        let bid = brain_id(&store, "t1");
        let labels = store.get_task_labels(&bid).unwrap();
        assert_eq!(labels.len(), 2);
        assert!(labels.contains(&"urgent".to_string()));
        assert!(labels.contains(&"backend".to_string()));

        // Change labels: remove "urgent", add "frontend"
        let mut issue = make_issue("t1", "Labeled", "open", 2);
        issue["labels"] = serde_json::json!(["backend", "frontend"]);
        write_jsonl(dir.path(), &[issue]);

        let r = import_beads_issues(&path, &store, false).unwrap();
        assert_eq!(r.issues_updated, 1);

        let labels = store.get_task_labels(&bid).unwrap();
        assert_eq!(labels.len(), 2);
        assert!(labels.contains(&"backend".to_string()));
        assert!(labels.contains(&"frontend".to_string()));
        assert!(!labels.contains(&"urgent".to_string()));
    }

    #[test]
    fn test_import_detects_priority_change() {
        let (dir, store) = setup();
        let issues = vec![make_issue("t1", "Task 1", "open", 2)];
        let path = write_jsonl(dir.path(), &issues);
        import_beads_issues(&path, &store, false).unwrap();
        assert_eq!(get_by_beads_id(&store, "t1").priority, 2);

        // Change priority
        let issues = vec![make_issue("t1", "Task 1", "open", 0)];
        write_jsonl(dir.path(), &issues);

        let r = import_beads_issues(&path, &store, false).unwrap();
        assert_eq!(r.issues_updated, 1);

        let t = get_by_beads_id(&store, "t1");
        assert_eq!(t.priority, 0);
    }

    #[test]
    fn test_import_detects_dep_changes() {
        let (dir, store) = setup();
        // t1 blocks t2 initially
        let mut t1 = make_issue("t1", "Blocker", "open", 1);
        t1["dependencies"] = serde_json::json!([{
            "issue_id": "t1",
            "depends_on_id": "t2",
            "type": "blocks",
            "created_at": "2026-02-25T10:00:00Z"
        }]);
        let t2 = make_issue("t2", "Blocked", "open", 2);
        let t3 = make_issue("t3", "Other", "open", 3);
        let path = write_jsonl(dir.path(), &[t1, t2, t3]);
        import_beads_issues(&path, &store, false).unwrap();

        let bid_t1 = brain_id(&store, "t1");
        let bid_t2 = brain_id(&store, "t2");
        let bid_t3 = brain_id(&store, "t3");

        // t2 should be blocked by t1
        let blocked = store.list_blocked().unwrap();
        assert_eq!(blocked.len(), 1);
        assert_eq!(blocked[0].task_id, bid_t2);

        // Change deps: t1 no longer blocks t2, now t3 blocks t2
        let t1 = make_issue("t1", "Blocker", "open", 1); // no deps
        let t2 = make_issue("t2", "Blocked", "open", 2);
        let mut t3 = make_issue("t3", "New Blocker", "open", 3);
        t3["dependencies"] = serde_json::json!([{
            "issue_id": "t3",
            "depends_on_id": "t2",
            "type": "blocks",
            "created_at": "2026-02-25T11:00:00Z"
        }]);
        write_jsonl(dir.path(), &[t1, t2, t3]);

        let r = import_beads_issues(&path, &store, false).unwrap();
        // t2 and t3 should be updated (dep changes), t1 skipped
        assert!(r.issues_updated >= 1);

        // t2 should now be blocked by t3, not t1
        let blocked = store.list_blocked().unwrap();
        assert_eq!(blocked.len(), 1);
        assert_eq!(blocked[0].task_id, bid_t2);

        let ready = store.list_ready().unwrap();
        let ready_ids: Vec<&str> = ready.iter().map(|r| r.task_id.as_str()).collect();
        assert!(ready_ids.contains(&bid_t1.as_str()));
        assert!(ready_ids.contains(&bid_t3.as_str()));
        assert!(!ready_ids.contains(&bid_t2.as_str()));
    }
}
