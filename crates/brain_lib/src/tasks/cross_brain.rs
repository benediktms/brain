use crate::config::{get_or_generate_brain_id, open_remote_task_store, resolve_brain_entry};
use crate::error::{BrainCoreError, Result};
use crate::tasks::TaskStore;
use crate::tasks::events::{
    CrossBrainRefPayload, EventType, StatusChangedPayload, TaskCreatedPayload, TaskEvent,
    TaskStatus, TaskType, new_task_id,
};
use crate::tasks::queries::{
    CrossBrainRef, DependencySummary, ExternalIdRow, TaskComment, TaskNoteLink, TaskRow,
};

/// Parameters for creating a task in a remote brain.
pub struct CrossBrainCreateParams {
    /// Name or stable ID of the target brain.
    pub target_brain: String,
    pub title: String,
    pub description: Option<String>,
    /// Priority (0-5). Defaults to 4 if not specified.
    pub priority: i32,
    pub task_type: Option<TaskType>,
    pub assignee: Option<String>,
    /// Parent task ID in the remote brain (optional).
    pub parent: Option<String>,
    /// Local task ID to attach a cross-brain reference from (optional).
    pub link_from: Option<String>,
    /// Ref type for the cross-brain link: depends_on | blocks | related.
    /// Defaults to "related".
    pub link_type: Option<String>,
}

/// Result of a successful cross-brain task creation.
#[derive(Debug)]
pub struct CrossBrainCreateResult {
    pub remote_task_id: String,
    pub remote_brain_name: String,
    pub remote_brain_id: String,
    pub local_ref_created: bool,
}

/// Create a task in a remote brain and optionally add a cross-brain reference
/// on the local side.
///
/// `local_store` is only used when `params.link_from` is `Some(_)`.
pub fn cross_brain_create(
    local_store: &TaskStore,
    params: CrossBrainCreateParams,
) -> Result<CrossBrainCreateResult> {
    // 1. Resolve brain entry.
    let (name, entry) = resolve_brain_entry(&params.target_brain)?;

    // 2. Open the remote task store.
    let remote_store = open_remote_task_store(&name, &entry)?;

    // 3. Get the remote prefix.
    let remote_prefix = remote_store.get_project_prefix()?;

    // 4. Generate a task ID for the remote brain.
    let remote_task_id = new_task_id(&remote_prefix);

    // 5. Resolve parent task ID in the remote brain (if provided).
    let parent_task_id = match params.parent {
        Some(ref p) => Some(remote_store.resolve_task_id(p)?),
        None => None,
    };

    // 6. Build and append the TaskCreated event to the remote store.
    let event = TaskEvent::from_payload(
        &remote_task_id,
        "cross-brain",
        TaskCreatedPayload {
            title: params.title,
            description: params.description,
            priority: params.priority,
            status: TaskStatus::Open,
            due_ts: None,
            task_type: params.task_type,
            assignee: params.assignee,
            defer_until: None,
            parent_task_id,
        },
    );
    remote_store.append(&event)?;

    // 7. Determine the remote brain ID.
    let remote_brain_id = if let Some(ref id) = entry.id {
        id.clone()
    } else {
        get_or_generate_brain_id(&entry.root.join(".brain"))?
    };

    // 8. Optionally create a cross-brain reference on the local task.
    let local_ref_created = if let Some(ref link_from) = params.link_from {
        let local_task_id = local_store.resolve_task_id(link_from)?;
        let ref_type = params.link_type.unwrap_or_else(|| "related".to_string());
        let ref_payload = CrossBrainRefPayload {
            brain_id: remote_brain_id.clone(),
            remote_task: remote_task_id.clone(),
            ref_type,
            note: None,
        };
        let ref_event = TaskEvent::new(
            local_task_id,
            "cross-brain",
            EventType::CrossBrainRefAdded,
            &ref_payload,
        );
        local_store.append(&ref_event)?;
        true
    } else {
        false
    };

    Ok(CrossBrainCreateResult {
        remote_task_id,
        remote_brain_name: name,
        remote_brain_id,
        local_ref_created,
    })
}

// ── Cross-brain fetch ────────────────────────────────────────────────────────

/// Full details returned from a cross-brain task fetch.
#[derive(Debug)]
pub struct CrossBrainFetchResult {
    pub task: TaskRow,
    pub remote_brain_name: String,
    pub remote_brain_id: String,
    pub labels: Vec<String>,
    pub comments: Vec<TaskComment>,
    pub children: Vec<TaskRow>,
    pub dependency_summary: DependencySummary,
    pub note_links: Vec<TaskNoteLink>,
    pub cross_refs: Vec<CrossBrainRef>,
    pub external_ids: Vec<ExternalIdRow>,
}

/// Fetch a task and its full enrichment from a remote brain.
pub fn cross_brain_fetch(target_brain: &str, task_id: &str) -> Result<CrossBrainFetchResult> {
    let (name, entry) = resolve_brain_entry(target_brain)?;
    let remote_store = open_remote_task_store(&name, &entry)?;
    let remote_brain_id = if let Some(ref id) = entry.id {
        id.clone()
    } else {
        get_or_generate_brain_id(&entry.root.join(".brain"))?
    };

    let (task, labels, comments, children, dependency_summary, note_links, cross_refs, external_ids) =
        cross_brain_fetch_inner(&remote_store, task_id)?;

    Ok(CrossBrainFetchResult {
        task,
        remote_brain_name: name,
        remote_brain_id,
        labels,
        comments,
        children,
        dependency_summary,
        note_links,
        cross_refs,
        external_ids,
    })
}

/// Internal implementation for testing — accepts an already-opened remote store.
#[cfg(test)]
pub(crate) fn cross_brain_fetch_inner(
    remote_store: &TaskStore,
    task_id: &str,
) -> Result<(
    TaskRow,
    Vec<String>,
    Vec<TaskComment>,
    Vec<TaskRow>,
    DependencySummary,
    Vec<TaskNoteLink>,
    Vec<CrossBrainRef>,
    Vec<ExternalIdRow>,
)> {
    _cross_brain_fetch_inner(remote_store, task_id)
}

#[cfg(not(test))]
fn cross_brain_fetch_inner(
    remote_store: &TaskStore,
    task_id: &str,
) -> Result<(
    TaskRow,
    Vec<String>,
    Vec<TaskComment>,
    Vec<TaskRow>,
    DependencySummary,
    Vec<TaskNoteLink>,
    Vec<CrossBrainRef>,
    Vec<ExternalIdRow>,
)> {
    _cross_brain_fetch_inner(remote_store, task_id)
}

fn _cross_brain_fetch_inner(
    remote_store: &TaskStore,
    task_id: &str,
) -> Result<(
    TaskRow,
    Vec<String>,
    Vec<TaskComment>,
    Vec<TaskRow>,
    DependencySummary,
    Vec<TaskNoteLink>,
    Vec<CrossBrainRef>,
    Vec<ExternalIdRow>,
)> {
    let resolved = remote_store.resolve_task_id(task_id)?;
    let task = remote_store
        .get_task(&resolved)?
        .ok_or_else(|| BrainCoreError::TaskEvent(format!("task '{task_id}' not found in remote brain")))?;
    let labels = remote_store.get_task_labels(&resolved).unwrap_or_default();
    let comments = remote_store.get_task_comments(&resolved).unwrap_or_default();
    let children = remote_store.get_children(&resolved).unwrap_or_default();
    let dependency_summary = remote_store.get_dependency_summary(&resolved).unwrap_or_default();
    let note_links = remote_store.get_task_note_links(&resolved).unwrap_or_default();
    let cross_refs = remote_store.get_cross_brain_refs(&resolved).unwrap_or_default();
    let external_ids = remote_store.get_external_ids(&resolved).unwrap_or_default();
    Ok((task, labels, comments, children, dependency_summary, note_links, cross_refs, external_ids))
}

// ── Cross-brain close ────────────────────────────────────────────────────────

/// Parameters for closing tasks in a remote brain.
pub struct CrossBrainCloseParams {
    pub target_brain: String,
    pub task_ids: Vec<String>,
}

/// Result of a cross-brain close operation.
#[derive(Debug)]
pub struct CrossBrainCloseResult {
    pub remote_brain_name: String,
    pub remote_brain_id: String,
    /// Task IDs that were successfully closed (short/resolved IDs).
    pub closed: Vec<String>,
    /// Task IDs that failed with their error message.
    pub failed: Vec<(String, String)>,
    /// Task IDs that became unblocked as a result.
    pub unblocked: Vec<String>,
}

/// Close tasks in a remote brain.
///
/// `local_store` is accepted for API consistency with `cross_brain_create` and
/// future cross-ref linking but is not mutated in this implementation.
pub fn cross_brain_close(
    _local_store: &TaskStore,
    params: CrossBrainCloseParams,
) -> Result<CrossBrainCloseResult> {
    let (name, entry) = resolve_brain_entry(&params.target_brain)?;
    let remote_store = open_remote_task_store(&name, &entry)?;
    let remote_brain_id = if let Some(ref id) = entry.id {
        id.clone()
    } else {
        get_or_generate_brain_id(&entry.root.join(".brain"))?
    };

    let (closed, failed, unblocked) =
        cross_brain_close_inner(&remote_store, &params.task_ids)?;

    Ok(CrossBrainCloseResult {
        remote_brain_name: name,
        remote_brain_id,
        closed,
        failed,
        unblocked,
    })
}

/// Internal implementation for testing — accepts an already-opened remote store.
#[cfg(test)]
pub(crate) fn cross_brain_close_inner(
    remote_store: &TaskStore,
    task_ids: &[String],
) -> Result<(Vec<String>, Vec<(String, String)>, Vec<String>)> {
    _cross_brain_close_inner(remote_store, task_ids)
}

#[cfg(not(test))]
fn cross_brain_close_inner(
    remote_store: &TaskStore,
    task_ids: &[String],
) -> Result<(Vec<String>, Vec<(String, String)>, Vec<String>)> {
    _cross_brain_close_inner(remote_store, task_ids)
}

fn _cross_brain_close_inner(
    remote_store: &TaskStore,
    task_ids: &[String],
) -> Result<(Vec<String>, Vec<(String, String)>, Vec<String>)> {
    let mut closed = Vec::new();
    let mut failed = Vec::new();
    let mut unblocked = Vec::new();

    for raw_id in task_ids {
        let resolved = match remote_store.resolve_task_id(raw_id) {
            Ok(id) => id,
            Err(e) => {
                failed.push((raw_id.clone(), e.to_string()));
                continue;
            }
        };

        let event = TaskEvent::from_payload(
            &resolved,
            "cross-brain",
            StatusChangedPayload {
                new_status: TaskStatus::Done,
            },
        );

        if let Err(e) = remote_store.append(&event) {
            failed.push((raw_id.clone(), e.to_string()));
            continue;
        }

        let newly_unblocked = remote_store
            .list_newly_unblocked(&resolved)
            .unwrap_or_default();
        unblocked.extend(newly_unblocked);
        closed.push(resolved);
    }

    Ok((closed, failed, unblocked))
}

/// Internal implementation that accepts already-resolved parameters.
///
/// Separated from `cross_brain_create` so tests can bypass the global config
/// and `BRAIN_HOME` env var by providing the remote store directly.
#[cfg(test)]
pub(crate) fn cross_brain_create_inner(
    local_store: &TaskStore,
    remote_store: &TaskStore,
    remote_brain_id: String,
    remote_brain_name: String,
    params: CrossBrainCreateParams,
) -> Result<CrossBrainCreateResult> {
    let remote_prefix = remote_store.get_project_prefix()?;
    let remote_task_id = new_task_id(&remote_prefix);

    let parent_task_id = match params.parent {
        Some(ref p) => Some(remote_store.resolve_task_id(p)?),
        None => None,
    };

    let event = TaskEvent::from_payload(
        &remote_task_id,
        "cross-brain",
        TaskCreatedPayload {
            title: params.title,
            description: params.description,
            priority: params.priority,
            status: TaskStatus::Open,
            due_ts: None,
            task_type: params.task_type,
            assignee: params.assignee,
            defer_until: None,
            parent_task_id,
        },
    );
    remote_store.append(&event)?;

    let local_ref_created = if let Some(ref link_from) = params.link_from {
        let local_task_id = local_store.resolve_task_id(link_from)?;
        let ref_type = params.link_type.unwrap_or_else(|| "related".to_string());
        let ref_payload = CrossBrainRefPayload {
            brain_id: remote_brain_id.clone(),
            remote_task: remote_task_id.clone(),
            ref_type,
            note: None,
        };
        let ref_event = TaskEvent::new(
            local_task_id,
            "cross-brain",
            EventType::CrossBrainRefAdded,
            &ref_payload,
        );
        local_store.append(&ref_event)?;
        true
    } else {
        false
    };

    Ok(CrossBrainCreateResult {
        remote_task_id,
        remote_brain_name,
        remote_brain_id,
        local_ref_created,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{BrainToml, save_brain_toml};
    use crate::db::Db;
    use crate::tasks::events::{TaskCreatedPayload, TaskEvent, TaskStatus};
    use tempfile::TempDir;

    /// Create a minimal brain setup: `.brain/brain.toml` with the given name
    /// and a task store backed by files on disk.
    fn make_brain(brain_home: &std::path::Path, name: &str) -> (TempDir, TaskStore) {
        // Project root (simulates where the code lives).
        let project_tmp = TempDir::new().unwrap();

        // Write .brain/brain.toml.
        let brain_dir = project_tmp.path().join(".brain");
        std::fs::create_dir_all(&brain_dir).unwrap();
        let toml_cfg = BrainToml {
            name: name.to_string(),
            notes: vec![],
            id: Some(format!("{name}-id")),
        };
        save_brain_toml(&brain_dir, &toml_cfg).unwrap();

        // Create the task store under $BRAIN_HOME/brains/<name>/
        let brain_data_dir = brain_home.join("brains").join(name);
        std::fs::create_dir_all(&brain_data_dir).unwrap();
        let sqlite_db = brain_data_dir.join("brain.db");
        let db = Db::open(&sqlite_db).unwrap();
        let tasks_dir = brain_data_dir.join("tasks");
        let store = TaskStore::new(&tasks_dir, db).unwrap();
        store.rebuild_projections().unwrap();

        (project_tmp, store)
    }

    fn add_task(store: &TaskStore, task_id: &str, title: &str) {
        let event = TaskEvent::from_payload(
            task_id,
            "test",
            TaskCreatedPayload {
                title: title.to_string(),
                description: None,
                priority: 4,
                status: TaskStatus::Open,
                due_ts: None,
                task_type: None,
                assignee: None,
                defer_until: None,
                parent_task_id: None,
            },
        );
        store.append(&event).unwrap();
    }

    // -----------------------------------------------------------------------
    // Test: basic cross-brain creation via the inner function
    // -----------------------------------------------------------------------

    #[test]
    fn test_cross_brain_create_basic() {
        let brain_home_tmp = TempDir::new().unwrap();
        let brain_home = brain_home_tmp.path();

        let (_local_tmp, local_store) = make_brain(brain_home, "local-brain");
        let (_remote_tmp, remote_store) = make_brain(brain_home, "remote-brain");

        let params = CrossBrainCreateParams {
            target_brain: "remote-brain".to_string(),
            title: "Remote task".to_string(),
            description: Some("A task in the remote brain".to_string()),
            priority: 2,
            task_type: Some(TaskType::Feature),
            assignee: Some("alice".to_string()),
            parent: None,
            link_from: None,
            link_type: None,
        };

        let result = cross_brain_create_inner(
            &local_store,
            &remote_store,
            "remote-brain-id".to_string(),
            "remote-brain".to_string(),
            params,
        )
        .unwrap();

        assert_eq!(result.remote_brain_name, "remote-brain");
        assert_eq!(result.remote_brain_id, "remote-brain-id");
        assert!(!result.local_ref_created);

        // The task must appear in the remote store.
        let task = remote_store
            .get_task(&result.remote_task_id)
            .unwrap()
            .expect("task should exist in remote store");
        assert_eq!(task.title, "Remote task");
        assert_eq!(task.priority, 2);

        // The local store must be untouched.
        assert!(local_store.list_all().unwrap().is_empty());
    }

    // -----------------------------------------------------------------------
    // Test: cross-brain creation with link_from
    // -----------------------------------------------------------------------

    #[test]
    fn test_cross_brain_create_with_link_from() {
        let brain_home_tmp = TempDir::new().unwrap();
        let brain_home = brain_home_tmp.path();

        let (_local_tmp, local_store) = make_brain(brain_home, "local-brain");
        let (_remote_tmp, remote_store) = make_brain(brain_home, "remote-brain");

        // Seed the local store with a task to link from.
        add_task(&local_store, "LOCAL-001", "Local parent task");

        let params = CrossBrainCreateParams {
            target_brain: "remote-brain".to_string(),
            title: "Remote task with link".to_string(),
            description: None,
            priority: 4,
            task_type: None,
            assignee: None,
            parent: None,
            link_from: Some("LOCAL-001".to_string()),
            link_type: Some("depends_on".to_string()),
        };

        let result = cross_brain_create_inner(
            &local_store,
            &remote_store,
            "remote-brain-id".to_string(),
            "remote-brain".to_string(),
            params,
        )
        .unwrap();

        assert!(result.local_ref_created);

        // The task must appear in the remote store.
        let remote_task = remote_store
            .get_task(&result.remote_task_id)
            .unwrap()
            .expect("task should exist in remote store");
        assert_eq!(remote_task.title, "Remote task with link");

        // The cross-brain ref must appear on the local task.
        let refs = local_store.get_cross_brain_refs("LOCAL-001").unwrap();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].brain_id, "remote-brain-id");
        assert_eq!(refs[0].remote_task, result.remote_task_id);
        assert_eq!(refs[0].ref_type, "depends_on");
    }

    // -----------------------------------------------------------------------
    // Test: link_from defaults ref_type to "related"
    // -----------------------------------------------------------------------

    #[test]
    fn test_cross_brain_create_link_default_ref_type() {
        let brain_home_tmp = TempDir::new().unwrap();
        let brain_home = brain_home_tmp.path();

        let (_local_tmp, local_store) = make_brain(brain_home, "local-brain");
        let (_remote_tmp, remote_store) = make_brain(brain_home, "remote-brain");

        add_task(&local_store, "LOCAL-001", "Local task");

        let params = CrossBrainCreateParams {
            target_brain: "remote-brain".to_string(),
            title: "Remote task".to_string(),
            description: None,
            priority: 4,
            task_type: None,
            assignee: None,
            parent: None,
            link_from: Some("LOCAL-001".to_string()),
            link_type: None, // should default to "related"
        };

        let result = cross_brain_create_inner(
            &local_store,
            &remote_store,
            "remote-brain-id".to_string(),
            "remote-brain".to_string(),
            params,
        )
        .unwrap();

        assert!(result.local_ref_created);

        let refs = local_store.get_cross_brain_refs("LOCAL-001").unwrap();
        assert_eq!(refs[0].ref_type, "related");
    }

    // -----------------------------------------------------------------------
    // Test: invalid target brain returns error (via public function + BRAIN_HOME)
    // -----------------------------------------------------------------------

    #[test]
    fn test_cross_brain_create_invalid_target_brain() {
        let brain_home_tmp = TempDir::new().unwrap();
        let brain_home = brain_home_tmp.path();

        // Set BRAIN_HOME so resolve_brain_entry loads an empty config.
        // SAFETY: single-threaded test; no concurrent env access.
        unsafe {
            std::env::set_var("BRAIN_HOME", brain_home);
        }

        // Create a config with no "nonexistent" brain.
        let config_path = brain_home.join("config.toml");
        std::fs::write(&config_path, "[brains]\n").unwrap();

        let (_local_tmp, local_store) = make_brain(brain_home, "local-brain");

        let params = CrossBrainCreateParams {
            target_brain: "nonexistent".to_string(),
            title: "Will fail".to_string(),
            description: None,
            priority: 4,
            task_type: None,
            assignee: None,
            parent: None,
            link_from: None,
            link_type: None,
        };

        let err = cross_brain_create(&local_store, params).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("nonexistent"),
            "error should mention the requested brain: {msg}"
        );

        unsafe {
            std::env::remove_var("BRAIN_HOME");
        }
    }

    // -----------------------------------------------------------------------
    // Test: task is written to remote JSONL, not local JSONL
    // -----------------------------------------------------------------------

    #[test]
    fn test_cross_brain_create_writes_to_remote_not_local() {
        let brain_home_tmp = TempDir::new().unwrap();
        let brain_home = brain_home_tmp.path();

        let (_local_tmp, local_store) = make_brain(brain_home, "local-brain");
        let (_remote_tmp, remote_store) = make_brain(brain_home, "remote-brain");

        let params = CrossBrainCreateParams {
            target_brain: "remote-brain".to_string(),
            title: "Only in remote".to_string(),
            description: None,
            priority: 4,
            task_type: None,
            assignee: None,
            parent: None,
            link_from: None,
            link_type: None,
        };

        let result = cross_brain_create_inner(
            &local_store,
            &remote_store,
            "remote-brain-id".to_string(),
            "remote-brain".to_string(),
            params,
        )
        .unwrap();

        // Task must exist in remote store.
        assert!(
            remote_store
                .get_task(&result.remote_task_id)
                .unwrap()
                .is_some()
        );

        // Task must NOT exist in local store.
        assert!(
            local_store
                .get_task(&result.remote_task_id)
                .unwrap()
                .is_none()
        );
        assert!(local_store.list_all().unwrap().is_empty());
    }

    // -----------------------------------------------------------------------
    // Test: cross-brain fetch
    // -----------------------------------------------------------------------

    #[test]
    fn test_cross_brain_fetch_basic() {
        let brain_home_tmp = TempDir::new().unwrap();
        let brain_home = brain_home_tmp.path();

        let (_remote_tmp, remote_store) = make_brain(brain_home, "remote-brain");

        // Add a task with a label and a comment.
        add_task(&remote_store, "REMOTE-001", "Fetchable task");

        let label_event = TaskEvent::new(
            "REMOTE-001",
            "test",
            crate::tasks::events::EventType::LabelAdded,
            &crate::tasks::events::LabelPayload { label: "important".to_string() },
        );
        remote_store.append(&label_event).unwrap();

        let comment_event = TaskEvent::from_payload(
            "REMOTE-001",
            "test",
            crate::tasks::events::CommentPayload { body: "A comment".to_string() },
        );
        remote_store.append(&comment_event).unwrap();

        let (task, labels, comments, children, dep_summary, note_links, cross_refs, external_ids) =
            cross_brain_fetch_inner(&remote_store, "REMOTE-001").unwrap();

        assert_eq!(task.task_id, "REMOTE-001");
        assert_eq!(task.title, "Fetchable task");
        assert_eq!(labels, vec!["important"]);
        assert_eq!(comments.len(), 1);
        assert_eq!(comments[0].body, "A comment");
        assert!(children.is_empty());
        assert_eq!(dep_summary.total_deps, 0);
        assert!(note_links.is_empty());
        assert!(cross_refs.is_empty());
        assert!(external_ids.is_empty());
    }

    #[test]
    fn test_cross_brain_fetch_not_found() {
        let brain_home_tmp = TempDir::new().unwrap();
        let brain_home = brain_home_tmp.path();

        let (_remote_tmp, remote_store) = make_brain(brain_home, "remote-brain");

        let result = cross_brain_fetch_inner(&remote_store, "nonexistent");
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // Test: cross-brain close
    // -----------------------------------------------------------------------

    #[test]
    fn test_cross_brain_close_basic() {
        let brain_home_tmp = TempDir::new().unwrap();
        let brain_home = brain_home_tmp.path();

        let (_remote_tmp, remote_store) = make_brain(brain_home, "remote-brain");

        add_task(&remote_store, "REMOTE-001", "Task to close");
        add_task(&remote_store, "REMOTE-002", "Another task to close");

        let (closed, failed, unblocked) =
            cross_brain_close_inner(&remote_store, &[
                "REMOTE-001".to_string(),
                "REMOTE-002".to_string(),
            ])
            .unwrap();

        assert_eq!(closed.len(), 2);
        assert!(failed.is_empty());
        assert!(unblocked.is_empty());

        // Verify both tasks are marked done in the remote store.
        let t1 = remote_store.get_task("REMOTE-001").unwrap().unwrap();
        let t2 = remote_store.get_task("REMOTE-002").unwrap().unwrap();
        assert_eq!(t1.status, "done");
        assert_eq!(t2.status, "done");
    }

    #[test]
    fn test_cross_brain_close_unblocks_dependents() {
        let brain_home_tmp = TempDir::new().unwrap();
        let brain_home = brain_home_tmp.path();

        let (_remote_tmp, remote_store) = make_brain(brain_home, "remote-brain");

        add_task(&remote_store, "REMOTE-001", "Blocker");
        add_task(&remote_store, "REMOTE-002", "Blocked");

        // Make REMOTE-002 depend on REMOTE-001.
        let dep_event = TaskEvent::new(
            "REMOTE-002",
            "test",
            crate::tasks::events::EventType::DependencyAdded,
            &crate::tasks::events::DependencyPayload {
                depends_on_task_id: "REMOTE-001".to_string(),
            },
        );
        remote_store.append(&dep_event).unwrap();

        let (closed, failed, unblocked) =
            cross_brain_close_inner(&remote_store, &["REMOTE-001".to_string()]).unwrap();

        assert_eq!(closed, vec!["REMOTE-001"]);
        assert!(failed.is_empty());
        assert_eq!(unblocked, vec!["REMOTE-002"]);
    }

    #[test]
    fn test_cross_brain_close_partial_failure() {
        let brain_home_tmp = TempDir::new().unwrap();
        let brain_home = brain_home_tmp.path();

        let (_remote_tmp, remote_store) = make_brain(brain_home, "remote-brain");

        add_task(&remote_store, "REMOTE-001", "Real task");

        let (closed, failed, _) =
            cross_brain_close_inner(&remote_store, &[
                "REMOTE-001".to_string(),
                "nonexistent-99".to_string(),
            ])
            .unwrap();

        assert_eq!(closed.len(), 1);
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].0, "nonexistent-99");
    }
}
