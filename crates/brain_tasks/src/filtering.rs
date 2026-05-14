use std::collections::{HashMap, HashSet};

use crate::domain::Task;
use crate::events::TaskType;

/// Criteria for filtering tasks beyond the base status query.
#[derive(Debug, Default)]
pub struct TaskFilter {
    pub priority: Option<i32>,
    pub task_type: Option<TaskType>,
    pub assignee: Option<String>,
    pub label: Option<String>,
    pub search: Option<String>,
}

impl TaskFilter {
    pub fn is_empty(&self) -> bool {
        self.priority.is_none()
            && self.task_type.is_none()
            && self.assignee.is_none()
            && self.label.is_none()
            && self.search.is_none()
    }
}

/// Apply in-memory filters to a list of tasks.
///
/// - `fts_ids`: if a search was performed, the set of matching task_ids from FTS.
/// - `labels_map`: pre-fetched label map for label filtering.
pub fn apply_filters(
    tasks: Vec<Task>,
    filter: &TaskFilter,
    fts_ids: Option<&HashSet<String>>,
    labels_map: Option<&HashMap<String, Vec<String>>>,
) -> Vec<Task> {
    tasks
        .into_iter()
        .filter(|t| {
            let task_id = t.id.as_str();
            if let Some(fts) = fts_ids
                && !fts.contains(task_id)
            {
                return false;
            }
            if let Some(p) = filter.priority
                && t.priority.as_i32() != p
            {
                return false;
            }
            if let Some(ref tt) = filter.task_type
                && t.task_type != *tt
            {
                return false;
            }
            if let Some(ref a) = filter.assignee
                && !t
                    .assignee
                    .as_deref()
                    .is_some_and(|v| v.eq_ignore_ascii_case(a))
            {
                return false;
            }
            if let Some(ref label) = filter.label {
                let has_label = labels_map
                    .and_then(|m| m.get(task_id))
                    .is_some_and(|labels| labels.iter().any(|l| l == label));
                if !has_label {
                    return false;
                }
            }
            true
        })
        .collect()
}
