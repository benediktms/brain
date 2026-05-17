//! Pure helpers for routing file-watcher events to the correct brain via
//! longest-prefix path matching.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use brain_lib::prelude::FileEvent;

use super::instance::BrainInstance;

/// Build a sorted prefix lookup: `Vec<(prefix_path, brain_name)>`, longest
/// prefix first so that more-specific paths match before shorter ones.
pub fn build_prefix_map(brains: &HashMap<String, BrainInstance>) -> Vec<(PathBuf, String)> {
    let mut map: Vec<(PathBuf, String)> = brains
        .values()
        .flat_map(|inst| {
            inst.note_dirs
                .iter()
                .map(|dir| (dir.clone(), inst.name.clone()))
                .collect::<Vec<_>>()
        })
        .collect();

    // Sort by path component count descending (longest prefix first)
    map.sort_by_key(|(p, _)| std::cmp::Reverse(p.components().count()));

    map
}

/// Given an event path, find the brain whose note directory is the longest
/// prefix of that path.
pub fn lookup_brain(prefix_map: &[(PathBuf, String)], event_path: &Path) -> Option<String> {
    for (prefix, brain_name) in prefix_map {
        if event_path.starts_with(prefix) {
            return Some(brain_name.clone());
        }
    }
    None
}

/// Extract the primary path from a [`FileEvent`] for routing purposes.
pub fn event_primary_path(event: &FileEvent) -> PathBuf {
    match event {
        FileEvent::Changed(p) | FileEvent::Created(p) | FileEvent::Deleted(p) => p.clone(),
        FileEvent::Renamed { to, .. } => to.clone(),
    }
}
