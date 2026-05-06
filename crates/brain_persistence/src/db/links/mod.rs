//! Polymorphic edge graph and legacy file-link helpers.
//!
//! The Rust module path is `crate::db::links`; the polymorphic SQL table is
//! `entity_links` (v49+). The legacy `links` table (note/wiki linking, used by
//! the PageRank computation) is handled by `file_links`.
//!
//! This asymmetry avoids a name collision with the legacy wiki-link table while
//! keeping the module name concise.

pub mod api;
pub mod entity_graph;
pub mod file_links;
pub mod projections;
pub mod traversal;

pub use api::{EntityLink, LinkError, add_link_checked, for_entity, remove_link};
pub use entity_graph::*;
pub use file_links::*;
pub use projections::{LinkEvent, apply_link_event, apply_link_remove};
pub use traversal::{ThreadResult, collect_linked_episode_set, collect_thread_episode_rows};
