//! Episodic memory domain crate.
//!
//! Owns episodes (time-ordered traces), procedures (reusable how-tos),
//! and the operations that compose them: retrieval, reflection, thread
//! walking, consolidation, and scope summarization.
//!
//! Boundary with `brain_records`: records are *artifacts you produced*
//! (documents, plans, analyses, snapshots) with content_hash + archival
//! lifecycle. Episodes here are *experiences you recorded* — append-only,
//! DAG-linked via `continues` edges, vector-indexed for semantic recall.

pub mod consolidate;
pub mod context;
pub mod reflect;
pub mod retrieve;
pub mod summarize_scope;
pub mod walk_thread;
pub mod write_episode;
pub mod write_procedure;
