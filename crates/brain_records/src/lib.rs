//! Records domain crate.
//!
//! Wave 2 of the workspace decomposition (saga-5df / brn-2fe). Provides the
//! `RecordStore` repository plus the records domain types (`Record`,
//! `RecordKind`, `ContentRef`, …). Persistence row types stay behind the
//! store boundary in `brain_persistence::db::records`.

pub mod capsule;
pub mod domain;
pub mod events;
pub mod integrity;
pub mod objects;
pub mod projections;
pub mod queries;
mod store;

pub use domain::{
    ContentRef, KindPolicy, Record, RecordDomain, RecordId, RecordKind, RecordStatus,
};
pub use store::{CreateRecordParams, RecordStore};
