//! Crate-private persistence query bindings.
//!
//! After Wave 3 of the brain_records redesign, this module is no longer
//! publicly accessible. External consumers reach the equivalent surface via
//! typed `RecordStore` methods (`list_records(&RecordQuery)`, `archive_record`,
//! `add_tag`, etc.) instead of constructing persistence `RecordFilter`
//! values or calling raw query functions.
//!
//! `RecordRow` and the raw query functions stay confined behind the
//! `From<RecordRow> for Record` boundary in [`crate::domain`]. The domain
//! `RecordLink` type in [`crate::domain`] mirrors the persistence shape
//! through a `From<brain_persistence::...::RecordLink>` boundary, so
//! consumers of `RecordStore::get_record_links` receive a typed domain
//! value with no persistence path in scope.

pub(crate) use brain_persistence::db::records::queries::{
    RecordFilter, RecordLink, RecordRow, compact_record_id, compact_record_ids, count_payload_refs,
    get_all_content_refs, get_record, get_record_links, get_record_tags, list_records,
    resolve_record_id,
};
