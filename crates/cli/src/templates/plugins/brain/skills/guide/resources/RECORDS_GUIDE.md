# Records Guide

## Artifacts vs Snapshots

| | Artifacts | Snapshots |
|---|---|---|
| Purpose | Work products, deliverables | Point-in-time state captures |
| Examples | Reports, analysis, generated code | Config state, debug dumps, test results |
| Create | `records_create_artifact` | `records_save_snapshot` |
| Content | `text` (plain) or `data` (base64) | `text` (plain) or `data` (base64) |

## Content-Addressed Storage

Records are stored in `~/.brain/objects/` using BLAKE3 content hashing. Two records with identical content share the same storage object (deduplication is automatic and global across all brains).

## Linking Records

Records can be linked to tasks and note chunks:
- `records_link_add`: Link a record to a task or chunk
- `records_link_remove`: Remove a link

This creates traceability between work products and the tasks that produced them.

## Tagging

- `records_tag_add`: Add a tag to a record (idempotent)
- `records_tag_remove`: Remove a tag

Tags enable filtering via `records_list` with the `tag` parameter.

## Cross-Brain Records

Pass `brain` parameter to create records in another brain project. Records are stored immediately but may not appear in vector search until the target brain's daemon indexes them.

## Archiving

Use `records_archive` to archive a record. Metadata is preserved but the record is hidden from default listings. Use `status: archived` filter in `records_list` to find archived records.

## Fetching Content

`records_fetch_content` returns:
- Text content (text/*, JSON, TOML, YAML) as UTF-8 in a `text` field
- Binary content as base64 in a `data` field
- `encoding` field indicates which format ('utf-8' or 'base64')
