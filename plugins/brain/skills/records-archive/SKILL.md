---
description: Archive a record (soft-delete; preserves history)
allowed-tools: "mcp__brain__*"
---

Archive a brain record. Emits a `RecordArchived` event — the record is hidden from default `records_list` results but remains retrievable with `status: archived` and is preserved in the event store.

If arguments are provided:
- $1: Record ID (full ID or unique prefix)
- --reason: Optional reason for archiving (free text)

If the record ID is missing, ask the user. Suggest `/brain:records-list` or `/brain:records-search` if they need to find one first.

Use the `records_archive` tool. Show:
- `record_id`
- `status: archived`
- `uri`

This action is not destructive — to surface the record again, list with `status: archived`.
