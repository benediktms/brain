---
description: List, view, and fetch records
argument-hint: [list | get <id> | fetch <id>]
---

Manage brain records (artifacts and snapshots).

**Search records:** Use `records_search` for semantic + keyword hybrid search across record content. Parameters: `query` (required), `k` (max results), `budget` (token budget), `tags` (boost matching tags), `brains` (cross-brain search).

**List records:** Use `records_list` with optional filters:
- `kind`: artifact or snapshot
- `status`: active or archived
- `tag`: filter by tag
- `task_id`: filter by linked task

**Get record details:** Use `records_get` with a record ID to see metadata, tags, and links.

**Fetch content:** Use `records_fetch_content` with a record ID to retrieve the raw content. Text content is returned as UTF-8; binary as base64.
