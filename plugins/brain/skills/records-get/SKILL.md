---
description: Get a record's full metadata including tags and links
allowed-tools: "mcp__brain__*"
---

Retrieve a brain record by ID with full metadata: tags, links to tasks, content hash, media type, timestamps, and status. Does not return the raw body — use `/brain:records-fetch-content` for that.

If arguments are provided:
- $1: Record ID (full ID or unique prefix)
- --brain: Target brain name or ID — optional, defaults to the current brain

If the record ID is missing, ask the user. Suggest `/brain:records-list` or `/brain:records-search` to find one first.

Use the `records_get` tool. Present:
- Identity: `record_id`, `title`, `kind`, `status`
- Content metadata: `media_type`, `content_size`, `content_hash`
- Provenance: `actor`, `created_at`, `updated_at`
- Relationships: `tags`, `links` (task associations), optional `task_id`
- Any `warnings` from the tool
