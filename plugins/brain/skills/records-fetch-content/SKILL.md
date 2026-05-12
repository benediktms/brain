---
description: Fetch the raw content of a record
allowed-tools: "mcp__brain__*"
---

Retrieve the raw content of a brain record. Text content (media types starting with `text/` or `application/json`) is returned as decoded UTF-8 in the `text` field; binary content is returned as base64 in the `data` field. The `encoding` field indicates which is in use.

If arguments are provided:
- $1: Record ID (full ID or unique prefix)
- --brain: Target brain name or ID — optional, defaults to the current brain

If the record ID is missing, ask the user. Suggest `/brain:records-get` first if they want metadata before pulling the body.

Use the `records_fetch_content` tool. Show:
- `title`, `kind`, `media_type`
- `encoding` and the matching content field (`text` or `data`)
- `content_hash` and `size` for verification
