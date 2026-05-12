---
description: Update a saga's title or description
allowed-tools: "mcp__brain__*"
---

Update the title or description of a saga.

Use `mcp__brain__sagas_update` with:
- `saga_id` (required): `saga-<hex>` short form (3+ lowercase hex chars) or bare 26-char ULID
- `title` (optional): new title; must not be empty if provided
- `description` (optional): new description; pass `null` to clear it
- `actor` (optional): who is making the change, defaults to `mcp`

At least one of `title` or `description` must be provided. Updates are allowed in any saga status, including closed and cancelled.

Use this skill to correct a saga title or expand its description after creation. It does not affect status or membership — use `/brain:sagas-start`, `/brain:sagas-close`, or the tasks skills for those.
