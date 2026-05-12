---
description: Create a forward-looking plan or execution outline
allowed-tools: "mcp__brain__*"
---

Create a plan record in brain for implementation plans, execution outlines, and multi-step proposals.

Use the `records_create_plan` tool with:
- `title` (required): Plan title
- `text`: Plain text content (mutually exclusive with `data`)
- `data`: Base64-encoded binary content (mutually exclusive with `text`)
- `description`: Optional description
- `tags`: Optional tags array
- `task_id`: Optional task ID to link this record to its originating task
- `media_type`: MIME type hint; defaults to `text/plain` for `text`, `application/octet-stream` for `data`
- `brain`: Target brain for cross-brain writes

Plans are embedded for search — find them later via `/brain:records-search`.

**Use this over alternatives when:**
- The record is forward-looking and prescribes steps to take
- Content includes acceptance criteria, milestones, or ordered tasks
- Use `records_create_analysis` instead for backward-looking investigations or findings
- Use `records_create_document` instead for durable reference material that is descriptive rather than prescriptive
- Use `records_save_snapshot` instead for point-in-time state captures with no embedding
