---
description: Create a backward-looking investigation or finding
allowed-tools: "mcp__brain__*"
---

Create an analysis record in brain for reports, investigations, findings, post-mortems, and retros.

Use the `records_create_analysis` tool with:
- `title` (required): Analysis title
- `text`: Plain text content (mutually exclusive with `data`)
- `data`: Base64-encoded binary content (mutually exclusive with `text`)
- `description`: Optional description
- `tags`: Optional tags array
- `task_id`: Optional task ID to link this record to its originating task
- `media_type`: MIME type hint; defaults to `text/plain` for `text`, `application/octet-stream` for `data`
- `brain`: Target brain for cross-brain writes

Analyses are embedded for search — find them later via `/brain:records-search`.

**Use this over alternatives when:**
- The record is backward- or inward-looking: evidence gathered, conclusions drawn
- Content includes findings, root causes, metrics, or recommendations from observation
- Use `records_create_plan` instead for forward-looking prescribed steps
- Use `records_create_document` instead for durable reference material not tied to a specific investigation
- Use `records_save_snapshot` instead for raw state captures with no embedding
