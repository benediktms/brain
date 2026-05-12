---
description: Create a durable reference document or spec
allowed-tools: "mcp__brain__*"
---

Create a document record in brain for generated prose, specs, design docs, and reusable reference material.

Use the `records_create_document` tool with:
- `title` (required): Document title
- `text`: Plain text content (mutually exclusive with `data`)
- `data`: Base64-encoded binary content (mutually exclusive with `text`)
- `description`: Optional description
- `tags`: Optional tags array
- `task_id`: Optional task ID to link this record to its originating task
- `media_type`: MIME type hint; defaults to `text/plain` for `text`, `application/octet-stream` for `data`
- `brain`: Target brain for cross-brain writes

Documents are embedded for search — find them later via `/brain:records-search`.

**Use this over alternatives when:**
- The record is a durable, descriptive reference intended to be consulted over time
- Content includes specs, design decisions, or prose that is not tied to a specific investigation or action plan
- Use `records_create_analysis` instead for investigation outputs with evidence and conclusions
- Use `records_create_plan` instead for forward-looking execution outlines with prescribed steps
- Use `records_save_snapshot` instead for point-in-time state captures with no embedding
