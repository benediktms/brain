---
description: Save a snapshot record
argument-hint: <title> [--text | --data]
---

Save a snapshot record in brain.

Use the `records_save_snapshot` tool with:
- `title` (required): Snapshot title
- `text`: Plain text content
- `data`: Base64-encoded binary content
- `description`: Optional description
- `tags`: Optional tags array
- `brain`: Target brain for cross-brain writes

Snapshots capture point-in-time state and are stored in brain's content-addressed object store.
