---
description: Create an artifact record
argument-hint: <title> [--text | --data]
---

Create a new artifact record in brain.

Use the `records_create_artifact` tool with:
- `title` (required): Artifact title
- `text`: Plain text content
- `data`: Base64-encoded binary content
- `description`: Optional description
- `tags`: Optional tags array
- `brain`: Target brain for cross-brain writes

If no content is provided, ask the user for the artifact content.

Artifacts are stored in brain's content-addressed object store and can be linked to tasks.
