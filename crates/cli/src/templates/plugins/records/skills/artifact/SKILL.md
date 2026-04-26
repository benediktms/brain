---
description: Create a typed record
allowed-tools: "mcp__brain__*"
---

Create a new typed record in brain.

Use one of these tools based on the record you are creating:
- `records_create_document` for generated prose, specs, and reusable documents
- `records_create_analysis` for reports, investigations, and analytic outputs
- `records_create_plan` for implementation plans and execution outlines
- `records_save_snapshot` for point-in-time state captures

The typed creation tools support:
- `title` (required): Record title
- `text`: Plain text content
- `data`: Base64-encoded binary content
- `description`: Optional description
- `tags`: Optional tags array
- `brain`: Target brain for cross-brain writes

If no content is provided, ask the user for the record content.

Documents, analyses, and plans are embedded for search. Snapshots are stored without embedding or summarization.
