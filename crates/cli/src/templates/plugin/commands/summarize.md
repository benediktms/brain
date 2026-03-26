---
description: Generate extractive summaries scoped to a directory or tag
argument-hint: <scope-type> <scope-value> [--regenerate]
---

Generate or retrieve an extractive summary scoped to a specific directory or tag.

Use the `memory_summarize_scope` tool with:
- `scope_type` (required): Either `"directory"` or `"tag"`
- `scope_value` (required): The directory path or tag name to summarize
- `regenerate`: Force regeneration even if a cached summary exists (default: false)

Examples:
- Summarize all notes in a directory: scope_type=`directory`, scope_value=`src/auth/`
- Summarize all content tagged "architecture": scope_type=`tag`, scope_value=`architecture`

Summaries are cached and reused on subsequent calls unless `regenerate` is true.
