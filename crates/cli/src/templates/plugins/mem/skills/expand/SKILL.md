---
description: Expand search stubs to full content
allowed-tools: "mcp__brain__*"
---

Expand stubs returned by `/mem:search` to their full content.

Use the `memory_expand` tool with:
- `memory_ids` (required): Array of chunk IDs from search results
- `budget_tokens`: Maximum tokens in response (optional, default: 2000)

Returns full text content with byte offsets into the source file.
