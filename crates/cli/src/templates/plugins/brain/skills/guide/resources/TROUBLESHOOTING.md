# Troubleshooting

## Common Issues

### MCP server not responding
**Symptom**: Brain MCP tools not available in Claude Code
**Fix**:
1. Check MCP is configured: `brain mcp setup claude`
2. Verify brain binary: `which brain && brain --version`
3. Restart Claude Code to reload MCP servers

### Daemon not running
**Symptom**: Index is stale, new notes not appearing in search
**Fix**:
1. Check status: `brain daemon status`
2. Start daemon: `brain daemon start`
3. For persistent service: `brain daemon install`

### No brain project found
**Symptom**: "no brain project found in current directory tree"
**Fix**:
1. Initialize: `brain init` in your project root
2. Or check you're in the right directory

### Embedding model missing
**Symptom**: "embedding model not found", search returns no results
**Fix**:
1. Download the model:
   ```bash
   curl -sSL https://raw.githubusercontent.com/benediktms/brain/master/scripts/setup-model.sh | bash
   ```
2. Or manually install via HuggingFace CLI

### Stale search results
**Symptom**: Recent notes not appearing in search
**Fix**:
1. Check daemon is running: `brain daemon status`
2. Force reindex: `brain reindex --full <notes-path>`
3. Check index health: `brain doctor <notes-path>`

### Cross-brain operations failing
**Symptom**: "brain not found" when using `brain` parameter
**Fix**:
1. List registered brains: `brain list`
2. Check the brain name matches exactly
3. Register if missing: `brain init` in the target project

### Task ID resolution errors
**Symptom**: "ambiguous prefix" or "task not found"
**Fix**:
- Use longer ID prefixes to disambiguate
- Use `tasks_list` to find the full ID
- Task IDs are case-sensitive
