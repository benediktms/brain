# Skill Naming Convention

## Overview

Brain ships one Claude Code plugin called `brain`. All skills live flat under
`plugins/brain/skills/` — one directory per skill. Slash commands are
addressable as `/brain:<skill-name>`, where `<skill-name>` is the directory
name verbatim. There is no nesting or sub-plugin hierarchy: every skill is a
direct child of the single `plugins/brain/skills/` directory.

## Directory Layout

```
plugins/brain/skills/
├── guide/
├── jobs/
├── list/
├── status/
├── mem-consolidate/
├── mem-procedure/
├── mem-reflect/
├── mem-search/
├── mem-summarize/
├── mem-write/
├── records-artifact/
├── records-search/
├── records-snapshot/
├── tasks-blocked/
├── tasks-close/
├── tasks-create/
├── tasks-dep/
├── tasks-label/
├── tasks-list/
├── tasks-next/
├── tasks-show/
├── tasks-stats/
└── tasks-update/
```

The top four entries (`guide`, `jobs`, `list`, `status`) are **admin skills**
with no domain prefix. Every other entry is a **domain skill** prefixed by its
domain segment followed by a hyphen.

## Naming Rules

| Concern | Convention |
|---|---|
| Skill directory name | Lowercase, hyphens as separators — e.g. `tasks-next` |
| Slash command | `/brain:<directory-name>` — e.g. `/brain:tasks-next` |
| MCP tool name (in skill body) | `mcp__brain__<tool_name>` where `<tool_name>` uses **underscores** — e.g. `mcp__brain__tasks_next` |
| Domain prefix | Same segment used in the MCP tool name — `tasks`, `mem`, `records`, `sagas` |
| Admin skills (no domain) | No prefix — `status`, `list`, `jobs`, `guide` |

**Hyphens** are the separator for skill directory names and therefore for slash
commands. **Underscores** are the separator for MCP tool names (the MCP
protocol convention is `mcp__<server>__<tool_name_with_underscores>`). A skill
directory named `tasks-next/` wraps the MCP tool `tasks_next`; its slash
command is `/brain:tasks-next`.

### Prefix policy

Domain skills get a `<domain>-` prefix. The domain is the same segment that
appears before the underscore in the corresponding MCP tool name:

- `tasks_next` → domain `tasks` → skill directory `tasks-next`
- `memory_retrieve` → domain `mem` (shortened conventionally) → skill directory `mem-search`
- `records_search` → domain `records` → skill directory `records-search`

When the `sagas` domain gains skills they will follow the same pattern:
`sagas-create`, `sagas-list`, etc.

Admin skills (`status`, `list`, `jobs`, `guide`) act on the plugin itself or
provide orientation; they have no domain prefix.

## Adding a New Skill

1. **Pick a name** following the convention above. If the skill wraps a domain
   MCP tool, derive the directory name from the tool name by replacing
   underscores with hyphens and keeping the domain prefix. If it is a
   standalone admin skill, use a single descriptive word.

2. **Create the skill file**:

   ```
   plugins/brain/skills/<name>/SKILL.md
   ```

3. **Write the frontmatter** — only `description` and `allowed-tools` are
   required. Do not add `name:` (the directory provides it) or `version:`:

   ```markdown
   ---
   description: One-line description of what the skill does
   allowed-tools: "mcp__brain__*"
   ---

   Body text explaining when and how to use the skill.
   ```

4. The skill **automatically becomes `/brain:<name>`** after the plugin is
   reinstalled or hot-reloaded. No manifest update is required.

## Example: `tasks-blocked`

The file `plugins/brain/skills/tasks-blocked/SKILL.md` is the canonical
minimal example:

```markdown
---
description: Show all blocked tasks
allowed-tools: "mcp__brain__*"
---

Display tasks that are currently blocked.

Use the `tasks_list` tool with `status` set to `blocked`. Present each blocked task showing:
- Task ID and title
- Priority
- What is blocking it (blocked_reason or dependency)

Suggest actions to unblock tasks where possible.
```

Note:
- The frontmatter is two keys only.
- The body references the MCP tool name with underscores (`tasks_list`).
- The slash command exposed to the user is `/brain:tasks-blocked` (hyphens).

## Cross-References

- **Slash-command examples inside skill bodies** must use the `/brain:<name>`
  form — e.g. `/brain:tasks-blocked`, not `/tasks:blocked` or
  `/brain tasks-blocked`.
- **Marketplace manifest** at `.claude-plugin/marketplace.json` references the
  plugin via `"source": "./plugins/brain"` relative to the marketplace root.
  Skills do not need individual entries in the manifest.
