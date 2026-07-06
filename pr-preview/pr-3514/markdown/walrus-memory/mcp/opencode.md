> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Add Walrus Memory to OpenCode so the agent can save and recall durable facts. OpenCode uses the **MCP server** (the memory tools); the automatic-memory plugin hooks are available on [Claude Code](/walrus-memory/mcp/claude-code), [Codex](/walrus-memory/mcp/codex), [Cursor](/walrus-memory/mcp/cursor), and [Antigravity](/walrus-memory/mcp/antigravity).

- [x] Node.js 20+
- [x] A Walrus Memory account. The first memory tool call opens a browser sign-in (`memwal_login`).

## Installation

Add the server to `~/.config/opencode/opencode.json` as a local (stdio) MCP server:

```json
{
  "mcp": {
    "memwal": {
      "type": "local",
      "command": ["npx", "-y", "@mysten-incubation/memwal-mcp"],
      "enabled": true
    }
  }
}
```

To pin a default namespace, add `"environment": { "MEMWAL_NAMESPACE": "<name>" }` to the server entry. Restart OpenCode, then ask the agent to run `memwal_login` on first use.

## Available tools

| Tool | Description |
|------|-------------|
| `memwal_remember` | Save a durable fact (preference, decision, constraint, identity). |
| `memwal_remember_bulk` | Save several distinct facts in one call. |
| `memwal_recall` | Semantic search across stored memories for relevant context. |
| `memwal_analyze` | Extract and save multiple facts from a passage of text. |
| `memwal_restore` | Rebuild the search index from Walrus (recovery). |
| `memwal_health` | Fast connectivity check. |
| `memwal_login` / `memwal_logout` | Connect or disconnect this client. |

The tool descriptions tell the agent to save and recall proactively. See [Reference](/walrus-memory/mcp/reference) for full parameters.

## Verify

Ask the agent what MCP tools it has available. You should see the `memwal_*` tools. State a durable fact and confirm the agent saves it with `memwal_remember`.

## Troubleshooting

- **Tools missing**: restart OpenCode after editing `opencode.json`.
- **Not signed in**: ask the agent to run `memwal_login`, approve in the browser, then retry.
- **`memwal_recall` returns nothing although you saved before**: run `memwal_restore <namespace>` to rebuild the index from Walrus.