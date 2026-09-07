> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Add Walrus Memory to Claude Desktop so the agent can save and recall durable facts. Claude Desktop uses the **MCP server** (the memory tools); the automatic-memory plugin hooks are available on [Claude Code](/walrus-memory/mcp/claude-code), [Codex](/walrus-memory/mcp/codex), [Cursor](/walrus-memory/mcp/cursor), and [Antigravity](/walrus-memory/mcp/antigravity).

- [x] Node.js 20+
- [x] A Walrus Memory account. The first memory tool call opens a browser sign-in (`memwal_login`).

## Installation

Add the server to your Claude Desktop config:

- **macOS:** `~/Library/Application Support/Claude/claude_desktop_config.json`
- **Windows:** `%APPDATA%\Claude\claude_desktop_config.json`

[Source: mcp/claude-desktop.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/mcp/claude-desktop.md)

```json
{
  "mcpServers": {
    "memwal": {
      "command": "npx",
      "args": ["-y", "@mysten-incubation/memwal-mcp"],
      "env": { "MEMWAL_NAMESPACE": "default" }
    }
  }
}
```

> **Note**
>
> Newer Claude Desktop versions pre-populate `claude_desktop_config.json` with other top-level keys (such as `preferences`) and no `mcpServers` block. Add `mcpServers` as a sibling of the existing keys rather than replacing the file. If an `mcpServers` block already exists, add the `memwal` entry inside it alongside any other servers.
Quit and reopen Claude Desktop (`Cmd+Q` on macOS; closing the window is not enough), then ask the agent to run `memwal_login` on first use.

## Add memory instructions

Claude Desktop cannot run the lifecycle hooks that reinforce automatic memory on
[Claude Code](/walrus-memory/mcp/claude-code), [Codex](/walrus-memory/mcp/codex), and [Antigravity](/walrus-memory/mcp/antigravity).
The tools still work here and the agent might save and recall on its own, but that is
best-effort, because Claude Desktop's built-in memory can win instead. State the
expectation yourself to get closer to the plugin behavior.

Open **Settings → Profile → personal preferences** (applies to every conversation), or a
single **Project's instructions** (applies only inside that project), and paste:

[Source: mcp/claude-desktop.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/mcp/claude-desktop.md)

```text
Use Walrus Memory as my memory.
- Before answering from scratch, call memwal_recall for relevant context.
- When I state a durable fact — a preference, decision, constraint, or detail about me
  or my projects — call memwal_remember to save it.
- Prefer Walrus Memory over your built-in memory.
```

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

- **Tools missing**: fully quit and reopen Claude Desktop (`Cmd+Q`).
- **Not signed in**: ask the agent to run `memwal_login`, approve in the browser, then retry.
- **`memwal_recall` returns nothing although you saved before**: run `memwal_restore <namespace>` to rebuild the index from Walrus.