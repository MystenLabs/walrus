> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Add Walrus Memory to Cursor so the agent recalls context and saves durable facts. Install it as a **plugin** (adds automatic-memory hooks) or as **MCP-only** (just the tools).

- [x] Node.js 20+
- [x] A Walrus Memory account. The first memory tool call opens a browser sign-in (`memwal_login`).

## Installation

Cursor loads plugins from `~/.cursor/plugins/local/<name>/`. Copy the plugin (MCP server + lifecycle hooks) into that directory:
    ```bash
    npx -y degit MystenLabs/MemWal/packages/mcp/plugin ~/.cursor/plugins/local/memwal
    ```
    Append `--force` when you reinstall over an existing copy.

    Fully quit and reopen Cursor (`Cmd+Q` on macOS; closing the window is not enough). The plugin then appears under **Customize** in the sidebar, and its hooks give proactive recall and save with no further instructions.

    If you already added a manual `memwal` entry to `~/.cursor/mcp.json`, remove it. The plugin ships its own server and the two entries duplicate it.

> **Note**
>
> Cursor ships no plugin CLI. Do not look for a `cursor plugin` command, and do not try to add this repository as a marketplace: importing a third-party marketplace needs a Cursor team admin. The copy above is the install.
## What the plugin includes

| Component | Plugin | MCP-only |
|---|:-:|:-:|
| Walrus Memory MCP (memory tools) | ✓ | ✓ |
| Lifecycle hooks (automatic recall/save) | ✓ | ✗ |

## Lifecycle hooks (plugin)

The plugin runs **lifecycle hooks** on Cursor's own events:

| Hook | Cursor event | What it does |
|------|--------------|--------------|
| Session start | `sessionStart` | Tells the agent to prefer the `memwal_*` tools over any built-in or local memory feature. |
| Before prompt | `beforeSubmitPrompt` | Detects recall/remember intent and reminds the agent. |
| Post-tool | `postToolUse` (Bash) | On command errors, reminds the agent to recall prior fixes. |

The hook scripts ship inside the plugin bundle (`packages/mcp/plugin/`), so the copy above installs them alongside the MCP server. The MCP-only setup gives the tools without these hooks.

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

Ask the agent what MCP tools it has available. You should see the `memwal_*` tools. State a durable fact (for example, a preferred package manager) and confirm the agent saves it with `memwal_remember`.

Cursor has no plugin CLI to list what loaded, so confirm the plugin from its logs instead. The newest folder under `~/Library/Application Support/Cursor/logs/` on macOS gets an `mcp-server-plugin-memwal-memwal.log` once the plugin loads. The `plugin-` prefix is Cursor's own naming for a plugin-scoped server.

## Troubleshooting

- **Tools missing**: restart Cursor; check the MCP connection status in Settings.
- **No plugin after restart**: confirm `~/.cursor/plugins/local/memwal` exists and holds `.cursor-plugin/plugin.json`, then fully quit and reopen Cursor and check for the log file named above.
- **`MCP rate limit: ip_active_cap` (HTTP 429), sometimes followed by 503**: too many concurrent `memwal-mcp` sessions from one machine. The usual cause is a duplicate server, where the plugin sits next to a leftover manual `memwal` entry in `~/.cursor/mcp.json`. An `mcp-server-user-memwal.log` beside the plugin log confirms it. Remove the manual entry, then list leftover processes with `pgrep -fl memwal-mcp` and close the clients still holding them before restarting Cursor.
- **Not signed in**: ask the agent to run `memwal_login`, approve in the browser, then retry.
- **`memwal_recall` returns nothing although you saved before**: run `memwal_restore <namespace>` to rebuild the index from Walrus.