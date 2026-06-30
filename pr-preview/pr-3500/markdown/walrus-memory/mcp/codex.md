> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Add Walrus Memory to Codex so it recalls context and saves durable facts as you work. Install it as a **plugin** (recommended; adds automatic-memory hooks) or as **MCP-only** (just the tools).

- [x] Node.js 20+
- [x] A Walrus Memory account. The first memory tool call opens a browser sign-in (`memwal_login`).

## Installation

  
    ### Install the hooks + MCP server

From a cloned repo:
        ```bash
        node packages/mcp/plugin/scripts/install_codex_hooks.mjs
        ```
        This merges the Walrus Memory hooks into `~/.codex/hooks.json` and registers `[mcp_servers.memwal]` in `~/.codex/config.toml`. Re-running is safe (idempotent); add `--uninstall` to remove the hooks.

      ### Enable the hooks feature flag

Add to `~/.codex/config.toml`:
        ```toml
        [features]
        codex_hooks = true
        ```

      ### Restart and sign in

Restart Codex. On first use the agent runs `memwal_login` to connect your wallet.

  
  
    Add to `~/.codex/config.toml`:
    ```toml
    [mcp_servers.memwal]
    command = "npx"
    args = ["-y", "@mysten-incubation/memwal-mcp"]
    ```
    Restart Codex, then ask the agent to run `memwal_login` on first use.
  

> **Warning**
>
> Do not combine both options: the plugin installer already registers `[mcp_servers.memwal]`. Adding it again creates a duplicate server.
## What the plugin includes

| Component | Plugin | MCP-only |
|---|:-:|:-:|
| Walrus Memory MCP (memory tools) | ✓ | ✓ |
| Lifecycle hooks (automatic recall/save) | ✓ | ✗ |

MCP-only still saves and recalls on its own because the tools are proactive. The plugin adds hooks that reinforce the behavior and make the agent prefer Walrus Memory over any built-in memory.

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

See [Reference](/walrus-memory/mcp/reference) for full parameters.

## Lifecycle hooks (plugin only)

| Hook | Event | What it does |
|------|-------|--------------|
| Session start | `SessionStart` | Announces that memory is active and reminds the agent to use the `memwal_*` tools. |
| User prompt | `UserPromptSubmit` | Detects when your message references past work or states a durable fact, and reminds the agent to recall or save. |
| Post-tool | `PostToolUse` (Bash) | When a command errors, reminds the agent to recall prior fixes and save the resolution. |

## Verify

Ask the agent what MCP tools it has available. You should see the `memwal_*` tools, including `memwal_remember_bulk` and `memwal_health`. Then state a durable fact and confirm the agent saves it with `memwal_remember`.

## Troubleshooting

- **Tools missing**: restart Codex.
- **Duplicate `memwal` errors**: you have both the plugin and a manual `[mcp_servers.memwal]`; remove the manual entry.
- **Hooks not firing**: confirm `codex_hooks = true` under `[features]` in `~/.codex/config.toml`, and that you restarted Codex.
- **`memwal_recall` returns nothing although you saved before**: run `memwal_restore <namespace>` to rebuild the index from Walrus.