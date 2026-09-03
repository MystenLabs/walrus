> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Add Walrus Memory to Codex so it recalls context and saves durable facts as you work. Install it as a **plugin** (recommended; adds automatic-memory hooks) or as **MCP-only** (just the tools).

- [x] Node.js 20+
- [x] A Codex CLI build with `codex plugin` support (shipped since ~April 2026; check with `codex plugin --help`) if you want the plugin install. MCP-only works on any version.
- [x] A Walrus Memory account. The first memory tool call opens a browser sign-in (`memwal_login`).

## Installation

### Add the marketplace and install

[Source: mcp/codex.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/mcp/codex.md)

```bash
        codex plugin marketplace add MystenLabs/MemWal
        codex plugin add memwal@memwal-plugins
        codex plugin list
        ```
        This registers the Walrus Memory plugin, which bundles the MCP server and the lifecycle hooks together as plugin-scoped resources. It does not write a `[mcp_servers.memwal]` block to `~/.codex/config.toml`.

      ### Trust the plugin hooks

Codex loads plugin-bundled hooks but does not run them until you trust the definition. Run `/hooks` (or follow the startup review prompt) and trust the Walrus Memory hook commands.

      ### Restart and sign in

Restart Codex. On first use the agent runs `memwal_login` to connect your wallet.

    :::note
Older Codex CLI builds without `codex plugin` support can still get the hooks from a cloned repo:
    ```bash
    node packages/mcp/plugin/scripts/install_codex_hooks.mjs
    ```
    This merges the Walrus Memory hooks into `~/.codex/hooks.json` and registers `[mcp_servers.memwal]` in `~/.codex/config.toml`. Re-running is safe (idempotent); add `--uninstall` to remove the hooks. This path writes the hooks file directly, so it needs `[features] codex_hooks = true` (or the modern equivalent `[features] hooks = true`, already the default) rather than the plugin trust flow above.
:::

> **Warning**
>
> Do not combine options: the plugin bundles the memwal MCP server on its own. Do not also add a manual `[mcp_servers.memwal]` block, which duplicates the server. (The cloned-repo fallback installer does write `[mcp_servers.memwal]` directly, so that one warning doesn't apply if you used it instead.)
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
| User prompt | `UserPromptSubmit` | Injects a decision rubric so the agent chooses recall vs save from meaning (any language or spelling). |
| Post-tool | `PostToolUse` (Bash) | When a command errors, reminds the agent to recall prior fixes and save the resolution. |

## Verify

Ask the agent what MCP tools it has available. You should see the `memwal_*` tools, including `memwal_remember_bulk` and `memwal_health`. Then state a durable fact and confirm the agent saves it with `memwal_remember`.

## Troubleshooting

- **Tools missing**: restart Codex.
- **Duplicate `memwal` errors**: the plugin already bundles the MCP server; you likely also have a manual `[mcp_servers.memwal]` block. Remove it.
- **Hooks not firing (plugin install)**: run `/hooks` and confirm the Walrus Memory hooks are trusted, not just installed.
- **Hooks not firing (cloned-repo fallback)**: confirm `codex_hooks = true` under `[features]` in `~/.codex/config.toml`, and that you restarted Codex.
- **`memwal_recall` returns nothing although you saved before**: run `memwal_restore <namespace>` to rebuild the index from Walrus.