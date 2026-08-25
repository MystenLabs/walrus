> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Every supported client runs the same local server, `npx -y @mysten-incubation/memwal-mcp`, and differs only in where the configuration lives. Pick your client below, add the server, restart, and sign in.

- [x] You need Node.js 20 or later, because the server runs through `npx` with no install step.
- [x] You need a [Walrus Memory account](/walrus-memory/fundamentals/concepts/ownership-and-access). An unauthenticated memory-tool call returns sign-in instructions rather than signing you in, so ask the agent to run `memwal_login` and follow the URL it returns to connect your wallet. Config files carry no keys.

## Set up your client

| **Client** | **Where the config lives** | **Setup** |
| --- | --- | --- |
| Claude Code | Managed by the CLI | `claude mcp add --scope user memwal -- npx -y @mysten-incubation/memwal-mcp`, or install the [plugin](/walrus-memory/mcp/claude-code) for automatic-memory hooks |
| Claude Desktop | `claude_desktop_config.json` | Add the [JSON block](#config-blocks) below; see [Claude Desktop](/walrus-memory/mcp/claude-desktop) for the per-OS file path |
| Cursor | `~/.cursor/mcp.json` | Add the [JSON block](#config-blocks) below; hooks are [optional](/walrus-memory/mcp/cursor) |
| Codex | `~/.codex/config.toml` | `codex plugin marketplace add MystenLabs/MemWal` then `codex plugin add memwal@memwal-plugins` for the [plugin](/walrus-memory/mcp/codex) with automatic-memory hooks, or add the [TOML block](#config-blocks) below for MCP-only |
| OpenCode | `~/.config/opencode/opencode.json` | Add the [OpenCode block](#config-blocks) below |
| Antigravity | Plugin directory or MCP config | `npx degit MystenLabs/MemWal/packages/mcp/plugin ~/.gemini/config/plugins/memwal`, or the [JSON block](#config-blocks); see [Antigravity](/walrus-memory/mcp/antigravity) |

After any of these, restart the client (MCP servers load at startup) and ask the agent to run `memwal_login`.

### Config blocks

Every client runs the same server and differs only in the file format. The server entry is the canonical configuration from the [`packages/mcp` README](https://github.com/MystenLabs/MemWal/tree/main/packages/mcp), and a CI check keeps every copy in these docs in sync with it.

Claude Desktop, Cursor, and Antigravity's MCP config all take this shape:

    ```json
    {
      "mcpServers": {
        "memwal": {
          "command": "npx",
          "args": ["-y", "@mysten-incubation/memwal-mcp"]
        }
      }
    }
    ```

## Configure a namespace

Every recall runs inside one account and namespace. To keep a client's memories in their own space, set the `MEMWAL_NAMESPACE` environment variable in the server entry (`env` in JSON configs, `environment` in OpenCode), or pass `"--namespace", "<name>"` in `args`. Without it, memories go to the `default` namespace.

## Verify

1. Open your client's MCP status view (for example `/mcp` in Claude Code) and confirm the status view lists `memwal` as connected with its tools.
2. Ask the agent to run `memwal_health`; it returns a fast connectivity check against the relayer.
3. State a durable fact, for example a package-manager preference, confirm the agent calls `memwal_remember`, then start a fresh session and confirm `memwal_recall` finds it.

If a step fails, run `npx -y @mysten-incubation/memwal-mcp --help` in a terminal to surface the real error, and set `MEMWAL_MCP_DEBUG=1` in the server's environment for verbose logging. The [Claude Code page](/walrus-memory/mcp/claude-code#troubleshooting-faq) carries the full troubleshooting FAQ; the errors apply to every client.