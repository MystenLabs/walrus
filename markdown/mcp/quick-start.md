> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

This page gets you from zero to a working Walrus Memory MCP server inside Cursor, Claude Desktop, Claude Code, or Codex.

- [x] **Node.js 20** or newer (`node -v` to check)
- [x] A **Sui wallet** with the Walrus Memory app authorized, Sui Wallet, Suiet, Phantom, or any [Sui-compatible wallet](https://memory.walrus.xyz)
- [x] An **MCP-aware client**: Cursor, Claude Desktop, Claude Code, Codex, Antigravity, or another MCP host

No npm install needed, `npx` fetches the `@mysten-incubation/memwal-mcp` package on demand.

## Choose your login path

You have two valid ways to get credentials onto the machine:

- **Inline from the MCP client**: add the package to the client config first, then let the agent call `memwal_login`
- **Manual from a terminal**: run `npx -y @mysten-incubation/memwal-mcp login --prod` yourself before wiring the client

Both paths end with the same local file: `~/.memwal/credentials.json`.

For most teams, the best default is:

- use the **stdio package** for local MCP clients
- use **Streamable HTTP** only when the client clearly supports remote MCP headers cleanly

### Sign in with your Sui wallet

    Run the login flow once from your terminal. Your browser opens to `https://memory.walrus.xyz/connect/mcp`, approve the connection in your Sui wallet.

    ```bash
    npx -y @mysten-incubation/memwal-mcp login --prod
    ```

    The package writes credentials to `~/.memwal/credentials.json`. For other environments use `--staging` or `--local`.

    :::warning
Run this in a real terminal (with a TTY). The login command opens a browser and waits for your wallet approval. If you wrap it in a non-interactive shell, the browser won't pop and the flow exits silently.
:::

  ### Add Walrus Memory to your MCP client

    Pick the snippet for your client. Drop it into the client's MCP config file.

    
      
      ```json
      // ~/.cursor/mcp.json
      {
        "mcpServers": {
          "memwal": {
            "command": "npx",
            "args": ["-y", "@mysten-incubation/memwal-mcp"]
          }
        }
      }
      ```
      
      
      ```json
      // macOS:   ~/Library/Application Support/Claude/claude_desktop_config.json
      // Windows: %APPDATA%\Claude\claude_desktop_config.json
      {
        "mcpServers": {
          "memwal": {
            "command": "npx",
            "args": ["-y", "@mysten-incubation/memwal-mcp"]
          }
        }
      }
      ```
      
      
      ```bash
      claude mcp add --scope user memwal -- npx -y @mysten-incubation/memwal-mcp
      ```
      
      
      ```toml
      # ~/.codex/config.toml
      [mcp_servers.memwal]
      command = "npx"
      args = ["-y", "@mysten-incubation/memwal-mcp"]
      ```
      
    

  ### Restart the client

    MCP servers load at client startup. Quit and reopen your MCP client (`Cmd+Q` on macOS, closing the window is not enough). On first start, `npx` fetches the package, expect a 5–10 second delay the first time.

## Common config locations

- **Cursor**: `~/.cursor/mcp.json`
- **Claude Desktop (macOS)**: `~/Library/Application Support/Claude/claude_desktop_config.json`
- **Codex**: `~/.codex/config.toml`
- **Claude Code**: managed through `claude mcp add` / `claude mcp list`

## What first run should look like

If you skip the manual terminal login and go straight to the MCP client, that is fine.

- The package does **not** crash when credentials are missing.
- Instead it starts in an **auth-required mode** that still exposes `memwal_login`.
- Ask the agent to run `memwal_login`, approve the browser flow, then retry the original memory action.

That behavior is intentional. It avoids the old UX where the MCP host only showed a vague “failed to start server” message.

### Check connectivity

Ask the agent in any conversation:

> What MCP tools do you have available?

You should see six tools:

- `memwal_remember`
- `memwal_recall`
- `memwal_analyze`
- `memwal_restore`
- `memwal_login`
- `memwal_logout`

If you only see five, or only `memwal_login`, credentials are missing. This is the expected first-run state in many MCP clients. Run the login command from step 1 again or ask the agent to call `memwal_login`.

If you do **not** see any Walrus Memory tools at all, the MCP host likely never loaded the package. Double-check the config file path, restart the client fully, and confirm you are on Node 20+.

### Save and recall a memory

```text
Use memwal_remember to save: "My favorite programming language is Rust and I drink black coffee in the mornings."
```

Wait a few seconds for the async upload to land on Walrus, then:

```text
Use memwal_recall to search for: "what is my favorite language?"
```

The agent should retrieve the memory you just saved.

### Extract multiple facts from a passage

```text
Use memwal_analyze on this paragraph: "I live in Saigon, work as a software engineer at MystenLabs, exercise at 6am, and am allergic to shellfish."
```

The tool extracts each distinct fact and saves them as separate memories. Follow up with `memwal_recall` to verify any one of them came back.

## Direct HTTP setup

If your MCP client supports remote servers with custom headers, you can connect directly to the hosted relayer instead of running `npx` locally.

Use:

- URL: `https://relayer.memory.walrus.xyz/api/mcp`
- Header: `Authorization: Bearer <delegatePrivateKey>`
- Header: `x-memwal-account-id: <accountId>`

Those values come from `~/.memwal/credentials.json` after a successful login. See [Reference](/walrus-memory/mcp/reference#streamable-http) for the full config shape and security notes.

## Switching environments

Need to hop between prod, staging, dev, or a local relayer without re-editing your client config?

```bash
$ npx -y @mysten-incubation/memwal-mcp --logout
$ npx -y @mysten-incubation/memwal-mcp login --staging
```

Your client config doesn't change, the package reads the saved environment from `~/.memwal/credentials.json` on each run. See [Environment presets](/walrus-memory/mcp/reference#environment-presets) for all four shortcuts.

## Local development

To point the package at a local dashboard + relayer during development:

```bash
$ npx -y @mysten-incubation/memwal-mcp login --local
```

That preset maps to:

- relayer: `http://127.0.0.1:8000`
- dashboard: `http://localhost:5173`

## Signing out

Ask the agent to call `memwal_logout`, or run from your terminal:

```bash
$ npx -y @mysten-incubation/memwal-mcp --logout
```

This deletes the local credentials file. The onchain delegate key is **not** revoked, visit the [Walrus Memory dashboard](https://memory.walrus.xyz) to remove it from your account if needed.

## Next steps

  
    All six tools with parameters, CLI flags, and transport routes
  
  
    Auth-required mode, local credentials, and bridge behavior
  
  
    Skip `npx`, point your client at the relayer URL directly
  
  
    Run your own relayer and route MCP traffic through it
  
  
    Full list of relayer + sidecar settings (Seal, Walrus, sessions)