> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Walrus Memory exposes a **Model Context Protocol (MCP) server** so MCP-aware clients can read from and write to your portable agent memory. Use it when you want Cursor, Claude Desktop, Claude Code, Codex, Antigravity, or any other MCP client to call Walrus Memory directly from an agent workflow, without writing custom integration code.

## Features

  

**Six Built-In Tools**

`memwal_remember`, `memwal_recall`, `memwal_analyze`, `memwal_restore`, `memwal_login`, `memwal_logout`

  

**Inline Browser Login**

Agents call `memwal_login` to open a browser sign-in, no separate CLI step, no client restart

  

**Two Transports**

Streamable HTTP for remote MCP clients, or stdio package (`npx`) for local-command clients

  

**Fully Under Your Control**

Seal-encrypted, stored on Walrus, tied to your delegate key, programmable permissions and explicit ownership

  

**Portable Across Clients**

Memories saved from Cursor surface in Claude Desktop, Codex, and vice versa, not locked to any single client

  

**Environment Presets**

`--prod` / `--staging` / `--local` flags switch networks without editing client configs

## When to use this

- You want an AI client to **call Walrus Memory directly**, no custom SDK integration code in your app
- You need the agent to **remember across conversations and sessions**
- You're running **multiple MCP clients** and want all of them to share one memory store
- You need **encrypted, user-owned memory** instead of platform-managed storage

## Supported clients

The package is designed first for MCP hosts that run **local commands**:

- Cursor
- Claude Desktop
- Claude Code
- Codex
- Antigravity

If your MCP host supports **remote Streamable HTTP** servers with custom headers, you can also skip the local package and point directly at the relayer. See [Reference](/walrus-memory/mcp/reference#streamable-http).

## Get started

  
    Install the package, sign in with your wallet, wire your client, and run your first tool call
  
  
    Auth-required mode, inline browser login, local credential storage, and the stdio bridge
  
  
    All six tools, CLI flags, environment presets, transport routes, and self-hosting notes
  
  
    Release history for the `@mysten-incubation/memwal-mcp` package
  
  
    Browse the `@mysten-incubation/memwal-mcp` package on GitHub
  
  
    Manage delegate keys, view storage, and revoke connected clients
  

## What happens on the client machine

The MCP package is not just a thin HTTP wrapper.

1. It checks for `~/.memwal/credentials.json`.
2. If the file is missing, it starts in an **auth-required mode** instead of crashing the MCP host.
3. In that mode the agent can still call `memwal_login` inline.
4. After wallet approval, the package writes credentials locally and future Walrus Memory tool calls succeed without reconfiguring the client.
5. Once signed in, the package bridges local stdio MCP traffic to the relayer and keeps `memwal_login` and `memwal_logout` local-only.

See [How It Works](/walrus-memory/mcp/how-it-works) for the full flow and security model.

## Why use the package instead of raw HTTP

- Most MCP hosts support local `command + args` servers before they support remote auth UX cleanly.
- The package can open the browser flow, save credentials, and recover from missing auth inline.
- It keeps bearer credentials out of the MCP client config in the common stdio path.

## Default memory namespace

Memory tools take an optional `namespace` so you can keep, say, `work` and
`personal` memories separate. Instead of having the agent pass it on every
call, pin a default once in your client config, the package fills it in
whenever the agent omits one.

**Cursor** (`~/.cursor/mcp.json`):

```json
{
  "mcpServers": {
    "memwal": {
      "command": "npx",
      "args": ["-y", "@mysten-incubation/memwal-mcp", "--namespace", "work"]
    }
  }
}
```

**Claude Desktop** (`claude_desktop_config.json`), through env var:

```json
{
  "mcpServers": {
    "memwal": {
      "command": "npx",
      "args": ["-y", "@mysten-incubation/memwal-mcp"],
      "env": { "MEMWAL_NAMESPACE": "work" }
    }
  }
}
```

An explicit per-call `namespace` from the agent always wins over the
configured default. If neither a flag/env default nor a per-call value is
set, the relayer applies its own `"default"` namespace. See
[Reference](/walrus-memory/mcp/reference#default-namespace) for the full precedence rules
and `memwal_restore` behavior.

## What the MCP package adds

Compared with wiring a raw HTTP MCP endpoint by hand, the package adds a few important runtime behaviors:

- **First-run recovery**: when credentials are missing, the MCP host still gets a healthy server plus `memwal_login`
- **Local session tools**: `memwal_login` and `memwal_logout` are handled on the client machine instead of forwarded upstream
- **Automatic tool surfacing**: the package injects local session tools alongside the relayer-backed memory tools
- **Session resilience**: the stdio bridge reconnects to the relayer if the underlying SSE session is dropped
- **Safer defaults**: the common `npx` path avoids pasting long-lived bearer credentials into client config files