> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

### Walrus Memory MCP v0.0.11

August 24, 2026 | [GitHub](https://github.com/MystenLabs/MemWal/releases/tag/%40mysten-incubation/memwal-mcp%400.0.11)

---

### Walrus Memory MCP v0.0.10

August 20, 2026 | [GitHub](https://github.com/MystenLabs/MemWal/releases/tag/%40mysten-incubation/memwal-mcp%400.0.10)

#### Changes

- [Answer orphaned tool calls whose upstream response never arrives with a retryable error instead of hanging indefinitely. The bridge now tracks in-flight request start times and sweeps expired calls through a per-request deadline (`MEMWAL_MCP_CALL_TIMEOUT_MS`, default 240s), reuses the existing late-reply drop so an expired call cannot get a second response, and enriches reconnect logs with pending request IDs and methods.](https://github.com/MystenLabs/MemWal/pull/690)
- [Inject the configured default namespace (`--namespace` / `MEMWAL_NAMESPACE`) into `memwal_remember_bulk` calls that omit one, so bulk facts land in the project namespace instead of the relayer fallback `default`.](https://github.com/MystenLabs/MemWal/pull/667)
- [Send proactive-usage instructions in the MCP `initialize` handshake, so the model knows when to save and recall without being asked. Clients moved to lazy tool loading, which keeps tool descriptions out of the model's context until a tool is explicitly loaded; the guidance lived only in those descriptions, so the model stopped using memory on its own and would offer its built-in memory or deny the tool existed. `instructions` travels with `initialize`, before any `tools/list`, so lazy loading cannot strip it.](https://github.com/MystenLabs/MemWal/pull/681)

---

### Walrus Memory MCP v0.0.8

August 14, 2026 | [GitHub](https://github.com/MystenLabs/MemWal/releases/tag/%40mysten-incubation/memwal-mcp%400.0.8)

---

### Walrus Memory MCP v0.0.7

August 14, 2026 | [GitHub](https://github.com/MystenLabs/MemWal/releases/tag/%40mysten-incubation/memwal-mcp%400.0.7)

---

### Walrus Memory MCP v0.0.6

July 31, 2026 | [GitHub](https://github.com/MystenLabs/MemWal/releases/tag/%40mysten-incubation/memwal-mcp%400.0.6)

---

### Package v0.0.5

June 12, 2026 | [GitHub](https://github.com/MystenLabs/MemWal/releases/tag/%40mysten-incubation/memwal-mcp%400.0.5)

Adds the automatic memory plugin for Claude Code, Codex, Cursor, and Antigravity, along with new
`memwal_remember_bulk` and `memwal_health` tools and proactive memory behavior. Also fixes the
plugin bundle so it ships `.mcp.json` correctly.

---

### Package v0.0.4

June 5, 2026 | [GitHub](https://github.com/MystenLabs/MemWal/releases/tag/%40mysten-incubation/memwal-mcp%400.0.4)

Fixes HTTPS dashboard sign-in callbacks and credential reload after login so tools work without
restarting the MCP client.

---

### Package v0.0.3

June 4, 2026 | [GitHub](https://github.com/MystenLabs/MemWal/releases/tag/%40mysten-incubation/memwal-mcp%400.0.3)

Rebrands package metadata from MemWal to Walrus Memory throughout the MCP server.

---

### Package v0.0.2

May 25, 2026 | [GitHub](https://github.com/MystenLabs/MemWal/releases/tag/%40mysten-incubation/memwal-mcp%400.0.2)

Adds relayer compatibility checks and rebrands from MemWal to Walrus Memory across the MCP server
package.

---

### Package v0.0.1

May 15, 2026 | [GitHub](https://github.com/MystenLabs/MemWal/releases/tag/%40mysten-incubation/memwal-mcp%400.0.1)

Initial release of the MCP server with stdio transport, browser wallet login, and inline session
tools. Includes memory tools for remember, recall, analyze, and restore.

---