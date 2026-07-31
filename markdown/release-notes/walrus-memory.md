> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

### Walrus Memory MCP v0.0.6

July 31, 2026 | [GitHub](https://github.com/MystenLabs/MemWal/releases/tag/%40mysten-incubation/memwal-mcp%400.0.6)

#### 0.0.6

#### Security

- Require a localhost preflight handshake proving the exact state, public key, and relayer before accepting a delegate-key login callback.

#### Fixed

- Authenticate MCP SSE POST messages and replayed requests with the delegate credentials.
- Keep the canonical login label from the verified local flow instead of trusting the browser callback.

---

### Walrus Memory MCP v0.0.5

June 12, 2026 | [GitHub](https://github.com/MystenLabs/MemWal/releases/tag/%40mysten-incubation/memwal-mcp%400.0.5)

Adds the automatic memory plugin for Claude Code, Codex, Cursor, and Antigravity, along with new
`memwal_remember_bulk` and `memwal_health` tools and proactive memory behavior. Also fixes the
plugin bundle so it ships `.mcp.json` correctly.

#### 0.0.5

#### Added

- Automatic memory plugin for Claude Code, Codex, Cursor, and Antigravity.
- New `memwal_remember_bulk` and `memwal_health` tools.

#### Fixed

- Ship the plugin's `.mcp.json` in the marketplace bundle. A root gitignore rule excluded it, so plugin installs loaded the lifecycle hooks but never registered the MCP server.

#### Changed

- Memory tools are now proactive — agents recall and save context on their own.

#### Fixed

- Plugin bundle now ships its `.mcp.json` so the MCP server registers on install.
- Automatically recover from dropped relayer connections that could hang tool calls.

---

### Walrus Memory MCP v0.0.4

June 5, 2026 | [GitHub](https://github.com/MystenLabs/MemWal/releases/tag/%40mysten-incubation/memwal-mcp%400.0.4)

Fixes HTTPS dashboard sign-in callbacks and credential reload after login so tools work without
restarting the MCP client.

#### 0.0.4

#### Fixed

- Accept HTTPS dashboard sign-in callbacks to the local `127.0.0.1` MCP listener.
- Reload credentials after `memwal_login` so memory tools work without restarting the MCP client.

---

### Walrus Memory MCP v0.0.3

June 4, 2026 | [GitHub](https://github.com/MystenLabs/MemWal/releases/tag/%40mysten-incubation/memwal-mcp%400.0.3)

Rebrands package metadata from MemWal to Walrus Memory throughout the MCP server.

#### 0.0.3

#### Changed

- Rebranded package metadata and documentation from MemWal to Walrus Memory.

---

### Walrus Memory MCP v0.0.2

May 25, 2026 | [GitHub](https://github.com/MystenLabs/MemWal/releases/tag/%40mysten-incubation/memwal-mcp%400.0.2)

Adds relayer compatibility checks and rebrands from MemWal to Walrus Memory across the MCP server
package.

#### 0.0.2

#### Added

- Added relayer compatibility metadata checks before opening the MCP bridge.

#### Changed

- Rebranded package metadata and documentation from MemWal to Walrus Memory.

---

### Walrus Memory MCP v0.0.1

May 15, 2026 | [GitHub](https://github.com/MystenLabs/MemWal/releases/tag/%40mysten-incubation/memwal-mcp%400.0.1)

Initial release of the MCP server with stdio transport, browser wallet login, and inline session
tools. Includes memory tools for remember, recall, analyze, and restore.

#### 0.0.1

#### Initial Release

- Stdio MCP server for MemWal with browser-based wallet login.
- Inline `memwal_login` and `memwal_logout` session tools.
- Memory tools for remember, recall, analyze, and restore through the MemWal relayer.
- Environment presets for production, dev, staging, and local relayers.

---