---
title: Package v0.0.14
description: Opt-in Streamable HTTP transport for the stdio bridge (MEMWAL_MCP_TRANSPORT=http), and memwal_remember now returns at accept by default instead of blo...
keywords: ["walrus memory", "release notes", "changelog", "memwal-mcp"]
---

**September 22, 2026**

Opt-in Streamable HTTP transport for the stdio bridge (`MEMWAL_MCP_TRANSPORT=http`), and
`memwal_remember` now returns at accept by default instead of blocking on the whole Walrus write.
A lost reply is answered with the relayer's health rather than a generic timeout, every tool call
is bound with its own deadline, `retry_after` is honoured, and the trusted launcher pins the
package version instead of resolving it through `npx` at start.
