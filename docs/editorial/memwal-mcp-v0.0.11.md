---
title: Package v0.0.11
description: The SSE handshake socket is no longer aborted on HTTP 429, 401, or a bad content type, a crash path that fired on Windows. login without a TTY now exi...
keywords: ["walrus memory", "release notes", "changelog", "memwal-mcp"]
---

**August 24, 2026**

The SSE handshake socket is no longer aborted on HTTP 429, 401, or a bad content type, a crash
path that fired on Windows. `login` without a TTY now exits 1 instead of reporting success, and
concurrent `memwal_login` calls reuse the in-flight listener and URL.
