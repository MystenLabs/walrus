---
title: Package v0.0.6
description: Security hardening around delegate-key login: a localhost preflight handshake must prove the exact state, public key, and relayer before a callback is...
keywords: ["walrus memory", "release notes", "changelog", "memwal-mcp"]
---

**July 31, 2026**

Security hardening around delegate-key login: a localhost preflight handshake must prove the exact
state, public key, and relayer before a callback is accepted, and SSE POST messages and replayed
requests authenticate with the delegate credentials.
