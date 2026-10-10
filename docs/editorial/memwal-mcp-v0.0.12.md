---
title: Package v0.0.12
description: The bridge forwards the client's initialize.clientInfo to the relayer so sidecar logs can name the coding agent per session. memwal_recall gains an op...
keywords: ["walrus memory", "release notes", "changelog", "memwal-mcp"]
---

**September 9, 2026**

The bridge forwards the client's `initialize.clientInfo` to the relayer so sidecar logs can name
the coding agent per session. `memwal_recall` gains an optional `maxDistance` cosine cutoff, and
result lines now carry both score and distance.
