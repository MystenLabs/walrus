---
title: Package v0.0.10
description: The signed-out tools/list stays conservative so a model without credentials does not spam memwal_remember, while memwal_recall is advertised as read-o...
keywords: ["walrus memory", "release notes", "changelog", "memwal-mcp"]
---

**August 20, 2026**

The signed-out `tools/list` stays conservative so a model without credentials does not spam
`memwal_remember`, while `memwal_recall` is advertised as read-only so clients that gate on
destructive tools still recall proactively. Cold-start descriptions align with the sidecar's.
