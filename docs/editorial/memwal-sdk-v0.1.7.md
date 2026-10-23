---
title: Package v0.1.7
description: restore() reports permanent decrypt or UTF-8 failures in a failed count instead of folding them into skipped or dropping them silently. Request-body h...
keywords: ["walrus memory", "release notes", "changelog", "memwal"]
---

**September 15, 2026**

`restore()` reports permanent decrypt or UTF-8 failures in a `failed` count instead of folding
them into `skipped` or dropping them silently. Request-body hashing moves to `@noble/hashes`,
removing a Node builtin import from a browser-reachable path that bundlers externalised without
warning.
