---
title: Package v0.1.6
description: recall() gains write-time (created_at) on results plus sort: "recent" and scoring weights, which over-fetches semantic candidates and orders them by w...
keywords: ["walrus memory", "release notes", "changelog", "memwal"]
---

**September 9, 2026**

`recall()` gains write-time (`created_at`) on results plus `sort: "recent"` and scoring weights,
which over-fetches semantic candidates and orders them by write time. Relayer 503s marked
`AUTH_UPSTREAM_UNAVAILABLE` are now reported as retryable credential-verification outages.
