---
title: Package v0.0.13
description: Stop telling users to retry a write whose reply was lost: the relayer answers remember, bulk, and analyze with HTTP 202 and finishes in a durable queu...
keywords: ["walrus memory", "release notes", "changelog", "memwal-mcp"]
---

**September 15, 2026**

Stop telling users to retry a write whose reply was lost: the relayer answers remember, bulk, and
analyze with HTTP 202 and finishes in a durable queue, so a client-side timeout does not mean the
write failed. Also: backoff on 429 handshakes, auth errors surfaced instead of parking calls,
credentials written via atomic 0600 rename, and `memwal_restore` retries pages whose truncation
was a download blip.
