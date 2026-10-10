---
title: Walrus Sites v2.10.0
description: Aggregator timing rework: each attempt is bounded by a 10-second fetch timeout (AGGREGATOR_REQUEST_TIMEOUT_MS) and fails over to the next URL, and Bun...
keywords: ["walrus sites", "release notes", "changelog", "mainnet", "portal", "aggregator"]
---

**Mainnet** | June 4, 2026

Aggregator timing rework: each attempt is bounded by a 10-second fetch timeout
(`AGGREGATOR_REQUEST_TIMEOUT_MS`) and fails over to the next URL, and Bun's `idleTimeout` is sized
from the full retry budget, so a slow aggregator now surfaces the portal's own failure page
instead of tripping the inbound connection and returning a generic proxy error.
