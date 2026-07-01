---
title: Walrus v1.26.4
description: Adds multi-URL Sui RPC configuration via rpc_urls and removes the implicit fallback to the public Testnet full node.
keywords: ["walrus", "release notes", "changelog", "mainnet"]
---

**Mainnet** | June 10, 2025

Adds multi-URL Sui RPC configuration via `rpc_urls` and removes the implicit fallback to the public
Testnet full node. Note the breaking change where metrics and REST API endpoints now bind to the
configured address rather than always to `0.0.0.0`.
