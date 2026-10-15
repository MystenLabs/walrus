---
title: Walrus Sites v2.9.0
description: Performance release: resource dynamic-field fetching collapses into a single batched multi_get call instead of chunked queries with inter-batch delays...
keywords: ["walrus sites", "release notes", "changelog", "mainnet", "site-builder", "performance"]
---

**Mainnet** | April 23, 2026

Performance release: resource dynamic-field fetching collapses into a single batched `multi_get`
call instead of chunked queries with inter-batch delays, and `sitemap` uses the active wallet
environment rather than whichever env was listed first.
