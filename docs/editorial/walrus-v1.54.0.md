---
title: Walrus v1.54.0
description: Requests for quilt patches of expired or nonexistent quilts now fail fast: the aggregator returns 404 BLOB_NOT_FOUND within about a second instead of...
keywords: ["walrus", "release notes", "changelog", "mainnet", "aggregator", "cli", "quilt"]
---

**Mainnet** | August 19, 2026

Requests for quilt patches of expired or nonexistent quilts now fail fast: the aggregator returns
404 `BLOB_NOT_FOUND` within about a second instead of 503 `BLOB_UNAVAILABLE` after a 10-20 second
fan-out, and `walrus read-quilt` reports "the blob ID does not exist" immediately rather than
timing out against the storage nodes.
