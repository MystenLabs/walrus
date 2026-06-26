---
title: Walrus v1.47.1
description: Enables storage pool support in the storage node and makes the aggregator return a retryable HTTP 503 BLOB_UNAVAILABLE (instead of 500) when a blob is...
keywords: ["walrus", "release notes", "changelog", "mainnet", "storage node", "aggregator", "blob"]
---

**Mainnet** | April 29, 2026

Enables storage pool support in the storage node and makes the aggregator return a retryable HTTP 503 `BLOB_UNAVAILABLE` (instead of 500) when a blob is only temporarily unretrievable. Blob attribute headers such as `Content-Type` are now correctly included on range requests when reading by object ID.
