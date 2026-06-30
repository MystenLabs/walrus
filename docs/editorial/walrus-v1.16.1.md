---
title: Walrus v1.16.1
description: Lets the CLI delete command accept multiple files, blob IDs, or object IDs (a breaking change to the delete JSON API) and adds...
keywords: ["walrus", "release notes", "changelog", "testnet", "publisher", "cli", "blob"]
---

**Testnet** | March 5, 2025

Lets the CLI `delete` command accept multiple files, blob IDs, or object IDs (a breaking change to
the delete JSON API) and adds multi-context configuration with a `--context` override. The publisher
now keeps created `Blob` objects by default, replacing `--keep` with `--burn-after-store`.
