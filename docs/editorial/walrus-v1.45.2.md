---
title: Walrus v1.45.2
description: Raises the storage node's RocksDB background threads from 2 to 16 and adds the opt-in garbage_collection.enable_immediate_data_deletion parameter.
keywords: ["walrus", "release notes", "changelog", "mainnet", "storage node", "cli", "rocksdb"]
---

**Mainnet** | April 1, 2026

Raises the storage node's RocksDB background threads from 2 to 16 and adds the opt-in `garbage_collection.enable_immediate_data_deletion` parameter. Also fixes a combined-daemon body size limit that was silently rejecting uploads with 413 errors and adds a `walrus info coin` CLI subcommand.
