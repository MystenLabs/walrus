---
title: Walrus v1.36.1
description: Adds a skip_consistency_check option (and --skip-consistency-check CLI flag) for use only with trusted writers, and trims...
keywords: ["walrus", "release notes", "changelog", "mainnet", "cli"]
---

**Mainnet** | November 7, 2025

Adds a `skip_consistency_check` option (and `--skip-consistency-check` CLI flag) for use only with
trusted writers, and trims `server_address` and `server_port` labels from request-duration metrics
for fast, successful responses.
