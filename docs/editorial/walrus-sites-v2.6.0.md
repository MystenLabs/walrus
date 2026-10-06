---
title: Walrus Sites v2.6.0
description: Portal configuration now loads from a YAML file (portal-config.yaml by default, PORTAL_CONFIG to override the path) instead of a spray of environment...
keywords: ["walrus sites", "release notes", "changelog", "mainnet", "portal", "configuration"]
---

**Mainnet** | February 26, 2026

Portal configuration now loads from a YAML file (`portal-config.yaml` by default, `PORTAL_CONFIG`
to override the path) instead of a spray of environment variables, which still work as overrides;
priority/retry URL lists are native YAML instead of pipe-delimited strings. This release also
carries v2.5.0's priority-based executor for RPC and aggregator URLs, replacing `Promise.any()`
racing, with `AGGREGATOR_URL` renamed to `AGGREGATOR_URL_LIST`.
