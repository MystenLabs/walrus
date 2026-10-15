---
title: Package v0.0.6
description: openclaw plugins install works again. The manifest had marked privateKey, accountId, and serverUrl as required in configSchema, so OpenClaw rejected t...
keywords: ["walrus memory", "release notes", "changelog", "oc-memwal"]
---

**August 20, 2026**

`openclaw plugins install` works again. The manifest had marked `privateKey`, `accountId`, and
`serverUrl` as required in `configSchema`, so OpenClaw rejected the config entry the installer
writes before credentials exist; validation moves to `parseConfig`, which reports clearer
per-field errors.
