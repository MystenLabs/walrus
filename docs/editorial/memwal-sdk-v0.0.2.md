---
title: Package v0.0.2
description: Adds per-request nonce signing to block replay attacks, replaces delegate key transport with ephemeral SEAL sessions, and...
keywords: ["walrus memory", "release notes", "changelog", "memwal"]
---

**April 30, 2026**

Adds per-request nonce signing to block replay attacks and replaces delegate key transport with
ephemeral SEAL sessions. SDK versions that do not include a nonce are rejected with HTTP 426.
