---
title: Package v0.1.5
description: recall() results carry dropped_count when the relayer omits matches that failed to download or decrypt, and health() surfaces relayer write-path liven...
keywords: ["walrus memory", "release notes", "changelog", "memwal"]
---

**August 26, 2026**

`recall()` results carry `dropped_count` when the relayer omits matches that failed to download or
decrypt, and `health()` surfaces relayer write-path liveness. `rememberManual` now sends the
base64 SEAL ciphertext directly instead of a pre-uploaded blob id, matching the manual endpoint.
