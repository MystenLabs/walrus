---
title: Walrus Sites v2.0.0
description: Major release: site-builder moves exclusively to quilts, cutting publishing cost and time, especially for larger sites. Updates re-upload all site dat...
keywords: ["walrus sites", "release notes", "changelog", "mainnet", "quilts", "site-builder", "breaking"]
---

**Mainnet** | November 17, 2025

Major release: `site-builder` moves exclusively to quilts, cutting publishing cost and time,
especially for larger sites. Updates re-upload all site data at this stage (redundant uploads were
removed in v2.2.1), so operators were pointed at dry-running updates on Testnet or republishing in the meantime.
`update-resource` was redesigned and renamed to `update-resources`, taking multiple resources in
one operation, and four blob-era flags are gone. Requires Walrus CLI v1.35.0 or later.
