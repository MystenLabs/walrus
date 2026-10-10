---
title: Walrus Sites v2.8.0
description: Server-side redirects arrive: rules are stored as dynamic fields on the on-chain Site object, configured in ws-resources.json with glob matching, and...
keywords: ["walrus sites", "release notes", "changelog", "mainnet", "redirects", "move", "portal"]
---

**Mainnet** | April 2, 2026

Server-side redirects arrive: rules are stored as dynamic fields on the on-chain `Site` object,
configured in `ws-resources.json` with glob matching, and evaluated by the portal before fetching
a resource, serving 3xx responses. Includes a Move contract upgrade, so point `package_id` in
`sites-config.yaml` at the new package or drop it for MVR resolution. Portal config key
`site_package` becomes `original_package_id`, and the portal moved to Mysten SDK v2.
