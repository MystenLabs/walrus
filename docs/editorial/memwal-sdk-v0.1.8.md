---
title: Package v0.1.8
description: recall() now sends its own deadline (deadline_ms) so a relayer that is about to miss it answers with a 504 naming the stuck step (auth, embed, vector_...
keywords: ["walrus memory", "release notes", "changelog", "memwal"]
---

**September 22, 2026**

`recall()` now sends its own deadline (`deadline_ms`) so a relayer that is about to miss it
answers with a 504 naming the stuck step (`auth`, `embed`, `vector_search`, `walrus_download`,
`seal_decrypt`) instead of the request aborting with nothing to show.
