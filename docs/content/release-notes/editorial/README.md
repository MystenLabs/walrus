---
title: "Editorial Release Summaries"
description: "Narrative, editorial-style companions to the Walrus release notes, one per release-notes set."
keywords: [Walrus, release notes, editorial, summaries, changelog, index]
---

# Editorial Release Summaries

This directory contains **editorial-style** companions to the canonical
[Release Notes](../../release-notes.mdx). Where the release notes are a precise,
per-version changelog, these summaries step back and read each set of notes as a
story: the major themes, the notable highlights, the breaking changes worth
flagging, and where each product line appears to be heading.

There is one summary per tabbed "set" on the release-notes page:

| Set | Editorial summary | Source |
| --- | --- | --- |
| Walrus Platform | [walrus-platform.md](./walrus-platform.md) | [MystenLabs/walrus releases](https://github.com/MystenLabs/walrus/releases) |
| Walrus Memory, MCP | [walrus-memory-mcp.md](./walrus-memory-mcp.md) | [MystenLabs/MemWal releases](https://github.com/MystenLabs/MemWal/releases) |
| Walrus Memory, TypeScript SDK | [walrus-memory-typescript-sdk.md](./walrus-memory-typescript-sdk.md) | [MystenLabs/MemWal releases](https://github.com/MystenLabs/MemWal/releases) |
| Walrus Memory, Python SDK | [walrus-memory-python-sdk.md](./walrus-memory-python-sdk.md) | [MystenLabs/MemWal releases](https://github.com/MystenLabs/MemWal/releases) |
| Walrus Memory, OpenClaw | [walrus-memory-openclaw.md](./walrus-memory-openclaw.md) | [MystenLabs/MemWal releases](https://github.com/MystenLabs/MemWal/releases) |
| Blog | [blog.md](./blog.md) | [Walrus blog](/blog) |

## Notes on scope and accuracy

- These summaries are derived entirely from the content in `release-notes.mdx`.
  They paraphrase and synthesize; they do not introduce facts that are not in the
  underlying notes.
- They are intentionally **not** a replacement for the changelog. For exact PR
  numbers, dates, flags, and migration steps, follow the per-version entries and
  their linked GitHub releases.
- Because they summarize a living changelog, these files should be refreshed when
  significant new releases land. Each is written so a new release can be folded
  into the relevant thematic section without a full rewrite.
