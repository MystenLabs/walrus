<!-- Copyright (c) Walrus Foundation -->
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Release Notes Editorial SOP

A script generates the release-notes page from GitHub releases and the editorial summaries in
`docs/editorial/`. The Builder Education team writes those summaries, following the ownership,
format, and resolution steps below.

## Editorial sources

`docs/site/src/scripts/generate-release-notes.js` combines two inputs:

1. **GitHub releases** from the three repositories the changelog covers: `MystenLabs/walrus`,
   `MystenLabs/MemWal`, and `MystenLabs/walrus-sites`.
2. **Editorial summaries** in `docs/editorial/`, one Markdown file per released version.

Every editorial summary lives in this repository, whichever repository published the release. When
a Walrus Memory or Walrus Sites release needs a summary, open the pull request here, not in the
product repository. The file name binds the summary to its release:

| **Product** | **Release tag** | **Editorial file** |
| --- | --- | --- |
| Walrus | `mainnet-v1.53.0`, `testnet-v1.53.0` | `docs/editorial/walrus-v1.53.0.md` |
| Walrus Memory SDK | `@mysten-incubation/memwal@0.0.7` | `docs/editorial/memwal-sdk-v0.0.7.md` |
| Walrus Memory MCP | `@mysten-incubation/memwal-mcp@0.0.9` | `docs/editorial/memwal-mcp-v0.0.9.md` |
| Walrus Memory Python | `memwal-python@0.1.4` | `docs/editorial/memwal-python-v0.1.4.md` |
| Walrus Memory OpenClaw | `@mysten-incubation/oc-memwal@0.0.5` | `docs/editorial/memwal-openclaw-v0.0.5.md` |
| Walrus Sites | `v2.12.0` | `docs/editorial/walrus-sites-v2.12.0.md` |

## How gaps surface

The `Editorial coverage` workflow runs on every published release, every Monday, and on demand. It
compares the releases in all three repositories against `docs/editorial/` and opens one tracking
issue per uncovered release, labeled `release-notes-editorial` and `documentation`. Each issue
carries the release link, the file to create, and a pre-written draft in the house format.

The workflow deduplicates against open **and** closed issues. A closed issue means the team already
handled or deliberately skipped that release, so it never reopens.

## Who owns an issue

The Builder Education team owns the `release-notes-editorial` queue. Triage happens in the regular
docs triage; whoever takes the issue assigns it to themselves.

- **Releases with GTM content:** the person who wrote the GTM content is the best author, since the
  summary restates it for a developer audience. Ask them first.
- **Maintenance releases with no content:** whoever picks up the issue writes it. You can usually
  ship the draft in the issue without edits.
- **Releases that should get no entry:** close the issue with a one-line reason. Do not delete it,
  because the closed issue is what stops the workflow from filing it again.

The team targets turnaround within the same week the issue opens, so the changelog keeps pace with
the release cadence.

## Summary format

The house format is a short frontmatter block and one or two paragraphs of prose. Keep the summary
developer-facing: what changed, why it matters, and what a reader must do about it.

```markdown
---
title: Walrus v1.50.0
description: A one-sentence summary for search and social previews.
keywords: ["walrus", "release notes", "changelog", "testnet"]
---

**Testnet** | June 3, 2026

One or two short paragraphs describing the developer-facing changes. Name the commands, endpoints,
or configuration fields that changed, and link to the pages that document them.
```

Rules that keep entries consistent:

- Write in the same voice as the rest of the docs; the [Sui documentation style guide](https://docs.sui.io/style-guide) applies.
- Describe user-visible impact, not commit history. Link the release for the full log.
- A maintenance release with nothing user-facing gets the standard maintenance sentence, which the
  generated draft already contains.
- Do not paste the raw GitHub release body. The point of the summary is that someone edited it.

## Resolving an issue

1. Create the file the issue names, starting from the draft it contains.
2. Run `npm run gen:release-notes` in `docs/site` to regenerate the page.
3. Open a pull request with both the editorial file and the regenerated output.
4. Close the issue when the pull request merges.

To check coverage locally at any time:

```sh
cd docs/site
node src/scripts/check-editorial-coverage.js              # all three sources
node src/scripts/check-editorial-coverage.js --source memwal
```
