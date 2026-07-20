# Walrus Memory Docs Integration — Design Document

## Goal

Serve the documentation from [MystenLabs/MemWal](https://github.com/MystenLabs/MemWal/tree/dev/docs)
(rebranded as **Walrus Memory**) alongside the existing Walrus docs at `docs.wal.app/walrus-memory/`,
built as part of the Docusaurus build in `MystenLabs/walrus`. The MemWal docs stay in their own repo
and are never moved.

---

## Current State

### Walrus Docusaurus site (`~/walrus/docs/site/`)

| Aspect | Detail |
|--------|--------|
| Framework | Docusaurus 3.9.2, classic preset |
| Content path | `../content` (i.e. `docs/content/`) |
| Sidebars | 4: `docsSidebar`, `sitesSidebar`, `operatorSidebar`, `examplesSidebar` |
| Build scripts | `prestart`/`prebuild` → `generate-import-context.js`, `copy-yaml-files.js`; `build:prep` → `inline-imports.js`, `copy-markdown-files.js`, `generate-llmstxt.mjs`; post-build → `generate-routes.js` |
| MDXComponents | Overridden in `src/theme/MDXContent/index.js` — registers `Link`, `Term`, `ImportContent`, `Tabs`, `TabItem`, `Cards`, `Card` |
| Shared components | `src/shared/components/` — `Cards/Card`, `ImportContent`, `Snippet`, `RelatedLink`, `Glossary`, `UnsafeLink`, `ThemeToggle`, `SidebarIframe` |
| External content | Uses git subtree for `src/shared/` from `ML-Shared-Docusaurus`; no other external content sources |

### MemWal docs (`MystenLabs/MemWal`, branch `dev`, path `docs/`)

| Aspect | Detail |
|--------|--------|
| Framework | Mintlify (`docs.json` with `$schema: mintlify.com/docs.json`) |
| File count | ~60 `.md`/`.mdx` files across 16 subdirectories |
| Frontmatter | `title` and `description` only (no `sidebar_position`, no imports) |
| Navigation | 11 tabs defined in `docs.json` |
| Assets | `logo-dark.svg`, `logo-light.svg`, `favicon.ico`, `src/css/custom.css` |
| Internal links | Absolute paths like `/sdk/usage/memwal` (relative to docs root, no `.md` extension) |

### Mintlify components used in MemWal docs

| Component | Count | Docusaurus equivalent |
|-----------|-------|-----------------------|
| `<Note>` | 11 | `:::note` admonition |
| `<Warning>` | 7 | `:::warning` admonition |
| `<Tip>` | 2 | `:::tip` admonition |
| `<Info>` | 1 | `:::info` admonition |
| `<CardGroup cols={N}>` | 10 | Walrus already has `<Cards>` component |
| `<Card title icon href>` | 51 | Walrus already has `<Card>` component (different props) |
| `<CodeGroup>` | 5 | `<Tabs groupId="pkg-manager">` + `<TabItem>` |
| `<Steps>` / `<Step>` | 4 | `<Cards type="steps">` (existing) or custom |
| `<Tabs>` / `<Tab title>` | 4 | `<Tabs>` + `<TabItem>` (already registered) |
| `<Accordion title>` | 1 | `<details><summary>` |

---

## Design

### Architecture overview

```
Build time:
  fetch-walrus-memory-docs.js
    │
    ▼
  git sparse-checkout of MystenLabs/MemWal:dev → docs/walrus-memory-content/
    │
    ▼
  transform-walrus-memory-docs.js
    │  • Rewrite Mintlify components → Docusaurus equivalents
    │  • Rewrite internal links: /foo → /walrus-memory/foo
    │  • Rename "MemWal" → "Walrus Memory" in content
    │  • Add sidebar_position from docs.json ordering
    │
    ▼
  docs/walrus-memory-content/  (transformed, gitignored)
    │
    ▼
  Docusaurus multi-instance docs plugin
    │  id: 'walrus-memory'
    │  path: '../walrus-memory-content'
    │  routeBasePath: 'walrus-memory'
    │  sidebarPath: './sidebarsWalrusMemory.js'
    │
    ▼
  Built site serves at /walrus-memory/*
```

### 1. Fetch script — `src/scripts/fetch-walrus-memory-docs.js`

**What it does:**
- Sparse-clones `MystenLabs/MemWal` branch `dev`, extracting only the `docs/` directory
- Copies the markdown/MDX files into `docs/walrus-memory-content/`
- Excludes non-content files: `docs.json`, `package.json`, `docs.Dockerfile`, `llms.txt`, `llms-full.txt`, `src/css/`, logos, favicon
- Skips execution if `docs/walrus-memory-content/` already exists and is fresh (mtime < 10 minutes) to avoid re-cloning on every `npm start` during development — pass `--force` to bypass

**When it runs:**
- Added to `prestart` and `prebuild` scripts in `package.json`, before `generate-import-context.js`

**Error handling:**
- If the clone fails (no network, repo unavailable), the script logs a warning and exits 0 so local development isn't blocked. The Docusaurus build will fail if the content dir is missing and the plugin is configured, which is the correct behavior for CI.

### 2. Transform script — `src/scripts/transform-walrus-memory-docs.js`

**What it does:**
Runs immediately after the fetch script. Processes every `.md`/`.mdx` file in `docs/walrus-memory-content/` with the following transformations:

#### a. Mintlify component → Docusaurus syntax

| Mintlify | Transformed to | Rationale |
|----------|---------------|-----------|
| `<Note>...</Note>` | `:::note\n...\n:::` | Native Docusaurus admonition — no component needed |
| `<Warning>...</Warning>` | `:::warning\n...\n:::` | Same |
| `<Tip>...</Tip>` | `:::tip\n...\n:::` | Same |
| `<Info>...</Info>` | `:::info\n...\n:::` | Same |
| `<CodeGroup>` | `<Tabs groupId="code">\n` | Reuses existing registered `Tabs` |
| Each ` ```lang title\n` inside CodeGroup | Wrapped in `<TabItem value="title" label="title">` | Standard Docusaurus pattern |
| `</CodeGroup>` | `</Tabs>` | — |
| `<Tabs>` | `<Tabs>` | Already registered |
| `<Tab title="X">` | `<TabItem value="X" label="X">` | Attribute rename |
| `</Tab>` | `</TabItem>` | — |
| `<CardGroup cols={N}>` | `<Cards>` | Existing Walrus shared component (CSS grid, ignores cols since it already handles responsive layout) |
| `</CardGroup>` | `</Cards>` | — |
| `<Card title="T" icon="I" href="H">` | `<Card title="T" href="/walrus-memory/H">` | Existing Walrus `Card` component. `icon` prop is dropped (Walrus Card doesn't support it). `href` is rewritten to include base path. Cards with no `href` are left without it. |
| `<Card title="T" icon="I">..body..</Card>` | `<Card title="T">..body..</Card>` | Body content preserved, icon dropped |
| `<Steps>` | `<Cards type="steps">` | Walrus shared component has a `type="steps"` mode that renders numbered steps |
| `<Step>` / `</Step>` | `<Card>` / `</Card>` | Each step becomes a card within the steps container |
| `</Steps>` | `</Cards>` | — |
| `<Accordion title="X">` | `<details><summary>X</summary>` | Native HTML, universally supported |
| `</Accordion>` | `</details>` | — |

**Important:** The `<Cards>` and `<Card>` components from `src/shared/components/Cards/` are already registered in `MDXContent/index.js`. The `<Tabs>` and `<TabItem>` components are also already registered. No new MDX component registrations are needed for the main docs instance.

However, the multi-instance plugin creates a **separate docs instance** that does NOT inherit the classic preset's MDX component overrides. We need to handle this (see section 4 below).

#### b. Internal link rewriting

All internal links pointing to MemWal doc pages need the `/walrus-memory/` prefix:

- `[text](/sdk/quick-start)` → `[text](/walrus-memory/sdk/quick-start)`
- `<Card href="/relayer/overview">` → `<Card href="/walrus-memory/relayer/overview">`
- Only rewrites paths that correspond to known MemWal doc pages (derived from `docs.json`)
- Does NOT rewrite external URLs or anchors

#### c. "MemWal" → "Walrus Memory" renaming

- Replaces `MemWal` → `Walrus Memory` in prose text, titles, and descriptions
- Preserves technical identifiers: package names (`@mysten-incubation/memwal`), code snippets, import statements, variable names, CLI commands
- Strategy: Replace in frontmatter `title` and `description` fields unconditionally. In body text, replace only when `MemWal` appears as a standalone word (not inside backticks, code blocks, or URLs)

#### d. Frontmatter enrichment

- Adds `slug` field derived from file path (Docusaurus uses this for URL generation)
- Converts Mintlify `title`/`description` to Docusaurus-compatible frontmatter (same fields, already compatible)

### 3. Sidebar — `sidebarsWalrusMemory.js`

A static sidebar file derived from the `docs.json` navigation. Using a static file (not dynamically generated) because:
- The MemWal nav structure changes infrequently
- A static file is debuggable and reviewable
- Avoids needing to parse `docs.json` at build time

```js
const sidebars = {
  walrusMemorySidebar: [
    {
      type: 'category',
      label: 'Getting Started',
      items: [
        'getting-started/what-is-walrus-memory',
        'getting-started/quick-start',
        'getting-started/choose-your-path',
      ],
    },
    {
      type: 'category',
      label: 'Fundamentals',
      items: [
        {
          type: 'category',
          label: 'Concepts',
          items: [
            'fundamentals/concepts/memory-space',
            'fundamentals/concepts/ownership-and-access',
          ],
        },
        {
          type: 'category',
          label: 'Architecture',
          items: [
            'fundamentals/architecture/core-components',
            'fundamentals/architecture/how-storage-works',
            'fundamentals/architecture/data-flow-security-model',
          ],
        },
      ],
    },
    {
      type: 'category',
      label: 'TypeScript SDK',
      items: [
        'sdk/quick-start',
        {
          type: 'category',
          label: 'Usage',
          items: [
            'sdk/usage/memwal',
            'sdk/usage/memwal-manual',
            'sdk/usage/with-memwal',
          ],
        },
        'sdk/api-reference',
        'sdk/changelog',
      ],
    },
    {
      type: 'category',
      label: 'Python SDK',
      items: [
        'python-sdk/quick-start',
        {
          type: 'category',
          label: 'Usage',
          items: [
            'python-sdk/usage/memwal',
            'python-sdk/usage/memwal-manual',
            'python-sdk/usage/with-memwal',
          ],
        },
        'python-sdk/api-reference',
        'python-sdk/changelog',
      ],
    },
    {
      type: 'category',
      label: 'Relayer',
      items: [
        'relayer/overview',
        'relayer/public-relayer',
        'relayer/self-hosting',
        'relayer/nautilus-tee',
        'relayer/observability',
        'relayer/versioning-and-compatibility',
        'relayer/api-reference',
      ],
    },
    {
      type: 'category',
      label: 'MCP',
      items: [
        'mcp/overview',
        'mcp/quick-start',
        'mcp/how-it-works',
        'mcp/reference',
        'mcp/changelog',
      ],
    },
    {
      type: 'category',
      label: 'Smart Contract',
      items: [
        'contract/overview',
        'contract/delegate-key-management',
        'contract/ownership-and-permissions',
      ],
    },
    {
      type: 'category',
      label: 'Indexer',
      items: [
        'indexer/purpose',
        'indexer/onchain-events',
        'indexer/database-sync',
      ],
    },
    {
      type: 'category',
      label: 'OpenClaw Plugin',
      items: [
        'openclaw/overview',
        'openclaw/quick-start',
        'openclaw/how-it-works',
        'openclaw/reference',
        'openclaw/changelog',
      ],
    },
    {
      type: 'category',
      label: 'Reference',
      items: [
        'reference/configuration',
        'reference/environment-variables',
      ],
    },
    {
      type: 'category',
      label: 'Contributing',
      items: [
        'contributing/run-docs-locally',
        'contributing/run-repo-locally',
        'contributing/docs-workflow',
      ],
    },
  ],
};

export default sidebars;
```

**Note:** Some files exist in the repo but are NOT in `docs.json` navigation (e.g., `architecture/permanent-registry-design.md`, `security/health-check-unsigned.md`, several SDK files like `advanced-usage.md`, `ai-integration.md`, `examples.md`, `example-map.md`, `research-app-example.md`, `relayer/benchmark-ci-setup.md`, `examples/example-apps.md`). These files will still be accessible by direct URL but won't appear in the sidebar, matching the behavior of the Mintlify site.

### 4. Docusaurus config changes — `docusaurus.config.js`

#### a. Add multi-instance docs plugin

Add to the `plugins` array:

```js
[
  '@docusaurus/plugin-content-docs',
  {
    id: 'walrus-memory',
    path: '../walrus-memory-content',
    routeBasePath: 'walrus-memory',
    sidebarPath: './sidebarsWalrusMemory.js',
    editUrl: 'https://github.com/MystenLabs/MemWal/tree/dev/docs/',
    remarkPlugins: [remarkMath],
    rehypePlugins: [rehypeKatex],
  },
],
```

**What's excluded vs the main docs instance:**
- No `remarkGlossary` — Walrus Memory docs don't use the Walrus glossary
- No `ImportContent` — MemWal docs don't use this component

#### b. Add navbar item

Add to `themeConfig.navbar.items`:

```js
{
  type: 'docSidebar',
  docsPluginId: 'walrus-memory',
  sidebarId: 'walrusMemorySidebar',
  label: 'Walrus Memory',
  position: 'right',
},
```

Placed after "Example Apps" and before "Blog".

#### c. MDX component scope for the second plugin instance

The classic preset's MDX component overrides in `src/theme/MDXContent/index.js` apply globally to ALL docs instances — both the main one and additional plugin instances. This is because `MDXContent` is a theme component that wraps all MDX rendering, not a per-plugin setting.

**Verification needed:** Confirm during implementation that `Cards`, `Card`, `Tabs`, and `TabItem` are available in the Walrus Memory docs pages. If not, we'll need to add explicit MDX component registration via the plugin's `mdxComponentScope` option or add imports to a wrapper.

#### d. Webpack alias for content path

Extend the `stepHeadingLoader` or `docsAliasPlugin` to add:

```js
'@walrus-memory': path.resolve(__dirname, '../walrus-memory-content/')
```

This is only needed if any build script references the content path. May not be necessary.

### 5. Package.json changes

```diff
- "prestart": "node src/scripts/generate-import-context.js && node src/scripts/copy-yaml-files.js",
+ "prestart": "node src/scripts/fetch-walrus-memory-docs.js && node src/scripts/transform-walrus-memory-docs.js && node src/scripts/generate-import-context.js && node src/scripts/copy-yaml-files.js",

- "prebuild": "node src/scripts/generate-import-context.js && node src/scripts/copy-yaml-files.js",
+ "prebuild": "node src/scripts/fetch-walrus-memory-docs.js --force && node src/scripts/transform-walrus-memory-docs.js && node src/scripts/generate-import-context.js && node src/scripts/copy-yaml-files.js",
```

**Difference:** `prebuild` passes `--force` to always fetch fresh content for CI builds. `prestart` uses the cache (skip if fresh) for fast local dev iteration.

### 6. Gitignore

Add to `docs/site/.gitignore`:

```
/walrus-memory-content
```

Wait — the content is at `docs/walrus-memory-content/`, not `docs/site/walrus-memory-content/`. So add to the repo root `.gitignore`:

```
docs/walrus-memory-content/
```

### 7. Impact on existing build scripts

| Script | Impact | Action needed |
|--------|--------|---------------|
| `generate-import-context.js` | Scans MDX for `<ImportContent>` tags. Walrus Memory docs don't use this. | Ensure it only scans `../content`, not `../walrus-memory-content`. **Check the script's glob pattern.** |
| `copy-yaml-files.js` | Converts YAML from `docs/data/`. | No impact — different source directory. |
| `inline-imports.js` | Processes inline imports. | Same — check if it scans broadly or only `../content`. |
| `copy-markdown-files.js` | Exports markdown to `static/markdown/`. | **Decision:** Should Walrus Memory docs be included in the markdown export? Probably yes, in a separate subdirectory `static/markdown/walrus-memory/`. Requires a minor modification. |
| `generate-llmstxt.mjs` | Generates `llms.txt` from `static/markdown/`. | If markdown export includes Walrus Memory, this will automatically pick them up. May need section labeling. |
| `generate-routes.js` | Post-build route mapping. | Automatically picks up all built HTML. No change needed. |

**Recommendation:** For the initial implementation, do NOT include Walrus Memory docs in the markdown export or `llms.txt`. This keeps the scope small. It can be added later by extending `copy-markdown-files.js`.

---

## Files to create

| # | File | Purpose |
|---|------|---------|
| 1 | `docs/site/src/scripts/fetch-walrus-memory-docs.js` | Sparse-clone MemWal docs from GitHub |
| 2 | `docs/site/src/scripts/transform-walrus-memory-docs.js` | Rewrite Mintlify syntax, links, and branding |
| 3 | `docs/site/sidebarsWalrusMemory.js` | Sidebar navigation for Walrus Memory docs |

## Files to modify

| # | File | Change |
|---|------|--------|
| 4 | `docs/site/docusaurus.config.js` | Add `plugin-content-docs` instance + navbar item |
| 5 | `docs/site/package.json` | Add fetch + transform to `prestart`/`prebuild` |
| 6 | `docs/site/.gitignore` or root `.gitignore` | Ignore `docs/walrus-memory-content/` |

## Files NOT modified

- `src/theme/MDXContent/index.js` — Already registers `Cards`, `Card`, `Tabs`, `TabItem` globally
- `sidebars.js` — Existing sidebars unchanged
- `src/shared/components/` — No new components needed
- MemWal repo — Nothing changes there

---

## Risks and mitigations

| Risk | Likelihood | Mitigation |
|------|-----------|------------|
| Mintlify component transform misses edge cases | Medium | Regex-based transforms can be fragile. Write targeted tests for each component pattern. Run build and visually inspect. |
| Walrus `<Card>` props don't match MemWal usage | Low | Walrus `Card` takes `title`, `href`, `className`, `children`. MemWal uses `title`, `icon`, `href`, `children`. Only `icon` is lost — acceptable since Walrus Card doesn't render icons. |
| `<Steps>`/`<Step>` → `<Cards type="steps">` mismatch | Medium | The Walrus steps-card expects `<Card>` children with titles. MemWal `<Step>` content may not have explicit titles. May need to extract the first heading or sentence as the card title. **Verify during implementation.** |
| Internal links have edge cases | Low | Build with `onBrokenLinks: 'throw'` catches any broken links. |
| MDXContent scope doesn't cover second plugin | Low | Docusaurus theme components apply globally. But if issues arise, add an explicit `mdxComponentsImport` to the plugin config. |
| Network dependency at build time | Medium | Fetch script exits 0 on failure for local dev. For CI, network is reliable. Could also vendor the content periodically. |
| `generate-import-context.js` scans too broadly | Low | Check the glob pattern; restrict to `../content` if needed. |

---

## Open questions for review

1. **Steps component:** Should `<Steps>/<Step>` map to `<Cards type="steps">/<Card>` (existing), or would a simpler `<ol>` with styled `<li>` elements be more appropriate? The existing steps-card component is designed for navigation cards with links, which may not fit instructional step-by-step content.

2. **Markdown export:** Should Walrus Memory docs be included in `static/markdown/` and `llms.txt` in the initial implementation, or deferred?

3. **Search integration:** The Walrus site uses Algolia search. Should Walrus Memory pages be indexed? This may require Algolia crawler configuration changes outside this repo.

4. **Kapa.ai widget:** The AI chatbot widget is configured for "Walrus Knowledge." Should it also cover Walrus Memory content? This is a configuration change on the Kapa.ai side.

5. **Redirects:** Should we add redirects from `/memwal/*` → `/walrus-memory/*` in case anyone has bookmarked the old Mintlify site paths? (Depends on whether the Mintlify site URL structure matched.)

6. **`editUrl`:** The `editUrl` points to `https://github.com/MystenLabs/MemWal/tree/dev/docs/`. This creates "Edit this page" links pointing to the MemWal repo. Confirm this is desired behavior.

7. **Files not in `docs.json`:** ~15 files exist in the MemWal docs directory but aren't in the navigation (e.g., `architecture/permanent-registry-design.md`, `sdk/advanced-usage.md`, `sdk/examples.md`). Should these be included in the sidebar, excluded from the build entirely, or left accessible by direct URL only?
