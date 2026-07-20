# Walrus Ecosystem Apps — Information Architecture

## Purpose

Define how first-party application documentation — starting with Walrus Memory and
scaling to additional apps in the future — should be organized within the Walrus docs
site (`docs.wal.app`).

This document replaces the previous Walrus Memory-only IA proposal. The core question
is no longer "where does Walrus Memory go?" but "what pattern do we establish now that
holds up when there are five first-party apps?"

---

## Current state

### Navbar and content structure

```
[Data Storage] [Walrus Sites] [Service Providers] [Example Apps] [Blog]
      │              │               │                  │
      ▼              ▼               ▼                  ▼
  docsSidebar    sitesSidebar   operatorSidebar   examplesSidebar
   ~31 pages      ~25 pages       ~17 pages          ~8 pages
```

All four sidebars are served from a **single docs plugin instance** pointing at
`docs/content/`. Walrus Sites lives in `content/sites/`, Service Providers in
`content/operator-guide/`, etc. They share one URL namespace (root `/`).

### What this means

Walrus Sites is not technically a separate docs instance — it's a separate sidebar
within the same plugin. This works because Sites content lives in the same repo and
same content directory. External content from other repos (like Walrus Memory) can't
use this pattern; it needs a separate plugin instance.

---

## The scalability problem

If each first-party app gets its own top-level navbar item:

```
Today (5 items):
[Data Storage] [Walrus Sites] [Service Providers] [Example Apps] [Blog]

With Walrus Memory (6 items):
[Data Storage] [Walrus Sites] [Walrus Memory] [Service Providers] [Example Apps] [Blog]

With 3 more apps (9 items):
[Data Storage] [Walrus Sites] [Walrus Memory] [App X] [App Y] [Service Providers] [Example Apps] [Blog]
```

At 7+ navbar items the navigation breaks down — it's visually overwhelming, especially
on mobile, and it implies equal weight for items that have very different sizes and
audiences.

---

## Proposed pattern: "Apps" navbar dropdown

### High-level structure

```
[Data Storage] [Apps ▾] [Service Providers] [Example Apps] [Blog]
                  │
                  ├── Walrus Sites        → /sites/...        (existing sidebar)
                  ├── Walrus Memory       → /walrus-memory/...  (new plugin instance)
                  ├── Future App A        → /app-a/...          (future plugin instance)
                  └── Future App B        → /app-b/...          (future plugin instance)
```

**One dropdown replaces N navbar items.** Each app inside the dropdown gets its own
sidebar. Adding a new app means: add a docs plugin instance, add a sidebar file, add
a dropdown entry. The navbar doesn't grow.

### Why a dropdown, not a hub page

A hub page (`/apps/` with cards linking to each app) forces an extra click before
reaching content. A dropdown gives direct access to any app from any page on the site.
The hub page can still exist as a landing page for the dropdown, but it's not the
primary navigation mechanism.

### Migration path for Walrus Sites

Walrus Sites currently lives in the navbar as a top-level item. The migration to
the dropdown should be handled in two phases:

**Phase 1 (now, with Walrus Memory launch):**
- Add the "Apps" dropdown containing Walrus Memory
- Keep Walrus Sites as a top-level navbar item (no disruption)
- The dropdown has one child, which is fine — it signals the pattern

```
[Data Storage] [Walrus Sites] [Apps ▾] [Service Providers] [Example Apps] [Blog]
                                  │
                                  └── Walrus Memory
```

**Phase 2 (when a 2nd app launches, or on a natural cadence):**
- Move Walrus Sites into the dropdown
- Add redirects from any changed URLs (though Sites URLs don't need to change
  if the sidebar just moves to the dropdown — the content path is unchanged)

```
[Data Storage] [Apps ▾] [Service Providers] [Example Apps] [Blog]
                  │
                  ├── Walrus Sites
                  ├── Walrus Memory
                  └── New App
```

This avoids a premature reorganization while establishing the pattern now.

### Docusaurus implementation

Docusaurus navbar supports dropdowns natively:

```js
{
  type: 'dropdown',
  label: 'Apps',
  position: 'right',
  items: [
    {
      type: 'docSidebar',
      docsPluginId: 'walrus-memory',
      sidebarId: 'walrusMemorySidebar',
      label: 'Walrus Memory',
    },
    // Future apps go here
  ],
}
```

When Phase 2 arrives, the existing Walrus Sites navbar item moves inside:

```js
{
  type: 'dropdown',
  label: 'Apps',
  position: 'right',
  items: [
    {
      type: 'docSidebar',
      sidebarId: 'sitesSidebar',
      label: 'Walrus Sites',
    },
    {
      type: 'docSidebar',
      docsPluginId: 'walrus-memory',
      sidebarId: 'walrusMemorySidebar',
      label: 'Walrus Memory',
    },
  ],
}
```

---

## URL strategy

### Flat namespaces (recommended)

Each app gets a top-level URL prefix:

```
/                        → Walrus core (Data Storage)
/sites/...               → Walrus Sites (existing, unchanged)
/walrus-memory/...       → Walrus Memory
/future-app/...          → Future App
/operator-guide/...      → Service Providers (existing, unchanged)
```

**Pros:**
- No URL changes for existing content
- Short, memorable URLs
- Each app's docs plugin instance maps cleanly to its routeBasePath

**Cons:**
- Top-level namespace gets crowded, but in practice there will be <10 apps

### Nested under `/apps/` (alternative)

```
/apps/walrus-sites/...
/apps/walrus-memory/...
/apps/future-app/...
```

**Cons that rule this out:**
- Breaking URL change for Walrus Sites (`/sites/` → `/apps/walrus-sites/`)
- Walrus Sites is part of the main docs plugin, not a separate instance, making
  the prefix change technically complex
- Longer URLs for no real benefit

**Recommendation:** Flat namespaces. Don't nest under `/apps/`.

---

## Per-app sidebar template

Every first-party app sidebar should follow a consistent structure so users build
familiarity across apps. Not every section is required — apps should include only
sections that have meaningful content.

### Template

```
{App Name} Sidebar
│
├── Getting Started                    ← REQUIRED: every app
│   ├── What is {App Name}              (overview, value prop, use cases)
│   ├── Quick Start                     (first success in <5 minutes)
│   └── [Choose Your Path]              (if multiple integration modes)
│
├── Concepts                           ← RECOMMENDED: if the app has domain concepts
│   ├── [Core concept 1]
│   ├── [Core concept 2]
│   └── [Architecture / How It Works]
│
├── [SDK / Client sections]            ← VARIES: one per SDK/language
│   ├── Quick Start
│   ├── [Usage patterns]
│   ├── API Reference
│   └── Changelog
│
├── [Integration sections]             ← VARIES: one per integration
│   ├── Overview
│   ├── Quick Start
│   └── Reference
│
├── [Infrastructure / Self-Hosting]    ← IF APPLICABLE: operator-facing content
│   ├── [Component overviews]
│   ├── [Deployment guides]
│   └── [Operational reference]
│
├── Reference                          ← RECOMMENDED: config, env vars, API tables
│   └── [reference pages]
│
└── Contributing                       ← OPTIONAL: if the app accepts contributions
    └── [contribution guides]
```

### Rules

1. **Getting Started is always first** and expanded by default. Every other section
   is collapsed.

2. **Max nesting depth: 2 levels** (category → page). No sub-sub-categories. If a
   section like "Integrations" has multiple children (MCP, OpenClaw), flatten them
   with prefixed titles rather than nesting:
   ```
   ├── Integrations
   │   ├── MCP Overview
   │   ├── MCP Quick Start
   │   ├── OpenClaw Overview
   │   └── OpenClaw Quick Start
   ```

3. **Categories should have 2–9 items.** If a category has 10+ items, split it. If
   it has only 1 item, fold it into the parent or promote it to standalone.

4. **Landing page:** Each app should designate a landing page that renders at
   `/{app-name}/`. This is typically the "What is {App Name}" page. It should
   contain a brief intro, key features, and navigation cards to major sections.

5. **Consistent terminology:** Use the same category labels across apps where the
   content type matches. "Getting Started" not "Get Started" or "Quickstart Guide."
   "Reference" not "API Docs" or "Configuration."

---

## Walrus Memory: applied template

Applying the template to Walrus Memory's 60+ pages:

```
walrusMemorySidebar
│
├── Getting Started                              ← expanded by default
│   ├── What is Walrus Memory                      landing page for /walrus-memory/
│   ├── Quick Start                                first memory in 60 seconds
│   ├── Choose Your Path                           decision tree (6 integration modes)
│   └── Example Apps                               4 demo app walkthroughs
│
├── Concepts & Architecture                      ← collapsed
│   ├── Memory Spaces                              namespace isolation model
│   ├── Ownership & Access                         delegates, Ed25519 keys
│   ├── Core Components                            system diagram, 6 components
│   ├── How Storage Works                          encryption → Walrus → vector index → recall
│   ├── Data Flow & Security Model                 trust boundaries, TEE option
│   └── Permanent Registry Design                  append-only rationale (currently orphan page)
│
├── TypeScript SDK                               ← collapsed
│   ├── Overview                                   SDK landscape, 3 entry points
│   ├── Quick Start                                install + first memory
│   ├── Walrus Memory Client                       default relayer-backed client
│   ├── Walrus Memory Manual Client                client-managed SEAL encryption
│   ├── withWalrusMemory (AI Middleware)            Vercel AI SDK integration
│   ├── Advanced Usage                             manual registration, analyze
│   ├── AI Integration                             recall-before-generate pattern
│   ├── Examples                                   code snippets
│   ├── API Reference                              method signatures, config shapes
│   └── Changelog
│
├── Python SDK                                   ← collapsed
│   ├── Quick Start                                install + first memory (async/sync)
│   ├── Walrus Memory Client                       default client
│   ├── Walrus Memory Manual Client                client-managed ops
│   ├── withWalrusMemory                           AI middleware
│   ├── API Reference
│   └── Changelog
│
├── Integrations                                 ← collapsed
│   ├── MCP Overview                               Model Context Protocol server
│   ├── MCP Quick Start                            setup for Cursor, Claude Desktop, etc.
│   ├── MCP How It Works                           6 tools, transports, auth flow
│   ├── MCP Reference                              tool signatures, config
│   ├── MCP Changelog
│   ├── OpenClaw Overview                          memory plugin for OpenClaw agents
│   ├── OpenClaw Quick Start                       auto-recall/capture setup
│   ├── OpenClaw How It Works                      multi-agent isolation, prompt injection protection
│   ├── OpenClaw Reference
│   └── OpenClaw Changelog
│
├── Infrastructure                               ← collapsed
│   ├── Relayer Overview                           Rust relayer architecture, trust model
│   ├── Public Relayer                             managed hosted option
│   ├── Self-Hosting a Relayer                     full deployment guide
│   ├── Nautilus TEE Deployment                    trusted execution environment
│   ├── Relayer Observability                      metrics, monitoring
│   ├── Relayer Versioning & Compatibility         version matrix
│   ├── Relayer API Reference                      endpoint signatures
│   ├── Relayer Benchmark CI                       CI perf testing (currently orphan)
│   ├── Smart Contract Overview                    Move contract on Sui, key objects
│   ├── Delegate Key Management                    onchain delegate operations
│   ├── Ownership & Permissions                    access control entry/view functions
│   ├── Indexer Purpose                            Sui event sync to PostgreSQL
│   ├── Indexer Onchain Events                     event types and handling
│   ├── Indexer Database Sync                      schema, polling, catch-up
│   └── Health Check Security                      unauthenticated endpoint rationale (currently orphan)
│
├── Reference                                    ← collapsed
│   ├── Configuration                              MemWalConfig, MemWalManualConfig shapes
│   └── Environment Variables                      relayer env var reference
│
└── Contributing                                 ← collapsed
    ├── Run Docs Locally                           local dev setup
    ├── Run Repo Locally                           full repo dev environment
    └── Docs Workflow                              PR and review process
```

**8 categories, ~60 pages, max depth 2.**

---

## Adding a future app: what it takes

When the next first-party app is ready, the steps are:

### 1. Content pipeline (from the technical design doc)

- Add or extend the fetch script to pull the new app's docs from its repo
- Add a transform script (or parameterize the existing one) for any
  framework-specific syntax conversion
- Add content to a gitignored directory: `docs/{app-name}-content/`

### 2. Docusaurus config

Add a new plugin instance:

```js
['@docusaurus/plugin-content-docs', {
  id: '{app-name}',
  path: '../{app-name}-content',
  routeBasePath: '{app-name}',
  sidebarPath: './sidebars{AppName}.js',
}],
```

### 3. Sidebar

Create `sidebars{AppName}.js` following the per-app template. Required sections:
Getting Started (with landing page). Other sections as the content demands.

### 4. Navbar

Add one entry to the "Apps" dropdown:

```js
{
  type: 'docSidebar',
  docsPluginId: '{app-name}',
  sidebarId: '{appName}Sidebar',
  label: '{App Display Name}',
},
```

### 5. Checklist

| Task | Notes |
|------|-------|
| Fetch script | Clone/sparse-checkout from source repo |
| Transform script | Convert framework-specific syntax if needed |
| Sidebar file | Follow template, flatten to 2 levels |
| Plugin instance in docusaurus.config.js | Set routeBasePath and sidebarPath |
| Dropdown entry in navbar | Add to "Apps" dropdown items |
| .gitignore | Add content directory |
| Landing page | Ensure "What is {App}" page exists and is set as default doc |
| Internal link rewriting | Prefix all internal links with `/{app-name}/` |
| Branding check | Any product name changes in prose |
| Search indexing | Verify Algolia crawl covers new paths |
| Kapa.ai | Add new paths to AI chatbot data sources |

---

## Cross-app concerns

### Consistent visual identity

All app landing pages should use a consistent layout. The existing Walrus `<Cards>`
component provides this out of the box. Each app's "What is {App}" page should:
- Lead with a 1–2 sentence value proposition
- Show feature highlights using `<Cards>` grid
- End with navigation cards to major sections
- Use the same card/admonition styling as the rest of the site

### Shared Example Apps sidebar

The existing "Example Apps" sidebar shows code examples for Walrus core. As apps
are added, two options:

**Option A:** Each app includes its own examples in its own sidebar (Walrus Memory
already has an "Example Apps" page under Getting Started). The main "Example Apps"
sidebar stays Walrus-core only.

**Option B:** The main "Example Apps" sidebar adds sections for each app:
```
examplesSidebar
├── Walrus Examples (existing)
├── Walrus Memory Examples
└── Future App Examples
```

**Recommendation:** Option A for now. Apps own their own examples. If there's demand
for a unified examples view later, Option B can be adopted without restructuring
any app's sidebar.

### Search and discoverability

- **Algolia:** Each new app's URL prefix needs to be crawlable. If the Algolia
  crawler is configured with explicit URL patterns, add the new prefix. If it
  crawls the full domain, no action needed.
- **Kapa.ai:** Add each app's URL prefix to the chatbot's data source list in the
  Kapa.ai dashboard.
- **llms.txt:** Extend the generation script to include each app's content. Each
  app gets its own section header in the generated file.
- **Sitemap:** Docusaurus generates sitemaps automatically per plugin instance.
  No manual action needed.

### Cross-linking between apps

Apps may reference each other (e.g., Walrus Memory docs reference Walrus blob
storage). Guidelines:

- Use **absolute paths** for cross-app links: `[Walrus blob storage](/getting-started)`
- Each app's transform script should NOT rewrite links that point to other apps'
  namespaces — only rewrite links internal to the app itself
- Cross-links from Walrus core to apps should be added editorially over time
  (e.g., "Store AI agent memory" card on the Walrus landing page linking to
  `/walrus-memory/`)

---

## Apps hub page (optional, deferred)

When there are 3+ apps, consider adding an `/apps/` hub page that showcases all
first-party apps with cards, descriptions, and links. This page would:

- Live as a standalone page (not in any docs plugin)
- Be linked from the "Apps" dropdown as the first item
- Show each app's name, one-line description, and link to its docs
- Optionally show status badges (GA, Beta, Preview)

Implementation: a React page at `docs/site/src/pages/apps.tsx` using the existing
`<Cards>` component.

This is not needed for the Walrus Memory launch but should be planned for when the
2nd or 3rd app is added.

---

## Decision log

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Navbar pattern | Dropdown ("Apps") | Scales to N apps without growing the navbar |
| Phase 1 navbar | Dropdown with Walrus Memory only; Sites stays top-level | Avoids disrupting existing nav before there's a reason to |
| Phase 2 trigger | When the 2nd app launches | Natural point to consolidate |
| URL structure | Flat (`/{app-name}/`) | No breaking changes, simple, short URLs |
| Internal sidebar template | 2-level max, Getting Started required, 8 categories max | Matches Walrus conventions, proven pattern |
| Per-app examples | Each app owns its examples in its sidebar | Simpler, no cross-sidebar coupling |
| Hub page | Deferred until 3+ apps | Not worth the effort for 1–2 apps |
| Walrus Memory sidebar structure | Option B from prior doc (restructured, flattened) | 8 categories, 2-level max, groups related content |

---

## Appendix: Walrus Memory content inventory

### By audience

| Audience | Pages | Content areas |
|----------|-------|---------------|
| App developer | ~30 | Getting started, concepts, TypeScript SDK, Python SDK, AI integration, MCP, OpenClaw, examples, config reference |
| Infra operator | ~13 | Relayer (overview, self-hosting, TEE, observability), contract, indexer, environment variables, security |
| Contributor | ~3 | Run docs locally, run repo locally, docs workflow |
| General | ~3 | What is Walrus Memory, quick start, choose your path |

### By content type

| Type | Pages | Examples |
|------|-------|----------|
| Concept / Overview | ~10 | What is Walrus Memory, memory spaces, ownership, core components |
| Tutorial / Getting started | ~8 | Quick start, SDK quick starts, first memory, example apps |
| How-to / Guide | ~10 | Self-hosting, TEE deployment, AI middleware integration, advanced usage |
| Reference | ~8 | API references (SDK, Python, Relayer), config, environment variables |
| Explanation / Architecture | ~8 | Data flow, security model, storage lifecycle, permanent registry design |
| Changelog | ~4 | SDK, Python SDK, MCP, OpenClaw changelogs |

### Orphan pages (not in current Mintlify navigation)

| File | Recommendation |
|------|----------------|
| `architecture/permanent-registry-design.md` | Include under Concepts & Architecture |
| `sdk/overview.md` | Include under TypeScript SDK |
| `sdk/usage.md` | Include as TypeScript SDK category index |
| `sdk/advanced-usage.md` | Include under TypeScript SDK |
| `sdk/ai-integration.md` | Include under TypeScript SDK |
| `sdk/examples.md` | Include under TypeScript SDK |
| `sdk/example-map.md` | Include under TypeScript SDK |
| `sdk/research-app-example.md` | Include under Getting Started → Example Apps or standalone |
| `python-sdk/usage.md` | Include as Python SDK category index |
| `relayer/benchmark-ci-setup.md` | Include under Infrastructure |
| `security/health-check-unsigned.md` | Include under Infrastructure |
| `examples/example-apps.md` | Include under Getting Started |

---

## Content gaps and future work

| Gap | Description | Priority |
|-----|-------------|----------|
| Apps hub page | Standalone `/apps/` page showcasing all first-party apps | Deferred (3+ apps) |
| Walrus Sites migration | Move Sites into the Apps dropdown | Phase 2 (2nd app launch) |
| Cross-linking from core | Add "Walrus Memory" references on Walrus landing page and relevant guides | Medium |
| Glossary integration | Add Walrus Memory terms (memory space, delegate key, namespace) to the Walrus glossary | Low |
| Changelog rendering | Verify `.mdx` changelog files render correctly after Mintlify → Docusaurus transform | Medium |
| Contributing docs | Update "Run Docs Locally" to reference Docusaurus build instead of Mintlify CLI | High |
| Search coverage | Verify Algolia + Kapa.ai index `/walrus-memory/` paths post-deploy | High |
