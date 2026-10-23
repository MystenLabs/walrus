# Walrus Documentation

Walrus developer documentation, built with [Docusaurus](https://docusaurus.io/) and deployed to [docs.wal.app](https://docs.wal.app/) by Vercel.

## Quick start

```bash
cd site
pnpm install   # install dependencies (first time only)
pnpm start     # local dev server with hot reload
```

To run a production build (checks broken links and anchors):

```bash
pnpm build
pnpm serve     # preview the production build locally
```

Requires Node.js 18+.

## Directory structure

```
docs/
├── blog/                  # Blog posts (MDX)
├── content/               # Main documentation pages (MDX)
│   ├── getting-started/   # Onboarding and quickstart guides
│   ├── system-overview/   # Architecture and design
│   ├── walrus-client/     # CLI and client usage
│   ├── typescript-sdk/    # TypeScript SDK reference
│   ├── http-api/          # HTTP API reference
│   ├── operator-guide/    # Storage node operator docs
│   ├── sites/             # Walrus Sites documentation
│   ├── examples/          # Example code and tutorials
│   ├── snippets/          # Reusable MDX snippets
│   └── ...
├── data/                  # YAML data files (portals list, etc.)
├── editorial/             # Release notes source files
├── examples/              # Standalone example projects (Move, JS, Python, shell)
├── fundamentals/          # Supplementary architecture content
├── walrus-memory-content/ # Walrus Memory product docs
└── site/                  # Docusaurus project root
    ├── docusaurus.config.js
    ├── sidebars.js
    ├── vercel.json        # Vercel config (redirects, rewrites, headers)
    ├── src/
    │   ├── components/    # Site-specific React components
    │   ├── pages/         # Standalone pages
    │   ├── plugins/       # Docusaurus plugins
    │   ├── scripts/       # Build-time generation scripts
    │   ├── shared/        # Shared Docusaurus components (git subtree)
    │   └── theme/         # Theme overrides (swizzled components)
    └── static/            # Static assets (images, fonts, etc.)
```

## Writing documentation

### Style guide

All documentation must follow the [Sui Documentation Style Guide](https://docs.sui.io/style-guide). Key rules:

- **Language:** US English, second person ("you"), present tense, active voice.
- **Page titles:** Title case. **Headings:** Sentence case.
- **Latin abbreviations:** Do not use `e.g.`, `i.e.`, or `etc.` Write "for example", "that is", or rephrase.
- **Word choices:** Use "might" (not "may"), "through" (not "via"), "because" (not causal "since").
- **Punctuation:** Oxford commas are mandatory. No exclamation marks.
- **Admonitions:** Use `:::info`, `:::tip`, `:::warning` (Docusaurus syntax).
- **Code blocks:** Use triple backticks with a language identifier.
- **Links:** Use relative paths for internal links.

### Tabs

Use `<Tabs>` and `<TabItem>` for Testnet/Mainnet differences or prerequisite
checklists. Wrap in `<div className="outlined-tabs">` for outlined styling.
These components are globally available and do not need to be imported.

### Snippets

Reusable content fragments live in `content/snippets/`. Import them into pages with the `import` statement for MDX.

## Build pipeline

The full build runs these steps in sequence:

1. **`prestart` / `prebuild`** — Generates skills data, prepares Walrus Memory
   content, processes imports, copies YAML files, and generates release notes
   from `editorial/` source files.
2. **`build:prep`** — Inlines imports, copies markdown files, and generates `llms.txt` / `llms-full.txt` for LLM consumption.
3. **`docusaurus build`** — Compiles the Docusaurus site.

## Redirects and custom headers

Redirects, rewrites, and custom HTTP headers are configured in `site/vercel.json`. Do not use
client-side JavaScript redirects.

## Shared components

This project uses shared TSX/JSX components from
[ML-Shared-Docusaurus](https://github.com/MystenLabs/ML-Shared-Docusaurus),
pulled in as a git subtree at `site/src/shared/`. These components are also
used by the Sui, SuiNS, and Seal documentation sites.

To update shared components:

```bash
git subtree pull --prefix=docs/site/src/shared https://github.com/MystenLabs/ML-Shared-Docusaurus.git master --squash
```

## Deployment

Vercel deploys the site through its GitHub integration: every push to `main` deploys
[docs.wal.app](https://docs.wal.app/), and every pull request gets a preview deployment. The
`Build and publish docs` workflow (`.github/workflows/publish-docs.yaml`) only checks that the site
builds; it also runs daily and when upstream Walrus Memory or Oyster docs change.
