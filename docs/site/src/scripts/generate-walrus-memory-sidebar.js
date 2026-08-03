// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0
//
// Generates sidebarsWalrusMemory.js from the transformed walrus-memory-content
// directory. Explicit page lists control ordering; autoDirs auto-discover new
// pages so upstream additions appear without manual sidebar edits.
//
// To change sidebar structure, edit STRUCTURE below. To add label overrides
// or exclusions, edit LABELS or EXCLUDE. Then run: npm run gen:sidebar

const fs = require("fs");
const path = require("path");

const SITE_ROOT = path.resolve(__dirname, "../../");
const CONTENT_DIR = path.resolve(SITE_ROOT, "../walrus-memory-content");
const SIDEBAR_OUT = path.join(SITE_ROOT, "sidebarsWalrusMemory.js");

// ── Exclusions ──────────────────────────────────────────────────────────
// Pages that should never appear in the sidebar, even if they exist on disk.
const EXCLUDE = new Set([
  // Consolidated pages with no unique content
  "sdk/example-map",
  "sdk/research-app-example",
  // Mintlify group-root index pages (no standalone value in Docusaurus)
  "sdk/usage",
  "python-sdk/usage",
]);

function isExcluded(slug) {
  if (EXCLUDE.has(slug)) return true;
  if (slug.startsWith("contributing/")) return true;
  if (slug.endsWith("/changelog") || slug === "changelog") return true;
  return false;
}

// ── Label overrides ─────────────────────────────────────────────────────
// Custom display labels for pages whose filename doesn't produce a good title.
const LABELS = {
  "sdk/quick-start": "TypeScript SDK Quick Start",
  "sdk/ai-integration": "AI SDK Integration",
  "sdk/cookbook-multi-tenant": "Multi-Tenant Apps",
  "sdk/cloudflare-workers": "Cloudflare Workers",
  "python-sdk/quick-start": "Python SDK Quick Start",
  "openclaw/overview": "What Is OpenClaw",
  "openclaw/quick-start": "Install and Configure",
  "openclaw/how-it-works": "Architecture and Hooks",
  "sdk/api-reference": "TypeScript SDK API",
  "python-sdk/api-reference": "Python SDK API",
  "mcp/reference": "MCP Tools Reference",
  "openclaw/reference": "OpenClaw Plugin Reference",
  "relayer/api-reference": "Relayer HTTP API",
};

// ── Sidebar structure ───────────────────────────────────────────────────
// Each category defines:
//   label        – sidebar display name
//   collapsed    – default state (omit for true)
//   link         – doc id used as the category landing page
//   items        – ordered list of page slugs and/or subcategory objects
//   autoDirs     – directories to scan for new pages not listed in any
//                  category; discovered pages append after explicit items
//   autoExclude  – slugs in autoDirs that belong to a different category
//                  (prevents duplication without needing to list them here)

const STRUCTURE = [
  {
    label: "Get Started",
    collapsed: false,
    link: "getting-started/what-is-walrus-memory",
    items: [
      "getting-started/quick-start",
      "getting-started/choose-your-path",
      "examples/example-apps",
      {
        type: "link",
        label: "GitHub",
        href: "https://github.com/MystenLabs/MemWal",
      },
    ],
    autoDirs: ["getting-started", "examples"],
  },
  {
    label: "Core Concepts",
    items: [
      "fundamentals/concepts/memory-space",
      "fundamentals/concepts/ownership-and-access",
      "fundamentals/architecture/core-components",
      "fundamentals/architecture/how-storage-works",
      "fundamentals/architecture/data-flow-security-model",
    ],
    autoDirs: ["fundamentals"],
  },
  {
    label: "Manage Memories",
    items: [],
    autoDirs: ["guides"],
  },
  {
    label: "AI Tools (MCP)",
    link: "mcp/overview",
    items: [
      "mcp/claude-code",
      "mcp/claude-desktop",
      "mcp/cursor",
      "mcp/codex",
      "mcp/opencode",
      "mcp/antigravity",
    ],
    autoDirs: ["mcp"],
    autoExclude: ["mcp/reference"],
  },
  {
    label: "TypeScript SDK",
    link: "sdk/overview",
    items: [
      "sdk/quick-start",
      "sdk/usage/memwal",
      "sdk/usage/memwal-manual",
      "sdk/usage/with-memwal",
      "sdk/examples",
      "sdk/ai-integration",
      "sdk/cookbook-multi-tenant",
      "sdk/cloudflare-workers",
      "sdk/advanced-usage",
    ],
    autoDirs: ["sdk"],
    autoExclude: ["sdk/api-reference"],
  },
  {
    label: "Python SDK",
    items: [
      "python-sdk/quick-start",
      "python-sdk/colab",
      "python-sdk/usage/memwal",
      "python-sdk/usage/memwal-manual",
      "python-sdk/usage/with-memwal",
    ],
    autoDirs: ["python-sdk"],
    autoExclude: ["python-sdk/api-reference"],
  },
  {
    label: "OpenClaw Plugin",
    items: [
      "openclaw/overview",
      "openclaw/quick-start",
      "openclaw/how-it-works",
    ],
    autoDirs: ["openclaw"],
    autoExclude: ["openclaw/reference"],
  },
  {
    label: "Relayer",
    items: [
      "relayer/overview",
      "relayer/public-relayer",
      "relayer/self-hosting",
      "relayer/nautilus-tee",
      "relayer/observability",
      "relayer/versioning-and-compatibility",
      "relayer/benchmark-ci-setup",
      "relayer/runbook-gas-pool",
    ],
    autoDirs: ["relayer"],
    autoExclude: ["relayer/api-reference"],
  },
  {
    label: "Troubleshooting",
    items: [],
    autoDirs: ["troubleshooting"],
  },
  {
    label: "Reference",
    items: [
      "reference/configuration",
      "reference/environment-variables",
      "sdk/api-reference",
      "python-sdk/api-reference",
      "mcp/reference",
      "openclaw/reference",
      "relayer/api-reference",
      "security/health-check-unsigned",
      {
        label: "Smart Contract",
        items: [
          "contract/overview",
          "contract/delegate-key-management",
          "contract/ownership-and-permissions",
        ],
        autoDirs: ["contract"],
      },
      {
        label: "Indexer",
        items: [
          "indexer/purpose",
          "indexer/onchain-events",
          "indexer/database-sync",
        ],
        autoDirs: ["indexer"],
      },
      "architecture/permanent-registry-design",
    ],
    autoDirs: ["reference", "security", "architecture"],
  },
];

// ── Filesystem scanner ──────────────────────────────────────────────────

function scanPages(dir, base) {
  const slugs = new Set();
  if (!fs.existsSync(dir)) return slugs;

  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const relPath = base ? `${base}/${entry.name}` : entry.name;
    if (entry.isDirectory()) {
      for (const slug of scanPages(path.join(dir, entry.name), relPath)) {
        slugs.add(slug);
      }
    } else if (entry.name.endsWith(".md") || entry.name.endsWith(".mdx")) {
      slugs.add(relPath.replace(/\.(mdx?)$/, ""));
    }
  }
  return slugs;
}

// ── Collect all explicitly placed slugs ─────────────────────────────────

function collectPlacedSlugs(categories) {
  const placed = new Set();

  function walkItems(items) {
    for (const item of items) {
      if (typeof item === "string") {
        placed.add(item);
      } else if (item.items) {
        if (item.link) placed.add(item.link);
        walkItems(item.items);
        if (item.autoExclude) {
          for (const slug of item.autoExclude) placed.add(slug);
        }
      }
    }
  }

  for (const cat of categories) {
    if (cat.link) placed.add(cat.link);
    if (cat.items) walkItems(cat.items);
    if (cat.autoExclude) {
      for (const slug of cat.autoExclude) placed.add(slug);
    }
  }
  return placed;
}

// ── Build a single doc item (string or labelled object) ─────────────────

function makeDocItem(slug) {
  const label = LABELS[slug];
  if (label) {
    return { type: "doc", id: slug, label };
  }
  return slug;
}

// ── Build a category recursively ────────────────────────────────────────

function buildCategory(def, availableSlugs, placedSlugs) {
  const cat = {
    type: "category",
    label: def.label,
    collapsed: def.collapsed !== false,
  };

  if (def.link) {
    cat.link = { type: "doc", id: def.link };
  }

  const items = [];

  // 1. Process explicit items in order
  for (const item of def.items || []) {
    if (typeof item === "string") {
      if (availableSlugs.has(item)) {
        items.push(makeDocItem(item));
      } else {
        console.warn(`  \u26a0\ufe0f  Page listed but not found on disk: ${item}`);
      }
    } else if (item.type === "link") {
      // External link item (e.g. the MemWal GitHub repo) — pass through as-is
      items.push(item);
    } else if (item.items) {
      // Subcategory — recurse
      const sub = buildCategory(item, availableSlugs, placedSlugs);
      if (sub.items.length > 0) {
        items.push(sub);
      }
    }
  }

  // 2. Auto-discover new pages from autoDirs
  if (def.autoDirs) {
    const autoExclude = new Set(def.autoExclude || []);
    const discovered = [];

    for (const dir of def.autoDirs) {
      for (const slug of availableSlugs) {
        if (!slug.startsWith(dir + "/")) continue;
        if (placedSlugs.has(slug)) continue;
        if (isExcluded(slug)) continue;
        if (autoExclude.has(slug)) continue;
        discovered.push(slug);
        placedSlugs.add(slug);
      }
    }

    if (discovered.length > 0) {
      discovered.sort();
      console.log(
        `  + Auto-discovered in ${def.label}: ${discovered.join(", ")}`,
      );
      for (const slug of discovered) {
        items.push(makeDocItem(slug));
      }
    }
  }

  cat.items = items;
  return cat;
}

// ── Format the sidebar as a JS module ───────────────────────────────────

function formatSidebar(sidebar) {
  const json = JSON.stringify({ walrusMemorySidebar: sidebar }, null, 2);
  return [
    "// Copyright (c) Walrus Foundation",
    "// SPDX-License-Identifier: Apache-2.0",
    "//",
    "// AUTO-GENERATED by generate-walrus-memory-sidebar.js \u2014 do not edit manually.",
    "// To change sidebar structure, edit the STRUCTURE config in that script.",
    "// To add label overrides, edit the LABELS map in that script.",
    "",
    "// @ts-check",
    "",
    "/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */",
    `const sidebars = ${json};`,
    "",
    "export default sidebars;",
    "",
  ].join("\n");
}

// ── Main ────────────────────────────────────────────────────────────────

function main() {
  if (!fs.existsSync(CONTENT_DIR)) {
    console.warn(
      "\u26a0\ufe0f  walrus-memory: content dir not found. Skipping sidebar generation.",
    );
    return;
  }

  const availableSlugs = scanPages(CONTENT_DIR);
  console.log(`walrus-memory sidebar: ${availableSlugs.size} pages on disk`);

  // Collect all explicitly referenced slugs so auto-discovery skips them
  const placedSlugs = collectPlacedSlugs(STRUCTURE);

  // Build sidebar categories
  const sidebar = [];
  for (const def of STRUCTURE) {
    const cat = buildCategory(def, availableSlugs, placedSlugs);
    if (cat.items.length === 0 && !cat.link) {
      console.log(`  - Skipping empty category: ${def.label}`);
      continue;
    }
    sidebar.push(cat);
  }

  // Warn about pages that exist on disk but aren't placed anywhere
  const unplaced = [];
  for (const slug of availableSlugs) {
    if (!placedSlugs.has(slug) && !isExcluded(slug)) {
      unplaced.push(slug);
    }
  }
  if (unplaced.length > 0) {
    console.warn(
      `\n\u26a0\ufe0f  Unplaced pages (add to STRUCTURE or EXCLUDE in generate-walrus-memory-sidebar.js):`,
    );
    for (const slug of unplaced.sort()) {
      console.warn(`     - ${slug}`);
    }
  }

  fs.writeFileSync(SIDEBAR_OUT, formatSidebar(sidebar));
  console.log(
    `\u2705 walrus-memory: generated sidebarsWalrusMemory.js (${sidebar.length} categories)\n`,
  );
}

main();
