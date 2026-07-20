// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0
//
// Generates the Walrus Memory sidebar dynamically from the upstream MemWal
// docs.json (Mintlify config) so the sidebar stays in sync when pages are
// added or removed upstream. Orphan pages (files that exist but are not
// listed in docs.json) are appended to the matching category.

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const CACHE_DIR = path.resolve(__dirname, ".cache-walrus-memory/docs");
const OUTPUT_DIR = path.resolve(__dirname, "../walrus-memory-content");

// Files renamed during transform (original slug → local slug)
const SLUG_RENAMES = {
  "getting-started/what-is-memwal": "getting-started/what-is-walrus-memory",
};

// Override tab labels where the Walrus site prefers a different name
const TAB_LABEL_OVERRIDES = {
  SDK: "TypeScript SDK",
};

// Pages removed during transform (consolidated into other pages)
const REMOVED_PAGES = new Set([
  "sdk/example-map",
  "sdk/research-app-example",
]);

function renameSlug(slug) {
  return SLUG_RENAMES[slug] || slug;
}

// --- Collect doc IDs from the output directory ---

function collectOutputDocIds(dir, prefix) {
  const ids = new Set();
  if (!fs.existsSync(dir)) return ids;
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const rel = prefix ? `${prefix}/${entry.name}` : entry.name;
    if (entry.isDirectory()) {
      for (const id of collectOutputDocIds(path.join(dir, entry.name), rel)) {
        ids.add(id);
      }
    } else if (/\.(mdx?)$/.test(entry.name)) {
      ids.add(rel.replace(/\.(mdx?)$/, ""));
    }
  }
  return ids;
}

// --- Extract all placed page IDs from a Mintlify pages array ---

function extractPageIds(pages, placed) {
  for (const page of pages) {
    if (typeof page === "string") {
      placed.add(renameSlug(page));
    } else if (page.pages) {
      if (page.root) placed.add(renameSlug(page.root));
      extractPageIds(page.pages, placed);
    }
  }
}

// --- Convert Mintlify structures to Docusaurus sidebar items ---

function convertPages(pages) {
  const items = [];
  for (const page of pages) {
    if (typeof page === "string") {
      const slug = renameSlug(page);
      if (REMOVED_PAGES.has(slug)) continue;
      items.push(slug);
    } else if (page.pages) {
      const category = {
        type: "category",
        label: page.group,
        collapsed: true,
        items: convertPages(page.pages),
      };
      if (page.root) {
        category.link = { type: "doc", id: renameSlug(page.root) };
      }
      items.push(category);
    }
  }
  return items;
}

function convertTab(tab, isFirst) {
  const groups = tab.groups || [];
  if (groups.length === 0) return null;

  const label = TAB_LABEL_OVERRIDES[tab.tab] || tab.tab;

  if (groups.length === 1) {
    const group = groups[0];
    const items = convertPages(group.pages);
    const category = {
      type: "category",
      label,
      collapsed: !isFirst,
      items,
    };
    // For the first tab, use the first page as the category link
    if (isFirst && items.length > 0 && typeof items[0] === "string") {
      category.link = { type: "doc", id: items[0] };
      category.items = items.slice(1);
    }
    return category;
  }

  // Multiple groups → nested categories
  const items = groups.map((g) => ({
    type: "category",
    label: g.group,
    collapsed: true,
    items: convertPages(g.pages),
  }));

  return { type: "category", label, collapsed: !isFirst, items };
}

// --- Orphan detection: pages that exist but are not in docs.json ---

function appendOrphans(sidebar, placedIds) {
  const allIds = collectOutputDocIds(OUTPUT_DIR, "");
  const orphans = [...allIds].filter((id) => !placedIds.has(id)).sort();
  if (orphans.length === 0) return;

  // Build prefix → sidebar-category-index map from existing items
  const prefixToIdx = {};
  for (let i = 0; i < sidebar.length; i++) {
    const item = sidebar[i];
    if (item.type !== "category") continue;
    const prefixes = new Set();
    (function walk(items) {
      for (const sub of items) {
        const id =
          typeof sub === "string" ? sub : sub.id || sub.link?.id || "";
        if (id.includes("/")) prefixes.add(id.split("/")[0]);
        if (sub.items) walk(sub.items);
      }
    })(item.items || []);
    if (item.link?.id?.includes("/"))
      prefixes.add(item.link.id.split("/")[0]);
    for (const p of prefixes) {
      if (prefixToIdx[p] === undefined) prefixToIdx[p] = i;
    }
  }

  let placed = 0;
  for (const id of orphans) {
    const prefix = id.includes("/") ? id.split("/")[0] : null;
    if (prefix && prefixToIdx[prefix] !== undefined) {
      sidebar[prefixToIdx[prefix]].items.push(id);
      placed++;
    }
  }

  if (placed > 0) {
    console.log(
      `ℹ️  walrus-memory sidebar: appended ${placed} page(s) not in docs.json to matching categories`,
    );
  }
}

// --- Main ---

function generateSidebar() {
  const docsJsonPath = path.join(CACHE_DIR, "docs.json");
  if (!fs.existsSync(docsJsonPath)) {
    console.warn(
      "⚠️  walrus-memory: docs.json not found, using minimal sidebar",
    );
    return [{ type: "doc", id: "index", label: "Walrus Memory" }];
  }

  const docsJson = JSON.parse(fs.readFileSync(docsJsonPath, "utf8"));
  const tabs = docsJson.navigation?.tabs || [];

  // Track which IDs are placed by docs.json
  const placedIds = new Set(["index"]);
  for (const tab of tabs) {
    for (const group of tab.groups || []) {
      extractPageIds(group.pages, placedIds);
    }
  }

  const sidebar = [{ type: "doc", id: "index", label: "Walrus Memory" }];
  for (let i = 0; i < tabs.length; i++) {
    const cat = convertTab(tabs[i], i === 0);
    if (cat) sidebar.push(cat);
  }

  appendOrphans(sidebar, placedIds);

  return sidebar;
}

/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  walrusMemorySidebar: generateSidebar(),
};

export default sidebars;
