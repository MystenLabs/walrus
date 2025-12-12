// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

import fs from "node:fs";
import path from "node:path";

const DOCS_ROOT = process.argv[2] ?? "docs";        // folder containing your .mdx tree
const OUT_DIR   = process.argv[3] ?? "static";      // where to write legacy .html files
const NEW_BASE  = process.argv[4] ?? "/docs";       // new site base path
const OLD_BASE  = process.argv[5] ?? "/";           // old urls were at site root (e.g. /walrus-sites/x.html)

// If you only want redirects for certain top-level dirs, add them here.
// Leaving empty means "all".
const TOP_LEVEL_ALLOWLIST = new Set([
  "walrus-sites",
  "usage",
  "design",
  "dev-guide",
  "legal",
  "operator-guide",
  // add others if needed
]);

function walk(dir) {
  const out = [];
  for (const ent of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, ent.name);
    if (ent.isDirectory()) out.push(...walk(full));
    else out.push(full);
  }
  return out;
}

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function htmlRedirect(to) {
return `<!doctype html>
<meta charset="utf-8">
<title>Redirecting…</title>
<link rel="canonical" href="${to}">
<meta http-equiv="refresh" content="0; url=${to}">
<script>location.replace(${JSON.stringify(to)});</script>
<p>Redirecting to <a href="${to}">${to}</a></p>
`;
}

function posixNoExt(relFile) {
  // Convert Windows backslashes to URL slashes and drop extension
  const rel = relFile.split(path.sep).join("/");
  return rel.replace(/\.[^.]+$/, "");
}

const docsAbs = path.resolve(DOCS_ROOT);
const outAbs  = path.resolve(OUT_DIR);

if (!fs.existsSync(docsAbs)) {
  console.error(`Docs root not found: ${docsAbs}`);
  process.exit(1);
}

const files = walk(docsAbs).filter(f => f.endsWith(".mdx"));

// Filter to top-level folders you care about (optional)
const filtered = files.filter(f => {
  const rel = path.relative(docsAbs, f);
  const top = rel.split(path.sep)[0];
  // allow intro.mdx at root if you want: return true when top is "intro.mdx"
  if (top.endsWith(".mdx")) return true;
  return TOP_LEVEL_ALLOWLIST.size === 0 || TOP_LEVEL_ALLOWLIST.has(top);
});

let count = 0;

for (const file of filtered) {
  const rel = path.relative(docsAbs, file);            // e.g. walrus-sites/tutorial.mdx
  const noExt = posixNoExt(rel);                       // e.g. walrus-sites/tutorial

  // OLD url: /walrus-sites/tutorial.html  (same path + .html)
  const oldHtmlRel = `${noExt}.html`;                  // walrus-sites/tutorial.html

  // NEW url: /docs/walrus-sites/tutorial
  const to = `${NEW_BASE}/${noExt}`.replace(/\/+/g, "/");

  // Write to OUT_DIR preserving dirs
  const outPath = path.join(outAbs, oldHtmlRel);
  ensureDir(path.dirname(outPath));

  fs.writeFileSync(outPath, htmlRedirect(to), "utf8");
  count++;
}

console.log(`Generated ${count} redirect HTML files into: ${outAbs}`);
console.log(`Example: ${path.join(outAbs, "walrus-sites", "tutorial.html")} -> ${NEW_BASE}/walrus-sites/tutorial`);
