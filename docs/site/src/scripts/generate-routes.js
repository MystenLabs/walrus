// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

const fs = require("fs");
const path = require("path");

const buildDir = path.join(__dirname, "../../build");
const manualPath = path.join(__dirname, "../../ws-resources.manual.json");
const outputPath = path.join(__dirname, "../../ws-resources.json");

/**
 * Checks if an HTML file is a Docusaurus meta-refresh redirect stub.
 */
function isRedirectStub(filePath) {
  const content = fs.readFileSync(filePath, "utf8").slice(0, 200);
  return content.includes('http-equiv="refresh"');
}

/**
 * Recursively finds all content HTML files in the build directory,
 * skipping Docusaurus redirect stubs (.html.html, .htm/index.html).
 */
function findHtmlFiles(dir, baseDir = dir) {
  const results = [];
  const entries = fs.readdirSync(dir, { withFileTypes: true });

  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      results.push(...findHtmlFiles(fullPath, baseDir));
    } else if (entry.name.endsWith(".html")) {
      if (isRedirectStub(fullPath)) {
        continue;
      }
      results.push("/" + path.relative(baseDir, fullPath));
    }
  }

  return results;
}

/**
 * Generates clean URL route mappings from HTML files in the build directory.
 *
 * For each `path/to/page.html` (not index.html): maps `/path/to/page` -> `/path/to/page.html`
 * For each `path/to/dir/index.html`: maps `/path/to/dir` -> `/path/to/dir/index.html`
 */
function generateRoutes(htmlFiles) {
  const routes = {};

  for (const htmlPath of htmlFiles) {
    const basename = path.basename(htmlPath, ".html");
    const dir = path.dirname(htmlPath);

    if (basename === "index") {
      // /path/to/dir/index.html -> /path/to/dir
      const cleanUrl = dir === "/" ? "/" : dir;
      if (cleanUrl !== "/") {
        routes[cleanUrl] = htmlPath;
      }
    } else {
      // /path/to/page.html -> /path/to/page
      const cleanUrl = dir === "/" ? `/${basename}` : `${dir}/${basename}`;
      routes[cleanUrl] = htmlPath;
    }
  }

  return routes;
}

/**
 * Recursively finds all markdown files in a directory.
 */
function findMarkdownFiles(dir, baseDir = dir) {
  const results = [];
  if (!fs.existsSync(dir)) return results;
  const entries = fs.readdirSync(dir, { withFileTypes: true });

  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      results.push(...findMarkdownFiles(fullPath, baseDir));
    } else if (entry.name.endsWith(".md")) {
      // Skip empty files (e.g., drafts)
      if (fs.statSync(fullPath).size === 0) continue;
      results.push("/" + path.relative(baseDir, fullPath));
    }
  }

  return results;
}

/**
 * Generates /docs/*.md -> /markdown/*.md route mappings
 * for all markdown files in the build/markdown directory.
 *
 * Headers for markdown files are set via a glob expression in
 * ws-resources.manual.json, so no per-file headers are needed here.
 */
function generateMarkdownRoutes(markdownDir) {
  const routes = {};
  const mdFiles = findMarkdownFiles(markdownDir);

  for (const mdPath of mdFiles) {
    // mdPath is like /examples/awesome-walrus.md
    const routeKey = "/docs" + mdPath;
    const targetPath = "/markdown" + mdPath;

    routes[routeKey] = targetPath;

    // For index.md files, also add a short-form route:
    // /docs/sites/index.md -> also accessible as /docs/sites.md
    const basename = path.basename(mdPath, ".md");
    if (basename === "index") {
      const dir = path.dirname(mdPath);
      if (dir !== "/") {
        routes["/docs" + dir + ".md"] = targetPath;
      }
    }
  }

  return routes;
}

console.log("🔗 Generating clean URL routes...");

if (!fs.existsSync(buildDir)) {
  console.error("❌ Build directory not found. Run 'docusaurus build' first.");
  process.exit(1);
}

if (!fs.existsSync(manualPath)) {
  console.error("❌ ws-resources.manual.json not found.");
  process.exit(1);
}

const wsResources = JSON.parse(fs.readFileSync(manualPath, "utf8"));

// Extract existing manual routes (these take precedence)
const manualRoutes = wsResources.routes || {};

// Generate clean URL routes from HTML build output
const htmlFiles = findHtmlFiles(buildDir);
const autoRoutes = generateRoutes(htmlFiles);

// Generate /docs/*.md -> /markdown/*.md routes for markdown exports
const markdownDir = path.join(buildDir, "markdown");
const mdRoutes = generateMarkdownRoutes(markdownDir);

// Merge: manual routes override auto-generated ones
const mergedRoutes = { ...autoRoutes, ...mdRoutes, ...manualRoutes };

// Sort keys for readability
const sortedRoutes = {};
for (const key of Object.keys(mergedRoutes).sort()) {
  sortedRoutes[key] = mergedRoutes[key];
}

// Write merged output to ws-resources.json
wsResources.routes = sortedRoutes;
fs.writeFileSync(outputPath, JSON.stringify(wsResources, null, 2) + "\n", "utf8");

const autoCount = Object.keys(autoRoutes).length;
const mdCount = Object.keys(mdRoutes).length;
const manualCount = Object.keys(manualRoutes).length;
const totalCount = Object.keys(sortedRoutes).length;

console.log(`  Auto-generated: ${autoCount} clean URL routes from HTML files`);
console.log(`  Auto-generated: ${mdCount} markdown routes (/docs/*.md)`);
console.log(`  Manual/legacy:  ${manualCount} existing routes preserved`);
console.log(`  Total:          ${totalCount} routes written`);
console.log("✅ Routes generated successfully");
