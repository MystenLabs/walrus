// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0
//
// Prepares the Oyster docs cache from the committed upstream copy and
// fetches the OpenAPI spec from the live testnet endpoint.
//
// Usage:
//   node src/scripts/fetch-oyster-docs.js [--force]
//
// The Oyster docs source is checked into docs/oyster-upstream/ (copied
// from MystenLabs/oyster). This script symlinks/copies it into the
// .cache-oyster/ directory that the transform script expects, then
// fetches the OpenAPI spec from the live Scalar page.
//
// Pass --force to re-fetch the OpenAPI spec even if it is fresh.

const fs = require("fs");
const path = require("path");

const SITE_ROOT = path.resolve(__dirname, "../../");
const CACHE_DIR = path.join(SITE_ROOT, ".cache-oyster/docs/src");
const UPSTREAM_DIR = path.resolve(SITE_ROOT, "../oyster-upstream");
const STATIC_DIR = path.join(SITE_ROOT, "static/oyster");
const FRESHNESS_MINUTES = 10;
const OPENAPI_DOCS_URL = "https://oyster.testnet.mystenlabs.com/api/docs";

const force =
  process.argv.includes("--force") || process.env.FORCE_FETCH === "1";

function main() {
  // Copy the committed upstream docs into the cache location the
  // transform script reads from.
  if (!fs.existsSync(UPSTREAM_DIR)) {
    console.error(
      "❌ oyster: docs/oyster-upstream/ not found. Cannot proceed.",
    );
    process.exit(1);
  }

  if (fs.existsSync(CACHE_DIR)) {
    fs.rmSync(CACHE_DIR, { recursive: true });
  }
  fs.mkdirSync(CACHE_DIR, { recursive: true });
  copyDir(UPSTREAM_DIR, CACHE_DIR);
  console.log("✅ oyster: copied committed upstream docs into cache");

  // Fetch the OpenAPI spec from the live Scalar page
  fetchOpenApiSpec();
}

function copyDir(src, dest) {
  for (const entry of fs.readdirSync(src, { withFileTypes: true })) {
    const srcPath = path.join(src, entry.name);
    const destPath = path.join(dest, entry.name);
    if (entry.isDirectory()) {
      fs.mkdirSync(destPath, { recursive: true });
      copyDir(srcPath, destPath);
    } else {
      fs.copyFileSync(srcPath, destPath);
    }
  }
}

async function fetchOpenApiSpec() {
  const specPath = path.join(STATIC_DIR, "openapi.json");

  // Check freshness of existing spec
  if (!force) {
    try {
      const stat = fs.statSync(specPath);
      if (Date.now() - stat.mtimeMs < FRESHNESS_MINUTES * 60 * 1000) {
        console.log("⏩ oyster: OpenAPI spec is fresh, skipping fetch");
        return;
      }
    } catch {
      // File doesn't exist, proceed with fetch
    }
  }

  console.log(`📥 oyster: fetching OpenAPI spec from ${OPENAPI_DOCS_URL}`);

  try {
    const resp = await fetch(OPENAPI_DOCS_URL);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const html = await resp.text();

    // The spec is embedded as a JSON object in the Scalar HTML page.
    let spec = null;

    // Strategy 1: Look for {"openapi": in the HTML (the raw spec blob)
    const specMatch = html.match(
      /(\{"openapi"\s*:\s*"3[\s\S]*?\})\s*(?:<\/script>|,\s*")/,
    );
    if (specMatch) {
      const startIdx = html.indexOf('{"openapi"');
      if (startIdx !== -1) {
        spec = extractJsonObject(html, startIdx);
      }
    }

    // Strategy 2: data attribute
    if (!spec) {
      const dataMatch = html.match(/data-spec='([^']+)'/);
      if (dataMatch) {
        spec = dataMatch[1];
      }
    }

    // Strategy 3: spec content between markers
    if (!spec) {
      const contentMatch = html.match(
        /"spec"\s*:\s*(\{[\s\S]*?"paths"\s*:\s*\{[\s\S]*?\})\s*\}/,
      );
      if (contentMatch) {
        spec = contentMatch[1] + "}";
      }
    }

    if (!spec) {
      throw new Error(
        "Could not extract OpenAPI spec from Scalar HTML page",
      );
    }

    const parsed = JSON.parse(spec);
    if (!parsed.openapi || !parsed.info) {
      throw new Error("Extracted JSON is not a valid OpenAPI spec");
    }

    // Rewrite relative server URL to absolute testnet URL
    if (parsed.servers) {
      parsed.servers = parsed.servers.map((s) => {
        if (s.url && s.url.startsWith("/")) {
          return {
            ...s,
            url: `https://oyster.testnet.mystenlabs.com${s.url}`,
          };
        }
        return s;
      });
    }

    fs.mkdirSync(STATIC_DIR, { recursive: true });
    fs.writeFileSync(specPath, JSON.stringify(parsed, null, 2));
    console.log(
      `✅ oyster: OpenAPI spec saved (${parsed.paths ? Object.keys(parsed.paths).length : "?"} paths)`,
    );

    // Generate a standalone Scalar HTML page for iframe embedding
    const scalarHtml = generateScalarPage(JSON.stringify(parsed));
    fs.writeFileSync(path.join(STATIC_DIR, "scalar.html"), scalarHtml);
    console.log("✅ oyster: Scalar standalone page generated");
  } catch (err) {
    console.warn(
      `⚠️  oyster: OpenAPI spec fetch failed (${err.message}). Using existing spec if available.`,
    );
  }
}

function extractJsonObject(str, start) {
  let depth = 0;
  let inString = false;
  let escape = false;

  for (let i = start; i < str.length; i++) {
    const ch = str[i];
    if (escape) { escape = false; continue; }
    if (ch === "\\") { escape = true; continue; }
    if (ch === '"') { inString = !inString; continue; }
    if (inString) continue;
    if (ch === "{") depth++;
    if (ch === "}") {
      depth--;
      if (depth === 0) return str.slice(start, i + 1);
    }
  }
  return null;
}

function generateScalarPage(specJson) {
  return `<!-- Copyright (c) Walrus Foundation -->
<!-- SPDX-License-Identifier: Apache-2.0 -->
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Walrus Oyster API Reference</title>
  <style>
    body { margin: 0; }
    hr { display: none !important; }
    .darklight-reference-promo { display: none !important; }
    [class*="separator"] { display: none !important; }
  </style>
</head>
<body>
  <script id="api-reference" type="application/json">${specJson}</script>
  <script>
    document.addEventListener('DOMContentLoaded', () => {
      const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
      const parentDark = window.parent !== window
        ? window.parent.document.documentElement.getAttribute('data-theme') === 'dark'
        : prefersDark;

      const config = {
        theme: 'kepler',
        darkMode: parentDark,
        showSidebar: true,
        hideDownloadButton: false,
        hideModels: false,
        defaultOpenAllTags: true,
        hideSearch: true,
      };

      const scriptEl = document.getElementById('api-reference');
      scriptEl.dataset.configuration = JSON.stringify(config);
    });
  </script>
  <script src="https://cdn.jsdelivr.net/npm/@scalar/api-reference"></script>
</body>
</html>`;
}

main();
