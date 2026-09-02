// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0
//
// Fetches Oyster documentation from MystenLabs/oyster at build time.
//
// Usage:
//   node src/scripts/fetch-oyster-docs.js [--force]
//
// Sparse-clones the docs/src/ directory from the oyster repo (main branch)
// into a local cache. Also fetches the OpenAPI spec from the live testnet
// endpoint (embedded inline in the Scalar HTML page) and saves it as a
// static JSON file.
//
// Pass --force to skip the freshness check (used in CI).
//
// On network failure the script logs a warning and exits 0 so local dev
// is not blocked.

const fs = require("fs");
const path = require("path");
const { execSync } = require("child_process");

const SITE_ROOT = path.resolve(__dirname, "../../");
const CACHE_DIR = path.join(SITE_ROOT, ".cache-oyster");
const STATIC_DIR = path.join(SITE_ROOT, "static/oyster");
const SOURCE_PATH = "docs/src";
const REPO = "MystenLabs/oyster";
const BRANCH = "main";
const FRESHNESS_MINUTES = 10;
const OPENAPI_DOCS_URL = "https://oyster.testnet.mystenlabs.com/api/docs";

const force = process.argv.includes("--force") || process.env.FORCE_FETCH === "1";

function isFresh(dir) {
  if (force) return false;
  try {
    const stat = fs.statSync(dir);
    return Date.now() - stat.mtimeMs < FRESHNESS_MINUTES * 60 * 1000;
  } catch {
    return false;
  }
}

function main() {
  const sourceDir = path.join(CACHE_DIR, SOURCE_PATH);

  if (isFresh(sourceDir)) {
    console.log(
      `⏩ oyster: cache is fresh (< ${FRESHNESS_MINUTES}m), skipping fetch`,
    );
    return;
  }

  console.log(`📥 oyster: fetching ${REPO}@${BRANCH}/${SOURCE_PATH}`);

  if (fs.existsSync(CACHE_DIR)) {
    fs.rmSync(CACHE_DIR, { recursive: true });
  }
  fs.mkdirSync(CACHE_DIR, { recursive: true });

  try {
    execSync(
      [
        `git clone --depth 1 --filter=blob:none --sparse`,
        `--branch ${BRANCH}`,
        `https://github.com/${REPO}.git`,
        `"${CACHE_DIR}"`,
      ].join(" "),
      { stdio: "pipe" },
    );

    execSync(`git -C "${CACHE_DIR}" sparse-checkout set "${SOURCE_PATH}"`, {
      stdio: "pipe",
    });

    const now = new Date();
    fs.utimesSync(sourceDir, now, now);

    console.log("✅ oyster: docs fetched successfully");
  } catch (err) {
    if (force) {
      console.error(`❌ oyster: docs fetch failed in CI (${err.message})`);
      process.exit(1);
    }
    console.warn(`⚠️  oyster: docs fetch failed (${err.message}). Using cached content.`);
  }

  // Fetch the OpenAPI spec from the live Scalar page (spec is embedded inline)
  fetchOpenApiSpec();
}

async function fetchOpenApiSpec() {
  const specPath = path.join(STATIC_DIR, "openapi.json");

  // Check freshness of existing spec
  if (!force) {
    try {
      const stat = fs.statSync(specPath);
      if (Date.now() - stat.mtimeMs < FRESHNESS_MINUTES * 60 * 1000) {
        console.log(`⏩ oyster: OpenAPI spec is fresh, skipping fetch`);
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
    // Extract it by finding the configuration object that contains the spec.
    // utoipa-scalar embeds the spec in a data-configuration attribute or
    // as an inline JSON object. Try multiple extraction strategies.
    let spec = null;

    // Strategy 1: Look for {"openapi": in the HTML (the raw spec blob)
    const specMatch = html.match(/(\{"openapi"\s*:\s*"3[\s\S]*?\})\s*(?:<\/script>|,\s*")/);
    if (specMatch) {
      // The match might be truncated by the non-greedy pattern. Instead,
      // find the start position and parse the JSON from there.
      const startIdx = html.indexOf('{"openapi"');
      if (startIdx !== -1) {
        // Walk forward to find the complete JSON object
        spec = extractJsonObject(html, startIdx);
      }
    }

    // Strategy 2: If the spec is in a data attribute
    if (!spec) {
      const dataMatch = html.match(/data-spec='([^']+)'/);
      if (dataMatch) {
        spec = dataMatch[1];
      }
    }

    // Strategy 3: Look for spec content between specific markers
    if (!spec) {
      const contentMatch = html.match(/"spec"\s*:\s*(\{[\s\S]*?"paths"\s*:\s*\{[\s\S]*?\})\s*\}/);
      if (contentMatch) {
        spec = contentMatch[1] + "}";
      }
    }

    if (!spec) {
      throw new Error("Could not extract OpenAPI spec from Scalar HTML page");
    }

    // Validate it's parseable JSON
    const parsed = JSON.parse(spec);
    if (!parsed.openapi || !parsed.info) {
      throw new Error("Extracted JSON is not a valid OpenAPI spec");
    }

    // Rewrite relative server URL to absolute testnet URL so "Test Request" works
    if (parsed.servers) {
      parsed.servers = parsed.servers.map((s) => {
        if (s.url && s.url.startsWith("/")) {
          return { ...s, url: `https://oyster.testnet.mystenlabs.com${s.url}` };
        }
        return s;
      });
    }

    fs.mkdirSync(STATIC_DIR, { recursive: true });
    fs.writeFileSync(specPath, JSON.stringify(parsed, null, 2));
    console.log(`✅ oyster: OpenAPI spec saved (${parsed.paths ? Object.keys(parsed.paths).length : "?"} paths)`);

    // Generate a standalone Scalar HTML page for iframe embedding
    const scalarHtml = generateScalarPage(JSON.stringify(parsed));
    fs.writeFileSync(path.join(STATIC_DIR, "scalar.html"), scalarHtml);
    console.log("✅ oyster: Scalar standalone page generated");
  } catch (err) {
    if (force) {
      console.error(`❌ oyster: OpenAPI spec fetch failed (${err.message})`);
      // Non-fatal even in CI — the docs still build without the interactive spec
    }
    console.warn(`⚠️  oyster: OpenAPI spec fetch failed (${err.message}). Spec page may not load.`);
  }
}

// Extract a complete JSON object starting at position `start` in `str`
// by counting braces.
function extractJsonObject(str, start) {
  let depth = 0;
  let inString = false;
  let escape = false;

  for (let i = start; i < str.length; i++) {
    const ch = str[i];

    if (escape) {
      escape = false;
      continue;
    }

    if (ch === "\\") {
      escape = true;
      continue;
    }

    if (ch === '"') {
      inString = !inString;
      continue;
    }

    if (inString) continue;

    if (ch === "{") depth++;
    if (ch === "}") {
      depth--;
      if (depth === 0) {
        return str.slice(start, i + 1);
      }
    }
  }

  return null;
}

function generateScalarPage(specJson) {
  return `<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Oyster API Reference</title>
  <style>
    body { margin: 0; }
    /* Remove any stray horizontal rules or borders */
    hr { display: none !important; }
    .darklight-reference-promo { display: none !important; }
    [class*="separator"] { display: none !important; }
  </style>
</head>
<body>
  <script id="api-reference" type="application/json">${specJson}</script>
  <script>
    document.addEventListener('DOMContentLoaded', () => {
      // Detect parent theme
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
