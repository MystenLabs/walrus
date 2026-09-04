// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0
//
// Fetches Oyster documentation and static files from MystenLabs/oyster.
//
// Usage:
//   node src/scripts/fetch-oyster-docs.js [--force]
//
// Sparse-clones docs/src/ (markdown content) and docs/static/ (llms.txt,
// openapi.json, scalar.html) from the public oyster repo. The static
// files are copied into the Docusaurus static/oyster/ directory.
//
// Pass --force to skip the freshness check (used in CI builds).

const fs = require("fs");
const path = require("path");
const { execSync } = require("child_process");

const SITE_ROOT = path.resolve(__dirname, "../../");
const CACHE_DIR = path.join(SITE_ROOT, ".cache-oyster");
const STATIC_DIR = path.join(SITE_ROOT, "static/oyster");
const SOURCE_PATH = "docs/src";
const STATIC_PATH = "docs/static";
const REPO = "MystenLabs/oyster";
const BRANCH = "main";
const FRESHNESS_MINUTES = 10;

const force =
  process.argv.includes("--force") || process.env.FORCE_FETCH === "1";

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
      `⏩ oyster: cache is fresh (< ${FRESHNESS_MINUTES}m), skipping`,
    );
    return;
  }

  console.log(`📥 oyster: fetching ${REPO}@${BRANCH}`);

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

    execSync(
      `git -C "${CACHE_DIR}" sparse-checkout set` +
        ` "${SOURCE_PATH}" "${STATIC_PATH}"`,
      { stdio: "pipe" },
    );

    const now = new Date();
    fs.utimesSync(sourceDir, now, now);

    // Copy static files (llms.txt, openapi.json, scalar.html) into
    // the Docusaurus static/oyster/ directory.
    const upstreamStatic = path.join(CACHE_DIR, STATIC_PATH);
    if (fs.existsSync(upstreamStatic)) {
      fs.mkdirSync(STATIC_DIR, { recursive: true });
      for (const f of fs.readdirSync(upstreamStatic)) {
        fs.copyFileSync(
          path.join(upstreamStatic, f),
          path.join(STATIC_DIR, f),
        );
      }
      console.log("✅ oyster: static files copied");
    }

    console.log("✅ oyster: fetch complete");
  } catch (err) {
    if (force) {
      console.error(`❌ oyster: fetch failed (${err.message})`);
      process.exit(1);
    }
    console.warn(
      `⚠️  oyster: fetch failed (${err.message}). ` +
        "Using cached content if available.",
    );
  }
}

main();
