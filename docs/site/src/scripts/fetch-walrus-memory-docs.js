// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0
//
// Fetches Walrus Memory documentation from MystenLabs/MemWal at build time.
//
// Usage:
//   node src/scripts/fetch-walrus-memory-docs.js [--force]
//
// Sparse-clones the docs/ directory from the MemWal repo (dev branch) into
// a local cache. Pass --force to skip the freshness check (used in CI builds).
//
// On network failure the script logs a warning and exits 0 so local dev
// is not blocked.

const fs = require("fs");
const path = require("path");
const { execSync } = require("child_process");

const SITE_ROOT = path.resolve(__dirname, "../../");
const CACHE_DIR = path.join(SITE_ROOT, ".cache-walrus-memory");
const SOURCE_PATH = "docs";
const REPO = "MystenLabs/MemWal";
const BRANCH = "dev";
const FRESHNESS_MINUTES = 10;

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
      `⏩ walrus-memory: cache is fresh (< ${FRESHNESS_MINUTES}m), skipping fetch`,
    );
    return;
  }

  console.log(`📥 walrus-memory: fetching ${REPO}@${BRANCH}/${SOURCE_PATH}`);

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

    console.log("✅ walrus-memory: fetched successfully");
  } catch (err) {
    console.warn(`⚠️  walrus-memory: fetch failed (${err.message}). Skipping.`);
  }
}

main();
