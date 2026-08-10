// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/**
 * Reports Walrus platform releases that have no editorial summary in
 * docs/editorial/, so no release ships without changelog coverage.
 *
 * The release-notes SOP covers releases that arrive with GTM content.
 * Releases without it (GitHub body is empty or just the Full Log link)
 * previously fell through: generate-release-notes.js either rendered them
 * raw or skipped them entirely. This script finds every such release and,
 * for each one, emits a ready-to-edit editorial draft in the house format.
 *
 * Run: node src/scripts/check-editorial-coverage.js [options]
 *   --json <path>            write machine-readable results for CI
 *   --releases-file <path>   read releases from a JSON file instead of the
 *                            GitHub API (testing seam; same shape as
 *                            /repos/MystenLabs/walrus/releases)
 *   --limit <n>              only check the newest <n> versions (default 25)
 *   --strict                 exit 1 when any release lacks an editorial file
 */

const fs = require("fs");
const path = require("path");
const https = require("https");

const EDITORIAL_DIR = path.resolve(__dirname, "../../../editorial");

// ── CLI args ───────────────────────────────────────────────────────

function parseArgs(argv) {
  const args = { limit: 25 };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--json") args.json = argv[++i];
    else if (a === "--releases-file") args.releasesFile = argv[++i];
    else if (a === "--limit") args.limit = parseInt(argv[++i], 10);
    else if (a === "--source") args.source = argv[++i];
    else if (a === "--strict") args.strict = true;
  }
  return args;
}

// ── GitHub API helpers (same approach as generate-release-notes.js) ──

function fetchJSON(urlPath) {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: "api.github.com",
      path: urlPath,
      method: "GET",
      headers: {
        "User-Agent": "walrus-editorial-coverage",
        Accept: "application/vnd.github.v3+json",
      },
    };
    const token = process.env.GITHUB_TOKEN;
    if (token) options.headers["Authorization"] = `token ${token}`;

    const req = https.request(options, (res) => {
      let data = "";
      res.on("data", (chunk) => (data += chunk));
      res.on("end", () => {
        if (res.statusCode === 200) resolve(JSON.parse(data));
        else reject(new Error(`GitHub API ${res.statusCode}: ${data.slice(0, 200)}`));
      });
    });
    req.on("error", reject);
    req.end();
  });
}

async function fetchAllPages(basePath) {
  const results = [];
  let page = 1;
  while (true) {
    const data = await fetchJSON(
      `${basePath}${basePath.includes("?") ? "&" : "?"}per_page=100&page=${page}`,
    );
    if (!Array.isArray(data) || data.length === 0) break;
    results.push(...data);
    if (data.length < 100) break;
    page++;
  }
  return results;
}

// ── Release parsing (mirrors generate-release-notes.js) ────────────

function extractNetwork(tag) {
  const lower = tag.toLowerCase();
  if (lower.includes("mainnet")) return "Mainnet";
  if (lower.includes("testnet")) return "Testnet";
  if (lower.includes("devnet")) return "Devnet";
  return "Other";
}

function parseVersion(tag) {
  const match = tag.match(/v?(\d+)\.(\d+)\.(\d+)/i);
  if (!match) return null;
  return `${parseInt(match[1])}.${parseInt(match[2])}.${parseInt(match[3])}`;
}

function versionSortKey(version) {
  const [major, minor, patch] = version.split(".").map(Number);
  return major * 1e6 + minor * 1e3 + patch;
}

// A body that is empty or only the boilerplate Full Log link counts as
// "no content": there is nothing for the changelog to say without an
// editorial summary.
function substantiveBody(body) {
  if (!body) return "";
  return body
    .replace(/^-{3,}\s*$/gm, "")
    .replace(/^#{1,6}\s*Full Log:.*$/gim, "")
    .trim();
}

function groupReleases(releases) {
  const byVersion = new Map();
  for (const r of releases) {
    if (r.draft) continue;
    const network = extractNetwork(r.tag_name);
    if (network === "Devnet" || network === "Other") continue;
    const version = parseVersion(r.tag_name);
    if (!version) continue;

    if (!byVersion.has(version)) byVersion.set(version, {});
    const entry = byVersion.get(version);
    const data = {
      tag: r.tag_name,
      network,
      body: r.body || "",
      date: r.published_at,
      url: r.html_url,
    };
    if (network === "Mainnet") entry.mainnet = data;
    else if (!entry.testnet) entry.testnet = data;
  }
  return byVersion;
}

// Product releases (Walrus Memory packages, Walrus Sites) publish one
// release per version rather than a Mainnet/Testnet pair, so they group by
// the version their tag encodes.
function groupProductReleases(releases, parseTag) {
  const byKey = new Map();
  for (const r of releases) {
    if (r.draft) continue;
    const parsed = parseTag(r.tag_name);
    if (!parsed) continue;
    if (byKey.has(parsed.key)) continue;
    byKey.set(parsed.key, {
      version: parsed.version,
      editorialSlug: parsed.editorialSlug,
      label: parsed.label,
      release: {
        tag: r.tag_name,
        network: parsed.label,
        body: r.body || "",
        date: r.published_at,
        url: r.html_url,
      },
    });
  }
  return byKey;
}

// ── Release sources ────────────────────────────────────────────────
//
// The changelog draws on three repositories, but every editorial summary
// lives in this repository under docs/editorial/. A missing summary for any
// product is fixed by a pull request here, which is why the tracking issues
// belong in this repository too.

// Mirrors memwalCategoryKey in generate-release-notes.js.
function memwalCategoryKey(pkg) {
  const lower = (pkg || "").toLowerCase();
  if (lower.includes("mcp")) return "mcp";
  if (lower.includes("python")) return "python";
  if (lower.includes("oc-") || lower.includes("openclaw")) return "openclaw";
  if (lower.includes("memwal")) return "sdk";
  return null;
}

const SOURCES = [
  {
    id: "walrus",
    label: "Walrus",
    repo: "MystenLabs/walrus",
    // Network-paired releases: mainnet-v1.53.0 and testnet-v1.53.0 are the
    // same version of the same product.
    paired: true,
    editorialPattern: /^walrus-v(\d+\.\d+\.\d+)\.md$/,
    editorialKey: (match) => `walrus|${match[1]}`,
    slugFor: (version) => `walrus-v${version}`,
    titleFor: (version) => `Walrus v${version}`,
  },
  {
    id: "memwal",
    label: "Walrus Memory",
    repo: "MystenLabs/MemWal",
    editorialPattern: /^memwal-(sdk|mcp|python|openclaw)-v(\d+\.\d+\.\d+)\.md$/,
    editorialKey: (match) => `memwal-${match[1]}|${match[2]}`,
    // Package releases tagged <package>@<version>.
    parseTag: (tag) => {
      const match = tag.match(/^(.+)@(\d+\.\d+\.\d+)$/);
      if (!match) return null;
      const category = memwalCategoryKey(match[1]);
      if (!category) return null;
      return {
        key: `memwal-${category}|${match[2]}`,
        version: match[2],
        editorialSlug: `memwal-${category}-v${match[2]}`,
        label: `Walrus Memory ${category}`,
      };
    },
  },
  {
    id: "walrus-sites",
    label: "Walrus Sites",
    repo: "MystenLabs/walrus-sites",
    editorialPattern: /^walrus-sites-v(\d+\.\d+\.\d+)\.md$/,
    editorialKey: (match) => `walrus-sites|${match[1]}`,
    parseTag: (tag) => {
      const version = parseVersion(tag);
      if (!version) return null;
      return {
        key: `walrus-sites|${version}`,
        version,
        editorialSlug: `walrus-sites-v${version}`,
        label: "Walrus Sites",
      };
    },
  },
];

// ── Editorial coverage ─────────────────────────────────────────────

// Returns the covered keys across every product, for example
// "walrus|1.53.0", "memwal-sdk|0.0.7", "walrus-sites|2.12.0".
function loadEditorialVersions() {
  const covered = new Set();
  if (!fs.existsSync(EDITORIAL_DIR)) return covered;
  for (const file of fs.readdirSync(EDITORIAL_DIR)) {
    for (const source of SOURCES) {
      const match = file.match(source.editorialPattern);
      if (match) {
        covered.add(source.editorialKey(match));
        break;
      }
    }
  }
  return covered;
}

// ── Draft skeleton in the docs/editorial house format ──────────────

function formatDate(isoDate) {
  if (!isoDate) return "";
  return new Date(isoDate).toLocaleDateString("en-US", {
    year: "numeric",
    month: "long",
    day: "numeric",
  });
}

function buildDraft(title, release, hasContent, productKeyword) {
  const summary = hasContent
    ? "TODO: one short paragraph summarizing the developer-facing changes below."
    : "A maintenance release with no user-facing changes documented in the release notes; see\n" +
      "the full commit log for internal updates.";
  const description = hasContent
    ? "TODO: first sentence of the summary."
    : "A maintenance release with no user-facing changes documented in the release notes; see the full commit log for...";
  const keywords = [productKeyword, "release notes", "changelog", release.network.toLowerCase()];

  return [
    "---",
    `title: ${title}`,
    `description: ${description}`,
    `keywords: [${keywords.map((k) => `"${k}"`).join(", ")}]`,
    "---",
    "",
    `**${release.network}** | ${formatDate(release.date)}`,
    "",
    summary,
    "",
  ].join("\n");
}

// ── Main ───────────────────────────────────────────────────────────

async function checkSource(source, args, editorial) {
  let releases;
  if (args.releasesFile) {
    // Testing seam: a single fixture stands in for the source under test.
    releases = JSON.parse(fs.readFileSync(args.releasesFile, "utf8"));
  } else {
    releases = await fetchAllPages(`/repos/${source.repo}/releases`);
  }

  const missing = [];
  let checked = 0;

  if (source.paired) {
    const byVersion = groupReleases(releases);
    const versions = [...byVersion.keys()]
      .sort((a, b) => versionSortKey(b) - versionSortKey(a))
      .slice(0, args.limit);
    checked = versions.length;

    for (const version of versions) {
      if (editorial.has(`${source.id}|${version}`)) continue;
      const entry = byVersion.get(version);
      const release = entry.mainnet || entry.testnet;
      if (!release) continue;

      const hasContent = substantiveBody(release.body).length >= 20;
      const title = source.titleFor(version);
      missing.push({
        product: source.label,
        repo: source.repo,
        version,
        tag: release.tag,
        network: release.network,
        date: release.date,
        url: release.url,
        hasContent,
        editorialFile: `docs/editorial/${source.slugFor(version)}.md`,
        draft: buildDraft(title, release, hasContent, source.id),
      });
    }
  } else {
    const byKey = groupProductReleases(releases, source.parseTag);
    const keys = [...byKey.keys()]
      .sort((a, b) => versionSortKey(b.split("|")[1]) - versionSortKey(a.split("|")[1]))
      .slice(0, args.limit);
    checked = keys.length;

    for (const key of keys) {
      if (editorial.has(key)) continue;
      const entry = byKey.get(key);
      const hasContent = substantiveBody(entry.release.body).length >= 20;
      const title = `${entry.label} v${entry.version}`;
      missing.push({
        product: entry.label,
        repo: source.repo,
        version: entry.version,
        tag: entry.release.tag,
        network: entry.release.network,
        date: entry.release.date,
        url: entry.release.url,
        hasContent,
        editorialFile: `docs/editorial/${entry.editorialSlug}.md`,
        draft: buildDraft(title, entry.release, hasContent, source.id),
      });
    }
  }

  return { checked, missing };
}

async function main() {
  const args = parseArgs(process.argv);
  const editorial = loadEditorialVersions();

  const sources = args.source
    ? SOURCES.filter((s) => s.id === args.source)
    : SOURCES;
  if (sources.length === 0) {
    console.error(
      `Unknown source '${args.source}'. Known: ${SOURCES.map((s) => s.id).join(", ")}.`,
    );
    process.exit(2);
  }

  const missing = [];
  for (const source of sources) {
    const result = await checkSource(source, args, editorial);
    console.log(
      `Checked newest ${result.checked} ${source.label} release version(s) from ${source.repo}.`,
    );
    missing.push(...result.missing);
  }

  console.log(`Editorial summaries on disk: ${editorial.size}`);
  if (missing.length === 0) {
    console.log("All checked releases have editorial summaries.");
  } else {
    console.log(`\n${missing.length} release(s) missing an editorial summary:\n`);
    for (const m of missing) {
      const kind = m.hasContent ? "raw release body only" : "no release content";
      console.log(
        `  - ${m.product} v${m.version} (${m.network}, ${kind}) -> create ${m.editorialFile}`,
      );
    }
    console.log(
      "\nEvery summary lives in this repository, so each fix is a pull request here,\n" +
        "whichever repository published the release.",
    );
  }

  if (args.json) {
    fs.writeFileSync(args.json, JSON.stringify({ missing }, null, 2));
    console.log(`\nWrote ${args.json}`);
  }

  if (args.strict && missing.length > 0) process.exit(1);
}

main().catch((err) => {
  console.error("check-editorial-coverage failed:", err.message);
  process.exit(2);
});
