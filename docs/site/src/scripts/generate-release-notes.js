// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/**
 * Generates a consolidated changelog from three sources:
 *   1. Walrus GitHub releases (MystenLabs/walrus)
 *   2. Walrus Memory GitHub releases (MystenLabs/MemWal)
 *   3. Walrus Sites GitHub releases (MystenLabs/walrus-sites)
 *
 * Each entry shows the editorial summary followed by linked
 * individual changes from the GitHub release body.
 *
 * Run: node src/scripts/generate-release-notes.js
 */

const fs = require("fs");
const path = require("path");
const https = require("https");

const EDITORIAL_DIR = path.resolve(__dirname, "../../../editorial");
const OUTPUT_DIR = path.resolve(__dirname, "../../../content/release-notes");
const OUTPUT_HUB = path.resolve(__dirname, "../../../content/release-notes.mdx");
const OUTPUT_WALRUS = path.resolve(OUTPUT_DIR, "walrus-platform.mdx");
const OUTPUT_MEMORY = path.resolve(OUTPUT_DIR, "walrus-memory.mdx");
const OUTPUT_SITES = path.resolve(OUTPUT_DIR, "walrus-sites.mdx");
const OUTPUT_JSON = path.resolve(__dirname, "../data/changelog.json");

// ── GitHub API helpers ─────────────────────────────────────────────

function fetchJSON(urlPath) {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: "api.github.com",
      path: urlPath,
      method: "GET",
      headers: {
        "User-Agent": "walrus-release-notes",
        Accept: "application/vnd.github.v3+json",
      },
    };

    const token = process.env.GITHUB_TOKEN;
    if (token) {
      options.headers["Authorization"] = `token ${token}`;
    }

    const req = https.request(options, (res) => {
      let data = "";
      res.on("data", (chunk) => (data += chunk));
      res.on("end", () => {
        if (res.statusCode === 200) {
          resolve(JSON.parse(data));
        } else {
          reject(
            new Error(
              `GitHub API ${res.statusCode}: ${data.slice(0, 200)}`,
            ),
          );
        }
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

// ── Content processing ─────────────────────────────────────────────

function sanitizeForMDX(content) {
  content = content.replace(/\r\n/g, "\n").replace(/\r/g, "\n");
  content = content.replace(/\boverriden\b/g, "overridden");

  content = content.replace(
    /(?<!\[#\d+\]\()https:\/\/github\.com\/([^/\s]+)\/([^/\s]+)\/pull\/(\d+)(?!\))/g,
    "[#$3](https://github.com/$1/$2/pull/$3)",
  );

  content = content.replace(/<([^>\s]+@[^>]+)>/g, "&lt;$1&gt;");

  const codeBlocks = [];
  content = content.replace(/(```[\s\S]*?```|`[^`]+`)/g, (match) => {
    codeBlocks.push(match);
    return `__CODE_BLOCK_${codeBlocks.length - 1}__`;
  });
  const validHtml = new Set([
    "a", "b", "i", "em", "strong", "code", "pre", "p", "br", "hr",
    "ul", "ol", "li", "h1", "h2", "h3", "h4", "h5", "h6",
    "details", "summary", "div", "span", "table", "tr", "td", "th",
    "thead", "tbody", "img", "blockquote", "sup", "sub",
  ]);
  content = content.replace(/<(\/?)([\w-]+)([^>]*)>/g, (match, slash, tag, rest) => {
    if (validHtml.has(tag.toLowerCase())) return match;
    return `&lt;${slash}${tag}${rest}&gt;`;
  });

  content = content.replace(/(\s|^)<(\s)/g, "$1&lt;$2");
  content = content.replace(/(\s)>(\s|$)/g, "$1&gt;$2");
  codeBlocks.forEach((block, index) => {
    content = content.replace(`__CODE_BLOCK_${index}__`, block);
  });

  return content;
}

function formatDate(isoDate) {
  if (!isoDate) return "";
  const d = new Date(isoDate);
  return d.toLocaleDateString("en-US", {
    year: "numeric",
    month: "long",
    day: "numeric",
  });
}

// ── Extract a summary paragraph from a GitHub release body ─────────

function extractSummary(body) {
  if (!body) return null;

  const lines = body.split("\n");

  // Try to find a "## Summary" section
  let inSummary = false;
  const summaryLines = [];
  for (const line of lines) {
    const trimmed = line.trim();
    if (/^#{1,3}\s+summary$/i.test(trimmed)) {
      inSummary = true;
      continue;
    }
    if (inSummary) {
      if (/^#{1,6}\s+/.test(trimmed) || trimmed === "---") break;
      summaryLines.push(line);
    }
  }
  const summaryText = summaryLines.join("\n").trim();
  if (summaryText) return summaryText;

  // Fallback: first non-empty paragraph that isn't a heading, rule,
  // bullet, link-only line, or metadata
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    if (trimmed.startsWith("#")) continue;
    if (trimmed === "---") continue;
    if (trimmed.startsWith("- ") || trimmed.startsWith("* ")) continue;
    if (trimmed.startsWith("**Full Changelog**")) continue;
    if (trimmed.startsWith("**Full Log**")) continue;
    if (/^\[#\d+\]/.test(trimmed)) continue;
    if (/^https?:\/\//.test(trimmed)) continue;
    if (trimmed.length > 30) return trimmed;
  }

  return null;
}

// ── Extract linked changes from a GitHub release body ──────────────

function extractLinkedChanges(body, repoSlug) {
  if (!body) return [];

  const changes = [];
  const lines = body.split("\n");

  for (const line of lines) {
    const trimmed = line.trim();

    if (
      trimmed.startsWith("#") ||
      trimmed === "---" ||
      trimmed === "" ||
      trimmed.startsWith("**Full Changelog**") ||
      trimmed.startsWith("**Full Log**")
    ) {
      continue;
    }

    // ── Pattern A: MemWal changeset lines ──
    const memwalMatch = trimmed.match(
      /^[-*]\s+\[#(\d+)\]\((https:\/\/github\.com\/[^)]+)\)[\s\S]*?!\s*-\s+(.+)$/,
    );
    if (memwalMatch) {
      changes.push({
        text: sanitizeForMDX(memwalMatch[3].trim()),
        url: memwalMatch[2],
      });
      continue;
    }

    // ── Pattern B: Bullet with linked PR ref at end ──
    const bulletLinkedEnd = trimmed.match(
      /^[-*]\s+(.+?)\s*\(\[#(\d+)\]\((https:\/\/github\.com\/[^)]+)\)\)\s*$/,
    );
    if (bulletLinkedEnd) {
      let text = bulletLinkedEnd[1].replace(/^\*\*([^*]+)\*\*:\s*/, "$1: ");
      changes.push({ text: sanitizeForMDX(text), url: bulletLinkedEnd[3] });
      continue;
    }

    // ── Pattern C: Bullet with bare PR ref at end ──
    const bulletBareEnd = trimmed.match(
      /^[-*]\s+(.+?)\s*\(#(\d+)\)\s*$/,
    );
    if (bulletBareEnd) {
      let text = bulletBareEnd[1].replace(/^\*\*([^*]+)\*\*:\s*/, "$1: ");
      changes.push({
        text: sanitizeForMDX(text),
        url: `https://github.com/${repoSlug}/pull/${bulletBareEnd[2]}`,
      });
      continue;
    }

    // ── Pattern D: [#NNN](url): description ──
    const linkedLine = trimmed.match(
      /^\[#(\d+)\]\((https:\/\/github\.com\/[^)]+)\):\s*(.+)$/,
    );
    if (linkedLine) {
      changes.push({ text: sanitizeForMDX(linkedLine[3].trim()), url: linkedLine[2] });
      continue;
    }

    // ── Pattern E: Full URL: description ──
    const fullUrlLine = trimmed.match(
      /^(https:\/\/github\.com\/[^/]+\/[^/]+\/pull\/\d+):\s*(.+)$/,
    );
    if (fullUrlLine) {
      changes.push({ text: sanitizeForMDX(fullUrlLine[2].trim()), url: fullUrlLine[1] });
      continue;
    }
  }

  return changes;
}

// ── Editorial summaries (docs/editorial/*.md) ──────────────────────

function loadEditorialSummaries() {
  const walrus = new Map();
  const memwal = new Map();
  const sites = new Map();
  if (!fs.existsSync(EDITORIAL_DIR)) return { walrus, memwal, sites };

  const files = fs
    .readdirSync(EDITORIAL_DIR)
    .filter((f) => f.endsWith(".md"));

  for (const file of files) {
    const raw = fs.readFileSync(path.join(EDITORIAL_DIR, file), "utf8");
    const fmMatch = raw.match(/^---\n([\s\S]*?)\n---\n([\s\S]*)$/);
    if (!fmMatch) continue;

    const frontmatter = fmMatch[1];
    let body = fmMatch[2].trim();
    body = body.replace(/^\*\*[^*]+\*\*(\s*\|.*)?(\n\n?|\n)/, "").trim();
    if (!body) continue;

    const titleMatch = frontmatter.match(/title:\s*(.+)/);
    const title = titleMatch
      ? titleMatch[1].trim().replace(/^["']|["']$/g, "")
      : null;

    // Walrus platform: walrus-v1.47.1.md → 1.47.1
    const walrusMatch = file.match(/^walrus-v(\d+\.\d+\.\d+)\.md$/);
    if (walrusMatch) {
      walrus.set(walrusMatch[1], { title, body });
      continue;
    }

    // MemWal packages
    const memwalMatch = file.match(
      /^memwal-(sdk|mcp|python|openclaw)-v(\d+\.\d+\.\d+)\.md$/,
    );
    if (memwalMatch) {
      const key = `${memwalMatch[1]}|${memwalMatch[2]}`;
      memwal.set(key, { title, body });
      continue;
    }

    // Walrus Sites: walrus-sites-v2.12.0.md → 2.12.0
    const sitesMatch = file.match(/^walrus-sites-v(\d+\.\d+\.\d+)\.md$/);
    if (sitesMatch) {
      sites.set(sitesMatch[1], { title, body });
    }
  }

  return { walrus, memwal, sites };
}

// ── Shared helpers ─────────────────────────────────────────────────

function extractNetwork(tag) {
  const lower = tag.toLowerCase();
  if (lower.includes("mainnet")) return "Mainnet";
  if (lower.includes("testnet")) return "Testnet";
  if (lower.includes("devnet")) return "Devnet";
  return "Other";
}

function cleanTag(tag) {
  return tag.replace(/^(mainnet|testnet|devnet)-/i, "");
}

function parseVersion(tag) {
  const match = tag.match(/v?(\d+)\.(\d+)\.(\d+)/i);
  if (!match) return null;
  return {
    major: parseInt(match[1]),
    minor: parseInt(match[2]),
    patch: parseInt(match[3]),
  };
}

function versionKey(v) {
  return `${v.major}.${v.minor}.${v.patch}`;
}

// ── Source 1: Walrus GitHub releases ───────────────────────────────

async function loadWalrusReleases(editorialWalrus = new Map()) {
  console.log("  Fetching MystenLabs/walrus releases...");
  const releases = await fetchAllPages(
    "/repos/MystenLabs/walrus/releases",
  );

  const byVersion = new Map();

  for (const r of releases) {
    if (r.draft) continue;
    const tag = r.tag_name;
    const network = extractNetwork(tag);
    if (network === "Devnet" || network === "Other") continue;

    const v = parseVersion(tag);
    if (!v) continue;
    const key = versionKey(v);

    if (!byVersion.has(key)) {
      byVersion.set(key, { version: v, mainnet: null, testnet: null });
    }
    const entry = byVersion.get(key);
    const data = {
      tag,
      network,
      body: r.body || "",
      date: r.published_at,
      url: r.html_url,
    };
    if (network === "Mainnet") entry.mainnet = data;
    else if (network === "Testnet" && !entry.testnet) entry.testnet = data;
  }

  const results = [];
  for (const [, entry] of byVersion) {
    const rel = entry.mainnet || entry.testnet;
    if (!rel) continue;
    // Skip releases with an empty body ONLY when no editorial summary
    // exists for the version: with an editorial file present, the entry
    // renders from the summary alone, so writing docs/editorial/ coverage
    // is sufficient to publish a release that shipped without content.
    const emptyBody = !rel.body || rel.body.trim().length < 20;
    if (emptyBody && !editorialWalrus.has(versionKey(entry.version))) continue;

    results.push({
      title: `Walrus ${cleanTag(rel.tag)}`,
      date: rel.date,
      network: rel.network,
      body: rel.body,
      source: "walrus",
      url: rel.url,
      tag: rel.tag,
      version: entry.version,
    });
  }

  results.sort((a, b) => {
    const va = a.version, vb = b.version;
    if (va.major !== vb.major) return vb.major - va.major;
    if (va.minor !== vb.minor) return vb.minor - va.minor;
    return vb.patch - va.patch;
  });

  console.log(`  Found ${results.length} Walrus releases`);
  return results;
}

// ── Source 2: MemWal GitHub releases ───────────────────────────────

function parseMemWalPackage(tag) {
  const match = tag.match(/^(.+)@(\d+\.\d+\.\d+)$/);
  if (!match) return null;
  return { package: match[1], version: match[2] };
}

function memwalCategoryKey(pkg) {
  const lower = (pkg || "").toLowerCase();
  if (lower.includes("mcp")) return "mcp";
  if (lower.includes("python")) return "python";
  if (lower.includes("oc-") || lower.includes("openclaw")) return "openclaw";
  if (lower.includes("memwal")) return "sdk";
  return "other";
}

async function loadMemWalReleases() {
  console.log("  Fetching MystenLabs/MemWal releases...");
  const releases = await fetchAllPages(
    "/repos/MystenLabs/MemWal/releases",
  );

  const results = [];
  for (const r of releases) {
    if (r.draft) continue;
    const tag = r.tag_name;
    const parsed = parseMemWalPackage(tag);
    if (!parsed) continue;
    if (!r.body || r.body.trim().length < 20) continue;

    let displayName = parsed.package
      .replace("@mysten-incubation/", "")
      .replace("memwal-", "Walrus Memory ")
      .replace("memwal", "Walrus Memory SDK");

    if (displayName === "Walrus Memory SDK")
      displayName = "Walrus Memory TypeScript SDK";
    if (displayName === "Walrus Memory mcp")
      displayName = "Walrus Memory MCP";
    if (displayName.startsWith("oc-"))
      displayName = displayName
        .replace("oc-", "")
        .replace("memwal", "OpenClaw");

    results.push({
      title: `${displayName} v${parsed.version}`,
      date: r.published_at,
      body: r.body,
      source: "memwal",
      url: r.html_url,
      tag,
      package: parsed.package,
      version: parsed.version,
    });
  }

  results.sort(
    (a, b) => new Date(b.date).getTime() - new Date(a.date).getTime(),
  );

  console.log(`  Found ${results.length} Walrus Memory releases`);
  return results;
}

// ── Source 3: Walrus Sites GitHub releases ─────────────────────────

async function loadWalrusSitesReleases() {
  console.log("  Fetching MystenLabs/walrus-sites releases...");
  const releases = await fetchAllPages(
    "/repos/MystenLabs/walrus-sites/releases",
  );

  const results = [];
  for (const r of releases) {
    if (r.draft) continue;
    const tag = r.tag_name;
    const v = parseVersion(tag);
    if (!v) continue;

    results.push({
      title: `Walrus Sites ${cleanTag(tag)}`,
      date: r.published_at,
      body: r.body || "",
      source: "walrus-sites",
      url: r.html_url,
      tag,
      version: v,
    });
  }

  results.sort((a, b) => {
    const va = a.version, vb = b.version;
    if (va.major !== vb.major) return vb.major - va.major;
    if (va.minor !== vb.minor) return vb.minor - va.minor;
    return vb.patch - va.patch;
  });

  console.log(`  Found ${results.length} Walrus Sites releases`);
  return results;
}

// ── Render a single release entry ──────────────────────────────────

function renderEntry({ heading, badge, date, url, editorial, changes }) {
  let out = `### ${heading}\n\n`;
  const dateStr = formatDate(date);
  const parts = [];
  if (badge) parts.push(`\`${badge}\``);
  if (dateStr) parts.push(dateStr);
  parts.push(`[GitHub](${url})`);
  out += parts.join(" | ") + "\n\n";

  if (editorial) {
    out += editorial + "\n\n";
  }

  if (changes.length > 0) {
    out += `#### Changes\n\n`;
    for (const c of changes) {
      out += `- [${c.text}](${c.url})\n`;
    }
    out += "\n";
  }

  out += "---\n\n";
  return out;
}

// ── Build the pages ───────────────────────────────────────────────

async function main() {
  console.log("Generating release notes...\n");

  const editorial = loadEditorialSummaries();
  console.log(`  Found ${editorial.walrus.size} Walrus editorial summaries`);
  console.log(`  Found ${editorial.memwal.size} Walrus Memory editorial summaries`);
  console.log(`  Found ${editorial.sites.size} Walrus Sites editorial summaries`);

  let walrusReleases = [];
  let memwalReleases = [];
  let sitesReleases = [];

  try {
    walrusReleases = await loadWalrusReleases(editorial.walrus);
  } catch (err) {
    console.warn("  Warning: could not fetch Walrus releases:", err.message);
  }

  const missingEditorial = walrusReleases.filter(
    (rel) => !editorial.walrus.has(versionKey(rel.version)),
  );
  if (missingEditorial.length > 0) {
    console.warn(
      `  Warning: ${missingEditorial.length} Walrus release(s) render without an editorial summary:`,
    );
    for (const rel of missingEditorial) {
      const v = versionKey(rel.version);
      console.warn(`    - v${v} (add docs/editorial/walrus-v${v}.md)`);
    }
    console.warn("  Run check-editorial-coverage.js for ready-to-edit drafts.");
  }

  try {
    memwalReleases = await loadMemWalReleases();
  } catch (err) {
    console.warn("  Warning: could not fetch MemWal releases:", err.message);
  }

  try {
    sitesReleases = await loadWalrusSitesReleases();
  } catch (err) {
    console.warn("  Warning: could not fetch Walrus Sites releases:", err.message);
  }

  if (!fs.existsSync(OUTPUT_DIR)) {
    fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  }

  // ── Hub page ──
  const hub = `---
title: Release Notes
description: 'Release notes for Walrus, Walrus Memory, and Walrus Sites.'
displayed_sidebar: null
hide_table_of_contents: true
questions:
  - What changed in the latest Walrus release?
  - Where can I find Walrus release history?
  - What breaking changes should I be aware of?
answer: 'Release notes for Walrus, Walrus Memory, and Walrus Sites.'
---

# Release Notes

- [**Walrus**](release-notes/walrus-platform) — Release notes from [Walrus](https://github.com/MystenLabs/walrus/releases).
- [**Walrus Memory**](release-notes/walrus-memory) — Release notes from
  [Walrus Memory](https://github.com/MystenLabs/MemWal/releases),
  including the MCP server, TypeScript SDK, Python SDK, and OpenClaw.
- [**Walrus Sites**](release-notes/walrus-sites) — Release notes from
  [Walrus Sites](https://github.com/MystenLabs/walrus-sites/releases).
`;

  fs.writeFileSync(OUTPUT_HUB, hub, "utf8");
  console.log(`\nWrote ${OUTPUT_HUB}`);

  // ── Walrus page ──
  let walrusMdx = `---
title: Walrus release notes
description: Release notes for the Walrus decentralized storage platform.
displayed_sidebar: null
hide_table_of_contents: true
questions:
  - What changed in the latest Walrus release?
  - Where can I find Walrus release history?
  - What breaking changes should I be aware of?
answer: Release notes for the Walrus decentralized storage platform.
---

`;

  if (walrusReleases.length > 0) {
    for (const rel of walrusReleases) {
      const vKey = versionKey(rel.version);
      const ed = editorial.walrus.get(vKey);
      const changes = extractLinkedChanges(rel.body, "MystenLabs/walrus");

      walrusMdx += renderEntry({
        heading: ed?.title || rel.title,
        badge: rel.network === "Mainnet" ? "Mainnet" : "Testnet",
        date: rel.date,
        url: rel.url,
        editorial: ed?.body || extractSummary(rel.body),
        changes,
      });
    }
  } else {
    walrusMdx += `No Walrus releases found.\n\n`;
  }

  fs.writeFileSync(OUTPUT_WALRUS, walrusMdx, "utf8");
  console.log(`Wrote ${OUTPUT_WALRUS}`);

  // ── Walrus Memory page (with sub-tabs) ──
  let memoryMdx = `---
title: Walrus Memory release notes
description: 'Release notes for Walrus Memory, MCP, TypeScript SDK, Python SDK, and OpenClaw.'
displayed_sidebar: null
hide_table_of_contents: true
questions:
  - What changed in the latest Walrus Memory release?
  - Where can I find Walrus Memory MCP release notes?
  - What is new in the Walrus Memory TypeScript SDK?
answer: 'Release notes for Walrus Memory, MCP, TypeScript SDK, Python SDK, and OpenClaw.'
---

`;

  if (memwalReleases.length > 0) {
    const categories = {
      mcp: { label: "MCP", releases: [] },
      sdk: { label: "TypeScript SDK", releases: [] },
      python: { label: "Python SDK", releases: [] },
      openclaw: { label: "OpenClaw", releases: [] },
      other: { label: "Other", releases: [] },
    };

    for (const rel of memwalReleases) {
      const pkg = (rel.package || "").toLowerCase();
      if (pkg.includes("mcp")) {
        categories.mcp.releases.push(rel);
      } else if (pkg.includes("python")) {
        categories.python.releases.push(rel);
      } else if (pkg.includes("oc-") || pkg.includes("openclaw")) {
        categories.openclaw.releases.push(rel);
      } else if (pkg.includes("memwal")) {
        categories.sdk.releases.push(rel);
      } else {
        categories.other.releases.push(rel);
      }
    }

    const activeCats = Object.entries(categories).filter(
      ([, cat]) => cat.releases.length > 0,
    );

    memoryMdx += `<Tabs groupId="memory-sub">\n\n`;

    for (const [key, cat] of activeCats) {
      const isFirst = key === activeCats[0][0];
      memoryMdx += `<TabItem value="${key}" label="${cat.label}"${isFirst ? " default" : ""}>\n\n`;

      for (const rel of cat.releases) {
        const ed = editorial.memwal.get(`${key}|${rel.version}`);
        const changes = extractLinkedChanges(rel.body, "MystenLabs/MemWal");

        memoryMdx += renderEntry({
          heading: ed?.title || rel.title,
          badge: null,
          date: rel.date,
          url: rel.url,
          editorial: ed?.body || extractSummary(rel.body),
          changes,
        });
      }

      memoryMdx += `</TabItem>\n\n`;
    }

    memoryMdx += `</Tabs>\n\n`;
  } else {
    memoryMdx += `No Walrus Memory releases found.\n\n`;
  }

  fs.writeFileSync(OUTPUT_MEMORY, memoryMdx, "utf8");
  console.log(`Wrote ${OUTPUT_MEMORY}`);

  // ── Walrus Sites page ──
  let sitesMdx = `---
title: Walrus Sites release notes
description: Release notes for Walrus Sites.
displayed_sidebar: null
hide_table_of_contents: true
questions:
  - What changed in the latest Walrus Sites release?
  - Where can I find Walrus Sites release history?
answer: Release notes for Walrus Sites.
---

`;

  if (sitesReleases.length > 0) {
    for (const rel of sitesReleases) {
      const vKey = versionKey(rel.version);
      const ed = editorial.sites.get(vKey);
      const changes = extractLinkedChanges(rel.body, "MystenLabs/walrus-sites");

      sitesMdx += renderEntry({
        heading: ed?.title || rel.title,
        badge: null,
        date: rel.date,
        url: rel.url,
        editorial: ed?.body || extractSummary(rel.body),
        changes,
      });
    }
  } else {
    sitesMdx += `No Walrus Sites releases found.\n\n`;
  }

  fs.writeFileSync(OUTPUT_SITES, sitesMdx, "utf8");
  console.log(`Wrote ${OUTPUT_SITES}`);

  // ── Generate JSON for the changelog page component ──

  const allEntries = [];

  for (const rel of walrusReleases) {
    const vKey = versionKey(rel.version);
    const ed = editorial.walrus.get(vKey);
    allEntries.push({
      id: `walrus-${rel.tag}`,
      date: rel.date,
      category: "walrus",
      title: ed?.title || rel.title,
      description: ed?.body || extractSummary(rel.body),
      badge: rel.network,
      githubUrl: rel.url,
      changes: extractLinkedChanges(rel.body, "MystenLabs/walrus"),
    });
  }

  for (const rel of memwalReleases) {
    const catKey = memwalCategoryKey(rel.package);
    const ed = editorial.memwal.get(`${catKey}|${rel.version}`);
    allEntries.push({
      id: `memwal-${rel.tag}`,
      date: rel.date,
      category: "walrus-memory",
      title: ed?.title || rel.title,
      description: ed?.body || extractSummary(rel.body),
      badge: null,
      githubUrl: rel.url,
      changes: extractLinkedChanges(rel.body, "MystenLabs/MemWal"),
    });
  }

  for (const rel of sitesReleases) {
    const vKey = versionKey(rel.version);
    const ed = editorial.sites.get(vKey);
    allEntries.push({
      id: `sites-${rel.tag}`,
      date: rel.date,
      category: "walrus-sites",
      title: ed?.title || rel.title,
      description: ed?.body || extractSummary(rel.body),
      badge: null,
      githubUrl: rel.url,
      changes: extractLinkedChanges(rel.body, "MystenLabs/walrus-sites"),
    });
  }

  allEntries.sort(
    (a, b) => new Date(b.date).getTime() - new Date(a.date).getTime(),
  );

  fs.writeFileSync(OUTPUT_JSON, JSON.stringify(allEntries, null, 2), "utf8");
  console.log(`Wrote ${OUTPUT_JSON} (${allEntries.length} entries)`);

  console.log(
    `\n  ${walrusReleases.length} Walrus + ${memwalReleases.length} Memory + ${sitesReleases.length} Sites releases`,
  );
}

main().catch((err) => {
  console.error("Error:", err);
  process.exit(1);
});
