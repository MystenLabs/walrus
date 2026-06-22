// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/**
 * Generates a consolidated release notes page from three sources:
 *   1. Walrus blog posts (docs/blog/*.mdx)
 *   2. Walrus GitHub releases (MystenLabs/walrus)
 *   3. Walrus Memory GitHub releases (MystenLabs/MemWal)
 *
 * Inspired by Sui's convert-release-notes.js.
 * Run: node src/scripts/generate-release-notes.js
 */

const fs = require("fs");
const path = require("path");
const https = require("https");

const BLOG_DIR = path.resolve(__dirname, "../../../blog");
const OUTPUT_PATH = path.resolve(
  __dirname,
  "../../../content/release-notes.mdx",
);

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
  // Convert raw GitHub PR URLs to links
  content = content.replace(
    /(?<!\[#\d+\]\()https:\/\/github\.com\/([^/\s]+)\/([^/\s]+)\/pull\/(\d+)(?!\))/g,
    "[#$3](https://github.com/$1/$2/pull/$3)",
  );

  // Escape email-like angle brackets
  content = content.replace(/<([^>\s]+@[^>]+)>/g, "&lt;$1&gt;");

  // Protect code blocks, then escape stray angle brackets
  const codeBlocks = [];
  content = content.replace(/(```[\s\S]*?```|`[^`]+`)/g, (match) => {
    codeBlocks.push(match);
    return `__CODE_BLOCK_${codeBlocks.length - 1}__`;
  });
  // Escape all <word> patterns that look like placeholders or stray tags
  // (but not valid HTML like <details>, <summary>, <a>, <br>, etc.)
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

function bumpHeadings(content) {
  // Convert all headings to h4 so they nest under the release h3
  return content.replace(
    /^(#{1,6})\s+(.*)$/gm,
    (_, hashes, text) => `#### ${text.trim()}`,
  );
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

// ── Source 1: Blog posts ───────────────────────────────────────────

function loadBlogPosts() {
  if (!fs.existsSync(BLOG_DIR)) return [];

  const files = fs
    .readdirSync(BLOG_DIR)
    .filter((f) => f.endsWith(".mdx") || f.endsWith(".md"))
    .sort()
    .reverse(); // newest first by filename

  return files.map((file) => {
    const raw = fs.readFileSync(path.join(BLOG_DIR, file), "utf8");
    const fmMatch = raw.match(/^---\n([\s\S]*?)\n---\n([\s\S]*)$/);
    if (!fmMatch) return null;

    const frontmatter = fmMatch[1];
    const body = fmMatch[2].trim();

    const titleMatch = frontmatter.match(/title:\s*(.+)/);
    const dateMatch = frontmatter.match(/date:\s*(.+)/);

    const title = titleMatch
      ? titleMatch[1].trim().replace(/^["']|["']$/g, "")
      : file;
    const date = dateMatch ? dateMatch[1].trim() : null;

    return { title, date, body, source: "blog", file };
  }).filter(Boolean);
}

// ── Source 2: Walrus GitHub releases ───────────────────────────────

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

async function loadWalrusReleases() {
  console.log("  Fetching MystenLabs/walrus releases...");
  const releases = await fetchAllPages(
    "/repos/MystenLabs/walrus/releases",
  );

  // Group by version, prefer mainnet over testnet
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

  // Convert to flat list, sorted newest first
  const results = [];
  for (const [, entry] of byVersion) {
    // Prefer mainnet, fall back to testnet
    const rel = entry.mainnet || entry.testnet;
    if (!rel) continue;
    if (!rel.body || rel.body.trim().length < 20) continue; // skip empty

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

  console.log(`  Found ${results.length} Walrus releases with content`);
  return results;
}

// ── Source 3: MemWal GitHub releases ───────────────────────────────

function parseMemWalPackage(tag) {
  // Tags like @mysten-incubation/memwal@0.0.7 or memwal-python@0.1.4
  const match = tag.match(/^(.+)@(\d+\.\d+\.\d+)$/);
  if (!match) return null;
  return { package: match[1], version: match[2] };
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

    // Clean up package name for display
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

  // Sort by date descending
  results.sort(
    (a, b) => new Date(b.date).getTime() - new Date(a.date).getTime(),
  );

  console.log(`  Found ${results.length} Walrus Memory releases with content`);
  return results;
}

// ── Build the page ─────────────────────────────────────────────────

async function main() {
  console.log("Generating release notes...\n");

  // Load all sources
  const blogPosts = loadBlogPosts();
  console.log(`  Found ${blogPosts.length} blog posts`);

  let walrusReleases = [];
  let memwalReleases = [];

  try {
    walrusReleases = await loadWalrusReleases();
  } catch (err) {
    console.warn("  Warning: could not fetch Walrus releases:", err.message);
  }

  try {
    memwalReleases = await loadMemWalReleases();
  } catch (err) {
    console.warn(
      "  Warning: could not fetch MemWal releases:",
      err.message,
    );
  }

  // ── Build MDX output ──

  let mdx = `---
title: "Release Notes"
description: "Release notes for Walrus, Walrus Memory, and related tools."
sidebar_label: "Release Notes"
---

# Release Notes

Release notes from [Walrus](https://github.com/MystenLabs/walrus/releases), [Walrus Memory](https://github.com/MystenLabs/MemWal/releases), and the [Walrus blog](/blog).

---

`;

  // ── Section 1: Walrus platform releases ──
  if (walrusReleases.length > 0) {
    mdx += `## Walrus Platform\n\n`;

    for (const rel of walrusReleases) {
      const badge =
        rel.network === "Mainnet" ? "**Mainnet**" : "**Testnet**";
      const dateStr = formatDate(rel.date);
      const link = `[GitHub](${rel.url})`;

      mdx += `### ${rel.title}\n\n`;
      mdx += `${badge} | ${dateStr} | ${link}\n\n`;

      let body = sanitizeForMDX(rel.body);
      body = bumpHeadings(body);
      body = body.replace(/\n{3,}/g, "\n\n").trim();
      mdx += body + "\n\n---\n\n";
    }
  }

  // ── Section 2: Walrus Memory releases ──
  if (memwalReleases.length > 0) {
    mdx += `## Walrus Memory\n\n`;

    for (const rel of memwalReleases) {
      const dateStr = formatDate(rel.date);
      const link = `[GitHub](${rel.url})`;

      mdx += `### ${rel.title}\n\n`;
      mdx += `${dateStr} | ${link}\n\n`;

      let body = sanitizeForMDX(rel.body);
      body = bumpHeadings(body);
      body = body.replace(/\n{3,}/g, "\n\n").trim();
      mdx += body + "\n\n---\n\n";
    }
  }

  // ── Section 3: Blog announcements ──
  if (blogPosts.length > 0) {
    mdx += `## Blog Announcements\n\n`;

    for (const post of blogPosts) {
      const dateStr = post.date
        ? formatDate(post.date)
        : "";
      const slug = post.file
        .replace(/\.mdx?$/, "");

      mdx += `### ${post.title}\n\n`;
      if (dateStr) mdx += `${dateStr} | `;
      mdx += `[Read full post](/blog/${slug})\n\n`;

      // Include first paragraph as summary
      const firstPara = post.body
        .split("\n\n")
        .find((p) => p.trim() && !p.startsWith("#") && !p.startsWith("<") && !p.startsWith("import"));
      if (firstPara) {
        mdx += firstPara.trim() + "\n\n";
      }

      mdx += "---\n\n";
    }
  }

  // Write output
  const outputDir = path.dirname(OUTPUT_PATH);
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true });
  }

  fs.writeFileSync(OUTPUT_PATH, mdx, "utf8");
  console.log(`\nWrote ${OUTPUT_PATH}`);
  console.log(
    `  ${walrusReleases.length} Walrus releases + ${memwalReleases.length} Memory releases + ${blogPosts.length} blog posts`,
  );
}

main().catch((err) => {
  console.error("Error:", err);
  process.exit(1);
});
