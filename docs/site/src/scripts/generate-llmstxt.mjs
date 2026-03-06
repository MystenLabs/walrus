// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

import fs from "fs";
import path from "path";

// ── CLI args ─────────────────────────────────────────────────────────────────
const args = process.argv.slice(2);
const flags = {};
const positional = [];

for (let i = 0; i < args.length; i++) {
  if (args[i].startsWith("--")) {
    flags[args[i].slice(2)] = args[i + 1];
    i++;
  } else {
    positional.push(args[i]);
  }
}

const scriptDir = path.dirname(new URL(import.meta.url).pathname);
const markdownDir = path.resolve(positional[0] ?? path.join(scriptDir, "../../static/markdown"));
const baseUrl      = flags["base-url"]    ?? "";
const outputFile = flags["output"] ?? path.join(scriptDir, "../../static/llms.txt");
const siteDesc     = flags["description"] ?? "";

// ── Auto-detect docusaurus config ────────────────────────────────────────────
let resolvedName = flags["name"] ?? null;
let resolvedBaseUrl = baseUrl;

function findDocusaurusConfig(startDir) {
  let dir = startDir;
  for (let i = 0; i < 6; i++) {
    for (const cfg of ["docusaurus.config.js", "docusaurus.config.ts"]) {
      const p = path.join(dir, cfg);
      if (fs.existsSync(p)) return fs.readFileSync(p, "utf8");
    }
    const parent = path.dirname(dir);
    if (parent === dir) break;
    dir = parent;
  }
  return null;
}

const configText = findDocusaurusConfig(markdownDir);
if (configText) {
  if (!resolvedName) {
    const m = configText.match(/\btitle:\s*['"](.+?)['"]/);
    if (m) resolvedName = m[1];
  }
  if (!resolvedBaseUrl) {
    const m = configText.match(/\burl:\s*['"](.+?)['"]/);
    if (m) resolvedBaseUrl = m[1];
  }
}
resolvedName ??= "Documentation";

// ── Known acronyms & casing for section titles ──────────────────────────────
const KNOWN_CASING = {
  "http": "HTTP",
  "api": "API",
  "apis": "APIs",
  "typescript": "TypeScript",
  "sdk": "SDK",
  "sdks": "SDKs",
  "cli": "CLI",
  "ci": "CI",
  "cd": "CD",
  "dns": "DNS",
  "ip": "IP",
  "url": "URL",
  "urls": "URLs",
  "json": "JSON",
  "sui": "Sui",
  "walrus": "Walrus",
  "id": "ID",
  "ids": "IDs",
  "nft": "NFT",
  "nfts": "NFTs",
  "seo": "SEO",
  "tos": "TOS",
  "faq": "FAQ",
  "css": "CSS",
  "html": "HTML",
  "wasm": "WASM",
  "gh": "GH",
  "suins": "SuiNS",
};

// ── Preferred section ordering ──────────────────────────────────────────────
// Spec best practice: order sections from most to least important for LLMs.
// Core concepts first, then usage, then reference, then optional/legal.
const SECTION_PRIORITY = [
  "Getting Started",
  "System Overview",
  "Core Concepts",
  "Usage",
  "Walrus Client",
  "HTTP API",
  "TypeScript SDK",
  "Examples",
  "Operator Guide",
  "Sites",
  "Walrus Sites",
  "Stake",
  "Troubleshooting",
  "Design",
  "Dev Guide",
  "Data Security",
  "Glossary",
  "Tusky Migration Guide",
  "Legal",
];

// ── Helpers ──────────────────────────────────────────────────────────────────

function walk(dir, results = []) {
  if (!fs.existsSync(dir)) return results;
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      walk(full, results);
    } else if (entry.name.endsWith(".md") || entry.name.endsWith(".mdx")) {
      results.push(full);
    }
  }
  return results;
}

/**
 * Extract frontmatter from markdown content.
 * Returns { data: {title, description, sidebar_label, ...}, bodyStartIndex }
 */
function extractFrontmatter(content) {
  const data = {};
  let bodyStart = 0;

  // Match YAML frontmatter between --- delimiters
  const fmMatch = content.match(/^---\s*\n([\s\S]*?)\n---\s*\n?/);
  if (fmMatch) {
    bodyStart = fmMatch[0].length;
    const fmText = fmMatch[1];
    for (const line of fmText.split("\n")) {
      const kv = line.match(/^(\w[\w_-]*):\s*(.+)$/);
      if (kv) {
        let val = kv[2].trim();
        // Strip surrounding quotes
        if ((val.startsWith('"') && val.endsWith('"')) ||
            (val.startsWith("'") && val.endsWith("'"))) {
          val = val.slice(1, -1);
        }
        data[kv[1]] = val;
      }
    }
  }

  return { data, bodyStart };
}

/**
 * Robust title and description extraction.
 * Priority for title: frontmatter title > sidebar_label > first H1 > filename
 * Priority for description: frontmatter description > first meaningful paragraph
 */
function parseMarkdown(filePath, content) {
  let title = "";
  let description = "";

  // Check for metadata sidecar written by export script
  const metaPath = filePath.replace(/\.mdx?$/, ".meta.json");
  if (fs.existsSync(metaPath)) {
    try {
      const meta = JSON.parse(fs.readFileSync(metaPath, "utf8"));
      if (meta.title) title = meta.title;
      if (meta.description) description = meta.description;
    } catch {}
  }

  // Parse frontmatter
  const { data: fm, bodyStart } = extractFrontmatter(content);

  // Title priority: sidecar > frontmatter title > sidebar_label > first H1
  if (!title && fm.title) title = fm.title;
  if (!title && fm.sidebar_label) title = fm.sidebar_label;

  // Get body after frontmatter
  let body = content.slice(bodyStart);

  // Strip common Docusaurus/MDX directives and imports before processing
  body = body
    .replace(/^import\s+.*$/gm, "")
    .replace(/^export\s+.*$/gm, "")
    .replace(/<Tabs[\s\S]*?<\/Tabs>/gi, "")
    .replace(/<TabItem[\s\S]*?<\/TabItem>/gi, "")
    .replace(/:::[\s\S]*?:::/g, (match) => {
      // Keep the text content inside admonitions
      return match.replace(/^:::\w*\s*/gm, "").replace(/\s*:::$/gm, "");
    });

  // Strip unwanted HTML
  body = body
    .replace(/<a\b[^>]*>[\s\S]*?<\/a>/gi, "")
    .replace(/<span\s+class="code-inline"[^>]*>[\s\S]*?<\/span>/gi, "")
    .replace(/&nbsp;●&nbsp;/g, "")
    .replace(/&nbsp;/g, " ")
    .replace(/&gt;/g, ">")
    .replace(/&lt;/g, "<")
    .replace(/&amp;/g, "&")
    // Strip linear.app issue links
    .replace(/\[([^\]]*)\]\(https?:\/\/linear\.app\/[^)]*\)/gi, "$1")
    .replace(/https?:\/\/linear\.app\/\S+/gi, "")
    .replace(/\{[^}]*linear\.app[^}]*\}/gi, "")
    .replace(/\{\/\s*/g, "")
    .replace(/\s*\/\}/g, "");

  // Fallback: first H1 in body
  if (!title) {
    const h1 = body.match(/^#\s+(.+)$/m);
    if (h1) {
      title = h1[1]
        .replace(/\[([^\]]+)\]\([^)]+\)/g, "$1")  // strip links
        .replace(/[*_`]/g, "")                      // strip emphasis/code
        .trim();
    }
  }

  // Validate that extracted title looks like a real title, not content.
  if (title && isLikelyContentNotTitle(title)) {
    title = "";
  }

  // Description from frontmatter
  if (!description && fm.description) {
    description = fm.description;
  }

  if (!description) {
    description = extractDescription(body);
  }

  // Discard redirect-page descriptions
  if (/redirecting/i.test(description)) description = "";

  // Ensure all descriptions end at a sentence boundary
  if (description) {
    description = ensureSentenceEnding(description);
  }

  return { title, description };
}

/**
 * Ensure a description ends at a complete sentence.
 * If it already ends with sentence punctuation, return as-is.
 * Otherwise, truncate back to the last sentence boundary.
 */
function ensureSentenceEnding(text) {
  text = text.trim();
  if (!text) return "";

  // Already ends with sentence-ending punctuation
  if (/[.!?]$/.test(text)) return text;

  // Find the last sentence boundary
  for (let i = text.length - 1; i >= 0; i--) {
    if (text[i] === "." || text[i] === "!" || text[i] === "?") {
      const nextChar = text[i + 1];
      if (!nextChar || nextChar === " " || nextChar === '"' || nextChar === "'") {
        return text.slice(0, i + 1).trim();
      }
    }
  }

  // No sentence boundary found — return empty to avoid fragments
  return "";
}

/**
 * Detect if a "title" is actually content mistakenly captured.
 */
function isLikelyContentNotTitle(text) {
  if (text.length > 120) return true;
  if (/^(Copyright|Store |Download |Run |NOTE:|Verify:|Basic )/i.test(text)) return true;
  if (/['"].*['"].*['"]/.test(text)) return true;
  if (/\.\w{2,4}\b/.test(text) && !/\b(API|SDK)\b/i.test(text)) return true;
  return false;
}

/**
 * Extract a clean description from markdown body text.
 */
function extractDescription(body) {
  let text = body.replace(/^#+\s+.+$/gm, "");
  text = text.replace(/```[\s\S]*?```/g, "");

  text = text.replace(/`([^`]+)`/g, (_, code) => {
    if (code.length < 30 && !/\s{2,}|[|<>{}]/.test(code)) {
      return code;
    }
    return "";
  });

  text = text.replace(/\[([^\]]+)\]\([^)]+\)/g, "$1");
  text = text.replace(/!\[[^\]]*\]\([^)]+\)/g, "");
  text = text.replace(/\*{1,3}([^*]+)\*{1,3}/g, "$1");
  text = text.replace(/_{1,3}([^_]+)_{1,3}/g, "$1");
  text = text.replace(/<[^>]+>/g, "");
  text = text.replace(/^\s*[-*+]\s+/gm, "");
  text = text.replace(/^\s*\d+\.\s+/gm, "");

  // Clean up artifacts from stripped content
  text = text.replace(/\(\s*,[\s,]*\)/g, "");
  text = text.replace(/\(\s*,/g, "(");
  text = text.replace(/,\s*\)/g, ")");
  text = text.replace(/\(\s*\)/g, "");
  text = text.replace(/,(\s*,)+/g, ",");
  text = text.replace(/:\s*,/g, ":");
  text = text.replace(/:(\s*:)+/g, ":");

  text = text.replace(/\n+/g, " ").replace(/\s+/g, " ").trim();

  if (text.length === 0) return "";

  return extractSentences(text, 300);
}

/**
 * Extract complete sentences from text up to maxLen characters.
 * Always ends at a sentence boundary (., !, or ?).
 * If no sentence boundary is found, returns empty string rather than a fragment.
 */
function extractSentences(text, maxLen = 300) {
  if (!text || !text.trim()) return "";

  // If the entire text fits and ends with sentence-ending punctuation, use it
  if (text.length <= maxLen) {
    if (/[.!?]$/.test(text.trim())) return text.trim();
    // Short text that doesn't end in punctuation — check if it's a single complete thought
    // (e.g., a full sentence without trailing period is still okay if it's short enough)
  }

  const chunk = text.slice(0, maxLen);

  // Find the last sentence-ending punctuation followed by a space or end-of-string
  // This handles ". ", "! ", "? " as well as "." at end of chunk
  let lastEnd = -1;
  for (let i = chunk.length - 1; i >= 0; i--) {
    if (chunk[i] === "." || chunk[i] === "!" || chunk[i] === "?") {
      // Make sure it's not part of an abbreviation (e.g., "e.g.", "i.e.", "Dr.")
      // by checking if the next char is a space, end of string, or quote
      const nextChar = chunk[i + 1];
      if (!nextChar || nextChar === " " || nextChar === '"' || nextChar === "'") {
        lastEnd = i;
        break;
      }
    }
  }

  if (lastEnd > 0) {
    return chunk.slice(0, lastEnd + 1).trim();
  }

  // No sentence boundary found within maxLen — try a larger window
  const extendedChunk = text.slice(0, maxLen * 2);
  for (let i = maxLen; i < extendedChunk.length; i++) {
    if (extendedChunk[i] === "." || extendedChunk[i] === "!" || extendedChunk[i] === "?") {
      const nextChar = extendedChunk[i + 1];
      if (!nextChar || nextChar === " " || nextChar === '"' || nextChar === "'") {
        return extendedChunk.slice(0, i + 1).trim();
      }
    }
  }

  // If text is short enough overall and has no sentence punctuation, return as-is
  if (text.length <= maxLen) {
    return text.trim();
  }

  // No sentence boundary found at all — return empty rather than a fragment
  return "";
}

function fileToUrlPath(filePath, rootDir) {
  let rel = path.relative(rootDir, filePath).replace(/\\/g, "/");
  rel = rel.replace(/\.mdx?$/, "");
  if (rel === "index" || rel.endsWith("/index")) {
    rel = rel.replace(/\/?index$/, "") || "/";
  }
  return rel || "/";
}

function joinUrl(base, p) {
  if (!base) return "/" + p.replace(/^\//, "");
  return base.replace(/\/$/, "") + "/" + p.replace(/^\//, "");
}

/**
 * Smart section title casing with proper acronym handling.
 */
function toSectionTitle(seg) {
  return seg
    .replace(/[-_]/g, " ")
    .split(" ")
    .map((word) => {
      const lower = word.toLowerCase();
      if (KNOWN_CASING[lower]) return KNOWN_CASING[lower];
      return word.charAt(0).toUpperCase() + word.slice(1).toLowerCase();
    })
    .join(" ");
}

function isLinearUrl(url) {
  return /linear\.app/i.test(url);
}

// ── Collect pages ─────────────────────────────────────────────────────────────

if (!fs.existsSync(markdownDir)) {
  console.error(`Directory not found: ${markdownDir}`);
  process.exit(1);
}

const files = walk(markdownDir)
  .filter((f) => {
    const rel = path.relative(markdownDir, f).replace(/\\/g, "/");
    return !rel.startsWith("snippets/") && !f.endsWith(".meta.json");
  })
  .sort();

if (!files.length) {
  console.error(`No .md/.mdx files found in: ${markdownDir}`);
  process.exit(1);
}

const pages = [];

for (const file of files) {
  const content = fs.readFileSync(file, "utf8");
  if (!content.trim()) {
    console.warn(`⚠ Skipping empty file: ${path.relative(markdownDir, file)}`);
    continue;
  }
  const { title, description } = parseMarkdown(file, content);
  const urlPath = fileToUrlPath(file, markdownDir);

  // Ensure URL path starts with /docs
  const docUrlPath = urlPath.startsWith("/docs")
    ? urlPath
    : "/docs" + (urlPath.startsWith("/") ? urlPath : "/" + urlPath);
  const url = joinUrl(resolvedBaseUrl, docUrlPath) + ".md";

  // Skip linear.app URLs
  if (isLinearUrl(url)) continue;

  // Derive title from filename if no heading found, with smart casing.
  // For index files, use the parent directory name (e.g., "getting-started/index.mdx" → "Getting Started")
  const filename = path.basename(file, path.extname(file));
  let derivedTitle = title;
  if (!derivedTitle) {
    if (filename === "index") {
      const parentDir = path.basename(path.dirname(file));
      derivedTitle = parentDir && parentDir !== "." ? toSectionTitle(parentDir) : "Overview";
    } else {
      derivedTitle = toSectionTitle(filename);
    }
  }

  const segments = docUrlPath.replace(/^\//, "").split("/");
  // segments[0] is "docs", so use segments[1] for category grouping
  const section = segments.length > 2
    ? toSectionTitle(segments[1])
    : segments.length > 1 && segments[1]
      ? toSectionTitle(segments[1])
      : "General";

  pages.push({ title: derivedTitle, url, description, section });
}

// ── Build llms.txt ────────────────────────────────────────────────────────────

const TARGET_CHARS = 120_000;

// Group pages by section
const sectionOrderRaw = [];
const grouped = {};
for (const page of pages) {
  if (!grouped[page.section]) {
    sectionOrderRaw.push(page.section);
    grouped[page.section] = [];
  }
  grouped[page.section].push(page);
}

// Sort sections by defined priority, with unknown sections at the end
const sectionOrder = [...sectionOrderRaw].sort((a, b) => {
  const ai = SECTION_PRIORITY.indexOf(a);
  const bi = SECTION_PRIORITY.indexOf(b);
  const aPri = ai === -1 ? 999 : ai;
  const bPri = bi === -1 ? 999 : bi;
  if (aPri !== bPri) return aPri - bPri;
  return a.localeCompare(b);
});

// Introductory context (spec: blockquote with key project info)
const introDesc = siteDesc ||
  "Walrus is a decentralized blob storage protocol built on Sui. " +
  "It provides robust, cost-effective storage for large binary objects " +
  "with high availability guarantees using erasure coding. " +
  "This documentation covers setup, usage, the client CLI, SDKs, " +
  "on-chain integration, and Walrus Sites (decentralized web hosting).";

// Spec-compliant: additional context paragraphs between blockquote and sections.
// These are freeform markdown (no headings) that help LLMs interpret the docs.
const contextNotes = [
  "- Walrus stores data as **blobs** (immutable byte arrays). All blobs are public; use an encryption service like Seal for private data.",
  "- Walrus has two networks: **Mainnet** (production, uses real SUI/WAL tokens) and **Testnet** (for development and testing).",
  "- The `walrus` CLI is the primary client. It supports storing, reading, and managing blobs, and can run as a local daemon exposing an HTTP API.",
  "- **Walrus Sites** enable fully decentralized web hosting: static assets stored on Walrus, with a Sui smart contract as the on-chain index.",
  "- Costs involve WAL tokens (for storage) and SUI tokens (for on-chain transactions).",
];

// Identify optional/secondary sections that LLMs can skip for shorter context
const OPTIONAL_SECTIONS = new Set(["Design", "Dev Guide", "Legal", "Tusky Migration Guide"]);

// Format link entries per the spec: - [Title](url): Description
// The spec uses `: description` after the link, not a separate indented line
function formatEntry({ title, url, description }) {
  if (description) {
    return `- [${title}](${url}): ${description}`;
  }
  return `- [${title}](${url})`;
}

// Wrap a line to max 100 chars
function wrapLine(line, indentSpaces = 0) {
  if (line.length <= 100) return [line];
  const indent = " ".repeat(indentSpaces);
  const words = line.trimStart().split(" ");
  const lines = [];
  let current = indent;
  for (const word of words) {
    if (current.length + word.length + 1 > 100 && current.trim().length > 0) {
      lines.push(current.trimEnd());
      current = indent + "    " + word + " ";
    } else {
      current += word + " ";
    }
  }
  if (current.trim()) lines.push(current.trimEnd());
  return lines;
}

// ── Build output ─────────────────────────────────────────────────────────────

// Separate required and optional sections per spec
const requiredSections = sectionOrder.filter((s) => !OPTIONAL_SECTIONS.has(s));
const optionalSections = sectionOrder.filter((s) => OPTIONAL_SECTIONS.has(s));

function buildOutput(includeDescriptions, includeOptional) {
  const lines = [`# ${resolvedName}`, ""];

  // Wrap blockquote to 100 chars (continuation lines start with "> ")
  const bqText = `> ${introDesc}`;
  if (bqText.length <= 100) {
    lines.push(bqText);
  } else {
    const words = introDesc.split(" ");
    let current = "> ";
    for (const word of words) {
      if (current.length + word.length + 1 > 100 && current.trim().length > 2) {
        lines.push(current.trimEnd());
        current = "> " + word + " ";
      } else {
        current += word + " ";
      }
    }
    if (current.trim().length > 1) lines.push(current.trimEnd());
  }
  lines.push("");

  // Context notes — wrap each bullet to 100 chars with 4-space continuation indent
  lines.push("Important notes:", "");
  for (const note of contextNotes) {
    lines.push(...wrapLine(note, 0));
  }
  lines.push("");

  // Required sections
  for (const section of requiredSections) {
    lines.push(`## ${section}`, "");
    for (const page of grouped[section]) {
      const entry = includeDescriptions ? formatEntry(page) : `- [${page.title}](${page.url})`;
      lines.push(...wrapLine(entry, 0));
    }
    lines.push("");
  }

  // Optional section (spec: ## Optional has special meaning — can be skipped for shorter context)
  if (includeOptional && optionalSections.length > 0) {
    lines.push("## Optional", "");
    for (const section of optionalSections) {
      // Use H3 sub-headings within Optional
      lines.push(`### ${section}`, "");
      for (const page of grouped[section]) {
        const entry = includeDescriptions ? formatEntry(page) : `- [${page.title}](${page.url})`;
        lines.push(...wrapLine(entry, 0));
      }
      lines.push("");
    }
  }

  return lines.join("\n");
}

// First pass: full descriptions + optional sections
let output = buildOutput(true, true);

// Second pass: full descriptions, drop optional sections
if (output.length > TARGET_CHARS) {
  output = buildOutput(true, false);
}

// Third pass: drop descriptions
if (output.length > TARGET_CHARS) {
  output = buildOutput(false, false);
}

// Fourth pass: drop pages proportionally per section
if (output.length > TARGET_CHARS) {
  const ratio = TARGET_CHARS / output.length;
  const finalLines = [`# ${resolvedName}`, ""];
  finalLines.push(...wrapLine(`> ${introDesc}`, 0), "");
  for (const section of requiredSections) {
    const sectionPages = grouped[section];
    const keep = Math.max(1, Math.floor(sectionPages.length * ratio));
    finalLines.push(`## ${section}`, "");
    for (const page of sectionPages.slice(0, keep)) {
      finalLines.push(...wrapLine(formatEntry(page), 0));
    }
    finalLines.push("");
  }
  output = finalLines.join("\n");
}

// Ensure output directory exists
const outDir = path.dirname(path.resolve(outputFile));
fs.mkdirSync(outDir, { recursive: true });

fs.writeFileSync(outputFile, output, "utf8");
console.log(`✓ Generated ${outputFile} with ${pages.length} pages across ${sectionOrder.length} sections (${output.length.toLocaleString()} chars)`);
