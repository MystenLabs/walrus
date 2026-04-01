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

function extractFrontmatter(content) {
  const data = {};
  let bodyStart = 0;

  const fmMatch = content.match(/^---\s*\n([\s\S]*?)\n---\s*\n?/);
  if (fmMatch) {
    bodyStart = fmMatch[0].length;
    const fmText = fmMatch[1];
    for (const line of fmText.split("\n")) {
      const kv = line.match(/^(\w[\w_-]*):\s*(.+)$/);
      if (kv) {
        let val = kv[2].trim();
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

function parseMarkdown(filePath, content) {
  let title = "";
  let description = "";

  const metaPath = filePath.replace(/\.mdx?$/, ".meta.json");
  if (fs.existsSync(metaPath)) {
    try {
      const meta = JSON.parse(fs.readFileSync(metaPath, "utf8"));
      if (meta.title) title = meta.title;
      if (meta.description) description = meta.description;
    } catch {}
  }

  const { data: fm, bodyStart } = extractFrontmatter(content);

  if (!title && fm.title) title = fm.title;
  if (!title && fm.sidebar_label) title = fm.sidebar_label;

  let body = content.slice(bodyStart);

  body = body
    .replace(/^import\s+.*$/gm, "")
    .replace(/^export\s+.*$/gm, "")
    .replace(/<Tabs[\s\S]*?<\/Tabs>/gi, "")
    .replace(/<TabItem[\s\S]*?<\/TabItem>/gi, "")
    .replace(/:::[\s\S]*?:::/g, (match) => {
      return match.replace(/^:::\w*\s*/gm, "").replace(/\s*:::$/gm, "");
    });

  body = body
    .replace(/<a\b[^>]*>[\s\S]*?<\/a>/gi, "")
    .replace(/<span\s+class="code-inline"[^>]*>[\s\S]*?<\/span>/gi, "")
    .replace(/&nbsp;●&nbsp;/g, "")
    .replace(/&nbsp;/g, " ")
    .replace(/&gt;/g, ">")
    .replace(/&lt;/g, "<")
    .replace(/&amp;/g, "&")
    .replace(/\[([^\]]*)\]\(https?:\/\/linear\.app\/[^)]*\)/gi, "$1")
    .replace(/https?:\/\/linear\.app\/\S+/gi, "")
    .replace(/\{[^}]*linear\.app[^}]*\}/gi, "")
    .replace(/\{\/\s*/g, "")
    .replace(/\s*\/\}/g, "");

  if (!title) {
    const h1 = body.match(/^#\s+(.+)$/m);
    if (h1) {
      title = h1[1]
        .replace(/\[([^\]]+)\]\([^)]+\)/g, "$1")
        .replace(/[*_`]/g, "")
        .trim();
    }
  }

  if (title && isLikelyContentNotTitle(title)) {
    title = "";
  }

  if (!description && fm.description) {
    description = fm.description;
  }

  if (!description) {
    description = extractDescription(body);
  }

  if (/redirecting/i.test(description)) description = "";

  if (description) {
    description = ensureSentenceEnding(description);
  }

  return { title, description };
}

function ensureSentenceEnding(text) {
  text = text.trim();
  if (!text) return "";
  if (/[.!?]$/.test(text)) return text;
  for (let i = text.length - 1; i >= 0; i--) {
    if (text[i] === "." || text[i] === "!" || text[i] === "?") {
      const nextChar = text[i + 1];
      if (!nextChar || nextChar === " " || nextChar === '"' || nextChar === "'") {
        return text.slice(0, i + 1).trim();
      }
    }
  }
  return "";
}

function isLikelyContentNotTitle(text) {
  if (text.length > 120) return true;
  if (/^(Copyright|Store |Download |Run |NOTE:|Verify:|Basic )/i.test(text)) return true;
  if (/['"].*['"].*['"]/.test(text)) return true;
  if (/\.\w{2,4}\b/.test(text) && !/\b(API|SDK)\b/i.test(text)) return true;
  return false;
}

function extractDescription(body) {
  let text = body.replace(/^#+\s+.+$/gm, "");
  text = text.replace(/```[\s\S]*?```/g, "");
  text = text.replace(/`([^`]+)`/g, (_, code) => {
    if (code.length < 30 && !/\s{2,}|[|<>{}]/.test(code)) return code;
    return "";
  });
  text = text.replace(/\[([^\]]+)\]\([^)]+\)/g, "$1");
  text = text.replace(/!\[[^\]]*\]\([^)]+\)/g, "");
  text = text.replace(/\*{1,3}([^*]+)\*{1,3}/g, "$1");
  text = text.replace(/_{1,3}([^_]+)_{1,3}/g, "$1");
  text = text.replace(/<[^>]+>/g, "");
  text = text.replace(/^\s*[-*+]\s+/gm, "");
  text = text.replace(/^\s*\d+\.\s+/gm, "");
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

function extractSentences(text, maxLen = 300) {
  if (!text || !text.trim()) return "";
  if (text.length <= maxLen) {
    if (/[.!?]$/.test(text.trim())) return text.trim();
  }
  const chunk = text.slice(0, maxLen);
  let lastEnd = -1;
  for (let i = chunk.length - 1; i >= 0; i--) {
    if (chunk[i] === "." || chunk[i] === "!" || chunk[i] === "?") {
      const nextChar = chunk[i + 1];
      if (!nextChar || nextChar === " " || nextChar === '"' || nextChar === "'") {
        lastEnd = i;
        break;
      }
    }
  }
  if (lastEnd > 0) return chunk.slice(0, lastEnd + 1).trim();
  const extendedChunk = text.slice(0, maxLen * 2);
  for (let i = maxLen; i < extendedChunk.length; i++) {
    if (extendedChunk[i] === "." || extendedChunk[i] === "!" || extendedChunk[i] === "?") {
      const nextChar = extendedChunk[i + 1];
      if (!nextChar || nextChar === " " || nextChar === '"' || nextChar === "'") {
        return extendedChunk.slice(0, i + 1).trim();
      }
    }
  }
  if (text.length <= maxLen) return text.trim();
  return "";
}

// ── CHANGED: Collapse file paths where filename matches parent directory ─────
// Docusaurus treats `linking/linking.md` as the index for `/docs/sites/linking`,
// not as `/docs/sites/linking/linking`. This function mirrors that behavior.
function fileToUrlPath(filePath, rootDir) {
  let rel = path.relative(rootDir, filePath).replace(/\\/g, "/");
  rel = rel.replace(/\.mdx?$/, "");

  // Strip trailing /index (explicit index files)
  if (rel === "index" || rel.endsWith("/index")) {
    rel = rel.replace(/\/?index$/, "") || "/";
  }

  // Collapse paths where filename matches parent directory name
  // e.g. "sites/linking/linking" → "sites/linking"
  const segments = rel.split("/");
  if (segments.length >= 2) {
    const filename = segments[segments.length - 1];
    const parentDir = segments[segments.length - 2];
    if (filename === parentDir) {
      segments.pop();
      rel = segments.join("/");
    }
  }

  return rel || "/";
}

function joinUrl(base, p) {
  if (!base) return "/" + p.replace(/^\//, "");
  return base.replace(/\/$/, "") + "/" + p.replace(/^\//, "");
}

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

  const docUrlPath = urlPath.startsWith("/docs")
    ? urlPath
    : "/docs" + (urlPath.startsWith("/") ? urlPath : "/" + urlPath);

  // CHANGED: Only build .md URL — no HTML duplicate
  const mdUrl = joinUrl(resolvedBaseUrl, docUrlPath) + ".md";

  if (isLinearUrl(mdUrl)) continue;

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
  const section = segments.length > 2
    ? toSectionTitle(segments[1])
    : segments.length > 1 && segments[1]
      ? toSectionTitle(segments[1])
      : "General";

  // CHANGED: Only store .md URL
  pages.push({ title: derivedTitle, mdUrl, description, section });
}

// ── Build llms.txt ────────────────────────────────────────────────────────────

const TARGET_CHARS = 120_000;

const sectionOrderRaw = [];
const grouped = {};
for (const page of pages) {
  if (!grouped[page.section]) {
    sectionOrderRaw.push(page.section);
    grouped[page.section] = [];
  }
  grouped[page.section].push(page);
}

const sectionOrder = [...sectionOrderRaw].sort((a, b) => {
  const ai = SECTION_PRIORITY.indexOf(a);
  const bi = SECTION_PRIORITY.indexOf(b);
  const aPri = ai === -1 ? 999 : ai;
  const bPri = bi === -1 ? 999 : bi;
  if (aPri !== bPri) return aPri - bPri;
  return a.localeCompare(b);
});

const introDesc = siteDesc ||
  "Walrus is a decentralized blob storage protocol built on Sui. " +
  "It provides robust, cost-effective storage for large binary objects " +
  "with high availability guarantees using erasure coding. " +
  "This documentation covers setup, usage, the client CLI, SDKs, " +
  "on-chain integration, and Walrus Sites (decentralized web hosting).";

const contextNotes = [
  "- Walrus stores data as **blobs** (immutable byte arrays). All blobs are public; use an encryption service like Seal for private data.",
  "- Walrus has two networks: **Mainnet** (production, uses real SUI/WAL tokens) and **Testnet** (for development and testing).",
  "- The `walrus` CLI is the primary client. It supports storing, reading, and managing blobs, and can run as a local daemon exposing an HTTP API.",
  "- **Walrus Sites** enable fully decentralized web hosting: static assets stored on Walrus, with a Sui smart contract as the on-chain index.",
  "- Costs involve WAL tokens (for storage) and SUI tokens (for on-chain transactions).",
];

const OPTIONAL_SECTIONS = new Set(["Design", "Dev Guide", "Legal", "Tusky Migration Guide"]);

// CHANGED: Single .md entry per page (no HTML duplicate)
function formatEntry({ title, mdUrl, description }) {
  return description
    ? `- [${title}](${mdUrl}): ${description}`
    : `- [${title}](${mdUrl})`;
}

function formatEntryCompact({ title, mdUrl }) {
  return `- [${title}](${mdUrl})`;
}

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

const requiredSections = sectionOrder.filter((s) => !OPTIONAL_SECTIONS.has(s));
const optionalSections = sectionOrder.filter((s) => OPTIONAL_SECTIONS.has(s));

function buildOutput(includeDescriptions, includeOptional) {
  const lines = [`# ${resolvedName}`, ""];

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

  lines.push("Important notes:", "");
  for (const note of contextNotes) {
    lines.push(...wrapLine(note, 0));
  }
  lines.push("");

  for (const section of requiredSections) {
    lines.push(`## ${section}`, "");
    for (const page of grouped[section]) {
      // CHANGED: formatEntry now returns a single string, not an array
      const entry = includeDescriptions ? formatEntry(page) : formatEntryCompact(page);
      lines.push(...wrapLine(entry, 0));
    }
    lines.push("");
  }

  if (includeOptional && optionalSections.length > 0) {
    lines.push("## Optional", "");
    for (const section of optionalSections) {
      lines.push(`### ${section}`, "");
      for (const page of grouped[section]) {
        const entry = includeDescriptions ? formatEntry(page) : formatEntryCompact(page);
        lines.push(...wrapLine(entry, 0));
      }
      lines.push("");
    }
  }

  return lines.join("\n");
}

let output = buildOutput(true, true);

if (output.length > TARGET_CHARS) {
  output = buildOutput(true, false);
}

if (output.length > TARGET_CHARS) {
  output = buildOutput(false, false);
}

if (output.length > TARGET_CHARS) {
  const ratio = TARGET_CHARS / output.length;
  const finalLines = [`# ${resolvedName}`, ""];
  finalLines.push(...wrapLine(`> ${introDesc}`, 0), "");
  for (const section of requiredSections) {
    const sectionPages = grouped[section];
    const keep = Math.max(1, Math.floor(sectionPages.length * ratio));
    finalLines.push(`## ${section}`, "");
    for (const page of sectionPages.slice(0, keep)) {
      for (const entry of formatEntry(page)) {
        finalLines.push(...wrapLine(entry, 0));
      }
    }
    finalLines.push("");
  }
  output = finalLines.join("\n");
}

const outDir = path.dirname(path.resolve(outputFile));
fs.mkdirSync(outDir, { recursive: true });

fs.writeFileSync(outputFile, output, "utf8");
console.log(`✓ Generated ${outputFile} with ${pages.length} pages across ${sectionOrder.length} sections (${output.length.toLocaleString()} chars)`);
