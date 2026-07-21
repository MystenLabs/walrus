// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// Remark plugin that injects a "Related topics" card grid at build time.
//
// For each doc page, it:
//  1. Scans the markdown AST for internal link nodes
//  2. Resolves each link against a pre-built metadata map (titles + descriptions)
//  3. Scores, deduplicates, and picks the top N links
//  4. Injects an mdxJsxFlowElement section at the end of the AST
//
// Because this runs during MDX compilation, the output is part of the static
// HTML — visible to search engines, LLM crawlers, and users without JS.

import { visit } from "unist-util-visit";
import path from "path";
import fs from "fs";
import matter from "gray-matter";

// ── File walking ────────────────────────────────────────────────────────────

function walkDir(dir, filter) {
  const out = [];
  try {
    const entries = fs.readdirSync(dir, { withFileTypes: true });
    for (const e of entries) {
      const abs = path.join(dir, e.name);
      try {
        const st = fs.statSync(abs);
        if (st.isDirectory()) out.push(...walkDir(abs, filter));
        else if (st.isFile() && filter(abs)) out.push(abs);
      } catch {
        continue;
      }
    }
  } catch {
    // ignore unreadable dirs
  }
  return out;
}

// ── Path helpers ────────────────────────────────────────────────────────────

function computeRoute(docsRoot, fileAbs) {
  const rel = path.relative(docsRoot, fileAbs).replace(/\\/g, "/");
  const noExt = rel.replace(/\.(md|mdx|markdown)$/i, "");
  if (noExt.endsWith("/index")) return `/${noExt.slice(0, -"/index".length)}`;
  return `/${noExt}`;
}

function normalizePath(raw) {
  let p = raw.split("#")[0].split("?")[0].replace(/\.(mdx?|MDX?)$/, "");
  if (p.endsWith("/")) p = p.slice(0, -1);
  return (p || "/").toLowerCase();
}

function firstParagraph(body) {
  const lines = body.split(/\r?\n/);
  const buf = [];
  for (const raw of lines) {
    const s = raw.trim();
    if (!s) {
      if (buf.length) break;
      continue;
    }
    if (/^import\s+/.test(s)) continue;
    if (/^#{1,6}\s/.test(s)) continue;
    if (/^{\s*@\w+:\s*.+}\s*$/.test(s)) continue;
    if (!/^[a-zA-Z]/.test(s)) continue;
    buf.push(s);
  }
  let paragraph = buf.join(" ").trim();
  if (!paragraph) return "";
  paragraph = paragraph
    .replace(/\[([^\]]+)]\([^)]+\)/g, "$1")
    .replace(/`([^`]+)`/g, "$1")
    .replace(/<[^>]+>/g, "")
    .replace(/\s+/g, " ")
    .trim();
  return paragraph;
}

// ── Title casing ────────────────────────────────────────────────────────────

const SPECIAL_CASING = new Map([
  ["grpc", "gRPC"], ["graphql", "GraphQL"], ["webrtc", "WebRTC"],
  ["websocket", "WebSocket"], ["devnet", "Devnet"], ["testnet", "Testnet"],
  ["mainnet", "Mainnet"], ["localnet", "Localnet"],
  ["sui", "Sui"], ["move", "Move"], ["walrus", "Walrus"],
  ["npm", "npm"], ["pnpm", "pnpm"], ["docker", "docker"],
  ["curl", "curl"], ["cargo", "cargo"], ["git", "git"],
]);

const LOWERCASE_WORDS = new Set([
  "a", "an", "the", "and", "but", "or", "nor", "for", "so", "yet",
  "at", "by", "in", "of", "on", "to", "up", "as", "is", "it",
  "via", "vs", "with", "from", "into", "onto", "over", "than", "upon",
]);

function toTitleCase(str) {
  return str.trim().split(/\s+/).map((word, i, arr) => {
    const canonical = SPECIAL_CASING.get(word.toLowerCase());
    if (canonical !== undefined) return canonical;
    if (i === 0 || i === arr.length - 1)
      return word.charAt(0).toUpperCase() + word.slice(1);
    if (word === word.toUpperCase() && word.length > 1) return word;
    if (/[A-Z]/.test(word.slice(1))) return word;
    const lower = word.toLowerCase();
    return LOWERCASE_WORDS.has(lower)
      ? lower
      : lower.charAt(0).toUpperCase() + lower.slice(1);
  }).join(" ");
}

function humanize(href) {
  const seg = href.replace(/\/$/, "").split("/").filter(Boolean).pop() || href;
  return toTitleCase(seg.split(/[-_]/).join(" "));
}

// ── Validation ──────────────────────────────────────────────────────────────

const GENERIC_TEXT = new Set([
  "here", "link", "this", "click here", "read more", "more", "source",
  "reference", "details", "learn more", "see more", "release", "releases",
  "changelog", "docs", "documentation", "example", "examples", "github",
  "github.com", "download", "downloads", "repository", "repo",
]);

function containsCodeArtifacts(text) {
  if (/[{}()<>;=]|=>/.test(text)) return true;
  if (/^\s*(export|import|const|let|var|function|class|return)\s/i.test(text)) return true;
  if (/^https?:\/\//i.test(text)) return true;
  if (/&[a-z]+;|&#\d+;/i.test(text)) return true;
  if ((text.match(/[^a-zA-Z0-9\s.,!?:;'"()-]/g) || []).length > text.length * 0.15) return true;
  return false;
}

function isCleanTitle(text) {
  if (!text || text.length < 3 || text.length > 120) return false;
  if (containsCodeArtifacts(text)) return false;
  if (GENERIC_TEXT.has(text.toLowerCase().trim())) return false;
  return true;
}

function isCleanDescription(text) {
  if (!text || text.length < 10 || text.length > 300) return false;
  if (containsCodeArtifacts(text)) return false;
  return true;
}

// ── AST text extraction ─────────────────────────────────────────────────────

function getTextContent(node) {
  if (node.type === "text") return node.value || "";
  if (node.type === "inlineCode") return node.value || "";
  if (!node.children) return "";
  return node.children.map(getTextContent).join("");
}

// ── AST node builders ───────────────────────────────────────────────────────

function jsxAttr(name, value) {
  return { type: "mdxJsxAttribute", name, value };
}

function jsxFlow(name, attributes, children) {
  return {
    type: "mdxJsxFlowElement",
    name,
    attributes,
    children,
    data: { _mdxExplicitJsx: true },
  };
}

function jsxText(name, attributes, children) {
  return {
    type: "mdxJsxTextElement",
    name,
    attributes,
    children,
    data: { _mdxExplicitJsx: true },
  };
}

function textNode(value) {
  return { type: "text", value };
}

function buildCardNode(link, routeBasePath) {
  const href = `/${routeBasePath}${link.href}`;

  const headerChildren = [
    jsxText("span", [jsxAttr("className", "card__title")], [
      textNode(link.title),
    ]),
  ];

  const cardChildren = [
    jsxFlow("div", [jsxAttr("className", "card__header")], headerChildren),
  ];

  if (link.description && link.description !== link.title) {
    cardChildren.push(
      jsxFlow("div", [jsxAttr("className", "card__copy")], [
        textNode(link.description),
      ]),
    );
  }

  return jsxFlow("a", [jsxAttr("href", href)], cardChildren);
}

function buildRelatedTopicsSection(links, sectionTitle, routeBasePath) {
  const cards = links.map((l) => buildCardNode(l, routeBasePath));

  return jsxFlow("div", [jsxAttr("className", "next-steps-module")], [
    jsxFlow("div", [jsxAttr("className", "next-steps-header")], [
      jsxFlow("h3", [], [textNode(sectionTitle)]),
    ]),
    jsxFlow("div", [jsxAttr("className", "next-steps-grid")], cards),
  ]);
}

// ── Plugin ──────────────────────────────────────────────────────────────────

export default function remarkRelatedTopics(options = {}) {
  const {
    docsDir,
    maxLinks = 4,
    title = "Related topics",
    routeBasePath = "docs",
  } = options;

  // Build the metadata map once at plugin initialization
  const metaMap = new Map();

  if (docsDir && fs.existsSync(docsDir)) {
    const files = walkDir(docsDir, (abs) =>
      /\.(md|mdx|markdown)$/i.test(abs),
    );

    for (const file of files) {
      try {
        const raw = fs.readFileSync(file, "utf8");
        const parsed = matter(raw);
        if (parsed.data.draft) continue;

        const route = computeRoute(docsDir, file);
        const normalized = normalizePath(route);
        const desc =
          typeof parsed.data.description === "string"
            ? parsed.data.description.trim()
            : firstParagraph(parsed.content);

        metaMap.set(normalized, {
          title: parsed.data.title || humanize(route),
          description: desc,
        });
      } catch {
        continue;
      }
    }
  }

  return (tree, file) => {
    // Determine the current page's route from the VFile path
    if (!file.path || !docsDir) return;

    const currentRoute = computeRoute(docsDir, file.path);
    const currentNorm = normalizePath(currentRoute);

    // Skip pages that contain <DocCardList />
    let hasDocCardList = false;
    visit(tree, "mdxJsxFlowElement", (node) => {
      if (node.name === "DocCardList") hasDocCardList = true;
    });
    if (hasDocCardList) return;

    // Collect internal links from the AST
    const seenPaths = new Set();
    const seenTitles = new Set();
    const links = [];

    visit(tree, "link", (node) => {
      const href = node.url;
      if (!href) return;

      // Skip external, anchor-only, and non-doc links
      if (/^(https?:\/\/|#|mailto:|tel:|javascript:|data:)/i.test(href))
        return;
      if (
        /\.(png|jpe?g|gif|svg|webp|pdf|zip|tar|gz|css|js|woff2?)$/i.test(href)
      )
        return;

      // Resolve the link to a normalized route
      let resolved;
      if (href.startsWith("/")) {
        // Absolute path — strip routeBasePath prefix (e.g., "/docs/foo" → "/foo")
        const prefix = `/${routeBasePath}/`;
        if (href.toLowerCase().startsWith(prefix)) {
          resolved = normalizePath("/" + href.slice(prefix.length));
        } else if (href.toLowerCase() === `/${routeBasePath}`) {
          resolved = "/";
        } else {
          // Absolute path not under routeBasePath — skip
          return;
        }
      } else {
        // Relative path — resolve against current file's directory
        const currentDir = path.dirname(currentRoute);
        const joined = path.posix.join(currentDir, href);
        resolved = normalizePath(joined);
      }

      // Skip self-links
      if (resolved === currentNorm) return;

      // Skip duplicates by path
      if (seenPaths.has(resolved)) return;

      // Look up metadata
      const meta = metaMap.get(resolved);

      // Determine title
      const linkText = getTextContent(node).trim();
      const candidates = [meta?.title, linkText, humanize(resolved)];
      let linkTitle;
      for (const raw of candidates) {
        if (raw && isCleanTitle(raw)) {
          linkTitle = toTitleCase(raw);
          break;
        }
      }
      if (!linkTitle) return;

      // Skip duplicate titles
      const titleKey = linkTitle.toLowerCase();
      if (seenTitles.has(titleKey)) return;

      seenPaths.add(resolved);
      seenTitles.add(titleKey);

      // Determine description
      let description;
      if (meta?.description && isCleanDescription(meta.description)) {
        description = meta.description;
      }

      // Score: prefer links with resolved metadata
      let score = meta?.title ? 10 : 5;
      if (description) score += 2;

      links.push({ href: resolved, title: linkTitle, description, score });
    });

    if (links.length === 0) return;

    // Sort by score descending, take top N
    links.sort((a, b) => b.score - a.score);
    const topLinks = links.slice(0, maxLinks);

    // Inject the section at the end of the AST
    tree.children.push(
      buildRelatedTopicsSection(topLinks, title, routeBasePath),
    );
  };
}
