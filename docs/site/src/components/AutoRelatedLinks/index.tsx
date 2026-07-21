// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// Generates a "Related topics" card grid by resolving internal links from
// the page's rendered markdown body against the descriptions plugin metadata.
//
// Injected globally via the DocItem/Layout theme wrapper — no MDX imports needed.
//
// Rules:
//  - Does NOT render if the page has a <DocCardList /> or .next-steps-module
//  - Prefers internal docs links; skips external links
//  - Deduplicates by normalized path AND by resolved title
//  - Maximum of 4 cards by default

import React, { useEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import Link from "@docusaurus/Link";
import { usePluginData } from "@docusaurus/useGlobalData";
import { useLocation } from "@docusaurus/router";

// ── Types ─────────────────────────────────────────────────────────────────────

type Meta = {
  id: string;
  path?: string;
  title?: string;
  description?: string;
  href?: string;
};

type ResolvedLink = {
  href: string;
  title: string;
  description?: string;
  external: boolean;
  score: number;
};

// ── Canonical casing ──────────────────────────────────────────────────────────

const SPECIAL_CASING = new Map<string, string>([
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

function toTitleCase(str: string): string {
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

function humanize(href: string): string {
  const seg = href.replace(/\/$/, "").split("/").filter(Boolean).pop() ?? href;
  return toTitleCase(seg.split(/[-_]/).join(" "));
}

// ── Path helpers ──────────────────────────────────────────────────────────────

function normalizePath(raw: string): string {
  let p = raw.split("#")[0].split("?")[0].replace(/\.(mdx?|MDX?)$/, "");
  if (p.endsWith("/")) p = p.slice(0, -1);
  return (p || "/").toLowerCase();
}

function isExternal(url: string): boolean {
  return /^https?:\/\//i.test(url);
}

// ── Link validation ───────────────────────────────────────────────────────────

function isValidInternalLink(href: string): boolean {
  if (!href) return false;
  if (/^(#|mailto:|tel:|javascript:|data:)/i.test(href)) return false;
  if (isExternal(href)) return false;
  const path = href.split("#")[0].split("?")[0];
  if (!path || path === "/") return false;
  if (/\.(png|jpe?g|gif|svg|webp|pdf|zip|tar|gz|exe|dmg|pkg|css|js|woff2?)$/i.test(path))
    return false;
  return true;
}

function containsCodeArtifacts(text: string): boolean {
  if (/[{}()<>;=]|=>/.test(text)) return true;
  if (/^\s*(export|import|const|let|var|function|class|return)\s/i.test(text)) return true;
  if (/^https?:\/\//i.test(text)) return true;
  if (/&[a-z]+;|&#\d+;/i.test(text)) return true;
  if ((text.match(/[^a-zA-Z0-9\s.,!?:;'"()-]/g) ?? []).length > text.length * 0.15) return true;
  return false;
}

const GENERIC_TEXT = new Set([
  "here", "link", "this", "click here", "read more", "more", "source",
  "reference", "details", "learn more", "see more", "release", "releases",
  "changelog", "docs", "documentation", "example", "examples", "github",
  "github.com", "download", "downloads", "repository", "repo",
]);

function isCleanTitle(text: string): boolean {
  if (!text || text.length < 3 || text.length > 120) return false;
  if (containsCodeArtifacts(text)) return false;
  if (GENERIC_TEXT.has(text.toLowerCase().trim())) return false;
  if (/^(from|on|in|at|see|the |this |that )\s/i.test(text.toLowerCase())) return false;
  return true;
}

function isCleanDescription(text: string): boolean {
  if (!text || text.length < 10 || text.length > 300) return false;
  if (containsCodeArtifacts(text)) return false;
  return true;
}

// ── Metadata resolution ───────────────────────────────────────────────────────

function useMetaSafe(): Meta[] | null {
  try {
    const raw = usePluginData("sui-description-plugin") as any;
    if (!raw) return null;
    if (Array.isArray(raw)) return raw as Meta[];
    if (Array.isArray(raw?.items)) return raw.items as Meta[];
    if (Array.isArray(raw?.descriptions)) return raw.descriptions as Meta[];
    if (Array.isArray(raw?.data)) return raw.data as Meta[];
    if (raw?.byId && typeof raw.byId === "object")
      return Object.values(raw.byId) as Meta[];
    if (typeof raw === "object") {
      const vals = Object.values(raw);
      if (vals.length && typeof vals[0] === "object" && "id" in (vals[0] as any))
        return vals as Meta[];
    }
    return null;
  } catch {
    return null;
  }
}

function resolveMeta(href: string, data: Meta[] | null): Meta | undefined {
  if (!data) return undefined;
  const norm = normalizePath(href);
  const noLead = norm.startsWith("/") ? norm.slice(1) : norm;
  const withLead = norm.startsWith("/") ? norm : "/" + norm;
  return data.find(
    (m) =>
      (m.id?.toLowerCase() === noLead) ||
      (m.id?.toLowerCase() === withLead) ||
      (m.path?.toLowerCase() === norm) ||
      (m.path?.toLowerCase() === withLead),
  );
}

// ── Link collection ──────────────────────────────────────────────────────────

function shouldSkipPage(): boolean {
  if (document.querySelector(".next-steps-module") !== null) return true;
  if (document.querySelector("[class*='docCardList']") !== null) return true;
  return false;
}

function collectLinks(
  articleEl: Element,
  currentPath: string,
  meta: Meta[] | null,
): ResolvedLink[] {
  const seenPaths = new Set<string>();
  const seenTitles = new Set<string>();
  const results: ResolvedLink[] = [];

  const contentArea = articleEl.querySelector(".theme-doc-markdown") ?? articleEl;

  for (const a of Array.from(contentArea.querySelectorAll("a[href]"))) {
    const href = a.getAttribute("href") ?? "";

    if (!isValidInternalLink(href)) continue;

    const normalized = normalizePath(href);

    if (normalized === normalizePath(currentPath)) continue;

    if (seenPaths.has(normalized)) continue;

    const resolved = resolveMeta(normalized, meta);
    let title: string | undefined;
    let description: string | undefined;

    const candidates = [
      resolved?.title,
      a.textContent?.trim(),
      humanize(normalized),
    ];

    for (const raw of candidates) {
      if (raw && isCleanTitle(raw)) {
        title = toTitleCase(raw);
        break;
      }
    }

    if (!title) continue;

    if (resolved?.description && isCleanDescription(resolved.description)) {
      description = resolved.description;
    }

    const titleKey = title.toLowerCase();
    if (seenTitles.has(titleKey)) continue;

    seenPaths.add(normalized);
    seenTitles.add(titleKey);

    let score = resolved?.title ? 10 : 5;

    if (description) score += 2;

    results.push({
      href: normalized,
      title,
      description,
      external: false,
      score,
    });
  }

  return results;
}

// ── Card component ────────────────────────────────────────────────────────────

function LinkCard({ link }: { link: ResolvedLink }) {
  return (
    <Link to={link.href}>
      <div className="card__header">
        <span className="card__title">{link.title}</span>
      </div>
      {link.description && link.description !== link.title && (
        <div className="card__copy">{link.description}</div>
      )}
    </Link>
  );
}

// ── Main component ────────────────────────────────────────────────────────────

interface AutoRelatedLinksProps {
  title?: string;
  description?: string;
  maxLinks?: number;
  contentSelector?: string;
}

export default function AutoRelatedLinks({
  title = "Related topics",
  description,
  maxLinks = 4,
  contentSelector = "article .theme-doc-markdown",
}: AutoRelatedLinksProps) {
  const meta = useMetaSafe();
  const { pathname } = useLocation();
  const [links, setLinks] = useState<ResolvedLink[]>([]);
  const [portalTarget, setPortalTarget] = useState<Element | null>(null);
  const containerRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    const timer = setTimeout(() => {
      const article = document.querySelector("article");
      const content = document.querySelector(contentSelector);
      if (!article || !content) return;
      if (shouldSkipPage()) return;

      const collected = collectLinks(article, pathname, meta);

      collected.sort((a, b) => b.score - a.score);
      setLinks(collected.slice(0, maxLinks));

      let mount = content.querySelector<HTMLElement>(".auto-related-links-mount");
      if (!mount) {
        mount = document.createElement("div");
        mount.className = "auto-related-links-mount";
        content.appendChild(mount);
      }
      containerRef.current = mount as HTMLDivElement;
      setPortalTarget(mount);
    }, 100);

    return () => {
      clearTimeout(timer);
      containerRef.current?.remove();
      containerRef.current = null;
      setPortalTarget(null);
    };
  }, [pathname, meta, maxLinks, contentSelector]);

  if (links.length === 0 || !portalTarget) return null;

  return createPortal(
    <div className="next-steps-module">
      <div className="next-steps-header">
        <h3>{title}</h3>
      </div>
      {description && (
        <p className="next-steps-description">{description}</p>
      )}
      <div className="next-steps-grid">
        {links.map((link) => (
          <LinkCard key={link.href} link={link} />
        ))}
      </div>
    </div>,
    portalTarget,
  );
}
