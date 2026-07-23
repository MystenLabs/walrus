// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0
//
// Transforms fetched Walrus Memory (MemWal) docs from Mintlify format into
// Docusaurus-compatible MDX.
//
// Transforms applied:
//   1. Mintlify admonitions (<Note>, <Warning>, <Tip>, <Info>) → :::note etc.
//   2. <CardGroup>/<Card> → <Cards>/<Card> (existing Walrus components)
//   3. <CodeGroup> → <Tabs>/<TabItem>
//   4. <Steps>/<Step> → ordered list items
//   5. <Tabs>/<Tab> → <Tabs>/<TabItem>
//   6. <Accordion> → <details>/<summary>
//   7. Internal links rewritten with /walrus-memory/ prefix
//   8. "MemWal" → "Walrus Memory" in prose (not code)
//   9. Frontmatter title/description preserved, slug added

const fs = require("fs");
const path = require("path");

const SITE_ROOT = path.resolve(__dirname, "../../");
const CACHE_DIR = path.join(SITE_ROOT, ".cache-walrus-memory/docs");
const OUTPUT_DIR = path.resolve(SITE_ROOT, "../walrus-memory-content");

// Known pages and rename mappings, built dynamically from cache files.
// knownPageSet: all page slugs (post-rename) that exist in the cache.
// slugRenameMap: original slug → renamed slug for files in RENAME_MAP.
let knownPageSet = new Set();
let slugRenameMap = {};

function buildKnownPages(cacheDir) {
  const files = collectFiles(cacheDir);
  const skip = new Set([
    "docs.json", "package.json", "docs.Dockerfile", "llms.txt", "llms-full.txt",
  ]);

  for (const { relPath } of files) {
    if (skip.has(path.basename(relPath))) continue;
    if (relPath.startsWith("src/")) continue;

    const originalSlug = relPath.replace(/\.(mdx?)$/, "");
    const renamedPath = RENAME_MAP[relPath];
    if (renamedPath) {
      const renamedSlug = renamedPath.replace(/\.(mdx?)$/, "");
      knownPageSet.add(renamedSlug);
      slugRenameMap[originalSlug] = renamedSlug;
    } else {
      knownPageSet.add(originalSlug);
    }
  }
}

// --- File rename map (remote filename → local filename) ---
const RENAME_MAP = {
  "getting-started/what-is-memwal.md": "getting-started/what-is-walrus-memory.md",
};

// --- Mintlify component transforms ---

function convertAdmonitions(content) {
  // <Note>...</Note> → :::note ... :::
  // <Warning>...</Warning> → :::warning ... :::
  // <Tip>...</Tip> → :::tip ... :::
  // <Info>...</Info> → :::info ... :::
  const admonitionTypes = {
    Note: "note",
    Warning: "warning",
    Tip: "tip",
    Info: "info",
  };

  let result = content;
  for (const [tag, type] of Object.entries(admonitionTypes)) {
    // Handle self-closing and content variants
    const re = new RegExp(
      `<${tag}>\\s*\\n?([\\s\\S]*?)\\n?\\s*<\\/${tag}>`,
      "gi",
    );
    result = result.replace(re, (_, inner) => {
      return `:::${type}\n${inner.trim()}\n:::`;
    });
  }
  return result;
}

function convertCardGroups(content) {
  // <CardGroup cols={N}> → <Cards>
  // </CardGroup> → </Cards>
  let result = content.replace(/<CardGroup[^>]*>/gi, "<Cards>");
  result = result.replace(/<\/CardGroup>/gi, "</Cards>");
  return result;
}

function convertCards(content) {
  // Cards WITH href: keep as <Card> (Walrus component requires href)
  // Cards WITHOUT href: convert to plain HTML div (decorative feature cards)

  // First, handle full <Card ...>body</Card> blocks
  return content.replace(
    /<Card\s+([^>]*?)>([\s\S]*?)<\/Card>/gi,
    (match, attrs, body) => {
      // Remove icon="..." attribute
      let cleaned = attrs.replace(/\s*icon\s*=\s*"[^"]*"/gi, "").trim();

      const hasHref = /href\s*=\s*"/i.test(cleaned);

      if (hasHref) {
        // Rewrite href to add prefix for internal paths
        cleaned = cleaned.replace(
          /href\s*=\s*"\/([^"]*?)"/gi,
          (_, p) => `href="/walrus-memory/${p}"`,
        );
        return `<Card ${cleaned}>${body}</Card>`;
      }

      // No href — convert to a styled div
      const titleMatch = /title\s*=\s*"([^"]+)"/i.exec(cleaned);
      const title = titleMatch ? titleMatch[1] : "";
      const trimmedBody = body.trim();
      return `<div className="feature-card">\n\n**${title}**\n\n${trimmedBody}\n\n</div>`;
    },
  );
}

function convertCodeGroups(content) {
  // <CodeGroup> containing multiple fenced code blocks with titles
  // → <Tabs groupId="code"> with <TabItem> wrappers
  return content.replace(
    /<CodeGroup>\s*\n([\s\S]*?)\n\s*<\/CodeGroup>/gi,
    (_, inner) => {
      // Split the inner content by code fences
      const blocks = [];
      const codeBlockRe = /```(\w+)\s+(.*?)\n([\s\S]*?)```/g;
      let m;
      while ((m = codeBlockRe.exec(inner))) {
        const lang = m[1];
        const label = m[2].trim() || lang;
        const code = m[3];
        blocks.push({ lang, label, code });
      }

      if (blocks.length === 0) {
        // No code blocks found, just return inner content wrapped in tabs
        return `<Tabs>\n${inner}\n</Tabs>`;
      }

      const tabItems = blocks
        .map(
          (b) =>
            `<TabItem value="${b.label}" label="${b.label}">\n\n\`\`\`${b.lang}\n${b.code}\`\`\`\n\n</TabItem>`,
        )
        .join("\n");

      // Detect groupId from tab labels
      const labels = blocks.map((b) => b.label.toLowerCase());
      const groupId = detectTabGroupId(labels);

      return `<Tabs groupId="${groupId}">\n${tabItems}\n</Tabs>`;
    },
  );
}

function detectTabGroupId(labels) {
  const pkgManagers = ["npm", "pnpm", "yarn", "bun"];
  const isPkgManager = labels.some((l) => pkgManagers.includes(l));
  if (isPkgManager) return "pkg-manager";

  const transports = ["http", "stdio"];
  const isTransport = labels.some((l) => transports.includes(l));
  if (isTransport) return "transport";

  return "code";
}

function convertTabs(content) {
  // <Tabs> stays as <Tabs>
  // <Tab title="X"> → <TabItem value="X" label="X">
  // </Tab> → </TabItem>
  let result = content.replace(
    /<Tab\s+title\s*=\s*"([^"]+)"[^>]*>/gi,
    '<TabItem value="$1" label="$1">',
  );
  result = result.replace(/<\/Tab>/gi, "</TabItem>");

  // Add groupId to <Tabs> that don't have one, based on their TabItem labels
  result = result.replace(
    /<Tabs>([\s\S]*?)<\/Tabs>/gi,
    (match, inner) => {
      if (match.includes("groupId")) return match;
      const labels = [];
      const labelRe = /label="([^"]+)"/gi;
      let m;
      while ((m = labelRe.exec(inner))) labels.push(m[1].toLowerCase());
      if (labels.length === 0) return match;
      const groupId = detectTabGroupId(labels);
      return `<Tabs groupId="${groupId}">${inner}</Tabs>`;
    },
  );

  return result;
}

function convertSteps(content) {
  // Remove <Steps> and </Steps> wrappers
  let result = content.replace(/<Steps>\s*\n?/gi, "");
  result = result.replace(/\n?\s*<\/Steps>/gi, "");

  // Pattern 1: <Step title="X">body</Step>
  result = result.replace(
    /<Step\s+title\s*=\s*"([^"]+)"[^>]*>\s*\n?([\s\S]*?)\n?\s*<\/Step>/gi,
    (_, title, body) => `### ${title}\n\n${body.trim()}\n`,
  );

  // Pattern 2: <Step> (no title attribute) — body contains ### heading
  // Just strip the <Step> and </Step> tags, leaving the body content
  result = result.replace(/<Step>\s*\n?/gi, "");
  result = result.replace(/\n?\s*<\/Step>/gi, "");

  return result;
}

function convertAccordions(content) {
  // <Accordion title="X">body</Accordion> → <details><summary>X</summary>body</details>
  return content.replace(
    /<Accordion\s+title\s*=\s*"([^"]+)"[^>]*>\s*\n?([\s\S]*?)\n?\s*<\/Accordion>/gi,
    (_, title, body) =>
      `<details>\n<summary>${title}</summary>\n\n${body.trim()}\n\n</details>`,
  );
}

// --- Link rewriting ---

function rewriteInternalLinks(content) {
  // Rewrite absolute internal links like [text](/sdk/quick-start)
  // to [text](/walrus-memory/sdk/quick-start)
  // Only rewrite if the path matches a known page (or a renamed original)
  return content.replace(
    /(\[([^\]]*)\]\()\/([^)#\s]+)(#[^)\s]*)?\)/g,
    (match, prefix, text, pagePath, anchor) => {
      const cleanPath = pagePath.replace(/\/$/, "");
      if (knownPageSet.has(cleanPath)) {
        return `${prefix}/walrus-memory/${cleanPath}${anchor || ""})`;
      }
      const renamed = slugRenameMap[cleanPath];
      if (renamed) {
        return `${prefix}/walrus-memory/${renamed}${anchor || ""})`;
      }
      // Not a known page — leave as-is (could be an external docs link)
      return match;
    },
  );
}

function rewriteCardHrefs(content) {
  // Card hrefs may have already been partially rewritten by convertCards
  // but some may use relative paths without leading /
  // This catches href="/path" that aren't already prefixed
  return content.replace(
    /href="\/(?!walrus-memory\/)([^"]+)"/gi,
    (match, p) => {
      const cleanPath = p.replace(/\/$/, "");
      if (knownPageSet.has(cleanPath)) {
        return `href="/walrus-memory/${cleanPath}"`;
      }
      const renamed = slugRenameMap[cleanPath];
      if (renamed) {
        return `href="/walrus-memory/${renamed}"`;
      }
      return match;
    },
  );
}

// --- Branding ---

function renameMemwal(content) {
  // Replace "MemWal" with "Walrus Memory" in prose text
  // Preserve in: code blocks, backticks, URLs, package names
  const lines = content.split("\n");
  let inCodeBlock = false;
  const result = [];

  for (const line of lines) {
    if (/^```/.test(line)) {
      inCodeBlock = !inCodeBlock;
      result.push(line);
      continue;
    }

    if (inCodeBlock) {
      result.push(line);
      continue;
    }

    // Replace MemWal outside of backticks
    // Split line by backtick segments
    const parts = line.split(/(`[^`]*`)/);
    const transformed = parts
      .map((part, i) => {
        // Odd indices are backtick-wrapped code — don't touch
        if (i % 2 === 1) return part;
        // Don't replace in URLs
        return part.replace(
          /\bMemWal\b/g,
          (m, offset, str) => {
            // Check if we're inside a URL
            const before = str.substring(0, offset);
            if (/https?:\/\/\S*$/.test(before)) return m;
            if (/github\.com\S*$/.test(before)) return m;
            return "Walrus Memory";
          },
        );
      })
      .join("");

    result.push(transformed);
  }

  return result.join("\n");
}

// --- Sui Documentation Style Guide enforcement ---

function enforceStyleGuide(content, relPath) {
  const lines = content.split("\n");
  let inCodeBlock = false;
  let inBashBlock = false;
  let inFrontmatter = false;
  let frontmatterCount = 0;
  const result = [];
  const sourceUrl = relPath
    ? `https://github.com/MystenLabs/MemWal/blob/dev/docs/${relPath}`
    : null;

  for (let i = 0; i < lines.length; i++) {
    let line = lines[i];

    if (/^---\s*$/.test(line)) {
      frontmatterCount++;
      if (frontmatterCount === 1) inFrontmatter = true;
      if (frontmatterCount === 2) inFrontmatter = false;
      result.push(line);
      continue;
    }

    if (inFrontmatter) {
      if (/^description:\s*"/.test(line)) {
        line = line.replace(/(description:\s*")([^"]*)(")/, (_, pre, desc, post) => {
          return pre + applyProseRules(desc) + post;
        });
      }
      // Replace ampersands in frontmatter title
      if (/^title:\s*"/.test(line)) {
        line = line.replace(/ & /g, " and ");
      }
      result.push(line);
      continue;
    }

    if (/^```/.test(line)) {
      if (!inCodeBlock) {
        inCodeBlock = true;
        inBashBlock = /^```(bash|shell|sh)\b/.test(line);
        // Convert Mintlify filename convention (```lang filename.ext)
        // to Docusaurus title attribute (```lang title="filename.ext")
        const fenceMatch = line.match(/^```(\w+)\s+(\S+.+)$/);
        if (fenceMatch && !line.includes('title=')) {
          const lang = fenceMatch[1];
          const filename = fenceMatch[2].trim();
          line = `\`\`\`${lang} title="${filename}"`;
        }
        // Add source link before every code block
        if (sourceUrl) {
          result.push(`<a href="${sourceUrl}" target="_blank" className="code-source-link">Source: ${relPath}</a>\n`);
        }
      } else {
        inCodeBlock = false;
        inBashBlock = false;
      }
      result.push(line);
      continue;
    }

    if (inCodeBlock && !inBashBlock) {
      result.push(line);
      continue;
    }

    // Inside bash code blocks, add $ prefix to commands
    if (inBashBlock) {
      if (line.trim() && !line.startsWith("$") && !line.startsWith("#") && !line.startsWith("//") &&
          !line.startsWith(" ") && !line.startsWith("\t") && !/^[A-Z_]+=/.test(line) &&
          !/^\d/.test(line.trim()) && !/^[{[\(]/.test(line.trim()) && !/^\.\.\./.test(line.trim())) {
        line = `$ ${line}`;
      }
    }

    if (!inBashBlock) {
      line = applyStyleRulesToLine(line);
    }

    if (/^#{2,6}\s+/.test(line)) {
      line = toSentenceCaseHeading(line);
    }

    result.push(line);
  }

  return result.join("\n");
}

function applyStyleRulesToLine(line) {
  const parts = line.split(/(`[^`]*`)/);
  const transformed = parts.map((part, i) => {
    if (i % 2 === 1) return part;
    return applyProseRules(part);
  });
  let result = transformed.join("");

  // Fix em dash → comma artifacts in table cells (| , | → |  |)
  result = result.replace(/\|\s*,\s*\|/g, "| |");
  // Also fix em dashes that survived in table cells (| — | should be blank)
  result = result.replace(/\|\s*—\s*\|/g, "| |");

  return result;
}

function applyProseRules(text) {
  let result = text;

  // Latin abbreviations
  result = result.replace(/\be\.g\.\s*/gi, "for example, ");
  result = result.replace(/\bi\.e\.\s*/gi, "that is, ");
  result = result.replace(/\betc\.\s*/gi, "and so on. ");
  result = result.replace(/\bet\.\s*al\.\s*/gi, "and others ");

  // Word preferences
  result = result.replace(/(?<![/\w])\bvia\b(?![/\w])/gi, "through");
  result = result.replace(/\bdApps?\b/g, (m) => (m.endsWith("s") ? "apps" : "app"));
  result = result.replace(/\bsimple\b/gi, (m) => {
    return m[0] === m[0].toUpperCase() ? "Basic" : "basic";
  });
  result = result.replace(/\bmay\b/g, "might");

  // Sui-specific capitalization
  result = result.replace(/\bmainnet\b/gi, "Mainnet");
  result = result.replace(/\btestnet\b/gi, "Testnet");
  result = result.replace(/\bdevnet\b/gi, "Devnet");
  result = result.replace(/\bon-chain\b/gi, "onchain");
  result = result.replace(/\boff-chain\b/gi, "offchain");
  result = result.replace(/\bkey-pair\b/gi, "key pair");
  // SEAL → Seal (product name, not acronym)
  result = result.replace(/\bSEAL\b/g, "Seal");

  // Remove exclamation marks at end of sentences
  result = result.replace(/([a-zA-Z])!(\s|$)/g, "$1.$2");

  // Replace em dashes
  result = result.replace(/\s*—\s*/g, ", ");

  // No ampersands in prose
  result = result.replace(/(?<![&\w])\s*&\s*(?![&\w])/g, " and ");

  // "since" (causal) → "because"
  result = result.replace(/(,\s+)[Ss]ince\s+/g, "$1because ");
  result = result.replace(/^\s*Since\s+(?!then|the\s+\d|19|20)/gm, "Because ");

  // Future tense "will" → present tense
  result = result.replace(/\bwill not\b/g, "does not");
  result = result.replace(/\bwill be\b/g, "is");
  result = result.replace(/\bwill have\b/g, "has");
  result = result.replace(/\bwill need\b/g, "needs");
  result = result.replace(/\bwill run\b/g, "runs");
  result = result.replace(/\bwill complete\b/g, "completes");
  result = result.replace(/\bwill pick\b/g, "picks");
  result = result.replace(/\bwill fail\b/g, "fails");
  result = result.replace(/\bwill return\b/g, "returns");
  result = result.replace(/\bwill use\b/g, "uses");
  result = result.replace(/\bwill see\b/g, "sees");
  result = result.replace(/\bwill receive\b/g, "receives");
  result = result.replace(/\bwill call\b/g, "calls");
  result = result.replace(/\bwill start\b/g, "starts");
  result = result.replace(/\bwill try\b/g, "tries");
  result = result.replace(/\bwill stay\b/g, "stays");
  result = result.replace(/\bwill send\b/g, "sends");
  result = result.replace(/\bwill appear\b/g, "appears");
  result = result.replace(/\bwill store\b/g, "stores");
  result = result.replace(/\bwill create\b/g, "creates");
  result = result.replace(/\bwill get\b/g, "gets");
  result = result.replace(/\bwill allow\b/g, "allows");
  result = result.replace(/\bwill enable\b/g, "enables");
  result = result.replace(/\bwill generate\b/g, "generates");
  result = result.replace(/\bwill automatically\b/g, "automatically");
  result = result.replace(/\bwill also\b/g, "also");
  result = result.replace(/\bwill guide\b/g, "guides");
  result = result.replace(/\bwill exit\b/g, "exits");
  result = result.replace(/\bwill include\b/g, "includes");
  result = result.replace(/\bwill work\b/g, "works");
  result = result.replace(/\bwill handle\b/g, "handles");
  result = result.replace(/\bwill look\b/g, "looks");
  result = result.replace(/\bwill contain\b/g, "contains");
  result = result.replace(/\bwill show\b/g, "shows");
  result = result.replace(/\bwill find\b/g, "finds");
  result = result.replace(/\bwill make\b/g, "makes");
  result = result.replace(/\bwill result\b/g, "results");
  result = result.replace(/\bwill produce\b/g, "produces");
  result = result.replace(/\bwill throw\b/g, "throws");
  result = result.replace(/\bwill log\b/g, "logs");
  result = result.replace(/\bwill print\b/g, "prints");
  result = result.replace(/\bwill output\b/g, "outputs");
  result = result.replace(/\bYou'll\b/g, "You");

  // First person "we" → second person or rephrase
  result = result.replace(/\bwe provide\b/gi, "the SDK provides");
  result = result.replace(/\bwe encourage\b/gi, "you should consider");
  result = result.replace(/\bwe also provide\b/gi, "the SDK also provides");
  result = result.replace(/\bwe recommend\b/gi, "consider");
  result = result.replace(/\bwe ship\b/gi, "the SDK ships");
  result = result.replace(/\bwe welcome\b/gi, "the team welcomes");
  result = result.replace(/\bwe continue\b/gi, "the team continues");
  result = result.replace(/\bwe don't\b/gi, "the SDK does not");
  result = result.replace(/\band we provide\b/gi, "and the SDK provides");
  result = result.replace(/\bwe ensure\b/gi, "this ensures");

  // "**Note:**" inline → remove the label (sentence should stand on its own)
  result = result.replace(/^\*\*Note:\*\*\s*/gm, "");

  // "info" as abbreviation → "information"
  result = result.replace(/\bmore info\b/gi, "more information");
  result = result.replace(/\bfor info\b/gi, "for information");

  return result;
}

function toSentenceCaseHeading(line) {
  const match = line.match(/^(#{2,6})\s+(.+)$/);
  if (!match) return line;
  const [, hashes, text] = match;

  const parts = text.split(/(`[^`]*`)/);
  const result = parts.map((part, i) => {
    if (i % 2 === 1) return part;
    if (i === 0) return sentenceCasePart(part, true);
    return sentenceCasePart(part, false);
  });

  return `${hashes} ${result.join("")}`;
}

const PRESERVE_CAPS = new Set([
  "Sui", "SUI", "Walrus", "Seal", "SEAL", "Mainnet", "Testnet", "Devnet",
  "Move", "Rust", "TypeScript", "JavaScript", "Python", "GraphQL", "gRPC",
  "JSON", "API", "APIs", "SDK", "SDKs", "CLI", "HTTP", "HTTPS", "URL",
  "URLs", "RPC", "PTB", "PTBs", "AES", "GCM", "DEK", "AAD", "MCP",
  "AI", "NFT", "NFTs", "DeFi", "ID", "IDs", "Ed25519", "TEE", "TLS",
  "REST", "Docker", "GitHub", "Vercel", "OpenClaw", "NemoClaw", "SuiNS",
  "Walrus Memory", "PostgreSQL", "Axum", "Node.js", "WAL", "STRIDE",
  "Seal",
]);

function sentenceCasePart(text, isFirst) {
  if (!text) return text;
  const words = text.split(/(\s+)/);
  let needsCap = isFirst; // Track if next real word should be capitalized
  return words.map((word, wi) => {
    if (/^\s+$/.test(word)) return word;
    if (PRESERVE_CAPS.has(word)) return word;
    if (/^[A-Z]{2,}$/.test(word)) return word;
    if (/[a-z][A-Z]/.test(word) || /^[A-Z][a-z]+[A-Z]/.test(word)) return word;
    // Numbered prefix like "1." — skip it and capitalize the next word
    if (/^\d+\.$/.test(word)) {
      needsCap = true;
      return word;
    }
    if ((isFirst && wi === 0) || needsCap) {
      needsCap = false;
      return word.charAt(0).toUpperCase() + word.slice(1).toLowerCase();
    }
    return word.toLowerCase();
  }).join("");
}

// --- Prerequisites tab conversion ---

function convertPrerequisites(content) {
  // Convert ## Prerequisites followed by a bullet list into the Walrus
  // docs outlined-tabs prerequisites component.
  return content.replace(
    /^(#{2,4})\s+Prerequisites\s*\n\n((?:- .+\n?)+)/gm,
    (_, hashes, listBlock) => {
      // Convert plain bullets to checkbox items
      const items = listBlock
        .trim()
        .split("\n")
        .map((line) => line.replace(/^- /, "- [x] "))
        .join("\n");

      return [
        `<div className="outlined-tabs">`,
        ``,
        `<Tabs>`,
        `<TabItem value="prereq" label="Prerequisites">`,
        ``,
        items,
        ``,
        `</TabItem>`,
        `</Tabs>`,
        ``,
        `</div>`,
        ``,
      ].join("\n");
    },
  );
}

// --- Fix stacked headings ---

function fixStackedHeadings(content) {
  // Stacked headings (## Foo\n\n### Bar with no body text between)
  // violate the style guide. Remove the parent heading if it has
  // no content before the next heading — the child heading is enough.
  const lines = content.split("\n");
  const result = [];

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    // Check if this is a heading
    if (/^#{2,6}\s+/.test(line)) {
      // Look ahead: skip blank lines and check if next non-blank is also a heading
      let j = i + 1;
      while (j < lines.length && lines[j].trim() === "") j++;
      if (j < lines.length && /^#{2,6}\s+/.test(lines[j])) {
        // This heading is followed by another heading with no body — skip it
        continue;
      }
    }
    result.push(line);
  }

  return result.join("\n");
}

// --- Fix numbered heading case ---

function fixNumberedHeadingCase(content) {
  // Headings like "## 1. default SDK" → "## 1. Default SDK"
  // Capitalize the first word after the number.
  return content.replace(
    /^(#{2,6}\s+\d+\.\s+)([a-z])/gm,
    (_, prefix, firstChar) => prefix + firstChar.toUpperCase(),
  );
}

// --- AgentPrompt injection ---

// Pages that should have an AgentPrompt callout injected after the frontmatter.
// Each key is a relative file path, and the value is the prompt text.
const AGENT_PROMPTS = {
  "getting-started/quick-start.md":
    "Run `curl -sL https://memory.walrus.xyz/skills/setup` and use the returned instructions to set up Walrus Memory in this AI client.",
};

function injectAgentPrompt(content, relPath) {
  const prompt = AGENT_PROMPTS[relPath];
  if (!prompt) return content;

  // Insert the AgentPrompt component right after the frontmatter closing ---
  return content.replace(
    /^(---\n[\s\S]*?\n---\n)/,
    `$1\n<AgentPrompt\n  prompt="${prompt}"\n/>\n`,
  );
}

// --- Main pipeline ---

function transformFile(content, relPath) {
  let result = content;

  // Order matters: convert components before link rewriting
  result = convertAdmonitions(result);
  result = convertCodeGroups(result);
  result = convertCardGroups(result);
  result = convertCards(result);
  result = convertTabs(result);
  result = convertSteps(result);
  result = convertAccordions(result);

  // Structural fixes
  result = convertPrerequisites(result);
  result = fixStackedHeadings(result);
  result = fixNumberedHeadingCase(result);

  // Rewrite links
  result = rewriteInternalLinks(result);
  result = rewriteCardHrefs(result);

  // Rename MemWal → Walrus Memory
  result = renameMemwal(result);

  // Rewrite memwal.ai URLs and link text → memory.walrus.xyz
  result = result.replace(/https:\/\/memwal\.ai/g, "https://memory.walrus.xyz");
  result = result.replace(
    /https:\/\/([a-z.]*?)\.?memwal\.ai/g,
    (_, sub) => sub
      ? `https://${sub}.memory.walrus.xyz`
      : "https://memory.walrus.xyz",
  );
  // Also replace bare "memwal.ai" in link text and prose
  result = result.replace(/\bmemwal\.ai\b/g, "memory.walrus.xyz");

  // Enforce Sui Documentation Style Guide
  result = enforceStyleGuide(result, relPath);

  return result;
}

function collectFiles(dir, base) {
  const entries = [];
  if (!fs.existsSync(dir)) return entries;

  for (const item of fs.readdirSync(dir, { withFileTypes: true })) {
    const fullPath = path.join(dir, item.name);
    const relPath = base ? `${base}/${item.name}` : item.name;

    if (item.isDirectory()) {
      // Skip non-content directories
      if (["src", "node_modules", ".git"].includes(item.name)) continue;
      entries.push(...collectFiles(fullPath, relPath));
    } else if (item.name.endsWith(".md") || item.name.endsWith(".mdx")) {
      entries.push({ fullPath, relPath });
    }
  }
  return entries;
}

function main() {
  if (!fs.existsSync(CACHE_DIR)) {
    console.warn(
      "⚠️  walrus-memory: cache not found. Was fetch-walrus-memory-docs.js run?",
    );
    return;
  }

  // Build the known-pages set from actual cache files so link rewriting
  // stays in sync automatically when the upstream repo adds or removes pages.
  buildKnownPages(CACHE_DIR);

  // Clean output directory (recreate fresh)
  if (fs.existsSync(OUTPUT_DIR)) {
    fs.rmSync(OUTPUT_DIR, { recursive: true });
  }
  fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  // Collect all .md/.mdx files from the cache
  const files = collectFiles(CACHE_DIR);

  // Skip non-content files
  const skipFiles = new Set([
    "docs.json",
    "package.json",
    "docs.Dockerfile",
    "llms.txt",
    "llms-full.txt",
  ]);

  let count = 0;
  for (const { fullPath, relPath } of files) {
    const basename = path.basename(relPath);
    if (skipFiles.has(basename)) continue;
    // Skip files in src/ directory (CSS, assets)
    if (relPath.startsWith("src/")) continue;

    const content = fs.readFileSync(fullPath, "utf8");
    let transformed = transformFile(content, relPath);
    transformed = injectAgentPrompt(transformed, relPath);

    // Apply file renames if configured
    const outputRelPath = RENAME_MAP[relPath] || relPath;

    // Preserve directory structure
    const outPath = path.join(OUTPUT_DIR, outputRelPath);
    const outDir = path.dirname(outPath);
    if (!fs.existsSync(outDir)) {
      fs.mkdirSync(outDir, { recursive: true });
    }

    fs.writeFileSync(outPath, transformed);
    count++;
  }

  // Write landing page (not in upstream repo, maintained here)
  const landingPage = `---
title: Walrus Memory
description: "Portable, verifiable memory for AI agents built on Walrus and Sui."
slug: /
---

Walrus Memory gives AI agents persistent, portable memory that works across apps, sessions, and
workflows. Ownership and access are enforced onchain through Sui smart contracts, and all content is
encrypted through Seal before reaching Walrus.

<Cards>
  <Card title="Get Started" href="/walrus-memory/getting-started/what-is-walrus-memory">
    Learn what Walrus Memory is and get up and running quickly
  </Card>
  <Card title="Concepts" href="/walrus-memory/fundamentals/concepts/memory-space">
    Memory spaces, ownership, delegates, and access control
  </Card>
  <Card title="Architecture" href="/walrus-memory/fundamentals/architecture/core-components">
    Core components, data flow, storage model, and security
  </Card>
  <Card title="TypeScript SDK" href="/walrus-memory/sdk/overview">
    Integrate memory into TypeScript apps with the SDK
  </Card>
  <Card title="Python SDK" href="/walrus-memory/python-sdk/quick-start">
    Use Walrus Memory from Python
  </Card>
  <Card title="Relayer" href="/walrus-memory/relayer/overview">
    Managed relayer, self-hosting, and API reference
  </Card>
  <Card title="MCP" href="/walrus-memory/mcp/overview">
    Model Context Protocol integration for AI tools
  </Card>
  <Card title="Smart Contract" href="/walrus-memory/contract/overview">
    Onchain ownership model, delegate keys, and permissions
  </Card>
  <Card title="Indexer" href="/walrus-memory/indexer/purpose">
    Event indexing, onchain events, and database sync
  </Card>
</Cards>
`;
  fs.writeFileSync(path.join(OUTPUT_DIR, "index.md"), landingPage);
  count++;

  console.log(
    `✅ walrus-memory: transformed ${count} files → walrus-memory-content/`,
  );
}

main();
