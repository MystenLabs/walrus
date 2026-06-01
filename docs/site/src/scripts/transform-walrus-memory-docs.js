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

// All known MemWal doc pages (from docs.json + orphans) for link rewriting
const KNOWN_PAGES = [
  "getting-started/what-is-memwal",
  "getting-started/quick-start",
  "getting-started/choose-your-path",
  "fundamentals/concepts/memory-space",
  "fundamentals/concepts/ownership-and-access",
  "fundamentals/architecture/core-components",
  "fundamentals/architecture/how-storage-works",
  "fundamentals/architecture/data-flow-security-model",
  "sdk/quick-start",
  "sdk/overview",
  "sdk/usage",
  "sdk/usage/memwal",
  "sdk/usage/memwal-manual",
  "sdk/usage/with-memwal",
  "sdk/api-reference",
  "sdk/changelog",
  "sdk/advanced-usage",
  "sdk/ai-integration",
  "sdk/examples",
  "sdk/example-map",
  "sdk/research-app-example",
  "python-sdk/quick-start",
  "python-sdk/usage",
  "python-sdk/usage/memwal",
  "python-sdk/usage/memwal-manual",
  "python-sdk/usage/with-memwal",
  "python-sdk/api-reference",
  "python-sdk/changelog",
  "relayer/overview",
  "relayer/public-relayer",
  "relayer/self-hosting",
  "relayer/nautilus-tee",
  "relayer/observability",
  "relayer/versioning-and-compatibility",
  "relayer/api-reference",
  "relayer/benchmark-ci-setup",
  "contract/overview",
  "contract/delegate-key-management",
  "contract/ownership-and-permissions",
  "indexer/purpose",
  "indexer/onchain-events",
  "indexer/database-sync",
  "mcp/overview",
  "mcp/quick-start",
  "mcp/how-it-works",
  "mcp/reference",
  "mcp/changelog",
  "openclaw/overview",
  "openclaw/quick-start",
  "openclaw/how-it-works",
  "openclaw/reference",
  "openclaw/changelog",
  "reference/configuration",
  "reference/environment-variables",
  "contributing/run-docs-locally",
  "contributing/run-repo-locally",
  "contributing/docs-workflow",
  "architecture/permanent-registry-design",
  "security/health-check-unsigned",
  "examples/example-apps",
];

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

      return `<Tabs groupId="code">\n${tabItems}\n</Tabs>`;
    },
  );
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
  // Only rewrite if the path matches a known page
  const knownSet = new Set(KNOWN_PAGES);

  return content.replace(
    /(\[([^\]]*)\]\()\/([^)#\s]+)(#[^)\s]*)?\)/g,
    (match, prefix, text, pagePath, anchor) => {
      const cleanPath = pagePath.replace(/\/$/, "");
      if (knownSet.has(cleanPath)) {
        return `${prefix}/walrus-memory/${cleanPath}${anchor || ""})`;
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
      const knownSet = new Set(KNOWN_PAGES);
      const cleanPath = p.replace(/\/$/, "");
      if (knownSet.has(cleanPath)) {
        return `href="/walrus-memory/${cleanPath}"`;
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

function enforceStyleGuide(content) {
  const lines = content.split("\n");
  let inCodeBlock = false;
  let inBashBlock = false;
  let inFrontmatter = false;
  let frontmatterCount = 0;
  const result = [];

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
      result.push(line);
      continue;
    }

    if (/^```/.test(line)) {
      if (!inCodeBlock) {
        inCodeBlock = true;
        inBashBlock = /^```(bash|shell|sh)\b/.test(line);
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
  return transformed.join("");
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
  "SEAL",
]);

function sentenceCasePart(text, isFirst) {
  if (!text) return text;
  const words = text.split(/(\s+)/);
  return words.map((word, wi) => {
    if (/^\s+$/.test(word)) return word;
    if (PRESERVE_CAPS.has(word)) return word;
    if (/^[A-Z]{2,}$/.test(word)) return word;
    if (/[a-z][A-Z]/.test(word) || /^[A-Z][a-z]+[A-Z]/.test(word)) return word;
    if (isFirst && wi === 0) {
      return word.charAt(0).toUpperCase() + word.slice(1).toLowerCase();
    }
    return word.toLowerCase();
  }).join("");
}

// --- Main pipeline ---

function transformFile(content) {
  let result = content;

  // Order matters: convert components before link rewriting
  result = convertAdmonitions(result);
  result = convertCodeGroups(result);
  result = convertCardGroups(result);
  result = convertCards(result);
  result = convertTabs(result);
  result = convertSteps(result);
  result = convertAccordions(result);

  // Rewrite links
  result = rewriteInternalLinks(result);
  result = rewriteCardHrefs(result);

  // Rename MemWal → Walrus Memory
  result = renameMemwal(result);

  // Enforce Sui Documentation Style Guide
  result = enforceStyleGuide(result);

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
    const transformed = transformFile(content);

    // Preserve directory structure
    const outPath = path.join(OUTPUT_DIR, relPath);
    const outDir = path.dirname(outPath);
    if (!fs.existsSync(outDir)) {
      fs.mkdirSync(outDir, { recursive: true });
    }

    fs.writeFileSync(outPath, transformed);
    count++;
  }

  console.log(
    `✅ walrus-memory: transformed ${count} files → walrus-memory-content/`,
  );
}

main();
