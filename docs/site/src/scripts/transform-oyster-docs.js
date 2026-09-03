// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0
//
// Transforms fetched Oyster docs (mdBook Markdown) into Docusaurus MDX.
//
// Transforms applied:
//   1. Add YAML frontmatter (title extracted from first # heading)
//   2. Rename README.md → index.mdx (Docusaurus convention)
//   3. Rewrite internal links: strip .md extensions, README → index
//   4. Convert blockquote callouts (> **Important:**) → :::warning admonitions
//   5. Rename .md → .mdx
//   6. Generate the Scalar API reference page

const fs = require("fs");
const path = require("path");

const SITE_ROOT = path.resolve(__dirname, "../../");
const CACHE_DIR = path.join(SITE_ROOT, ".cache-oyster/docs/src");
const OUTPUT_DIR = path.resolve(SITE_ROOT, "../oyster-content");

// Path to the OpenAPI spec (fetched by fetch-oyster-docs.js into static/)
const OPENAPI_SPEC_PATH = "/oyster/openapi.json";

// ── Frontmatter injection ─────────────────────────────────────────────

function extractTitle(content) {
  const match = content.match(/^#\s+(.+)$/m);
  return match ? match[1].trim() : null;
}

function addFrontmatter(content, relPath) {
  // If frontmatter already exists, skip
  if (content.startsWith("---\n")) return content;

  const title = extractTitle(content);
  if (!title) return content;

  // The introduction page serves as the landing page at /oyster
  const isIndex =
    relPath === "introduction.mdx" || relPath === "introduction.md";
  const slugLine = isIndex ? "slug: /\n" : "";

  const frontmatter = `---\ntitle: "${title.replace(/"/g, '\\"')}"\n${slugLine}---\n\n`;

  // Remove the first heading since title is now in frontmatter
  const withoutHeading = content.replace(/^#\s+.+\n+/, "");
  return frontmatter + withoutHeading;
}

// ── Link rewriting ────────────────────────────────────────────────────

function rewriteLinks(content) {
  // Rewrite relative .md links to Docusaurus-style paths:
  //   authentication.md         → authentication.mdx
  //   ../json-api/admin.md      → ../json-api/admin.mdx
  //   README.md                 → index.mdx
  //   ../guides/blob-lifecycle.md → ../guides/blob-lifecycle.mdx
  //   admin.md#create-api-key   → admin.mdx#create-api-key
  //
  // Keeping the file extension is critical: Docusaurus resolves links with
  // extensions as file-relative paths (based on the file's directory), while
  // bare slugs are resolved as URL-relative (based on the page's URL), which
  // breaks for index pages and pages with custom slugs.
  return content.replace(
    /(\[[^\]]*\]\()([^)]*\.md)((?:#[^)]*)?)\)/g,
    (match, prefix, mdPath, anchor) => {
      let cleaned = mdPath.replace(/\.md$/, ".mdx");
      // README.mdx → index.mdx
      cleaned = cleaned.replace(/\/README\.mdx$/, "/index.mdx");
      cleaned = cleaned.replace(/^README\.mdx$/, "index.mdx");
      return `${prefix}${cleaned}${anchor || ""})`;
    },
  );
}

// ── Callout conversion ────────────────────────────────────────────────

function convertCallouts(content) {
  // Convert patterns like:
  //   > **Important:** text...
  //   > continuation...
  //
  // Into:
  //   :::warning
  //   text...
  //   continuation...
  //   :::
  const lines = content.split("\n");
  const result = [];
  let i = 0;

  while (i < lines.length) {
    const line = lines[i];

    // Check for callout start: > **Important:** or > **Note ...**
    const calloutMatch = line.match(
      /^>\s*\*\*(Important|Note[^*]*):\*\*\s*(.*)/,
    );
    if (calloutMatch) {
      const type = calloutMatch[1].toLowerCase().startsWith("note")
        ? "info"
        : "warning";
      const firstLine = calloutMatch[2].trim();
      const bodyLines = firstLine ? [firstLine] : [];

      // Collect continuation lines (lines starting with >)
      i++;
      while (i < lines.length && lines[i].startsWith(">")) {
        const continuation = lines[i].replace(/^>\s?/, "");
        bodyLines.push(continuation);
        i++;
      }

      result.push(`:::${type}`);
      result.push(bodyLines.join("\n").trim());
      result.push(":::");
      result.push("");
      continue;
    }

    result.push(line);
    i++;
  }

  return result.join("\n");
}

// ── Footnote support ──────────────────────────────────────────────────
// mdBook footnotes ([^name]: ...) work in Docusaurus with remark-footnotes,
// which is already available. No transform needed.

// ── File collection ───────────────────────────────────────────────────

function collectFiles(dir, base) {
  const entries = [];
  if (!fs.existsSync(dir)) return entries;

  for (const item of fs.readdirSync(dir, { withFileTypes: true })) {
    const fullPath = path.join(dir, item.name);
    const relPath = base ? `${base}/${item.name}` : item.name;

    if (item.isDirectory()) {
      if ([".git", "node_modules"].includes(item.name)) continue;
      entries.push(...collectFiles(fullPath, relPath));
    } else if (item.name.endsWith(".md") || item.name.endsWith(".mdx")) {
      entries.push({ fullPath, relPath });
    }
  }
  return entries;
}

// ── Main transform pipeline ───────────────────────────────────────────

function transformFile(content, relPath) {
  let result = content;

  result = convertCallouts(result);
  result = rewriteLinks(result);
  result = addFrontmatter(result, relPath);

  return result;
}

function generateApiReferencePage() {
  const scalarUrl = OPENAPI_SPEC_PATH.replace('openapi.json', 'scalar.html');
  return `---
title: "Interactive API Reference"
description: "Explore the Walrus Oyster API interactively using the OpenAPI specification."
hide_table_of_contents: true
hide_title: true
---

import BrowserOnly from '@docusaurus/BrowserOnly';
import React from 'react';

<BrowserOnly>
  {() => {
    const style = document.createElement('style');
    style.textContent = \`
      /* Let the iframe fill the content area next to the sidebar */
      .docMainContainer_t2hy, [class*="docMainContainer"] { max-width: 100% !important; }
      .container { max-width: 100% !important; padding: 0 8px !important; }
      .col { max-width: 100% !important; flex: 0 0 100% !important; padding: 0 !important; }
      [class*="docItemContainer"] { max-width: 100% !important; padding: 0 !important; }
      .theme-doc-breadcrumbs,
      .pagination-nav,
      article > header,
      .theme-doc-toc-desktop,
      .theme-doc-toc-mobile,
      [class*="copyPageButton"],
      .theme-doc-markdown > p,
      .theme-doc-markdown > h1,
      .theme-doc-markdown > header,
      [class*="docTitle"],
      [class*="docFooter"],
      footer.py-6,
      .theme-doc-footer { display: none !important; }
      .theme-doc-markdown { margin: 0 !important; }
      .padding-top--md { padding-top: 0 !important; }
      .padding-bottom--lg { padding-bottom: 0 !important; }
      [class*="docItemContainer"], [class*="docItemCol"],
      article, .theme-doc-markdown, main,
      [class*="mainWrapper"], [class*="docsWrapper"],
      [class*="docSidebarContainer"], [class*="sidebarViewport"],
      [class*="sidebar_"], aside {
        border: none !important;
        border-right: none !important;
        box-shadow: none !important;
        outline: none !important;
      }
      /* Kill any pseudo-element borders on the sidebar */
      [class*="docSidebarContainer"]::after,
      [class*="sidebarViewport"]::after,
      aside::after { display: none !important; }
    \`;
    document.head.appendChild(style);
    return null;
  }}
</BrowserOnly>

<BrowserOnly>
  {() => {
    const [failed, setFailed] = React.useState(false);
    const iframeUrl = '${scalarUrl}';

    if (failed) {
      return (
        <div style={{ padding: '2rem', textAlign: 'center' }}>
          <p>The interactive API reference cannot load in this environment.</p>
          <p><a href={iframeUrl} target="_blank" rel="noopener noreferrer">Open the API reference in a new tab</a></p>
        </div>
      );
    }

    return (
      <iframe
        src={iframeUrl}
        style={{
          width: '100%',
          height: 'calc(100vh - 120px)',
          border: 'none',
          display: 'block',
          borderRadius: '4px',
        }}
        title="Walrus Oyster API Reference"
        onError={() => setFailed(true)}
        onLoad={(e) => {
          try {
            // Detect blocked iframe (github.io X-Frame-Options)
            const doc = e.target.contentDocument;
            if (!doc || !doc.body || doc.body.innerHTML === '') setFailed(true);
          } catch (_) {
            setFailed(true);
          }
        }}
      />
    );
  }}
</BrowserOnly>
`;
}

function main() {
  if (!fs.existsSync(CACHE_DIR)) {
    console.warn(
      "⚠️  oyster: cache not found. Creating placeholder so the build can proceed.",
    );
    // Docusaurus requires the content directory to exist even if empty.
    // Create it with a minimal index page so the plugin doesn't crash.
    fs.mkdirSync(OUTPUT_DIR, { recursive: true });
    fs.writeFileSync(
      path.join(OUTPUT_DIR, "index.mdx"),
      '---\ntitle: "Walrus Oyster API"\nslug: /\n---\n\nWalrus Oyster API documentation is not available in this build.\n',
    );
    return;
  }

  // Clean output directory
  if (fs.existsSync(OUTPUT_DIR)) {
    fs.rmSync(OUTPUT_DIR, { recursive: true });
  }
  fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  const files = collectFiles(CACHE_DIR);

  // Skip SUMMARY.md (mdBook-only file)
  const skipFiles = new Set(["SUMMARY.md"]);

  let count = 0;
  for (const { fullPath, relPath } of files) {
    const basename = path.basename(relPath);
    if (skipFiles.has(basename)) continue;

    const content = fs.readFileSync(fullPath, "utf8");
    const transformed = transformFile(content, relPath);

    // Rename README.md → index.mdx, others → .mdx
    let outputRelPath = relPath;
    outputRelPath = outputRelPath.replace(/README\.md$/, "index.mdx");
    outputRelPath = outputRelPath.replace(/\.md$/, ".mdx");

    const outPath = path.join(OUTPUT_DIR, outputRelPath);
    const outDir = path.dirname(outPath);
    if (!fs.existsSync(outDir)) {
      fs.mkdirSync(outDir, { recursive: true });
    }

    fs.writeFileSync(outPath, transformed);
    count++;
  }

  // Generate the Scalar API reference page
  const apiRefPath = path.join(OUTPUT_DIR, "api-reference.mdx");
  fs.writeFileSync(apiRefPath, generateApiReferencePage());
  count++;

  // Generate markdown files from the OpenAPI spec, split by tag
  const specPath = path.join(SITE_ROOT, "static/oyster/openapi.json");
  if (fs.existsSync(specPath)) {
    const generated = generateOpenApiMarkdownFiles(specPath, OUTPUT_DIR);
    count += generated;
    console.log(`✅ oyster: generated ${generated} OpenAPI markdown reference files`);
  }

  console.log(`✅ oyster: transformed ${count} files → oyster-content/`);
}

// ── OpenAPI spec → Markdown (split by tag) ────────────────────────────

function generateOpenApiMarkdownFiles(specPath, outputDir) {
  const spec = JSON.parse(fs.readFileSync(specPath, "utf8"));
  const apiDir = path.join(outputDir, "openapi");
  fs.mkdirSync(apiDir, { recursive: true });

  const baseUrl = spec.servers?.[0]?.url || "/api/v1";
  const specNote =
    ":::info\n" +
    "This page is auto-generated from the [OpenAPI spec](/oyster/openapi.json).\n" +
    "For an interactive explorer, see the [Interactive API Reference](../api-reference.mdx).\n" +
    ":::";

  // Group paths by tag
  const taggedPaths = {};
  const tagDescriptions = {};

  if (spec.tags) {
    for (const tag of spec.tags) {
      tagDescriptions[tag.name] = tag.description || "";
      taggedPaths[tag.name] = [];
    }
  }

  for (const [pathStr, methods] of Object.entries(spec.paths || {})) {
    for (const [method, op] of Object.entries(methods)) {
      if (method === "parameters" || method === "summary") continue;
      const tags = op.tags || ["Other"];
      for (const tag of tags) {
        if (!taggedPaths[tag]) taggedPaths[tag] = [];
        taggedPaths[tag].push({ path: pathStr, method, op });
      }
    }
  }

  let fileCount = 0;

  // Generate one file per tag
  for (const [tag, endpoints] of Object.entries(taggedPaths)) {
    if (endpoints.length === 0) continue;

    const slug = tag.toLowerCase().replace(/\s+/g, "-");
    const lines = [
      "---",
      `title: "${tag} Endpoints"`,
      `description: "OpenAPI reference for ${tag} endpoints in the Walrus Oyster API."`,
      "---",
      "",
      `# ${tag} Endpoints`,
      "",
      tagDescriptions[tag] || "",
      "",
      `Base URL: \`${baseUrl}\``,
      "",
      specNote,
      "",
    ];

    for (const { path: pathStr, method, op } of endpoints) {
      lines.push(`## ${method.toUpperCase()} \`${pathStr}\``);
      lines.push("");
      if (op.summary) lines.push(`**${op.summary}**`);
      lines.push("");
      if (op.description) {
        lines.push(op.description);
        lines.push("");
      }

      if (op.security && op.security.length > 0) {
        lines.push("**Authentication:** Required");
        lines.push("");
      }

      const params = op.parameters || [];
      if (params.length > 0) {
        lines.push("**Parameters:**");
        lines.push("");
        lines.push("| Name | In | Type | Required | Description |");
        lines.push("|------|-----|------|----------|-------------|");
        for (const p of params) {
          const type = p.schema?.type || "";
          const required = p.required ? "Yes" : "No";
          const desc = (p.description || "").replace(/\n/g, " ");
          lines.push(
            `| \`${p.name}\` | ${p.in} | ${type} | ${required} | ${desc} |`,
          );
        }
        lines.push("");
      }

      if (op.requestBody) {
        const content = op.requestBody.content || {};
        const contentTypes = Object.keys(content);
        if (contentTypes.length > 0) {
          lines.push(`**Request body:** \`${contentTypes.join("`, `")}\``);
          const firstContent = content[contentTypes[0]];
          if (firstContent?.schema) {
            const schemaDesc = describeSchema(firstContent.schema, spec, 0);
            if (schemaDesc) {
              lines.push("");
              lines.push("```json");
              lines.push(schemaDesc);
              lines.push("```");
            }
          }
          lines.push("");
        }
      }

      if (op.responses) {
        lines.push("**Responses:**");
        lines.push("");
        for (const [code, resp] of Object.entries(op.responses)) {
          const desc = resp.description || "";
          lines.push(`- **${code}**: ${desc}`);
        }
        lines.push("");
      }

      lines.push("---");
      lines.push("");
    }

    fs.writeFileSync(path.join(apiDir, `${slug}.mdx`), lines.join("\n"));
    fileCount++;
  }

  // Generate models file
  if (spec.components?.schemas) {
    const lines = [
      "---",
      'title: "API Models"',
      'description: "Data models and schemas used by the Walrus Oyster API."',
      "---",
      "",
      "# API Models",
      "",
      "Request and response schemas used by the Walrus Oyster API.",
      "",
      specNote,
      "",
    ];

    for (const [name, schema] of Object.entries(spec.components.schemas)) {
      lines.push(`## ${name}`);
      lines.push("");
      if (schema.description) {
        lines.push(schema.description);
        lines.push("");
      }
      if (schema.properties) {
        lines.push("| Field | Type | Required | Description |");
        lines.push("|-------|------|----------|-------------|");
        const required = new Set(schema.required || []);
        for (const [field, prop] of Object.entries(schema.properties)) {
          const type =
            prop.type || (prop.$ref ? prop.$ref.split("/").pop() : "object");
          const req = required.has(field) ? "Yes" : "No";
          const desc = (prop.description || "").replace(/\n/g, " ");
          lines.push(`| \`${field}\` | ${type} | ${req} | ${desc} |`);
        }
        lines.push("");
      }
      if (schema.enum) {
        lines.push(
          `**Enum values:** ${schema.enum.map((v) => `\`${v}\``).join(", ")}`,
        );
        lines.push("");
      }
    }

    fs.writeFileSync(path.join(apiDir, "models.mdx"), lines.join("\n"));
    fileCount++;
  }

  return fileCount;
}

function describeSchema(schema, spec, depth) {
  if (depth > 3) return "{ ... }";
  if (schema.$ref) {
    const refName = schema.$ref.split("/").pop();
    const resolved = spec.components?.schemas?.[refName];
    if (resolved) return describeSchema(resolved, spec, depth + 1);
    return `{ "${refName}": "..." }`;
  }
  if (schema.type === "object" && schema.properties) {
    const entries = Object.entries(schema.properties).map(([key, prop]) => {
      const type =
        prop.type || (prop.$ref ? prop.$ref.split("/").pop() : "object");
      return `  "${key}": "<${type}>"`;
    });
    return "{\n" + entries.join(",\n") + "\n}";
  }
  if (schema.type === "array" && schema.items) {
    const inner = describeSchema(schema.items, spec, depth + 1);
    return `[${inner}]`;
  }
  return null;
}

main();
