// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/**
 * Prepends the llms.txt directive to every exported markdown file.
 *
 * Runs after llms.txt generation so the index is built from clean content,
 * and before `docusaurus build` copies static/ into build/, so the directive
 * is part of the deployed .md files.
 */

const fs = require("fs");
const path = require("path");

const markdownDir = path.join(__dirname, "../../static/markdown");
const DIRECTIVE =
  "> For the complete documentation index, see [https://docs.wal.app/llms.txt](https://docs.wal.app/llms.txt)\n\n";
const DIRECTIVE_PREFIX = "> For the complete documentation index";

function findMarkdownFiles(dir) {
  const results = [];
  const entries = fs.readdirSync(dir, { withFileTypes: true });

  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      results.push(...findMarkdownFiles(fullPath));
    } else if (entry.name.endsWith(".md")) {
      results.push(fullPath);
    }
  }

  return results;
}

if (!fs.existsSync(markdownDir)) {
  console.error(`❌ Markdown export directory not found: ${markdownDir}`);
  process.exit(1);
}

let injectedCount = 0;

for (const filePath of findMarkdownFiles(markdownDir)) {
  const content = fs.readFileSync(filePath, "utf8");
  if (!content.trim()) continue;
  if (content.startsWith(DIRECTIVE_PREFIX)) continue;

  fs.writeFileSync(filePath, DIRECTIVE + content, "utf8");
  injectedCount++;
}

console.log(`  Injected llms.txt directive into ${injectedCount} markdown files`);
