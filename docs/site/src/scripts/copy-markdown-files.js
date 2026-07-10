// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

const fs = require('fs');
const path = require('path');
const matter = require('gray-matter');

const contentDir = path.join(__dirname, '../../.markdown/with-imports');
const outputDir = path.join(__dirname, '../../static/markdown');
const glossaryPath = path.join(__dirname, '../../static/glossary.json');

// NOTE: The llms.txt directive is injected into build/markdown/ files by
// generate-routes.js (post-build), not here, so that generate-llmstxt.mjs
// reads clean content without the self-referential directive.

/**
 * Checks if a markdown file should be skipped (draft or redirect)
 */
function shouldSkip(content) {
  const { data } = matter(content);
  if (data.draft === true) return true;
  if (typeof data.title === 'string' && data.title.startsWith('Redirecting')) return true;
  if (data.sidebar_class_name === 'hidden') return true;
  return false;
}

/**
 * Checks if a file's frontmatter indicates it should be excluded from link lists.
 */
function shouldExcludeFromLinkList(content) {
  const { data } = matter(content);
  if (data.draft === true) return true;
  if (typeof data.title === 'string' && data.title.startsWith('Redirecting')) return true;
  if (data.sidebar_class_name === 'hidden') return true;
  return false;
}

/**
 * Converts a file path to a docs URL path.
 */
function fileToUrlPath(filePath, baseDir) {
  let rel = path.relative(baseDir, filePath).replace(/\.mdx?$/, '');
  rel = rel.replace(/\/?index$/, '');
  return rel ? `/docs/${rel}` : '/docs';
}

/**
 * Generates a markdown link list to replace <DocCardList /> components.
 * Reads sibling files and subdirectory index files to build a list of child pages.
 */
function generateDocCardList(filePath, baseDir) {
  const dir = path.dirname(filePath);
  const items = [];

  let entries;
  try {
    entries = fs.readdirSync(dir, { withFileTypes: true });
  } catch {
    return '';
  }

  for (const entry of entries) {
    if (entry.isDirectory()) {
      // Look for index file in subdirectory
      for (const indexName of ['index.mdx', 'index.md']) {
        const indexPath = path.join(dir, entry.name, indexName);
        if (fs.existsSync(indexPath)) {
          try {
            const raw = fs.readFileSync(indexPath, 'utf8');
            if (shouldExcludeFromLinkList(raw)) break;
            const { data } = matter(raw);
            items.push({
              title: data.title || entry.name,
              description: data.description || '',
              url: fileToUrlPath(indexPath, baseDir),
            });
          } catch { /* skip unreadable files */ }
          break;
        }
      }
    } else if (
      entry.name !== 'index.mdx' &&
      entry.name !== 'index.md' &&
      (entry.name.endsWith('.mdx') || entry.name.endsWith('.md'))
    ) {
      const childPath = path.join(dir, entry.name);
      try {
        const raw = fs.readFileSync(childPath, 'utf8');
        if (shouldExcludeFromLinkList(raw)) continue;
        const { data } = matter(raw);
        items.push({
          title: data.title || entry.name.replace(/\.mdx?$/, ''),
          description: data.description || '',
          url: fileToUrlPath(childPath, baseDir),
        });
      } catch { /* skip unreadable files */ }
    }
  }

  if (items.length === 0) return '';

  return items
    .map(item =>
      item.description
        ? `- [${item.title}](${item.url}): ${item.description}`
        : `- [${item.title}](${item.url})`,
    )
    .join('\n');
}

/**
 * Generates markdown glossary content from glossary.json.
 */
function generateGlossaryContent() {
  try {
    const glossary = JSON.parse(fs.readFileSync(glossaryPath, 'utf8'));
    return glossary
      .map(entry => `**${entry.label}**: ${entry.definition}`)
      .join('\n\n');
  } catch {
    return '';
  }
}

/**
 * Removes or simplifies MDX/JSX components for cleaner markdown.
 * Protects fenced code blocks from being modified.
 */
function cleanMdxComponents(content, filePath, baseDir) {
  let cleaned = content;

  // ── Protect code blocks from JSX cleaning ──────────────────────────────
  const codeBlocks = [];
  cleaned = cleaned.replace(/```[\s\S]*?```/g, match => {
    codeBlocks.push(match);
    return `__CODE_BLOCK_${codeBlocks.length - 1}__`;
  });
  cleaned = cleaned.replace(/`[^`\n]+`/g, match => {
    codeBlocks.push(match);
    return `__CODE_BLOCK_${codeBlocks.length - 1}__`;
  });

  // ── Remove import statements ───────────────────────────────────────────
  cleaned = cleaned.replace(/^import\s+.*?from\s+['"].*?['"];?\s*$/gm, '');

  // ── Remove IMPORT_CONTENT_RESOLVED HTML comments ───────────────────────
  cleaned = cleaned.replace(/<!-- IMPORT_CONTENT_RESOLVED[\s\S]*?-->/g, '');
  cleaned = cleaned.replace(/<!-- \/IMPORT_CONTENT_RESOLVED -->/g, '');

  // ── Replace dynamic components with static content ─────────────────────
  if (cleaned.includes('<DocCardList')) {
    const cardContent = generateDocCardList(filePath, baseDir);
    cleaned = cleaned.replace(/<DocCardList\s*\/>/g, cardContent);
  }
  if (cleaned.includes('<GlossaryPage')) {
    const glossaryContent = generateGlossaryContent();
    cleaned = cleaned.replace(/<GlossaryPage\s*\/>/g, glossaryContent);
  }

  // ── Convert Card components to markdown links ──────────────────────────
  cleaned = cleaned.replace(
    /<Card[^>]*title="([^"]*)"[^>]*href="([^"]*)"[^>]*\/>/g,
    '- [$1]($2)',
  );
  cleaned = cleaned.replace(/<Cards[^>]*>/g, '');
  cleaned = cleaned.replace(/<\/Cards>/g, '');

  // ── Convert Docusaurus admonitions to blockquotes ──────────────────────
  cleaned = cleaned.replace(
    /^:::(tip|info|warning|danger|note|caution)(?:[ \t]+(.+))?\s*\n([\s\S]*?)^:::\s*$/gm,
    (_match, type, title, body) => {
      const label = type.charAt(0).toUpperCase() + type.slice(1);
      const header = title ? title.trim() : label;
      const lines = body
        .trim()
        .split('\n')
        .map(l => `> ${l}`)
        .join('\n');
      return `> **${header}**\n>\n${lines}`;
    },
  );

  // ── Convert <a> tags to markdown links before generic stripping ────────
  cleaned = cleaned.replace(
    /<a\s+href="([^"]*)"[^>]*>([\s\S]*?)<\/a>/g,
    '[$2]($1)',
  );

  // ── Collapse <Tabs>: keep only the first <TabItem> content ────────────
  cleaned = cleaned.replace(/<Tabs[^>]*>([\s\S]*?)<\/Tabs>/gi, (_match, inner) => {
    const firstTab = inner.match(/<TabItem[^>]*>([\s\S]*?)<\/TabItem>/i);
    return firstTab ? firstTab[1].trim() : inner;
  });
  // Remove any orphaned TabItem wrappers left over
  cleaned = cleaned.replace(/<\/?TabItem[^>]*>/gi, '');

  // ── Remove paired JSX/HTML tags, keep content (loop for nesting) ───────
  let prev;
  do {
    prev = cleaned;
    cleaned = cleaned.replace(/<(\w+)[^>]*>([\s\S]*?)<\/\1>/g, '$2');
  } while (cleaned !== prev);

  // ── Remove remaining self-closing JSX/HTML tags ────────────────────────
  cleaned = cleaned.replace(/<\w+[^>]*\/>/g, '');

  // ── Remove JSX expression comments ─────────────────────────────────────
  cleaned = cleaned.replace(/\{\/\*[\s\S]*?\*\/\}/g, '');

  // ── Restore code blocks ────────────────────────────────────────────────
  cleaned = cleaned.replace(/__CODE_BLOCK_(\d+)__/g, (_match, idx) => codeBlocks[parseInt(idx)]);

  // ── Clean up excessive newlines ────────────────────────────────────────
  cleaned = cleaned.replace(/\n{3,}/g, '\n\n');

  return cleaned.trim();
}

/**
 * Strips frontmatter and cleans MDX/JSX components from markdown.
 */
function stripFrontmatter(content, filePath, baseDir) {
  const { content: markdownContent } = matter(content);
  return cleanMdxComponents(markdownContent, filePath, baseDir);
}

/**
 * Recursively copies markdown files from content dir to build output.
 */
function copyMarkdownFiles(dir, baseDir = dir) {
  const files = fs.readdirSync(dir);

  files.forEach(file => {
    const filePath = path.join(dir, file);
    const stat = fs.statSync(filePath);

    if (stat.isDirectory()) {
      // Recursively process subdirectories
      copyMarkdownFiles(filePath, baseDir);
    } else if (file.endsWith('.md') || file.endsWith('.mdx')) {
      // Read and process markdown/mdx files
      const content = fs.readFileSync(filePath, 'utf8');

      // Skip drafts and redirect pages
      if (shouldSkip(content)) {
        const relativePath = path.relative(baseDir, filePath);
        console.log(`  ⏭ Skipped: ${relativePath}`);
        return;
      }

      const cleanContent = stripFrontmatter(content, filePath, baseDir);

      // Skip files that are empty after cleaning
      if (!cleanContent.trim()) {
        const relativePath = path.relative(baseDir, filePath);
        console.log(`  ⏭ Skipped (empty): ${relativePath}`);
        return;
      }

      // Preserve directory structure
      const relativePath = path.relative(baseDir, filePath);
      // Normalize all files to .md extension
      const outputPath = path.join(outputDir, relativePath.replace(/\.mdx?$/, '.md'));

      // Create directory structure if it doesn't exist
      fs.mkdirSync(path.dirname(outputPath), { recursive: true });
      fs.writeFileSync(outputPath, cleanContent, 'utf8');
      console.log(`  ✔ Copied: ${relativePath}`);
    }
  });
}

const blogDir = path.join(__dirname, '../../../blog');

console.log('📝 Starting markdown export...');
console.log(`Source: ${contentDir}`);
console.log(`Output: ${outputDir}\n`);

// Clean and recreate output directory to remove stale files from previous builds
if (fs.existsSync(outputDir)) {
  fs.rmSync(outputDir, { recursive: true });
}
fs.mkdirSync(outputDir, { recursive: true });

// Copy all doc markdown files
copyMarkdownFiles(contentDir);

// Copy blog posts (these don't go through inline-imports, read directly from source)
const blogOutputDir = path.join(outputDir, 'blog');
fs.mkdirSync(blogOutputDir, { recursive: true });
if (fs.existsSync(blogDir)) {
  const blogFiles = fs.readdirSync(blogDir).filter(f => f.endsWith('.md') || f.endsWith('.mdx'));
  for (const file of blogFiles) {
    const filePath = path.join(blogDir, file);
    const content = fs.readFileSync(filePath, 'utf8');
    if (shouldSkip(content)) continue;
    const cleanContent = stripFrontmatter(content, filePath, blogDir);
    if (!cleanContent.trim()) continue;
    const outputPath = path.join(blogOutputDir, file.replace(/\.mdx?$/, '.md'));
    fs.writeFileSync(outputPath, cleanContent, 'utf8');
    console.log(`  ✔ Blog: ${file}`);
  }
}

// Copy Walrus Memory docs (external content, read directly from transformed source)
const walrusMemoryDir = path.join(__dirname, '../../../walrus-memory-content');
const walrusMemoryOutputDir = path.join(outputDir, 'walrus-memory');
if (fs.existsSync(walrusMemoryDir)) {
  console.log('\n📝 Exporting Walrus Memory docs...');
  fs.mkdirSync(walrusMemoryOutputDir, { recursive: true });
  function copyWalrusMemoryFiles(dir, baseDir) {
    const files = fs.readdirSync(dir);
    files.forEach(file => {
      const filePath = path.join(dir, file);
      const stat = fs.statSync(filePath);
      if (stat.isDirectory()) {
        copyWalrusMemoryFiles(filePath, baseDir);
      } else if (file.endsWith('.md') || file.endsWith('.mdx')) {
        const content = fs.readFileSync(filePath, 'utf8');
        if (shouldSkip(content)) return;
        const cleanContent = stripFrontmatter(content, filePath, baseDir);
        if (!cleanContent.trim()) return;
        const relativePath = path.relative(baseDir, filePath);
        const outPath = path.join(walrusMemoryOutputDir, relativePath.replace(/\.mdx?$/, '.md'));
        fs.mkdirSync(path.dirname(outPath), { recursive: true });
        fs.writeFileSync(outPath, cleanContent, 'utf8');
        console.log(`  ✔ Walrus Memory: ${relativePath}`);
      }
    });
  }
  copyWalrusMemoryFiles(walrusMemoryDir, walrusMemoryDir);
} else {
  console.log('\n⏩ Walrus Memory docs not found, skipping export');
}

console.log('\n✅ Markdown files exported successfully');
