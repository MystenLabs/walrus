/*
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
*/

/**
 * Generates goal frontmatter for all .mdx pages based on page type and content.
 *
 * Usage:
 *   node scripts/generate-goals.mjs          # dry run — prints what would change
 *   node scripts/generate-goals.mjs --apply  # writes goals into frontmatter
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import matter from 'gray-matter';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const CONTENT_ROOT = path.resolve(__dirname, '..', '..', '..', 'content');

const dryRun = !process.argv.includes('--apply');

// ─── Helpers ────────────────────────────────────────────────────────────────

function globMdx(dir) {
  const results = [];
  function walk(d) {
    for (const entry of fs.readdirSync(d, { withFileTypes: true })) {
      const full = path.join(d, entry.name);
      if (entry.isDirectory()) {
        if (['node_modules', '.docusaurus', 'build', 'dist'].includes(entry.name)) continue;
        walk(full);
      } else if (entry.name.endsWith('.mdx')) {
        results.push(full);
      }
    }
  }
  walk(dir);
  return results;
}

function getHeadings(body) {
  const headings = [];
  for (const line of body.split('\n')) {
    const m = line.match(/^(#{1,6})\s+(.*)$/);
    if (m) headings.push({ level: m[1].length, text: m[2].trim() });
  }
  return headings;
}

function countCodeBlocks(body) {
  return (body.match(/^```/gm) || []).length / 2;
}

function hasPattern(body, pattern) {
  return new RegExp(pattern, 'i').test(body);
}

function countPattern(body, pattern) {
  return (body.match(new RegExp(pattern, 'gi')) || []).length;
}

function countWords(body) {
  const cleaned = body.replace(/```[\s\S]*?```/g, '').replace(/`[^`\n]+`/g, '').replace(/^---[\s\S]*?---\n?/, '');
  return (cleaned.match(/[a-zA-Z0-9]+/g) || []).length;
}

// ─── Archetype detection ────────────────────────────────────────────────────

function getArchetype(relPath, data, body) {
  // Skip snippets
  if (relPath.startsWith('snippets/')) return 'skip';

  // Category index pages (just a landing/nav page)
  if (relPath.endsWith('/index.mdx') || relPath.match(/^[^/]+\.mdx$/)) {
    const wc = countWords(body);
    if (wc < 200) return 'index';
  }

  // Getting started
  if (relPath.startsWith('getting-started/')) return 'onboarding';

  // Walrus client
  if (relPath.startsWith('walrus-client/')) return 'guide';

  // Operator guide
  if (relPath.startsWith('operator-guide/')) return 'operator';

  // Sites
  if (relPath.startsWith('sites/')) return 'guide';

  // System overview
  if (relPath.startsWith('system-overview/')) return 'guide';

  // HTTP API
  if (relPath.startsWith('http-api/')) return 'reference';

  // Examples
  if (relPath.startsWith('examples/')) return 'example';

  // Troubleshooting
  if (relPath.startsWith('troubleshooting/')) return 'troubleshooting';

  // TypeScript SDK
  if (relPath.startsWith('typescript-sdk/') || relPath.startsWith('typescript-sdk.mdx')) return 'sdk-reference';

  // Legal
  if (relPath.startsWith('legal/')) return 'reference';

  return 'guide';
}

// ─── Goal generators by archetype ───────────────────────────────────────────

function generateGoal(archetype, relPath, data, body) {
  const title = data.title || path.basename(relPath, '.mdx').replace(/-/g, ' ');
  const headings = getHeadings(body);
  const codeBlocks = countCodeBlocks(body);
  const wc = countWords(body);
  const h2s = headings.filter(h => h.level === 2).map(h => h.text);

  switch (archetype) {
    case 'onboarding':
      return generateOnboardingGoal(title, body, headings, codeBlocks, h2s, relPath);
    case 'example':
      return generateExampleGoal(title, body, headings, codeBlocks, h2s);
    case 'guide':
      return generateGuideGoal(title, body, headings, codeBlocks, wc);
    case 'troubleshooting':
      return generateTroubleshootingGoal(title, body, headings, wc);
    case 'reference':
      return generateReferenceGoal(title, body, headings, wc);
    case 'operator':
      return generateOperatorGoal(title, body, headings, codeBlocks, wc);
    case 'sdk-reference':
      return generateSdkReferenceGoal(title, body, headings, codeBlocks);
    case 'index':
      return generateIndexGoal(title, headings);
    default:
      return generateGuideGoal(title, body, headings, codeBlocks, wc);
  }
}

// Append GEO/AEO checks to any generated goal (called after archetype-specific generation)
function appendGeoChecks(goal) {
  if (!goal || !goal.requires) return goal;
  goal.requires.push({ has_questions: true, label: 'Needs questions for AI search visibility' });
  goal.requires.push({ has_answer: true, label: 'Needs answer summary for AI citation' });
  return goal;
}

function generateOnboardingGoal(title, body, headings, codeBlocks, h2s, relPath) {
  const requires = [];

  // All onboarding pages need code examples
  requires.push({ pattern: '```', min: 2, label: 'Has command or code examples' });

  // Should have frontmatter
  requires.push({ has_frontmatter: ['title', 'description', 'keywords'] });

  // Page-specific checks based on what the page teaches
  if (hasPattern(body, 'install|walrus')) {
    requires.push({ pattern: 'walrus --version|walrus -V|walrus --help', min: 1, label: 'Shows how to verify installation' });
  }

  requires.push({ min_words: 300, label: 'Needs more walkthrough depth' });

  return {
    description: `Reader can ${title.toLowerCase()} and verify the result`,
    requires,
  };
}

function generateExampleGoal(title, body, headings, codeBlocks, h2s) {
  const requires = [];

  // All examples need the standard structure
  requires.push({
    headings: [
      { pattern: 'When to use|Use case' },
      { pattern: 'What you learn|Learning' },
      { pattern: 'Prerequisites' },
      { pattern: 'Setup|Getting started' },
      { pattern: 'Run' },
      { pattern: 'Troubleshooting|Common issues' },
    ],
    label: 'Has standard example page sections',
  });

  requires.push({ pattern: '```', min: 3, label: 'Has code blocks for setup, source, and output' });
  requires.push({ has_frontmatter: ['title', 'description', 'keywords'] });
  requires.push({ min_words: 800, label: 'Needs more explanation depth' });

  // Check for architecture diagram
  if (hasPattern(body, 'mermaid|```mermaid|Architecture')) {
    requires.push({ pattern: 'mermaid|sequenceDiagram|graph|flowchart', min: 1, label: 'Has architecture diagram' });
  }

  // Check for key code highlights section
  requires.push({ headings: [{ pattern: 'Key code|Code highlight|Walkthrough' }], label: 'Has code explanation section' });

  return {
    description: `Reader can run the ${title.toLowerCase()} example and understand the pattern`,
    requires,
  };
}

function generateGuideGoal(title, body, headings, codeBlocks, wc) {
  const requires = [];

  requires.push({ has_frontmatter: ['title', 'description', 'keywords'] });
  requires.push({ min_words: 300, label: 'Needs more content depth' });

  // Guides with code should have code examples
  if (codeBlocks >= 1) {
    requires.push({ pattern: '```', min: 1, label: 'Has code examples' });
  }

  // If it links to other pages, it should be well-connected
  const internalLinks = countPattern(body, '\\]\\(/[^)]+\\)');
  if (internalLinks >= 3) {
    requires.push({ pattern: '\\]\\(/[^)]+\\)', min: 2, label: 'Links to related documentation' });
  }

  return {
    description: `Reader understands ${title.toLowerCase()} and can apply the concepts`,
    requires,
  };
}

function generateTroubleshootingGoal(title, body, headings, wc) {
  const requires = [];

  requires.push({ has_frontmatter: ['title', 'description', 'keywords'] });

  // Troubleshooting pages should have problem/solution pairs
  const h4count = headings.filter(h => h.level === 4).length;
  if (h4count >= 2) {
    requires.push({ pattern: 'Solution|Fix|Cause', min: 2, label: 'Has problem-solution pairs' });
  }

  requires.push({ min_words: 300, label: 'Needs more content depth' });

  if (countPattern(body, '```') >= 2) {
    requires.push({ pattern: '```', min: 2, label: 'Has code examples showing fixes' });
  }

  return {
    description: `Reader can diagnose and fix common ${title.toLowerCase().replace('troubleshooting ', '')} issues`,
    requires,
  };
}

function generateReferenceGoal(title, body, headings, wc) {
  const requires = [];

  requires.push({ has_frontmatter: ['title', 'description'] });
  requires.push({ min_words: 100, label: 'Has substantive reference content' });

  // Reference pages often have tables
  if (hasPattern(body, '\\|.*\\|')) {
    requires.push({ has_tables: true, min: 1, label: 'Has reference table' });
  }

  return {
    description: `Reader can look up ${title.toLowerCase()} details`,
    requires,
  };
}

function generateOperatorGoal(title, body, headings, codeBlocks, wc) {
  const requires = [];

  requires.push({ has_frontmatter: ['title', 'description', 'keywords'] });
  requires.push({ min_words: 300, label: 'Needs more operational depth' });

  if (codeBlocks >= 1) {
    requires.push({ pattern: '```', min: 1, label: 'Has configuration or command examples' });
  }

  return {
    description: `Operator can ${title.toLowerCase()} following the documented procedure`,
    requires,
  };
}

function generateSdkReferenceGoal(title, body, headings, codeBlocks) {
  const requires = [];

  requires.push({ has_frontmatter: ['title', 'description'] });
  requires.push({ pattern: '```', min: 1, label: 'Has SDK code examples' });
  requires.push({ min_words: 200, label: 'Has substantive API documentation' });

  // SDK pages should show import/setup
  if (hasPattern(body, 'import|require|use ')) {
    requires.push({ pattern: 'import|require|use ', min: 1, label: 'Shows SDK import or setup' });
  }

  return {
    description: `Developer can integrate ${title.toLowerCase()} using the SDK`,
    requires,
  };
}

function generateIndexGoal(title, headings) {
  return {
    description: `Reader can navigate to the right ${title.toLowerCase()} subtopic`,
    requires: [
      { has_frontmatter: ['title', 'description'] },
    ],
  };
}

// ─── Main ───────────────────────────────────────────────────────────────────

function main() {
  const files = globMdx(CONTENT_ROOT);
  let applied = 0;
  let skipped = 0;
  let alreadyHasGoal = 0;

  for (const filePath of files) {
    const relPath = path.relative(CONTENT_ROOT, filePath);
    const raw = fs.readFileSync(filePath, 'utf8');
    const { data, content: body } = matter(raw);

    // Skip if already has a goal
    if (data.goal) {
      alreadyHasGoal++;
      continue;
    }

    const archetype = getArchetype(relPath, data, body);
    if (archetype === 'skip') {
      skipped++;
      continue;
    }

    const goal = appendGeoChecks(generateGoal(archetype, relPath, data, body));
    if (!goal) {
      skipped++;
      continue;
    }

    // Add goal to frontmatter
    data.goal = goal;

    if (dryRun) {
      console.log(`[${archetype}] ${relPath}`);
      console.log(`  → "${goal.description}"`);
      console.log(`  → ${goal.requires.length} checks`);
    } else {
      // Rebuild the file with updated frontmatter
      const newRaw = matter.stringify(body, data);
      fs.writeFileSync(filePath, newRaw, 'utf8');
    }

    applied++;
  }

  console.log(`\n${'─'.repeat(50)}`);
  console.log(`${dryRun ? 'DRY RUN' : 'APPLIED'}`);
  console.log(`  Goals generated: ${applied}`);
  console.log(`  Already had goal: ${alreadyHasGoal}`);
  console.log(`  Skipped (snippets/no-archetype): ${skipped}`);
  console.log(`  Total files: ${files.length}`);

  if (dryRun) {
    console.log(`\nRun with --apply to write goals into frontmatter.`);
  }
}

main();
