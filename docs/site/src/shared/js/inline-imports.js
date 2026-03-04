// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

const fs = require("fs");
const path = require("path");

// ---------------------------------------------------------------------------
// Adjust these paths to match your project layout
// ---------------------------------------------------------------------------
const DOCS_DIR = path.resolve(__dirname, "../../../../content");
const GENERATED_MAP_PATH = path.resolve(
  __dirname,
  "../../.generated/ImportContentMap",
);
const SNIPPETS_DIR = path.resolve(__dirname, "../../../../content/snippets");
const UTILS_PATH = path.resolve(
  __dirname,
  "../components/ImportContent/utils",
);
const DEFAULT_OUT_DIR = path.resolve(__dirname, "../../../.markdown/with-imports");

// ---------------------------------------------------------------------------
// Import the project's own content map and utils
// ---------------------------------------------------------------------------
let importContentMap = {};
try {
  const mapRaw = fs.readFileSync(GENERATED_MAP_PATH + ".ts", "utf-8");
  // Extract the object literal from: export const importContentMap = { ... };
  const match = mapRaw.match(
    /export\s+const\s+importContentMap\s*=\s*(\{[\s\S]*\})\s*;?\s*$/,
  );
  if (!match) {
    throw new Error("Could not find importContentMap object in file");
  }
  // The values use regular string literals (double-quoted with \n escapes),
  // so eval is safe here on a machine-generated file.
  importContentMap = eval("(" + match[1] + ")");
} catch (err) {
  console.error(
    "⚠  Could not load importContentMap. Run `pnpm prebuild` first.\n",
    err,
  );
  process.exit(1);
}

let utils;
try {
  utils = require(UTILS_PATH);
  utils = utils.default ?? utils;
} catch (err) {
  console.error("⚠  Could not load ImportContent/utils.\n", err);
  process.exit(1);
}

// ---------------------------------------------------------------------------
// Parse CLI args
// ---------------------------------------------------------------------------
const args = process.argv.slice(2);
const outDirIdx = args.indexOf("--out-dir");
const outDir =
  outDirIdx !== -1 && args[outDirIdx + 1]
    ? path.resolve(args[outDirIdx + 1])
    : DEFAULT_OUT_DIR;

// ---------------------------------------------------------------------------
// Regex to match <ImportContent ... /> (self-closing) in MDX source.
// Handles multi-line attribute lists.
// ---------------------------------------------------------------------------
const IMPORT_CONTENT_RE = /<ImportContent\s+([\s\S]*?)\/>/g;

/**
 * Parse JSX-style attributes from the captured attribute string.
 * Handles: key="value", key='value', key={expr}, and bare boolean flags.
 *
 * @param {string} attrString
 * @returns {Record<string, string | boolean>}
 */
function parseAttributes(attrString) {
  const attrs = {};

  const attrRe =
    /(\w+)(?:\s*=\s*(?:"([^"]*)"|'([^']*)'|\{([^}]*)\}))?/g;

  let m;
  while ((m = attrRe.exec(attrString)) !== null) {
    const key = m[1];
    const value = m[2] ?? m[3] ?? m[4];
    if (value !== undefined) {
      attrs[key] = value.replace(/^["']|["']$/g, "");
    } else {
      // Bare attribute → boolean true (e.g. noComments, noTitle)
      attrs[key] = true;
    }
  }

  return attrs;
}

// ---------------------------------------------------------------------------
// Resolve the language for a given file extension (mirrors the component)
// ---------------------------------------------------------------------------
/**
 * @param {string | undefined} ext
 * @param {string} [explicit]
 * @returns {string}
 */
function resolveLanguage(ext, explicit) {
  if (explicit) return explicit;
  switch (ext) {
    case "lock":
      return "toml";
    case "sh":
      return "shell";
    case "mdx":
      return "markdown";
    case "tsx":
      return "ts";
    case "rs":
      return "rust";
    case "move":
      return "move";
    case "prisma":
      return "ts";
    default:
      return ext || "text";
  }
}

// ---------------------------------------------------------------------------
// Resolve a single <ImportContent /> invocation to markdown text
// ---------------------------------------------------------------------------
/**
 * @param {Record<string, string | boolean>} attrs
 * @returns {string}
 */
function resolveImportContent(attrs) {
  const source = /** @type {string} */ (attrs.source);
  const mode = /** @type {string} */ (attrs.mode);

  if (!source || !mode) {
    return `<!-- ImportContent: missing source or mode -->`;
  }

  // ── Snippet mode ──────────────────────────────────────────────────────
  if (mode === "snippet") {
    const normalized = source.replace(/^\.\//, "");
    const candidates = [
      path.join(SNIPPETS_DIR, normalized),
      path.join(SNIPPETS_DIR, normalized + ".mdx"),
      path.join(SNIPPETS_DIR, normalized + ".md"),
    ];

    for (const candidate of candidates) {
      if (fs.existsSync(candidate)) {
        let snippetContent = fs.readFileSync(candidate, "utf-8");
        // Strip frontmatter if present
        snippetContent = snippetContent.replace(/^---[\s\S]*?---\s*\n/, "");
        // Strip import statements (they're React-only)
        snippetContent = snippetContent.replace(
          /^import\s+.*?;\s*\n/gm,
          "",
        );
        // Strip export statements
        snippetContent = snippetContent.replace(
          /^export\s+.*?;\s*\n/gm,
          "",
        );
        return snippetContent.trim();
      }
    }

    return `<!-- ImportContent: snippet not found: ${source} -->`;
  }

  // ── Code mode ─────────────────────────────────────────────────────────
  const isGitHub = Boolean(attrs.org && attrs.repo);

  // GitHub content can't be resolved at build time without network access.
  // Leave a placeholder that the export consumer can optionally fill later.
  if (isGitHub) {
    const org = attrs.org;
    const repo = attrs.repo;
    const branch = attrs.ref || "main";
    const filePath = source.replace(/^\.\/?/, "");
    const url = `https://github.com/${org}/${repo}/blob/${branch}/${filePath}`;
    return `<!-- ImportContent: GitHub source — resolve at export time or visit ${url} -->`;
  }

  const cleaned = source.replace(/^\/+/, "").replace(/^\.\//, "");
  const match = cleaned.match(/\.([^.]+)$/);
  const ext = match ? match[1] : undefined;
  const resolvedLanguage = resolveLanguage(ext, /** @type {string} */ (attrs.language));

  let content = importContentMap[cleaned];
  if (content == null) {
    return `<!-- ImportContent: file not found in manifest: ${cleaned} -->`;
  }

  // Apply the same transformations as the React component
  let out = content
    .replace(
      /^\/\/\s*Copyright.*Mysten Labs.*\n\/\/\s*SPDX-License.*?\n?$/gim,
      "",
    )
    .replace(
      /\[dependencies\]\nsui\s?=\s?{\s?local\s?=.*sui-framework.*\n/i,
      "[dependencies]",
    );

  if (attrs.tag) {
    out = utils.returnTag(out, attrs.tag);
  }
  if (attrs.module) {
    out = utils.returnModules(out, attrs.module);
  }
  if (attrs.component) {
    out = utils.returnComponents(source, attrs.component);
  }
  if (attrs.fun) {
    out = utils.returnFunctions(
      out,
      attrs.fun,
      resolvedLanguage,
      Boolean(attrs.signatureOnly),
    );
  }
  if (attrs.variable) {
    out = utils.returnVariables(out, attrs.variable, resolvedLanguage);
  }
  if (attrs.struct) {
    out = utils.returnStructs(out, attrs.struct, resolvedLanguage);
  }
  if (attrs.type) {
    out = utils.returnTypes(out, attrs.type);
  }
  if (attrs.impl) {
    out = utils.returnImplementations(out, attrs.impl);
  }
  if (attrs.trait) {
    out = utils.returnTraits(out, attrs.trait);
  }
  if (attrs.enumeration) {
    out = utils.returnEnums(out, attrs.enumeration);
  }
  if (attrs.dep) {
    out = utils.returnDeps(out, attrs.dep);
  }
  if (attrs.test) {
    out = utils.returnTests(out, attrs.test);
  }

  // Remove docs:: comments
  out = out.replace(/^\s*\/\/\s*docs::\/?.*\r?$\n?/gm, "");

  if (attrs.noTests) {
    out = utils.returnNotests(out);
  }
  if (attrs.noComments) {
    out = out.replace(/^ *\/\/.*\n/gm, "");
  }

  // Remove leading blank line
  out = out.replace(/^\s*\n/, "");

  // For markdown/md style, return raw text
  if (/^m(?:d|arkdown)$/i.test(String(attrs.style || ""))) {
    return out.trim();
  }

  // Default: wrap in a fenced code block
  const title = attrs.org
    ? `github.com/${attrs.org}/${attrs.repo}/${cleaned}`
    : cleaned;
  const titlePart = attrs.noTitle ? "" : ` title="${title}"`;
  return `\`\`\`${resolvedLanguage}${titlePart}\n${out.trimEnd()}\n\`\`\``;
}

// ---------------------------------------------------------------------------
// Process a single MDX file
// ---------------------------------------------------------------------------
/**
 * @param {string} mdxPath
 * @returns {{ original: string, resolved: string, changed: boolean }}
 */
function processFile(mdxPath) {
  const original = fs.readFileSync(mdxPath, "utf-8");
  let changed = false;

  const resolved = original.replace(IMPORT_CONTENT_RE, (_fullMatch, attrStr) => {
    const attrs = parseAttributes(attrStr);
    changed = true;
    const inlined = resolveImportContent(attrs);

    return [
      `<!-- IMPORT_CONTENT_RESOLVED source="${attrs.source}" mode="${attrs.mode}" -->`,
      inlined,
      `<!-- /IMPORT_CONTENT_RESOLVED -->`,
    ].join("\n");
  });

  return { original, resolved, changed };
}

// ---------------------------------------------------------------------------
// Copy a file, creating parent directories as needed
// ---------------------------------------------------------------------------
function copyFile(src, dest) {
  fs.mkdirSync(path.dirname(dest), { recursive: true });
  fs.copyFileSync(src, dest);
}

// ---------------------------------------------------------------------------
// Main — full mirror mode
// ---------------------------------------------------------------------------
function main() {
  const allFiles = fs.readdirSync(DOCS_DIR, {
    recursive: true,
    encoding: "utf-8",
  });

  // Filter out node_modules and directories (readdirSync with recursive
  // returns both files and dirs on some Node versions)
  const files = allFiles.filter((f) => {
    if (f.includes("node_modules")) return false;
    const fullPath = path.join(DOCS_DIR, f);
    try {
      return fs.statSync(fullPath).isFile();
    } catch {
      return false;
    }
  });

  console.log(`Mirroring ${files.length} files from ${DOCS_DIR} → ${outDir}`);

  let resolvedCount = 0;
  let copiedCount = 0;

  for (const relFile of files) {
    const srcPath = path.join(DOCS_DIR, relFile);
    const destPath = path.join(outDir, relFile);
    const isMdx = /\.mdx?$/.test(relFile);

    if (isMdx) {
      const { resolved, changed } = processFile(srcPath);

      fs.mkdirSync(path.dirname(destPath), { recursive: true });
      // Always write the file (resolved or not) so the mirror is complete
      fs.writeFileSync(destPath, resolved, "utf-8");

      if (changed) {
        resolvedCount++;
        console.log(`  ✔ ${relFile} (resolved)`);
      } else {
        copiedCount++;
      }
    } else {
      // Non-MDX file: copy as-is
      copyFile(srcPath, destPath);
      copiedCount++;
    }
  }

  console.log(
    `\nDone. Mirrored ${files.length} files:` +
      `\n  ${resolvedCount} MDX file(s) with ImportContent resolved` +
      `\n  ${copiedCount} file(s) copied as-is`,
  );
}

main();
