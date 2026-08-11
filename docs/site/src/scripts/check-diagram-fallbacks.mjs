// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/**
 * CI gate for interactive diagram fallback assets.
 *
 * The interactive diagrams standard requires that every interactive diagram
 * ships as three artifacts in the same directory:
 *
 *   1. interactive_<topic>_v<n>.html  - the self-contained interactive HTML
 *   2. interactive_<topic>_v<n>.svg   - the static fallback SVG source
 *   3. interactive_<topic>_v<n>.png   - the fallback PNG export
 *
 * The fallback carries accessibility: it is what a screen reader, a print
 * export, and a reduced-motion reader depend on. This script fails when an
 * interactive diagram HTML file is added or modified without its complete
 * fallback set.
 *
 * The check is diff-scoped so that pre-existing diagrams without fallbacks
 * only start failing once a PR touches them.
 *
 * Usage:
 *   node check-diagram-fallbacks.mjs --base <git-ref>   # check files changed since <git-ref>
 *   node check-diagram-fallbacks.mjs [file...]          # check the given files
 *   node check-diagram-fallbacks.mjs --all              # audit every diagram in the repo
 */

import { execFileSync } from "node:child_process";
import { existsSync } from "node:fs";
import path from "node:path";

const DIAGRAM_PATTERN = /(^|\/)interactive_[^/]+\.html?$/;
// Build caches contain copies of the diagram files; they are not sources.
const EXCLUDED_PATH_FRAGMENTS = ["/.cache-walrus-memory/", "/node_modules/", "/build/"];
const STANDARD_REFERENCE =
    "See the interactive diagrams standard (docs/site/README.md, " +
    "\"Interactive diagram fallback assets\").";

function gitLines(args) {
    return execFileSync("git", args, { encoding: "utf8" })
        .split("\n")
        .map((line) => line.trim())
        .filter(Boolean);
}

function isExcluded(file) {
    const normalized = `/${file}`;
    return EXCLUDED_PATH_FRAGMENTS.some((fragment) => normalized.includes(fragment));
}

function collectTargets(argv) {
    if (argv.includes("--all")) {
        return gitLines(["ls-files", "*.html"]).filter(
            (file) => DIAGRAM_PATTERN.test(file) && !isExcluded(file),
        );
    }

    const baseIndex = argv.indexOf("--base");
    if (baseIndex !== -1) {
        const base = argv[baseIndex + 1];
        if (!base) {
            console.error("--base requires a git ref argument.");
            process.exit(2);
        }
        // Added and modified files only: deleting a diagram must not fail the
        // check, and renames surface as an addition of the new path.
        return gitLines([
            "diff",
            "--name-only",
            "--diff-filter=AMR",
            `${base}...HEAD`,
        ]).filter((file) => DIAGRAM_PATTERN.test(file) && !isExcluded(file));
    }

    return argv.filter((file) => DIAGRAM_PATTERN.test(file) && !isExcluded(file));
}

const targets = collectTargets(process.argv.slice(2));

if (targets.length === 0) {
    console.log("No added or modified interactive diagram files to check.");
    process.exit(0);
}

const failures = [];

for (const file of targets) {
    const dir = path.dirname(file);
    const stem = path.basename(file).replace(/\.html?$/, "");
    const missing = [".svg", ".png"]
        .map((ext) => path.join(dir, `${stem}${ext}`))
        .filter((sibling) => !existsSync(sibling));
    if (missing.length > 0) {
        failures.push({ file, missing });
    }
}

if (failures.length === 0) {
    console.log(`Checked ${targets.length} interactive diagram file(s); all fallback assets present.`);
    process.exit(0);
}

console.error("Interactive diagrams are missing fallback assets:\n");
for (const { file, missing } of failures) {
    console.error(`  ${file}`);
    for (const sibling of missing) {
        console.error(`    missing: ${sibling}`);
    }
}
console.error(
    "\nEvery interactive diagram must ship its static fallback SVG source and " +
        "PNG export next to the HTML file. " +
        STANDARD_REFERENCE,
);
process.exit(1);
