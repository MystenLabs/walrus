// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/**
 * Quality evaluation for interactive diagram fallback assets.
 *
 * check-diagram-fallbacks.mjs answers one question: do the three artifacts
 * exist? That is necessary and not sufficient. A diagram passes that gate with
 * a blank SVG and a one-pixel PNG, which is the obvious way to satisfy a
 * presence check without giving a reader anything.
 *
 * This script evaluates the fallbacks that do exist, across four dimensions:
 *
 *   substance     - the fallback holds real content rather than a placeholder
 *   accessibility - the SVG names itself, which is what a screen reader reads
 *   fidelity      - the fallback is not older than the diagram it stands in for
 *   weight        - the interactive HTML stays within a size budget
 *
 * Presence is deliberately not re-checked here. A diagram with no fallbacks is
 * reported as unevaluated rather than as a failure, so that the two scripts
 * report on different things and neither hides the other.
 *
 * Warnings do not fail by default, because the repository starts below these
 * thresholds and teams switch off a gate nobody can pass. Pass --strict to
 * turn findings into a non-zero exit once coverage arrives.
 *
 * Usage:
 *   node eval-diagram-quality.mjs --all              # evaluate every diagram
 *   node eval-diagram-quality.mjs --base <git-ref>   # evaluate changed diagrams
 *   node eval-diagram-quality.mjs [file...]          # evaluate the given files
 *
 *   --strict   exit 1 when any finding is reported
 *   --json     emit machine-readable output for dashboards
 */

import { execFileSync } from "node:child_process";
import { existsSync, readFileSync, statSync } from "node:fs";
import path from "node:path";

const DIAGRAM_PATTERN = /(^|\/)interactive_[^/]+\.html?$/;
const EXCLUDED_PATH_FRAGMENTS = ["/.cache-walrus-memory/", "/node_modules/", "/build/"];

// An SVG carrying a real diagram is comfortably larger than this; the floor is
// set to catch empty documents and one-shape placeholders, not to judge detail.
const MIN_SVG_BYTES = 1024;
// A PNG export of a diagram at any usable resolution clears this easily. A
// 1x1 transparent PNG is about 70 bytes.
const MIN_PNG_BYTES = 4096;
// Interactive diagrams are self-contained, so they carry their own scripts and
// styles and run large. This budget flags the ones a reader waits on.
const MAX_HTML_BYTES = 2 * 1024 * 1024;

// Elements that mean the SVG actually draws something.
const SVG_CONTENT_PATTERN = /<(path|rect|circle|ellipse|line|polygon|polyline|text|image|use)\b/i;

function gitLines(args) {
    return execFileSync("git", args, { encoding: "utf8" })
        .split("\n")
        .map((line) => line.trim())
        .filter(Boolean);
}

function isExcluded(file) {
    return EXCLUDED_PATH_FRAGMENTS.some((fragment) => `/${file}`.includes(fragment));
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
        return gitLines([
            "diff",
            "--name-only",
            "--diff-filter=AMR",
            `${base}...HEAD`,
        ]).filter((file) => DIAGRAM_PATTERN.test(file) && !isExcluded(file));
    }

    return argv.filter((file) => DIAGRAM_PATTERN.test(file) && !isExcluded(file));
}

/** Last commit timestamp for a path, or null when git does not know the file. */
function lastCommitSeconds(file) {
    try {
        const [stamp] = gitLines(["log", "-1", "--format=%ct", "--", file]);
        return stamp ? Number(stamp) : null;
    } catch {
        return null;
    }
}

function humanBytes(bytes) {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KiB`;
    return `${(bytes / (1024 * 1024)).toFixed(1)} MiB`;
}

function evaluate(file) {
    const dir = path.dirname(file);
    const stem = path.basename(file).replace(/\.html?$/, "");
    const svg = path.join(dir, `${stem}.svg`);
    const png = path.join(dir, `${stem}.png`);
    const findings = [];

    // Presence belongs to check-diagram-fallbacks.mjs. Report and step aside so
    // the two scripts never disagree about the same file.
    if (!existsSync(svg) || !existsSync(png)) {
        return { file, evaluated: false, findings };
    }

    const htmlBytes = statSync(file).size;
    if (htmlBytes > MAX_HTML_BYTES) {
        findings.push({
            dimension: "weight",
            message:
                `interactive HTML is ${humanBytes(htmlBytes)}, over the ` +
                `${humanBytes(MAX_HTML_BYTES)} budget`,
        });
    }

    const svgBytes = statSync(svg).size;
    if (svgBytes < MIN_SVG_BYTES) {
        findings.push({
            dimension: "substance",
            message: `${path.basename(svg)} is ${humanBytes(svgBytes)}, which reads as a placeholder`,
        });
    }

    const svgSource = readFileSync(svg, "utf8");
    if (!SVG_CONTENT_PATTERN.test(svgSource)) {
        findings.push({
            dimension: "substance",
            message: `${path.basename(svg)} contains no drawing elements`,
        });
    }

    // The accessibility rationale in the standard rests on the fallback being
    // readable by assistive technology, and an untitled SVG announces nothing.
    if (!/<title\b/i.test(svgSource)) {
        findings.push({
            dimension: "accessibility",
            message: `${path.basename(svg)} has no <title>, so a screen reader has nothing to announce`,
        });
    }

    const pngBytes = statSync(png).size;
    if (pngBytes < MIN_PNG_BYTES) {
        findings.push({
            dimension: "substance",
            message: `${path.basename(png)} is ${humanBytes(pngBytes)}, which reads as a placeholder`,
        });
    }

    // A fallback older than its diagram is worse than a missing one, because
    // the presence gate reports it as compliant while it shows stale content.
    const htmlCommitted = lastCommitSeconds(file);
    for (const sibling of [svg, png]) {
        const siblingCommitted = lastCommitSeconds(sibling);
        if (htmlCommitted && siblingCommitted && siblingCommitted < htmlCommitted) {
            findings.push({
                dimension: "fidelity",
                message: `${path.basename(sibling)} was last updated before the diagram it stands in for`,
            });
        }
    }

    return { file, evaluated: true, findings };
}

const argv = process.argv.slice(2);
const strict = argv.includes("--strict");
const asJson = argv.includes("--json");
const targets = collectTargets(argv.filter((arg) => !arg.startsWith("--") || arg === "--all"));

if (targets.length === 0) {
    if (asJson) {
        console.log(JSON.stringify({ targets: 0, evaluated: 0, unevaluated: 0, findings: [] }, null, 2));
    } else {
        console.log("No interactive diagram files to evaluate.");
    }
    process.exit(0);
}

const results = targets.map(evaluate);
const evaluated = results.filter((result) => result.evaluated);
const unevaluated = results.filter((result) => !result.evaluated);
const withFindings = evaluated.filter((result) => result.findings.length > 0);
const findingCount = withFindings.reduce((total, result) => total + result.findings.length, 0);

if (asJson) {
    console.log(
        JSON.stringify(
            {
                targets: targets.length,
                evaluated: evaluated.length,
                unevaluated: unevaluated.map((result) => result.file),
                findings: withFindings.flatMap((result) =>
                    result.findings.map((finding) => ({ file: result.file, ...finding })),
                ),
            },
            null,
            2,
        ),
    );
} else {
    console.log(
        `Evaluated ${evaluated.length} of ${targets.length} interactive diagram(s).`,
    );

    if (unevaluated.length > 0) {
        console.log(
            `\n${unevaluated.length} diagram(s) skipped because their fallback assets are missing. ` +
                "Run check-diagram-fallbacks.mjs to list them.",
        );
    }

    if (findingCount > 0) {
        console.log(`\n${findingCount} finding(s):\n`);
        for (const result of withFindings) {
            console.log(`  ${result.file}`);
            for (const finding of result.findings) {
                console.log(`    [${finding.dimension}] ${finding.message}`);
            }
        }
    } else if (evaluated.length > 0) {
        console.log("No quality findings.");
    }
}

process.exit(strict && findingCount > 0 ? 1 : 0);
