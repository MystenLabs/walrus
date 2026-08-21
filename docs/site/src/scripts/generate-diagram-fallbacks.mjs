// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/**
 * Generator for interactive diagram fallback assets.
 *
 * The interactive diagrams standard requires an SVG source and a PNG export
 * beside every interactive_<topic>_v<n>.html. check-diagram-fallbacks.mjs
 * enforces that and eval-diagram-quality.mjs judges the result, but until now
 * nothing produced them, so contributors had to export by hand.
 *
 * The diagrams render their content as a single inline SVG that already
 * carries xmlns, a viewBox, role="img", a <title>, and a <desc>, and styles
 * itself with presentation attributes rather than document CSS. That makes the
 * rendered SVG self-contained, so the fallback is a faithful serialization of
 * what the reader sees rather than a re-drawing or a traced raster.
 *
 * Playwright is not a dependency of this package. CI images already ship
 * Chromium, and adding a browser automation library to the docs site for a
 * generation step that runs rarely is not worth the install cost. Install it
 * in your working copy when you need to regenerate:
 *
 *   npm install --no-save playwright-core
 *
 * Usage:
 *   node generate-diagram-fallbacks.mjs --all           # regenerate everything
 *   node generate-diagram-fallbacks.mjs [file...]       # regenerate the given diagrams
 *
 *   --missing-only   skip diagrams that already have both assets
 *   --scale <n>      PNG device scale factor (default 2)
 */

import { execFileSync } from "node:child_process";
import { existsSync, writeFileSync } from "node:fs";
import path from "node:path";

const DIAGRAM_PATTERN = /(^|\/)interactive_[^/]+\.html?$/;
const EXCLUDED_PATH_FRAGMENTS = ["/.cache-walrus-memory/", "/node_modules/", "/build/"];

// Where Playwright's browsers live in the CI image and the dev containers.
const CHROMIUM_CANDIDATES = [
    process.env.CHROMIUM_PATH,
    "/opt/pw-browsers/chromium-1194/chrome-linux/chrome",
    "/opt/pw-browsers/chromium/chrome-linux/chrome",
].filter(Boolean);

// The diagrams lay out to a fixed width; render wider so nothing reflows, then
// crop to the SVG's own bounding box.
const VIEWPORT = { width: 1600, height: 1200 };

// Several diagrams run a walkthrough that reveals elements in sequence and then
// loops, so a fixed wait captures an arbitrary frame with parts of the diagram
// still faded out. The fallback has to show the whole thing, so poll until no
// element is partially transparent and capture that frame. Static diagrams
// satisfy this immediately; the observed loop period is about 24 seconds, so
// the timeout allows for a full cycle plus margin. Diagrams do not honour
// prefers-reduced-motion, so emulating it does not help here.
const REVEAL_POLL_MS = 500;
const REVEAL_TIMEOUT_MS = 40000;

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
    return argv.filter((arg) => DIAGRAM_PATTERN.test(arg) && !isExcluded(arg));
}

function resolveChromium() {
    const found = CHROMIUM_CANDIDATES.find((candidate) => existsSync(candidate));
    if (!found) {
        console.error(
            "Could not find a Chromium binary. Looked in:\n" +
                CHROMIUM_CANDIDATES.map((c) => `  ${c}`).join("\n") +
                "\nSet CHROMIUM_PATH to override.",
        );
        process.exit(2);
    }
    return found;
}

/**
 * Poll until the diagram shows every element at full opacity, which is the end
 * state of the reveal walkthrough and the only frame worth freezing. Returns
 * false when no such frame appears before the timeout.
 */
async function waitForFullReveal(page) {
    const deadline = Date.now() + REVEAL_TIMEOUT_MS;
    while (Date.now() < deadline) {
        const faded = await page.evaluate(() => {
            const svg = document.querySelector("svg");
            if (!svg) return -1;
            return [...svg.querySelectorAll("*")].filter((node) => {
                const opacity = node.getAttribute("opacity") ?? node.style.opacity;
                return opacity !== null && opacity !== "" && Number(opacity) < 0.9;
            }).length;
        });
        if (faded === 0) return true;
        await page.waitForTimeout(REVEAL_POLL_MS);
    }
    return false;
}

async function loadPlaywright() {
    try {
        return await import("playwright-core");
    } catch {
        console.error(
            "playwright-core is not installed. This package does not depend on it, " +
                "because the generation step runs rarely. Install it for this run:\n\n" +
                "  npm install --no-save playwright-core\n",
        );
        process.exit(2);
    }
}

const argv = process.argv.slice(2);
const missingOnly = argv.includes("--missing-only");
const scaleIndex = argv.indexOf("--scale");
const scale = scaleIndex === -1 ? 2 : Number(argv[scaleIndex + 1]) || 2;

const targets = collectTargets(argv);
if (targets.length === 0) {
    console.log("No interactive diagram files to generate fallbacks for.");
    process.exit(0);
}

const { chromium } = await loadPlaywright();
const browser = await chromium.launch({ executablePath: resolveChromium() });

let written = 0;
let skipped = 0;
const failures = [];

for (const file of targets) {
    const dir = path.dirname(file);
    const stem = path.basename(file).replace(/\.html?$/, "");
    const svgPath = path.join(dir, `${stem}.svg`);
    const pngPath = path.join(dir, `${stem}.png`);

    if (missingOnly && existsSync(svgPath) && existsSync(pngPath)) {
        skipped += 1;
        continue;
    }

    const page = await browser.newPage({ viewport: VIEWPORT, deviceScaleFactor: scale });
    const pageErrors = [];
    page.on("pageerror", (error) => pageErrors.push(String(error).slice(0, 200)));

    try {
        await page.goto(`file://${path.resolve(file)}`, {
            waitUntil: "networkidle",
            timeout: 60000,
        });

        const handle = await page.$("svg");
        if (!handle) {
            failures.push({ file, reason: "no <svg> element in the rendered page" });
            continue;
        }

        const revealed = await waitForFullReveal(page);
        if (!revealed) {
            failures.push({
                file,
                reason:
                    `no fully revealed frame within ${REVEAL_TIMEOUT_MS / 1000}s; ` +
                    "capturing now would bake a partial walkthrough frame into the fallback",
            });
            continue;
        }

        // A diagram that threw while drawing may have rendered partially, and a
        // partial fallback is worse than none because it passes the presence gate.
        if (pageErrors.length > 0) {
            failures.push({ file, reason: `page errors during render: ${pageErrors[0]}` });
            continue;
        }

        const markup = await handle.evaluate((node) => node.outerHTML);
        if (!/<title\b/i.test(markup)) {
            failures.push({
                file,
                reason: "rendered SVG has no <title>; a screen reader would announce nothing",
            });
            continue;
        }

        writeFileSync(svgPath, `<?xml version="1.0" encoding="UTF-8"?>\n${markup}\n`, "utf8");
        await handle.screenshot({ path: pngPath });
        written += 1;
        console.log(`  wrote ${svgPath} and ${pngPath}`);
    } catch (error) {
        failures.push({ file, reason: String(error).slice(0, 200) });
    } finally {
        await page.close();
    }
}

await browser.close();

console.log(
    `\nGenerated fallbacks for ${written} diagram(s)` +
        (skipped > 0 ? `, skipped ${skipped} that already had both assets` : "") +
        ".",
);

if (failures.length > 0) {
    console.error(`\n${failures.length} diagram(s) failed:`);
    for (const { file, reason } of failures) {
        console.error(`  ${file}\n    ${reason}`);
    }
    process.exit(1);
}
