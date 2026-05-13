// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

const { decode } = require("he");

export function truncateAtWord(text, maxChars = 250) {
    if (text.length <= maxChars) return text;
    const decoded = decode(text);
    const truncated = decoded.slice(0, maxChars);
    return truncated.slice(0, truncated.lastIndexOf(" ")) + "…";
}

export function getDeepestHierarchyLabel(hierarchy) {
    const levels = ["lvl0", "lvl1", "lvl2", "lvl3", "lvl4", "lvl5", "lvl6"];
    let lastValue = null;

    for (const lvl of levels) {
        const value = hierarchy[lvl];
        if (value == null) {
            break;
        }
        lastValue = value;
    }

    return lastValue || hierarchy.lvl6 || "";
}

/**
 * Strip tooltip text injected by the Algolia crawler.
 * Pattern: a word is immediately followed by itself + a capital-letter
 * definition ending in a period.
 */
export function cleanTooltipText(text: string): string {
    // Remove zero-width spaces
    let cleaned = text.replace(/\u200B/g, "");
    // Strip duplicated-word tooltip definitions
    cleaned = cleaned.replace(/(\b\w{2,})\1[A-Z][^.]*\.\s?/g, "$1 ");
    return cleaned.trim();
}

/**
 * Build an ordered breadcrumb array from a DocSearch hierarchy object.
 * Deduplicates adjacent identical levels. Strips crawler tooltip artefacts.
 */
export function getHierarchyBreadcrumbs(hierarchy): string[] {
    if (!hierarchy) return [];
    const levels = ["lvl0", "lvl1", "lvl2", "lvl3", "lvl4", "lvl5", "lvl6"];
    const crumbs: string[] = [];
    for (const lvl of levels) {
        const raw = hierarchy[lvl];
        if (raw == null) break;
        const value = cleanTooltipText(raw);
        if (crumbs.length === 0 || crumbs[crumbs.length - 1] !== value) {
            crumbs.push(value);
        }
    }
    return crumbs;
}
