// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

import React from "react";
import { useHits } from "react-instantsearch";
import { useHistory } from "@docusaurus/router";
import { truncateAtWord } from "./utils";

// Strips the crawler "__" format: "Page Title__Section__" → "Page Title"
function cleanHierarchyValue(value: string | null | undefined): string {
    if (!value) return "";
    return value.split("__")[0].trim();
}

export default function CustomHitsContent({ name }) {
    const { hits: items } = useHits();
    const history = useHistory();
    const currentHost = typeof window !== "undefined" ? window.location.host : "";

    let siteToVisit = "Try your search again with different keywords";
    if (name === "sui_docs") {
        siteToVisit =
            `${siteToVisit}. If you are unable to find the information you need, ` +
            `try one of the official Sui support channels: ` +
            `<a href="https://github.com/MystenLabs/sui/issues/new/choose" ` +
            `target="_blank">GitHub</a>, ` +
            `<a href="https://discord.gg/Sui" target="_blank">Discord</a>, or ` +
            `<a href="https://t.me/SuiTokenNetwork" target="_blank">Telegram</a>.`;
    } else if (name === "suins_docs") {
        siteToVisit = `${siteToVisit} or visit the official <a href="https://docs.suins.io" target="_blank">SuiNS doc</a> site.`;
    } else if (name === "move_book") {
        siteToVisit = `${siteToVisit} or visit <a href="https://move-book.com/" target="_blank">The Move Book</a> dedicated site.`;
    } else if (name === "sui_sdks") {
        siteToVisit = `${siteToVisit} or visit the official <a href="https://sdk.mystenlabs.com" target="_blank">Sui SDKs</a> site.`;
    } else if (name === "walrus_docs") {
        siteToVisit = `${siteToVisit} or visit the official <a href="https://docs.wal.app/" target="_blank">Walrus Docs</a> site.`;
    } else {
        siteToVisit = `${siteToVisit}.`;
    }

    if (items.length === 0) {
        return (
            <>
                <p>No results found.</p>
                <p dangerouslySetInnerHTML={{ __html: siteToVisit }} />
            </>
        );
    }

    const grouped = items.reduce(
        (acc, hit) => {
            const key = hit.url_without_anchor;
            if (!acc[key]) acc[key] = [];
            acc[key].push(hit);
            return acc;
        },
        {} as Record<string, typeof items>,
    );

    return (
        <>
            {Object.entries(grouped).map(([key, group], index) => {
                // Clean group header — strips "__" crawler format from other indices
                const groupTitle =
                    cleanHierarchyValue(group[0].hierarchy?.lvl1) ||
                    cleanHierarchyValue(group[0].hierarchy?.lvl0) ||
                    group[0].title ||
                    "[no title]";

                return (
                    <div
                        className="p-6 pb-[40px] mb-6 bg-wal-gray-5 dark:bg-wal-white-10 rounded-[20px]"
                        key={index}
                    >
                        {/* Group header: page title */}
                        <div className="text-xl text-wal-purple-dark dark:text-wal-purple font-semibold mb-4">
                            {groupTitle}
                        </div>

                        <div>
                            {group.map((hit, i) => {
                                const level = hit.type;

                                // For content type, sectionTitle would duplicate the page title — skip it
                                // For lvl1/lvl2/lvl3, show the specific heading
                                const rawSectionTitle =
                                    level === "content" ? null : hit.hierarchy?.[level];
                                const sectionTitle =
                                    cleanHierarchyValue(rawSectionTitle) || null;

                                // Don't show sectionTitle if identical to group header
                                const showSectionTitle =
                                    sectionTitle && sectionTitle !== groupTitle;

                                const hitHost = new URL(hit.url).host;
                                const isInternal = hitHost === currentHost;

                                return (
                                    <div key={i} className="mb-2">
                                        {showSectionTitle && (
                                            isInternal ? (
                                                <button
                                                    onClick={() =>
                                                        history.push(new URL(hit.url).pathname)
                                                    }
                                                    className={
                                                        "text-base text-blue-600 " +
                                                        "hover:text-wal-green-dark underline " +
                                                        "text-left bg-transparent border-0 pl-0 " +
                                                        "cursor-pointer font-[Inter]"
                                                    }
                                                >
                                                    {sectionTitle}
                                                </button>
                                            ) : (
                                                <a
                                                    href={hit.url}
                                                    target="_blank"
                                                    rel="noopener noreferrer"
                                                    className="text-base text-wal-link underline pb-2"
                                                >
                                                    {sectionTitle}
                                                </a>
                                            )
                                        )}
                                        <p
                                            className="font-normal text-base text-wal-gray-50 dark:text-wal-white-80"
                                            dangerouslySetInnerHTML={{
                                                __html: hit.content
                                                    ? truncateAtWord(
                                                          hit._highlightResult.content.value,
                                                      )
                                                    : "",
                                            }}
                                        />
                                    </div>
                                );
                            })}
                        </div>
                    </div>
                );
            })}
        </>
    );
}
