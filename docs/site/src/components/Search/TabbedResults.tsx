// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

import React from "react";

export default function TabbedResults({ activeTab, onChange, tabs, showTooltips = true }) {
    const suitooltip = "Search results from the official Sui Docs";
    const suinstooltip = "Search results from Sui Name Service";
    const movetooltip = "Search results from The Move Book";
    const dapptooltip = "Search results from the Sui ecosystem SDKs";
    const walrustooltip = "Search results from the Walrus decentralized storage platform";
    return (
        <div
            className={
                "mb-4 flex justify-start border-2 border-solid border-transparent " +
                "rounded-t-lg border-b-wal-gray-50 dark:border-b-wal-white-30"
            }
        >
            {tabs.map(({ label, indexName, count }) => (
                <div className="relative group inline-block" key={indexName}>
                    <button
                        className={
                            "mr-4 flex items-center font-semibold text-sm lg:text-md " +
                            "xl:text-lg bg-[var(--ifm-background-color)] cursor-pointer " +
                            "dark:text-wal-white-80 " +
                            (activeTab === indexName
                                ? "text-wal-gray-80 font-bold border-2 border-solid " +
                                  "border-transparent border-b-wal-link-hover dark:border-b-wal-link"
                                : "border-transparent text-wal-grey-40")
                        }
                        onClick={() => onChange(indexName)}
                    >
                        {label}{" "}
                        <span
                            className={
                                "dark:text-wal-white-80 text-xs rounded-full ml-1 py-1 " +
                                "px-2 border border-solid " +
                                (activeTab === indexName
                                    ? "dark:!text-wal-green bg-transparent " +
                                      "border-wal-gray-80 dark:border-wal-white-80"
                                    : "bg-wal-gray-10 dark:bg-wal-white-30 border-transparent")
                            }
                        >
                            {count}
                        </span>
                    </button>
                    {showTooltips && (
                        <div
                            className={
                                "absolute bottom-full left-1/2 -translate-x-1/2 mb-2 " +
                                "w-max max-w-xs px-2 py-1 text-sm text-white bg-gray-800 " +
                                "rounded tooltip-delay"
                            }
                        >
                            {label === "Sui"
                                ? suitooltip
                                : label === "SuiNS"
                                    ? suinstooltip
                                    : label === "The Move Book"
                                        ? movetooltip
                                        : label === "SDKs"
                                            ? dapptooltip
                                            : walrustooltip}
                        </div>
                    )}
                </div>
            ))}
        </div>
    );
}
