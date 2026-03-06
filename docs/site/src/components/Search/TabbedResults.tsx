// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

import React from "react";

export default function TabbedResults({ activeTab, onChange, tabs, showTooltips = true }) {
    const suitooltip = "Search results from the official Sui Docs";
    const suinstooltip = "Search results from Sui Name Service";
    const movetooltip = "Search results from The Move Book";
    const dapptooltip = "Search results from the Sui ecosystem SDKs";
    const walrustooltip = "Search results from the Walrus decentralized storage platform";

    const isDark =
        typeof document !== "undefined" &&
        document.documentElement.getAttribute("data-theme") === "dark";
    const tabBg = isDark
        ? "var(--color-walrus-dark-gray-200)"
        : "var(--color-walrus-tusk)";

    return (
        <div
            className={
                "mb-4 flex justify-start border-2 border-solid border-transparent " +
                "rounded-t-lg border-b-[--color-walrus-dark-gray-300] dark:border-b-[--color-walrus-dark-gray-450]"
            }
        >
            {tabs.map(({ label, indexName, count }) => (
                <div className="relative group inline-block" key={indexName}>
                    <button
                        className={
                            "mr-4 flex items-center font-semibold text-sm lg:text-md " +
                            "xl:text-lg cursor-pointer " +
                            "dark:text-[--color-walrus-dark-gray-500] " +
                            (activeTab === indexName
                                ? "text-[--color-walrus-dark-gray-200] font-bold border-2 border-solid " +
                                  "border-transparent border-b-[--color-walrus-purple] dark:border-b-[--color-walrus-mint]"
                                : "border-transparent text-[--color-walrus-light-gray-400]")
                        }
                        style={{ backgroundColor: tabBg }}
                        onClick={() => onChange(indexName)}
                    >
                        {label}{" "}
                        <span
                            className={
                                "dark:text-[--color-walrus-dark-gray-500] text-xs rounded-full ml-1 py-1 " +
                                "px-2 border border-solid " +
                                (activeTab === indexName
                                    ? "dark:!text-[--color-walrus-mint] bg-transparent " +
                                      "border-[--color-walrus-dark-gray-200] dark:border-[--color-walrus-dark-gray-500]"
                                    : "border-transparent")
                            }
                            style={
                                activeTab !== indexName
                                    ? {
                                          backgroundColor: isDark
                                              ? "var(--color-walrus-dark-gray-300)"
                                              : "var(--color-walrus-light-gray-200)",
                                      }
                                    : undefined
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