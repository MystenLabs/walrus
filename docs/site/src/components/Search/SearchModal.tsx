// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

import React, { useState, useEffect } from "react";
import { liteClient as algoliasearch } from "algoliasearch/lite";
import {
    InstantSearch,
    useInfiniteHits,
    useInstantSearch,
    Index,
} from "react-instantsearch";
import {
    truncateAtWord,
    getDeepestHierarchyLabel,
    getHierarchyBreadcrumbs,
    cleanTooltipText,
} from "./utils";
import ControlledSearchBox from "./ControlledSearchBox";
import TabbedResults from "./TabbedResults";

const baseSearchClient = algoliasearch(
    "M9JD2UP87M",
    "826134b026a63bb35692f08f1dc85d1c",
);

const searchClient = {
    ...baseSearchClient,
    search(requests: any[]) {
        const hasValidQuery = requests.some(
            (req) => req.params?.query?.length >= 3,
        );
        if (!hasValidQuery) {
            return Promise.resolve({
                results: requests.map(() => ({
                    hits: [],
                    nbHits: 0,
                    processingTimeMS: 0,
                })),
            });
        }
        return baseSearchClient.search(requests);
    },
};

const indices = [
    { label: "Walrus", indexName: "walrus_docs" },
    { label: "Sui", indexName: "sui_docs" },
    { label: "SuiNS", indexName: "suins_docs" },
    { label: "The Move Book", indexName: "move_book" },
    { label: "SDKs", indexName: "sui_sdks" },
];

// Shared inline style objects so the modal bg is always visible
const modalBg = "var(--color-walrus-tusk)";
const modalBgDark = "var(--color-walrus-dark-gray-200)";

function useIsDark() {
    const [dark, setDark] = useState(false);
    useEffect(() => {
        const check = () =>
            setDark(
                document.documentElement.getAttribute("data-theme") ===
                    "dark",
            );
        check();
        const obs = new MutationObserver(check);
        obs.observe(document.documentElement, {
            attributes: true,
            attributeFilter: ["data-theme"],
        });
        return () => obs.disconnect();
    }, []);
    return dark;
}

function HitItem({ hit }: { hit: any }) {
    const crumbs = getHierarchyBreadcrumbs(hit.hierarchy);
    const title =
        crumbs.length > 0
            ? crumbs[crumbs.length - 1]
            : cleanTooltipText(hit.hierarchy?.lvl0 || "Untitled");
    const breadcrumb = crumbs.length > 1 ? crumbs.slice(0, -1) : [];

    return (
        <a
            href={hit.url}
            className="modal-result block px-4 py-3 -mx-2 rounded-lg no-underline hover:bg-black/5 dark:hover:bg-white/5 transition-colors"
        >
            {breadcrumb.length > 0 && (
                <div
                    className={
                        "text-xs mb-1 truncate" +
                        " text-[--color-walrus-dark-gray-300]" +
                        " dark:text-[--color-walrus-dark-gray-450]"
                    }
                >
                    {breadcrumb.join(" > ")}
                </div>
            )}
            <div
                className={
                    "text-sm font-medium" +
                    " text-[--color-walrus-dark-gray-100]" +
                    " dark:text-[--color-walrus-white-90]"
                }
            >
                {title}
            </div>
            {hit.content && (
                <p
                    className={
                        "text-xs mt-1 mb-0 line-clamp-2" +
                        " text-[--color-walrus-dark-gray-400]" +
                        " dark:text-[--color-walrus-dark-gray-500]"
                    }
                    dangerouslySetInnerHTML={{
                        __html: truncateAtWord(
                            hit._highlightResult.content.value,
                            120,
                        ),
                    }}
                />
            )}
        </a>
    );
}

function HitsList({
    scrollContainerRef,
}: {
    scrollContainerRef: React.RefObject<HTMLDivElement>;
}) {
    const { hits, isLastPage, showMore } = useInfiniteHits();

    useEffect(() => {
        const el = scrollContainerRef.current;
        if (!el) return;

        const handleScroll = () => {
            const atBottom =
                el.scrollTop + el.clientHeight >= el.scrollHeight - 1;
            if (atBottom && !isLastPage) {
                showMore();
            }
        };

        el.addEventListener("scroll", handleScroll);
        return () => el.removeEventListener("scroll", handleScroll);
    }, [isLastPage, showMore, scrollContainerRef]);

    return (
        <div>
            {hits.map((hit) => (
                <HitItem key={hit.objectID} hit={hit} />
            ))}
        </div>
    );
}

function EmptyState({ label }: { label: string }) {
    const { results } = useInstantSearch();
    if (results?.hits?.length === 0) {
        return (
            <p
                className={
                    "text-sm text-[--color-walrus-dark-gray-300]" +
                    " dark:text-[--color-walrus-dark-gray-450]"
                }
            >
                No results in {label}
            </p>
        );
    }
    return null;
}

function ResultsUpdater({
    indexName,
    onUpdate,
}: {
    indexName: string;
    onUpdate: (index: string, count: number) => void;
}) {
    const { results } = useInstantSearch();
    const previousHitsRef = React.useRef<number | null>(null);
    useEffect(() => {
        if (results && results.nbHits !== previousHitsRef.current) {
            previousHitsRef.current = results.nbHits;
            onUpdate(indexName, results.nbHits);
        }
    }, [results?.nbHits, indexName, onUpdate, results]);
    return null;
}

export default function MultiIndexSearchModal({
    isOpen,
    onClose,
}: {
    isOpen: boolean;
    onClose: () => void;
}) {
    const isDark = useIsDark();
    const bg = isDark ? modalBgDark : modalBg;

    const [activeIndex, setActiveIndex] = useState(
        indices[0].indexName,
    );
    const [tabCounts, setTabCounts] = React.useState<
        Record<string, number>
    >({
        walrus_docs: 0,
    });
    const [query, setQuery] = React.useState("");
    const scrollContainerRef =
        React.useRef<HTMLDivElement>(null);
    const searchBoxRef = React.useRef<HTMLInputElement>(null);
    useEffect(() => {
        if (isOpen) {
            document.body.style.overflow = "hidden";
            setTimeout(() => {
                searchBoxRef.current?.focus();
            }, 300);
        } else {
            document.body.style.overflow = "";
        }
        return () => {
            document.body.style.overflow = "";
        };
    }, [isOpen]);

    useEffect(() => {
        if (!isOpen) return;
        const handleKeyDown = (e: KeyboardEvent) => {
            if (e.key === "Escape") onClose();
        };
        document.addEventListener("keydown", handleKeyDown);
        return () =>
            document.removeEventListener("keydown", handleKeyDown);
    }, [isOpen, onClose]);

    const activeMeta = {
        walrus_docs: null,
        sui_docs: {
            label: "Sui Docs",
            url: "https://docs.sui.io",
        },
        suins_docs: {
            label: "SuiNS Docs",
            url: "https://docs.suins.io",
        },
        move_book: {
            label: "The Move Book",
            url: "https://move-book.com/",
        },
        sui_sdks: {
            label: "SDK Docs",
            url: "https://sdk.mystenlabs.com",
        },
    }[activeIndex];

    if (!isOpen) return null;

    const overlayBg = isDark
        ? "rgba(28,34,40,0.8)"
        : "rgba(83,87,90,0.7)";

    const footerClasses =
        "h-12 flex items-center justify-between text-xs" +
        " border-t border-solid" +
        " border-[--color-walrus-dark-gray-300]" +
        " border-b-transparent" +
        " border-l-transparent border-r-transparent";

    const footerLinkClasses =
        "text-[--color-walrus-purple]" +
        " hover:text-[--color-walrus-violet]" +
        " dark:text-[--color-walrus-mint]" +
        " dark:hover:text-[--color-walrus-violet] underline";

    const hintClasses =
        "text-xs text-[--color-walrus-dark-gray-300]" +
        " dark:text-[--color-walrus-dark-gray-450]" +
        " pl-4 mb-2 -mt-6";

    return (
        <div
            className="fixed inset-0 z-500 flex justify-center p-4"
            style={{ backgroundColor: overlayBg }}
            onClick={(e) => {
                if (e.target === e.currentTarget) onClose();
            }}
        >
            <div
                className={
                    "w-full max-w-4xl px-6 rounded-lg" +
                    " shadow-md max-h-[600px] flex flex-col"
                }
                style={{ backgroundColor: bg }}
            >
                <div
                    ref={scrollContainerRef}
                    className="flex-1 overflow-y-auto"
                >
                    <InstantSearch
                        searchClient={searchClient}
                        indexName={activeIndex}
                    >
                        <div
                            className="rounded-t sticky top-0 z-10"
                            style={{ backgroundColor: bg }}
                        >
                            <div
                                className="h-8 flex justify-end"
                                style={{ backgroundColor: bg }}
                            >
                                <button
                                    onClick={onClose}
                                    className={
                                        "bg-transparent border-none" +
                                        " outline-none text-xs" +
                                        " cursor-pointer" +
                                        " text-[--color-walrus-dark-gray-300]" +
                                        " dark:text-[--color-walrus-dark-gray-450]"
                                    }
                                >
                                    ESC
                                </button>
                            </div>
                            <ControlledSearchBox
                                placeholder={`Search`}
                                query={query}
                                onChange={setQuery}
                                inputRef={searchBoxRef}
                            />
                            {query.length < 3 && (
                                <p className={hintClasses}>
                                    Type at least 3 characters to
                                    search
                                </p>
                            )}
                            <TabbedResults
                                activeTab={activeIndex}
                                onChange={setActiveIndex}
                                showTooltips={false}
                                tabs={indices.map((tab) => ({
                                    ...tab,
                                    count:
                                        tabCounts[tab.indexName] ||
                                        0,
                                }))}
                            />
                        </div>
                        <div className="px-6 pb-4">
                            {indices.map((index) => (
                                <Index
                                    indexName={index.indexName}
                                    key={index.indexName}
                                >
                                    <ResultsUpdater
                                        indexName={
                                            index.indexName
                                        }
                                        onUpdate={(
                                            indexName,
                                            count,
                                        ) =>
                                            setTabCounts(
                                                (prev) => ({
                                                    ...prev,
                                                    [indexName]:
                                                        count,
                                                }),
                                            )
                                        }
                                    />
                                    {index.indexName ===
                                        activeIndex && (
                                        <>
                                            <HitsList
                                                scrollContainerRef={
                                                    scrollContainerRef
                                                }
                                            />
                                            <EmptyState
                                                label={
                                                    index.label
                                                }
                                            />
                                        </>
                                    )}
                                </Index>
                            ))}
                        </div>
                    </InstantSearch>
                </div>
                <div
                    className={footerClasses}
                    style={{ backgroundColor: bg }}
                >
                    <a
                        href={`/search?q=${encodeURIComponent(query)}`}
                        className={footerLinkClasses}
                    >
                        View all results
                    </a>
                    {activeMeta && (
                        <a
                            href={activeMeta.url}
                            target="_blank"
                            rel="noopener noreferrer"
                            className={footerLinkClasses}
                        >
                            Visit {activeMeta.label} →
                        </a>
                    )}
                </div>
            </div>
        </div>
    );
}
