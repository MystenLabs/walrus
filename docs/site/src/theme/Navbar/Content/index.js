// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

import * as React from "react";
import { useThemeConfig, ErrorCauseBoundary } from "@docusaurus/theme-common";
import {
  splitNavbarItems,
  useNavbarMobileSidebar,
} from "@docusaurus/theme-common/internal";
import NavbarItem from "@theme/NavbarItem";
import ThemeToggle from "@site/src/shared/components/ThemeToggle";
import NavbarMobileSidebarToggle from "@theme/Navbar/MobileSidebar/Toggle";
import NavbarSearch from "@theme/Navbar/Search";
import SearchModal from "@site/src/components/Search/SearchModal";
import Link from "@docusaurus/Link";
import useBaseUrl from "@docusaurus/useBaseUrl";
import { useThemeConfig as useThemeConfigFull } from "@docusaurus/theme-common";

function useNavbarItems() {
  return useThemeConfig().navbar.items;
}

// Safe wrapper: if the provider isn't present, don't crash
function useMobileSidebarSafe() {
  try {
    return useNavbarMobileSidebar();
  } catch {
    return { disabled: true, toggle: () => {} };
  }
}

function NavbarItems({ items }) {
  return (
    <div className="flex items-center justify-start gap-2 min-w-0 shrink overflow-hidden">
      {items.map((item, i) => (
        <ErrorCauseBoundary
          key={i}
          onError={(error) =>
            new Error(
              `A theme navbar item failed to render.
Please double-check the following navbar item (themeConfig.navbar.items) of your Docusaurus config:
${JSON.stringify(item, null, 2)}`,
              { cause: error },
            )
          }
        >
          <NavbarItem {...item} />
        </ErrorCauseBoundary>
      ))}
    </div>
  );
}

function NavbarContentLayout({ left, right }) {
  return (
    <div className="navbar__inner" style={{ flexWrap: "nowrap", gap: "0.5rem" }}>
      <div
        className="navbar__items"
        style={{ flexShrink: 1, minWidth: 0, overflow: "hidden" }}
      >
        {left}
      </div>
      <div
        className="navbar__items navbar__items--right"
        style={{ flexShrink: 0, marginLeft: "auto" }}
      >
        {right}
      </div>
    </div>
  );
}

function SearchLauncher() {
  const [open, setOpen] = React.useState(false);

  return (
    <>
      <button
        type="button"
        className="DocSearch DocSearch-Button flex items-center cursor-pointer shrink-0"
        onClick={() => setOpen(true)}
      >
        <span className="DocSearch-Button-Container flex">
          <svg
            width="20"
            height="20"
            className="DocSearch-Search-Icon"
            viewBox="0 0 20 20"
            aria-hidden="true"
          >
            <path
              d="M14.386 14.386l4.0877 4.0877-4.0877-4.0877c-2.9418 2.9419-7.7115 2.9419-10.6533 0-2.9419-2.9418-2.9419-7.7115
              0-10.6533 2.9418-2.9419 7.7115-2.9419 10.6533 0 2.9419 2.9418 2.9419 7.7115 0 10.6533z"
              stroke="currentColor"
              fill="none"
              fillRule="evenodd"
              strokeLinecap="round"
              strokeLinejoin="round"
            />
          </svg>
          <span className="DocSearch-Button-Placeholder font-semibold">
            Search
          </span>
        </span>
      </button>
      <SearchModal isOpen={open} onClose={() => setOpen(false)} />
    </>
  );
}

function KapaButton() {
  const handleClick = () => {
    if (typeof window !== "undefined" && window.Kapa) {
      window.Kapa.open();
    }
  };

  return (
    <button
      type="button"
      onClick={handleClick}
      className="kapa-trigger-btn flex items-center gap-2.5 cursor-pointer bg-white text-gray-900 font-semibold
      text-base px-5 py-2.5 rounded-full border border-gray-200 hover:bg-gray-50 transition-colors mx-0 min-[1100px]:mx-1 min-[1300px]:mx-2 shrink-0"
    >
      <img src="/img/logo.svg" alt="" width="25" height="25" />
      <span className="kapa-label">Ask Walrus AI</span>
    </button>
  );
}

// Fully custom logo with inline styles — immune to any stylesheet overrides
function CustomLogo() {
  const { navbar } = useThemeConfigFull();
  const logoSrc = useBaseUrl(navbar.logo?.src || "/img/logo.svg");
  const logoHref = useBaseUrl(navbar.logo?.href || "/");
  const title = navbar.title || "";

  return (
    <Link
      to={logoHref}
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: "0.5rem",
        flexShrink: 0,
        flexGrow: 0,
        whiteSpace: "nowrap",
        textDecoration: "none",
        color: "inherit",
        minWidth: "fit-content",
      }}
    >
      <img
        src={logoSrc}
        alt={navbar.logo?.alt || title}
        style={{ height: "2rem", width: "auto", display: "block", flexShrink: 0 }}
      />
      {title && (
        <span
          style={{
            fontWeight: 600,
            fontSize: "1rem",
            whiteSpace: "nowrap",
            flexShrink: 0,
            overflow: "visible",
          }}
        >
          {title}
        </span>
      )}
    </Link>
  );
}

export default function NavbarContent() {
  const mobileSidebar = useMobileSidebarSafe();
  const items = useNavbarItems();
  const [leftItems, rightItems] = splitNavbarItems(items);
  const searchBarItem = items.find((item) => item.type === "search");

  return (
    <NavbarContentLayout
      left={
        <>
          {!mobileSidebar.disabled && <NavbarMobileSidebarToggle />}
          <div className="shrink-0">
            <CustomLogo />
          </div>
          <NavbarItems items={leftItems} />
        </>
      }
      right={
        <div className="flex items-center gap-1 min-[1100px]:gap-2 min-[1300px]:gap-3 min-[1430px]:gap-4 shrink-0">
          <NavbarItems items={rightItems} />
          <ThemeToggle />
          <KapaButton />
          {!searchBarItem && (
            <NavbarSearch>
              <SearchLauncher />
            </NavbarSearch>
          )}
        </div>
      }
    />
  );
}
