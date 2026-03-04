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
import NavbarLogo from "@theme/Navbar/Logo";
import NavbarSearch from "@theme/Navbar/Search";
import SearchModal from "@site/src/components/Search/SearchModal";

function useNavbarItems() {
  return useThemeConfig().navbar.items;
}

function useMobileSidebarSafe() {
  try {
    return useNavbarMobileSidebar();
  } catch {
    return { disabled: true, toggle: () => {} };
  }
}

function NavbarItems({ items }) {
  return (
    <div className="flex flex-[8_1_0%] items-center justify-start gap-8 min-[1100px]:gap-16 min-w-0">
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
    </>
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
      text-base px-5 py-2.5 rounded-full border border-gray-200 hover:bg-gray-50 transition-colors mx-2 shrink-0"
    >
      <img src="/img/logo.svg" alt="" width="25" height="25" />
      <span className="kapa-label">Ask Walrus AI</span>
    </button>
  );
}

export default function NavbarContent() {
  const mobileSidebar = useMobileSidebarSafe();
  const items = useNavbarItems();
  const [leftItems, rightItems] = splitNavbarItems(items);
  const searchBarItem = items.find((item) => item.type === "search");

  return (
    <>
      {/* Force brand to never collapse regardless of stylesheet order */}
      <style>{`
        .navbar__brand {
          flex-shrink: 0 !important;
          flex-grow: 0 !important;
          overflow: visible !important;
          min-width: auto !important;
          width: auto !important;
        }
        .navbar__title {
          overflow: visible !important;
          white-space: nowrap !important;
          flex-shrink: 0 !important;
          width: auto !important;
        }
        /* Nav links shrink, not the logo */
        .navbar__items .navbar__link {
          white-space: nowrap !important;
          flex-shrink: 1 !important;
          font-size: clamp(0.65rem, 1vw, 0.875rem) !important;
          padding-left: clamp(0.2rem, 0.5vw, 0.75rem) !important;
          padding-right: clamp(0.2rem, 0.5vw, 0.75rem) !important;
        }
      `}</style>
      <div
        className="navbar__inner"
        style={{ flexWrap: "nowrap", justifyContent: "space-between" }}
      >
        {/* Left side: logo never shrinks, links absorb compression */}
        <div
          className="navbar__items"
          style={{
            flexShrink: 1,
            minWidth: 0,
            flexWrap: "nowrap",
            overflow: "visible",
            gap: "0.5rem",
          }}
        >
          {!mobileSidebar.disabled && <NavbarMobileSidebarToggle />}
          <NavbarLogo />
<<<<<<< Updated upstream
          <NavbarItems items={leftItems} />
        </>
      }
      right={
        <div className="flex items-center gap-3 min-w-0">
          <NavbarItems items={rightItems} />
          <ThemeToggle />
          <KapaButton />
          {!searchBarItem && (
            <NavbarSearch>
              <SearchLauncher />
            </NavbarSearch>
          )}
        </div>
      </div>
    </>
  );
}