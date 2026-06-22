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
import { useLocation } from "@docusaurus/router";
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

function AppsDropdown({ items }) {
  const [open, setOpen] = React.useState(false);
  const ref = React.useRef(null);
  const location = useLocation();
  const isActive = items.some((item) => location.pathname.startsWith(item.href));

  React.useEffect(() => {
    function handleClickOutside(e) {
      if (ref.current && !ref.current.contains(e.target)) setOpen(false);
    }
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  const menuStyle = {
    position: "absolute",
    top: "100%",
    left: 0,
    minWidth: "11rem",
    padding: "0.4rem 0",
    margin: 0,
    listStyle: "none",
    background: "var(--ifm-background-surface-color, #fff)",
    border: "1px solid rgba(0,0,0,0.08)",
    borderRadius: "0.5rem",
    boxShadow: "0 4px 16px rgba(0,0,0,0.1)",
    zIndex: 200,
    opacity: open ? 1 : 0,
    visibility: open ? "visible" : "hidden",
    pointerEvents: open ? "auto" : "none",
    transform: open ? "translateY(0)" : "translateY(4px)",
    transition: "opacity 0.15s ease, visibility 0.15s ease, transform 0.15s ease",
  };

  return (
    <div
      ref={ref}
      style={{ position: "relative" }}
      onMouseEnter={() => setOpen(true)}
      onMouseLeave={() => setOpen(false)}
    >
      <span
        role="button"
        tabIndex={0}
        className={`navbar__item navbar__link ${isActive ? "navbar__link--active" : ""}`}
        onClick={() => setOpen(!open)}
        onKeyDown={(e) => { if (e.key === "Enter") setOpen(!open); }}
        style={{ cursor: "pointer" }}
      >
        Apps ▾
      </span>
      <ul style={menuStyle}>
        {items.map((item, i) => (
          <li key={i} style={{ margin: 0, padding: 0, listStyle: "none" }}>
            <Link
              to={item.href || item.to}
              style={{
                display: "block",
                padding: "0.45rem 1rem",
                color: "var(--ifm-font-color-base, #000)",
                textDecoration: "none",
                fontSize: "0.88rem",
                fontWeight: 500,
                whiteSpace: "nowrap",
              }}
              onMouseEnter={(e) => {
                e.currentTarget.style.background = "rgba(0,0,0,0.04)";
                e.currentTarget.style.color = "#613dff";
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.background = "transparent";
                e.currentTarget.style.color = "var(--ifm-font-color-base, #000)";
              }}
              onClick={() => setOpen(false)}
            >
              {item.label}
            </Link>
          </li>
        ))}
      </ul>
    </div>
  );
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

function NavbarContentLayout({ left, right, tabs }) {
  return (
    <div style={{ width: "100%" }}>
      {/* Top row: logo flush left, search + actions right */}
      <div
        className="navbar__inner"
        style={{
          flexWrap: "nowrap",
          gap: "0.5rem",
          padding: "0 1rem",
          width: "100%",
          height: "2.75rem",
          alignItems: "center",
        }}
      >
        <div
          className="navbar__items"
          style={{ flexShrink: 0 }}
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
      {/* Bottom row: section tabs, aligned with content area */}
      {tabs && (
        <div
          className="navbar-tabs"
          style={{
            padding: "0 1rem",
            display: "flex",
            alignItems: "center",
            gap: "0.25rem",
            height: "2.25rem",
            borderTop: "1px solid rgba(128,128,128,0.08)",
            overflowX: "auto",
          }}
        >
          {tabs}
        </div>
      )}
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
      className="kapa-trigger-btn flex items-center gap-2 cursor-pointer bg-white text-gray-900 font-semibold
      text-xs px-3.5 py-1.5 rounded-full border border-gray-200 hover:bg-gray-50 transition-colors mx-0 min-[1100px]:mx-1 min-[1300px]:mx-2 shrink-0"
    >
      <img src="/img/walrus-mascot.png" alt="" width="18" height="18" style={{ borderRadius: "50%" }} />
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
        style={{ height: "1.6rem", width: "auto", display: "block", flexShrink: 0 }}
      />
      {title && (
        <span
          style={{
            fontWeight: 600,
            fontSize: "0.88rem",
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

  // Separate nav links (for tab row) from action items (github icon)
  const sectionLinks = rightItems.filter(
    (item) => item.type === "docSidebar" || item.docsPluginId
  );
  const otherLinks = rightItems.filter(
    (item) => item.type !== "docSidebar" && !item.docsPluginId
  );

  // Build the full tab list including Walrus Memory
  const walrusMemoryItem = {
    type: "doc",
    docId: "getting-started/what-is-walrus-memory",
    docsPluginId: "walrus-memory",
    label: "Walrus Memory",
    position: "right",
  };

  // Insert Walrus Memory after Walrus Console
  const insertAfter = sectionLinks.findIndex(
    (item) => item.sidebarId === "consoleSidebar" || item.label === "Walrus Console"
  );
  const tabItems = [
    ...sectionLinks.slice(0, insertAfter + 1),
    walrusMemoryItem,
    ...sectionLinks.slice(insertAfter + 1),
  ];

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
        <div className="flex items-center gap-2 shrink-0">
          <KapaButton />
          <NavbarItems items={otherLinks} />
          <ThemeToggle />
          {!searchBarItem && (
            <NavbarSearch>
              <SearchLauncher />
            </NavbarSearch>
          )}
        </div>
      }
      tabs={
        <div className="hidden min-[997px]:flex items-center gap-0.5">
          <NavbarItems items={tabItems} />
        </div>
      }
    />
  );
}
