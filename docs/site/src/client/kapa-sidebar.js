// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// Detects Kapa sidebar open/close and toggles .kapa-sidebar-open on <html>.
// Kapa renders in Shadow DOM so we can't query its internals.
// Strategy: hook Kapa.open for instant open detection, then use
// elementFromPoint to detect close (checks if right edge of screen
// is covered by a non-docusaurus element).

if (typeof window !== "undefined") {
  const OPEN_CLASS = "kapa-sidebar-open";
  let kapaOpen = false;
  let hookedRef = null;

  function syncClass() {
    document.documentElement.classList.toggle(OPEN_CLASS, kapaOpen);
  }

  function hookKapa() {
    if (!window.Kapa || !window.Kapa.open || window.Kapa.open === hookedRef)
      return;

    const origOpen = window.Kapa.open;
    const origClose = window.Kapa.close;

    window.Kapa.open = function (...args) {
      kapaOpen = true;
      syncClass();
      return origOpen.apply(this, args);
    };

    window.Kapa.close = function (...args) {
      kapaOpen = false;
      syncClass();
      return origClose.apply(this, args);
    };

    hookedRef = window.Kapa.open;
  }

  // Check if Kapa sidebar is covering the right side of the viewport.
  // Since Kapa uses Shadow DOM, we can't query its elements directly.
  // Instead, check if the element at the right edge of the screen
  // belongs to the doc app or to something else (Kapa's panel).
  function isSidebarVisible() {
    const x = window.innerWidth - 50;
    const y = window.innerHeight / 2;
    const el = document.elementFromPoint(x, y);
    if (!el) return false;
    const docRoot = document.getElementById("__docusaurus");
    if (docRoot && docRoot.contains(el)) return false;
    if (el === document.body || el === document.documentElement) return false;
    return true;
  }

  // Hook into History API so the class persists across SPA navigation.
  // Docusaurus uses pushState for client-side routing.
  const origPush = history.pushState.bind(history);
  const origReplace = history.replaceState.bind(history);
  history.pushState = function (...args) {
    const r = origPush(...args);
    syncClass();
    return r;
  };
  history.replaceState = function (...args) {
    const r = origReplace(...args);
    syncClass();
    return r;
  };
  window.addEventListener("popstate", syncClass);

  // Poll every 300ms: re-hook Kapa if it reinitializes, and
  // detect open/close state via elementFromPoint fallback.
  setInterval(() => {
    hookKapa();

    const visible = isSidebarVisible();
    if (visible && !kapaOpen) {
      kapaOpen = true;
    } else if (!visible && kapaOpen) {
      kapaOpen = false;
    }
    syncClass();
  }, 300);
}
