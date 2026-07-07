// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

import React, { useEffect, useState } from 'react';
import { createPortal } from 'react-dom';
import Head from '@docusaurus/Head';
import WalrusLogo from '@site/static/img/Walrus_Docs.svg';
import SearchModal from '@site/src/components/Search/SearchModal';

function hideEl(el: HTMLElement): () => void {
  const prev = el.style.display;
  el.style.display = 'none';
  return () => { el.style.display = prev; };
}

function setStyle(el: HTMLElement, prop: string, value: string): () => void {
  const prev = (el.style as any)[prop];
  (el.style as any)[prop] = value;
  return () => { (el.style as any)[prop] = prev; };
}

const LANDING_CSS = `
/* ── Hide Docusaurus chrome ── */
.navbar { display: none !important; }
.footer { display: none !important; }
#copy-page-button-container { display: none !important; }

#__docusaurus_skipToContent_fallback {
  background: #0d0f12 !important; padding: 0 !important; margin: 0 !important;
}
#__docusaurus_skipToContent_fallback > main,
#__docusaurus_skipToContent_fallback > main.container,
#__docusaurus_skipToContent_fallback > main.container--fluid {
  background: #0d0f12 !important; padding: 0 !important; margin: 0 !important; max-width: 100% !important;
}
#__docusaurus_skipToContent_fallback .margin-vert--lg { margin: 0 !important; }
#__docusaurus_skipToContent_fallback .row {
  background: #0d0f12 !important; padding: 0 !important; margin: 0 !important;
}
#__docusaurus_skipToContent_fallback .col {
  background: #0d0f12 !important; padding: 0 !important;
  max-width: 100% !important; flex: none !important; width: 100% !important;
}
#__docusaurus_skipToContent_fallback article {
  background: #0d0f12 !important; padding: 0 !important; margin: 0 !important; max-width: 100% !important;
}
html, body { background: #0d0f12 !important; }
[class*="mainWrapper"] { background: #0d0f12 !important; padding-top: 0 !important; }
[class*="mdxPageWrapper"] { background: #0d0f12 !important; padding: 0 !important; margin: 0 !important; }

/* ── Landing root ── */
.landing-root {
  --white: #faf8f5;
  --black: #0d0f12;
  --purple: #CAB1FF;
  --purple-dim: rgba(202,177,255,0.1);
  --purple-hover: rgba(202,177,255,0.16);
  --violet: #CAB1FF;
  --mint: #98EFDD;
  --yellow: #E8FF75;
  --gray-muted: rgba(255,255,255,0.45);
  --surface: #1c2228;
  --surface-hover: #252b31;
  --border: rgba(255,255,255,0.08);
  --border-hover: rgba(255,255,255,0.16);
  --mono: 'JetBrains Mono', monospace;
  --sans: 'DM Sans', -apple-system, sans-serif;
  --radius: 20px;
  font-family: var(--sans);
  background: var(--black);
  color: var(--white);
  -webkit-font-smoothing: antialiased;
  line-height: 1.6;
  min-height: 100vh;
}
.landing-root *, .landing-root *::before, .landing-root *::after { box-sizing: border-box; }
.landing-root a { color: var(--purple); text-decoration: none; }
.landing-root a:hover { color: #98EFDD; }
.landing-root,
.landing-root * {
  --ifm-background-color: #0d0f12 !important;
  --ifm-background-surface-color: #0d0f12 !important;
}

.landing-wrap { max-width: 1120px; margin: 0 auto; padding: 0 24px; }
@media (min-width: 768px) { .landing-wrap { padding: 0 40px; } }

/* ── Topbar ── */
.landing-root .topbar {
  position: sticky; top: 0; z-index: 50;
  background: #ffffff;
  backdrop-filter: blur(16px) saturate(1.4);
  border-bottom: 1px solid var(--border);
}
.landing-root .topbar-inner {
  display: flex; align-items: center; justify-content: space-between;
  height: 56px; max-width: 1120px; margin: 0 auto; padding: 0 24px;
}
@media (min-width: 768px) { .landing-root .topbar-inner { padding: 0 40px; } }
.landing-root .topbar-logo {
  display: flex; align-items: center; gap: 10px;
  font-weight: 500; font-size: 0.95rem; letter-spacing: 0.02em; color: var(--white);
}
.landing-root .topbar-logo svg {
  height: 28px; width: auto; margin-bottom: 3px; color: #000;
}
.landing-root .topbar-logo .sep { color: rgba(0,0,0,0.2); font-weight: 300; font-size: 2rem; }
.landing-root .topbar-logo .docs-label { color: rgba(0,0,0,0.5); font-weight: 700; font-size: 2rem; }
.landing-root .topbar-links { display: flex; gap: 6px; align-items: center; }
.landing-root .topbar-links a {
  font-size: 1.3rem; padding: 6px 14px; border-radius: 8px;
  color: rgba(0,0,0,0.55); transition: all 0.2s;
}
.landing-root .topbar-links a:hover { color: #000; background: rgba(0,0,0,0.04); }
.landing-root .topbar-links a.primary {
  color: #000; background: rgba(0,0,0,0.05);
  border: 1px solid rgba(0,0,0,0.12); font-weight: 500;
}
.landing-root .topbar-links a.primary:hover {
  background: var(--purple-dim); border-color: var(--purple);
}
.landing-root .topbar-links .kapa-landing-btn {
  font-size: 1.3rem; padding: 6px 14px; border-radius: 8px;
  color: var(--purple); background: var(--purple-dim);
  border: 1px solid rgba(97,61,255,0.15); font-weight: 500;
  cursor: pointer; transition: all 0.2s; font-family: var(--sans);
}
.landing-root .topbar-links .kapa-landing-btn:hover {
  background: var(--purple-hover); border-color: var(--purple);
}

/* ── Hero ── */
.landing-root .hero {
  position: relative; padding: 48px 0 24px; overflow: hidden;
  background: var(--black);
}
.landing-root .hero-inner {
  position: relative; z-index: 5;
  margin-bottom: 0; padding-top: 0;
  text-align: center;
  opacity: 0; animation: landingFadeIn 0.5s ease forwards 0.1s;
}
.landing-root .hero-title {
  font-size: clamp(1.75rem, 4vw, 2.5rem);
  font-weight: 600; line-height: 1.15; letter-spacing: -0.03em;
  margin: 0 0 12px; color: var(--white);
}
.landing-root .hero-sub {
  font-size: 1rem; color: var(--white); opacity: 0.55;
  line-height: 1.55; max-width: 560px; margin: 0 auto;
}

/* ── Product card grid (Mem0-inspired) ── */
.landing-root .product-grid {
  display: grid; grid-template-columns: repeat(2, 1fr); gap: 16px;
  padding: 32px 0 0;
  opacity: 0; animation: landingFadeIn 0.6s ease forwards 0.3s;
}
@media (max-width: 640px) {
  .landing-root .product-grid { grid-template-columns: 1fr; }
}

.landing-root .product-card {
  background: var(--surface); border: 1px solid var(--border);
  border-radius: 14px; padding: 0;
  transition: all 0.2s ease; cursor: pointer;
  display: flex; flex-direction: column;
  text-decoration: none !important;
  overflow: hidden;
}
.landing-root .product-card:hover {
  border-color: rgba(255,255,255,0.18);
  transform: translateY(-2px);
  box-shadow: 0 8px 24px rgba(0,0,0,0.25);
}

.landing-root .product-card-thumb {
  height: 120px;
  background: linear-gradient(135deg, rgba(202,177,255,0.06) 0%, rgba(152,239,221,0.04) 100%);
  display: flex; align-items: center; justify-content: center;
  border-bottom: 1px solid var(--border);
}
.landing-root .product-card-thumb svg {
  width: 48px; height: 48px; color: var(--purple); opacity: 0.5;
}
.landing-root .product-card-thumb--muted {
  background: linear-gradient(135deg, rgba(255,255,255,0.02) 0%, rgba(255,255,255,0.01) 100%);
}
.landing-root .product-card-thumb--muted svg {
  color: var(--gray-muted); opacity: 0.3;
}

.landing-root .product-card h3 {
  font-size: 0.95rem; font-weight: 700;
  line-height: 1.3; margin: 0;
  color: var(--white);
  padding: 16px 18px 0;
}
.landing-root .product-card p {
  font-size: 0.82rem; color: var(--white); opacity: 0.5;
  line-height: 1.5; margin: 0;
  padding: 6px 18px 0;
  flex: 1;
}
.landing-root .product-card .under-hood {
  font-style: italic; opacity: 0.6;
}
.landing-root .coming-soon-badge {
  font-size: 0.6rem; font-weight: 500; letter-spacing: 0.03em;
  text-transform: uppercase; opacity: 0.5;
  background: rgba(255,255,255,0.08); border: 1px solid rgba(255,255,255,0.1);
  border-radius: 4px; padding: 2px 6px; margin-left: 8px;
  vertical-align: middle;
}
.landing-root .product-card .product-arrow {
  display: flex; align-items: center; justify-content: flex-end;
  padding: 12px 18px;
  color: var(--purple); opacity: 0.4;
  transition: opacity 0.2s ease;
}
.landing-root .product-card:hover .product-arrow { opacity: 1; }
.landing-root .product-card .product-arrow svg { width: 12px; height: 12px; }
.landing-root .product-card--muted { opacity: 0.7; }
.landing-root .product-card--muted:hover { opacity: 1; }

/* ── Landing search bar ── */
.landing-root .landing-search {
  max-width: 560px; margin: 0 auto;
  padding: 20px 0 0;
  opacity: 0; animation: landingFadeIn 0.5s ease forwards 0.2s;
}
.landing-root .landing-search-btn {
  width: 100%; display: flex; align-items: center; gap: 14px;
  background: var(--surface); border: 1px solid var(--border);
  border-radius: var(--radius); padding: 18px 24px;
  cursor: pointer; transition: all 0.2s ease;
  font-family: var(--sans);
}
.landing-root .landing-search-btn:hover {
  border-color: rgba(255,255,255,0.16); background: var(--surface-hover);
  box-shadow: 0 6px 24px rgba(0,0,0,0.25);
}
.landing-root .landing-search-btn svg {
  width: 20px; height: 20px; color: var(--purple); flex-shrink: 0;
}
.landing-root .landing-search-btn span {
  font-size: 1rem; color: rgba(255,255,255,0.35); font-weight: 400;
}
.landing-root .landing-search-btn kbd {
  margin-left: auto; font-family: var(--sans);
  font-size: 0.72rem; color: rgba(255,255,255,0.25);
  background: rgba(255,255,255,0.06); border: 1px solid rgba(255,255,255,0.08);
  border-radius: 6px; padding: 3px 9px; font-weight: 500;
}

/* ── Search modal z-index fix ── */
.landing-root .fixed.inset-0,
.landing-root [class*="z-500"] {
  z-index: 9999 !important;
}

/* ── Divider ── */
.landing-root .divider {
  border: none; border-top: 1px solid var(--border); margin: 0;
}


/* ── Footer ── */
.landing-root .page-footer {
  border-top: 1px solid var(--border); margin-top: 80px; padding: 36px 0;
  display: flex; align-items: center;
  justify-content: space-between;
  flex-wrap: wrap; gap: 16px;
}
.landing-root .footer-left {
  font-size: 0.75rem; color: rgba(255,255,255,0.3);
}
.landing-root .footer-right {
  display: flex; gap: 20px;
}
.landing-root .footer-right a {
  color: var(--purple); font-size: 0.8rem; transition: color 0.2s;
}
.landing-root .footer-right a:hover { color: #4c2ecc; }

/* ── Animations ── */
@keyframes landingFadeIn {
  from { opacity: 0; transform: translateY(10px); }
  to   { opacity: 1; transform: translateY(0); }
}
.landing-root .scroll-reveal {
  opacity: 0; transform: translateY(16px);
  transition: opacity 0.5s ease, transform 0.5s ease;
}
.landing-root .scroll-reveal.visible {
  opacity: 1; transform: translateY(0);
}
`;

export default function LandingPage() {
  const [searchOpen, setSearchOpen] = useState(false);

  useEffect(() => {
    const cleanups: Array<() => void> = [];

    const navbar = document.querySelector('.navbar') as HTMLElement | null;
    if (navbar) cleanups.push(hideEl(navbar));

    document.querySelectorAll('.footer, footer').forEach((f) => {
      if (!f.closest('.landing-root')) cleanups.push(hideEl(f as HTMLElement));
    });

    const copyBtn = document.getElementById('copy-page-button-container');
    if (copyBtn) cleanups.push(hideEl(copyBtn));

    cleanups.push(setStyle(document.documentElement, 'background', '#0d0f12'));
    cleanups.push(setStyle(document.body, 'background', '#0d0f12'));

    const root = document.querySelector(
      '.landing-root',
    ) as HTMLElement | null;
    if (root) {
      let el: HTMLElement | null = root.parentElement;
      while (el && el !== document.body) {
        cleanups.push(setStyle(el, 'padding', '0'));
        cleanups.push(setStyle(el, 'margin', '0'));
        cleanups.push(setStyle(el, 'maxWidth', '100%'));
        cleanups.push(setStyle(el, 'width', '100%'));
        cleanups.push(setStyle(el, 'background', '#0d0f12'));
        el = el.parentElement;
      }
    }

    const obs = new IntersectionObserver(
      (entries) => {
        entries.forEach((e) => {
          if (e.isIntersecting) e.target.classList.add('visible');
        });
      },
      { threshold: 0.1, rootMargin: '0px 0px -30px 0px' },
    );
    document
      .querySelectorAll('.landing-root .scroll-reveal')
      .forEach((el) => obs.observe(el));

    // "/" key opens the search bar
    function handleKey(e: KeyboardEvent) {
      if (e.key === '/' && !e.metaKey && !e.ctrlKey
          && !(e.target instanceof HTMLInputElement)
          && !(e.target instanceof HTMLTextAreaElement)) {
        e.preventDefault();
        setSearchOpen(true);
      }
    }
    document.addEventListener('keydown', handleKey);

    return () => {
      cleanups.forEach((fn) => fn());
      obs.disconnect();
      document.removeEventListener('keydown', handleKey);
    };
  }, []);

  const arrowIcon = (
    <svg viewBox="0 0 13 13" fill="none">
      <path
        d={
          'M11.52 5.66L5.86 0L5.16 .71L10.31 5.86H0V6.86'
          + 'H10.31L5.16 12.02L5.86 12.73L11.52 7.07L12.23'
          + ' 6.36L11.52 5.66Z'
        }
        fill="currentColor"
      />
    </svg>
  );

  return (
    <div className="landing-root">
      <Head>
        <link
          href={
            'https://fonts.googleapis.com/css2?family=DM+Sans'
            + ':opsz,wght@9..40,300;9..40,400;9..40,500;9..40,600'
            + '&family=JetBrains+Mono:wght@400;500&display=swap'
          }
          rel="stylesheet"
        />
      </Head>
      <style dangerouslySetInnerHTML={{ __html: LANDING_CSS }} />

      <div className="landing-wrap">
        <div className="hero-inner">
          <h1 className="hero-title">Build with Walrus</h1>
          <p className="hero-sub">
            Keep critical data persistent, portable, and under your control
            across apps, providers, and agents.
          </p>
        </div>

        <div className="landing-search">
          <button
            type="button"
            className="landing-search-btn"
            onClick={() => setSearchOpen(true)}
          >
            <svg viewBox="0 0 20 20" fill="none">
              <path
                d={
                  "M14.386 14.386l4.088 4.088-4.088-4.088c-2.942 2.942-7.711 2.942-10.653 0" +
                  "-2.942-2.942-2.942-7.711 0-10.653 2.942-2.942 7.711-2.942 10.653 0" +
                  " 2.942 2.942 2.942 7.711 0 10.653z"
                }
                stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"
              />
            </svg>
            <span>Search docs or ask Walrus AI...</span>
            <kbd>/</kbd>
          </button>
          {searchOpen && createPortal(
            <SearchModal isOpen={searchOpen} onClose={() => setSearchOpen(false)} />,
            document.body,
          )}
        </div>

        <div className="product-grid">
          <a className="product-card" href="/walrus-memory/getting-started/what-is-walrus-memory">
            <div className="product-card-thumb">
              <svg viewBox="0 0 48 48" fill="none" stroke="currentColor" strokeWidth="1.5">
                <circle cx="24" cy="16" r="10" />
                <path d="M24 26v4M18 38h12M20 26c0 2.5 1.8 4 4 4s4-1.5 4-4" />
              </svg>
            </div>
            <h3>Walrus Memory</h3>
            <p>Portable memory layer that gives AI agents persistent context across apps and sessions.</p>
            <span className="product-arrow">{arrowIcon}</span>
          </a>
          <a className="product-card product-card--muted" href="/docs/getting-started">
            <div className="product-card-thumb">
              <svg viewBox="0 0 48 48" fill="none" stroke="currentColor" strokeWidth="1.5">
                <rect x="8" y="8" width="32" height="32" rx="4" />
                <path d="M8 18h32M18 48V18" />
              </svg>
            </div>
            <h3>Walrus Console <span className="coming-soon-badge">Coming soon</span></h3>
            <p>Unified control plane for managing files, datasets, memory, and other assets on Walrus.</p>
            <span className="product-arrow">{arrowIcon}</span>
          </a>
          <a className="product-card product-card--muted" href="/docs/getting-started">
            <div className="product-card-thumb">
              <svg viewBox="0 0 48 48" fill="none" stroke="currentColor" strokeWidth="1.5">
                <circle cx="18" cy="38" r="3" /><circle cx="36" cy="38" r="3" />
                <path d="M4 4h8l5.36 26.78a4 4 0 004 3.22h15.28a4 4 0 004-3.22L44 14H12" />
              </svg>
            </div>
            <h3>Walrus Marketplace <span className="coming-soon-badge">Coming soon</span></h3>
            <p>Open marketplace where developers and AI agents discover, license, and access data.</p>
            <span className="product-arrow">{arrowIcon}</span>
          </a>
          <a className="product-card" href="/docs/getting-started">
            <div className="product-card-thumb">
              <svg viewBox="0 0 48 48" fill="none" stroke="currentColor" strokeWidth="1.5">
                <path d="M42 32V16a4 4 0 00-2-3.46l-14-8a4 4 0 00-4 0l-14 8A4 4 0 006 16v16a4 4 0 002 3.46l14 8a4 4 0 004 0l14-8A4 4 0 0042 32z" />
                <polyline points="6.54 13.92 24 24.02 41.46 13.92" />
                <line x1="24" y1="44.16" x2="24" y2="24" />
              </svg>
            </div>
            <h3>Walrus Protocol</h3>
            <p>Open source, decentralized data storage. <span className="under-hood">Under the hood.</span></p>
            <span className="product-arrow">{arrowIcon}</span>
          </a>
        </div>

        <footer className="page-footer">
          <div className="footer-left">
            © 2026 Walrus Foundation
          </div>
          <nav className="footer-right">
            <a
              href="https://github.com/MystenLabs/walrus"
              target="_blank"
              rel="noopener noreferrer"
            >
              GitHub
            </a>
            <a
              href="https://discord.gg/walrusprotocol"
              target="_blank"
              rel="noopener noreferrer"
            >
              Discord
            </a>
            <a
              href="https://x.com/walrusprotocol"
              target="_blank"
              rel="noopener noreferrer"
            >
              X
            </a>
            <a
              href="https://docs.wal.app/docs/legal/privacy"
              target="_blank"
              rel="noopener noreferrer"
            >
              Privacy
            </a>
            <a
              href={
                'https://docs.wal.app/docs/legal'
                + '/walrus_general_tos'
              }
              target="_blank"
              rel="noopener noreferrer"
            >
              Terms
            </a>
          </nav>
        </footer>
      </div>
    </div>
  );
}
