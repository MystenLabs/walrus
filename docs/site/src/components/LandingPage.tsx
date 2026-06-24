// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

import React, { useEffect, useState } from 'react';
import { createPortal } from 'react-dom';
import Head from '@docusaurus/Head';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
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
  background: #fff !important; padding: 0 !important; margin: 0 !important;
}
#__docusaurus_skipToContent_fallback > main,
#__docusaurus_skipToContent_fallback > main.container,
#__docusaurus_skipToContent_fallback > main.container--fluid {
  background: #fff !important; padding: 0 !important; margin: 0 !important; max-width: 100% !important;
}
#__docusaurus_skipToContent_fallback .margin-vert--lg { margin: 0 !important; }
#__docusaurus_skipToContent_fallback .row {
  background: #fff !important; padding: 0 !important; margin: 0 !important;
}
#__docusaurus_skipToContent_fallback .col {
  background: #fff !important; padding: 0 !important;
  max-width: 100% !important; flex: none !important; width: 100% !important;
}
#__docusaurus_skipToContent_fallback article {
  background: #fff !important; padding: 0 !important; margin: 0 !important; max-width: 100% !important;
}
html, body { background: #fff !important; }
[class*="mainWrapper"] { background: #fff !important; padding-top: 0 !important; }
[class*="mdxPageWrapper"] { background: #fff !important; padding: 0 !important; margin: 0 !important; }

/* ── Landing root ── */
.landing-root {
  --white: #1a1a2e;
  --black: #ffffff;
  --purple: #613dff;
  --purple-dim: rgba(97,61,255,0.06);
  --purple-hover: rgba(97,61,255,0.1);
  --violet: #613dff;
  --mint: #0d9488;
  --yellow: #b8860b;
  --gray-muted: rgba(0,0,0,0.5);
  --surface: #f6f8fa;
  --surface-hover: #eef1f4;
  --border: rgba(0,0,0,0.08);
  --border-hover: rgba(0,0,0,0.16);
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
  --ifm-background-color: #fff !important;
  --ifm-background-surface-color: #fff !important;
}

.landing-wrap { max-width: 1120px; margin: 0 auto; padding: 0 24px; }
@media (min-width: 768px) { .landing-wrap { padding: 0 40px; } }

/* ── Topbar (two-row, matches site navbar) ── */
.landing-root .topbar {
  position: sticky; top: 0; z-index: 50;
  background: #f0f1f3;
  border-bottom: 1px solid rgba(0,0,0,0.06);
}
.landing-root .topbar-row {
  display: flex; align-items: center; justify-content: space-between;
  height: 2.75rem; padding: 0 1rem;
}
.landing-root .topbar-logo {
  display: flex; align-items: center; gap: 8px;
  flex-shrink: 0;
}
.landing-root .topbar-logo svg {
  height: 1.6rem; width: auto; color: #000;
}
.landing-root .topbar-actions { display: flex; gap: 8px; align-items: center; }
.landing-root .topbar-actions a,
.landing-root .topbar-actions button {
  font-size: 0.75rem; padding: 5px 12px; border-radius: 1.25rem;
  color: rgba(0,0,0,0.5); transition: all 0.15s;
  text-decoration: none; font-family: var(--sans);
  background: transparent; border: 1px solid rgba(0,0,0,0.1);
  cursor: pointer;
}
.landing-root .topbar-actions a:hover,
.landing-root .topbar-actions button:hover {
  color: #000; border-color: rgba(0,0,0,0.2);
}
.landing-root .topbar-actions .kapa-landing-btn {
  color: var(--mint); border-color: rgba(152,239,221,0.2);
}
.landing-root .topbar-actions .kapa-landing-btn:hover {
  border-color: var(--mint);
}
.landing-root .topbar-tabs {
  display: flex; align-items: center; justify-content: center; gap: 2px;
  height: 2.25rem; padding: 0 1rem;
  border-top: 1px solid rgba(255,255,255,0.04);
  overflow-x: auto;
  background: var(--black);
}
.landing-root .topbar-tabs a {
  font-size: 0.8125rem; font-weight: 500; padding: 0.25rem 0.625rem;
  color: var(--gray-muted); text-decoration: none; transition: color 0.15s;
  white-space: nowrap;
}
.landing-root .topbar-tabs a:hover { color: var(--white); }

/* ── Logo ── */
.landing-root .landing-logo {
  padding: 2rem 0 0;
  opacity: 0; animation: landingFadeIn 0.5s ease forwards 0.1s;
}
.landing-root .landing-logo svg {
  height: 2rem; width: auto; color: #000;
}

/* ── Hero ── */
.landing-root .hero {
  position: relative; padding: 40px 0 32px; overflow: hidden;
  background: var(--black);
}
.landing-root .hero-inner {
  position: relative; z-index: 5; max-width: 1500px;
  margin-bottom: 36px; padding-top: 20px;
}
.landing-root .hero-badge {
  display: inline-block;
  font-family: var(--mono); font-size: 0.72rem; font-weight: 500;
  letter-spacing: 0.08em; text-transform: uppercase;
  color: var(--purple); margin-bottom: 16px;
  opacity: 0; animation: landingFadeIn 0.5s ease forwards 0.1s;
}
.landing-root .hero h1 {
  font-size: clamp(2rem, 5vw, 3rem);
  font-weight: 500; line-height: 1.08; letter-spacing: -0.025em;
  margin-bottom: 14px; color: var(--white);
  opacity: 0; animation: landingFadeIn 0.6s ease forwards 0.2s;
}
.landing-root .hero p {
  font-size: 1rem; color: var(--white); line-height: 1.6;
  max-width: 1500px;
  opacity: 0; animation: landingFadeIn 0.6s ease forwards 0.35s;
}

/* ── Product cards ── */
.landing-root .quickstart {
  display: grid; grid-template-columns: repeat(2, 1fr); gap: 14px;
  padding-bottom: 0;
  opacity: 0; animation: landingFadeIn 0.6s ease forwards 0.65s;
}
@media (max-width: 640px) {
  .landing-root .quickstart { grid-template-columns: 1fr; }
}
.landing-root .qs-card {
  position: relative; overflow: hidden;
  background: var(--surface); border: 1px solid var(--border);
  border-radius: var(--radius); padding: 24px 22px 20px;
  transition: all 0.25s ease; cursor: pointer;
  display: flex; flex-direction: column; gap: 8px;
}
.landing-root .qs-card::before {
  content: ''; position: absolute; inset: 0; opacity: 0;
  transition: opacity 0.3s ease; border-radius: inherit; z-index: 0;
}
.landing-root .qs-card:hover {
  border-color: rgba(255,255,255,0.18); background: var(--surface-hover);
  transform: translateY(-2px);
  box-shadow: 0 10px 32px rgba(0,0,0,0.35);
}
.landing-root .qs-card:hover::before { opacity: 1; }
.landing-root .qs-card--purple::before {
  background: radial-gradient(ellipse at top right, rgba(202,177,255,0.07) 0%, transparent 60%);
}
.landing-root .qs-card--mint::before {
  background: radial-gradient(ellipse at top right, rgba(152,239,221,0.07) 0%, transparent 60%);
}
.landing-root .qs-card--yellow::before {
  background: radial-gradient(ellipse at top right, rgba(232,255,117,0.07) 0%, transparent 60%);
}
.landing-root .qs-card .qs-card-top {
  position: relative; z-index: 1;
  display: flex; align-items: center; gap: 12px;
}
.landing-root .qs-card .qs-icon {
  width: 38px; height: 38px; border-radius: 10px; flex-shrink: 0;
  display: flex; align-items: center; justify-content: center;
}
.landing-root .qs-card--purple .qs-icon { background: rgba(202,177,255,0.12); }
.landing-root .qs-card--mint .qs-icon { background: rgba(152,239,221,0.12); }
.landing-root .qs-card--yellow .qs-icon { background: rgba(232,255,117,0.12); }
.landing-root .qs-card .qs-icon svg { width: 19px; height: 19px; }
.landing-root .qs-card--purple .qs-icon svg { color: var(--purple); }
.landing-root .qs-card--mint .qs-icon svg { color: var(--mint); }
.landing-root .qs-card--yellow .qs-icon svg { color: var(--yellow); }
.landing-root .qs-card h3 {
  position: relative; z-index: 1;
  font-size: 1.05rem; font-weight: 600;
  line-height: 1.3; margin: 0; color: var(--white);
}
.landing-root .qs-card p {
  position: relative; z-index: 1;
  font-size: 0.85rem; color: var(--white); opacity: 0.5;
  line-height: 1.5; margin: 0;
}
.landing-root .qs-card .qs-arrow {
  position: relative; z-index: 1;
  font-size: 0.78rem; font-weight: 500;
  display: flex; align-items: center; gap: 5px; margin-top: 4px;
  transition: gap 0.2s ease;
}
.landing-root .qs-card--purple .qs-arrow { color: var(--purple); }
.landing-root .qs-card--mint .qs-arrow { color: var(--mint); }
.landing-root .qs-card--yellow .qs-arrow { color: var(--yellow); }
.landing-root .qs-card:hover .qs-arrow { gap: 9px; }
.landing-root .qs-card .qs-arrow svg { width: 11px; height: 11px; }

/* ── Landing search bar ── */
.landing-root .landing-search {
  max-width: 760px; margin: 0 auto;
  padding: 0 0 28px;
  opacity: 0; animation: landingFadeIn 0.6s ease forwards 0.5s;
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
  width: 20px; height: 20px; color: rgba(0,0,0,0.3); flex-shrink: 0;
}
.landing-root .landing-search-btn span {
  font-size: 1rem; color: rgba(0,0,0,0.35); font-weight: 400;
}
.landing-root .landing-search-btn kbd {
  margin-left: auto; font-family: var(--sans);
  font-size: 0.72rem; color: rgba(0,0,0,0.25);
  background: rgba(0,0,0,0.04); border: 1px solid rgba(0,0,0,0.08);
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

.landing-root .hero-lead {
  font-size: 1.1rem;
  line-height: 1.5;
  margin-bottom: 1px;
  color: var(--white);
}

.landing-root .hero-body {
  font-size: 0.9rem;
  line-height: 1.6;
  margin-bottom: 1px;
  color: var(--white);
  opacity: 0.6;
}

.landing-root .hero-features {
  list-style: none;
  padding: 0;
  margin: 0;
  display: flex;
  gap: 24px;
  flex-wrap: wrap;
}

.landing-root .hero-features li {
  font-size: 0.85rem;
  font-weight: 500;
  color: var(--white);
  display: flex;
  align-items: center;
  gap: 8px;
}

.landing-root .hero-features li::before {
  content: '';
  width: 6px;
  height: 6px;
  border-radius: 50%;
  background: var(--purple);
  flex-shrink: 0;
}

/* ── Footer ── */
.landing-root .page-footer {
  border-top: 1px solid rgba(0,0,0,0.08); margin-top: 80px; padding: 36px 0;
  display: flex; align-items: center;
  justify-content: space-between;
  flex-wrap: wrap; gap: 16px;
}
.landing-root .footer-left {
  font-size: 0.75rem; color: rgba(0,0,0,0.3);
}
.landing-root .footer-right {
  display: flex; gap: 20px;
}
.landing-root .footer-right a {
  color: rgba(0,0,0,0.4); font-size: 0.8rem; transition: color 0.2s;
}
.landing-root .footer-right a:hover { color: #613dff; }

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
  const { siteConfig } = useDocusaurusContext();
  const baseUrl = siteConfig.baseUrl;

  // Prepend baseUrl to internal paths for correct preview deploy links
  const u = (path: string) => `${baseUrl}${path.replace(/^\//, '')}`;

  useEffect(() => {
    const cleanups: Array<() => void> = [];

    const navbar = document.querySelector('.navbar') as HTMLElement | null;
    if (navbar) cleanups.push(hideEl(navbar));

    document.querySelectorAll('.footer, footer').forEach((f) => {
      if (!f.closest('.landing-root')) cleanups.push(hideEl(f as HTMLElement));
    });

    const copyBtn = document.getElementById('copy-page-button-container');
    if (copyBtn) cleanups.push(hideEl(copyBtn));

    cleanups.push(setStyle(document.documentElement, 'background', '#fff'));
    cleanups.push(setStyle(document.body, 'background', '#fff'));

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
        cleanups.push(setStyle(el, 'background', '#fff'));
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
        <div className="landing-logo">
          <WalrusLogo />
        </div>
        <div className="hero-inner">
          <p className="hero-lead">
            A verifiable data platform for high-stakes systems that
            require provable, programmable, always-available data
            with no performance tradeoffs.
          </p>
          <p className="hero-body">
            Modern financial systems and AI agents depend on fast,
            reliable, and verifiable data. Traditional storage assumes
            integrity and pushes trust outside the data layer. Walrus
            embeds availability, integrity, and programmability
            directly into storage itself.
          </p>
          <ul className="hero-features">
            <li>Highly available</li>
            <li>Cryptographically verifiable</li>
            <li>Programmable through smart contracts</li>
          </ul>
        </div>

        <div className="landing-search">
          <button
            type="button"
            className="landing-search-btn"
            onClick={() => setSearchOpen(true)}
          >
            <svg viewBox="0 0 20 20" fill="none">
              <path
                d="M14.386 14.386l4.088 4.088-4.088-4.088c-2.942 2.942-7.711 2.942-10.653 0-2.942-2.942-2.942-7.711 0-10.653 2.942-2.942 7.711-2.942 10.653 0 2.942 2.942 2.942 7.711 0 10.653z"
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

        <div className="quickstart">
          <a className="qs-card qs-card--purple" href={u('/docs/getting-started')}>
            <div className="qs-card-top">
              <div className="qs-icon">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
                  <path d="M21 16V8a2 2 0 00-1-1.73l-7-4a2 2 0 00-2 0l-7 4A2 2 0 003 8v8a2 2 0 001 1.73l7 4a2 2 0 002 0l7-4A2 2 0 0021 16z" />
                  <polyline points="3.27 6.96 12 12.01 20.73 6.96" />
                  <line x1="12" y1="22.08" x2="12" y2="12" />
                </svg>
              </div>
              <h3>Data Storage</h3>
            </div>
            <p>Verifiable storage, erasure coding, and programmable access.</p>
            <span className="qs-arrow">Get started {arrowIcon}</span>
          </a>
          <a className="qs-card qs-card--mint" href={u('/walrus-memory/getting-started/what-is-walrus-memory')}>
            <div className="qs-card-top">
              <div className="qs-icon">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
                  <path d="M12 2a7 7 0 017 7c0 3.87-3.13 7-7 7s-7-3.13-7-7a7 7 0 017-7z" />
                  <path d="M12 16v2M8 22h8M9 16c0 1.5 1.34 2 3 2s3-.5 3-2" />
                </svg>
              </div>
              <h3>Walrus Memory</h3>
            </div>
            <p>Portable, encrypted memory for AI agents.</p>
            <span className="qs-arrow">Learn more {arrowIcon}</span>
          </a>
          <a className="qs-card qs-card--yellow" href={u('/docs/console')}>
            <div className="qs-card-top">
              <div className="qs-icon">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
                  <rect x="3" y="3" width="18" height="18" rx="2" />
                  <path d="M3 9h18M9 21V9" />
                </svg>
              </div>
              <h3>Walrus Console</h3>
            </div>
            <p>Visual dashboard for the Walrus network.</p>
            <span className="qs-arrow">Coming soon {arrowIcon}</span>
          </a>
          <a className="qs-card qs-card--purple" href={u('/docs/marketplace')}>
            <div className="qs-card-top">
              <div className="qs-icon">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
                  <circle cx="9" cy="21" r="1" /><circle cx="20" cy="21" r="1" />
                  <path d="M1 1h4l2.68 13.39a2 2 0 002 1.61h9.72a2 2 0 002-1.61L23 6H6" />
                </svg>
              </div>
              <h3>Walrus Marketplace</h3>
            </div>
            <p>Buy, sell, and trade data and storage on Walrus.</p>
            <span className="qs-arrow">Coming soon {arrowIcon}</span>
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
