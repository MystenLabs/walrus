// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

import React, { useEffect } from 'react';
import Head from '@docusaurus/Head';
import WalrusLogo from '@site/static/img/Walrus_Docs.svg';

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
  background: #FFFFFF !important; padding: 0 !important; margin: 0 !important;
}
#__docusaurus_skipToContent_fallback > main,
#__docusaurus_skipToContent_fallback > main.container,
#__docusaurus_skipToContent_fallback > main.container--fluid {
  background: #FFFFFF !important; padding: 0 !important; margin: 0 !important; max-width: 100% !important;
}
#__docusaurus_skipToContent_fallback .margin-vert--lg { margin: 0 !important; }
#__docusaurus_skipToContent_fallback .row {
  background: #FFFFFF !important; padding: 0 !important; margin: 0 !important;
}
#__docusaurus_skipToContent_fallback .col {
  background: #FFFFFF !important; padding: 0 !important;
  max-width: 100% !important; flex: none !important; width: 100% !important;
}
#__docusaurus_skipToContent_fallback article {
  background: #FFFFFF !important; padding: 0 !important; margin: 0 !important; max-width: 100% !important;
}
html, body { background: #FFFFFF !important; }
[class*="mainWrapper"] { background: #FFFFFF !important; padding-top: 0 !important; }
[class*="mdxPageWrapper"] { background: #FFFFFF !important; padding: 0 !important; margin: 0 !important; }

/* ── Landing root ── */
.landing-root {
  --white: #000000;
  --black: #FFFFFF;
  --purple: #613DFF;
  --purple-dim: rgba(97,61,255,0.08);
  --purple-hover: rgba(97,61,255,0.12);
  --violet: #CAB1FF;
  --mint: #98EFDD;
  --yellow: #E8FF75;
  --gray-muted: rgba(0,0,0,0.4);
  --surface: #f4f5f7;
  --surface-hover: #ecedf0;
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
.landing-root a:hover { color: #4c2ecc; }
.landing-root,
.landing-root * {
  --ifm-background-color: #FFFFFF !important;
  --ifm-background-surface-color: #FFFFFF !important;
}

.landing-wrap { max-width: 1120px; margin: 0 auto; padding: 0 24px; }
@media (min-width: 768px) { .landing-wrap { padding: 0 40px; } }

/* ── Topbar ── */
.landing-root .topbar {
  position: sticky; top: 0; z-index: 50;
  background: rgba(255, 255, 255, 0.85);
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
  height: 28px; width: auto; margin-bottom: 3px; color: var(--white);
}
.landing-root .topbar-logo .sep { color: rgba(0,0,0,0.2); font-weight: 300; font-size: 2rem; }
.landing-root .topbar-logo .docs-label { color: rgba(0,0,0,0.4); font-weight: 700; font-size: 2rem; }
.landing-root .topbar-links { display: flex; gap: 6px; align-items: center; }
.landing-root .topbar-links a {
  font-size: 1.3rem; padding: 6px 14px; border-radius: 8px;
  color: rgba(0,0,0,0.5); transition: all 0.2s;
}
.landing-root .topbar-links a:hover { color: var(--white); background: rgba(0,0,0,0.04); }
.landing-root .topbar-links a.primary {
  color: var(--white); background: var(--black);
  border: 1px solid var(--border); font-weight: 500;
}
.landing-root .topbar-links a.primary:hover {
  background: var(--purple-dim); border-color: var(--purple);
}

/* ── Hero ── */
.landing-root .hero {
  position: relative; padding: 72px 0 56px; overflow: hidden;
  background: var(--black);
}
.landing-root .hero-inner {
  position: relative; z-index: 5; max-width: 1500px;
  margin-bottom: 28px; padding-top: 20px;
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

/* ── Quick-start cards ── */
.landing-root .quickstart {
  display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px;
  padding-bottom: 15px;
  opacity: 0; animation: landingFadeIn 0.6s ease forwards 0.5s;
}
@media (max-width: 900px) {
  .landing-root .quickstart { grid-template-columns: repeat(2, 1fr); }
}
@media (max-width: 520px) {
  .landing-root .quickstart { grid-template-columns: 1fr; }
}
.landing-root .qs-card {
  background: var(--surface); border: 1px solid var(--border);
  border-radius: var(--radius); padding: 22px 18px;
  transition: all 0.25s ease; cursor: pointer;
  display: flex; flex-direction: column; gap: 8px;
}
.landing-root .qs-card:hover {
  border-color: var(--purple); background: var(--surface-hover);
  transform: translateY(-2px);
  box-shadow: 0 8px 32px rgba(0,0,0,0.06);
}
.landing-root .qs-card .qs-icon {
  width: 34px; height: 34px; border-radius: 9px;
  background: var(--purple-dim);
  display: flex; align-items: center; justify-content: center;
}
.landing-root .qs-card .qs-icon svg {
  width: 17px; height: 17px; color: var(--purple);
}
.landing-root .qs-card h3 {
  font-size: 0.9rem; font-weight: 600;
  line-height: 1.3; margin: 0; color: var(--white);
}
.landing-root .qs-card p {
  font-size: 0.8rem; color: var(--white);
  line-height: 1.45; flex: 1; margin: 0;
}
.landing-root .qs-card .qs-arrow {
  font-size: 0.75rem; color: var(--purple); font-weight: 500;
  display: flex; align-items: center; gap: 4px; margin-top: 2px;
}
.landing-root .qs-card .qs-arrow svg { width: 10px; height: 10px; }

/* ── Divider ── */
.landing-root .divider {
  border: none; border-top: 1px solid var(--border); margin: 0;
}

/* ── Section heading ── */
.landing-root .section-head {
  padding: 20px 0 10px;
  display: flex; align-items: baseline; gap: 12px;
}
.landing-root .section-head .mono-label {
  font-family: var(--mono); font-size: 1.5rem; font-weight: 500;
  letter-spacing: 0.1em; text-transform: uppercase;
  color: var(--purple); flex-shrink: 0;
}
.landing-root .section-head h2 {
  font-size: 1.4rem; font-weight: 500;
  letter-spacing: -0.015em; margin: 0; color: var(--white);
}

/* ── Capabilities ── */
.landing-root .cap-grid {
  display: grid; grid-template-columns: 1fr 1fr;
  gap: 10px; padding-bottom: 15px;
}
@media (max-width: 700px) {
  .landing-root .cap-grid { grid-template-columns: 1fr; }
}
.landing-root .cap-card {
  background: var(--surface); border: 1px solid var(--border);
  border-radius: var(--radius); padding: 24px 22px;
  transition: border-color 0.2s, background 0.2s;
}
.landing-root .cap-card:hover {
  border-color: var(--border-hover);
  background: var(--surface-hover);
}
.landing-root .cap-card h3 {
  font-size: 0.9rem; font-weight: 600;
  margin: 0 0 6px 0; color: var(--white);
  display: flex; align-items: center; gap: 8px;
}
.landing-root .cap-card h3 .tag {
  font-family: var(--mono); font-size: 0.62rem; font-weight: 500;
  color: var(--purple); background: var(--purple-dim);
  padding: 2px 7px; border-radius: 4px; letter-spacing: 0.03em;
}
.landing-root .cap-card p {
  font-size: 0.85rem; color: var(--white);
  line-height: 1.55; margin: 0;
}
.landing-root .cap-card .cap-detail {
  margin-top: 10px; padding-top: 10px;
  border-top: 1px solid var(--border);
  font-family: var(--mono); font-size: 0.72rem;
  color: var(--white); line-height: 1.6; opacity: 0.5;
}

/* ── Use-case rows ── */
.landing-root .usecase-grid {
  display: grid; grid-template-columns: repeat(4, 1fr);
  gap: 10px; padding-bottom: 15px;
}
@media (max-width: 900px) {
  .landing-root .usecase-grid { grid-template-columns: repeat(2, 1fr); }
}
@media (max-width: 520px) {
  .landing-root .usecase-grid { grid-template-columns: 1fr; }
}
.landing-root .uc-card {
  background: var(--surface); border: 1px solid var(--border);
  border-radius: var(--radius); padding: 22px 18px;
  transition: border-color 0.2s;
}
.landing-root .uc-card:hover {
  border-color: var(--border-hover);
}
.landing-root .uc-card h4 {
  font-size: 0.85rem; font-weight: 600;
  margin: 0 0 8px 0; color: var(--white);
}
.landing-root .uc-card ul {
  list-style: none; padding: 0; margin: 0;
}
.landing-root .uc-card li {
  font-size: 0.78rem; color: var(--white); line-height: 1.5;
  padding: 2px 0 2px 14px; position: relative; opacity: 0.7;
}
.landing-root .uc-card li::before {
  content: ''; position: absolute; left: 0; top: 9px;
  width: 4px; height: 4px; border-radius: 50%;
  background: var(--purple); opacity: 0.6;
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

/* ── Not-for ── */
.landing-root .notfor { padding-bottom: 64px; }
.landing-root .notfor-row { display: flex; gap: 8px; flex-wrap: wrap; }
.landing-root .notfor-chip {
  font-size: 0.8rem; color: var(--white); opacity: 0.7;
  background: var(--surface); border: 1px solid var(--border);
  border-radius: 10px; padding: 9px 16px;
}

/* ── Footer ── */
.landing-root .page-footer {
  border-top: 1px solid var(--border); padding: 36px 0;
  display: flex; align-items: center;
  justify-content: space-between;
  flex-wrap: wrap; gap: 16px;
}
.landing-root .footer-left {
  font-size: 0.75rem; color: rgba(0,0,0,0.25);
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
  useEffect(() => {
    const cleanups: Array<() => void> = [];

    const navbar = document.querySelector('.navbar') as HTMLElement | null;
    if (navbar) cleanups.push(hideEl(navbar));

    document.querySelectorAll('.footer, footer').forEach((f) => {
      if (!f.closest('.landing-root')) cleanups.push(hideEl(f as HTMLElement));
    });

    const copyBtn = document.getElementById('copy-page-button-container');
    if (copyBtn) cleanups.push(hideEl(copyBtn));

    cleanups.push(setStyle(document.documentElement, 'background', '#FFFFFF'));
    cleanups.push(setStyle(document.body, 'background', '#FFFFFF'));

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
        cleanups.push(setStyle(el, 'background', '#FFFFFF'));
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

    return () => {
      cleanups.forEach((fn) => fn());
      obs.disconnect();
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

      <header className="topbar">
        <div className="topbar-inner">
          <div className="topbar-logo">
            <WalrusLogo />
          </div>
          <nav className="topbar-links">
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
            <a href="/docs/getting-started" className="primary">
              Get Started →
            </a>
          </nav>
        </div>
      </header>

      <div className="landing-wrap">
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

        <div className="quickstart">
          <a className="qs-card" href="/docs/getting-started">
            <div className="qs-icon">
              <svg
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="2"
                strokeLinecap="round"
              >
                <path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z" />
              </svg>
            </div>
            <h3>Data Storage</h3>
            <p>
              CLI tools, environment setup, and core storage
              operations for developers.
            </p>
            <span className="qs-arrow">
              Get started {arrowIcon}
            </span>
          </a>
          <a
            className="qs-card"
            href="/docs/sites/introduction/components"
          >
            <div className="qs-icon">
              <svg
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="2"
                strokeLinecap="round"
              >
                <rect x="2" y="3" width="20" height="14" rx="2" />
                <path d="M8 21h8M12 17v4" />
              </svg>
            </div>
            <h3>Walrus Sites</h3>
            <p>
              Deploy decentralized static websites with true
              decentralization.
            </p>
            <span className="qs-arrow">
              Learn more {arrowIcon}
            </span>
          </a>
          <a className="qs-card" href="/docs/operator-guide">
            <div className="qs-icon">
              <svg
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="2"
                strokeLinecap="round"
              >
                <circle cx="12" cy="12" r="3" />
                <path
                  d={
                    'M19.4 15a1.65 1.65 0 00.33 1.82l.06.06a2 2 0 01-2.83'
                    + ' 2.83l-.06-.06a1.65 1.65 0 00-1.82-.33 1.65 1.65 0'
                    + ' 00-1 1.51V21a2 2 0 01-4 0v-.09A1.65 1.65 0 009'
                    + ' 19.4a1.65 1.65 0 00-1.82.33l-.06.06a2 2 0'
                    + ' 01-2.83-2.83l.06-.06A1.65 1.65 0 004.68 15a1.65'
                    + ' 1.65 0 00-1.51-1H3a2 2 0 010-4h.09A1.65 1.65 0'
                    + ' 004.6 9a1.65 1.65 0 00-.33-1.82l-.06-.06a2 2 0'
                    + ' 012.83-2.83l.06.06A1.65 1.65 0 009 4.68a1.65'
                    + ' 1.65 0 001-1.51V3a2 2 0 014 0v.09a1.65 1.65 0'
                    + ' 001 1.51 1.65 1.65 0 001.82-.33l.06-.06a2 2 0'
                    + ' 012.83 2.83l-.06.06A1.65 1.65 0 0019.4 9a1.65'
                    + ' 1.65 0 001.51 1H21a2 2 0 010 4h-.09a1.65 1.65'
                    + ' 0 00-1.51 1z'
                  }
                />
              </svg>
            </div>
            <h3>Service Providers</h3>
            <p>
              Operate storage nodes, aggregators, and publishers
              on the network.
            </p>
            <span className="qs-arrow">
              View guide {arrowIcon}
            </span>
          </a>
          <a
            className="qs-card"
            href="/docs/examples/checkpoint-data"
          >
            <div className="qs-icon">
              <svg
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="2"
                strokeLinecap="round"
              >
                <path
                  d={
                    'M14.7 6.3a1 1 0 000 1.4l1.6 1.6a1 1 0'
                    + ' 001.4 0l3.77-3.77a6 6 0 01-7.94 7.94l-6.91'
                    + ' 6.91a2.12 2.12 0 01-3-3l6.91-6.91a6 6 0'
                    + ' 017.94-7.94l-3.76 3.76z'
                  }
                />
              </svg>
            </div>
            <h3>Examples</h3>
            <p>
              Reference applications and integration patterns
              using Walrus.
            </p>
            <span className="qs-arrow">
              Explore {arrowIcon}
            </span>
          </a>
        </div>

        <hr className="divider" />
        <div className="section-head scroll-reveal">
          <span className="mono-label">01</span>
          <h2>Core capabilities</h2>
        </div>
        <div className="cap-grid">
          <div className="cap-card scroll-reveal">
            <h3>Storage &amp; retrieval</h3>
            <p>
              Walrus supports writing and reading large blobs of
              unstructured data. Data is content-addressed. Any change
              to the data produces a new identifier. This makes
              integrity tamper-evident and enables independent
              verification of stored content. Walrus also enables
              anyone to prove that a blob has been stored and remains
              available for retrieval.
            </p>
          </div>
          <div className="cap-card scroll-reveal">
            <h3>Data availability and fault tolerance</h3>
            <p>
              Walrus uses erasure coding and high redundancy (~4.5x)
              to maintain availability even under partial node
              failure.
            </p>
            <ul className="cap-stats">
              <li>
                Reads remain available with up to 2/3 responsive
                nodes.
              </li>
              <li>
                Writes tolerate up to 1/3 unavailable nodes.
              </li>
            </ul>
            <p>
              This model is more robust than partial-replication
              systems and more cost-efficient than full replication.
            </p>
          </div>
          <div className="cap-card scroll-reveal">
            <h3>Cost efficiency</h3>
            <p>
              Through erasure coding, Walrus maintains storage
              overhead at approximately 5x the size of stored data
              while delivering strong durability and Byzantine fault
              tolerance. This enables production-grade availability
              without full replication costs.
            </p>
          </div>
          <div className="cap-card scroll-reveal">
            <h3>Integration with Sui</h3>
            <p>
              Walrus leverages Sui for coordination, attesting
              availability, and payments. Storage space is represented
              as a resource on Sui, which can be owned, split, merged,
              and transferred. Stored blobs are also represented by
              objects on Sui, which means that smart contracts can
              check whether a blob is available and for how long,
              extend its lifetime, or optionally delete it.
            </p>
          </div>
          <div className="cap-card scroll-reveal">
            <h3>Epochs &amp; WAL</h3>
            <p>
              Walrus is operated by a committee of storage nodes that
              evolve between epochs. A native token, WAL (and its
              subdivision FROST, where 1 WAL is equal to 1 billion
              FROST), is used to delegate stake to storage nodes, and
              those with high stake become part of the epoch committee.
              The WAL token is also used for payments for storage. At
              the end of each epoch, rewards for selecting storage
              nodes, storing, and serving blobs are distributed to
              storage nodes and those that stake with them. All these
              processes are mediated by smart contracts on the Sui
              platform.
            </p>
          </div>
          <div className="cap-card scroll-reveal">
            <h3>Flexible access</h3>
            <p>
              You can interact with Walrus through a command-line
              interface (CLI), software development kits (SDKs), and
              Web2 HTTP technologies. Walrus is designed to work well
              with traditional caches and content distribution
              networks (CDNs), while ensuring all operations can also
              be run using local tools to maximize decentralization.
            </p>
            <div className="cap-detail">
              Interfaces: CLI · SDK · HTTP API
            </div>
          </div>
        </div>

        <hr className="divider" />
        <div className="section-head scroll-reveal">
          <span className="mono-label">02</span>
          <h2>When to use Walrus</h2>
        </div>
        <div className="usecase-grid">
          <div className="uc-card scroll-reveal">
            <h4>Independently verifiable</h4>
            <p>
              You need to prove where data came from, confirm it has
              not been altered, or anchor workflows to specific
              dataset versions.
            </p>
            <ul>
              <li>AI model artifacts &amp; agent memory</li>
              <li>Execution logs for exchanges</li>
              <li>Onchain governance data</li>
              <li>Audit trails for financial systems</li>
            </ul>
          </div>
          <div className="uc-card scroll-reveal">
            <h4>Highly available under failure</h4>
            <p>
              Your system cannot tolerate downtime, partial node
              failure, or data loss.
            </p>
            <ul>
              <li>Market infrastructure</li>
              <li>Autonomous agents coordinating state</li>
              <li>Financial protocols with real risk</li>
            </ul>
          </div>
          <div className="uc-card scroll-reveal">
            <h4>Programmable at the data layer</h4>
            <p>
              You need smart contracts to manage, verify, or automate
              around stored data.
            </p>
            <ul>
              <li>Versioned datasets in AI workflows</li>
              <li>Contract-controlled storage lifetimes</li>
              <li>Onchain verification of offchain artifacts</li>
            </ul>
          </div>
          <div className="uc-card scroll-reveal">
            <h4>Cost-efficient at scale</h4>
            <p>
              You require strong durability and Byzantine fault
              tolerance without full-replication overhead.
            </p>
          </div>
        </div>

        <hr className="divider" />
        <div className="section-head scroll-reveal">
          <span className="mono-label">03</span>
          <h2>When not to use Walrus</h2>
        </div>
        <p>Walrus is not optimized for:</p>
        <ul>
          <li>
            Small, ephemeral application state better suited for
            direct onchain storage
          </li>
          <li>Ultra-low-latency in-memory databases</li>
          <li>
            Pure archival storage without verification requirements
          </li>
        </ul>
        <p>
          Walrus is designed for high-stakes systems where
          availability, integrity, and programmability are structural
          requirements, not optional features.
        </p>

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
