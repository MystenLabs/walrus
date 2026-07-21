// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

import React from 'react';
import Head from '@docusaurus/Head';
import WalrusLogo from '@site/static/img/Walrus_Docs.svg';

const LANDING_CSS = `
/* ── Hide Docusaurus chrome ── */
.navbar { display: none !important; }
#copy-page-button-container { display: none !important; }

#__docusaurus_skipToContent_fallback {
  background: #000 !important; padding: 0 !important; margin: 0 !important;
}
#__docusaurus_skipToContent_fallback > main,
#__docusaurus_skipToContent_fallback > main.container,
#__docusaurus_skipToContent_fallback > main.container--fluid {
  background: #000 !important; padding: 0 !important; margin: 0 !important; max-width: 100% !important;
}
#__docusaurus_skipToContent_fallback .margin-vert--lg { margin: 0 !important; }
#__docusaurus_skipToContent_fallback .row {
  background: #000 !important; padding: 0 !important; margin: 0 !important;
}
#__docusaurus_skipToContent_fallback .col {
  background: #000 !important; padding: 0 !important;
  max-width: 100% !important; flex: none !important; width: 100% !important;
}
#__docusaurus_skipToContent_fallback article {
  background: #000 !important; padding: 0 !important; margin: 0 !important; max-width: 100% !important;
}
html, body { background: #000 !important; }
[class*="mainWrapper"] { background: #000 !important; padding-top: 0 !important; }
[class*="mdxPageWrapper"] { background: #000 !important; padding: 0 !important; margin: 0 !important; }

/* ── Landing root ── */
.landing-root {
  --white: #faf8f5;
  --black: #000;
  --purple: #CAB1FF;
  --purple-dim: rgba(202,177,255,0.1);
  --purple-hover: rgba(202,177,255,0.16);
  --text-secondary: #b0afac;
  --border: rgba(255,255,255,0.08);
  --border-hover: rgba(255,255,255,0.16);
  --sans: 'Google Sans Flex', -apple-system, sans-serif;
  --display: 'Ratch', 'Google Sans Flex', sans-serif;
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
  --ifm-background-color: #000 !important;
  --ifm-background-surface-color: #000 !important;
}

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
  display: flex;
  flex-direction: column;
  align-items: center;
  text-align: center;
  padding: clamp(88px, 16vh, 168px) 24px clamp(44px, 7vh, 84px);
  background: #000;
}
.landing-root .hero-title {
  font-family: var(--display);
  font-weight: 500;
  font-size: clamp(2.75rem, 9vw, 6rem);
  line-height: 0.95;
  letter-spacing: -0.02em;
  margin: 0;
  color: var(--white);
}
.landing-root .hero-subline {
  max-width: 672px;
  margin: 12px 0 0;
  font-family: var(--sans);
  font-weight: 400;
  font-size: clamp(1rem, 2.4vw, 1.25rem);
  line-height: 1.5;
  letter-spacing: -0.01em;
  color: var(--text-secondary);
}

/* ── Home cards ── */
.landing-root .home-grid {
  max-width: 1200px;
  margin: 0 auto;
  padding: 0 24px 112px;
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 24px;
}
@media (max-width: 700px) {
  .landing-root .home-grid { grid-template-columns: 1fr; }
}
.landing-root .home-card {
  display: flex;
  flex-direction: column;
  background: rgba(161, 200, 255, 0.1);
  border: 1px solid var(--border);
  border-radius: 12px;
  overflow: hidden;
  color: inherit;
  transition: transform 0.25s ease, border-color 0.25s ease;
}
.landing-root .home-card:hover {
  color: inherit;
  transform: scale(1.02);
  border-color: var(--border-hover);
}
.landing-root .home-card-media {
  display: block;
  width: 100%;
  height: auto;
  background: #000;
}
.landing-root .home-card-body {
  flex: 1;
  padding: 28px 24px 40px;
  text-align: center;
}
.landing-root .home-card-body h3 {
  font-family: var(--display);
  font-weight: 500;
  font-size: 1.75rem;
  line-height: 1.2;
  margin: 0;
  color: var(--white);
}
.landing-root .home-card-body p {
  max-width: 364px;
  margin: 10px auto 0;
  font-family: var(--sans);
  font-weight: 400;
  font-size: 1rem;
  line-height: 1.5;
  color: var(--text-secondary);
}
`;

const HOME_CARDS = [
  {
    image: '/img/home/walrus-memory.webp',
    title: 'Walrus Memory',
    description:
      'Portable memory layer for AI agents that persists context '
      + 'across apps and sessions.',
    href: '/walrus-memory/getting-started/what-is-walrus-memory',
  },
  {
    image: '/img/home/walrus-protocol.webp',
    title: 'Walrus Protocol',
    description:
      'Open-source decentralized storage infrastructure for '
      + 'building your own data layer.',
    href: '/docs/getting-started',
  },
  {
    image: '/img/home/walrus-skills.webp',
    title: 'Walrus Skills',
    description:
      'Pre-built skills for AI coding agents that accelerate '
      + 'Walrus development.',
    href: '/skills',
  },
  {
    image: '/img/home/walrus-sites.webp',
    title: 'Walrus Sites',
    description:
      'Publish highly resilient websites and frontend applications '
      + 'on Walrus.',
    href: '/docs/sites',
  },
];

export default function LandingPage() {
  return (
    <div className="landing-root">
      <Head>
        <link rel="preconnect" href="https://fonts.googleapis.com" />
        <link rel="preconnect" href="https://fonts.gstatic.com" crossOrigin="anonymous" />
        <link
          href={
            'https://fonts.googleapis.com/css2'
            + '?family=Google+Sans+Flex:opsz@6..144'
            + '&display=swap'
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
            <button
              className="kapa-landing-btn"
              onClick={() => { if ((window as any).Kapa) (window as any).Kapa.open(); }}
            >
              Ask Walrus AI
            </button>
            <a href="/docs/getting-started" className="primary">
              Get Started →
            </a>
          </nav>
        </div>
      </header>

      <section className="hero">
        <h1 className="hero-title">Build with Walrus</h1>
        <p className="hero-subline">
          Keep critical data persistent, portable, and under your
          control across apps, providers, and agents.
        </p>
      </section>

      <section className="home-grid">
        {HOME_CARDS.map((card) => (
          <a className="home-card" key={card.title} href={card.href}>
            <img
              className="home-card-media"
              src={card.image}
              alt=""
              width={1110}
              height={451}
              loading="lazy"
            />
            <div className="home-card-body">
              <h3>{card.title}</h3>
              <p>{card.description}</p>
            </div>
          </a>
        ))}
      </section>
    </div>
  );
}
