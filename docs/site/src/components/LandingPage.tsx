import React, { useEffect } from 'react';
import Head from '@docusaurus/Head';

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
  --white: #fff;
  --black: #000;
  --violet: #cab1ff;
  --violet-dim: rgba(202,177,255,0.10);
  --violet-hover: rgba(202,177,255,0.15);
  --gray-muted: rgba(255,255,255,0.4);
  --surface: #111318;
  --surface-hover: #1a1d24;
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
.landing-root a { color: var(--violet); text-decoration: none; }
.landing-root a:hover { color: #ddd0ff; }
.landing-root,
.landing-root * {
  --ifm-background-color: #000 !important;
  --ifm-background-surface-color: #000 !important;
}

.landing-wrap { max-width: 1120px; margin: 0 auto; padding: 0 24px; }
@media (min-width: 768px) { .landing-wrap { padding: 0 40px; } }

/* ── Topbar ── */
.landing-root .topbar {
  position: sticky; top: 0; z-index: 50;
  background: rgba(0,0,0,0.85);
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
.landing-root .topbar-logo svg { height: 28px; width: auto;   margin-bottom: 3px;}
.landing-root .topbar-logo .sep { color: rgba(255,255,255,0.3); font-weight: 300; font-size: 2rem; }
.landing-root .topbar-logo .docs-label { color: rgba(255,255,255,0.5); font-weight: 700;  font-size: 2rem; }
.landing-root .topbar-links { display: flex; gap: 6px; align-items: center; }
.landing-root .topbar-links a {
  font-size: 1.3rem; padding: 6px 14px; border-radius: 8px;
  color: rgba(255,255,255,0.6); transition: all 0.2s;
}
.landing-root .topbar-links a:hover { color: var(--white); background: rgba(255,255,255,0.06); }
.landing-root .topbar-links a.primary {
  color: var(--white); background: var(--black);
  border: 1px solid var(--border); font-weight: 500;
}
.landing-root .topbar-links a.primary:hover {
  background: var(--violet-dim); border-color: var(--violet);
}

/* ── Hero ── */
.landing-root .hero {
  position: relative; padding: 72px 0 56px; overflow: hidden;
  background: var(--black);
}
.landing-root .hero-inner { position: relative; z-index: 5; max-width: 1500px;   margin-bottom: 28px; }
.landing-root .hero-badge {
  display: inline-block;
  font-family: var(--mono); font-size: 0.72rem; font-weight: 500;
  letter-spacing: 0.08em; text-transform: uppercase;
  color: var(--violet); margin-bottom: 16px;
  opacity: 0; animation: landingFadeIn 0.5s ease forwards 0.1s;
}
.landing-root .hero h1 {
  font-size: clamp(2rem, 5vw, 3rem);
  font-weight: 500; line-height: 1.08; letter-spacing: -0.025em;
  margin-bottom: 14px; color: var(--white);
  opacity: 0; animation: landingFadeIn 0.6s ease forwards 0.2s;
}
.landing-root .hero p {
  font-size: 1rem; color: var(--white); line-height: 1.6; max-width: 1500px;
  opacity: 0; animation: landingFadeIn 0.6s ease forwards 0.35s;
}

/* ── Quick-start cards ── */
.landing-root .quickstart {
  display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px;
  padding-bottom: 64px;
  opacity: 0; animation: landingFadeIn 0.6s ease forwards 0.5s;
}
@media (max-width: 900px) { .landing-root .quickstart { grid-template-columns: repeat(2, 1fr); } }
@media (max-width: 520px) { .landing-root .quickstart { grid-template-columns: 1fr; } }
.landing-root .qs-card {
  background: var(--surface); border: 1px solid var(--border);
  border-radius: var(--radius); padding: 22px 18px;
  transition: all 0.25s ease; cursor: pointer;
  display: flex; flex-direction: column; gap: 8px;
}
.landing-root .qs-card:hover {
  border-color: var(--violet); background: var(--surface-hover);
  transform: translateY(-2px); box-shadow: 0 8px 32px rgba(0,0,0,0.4);
}
.landing-root .qs-card .qs-icon {
  width: 34px; height: 34px; border-radius: 9px;
  background: var(--violet-dim);
  display: flex; align-items: center; justify-content: center;
}
.landing-root .qs-card .qs-icon svg { width: 17px; height: 17px; color: var(--violet); }
.landing-root .qs-card h3 { font-size: 0.9rem; font-weight: 600; line-height: 1.3; margin: 0; color: var(--white); }
.landing-root .qs-card p { font-size: 0.8rem; color: var(--white); line-height: 1.45; flex: 1; margin: 0; }
.landing-root .qs-card .qs-arrow {
  font-size: 0.75rem; color: var(--violet); font-weight: 500;
  display: flex; align-items: center; gap: 4px; margin-top: 2px;
}
.landing-root .qs-card .qs-arrow svg { width: 10px; height: 10px; }

/* ── Divider ── */
.landing-root .divider { border: none; border-top: 1px solid var(--border); margin: 0; }

/* ── Section heading ── */
.landing-root .section-head {
  padding: 56px 0 28px;
  display: flex; align-items: baseline; gap: 12px;
}
.landing-root .section-head .mono-label {
  font-family: var(--mono); font-size: 1.5rem; font-weight: 500;
  letter-spacing: 0.1em; text-transform: uppercase; color: var(--violet);
  flex-shrink: 0;
}
.landing-root .section-head h2 {
  font-size: 1.4rem; font-weight: 500; letter-spacing: -0.015em; margin: 0; color: var(--white);
}

/* ── Capabilities ── */
.landing-root .cap-grid {
  display: grid; grid-template-columns: 1fr 1fr; gap: 10px; padding-bottom: 64px;
}
@media (max-width: 700px) { .landing-root .cap-grid { grid-template-columns: 1fr; } }
.landing-root .cap-card {
  background: var(--surface); border: 1px solid var(--border);
  border-radius: var(--radius); padding: 24px 22px;
  transition: border-color 0.2s, background 0.2s;
}
.landing-root .cap-card:hover { border-color: var(--border-hover); background: var(--surface-hover); }
.landing-root .cap-card h3 {
  font-size: 0.9rem; font-weight: 600; margin: 0 0 6px 0; color: var(--white);
  display: flex; align-items: center; gap: 8px;
}
.landing-root .cap-card h3 .tag {
  font-family: var(--mono); font-size: 0.62rem; font-weight: 500;
  color: var(--violet); background: var(--violet-dim);
  padding: 2px 7px; border-radius: 4px; letter-spacing: 0.03em;
}
.landing-root .cap-card p { font-size: 0.85rem; color: var(--white); line-height: 1.55; margin: 0; }
.landing-root .cap-card .cap-detail {
  margin-top: 10px; padding-top: 10px; border-top: 1px solid var(--border);
  font-family: var(--mono); font-size: 0.72rem; color: var(--white); line-height: 1.6;
  opacity: 0.5;
}

/* ── Use-case rows ── */
.landing-root .usecase-grid {
  display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; padding-bottom: 64px;
}
@media (max-width: 900px) { .landing-root .usecase-grid { grid-template-columns: repeat(2, 1fr); } }
@media (max-width: 520px) { .landing-root .usecase-grid { grid-template-columns: 1fr; } }
.landing-root .uc-card {
  background: var(--surface); border: 1px solid var(--border);
  border-radius: var(--radius); padding: 22px 18px;
  transition: border-color 0.2s;
}
.landing-root .uc-card:hover { border-color: var(--border-hover); }
.landing-root .uc-card h4 { font-size: 0.85rem; font-weight: 600; margin: 0 0 8px 0; color: var(--white); }
.landing-root .uc-card ul { list-style: none; padding: 0; margin: 0; }
.landing-root .uc-card li {
  font-size: 0.78rem; color: var(--white); line-height: 1.5;
  padding: 2px 0 2px 14px; position: relative; opacity: 0.7;
}
.landing-root .uc-card li::before {
  content: ''; position: absolute; left: 0; top: 9px;
  width: 4px; height: 4px; border-radius: 50%;
  background: var(--violet); opacity: 0.6;
}

.landing-root .hero-lead {
  font-size: 1.1rem;
  line-height: 1.5;
  margin-bottom: 16px;
  color: var(--white);
}

.landing-root .hero-body {
  font-size: 0.9rem;
  line-height: 1.6;
  margin-bottom: 24px;
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
  background: var(--violet);
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
  display: flex; align-items: center; justify-content: space-between;
  flex-wrap: wrap; gap: 16px;
}
.landing-root .footer-left { font-size: 0.75rem; color: rgba(255,255,255,0.25); }
.landing-root .footer-right { display: flex; gap: 20px; }
.landing-root .footer-right a { color: var(--violet); font-size: 0.8rem; transition: color 0.2s; }
.landing-root .footer-right a:hover { color: #ddd0ff; }

/* ── Animations ── */
@keyframes landingFadeIn {
  from { opacity: 0; transform: translateY(10px); }
  to   { opacity: 1; transform: translateY(0); }
}
.landing-root .scroll-reveal {
  opacity: 0; transform: translateY(16px);
  transition: opacity 0.5s ease, transform 0.5s ease;
}
.landing-root .scroll-reveal.visible { opacity: 1; transform: translateY(0); }
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

    cleanups.push(setStyle(document.documentElement, 'background', '#000'));
    cleanups.push(setStyle(document.body, 'background', '#000'));

    const root = document.querySelector('.landing-root') as HTMLElement | null;
    if (root) {
      let el: HTMLElement | null = root.parentElement;
      while (el && el !== document.body) {
        cleanups.push(setStyle(el, 'padding', '0'));
        cleanups.push(setStyle(el, 'margin', '0'));
        cleanups.push(setStyle(el, 'maxWidth', '100%'));
        cleanups.push(setStyle(el, 'width', '100%'));
        cleanups.push(setStyle(el, 'background', '#000'));
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
    document.querySelectorAll('.landing-root .scroll-reveal').forEach((el) => obs.observe(el));

    return () => {
      cleanups.forEach((fn) => fn());
      obs.disconnect();
    };
  }, []);

  return (
    <div className="landing-root">
      <Head>
        <link
          href="https://fonts.googleapis.com/css2?family=DM+Sans:opsz,wght@9..40,300;9..40,400;9..40,500;9..40,600&family=JetBrains+Mono:wght@400;500&display=swap"
          rel="stylesheet"
        />
      </Head>
      <style dangerouslySetInnerHTML={{ __html: LANDING_CSS }} />

      <header className="topbar">
        <div className="topbar-inner">
          <div className="topbar-logo">
            <svg viewBox="0 0 124 29" xmlns="http://www.w3.org/2000/svg" fill="currentColor">
              <path d="M34.1435 14.1431C34.5465 12.879 35.1193 11.8005 35.8541 10.8998C36.5928 10.0031 37.4658 9.28805 38.4732 8.7666C39.4806 8.24119 40.5511 7.98047 41.677 7.98047C43.3164 7.98047 44.711 8.47032 45.8684 9.45001C47.022 10.4297 47.9069 11.7017 48.531 13.2582H48.934V8.17799H54.6977V28.325H48.934V23.2448H48.531C47.9148 24.8052 47.0259 26.0733 45.8684 27.053C44.7149 28.0327 43.3164 28.5225 41.677 28.5225C40.5472 28.5225 39.4885 28.2618 38.493 27.7364C37.4974 27.211 36.6284 26.4999 35.8738 25.6032C35.1232 24.7064 34.5425 23.6201 34.1396 22.3599C33.7366 21.0958 33.5352 19.725 33.5352 18.2515C33.5352 16.778 33.7366 15.4033 34.1396 14.1431H34.1435ZM39.8045 20.3294C40.0336 20.9615 40.3576 21.5106 40.7724 21.9807C41.1911 22.4508 41.6849 22.8221 42.2656 23.0907C42.8424 23.3593 43.4666 23.4937 44.1381 23.4937C44.8097 23.4937 45.4694 23.3593 46.0304 23.0907C46.5953 22.8221 47.0773 22.4587 47.4802 22.0044C47.8832 21.5461 48.211 21.001 48.4678 20.3729C48.7246 19.7408 48.851 19.0376 48.851 18.2554C48.851 17.4733 48.7246 16.774 48.4678 16.138C48.211 15.506 47.8832 14.9648 47.4802 14.5065C47.0773 14.0483 46.5953 13.6888 46.0304 13.4202C45.4655 13.1515 44.8334 13.0172 44.1381 13.0172C43.4429 13.0172 42.8424 13.1515 42.2656 13.4202C41.6889 13.6888 41.1911 14.0601 40.7724 14.5302C40.3536 15.0003 40.0336 15.5494 39.8045 16.1815C39.5754 16.8135 39.4608 17.5049 39.4608 18.2594C39.4608 19.0139 39.5754 19.7013 39.8045 20.3373V20.3294Z" />
              <path d="M57.6055 0H63.5311V28.3283H57.6055V0Z" />
              <path d="M66.4414 8.18194H72.1617V13.2621H72.6871C73.1453 11.7294 73.967 10.4811 75.164 9.51322C76.361 8.54537 78.032 8.03577 80.1811 7.98047V14.309H78.3678C77.3486 14.309 76.4598 14.4315 75.7092 14.6724C74.9586 14.9134 74.3344 15.2847 73.8367 15.7825C73.3389 16.2802 72.9676 16.9163 72.7266 17.6984C72.4856 18.4767 72.3632 19.4445 72.3632 20.598V28.3329H66.4375V8.18194H66.4414Z" />
              <path d="M82.2188 8.18144H88.101V18.6974C88.101 20.2578 88.4012 21.4271 89.0096 22.2014C89.614 22.9796 90.491 23.3707 91.6484 23.3707C92.1857 23.3707 92.7032 23.264 93.201 23.0468C93.6987 22.8334 94.153 22.4779 94.5718 21.9762C94.9905 21.4785 95.3303 20.8148 95.5989 19.9813C95.8675 19.1517 96.0018 18.1009 96.0018 16.8407V8.17749H101.967V28.3245H96.2033V23.2877H95.6779C95.4369 24.066 95.113 24.781 94.71 25.421C94.3071 26.0649 93.8172 26.6219 93.2405 27.092C92.6637 27.5621 92.004 27.9334 91.2653 28.202C90.5265 28.4707 89.7404 28.605 88.9068 28.605C87.9113 28.605 87.0067 28.443 86.1889 28.1191C85.3712 27.7951 84.672 27.2776 84.0952 26.5666C83.5184 25.8555 83.0602 24.9469 82.7244 23.8487C82.3886 22.7465 82.2227 21.4311 82.2227 19.8983V8.17749L82.2188 8.18144Z" />
              <path d="M109.961 21.6364C110.068 22.6043 110.451 23.3312 111.111 23.8131C111.767 24.299 112.77 24.536 114.113 24.536C114.544 24.536 114.943 24.4965 115.322 24.4136C115.697 24.3346 116.025 24.22 116.31 24.0699C116.594 23.9237 116.823 23.7262 116.997 23.4852C117.171 23.2442 117.258 22.9598 117.258 22.6398C117.258 22.2883 117.183 22.0275 117.036 21.8537C116.886 21.6799 116.693 21.5179 116.452 21.3678C116.211 21.2216 115.95 21.1071 115.666 21.0241C115.381 20.9412 115.053 20.8622 114.678 20.7832L111.052 20.1787C110.273 20.0444 109.491 19.843 108.713 19.5743C107.935 19.3057 107.235 18.9502 106.619 18.5038C106.003 18.0574 105.497 17.4846 105.11 16.7696C104.719 16.0585 104.525 15.2052 104.525 14.2137C104.525 12.9495 104.786 11.8908 105.312 11.0336C105.837 10.1764 106.52 9.48898 107.366 8.97938C108.211 8.46978 109.159 8.10635 110.206 7.89302C111.253 7.67575 112.312 7.56909 113.386 7.56909C115.081 7.56909 116.503 7.76266 117.657 8.15375C118.81 8.54484 119.758 9.06234 120.497 9.70625C121.236 10.3502 121.793 11.1047 122.168 11.9619C122.543 12.8192 122.8 13.7199 122.934 14.664H116.93C116.851 13.8344 116.527 13.1826 115.962 12.7086C115.401 12.2385 114.536 12.0054 113.382 12.0054C112.979 12.0054 112.608 12.033 112.272 12.0844C111.936 12.1397 111.632 12.2464 111.364 12.4083C111.095 12.5703 110.878 12.7718 110.72 13.0127C110.558 13.2537 110.479 13.5381 110.479 13.8581C110.479 14.0991 110.538 14.3282 110.66 14.5415C110.783 14.7588 110.945 14.9247 111.146 15.0432C111.348 15.1657 111.581 15.2645 111.853 15.3435C112.122 15.4225 112.418 15.5054 112.738 15.5844L116.527 16.2679C117.333 16.4022 118.131 16.5957 118.925 16.8525C119.715 17.1093 120.434 17.4688 121.082 17.9389C121.73 18.409 122.231 18.9857 122.591 19.6731C122.95 20.3605 123.136 21.1979 123.136 22.1895C123.136 23.3707 122.903 24.3938 122.433 25.251C121.963 26.1083 121.319 26.8154 120.501 27.3685C119.683 27.9176 118.727 28.3284 117.641 28.597C116.554 28.8657 115.377 29 114.113 29C112.446 29 111.004 28.8064 109.779 28.4153C108.555 28.0242 107.536 27.4949 106.718 26.8233C105.9 26.1517 105.268 25.3696 104.826 24.4649C104.379 23.5682 104.107 22.6201 104 21.6246H109.965L109.961 21.6364Z" />
              <path d="M11.9975 23.1732L13.4117 8.17749H20.2618L21.6761 23.1732L22.4701 23.3272C23.2286 21.5535 25.7174 15.8729 26.2428 8.18144H33.6577L28.0955 28.3284H18.4407L17.054 18.2549H16.6353L15.2487 28.3284H5.58197L0 8.18144H7.43078C7.95619 15.8768 10.441 21.5535 11.2034 23.3272L11.9975 23.1732Z" />
            </svg>
            <span className="sep">/</span>
            <span className="docs-label">docs</span>
          </div>
          <nav className="topbar-links">
            <a href="https://github.com/MystenLabs/walrus" target="_blank" rel="noopener noreferrer">GitHub</a>
            <a href="https://discord.gg/walrusprotocol" target="_blank" rel="noopener noreferrer">Discord</a>
            <a href="/docs/getting-started" className="primary">Get Started →</a>
          </nav>
        </div>
      </header>

      <div className="landing-wrap">
        <div className="hero-inner">
        <p className="hero-lead">
            A verifiable data platform for high-stakes systems that require provable,
            programmable, always-available data with no performance tradeoffs.
        </p>
        <p className="hero-body">
            Modern financial systems and AI agents depend on fast, reliable, and
            verifiable data. Traditional storage assumes integrity and pushes trust
            outside the data layer. Walrus embeds availability, integrity, and
            programmability directly into storage itself.
        </p>
        <ul className="hero-features">
            <li>Highly available</li>
            <li>Cryptographically verifiable</li>
            <li>Programmable through smart contracts</li>
        </ul>
        </div>
        <div>
        
        </div>

        <div className="quickstart">
          <a className="qs-card" href="/docs/getting-started">
            <div className="qs-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round"><path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z" /></svg></div>
            <h3>Data Storage</h3><p>CLI tools, environment setup, and core storage operations for developers.</p>
            <span className="qs-arrow">Get started <svg viewBox="0 0 13 13" fill="none"><path d="M11.52 5.66L5.86 0L5.16 .71L10.31 5.86H0V6.86H10.31L5.16 12.02L5.86 12.73L11.52 7.07L12.23 6.36L11.52 5.66Z" fill="currentColor" /></svg></span>
          </a>
          <a className="qs-card" href="/docs/sites/introduction/components">
            <div className="qs-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round"><rect x="2" y="3" width="20" height="14" rx="2" /><path d="M8 21h8M12 17v4" /></svg></div>
            <h3>Walrus Sites</h3><p>Deploy decentralized static websites with true decentralization.</p>
            <span className="qs-arrow">Learn more <svg viewBox="0 0 13 13" fill="none"><path d="M11.52 5.66L5.86 0L5.16 .71L10.31 5.86H0V6.86H10.31L5.16 12.02L5.86 12.73L11.52 7.07L12.23 6.36L11.52 5.66Z" fill="currentColor" /></svg></span>
          </a>
          <a className="qs-card" href="/docs/operator-guide">
            <div className="qs-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round"><circle cx="12" cy="12" r="3" /><path d="M19.4 15a1.65 1.65 0 00.33 1.82l.06.06a2 2 0 01-2.83 2.83l-.06-.06a1.65 1.65 0 00-1.82-.33 1.65 1.65 0 00-1 1.51V21a2 2 0 01-4 0v-.09A1.65 1.65 0 009 19.4a1.65 1.65 0 00-1.82.33l-.06.06a2 2 0 01-2.83-2.83l.06-.06A1.65 1.65 0 004.68 15a1.65 1.65 0 00-1.51-1H3a2 2 0 010-4h.09A1.65 1.65 0 004.6 9a1.65 1.65 0 00-.33-1.82l-.06-.06a2 2 0 012.83-2.83l.06.06A1.65 1.65 0 009 4.68a1.65 1.65 0 001-1.51V3a2 2 0 014 0v.09a1.65 1.65 0 001 1.51 1.65 1.65 0 001.82-.33l.06-.06a2 2 0 012.83 2.83l-.06.06A1.65 1.65 0 0019.4 9a1.65 1.65 0 001.51 1H21a2 2 0 010 4h-.09a1.65 1.65 0 00-1.51 1z" /></svg></div>
            <h3>Service Providers</h3><p>Operate storage nodes, aggregators, and publishers on the network.</p>
            <span className="qs-arrow">View guide <svg viewBox="0 0 13 13" fill="none"><path d="M11.52 5.66L5.86 0L5.16 .71L10.31 5.86H0V6.86H10.31L5.16 12.02L5.86 12.73L11.52 7.07L12.23 6.36L11.52 5.66Z" fill="currentColor" /></svg></span>
          </a>
          <a className="qs-card" href="/docs/examples/checkpoint-data">
            <div className="qs-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round"><path d="M14.7 6.3a1 1 0 000 1.4l1.6 1.6a1 1 0 001.4 0l3.77-3.77a6 6 0 01-7.94 7.94l-6.91 6.91a2.12 2.12 0 01-3-3l6.91-6.91a6 6 0 017.94-7.94l-3.76 3.76z" /></svg></div>
            <h3>Examples</h3><p>Reference applications and integration patterns using Walrus.</p>
            <span className="qs-arrow">Explore <svg viewBox="0 0 13 13" fill="none"><path d="M11.52 5.66L5.86 0L5.16 .71L10.31 5.86H0V6.86H10.31L5.16 12.02L5.86 12.73L11.52 7.07L12.23 6.36L11.52 5.66Z" fill="currentColor" /></svg></span>
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
            <p>Walrus supports writing and reading large blobs of unstructured data. Data is content-addressed. Any change to the data produces a new identifier. This makes integrity tamper-evident and enables independent verification of stored content. Walrus also enables anyone to prove that a blob has been stored and remains available for retrieval.</p>
        </div>
        <div className="cap-card scroll-reveal">
            <h3>Data availability and fault tolerance</h3>
            <p>Walrus uses erasure coding and high redundancy (~4.5x) to maintain availability even under partial node failure.</p>
            <ul className="cap-stats">
            <li>Reads remain available with up to 2/3 responsive nodes.</li>
            <li>Writes tolerate up to 1/3 unavailable nodes.</li>
            </ul>
            <p>This model is more robust than partial-replication systems and more cost-efficient than full replication.</p>
        </div>
        <div className="cap-card scroll-reveal">
            <h3>Cost efficiency</h3>
            <p>Through erasure coding, Walrus maintains storage overhead at approximately 5x the size of stored data while delivering strong durability and Byzantine fault tolerance. This enables production-grade availability without full replication costs.</p>
        </div>
        <div className="cap-card scroll-reveal">
            <h3>Integration with Sui</h3>
            <p>Walrus leverages Sui for coordination, attesting availability, and payments. Storage space is represented as a resource on Sui, which can be owned, split, merged, and transferred. Stored blobs are also represented by objects on Sui, which means that smart contracts can check whether a blob is available and for how long, extend its lifetime, or optionally delete it.</p>
        </div>
        <div className="cap-card scroll-reveal">
            <h3>Epochs &amp; WAL</h3>
            <p>Walrus is operated by a committee of storage nodes that evolve between epochs. A native token, WAL (and its subdivision FROST, where 1 WAL is equal to 1 billion FROST), is used to delegate stake to storage nodes, and those with high stake become part of the epoch committee. The WAL token is also used for payments for storage. At the end of each epoch, rewards for selecting storage nodes, storing, and serving blobs are distributed to storage nodes and those that stake with them. All these processes are mediated by smart contracts on the Sui platform.</p>
        </div>
        <div className="cap-card scroll-reveal">
            <h3>Flexible access</h3>
            <p>You can interact with Walrus through a command-line interface (CLI), software development kits (SDKs), and Web2 HTTP technologies. Walrus is designed to work well with traditional caches and content distribution networks (CDNs), while ensuring all operations can also be run using local tools to maximize decentralization.</p>
            <div className="cap-detail">Interfaces: CLI · SDK · HTTP API</div>
        </div>
        </div>

        <hr className="divider" />
        <div className="section-head scroll-reveal"><span className="mono-label">02</span><h2>When to use Walrus</h2></div>
        <div className="usecase-grid">
          <div className="uc-card scroll-reveal"><h4>Independently verifiable</h4><p>You need to prove where data came from, confirm it has not been altered, or anchor workflows to specific dataset versions.</p><ul><li>AI model artifacts &amp; agent memory</li><li>Execution logs for exchanges</li><li>On-chain governance data</li><li>Audit trails for financial systems</li></ul></div>
          <div className="uc-card scroll-reveal"><h4>Highly available under failure</h4><p>Your system cannot tolerate downtime, partial node failure, or data loss.</p><ul><li>Market infrastructure</li><li>Autonomous agents coordinating state</li><li>Financial protocols with real risk</li></ul></div>
          <div className="uc-card scroll-reveal"><h4>Programmable at the data layer</h4><p>You need smart contracts to manage, verify, or automate around stored data.</p><ul><li>Versioned datasets in AI workflows</li><li>Contract-controlled storage lifetimes</li><li>On-chain verification of off-chain artifacts</li></ul></div>
          <div className="uc-card scroll-reveal"><h4>Cost-efficient at scale</h4><p>You require strong durability and Byzantine fault tolerance without full-replication overhead.</p></div>
        </div>

        <hr className="divider" />
        <div className="section-head scroll-reveal"><span className="mono-label">03</span><h2>When not to use Walrus</h2></div>
        <p>Walrus is not optimized for:</p>
        <ul><li>Small, ephemeral application state better suited for direct on-chain storage</li>
        <li>Ultra-low-latency in-memory databases</li>
        <li>Pure archival storage without verification requirements</li>
        </ul>
        <p>Walrus is designed for high-stakes systems where availability, integrity, and programmability are structural requirements, not optional features.</p>

        <footer className="page-footer">
          <div className="footer-left">© 2026 Walrus Foundation</div>
          <nav className="footer-right">
            <a href="https://github.com/MystenLabs/walrus" target="_blank" rel="noopener noreferrer">GitHub</a>
            <a href="https://discord.gg/walrusprotocol" target="_blank" rel="noopener noreferrer">Discord</a>
            <a href="https://x.com/walrusprotocol" target="_blank" rel="noopener noreferrer">X</a>
            <a href="https://docs.wal.app/docs/legal/privacy" target="_blank" rel="noopener noreferrer">Privacy</a>
            <a href="https://docs.wal.app/docs/legal/walrus_general_tos" target="_blank" rel="noopener noreferrer">Terms</a>
          </nav>
        </footer>
      </div>
    </div>
  );
}