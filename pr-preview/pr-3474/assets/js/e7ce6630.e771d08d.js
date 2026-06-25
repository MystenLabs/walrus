"use strict";(globalThis.webpackChunkwalrus_docusaurus=globalThis.webpackChunkwalrus_docusaurus||[]).push([[6490],{35756(r,o,a){a.d(o,{R:()=>i,x:()=>d});var e=a(59471);let n={},t=e.createContext(n);function i(r){let o=e.useContext(t);return e.useMemo(function(){return"function"==typeof r?r(o):{...o,...r}},[o,r])}function d(r){let o;return o=r.disableParentContext?"function"==typeof r.components?r.components(n):r.components||n:i(r.components),e.createElement(t.Provider,{value:o},r.children)}},66034(r,o,a){a.r(o),a.d(o,{assets:()=>x,contentTitle:()=>m,default:()=>v,frontMatter:()=>h,metadata:()=>e,toc:()=>f});let e=JSON.parse('{"type":"mdx","permalink":"/walrus/pr-preview/pr-3474/","source":"@site/src/pages/index.mdx","title":"Walrus","frontMatter":{"title":"Walrus","hide_table_of_contents":true},"unlisted":false}');var n=a(62615),t=a(35756),i=a(59471),d=a(58289),s=a(52439),l=a(48153);function c(r){let o=r.style.display;return r.style.display="none",()=>{r.style.display=o}}function p(r,o,a){let e=r.style[o];return r.style[o]=a,()=>{r.style[o]=e}}let g=`
/* \u{2500}\u{2500} Hide Docusaurus chrome \u{2500}\u{2500} */
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

/* \u{2500}\u{2500} Landing root \u{2500}\u{2500} */
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

/* \u{2500}\u{2500} Topbar \u{2500}\u{2500} */
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

/* \u{2500}\u{2500} Hero \u{2500}\u{2500} */
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

/* \u{2500}\u{2500} Product card grid (Mem0-inspired) \u{2500}\u{2500} */
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

/* \u{2500}\u{2500} Landing search bar \u{2500}\u{2500} */
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

/* \u{2500}\u{2500} Search modal z-index fix \u{2500}\u{2500} */
.landing-root .fixed.inset-0,
.landing-root [class*="z-500"] {
  z-index: 9999 !important;
}

/* \u{2500}\u{2500} Divider \u{2500}\u{2500} */
.landing-root .divider {
  border: none; border-top: 1px solid var(--border); margin: 0;
}


/* \u{2500}\u{2500} Footer \u{2500}\u{2500} */
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

/* \u{2500}\u{2500} Animations \u{2500}\u{2500} */
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
`;function u(){let[r,o]=(0,i.useState)(!1);(0,i.useEffect)(()=>{let r=[],a=document.querySelector(".navbar");a&&r.push(c(a)),document.querySelectorAll(".footer, footer").forEach(o=>{o.closest(".landing-root")||r.push(c(o))});let e=document.getElementById("copy-page-button-container");e&&r.push(c(e)),r.push(p(document.documentElement,"background","#0d0f12")),r.push(p(document.body,"background","#0d0f12"));let n=document.querySelector(".landing-root");if(n){let o=n.parentElement;for(;o&&o!==document.body;)r.push(p(o,"padding","0")),r.push(p(o,"margin","0")),r.push(p(o,"maxWidth","100%")),r.push(p(o,"width","100%")),r.push(p(o,"background","#0d0f12")),o=o.parentElement}let t=new IntersectionObserver(r=>{r.forEach(r=>{r.isIntersecting&&r.target.classList.add("visible")})},{threshold:.1,rootMargin:"0px 0px -30px 0px"});function i(r){"/"!==r.key||r.metaKey||r.ctrlKey||r.target instanceof HTMLInputElement||r.target instanceof HTMLTextAreaElement||(r.preventDefault(),o(!0))}return document.querySelectorAll(".landing-root .scroll-reveal").forEach(r=>t.observe(r)),document.addEventListener("keydown",i),()=>{r.forEach(r=>r()),t.disconnect(),document.removeEventListener("keydown",i)}},[]);let a=(0,n.jsx)("svg",{viewBox:"0 0 13 13",fill:"none",children:(0,n.jsx)("path",{d:"M11.52 5.66L5.86 0L5.16 .71L10.31 5.86H0V6.86H10.31L5.16 12.02L5.86 12.73L11.52 7.07L12.23 6.36L11.52 5.66Z",fill:"currentColor"})});return(0,n.jsxs)("div",{className:"landing-root",children:[(0,n.jsx)(s.A,{children:(0,n.jsx)("link",{href:"https://fonts.googleapis.com/css2?family=DM+Sans:opsz,wght@9..40,300;9..40,400;9..40,500;9..40,600&family=JetBrains+Mono:wght@400;500&display=swap",rel:"stylesheet"})}),(0,n.jsx)("style",{dangerouslySetInnerHTML:{__html:g}}),(0,n.jsxs)("div",{className:"landing-wrap",children:[(0,n.jsxs)("div",{className:"hero-inner",children:[(0,n.jsx)("h1",{className:"hero-title",children:"Build with Walrus"}),(0,n.jsx)("p",{className:"hero-sub",children:"Keep critical data persistent, portable, and under your control across apps, providers, and agents."})]}),(0,n.jsxs)("div",{className:"landing-search",children:[(0,n.jsxs)("button",{type:"button",className:"landing-search-btn",onClick:()=>o(!0),children:[(0,n.jsx)("svg",{viewBox:"0 0 20 20",fill:"none",children:(0,n.jsx)("path",{d:"M14.386 14.386l4.088 4.088-4.088-4.088c-2.942 2.942-7.711 2.942-10.653 0-2.942-2.942-2.942-7.711 0-10.653 2.942-2.942 7.711-2.942 10.653 0 2.942 2.942 2.942 7.711 0 10.653z",stroke:"currentColor",strokeWidth:"1.5",strokeLinecap:"round",strokeLinejoin:"round"})}),(0,n.jsx)("span",{children:"Search docs or ask Walrus AI..."}),(0,n.jsx)("kbd",{children:"/"})]}),r&&(0,d.createPortal)((0,n.jsx)(l.A,{isOpen:r,onClose:()=>o(!1)}),document.body)]}),(0,n.jsxs)("div",{className:"product-grid",children:[(0,n.jsxs)("a",{className:"product-card",href:"/walrus-memory/getting-started/what-is-walrus-memory",children:[(0,n.jsx)("div",{className:"product-card-thumb",children:(0,n.jsxs)("svg",{viewBox:"0 0 48 48",fill:"none",stroke:"currentColor",strokeWidth:"1.5",children:[(0,n.jsx)("circle",{cx:"24",cy:"16",r:"10"}),(0,n.jsx)("path",{d:"M24 26v4M18 38h12M20 26c0 2.5 1.8 4 4 4s4-1.5 4-4"})]})}),(0,n.jsx)("h3",{children:"Walrus Memory"}),(0,n.jsx)("p",{children:"Portable memory layer that gives AI agents persistent context across apps and sessions."}),(0,n.jsx)("span",{className:"product-arrow",children:a})]}),(0,n.jsxs)("a",{className:"product-card product-card--muted",href:"/docs/getting-started",children:[(0,n.jsx)("div",{className:"product-card-thumb",children:(0,n.jsxs)("svg",{viewBox:"0 0 48 48",fill:"none",stroke:"currentColor",strokeWidth:"1.5",children:[(0,n.jsx)("rect",{x:"8",y:"8",width:"32",height:"32",rx:"4"}),(0,n.jsx)("path",{d:"M8 18h32M18 48V18"})]})}),(0,n.jsxs)("h3",{children:["Walrus Console ",(0,n.jsx)("span",{className:"coming-soon-badge",children:"Coming soon"})]}),(0,n.jsx)("p",{children:"Unified control plane for managing files, datasets, memory, and other assets on Walrus."}),(0,n.jsx)("span",{className:"product-arrow",children:a})]}),(0,n.jsxs)("a",{className:"product-card product-card--muted",href:"/docs/getting-started",children:[(0,n.jsx)("div",{className:"product-card-thumb",children:(0,n.jsxs)("svg",{viewBox:"0 0 48 48",fill:"none",stroke:"currentColor",strokeWidth:"1.5",children:[(0,n.jsx)("circle",{cx:"18",cy:"38",r:"3"}),(0,n.jsx)("circle",{cx:"36",cy:"38",r:"3"}),(0,n.jsx)("path",{d:"M4 4h8l5.36 26.78a4 4 0 004 3.22h15.28a4 4 0 004-3.22L44 14H12"})]})}),(0,n.jsxs)("h3",{children:["Walrus Marketplace ",(0,n.jsx)("span",{className:"coming-soon-badge",children:"Coming soon"})]}),(0,n.jsx)("p",{children:"Open marketplace where developers and AI agents discover, license, and access data."}),(0,n.jsx)("span",{className:"product-arrow",children:a})]}),(0,n.jsxs)("a",{className:"product-card",href:"/docs/getting-started",children:[(0,n.jsx)("div",{className:"product-card-thumb",children:(0,n.jsxs)("svg",{viewBox:"0 0 48 48",fill:"none",stroke:"currentColor",strokeWidth:"1.5",children:[(0,n.jsx)("path",{d:"M42 32V16a4 4 0 00-2-3.46l-14-8a4 4 0 00-4 0l-14 8A4 4 0 006 16v16a4 4 0 002 3.46l14 8a4 4 0 004 0l14-8A4 4 0 0042 32z"}),(0,n.jsx)("polyline",{points:"6.54 13.92 24 24.02 41.46 13.92"}),(0,n.jsx)("line",{x1:"24",y1:"44.16",x2:"24",y2:"24"})]})}),(0,n.jsx)("h3",{children:"Walrus Protocol"}),(0,n.jsxs)("p",{children:["Open source, decentralized data storage. ",(0,n.jsx)("span",{className:"under-hood",children:"Under the hood."})]}),(0,n.jsx)("span",{className:"product-arrow",children:a})]})]}),(0,n.jsxs)("footer",{className:"page-footer",children:[(0,n.jsx)("div",{className:"footer-left",children:"\xa9 2026 Walrus Foundation"}),(0,n.jsxs)("nav",{className:"footer-right",children:[(0,n.jsx)("a",{href:"https://github.com/MystenLabs/walrus",target:"_blank",rel:"noopener noreferrer",children:"GitHub"}),(0,n.jsx)("a",{href:"https://discord.gg/walrusprotocol",target:"_blank",rel:"noopener noreferrer",children:"Discord"}),(0,n.jsx)("a",{href:"https://x.com/walrusprotocol",target:"_blank",rel:"noopener noreferrer",children:"X"}),(0,n.jsx)("a",{href:"https://docs.wal.app/docs/legal/privacy",target:"_blank",rel:"noopener noreferrer",children:"Privacy"}),(0,n.jsx)("a",{href:"https://docs.wal.app/docs/legal/walrus_general_tos",target:"_blank",rel:"noopener noreferrer",children:"Terms"})]})]})]})]})}let h={title:"Walrus",hide_table_of_contents:!0},m,x={},f=[];function b(r){return(0,n.jsx)(u,{})}function v(r={}){let{wrapper:o}={...(0,t.R)(),...r.components};return o?(0,n.jsx)(o,{...r,children:(0,n.jsx)(b,{...r})}):b(r)}}}]);