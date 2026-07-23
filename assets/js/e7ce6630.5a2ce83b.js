"use strict";(globalThis.webpackChunkwalrus_docusaurus=globalThis.webpackChunkwalrus_docusaurus||[]).push([[6490],{35756(a,e,t){t.d(e,{R:()=>i,x:()=>l});var r=t(59471);let o={},n=r.createContext(o);function i(a){let e=r.useContext(n);return r.useMemo(function(){return"function"==typeof a?a(e):{...e,...a}},[e,a])}function l(a){let e;return e=a.disableParentContext?"function"==typeof a.components?a.components(o):a.components||o:i(a.components),r.createElement(n.Provider,{value:e},a.children)}},66034(a,e,t){t.r(e),t.d(e,{assets:()=>p,contentTitle:()=>m,default:()=>h,frontMatter:()=>c,metadata:()=>r,toc:()=>g});let r=JSON.parse('{"type":"mdx","permalink":"/","source":"@site/src/pages/index.mdx","title":"Walrus","frontMatter":{"title":"Walrus","hide_table_of_contents":true},"unlisted":false}');var o=t(62615),n=t(35756);t(59471);var i=t(52439);let l=`
/* \u{2500}\u{2500} Theme-aware landing palette \u{2500}\u{2500} */
/* :root supplies the light-mode values; [data-theme="dark"] overrides them. */
/* These are defined globally (the style tag only mounts on the homepage). */
:root {
  --l-bg: #ffffff;
  --l-surface: #f4f5f7;
  --l-text: #12141a;
  --l-text-secondary: #5b5f66;
  --l-border: rgba(0,0,0,0.10);
  --l-border-hover: rgba(0,0,0,0.24);
  --l-media-bg: #eceff4;
  --l-link: #6d47ff;
  --l-link-hover: #12b5a4;
}
[data-theme="dark"] {
  --l-bg: #000000;
  --l-surface: rgba(161,200,255,0.10);
  --l-text: #faf8f5;
  --l-text-secondary: #b0afac;
  --l-border: rgba(255,255,255,0.08);
  --l-border-hover: rgba(255,255,255,0.16);
  --l-media-bg: #000000;
  --l-link: #CAB1FF;
  --l-link-hover: #98EFDD;
}

/* \u{2500}\u{2500} Neutralize Docusaurus chrome (theme-aware background) \u{2500}\u{2500} */
#copy-page-button-container { display: none !important; }

#__docusaurus_skipToContent_fallback {
  background: var(--l-bg) !important; padding: 0 !important; margin: 0 !important;
}
#__docusaurus_skipToContent_fallback > main,
#__docusaurus_skipToContent_fallback > main.container,
#__docusaurus_skipToContent_fallback > main.container--fluid {
  background: var(--l-bg) !important; padding: 0 !important; margin: 0 !important; max-width: 100% !important;
}
#__docusaurus_skipToContent_fallback .margin-vert--lg { margin: 0 !important; }
#__docusaurus_skipToContent_fallback .row {
  background: var(--l-bg) !important; padding: 0 !important; margin: 0 !important;
}
#__docusaurus_skipToContent_fallback .col {
  background: var(--l-bg) !important; padding: 0 !important;
  max-width: 100% !important; flex: none !important; width: 100% !important;
}
#__docusaurus_skipToContent_fallback article {
  background: var(--l-bg) !important; padding: 0 !important; margin: 0 !important; max-width: 100% !important;
}
html, body { background: var(--l-bg) !important; }
[class*="mainWrapper"] { background: var(--l-bg) !important; padding-top: 0 !important; }
[class*="mdxPageWrapper"] { background: var(--l-bg) !important; padding: 0 !important; margin: 0 !important; }

@layer docusaurus.infima {
  #__docusaurus_skipToContent_fallback main.container {
    margin: 0 !important;
    padding: 0 !important;
    max-width: 100% !important;
  }
}

/* \u{2500}\u{2500} Landing root \u{2500}\u{2500} */
.landing-root {
  --sans: 'Google Sans Flex', -apple-system, sans-serif;
  --display: 'Ratch', 'Google Sans Flex', sans-serif;
  font-family: var(--sans);
  background: var(--l-bg);
  color: var(--l-text);
  -webkit-font-smoothing: antialiased;
  line-height: 1.6;
  min-height: 100vh;
}
.landing-root *, .landing-root *::before, .landing-root *::after { box-sizing: border-box; }
.landing-root a { color: var(--l-link); text-decoration: none; }
.landing-root a:hover { color: var(--l-link-hover); }
.landing-root,
.landing-root * {
  --ifm-background-color: var(--l-bg) !important;
  --ifm-background-surface-color: var(--l-bg) !important;
}

/* \u{2500}\u{2500} Hero \u{2500}\u{2500} */
.landing-root .hero {
  display: flex;
  flex-direction: column;
  align-items: center;
  text-align: center;
  padding: clamp(66px, 12vh, 168px) 24px clamp(44px, 7vh, 84px);
  background: var(--l-bg);
}
.landing-root .hero-title {
  font-family: var(--display);
  font-weight: 500;
  font-size: clamp(2.75rem, 9vw, 6rem);
  line-height: 0.95;
  letter-spacing: -0.02em;
  margin: 0;
  color: var(--l-text);
}
.landing-root .hero-subline {
  max-width: 672px;
  margin: 12px 0 0;
  font-family: var(--sans);
  font-weight: 400;
  font-size: clamp(1rem, 2.4vw, 1.25rem);
  line-height: 1.5;
  letter-spacing: -0.01em;
  color: var(--l-text-secondary);
}

/* \u{2500}\u{2500} Home cards \u{2500}\u{2500} */
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
  background: var(--l-surface);
  border: 1px solid var(--l-border);
  border-radius: 12px;
  overflow: hidden;
  color: inherit;
  transition: transform 0.25s ease, border-color 0.25s ease;
}
.landing-root .home-card:hover {
  color: inherit;
  transform: scale(1.02);
  border-color: var(--l-border-hover);
}
.landing-root .home-card-media {
  display: block;
  width: 100%;
  height: auto;
  background: var(--l-media-bg);
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
  color: var(--l-text);
}
.landing-root .home-card-body p {
  max-width: 364px;
  margin: 10px auto 0;
  font-family: var(--sans);
  font-weight: 400;
  font-size: 1rem;
  line-height: 1.5;
  color: var(--l-text-secondary);
}
`,s=[{image:"/img/home/walrus-memory.webp",title:"Walrus Memory",description:"Portable memory layer for AI agents that persists context across apps and sessions.",href:"/walrus-memory/getting-started/what-is-walrus-memory"},{image:"/img/home/walrus-protocol.webp",title:"Walrus Protocol",description:"Open-source decentralized storage infrastructure for building your own data layer.",href:"/docs/getting-started"},{image:"/img/home/walrus-skills.webp",title:"Walrus Skills",description:"Pre-built skills for AI coding agents that accelerate Walrus development.",href:"/skills"},{image:"/img/home/walrus-sites.webp",title:"Walrus Sites",description:"Publish highly resilient websites and frontend applications on Walrus.",href:"/docs/sites"}];function d(){return(0,o.jsxs)("div",{className:"landing-root",children:[(0,o.jsxs)(i.A,{children:[(0,o.jsx)("link",{rel:"preconnect",href:"https://fonts.googleapis.com"}),(0,o.jsx)("link",{rel:"preconnect",href:"https://fonts.gstatic.com",crossOrigin:"anonymous"}),(0,o.jsx)("link",{href:"https://fonts.googleapis.com/css2?family=Google+Sans+Flex:opsz@6..144&display=swap",rel:"stylesheet"})]}),(0,o.jsx)("style",{dangerouslySetInnerHTML:{__html:l}}),(0,o.jsxs)("section",{className:"hero",children:[(0,o.jsx)("h1",{className:"hero-title",children:"Build with Walrus"}),(0,o.jsx)("p",{className:"hero-subline",children:"Keep critical data persistent, portable, and under your control across apps, providers, and agents."})]}),(0,o.jsx)("section",{className:"home-grid",children:s.map(a=>(0,o.jsxs)("a",{className:"home-card",href:a.href,children:[(0,o.jsx)("img",{className:"home-card-media",src:a.image,alt:"",width:1110,height:451,loading:"lazy"}),(0,o.jsxs)("div",{className:"home-card-body",children:[(0,o.jsx)("h3",{children:a.title}),(0,o.jsx)("p",{children:a.description})]})]},a.title))})]})}let c={title:"Walrus",hide_table_of_contents:!0},m,p={},g=[];function u(a){return(0,o.jsx)(d,{})}function h(a={}){let{wrapper:e}={...(0,n.R)(),...a.components};return e?(0,o.jsx)(e,{...a,children:(0,o.jsx)(u,{...a})}):u(a)}}}]);