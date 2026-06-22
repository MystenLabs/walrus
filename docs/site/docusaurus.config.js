// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// @ts-check
import { themes as prismThemes } from "prism-react-renderer";
import remarkGlossary from "./src/shared/plugins/remark-glossary.js";
import remarkMath from "remark-math";
import rehypeKatex from "rehype-katex";

import path from "path";
import { fileURLToPath } from "url";
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

async function tailwindPlugin() {
  return {
    name: "docusaurus-tailwindcss",
    configurePostCss(postcssOptions) {
      // Tailwind v4: PostCSS plugin is @tailwindcss/postcss
      postcssOptions.plugins.push(require("@tailwindcss/postcss"));
      postcssOptions.plugins.push(require("autoprefixer"));
      return postcssOptions;
    },
  };
}

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: "Walrus Docs",
  tagline: "Where the world's data becomes reliable, valuable, and governable",
  favicon: "img/favicon.png",
  trailingSlash: false,

  future: {
    v4: true,
    experimental_faster: {
      swcJsMinimizer: true,
    },
  },

  markdown: {
    mermaid: true,
  },

  themes: ["@docusaurus/theme-mermaid"],

  url: "https://docs.wal.app",
  baseUrl: process.env.DOCUSAURUS_BASE_URL || "/",

  headTags: [
    {
      tagName: "link",
      attributes: {
        rel: "service-doc",
        href: "/llms.txt",
        type: "text/plain",
        title: "LLM-optimized documentation",
      },
    },
    {
      tagName: "link",
      attributes: {
        rel: "service-doc",
        href: "/docs/http-api",
        title: "Walrus HTTP API Reference",
      },
    },
    {
      tagName: "link",
      attributes: {
        rel: "api-catalog",
        href: "/.well-known/api-catalog",
        type: "application/linkset+json",
      },
    },
    {
      tagName: "link",
      attributes: {
        rel: "mcp-server-card",
        href: "/.well-known/mcp/server-card.json",
        type: "application/json",
      },
    },
    {
      tagName: "link",
      attributes: {
        rel: "sitemap",
        href: "/sitemap.xml",
        type: "application/xml",
      },
    },
  ],

  clientModules: [
    "./src/client/webmcp.js",
    "./src/client/kapa-sidebar.js",
  ],

  onBrokenLinks: "throw",
  onBrokenMarkdownLinks: "throw",

  i18n: {
    defaultLocale: "en",
    locales: ["en"],
  },

  plugins: [
    function llmsTxtDirectivePlugin() {
      return {
        name: 'llms-txt-directive-plugin',
        injectHtmlTags() {
          return {
            preBodyTags: [
              {
                tagName: 'a',
                attributes: {
                  href: '/llms.txt',
                  hidden: 'hidden',
                },
                innerHTML: 'llms.txt',
              },
            ],
          };
        },
      };
    },
    "docusaurus-plugin-copy-page-button",

    // ✅ Tailwind must be here once
    tailwindPlugin,

    [
      require.resolve("./src/shared/plugins/plausible"),
      {
        domain: "docs.wal.app",
        enableInDev: false,
        trackOutboundLinks: true,
        hashMode: false,
        trackLocalhost: false,
      },
    ],
    [
      "@docusaurus/plugin-client-redirects",
      {
        fromExtensions: ["html", "htm"],
        redirects: [{ from: "/index.html", to: "/" }],
        createRedirects(existingPath) {
          if (existingPath === "/" || existingPath === "") return undefined;

          const normalized =
            existingPath.length > 1 && existingPath.endsWith("/")
              ? existingPath.slice(0, -1)
              : existingPath;

          const redirects = [];
          const addLegacy = (fromPath) => {
            redirects.push(fromPath);
            redirects.push(`${fromPath}.html`);
          };

          if (normalized.startsWith("/docs/")) {
            const newPath = normalized.replace("/docs/", "/");
            addLegacy(newPath);
          }

          return redirects.length ? redirects : undefined;
        },
      },
    ],
    function stepHeadingLoader() {
      return {
        name: "step-heading-loader",
        configureWebpack() {
          return {
            module: {
              rules: [
                {
                  test: /\.mdx?$/,
                  enforce: "pre",
                  include: [path.resolve(__dirname, "../content")],
                  use: [
                    {
                      loader: path.resolve(
                        __dirname,
                        "./src/shared/plugins/inject-code/stepLoader.js",
                      ),
                    },
                  ],
                },
              ],
            },
            resolve: {
              alias: {
                "@repo": path.resolve(__dirname, "../../"),
                "@docs": path.resolve(__dirname, "../content/"),
              },
            },
          };
        },
      };
    },
    function docsAliasPlugin() {
      return {
        name: "docs-alias-plugin",
        configureWebpack() {
          return {
            resolve: {
              alias: {
                "@docs": path.resolve(__dirname, "../content"),
              },
            },
          };
        },
      };
    },

    path.resolve(__dirname, "./src/shared/plugins/descriptions"),

    [
      "@docusaurus/plugin-content-docs",
      {
        id: "walrus-memory",
        path: "../walrus-memory-content",
        routeBasePath: "walrus-memory",
        sidebarPath: "./sidebarsWalrusMemory.js",
        remarkPlugins: [remarkMath],
        rehypePlugins: [rehypeKatex],
      },
    ],
  ],

  presets: [
  [
    "classic",
    /** @type {import('@docusaurus/preset-classic').Options} */
    ({
      docs: {
        path: "../content",
        sidebarPath: "./sidebars.js",
        editUrl: "https://github.com/MystenLabs/walrus/tree/main/docs/site/",
        remarkPlugins: [
          [remarkGlossary, { glossaryFile: "static/glossary.json" }],
          remarkMath,
        ],
        rehypePlugins: [rehypeKatex],
      },
      sitemap: {
        ignorePatterns: [
          '/docs/walrus-sites/**',
          '/docs/usage/**',
          '/docs/design/**',
          '/docs/dev-guide/**',
          '/docs/operator-guide/storage-node',
          '/docs/operator-guide/storage-node-*',
          '/docs/operator-guide/commission-governance',
          '/docs/operator-guide/backup-restore-guide',
          '/docs/operator-guide/auth-publisher',
          '/docs/operator-guide/aggregator',
          '/docs/snippets/**',
        ],
      },
      blog: {
        path: "../blog",
        postsPerPage: "ALL",
        blogSidebarTitle: "All posts",
        blogSidebarCount: "ALL",
        showReadingTime: true,
        feedOptions: {
          type: ["rss", "atom"],
          xslt: true,
        },
        onInlineTags: "warn",
        onInlineAuthors: "warn",
        onUntruncatedBlogPosts: "warn",
        remarkPlugins: [remarkMath],
        rehypePlugins: [rehypeKatex],
      },
      pages: {
        remarkPlugins: [
          [remarkGlossary, { glossaryFile: "static/glossary.json" }],
          remarkMath,
        ],
        rehypePlugins: [rehypeKatex],
      },
      theme: {
        customCss: path.resolve(__dirname, "./src/css/custom.css"),
      },
    }),
  ],
  ],

  scripts: [
    "/google-tag.js",
    "/clarity.js",
    {
      src: "https://widget.kapa.ai/kapa-widget.bundle.js",
      "data-website-id": "206d9923-4daf-4f2e-aeac-e7683daf5088",
      "data-project-name": "Walrus Knowledge",
      "data-project-color": "#37c3b0ff",
      "data-button-hide": "true",
      "data-view-mode": "sidebar",
      "data-modal-overlay-hidden": "true",
      "data-modal-lock-scroll": "false",
      "data-modal-title": "Ask Walrus AI",
      "data-modal-ask-ai-input-placeholder": "Ask me anything about Walrus!",
      "data-modal-example-questions":
        "How do I store data on Walrus?,What is a blob?,What are Walrus Sites?,How much does storage cost?",
      "data-modal-body-bg-color": "#E0E2E6",
      "data-source-link-bg-color": "#FFFFFF",
      "data-source-link-border": "#37c3b0ff",
      "data-answer-feedback-button-bg-color": "#FFFFFF",
      "data-answer-copy-button-bg-color": "#FFFFFF",
      "data-thread-clear-button-bg-color": "#FFFFFF",
      "data-modal-image": "/img/logo.svg",
      async: true,
    },
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      colorMode: {
        defaultMode: "dark",
        respectPrefersColorScheme: true,
      },
      image: "img/walrus-card.jpg",
      metadata: [
      { property: 'og:image', content: 'https://docs.wal.app/img/walrus-card.jpg' },
    ],
      navbar: {
        logo: { alt: "Walrus", src: "img/Walrus_Docs.svg" },
        items: [
          {
            type: "docSidebar",
            sidebarId: "consoleSidebar",
            position: "right",
            label: "Walrus Console",
          },
          {
            type: "docSidebar",
            sidebarId: "marketplaceSidebar",
            position: "right",
            label: "Walrus Marketplace",
          },
          {
            type: "docSidebar",
            sidebarId: "docsSidebar",
            position: "right",
            label: "Data Storage",
          },
          {
            to: "/docs/release-notes",
            label: "Release Notes",
            position: "right",
          },
          {
            href: "https://github.com/MystenLabs/walrus",
            position: "right",
            className: "header-github-link",
            "aria-label": "GitHub repository",
          },
        ],
      },
      footer: {
        style: "dark",
        copyright: `Copyright © ${new Date().getFullYear()}
                    Walrus Foundation. All rights reserved.`,
      },
      prism: {
        theme: prismThemes.github,
        darkTheme: prismThemes.dracula,
      },
      mermaid: {
        theme: {
          light: "base",
          dark: "base",
        },
        options: {
          themeVariables: {
            background: "#000000",
            primaryColor: "#613DFF",
            primaryTextColor: "#FAF8F5",
            primaryBorderColor: "#98EFDD",
            mainBkg: "#613DFF",
            secondaryColor: "#CAB1FF",
            secondaryTextColor: "#000000",
            secondBkg: "#1E1A33",
            tertiaryColor: "#98EFDD",
            tertiaryTextColor: "#000000",
            lineColor: "#98EFDD",
            signalColor: "#98EFDD",
            signalTextColor: "#98EFDD",
            noteBkgColor: "#1E1A33",
            noteTextColor: "#FAF8F5",
            noteBorderColor: "#98EFDD",
            activationBkgColor: "#CAB1FF",
            activationBorderColor: "#613DFF",
            labelBoxBkgColor: "#613DFF",
            labelBoxBorderColor: "#98EFDD",
            labelTextColor: "#FAF8F5",
            loopTextColor: "#FAF8F5",
            fontSize: "14px",
            fontFamily: "Inter, sans-serif",
          },
        },
      },
    }),

  customFields: {
    pushFeedbackId: "ilacd94goh",
    github: "MystenLabs/walrus",
    twitterX: "walrusprotocol",
    discord: "walrusprotocol",
  },
};

export default config;
