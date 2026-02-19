// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// @ts-check
import { themes as prismThemes } from "prism-react-renderer";
import remarkGlossary from "./src/shared/plugins/remark-glossary.js";
import remarkMath from 'remark-math';
import rehypeKatex from 'rehype-katex';

import path from "path";
import { fileURLToPath } from "url";
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

/** @type {import('@docusaurus/types').Config} */
const config = {
    title: "Walrus Docs",
    tagline: "Where the world's data becomes reliable, valuable, and governable",
    favicon: "img/favicon.ico",
    trailingSlash: false,

    future: {
        v4: true,
        experimental_faster: {
        swcJsMinimizer: true,
    },
    },

    url: "https://docs.wal.app",
    baseUrl: process.env.DOCUSAURUS_BASE_URL || "/",

    onBrokenLinks: "throw",
    onBrokenMarkdownLinks: "throw",

    i18n: {
        defaultLocale: "en",
        locales: ["en"],
    },

    plugins: [
        "docusaurus-plugin-copy-page-button",
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

                redirects: [
                    { from: "/index.html", to: "/" },
                ],

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
                  include: [
                    path.resolve(__dirname, "../content"),
                  ],
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
        "./src/plugins/tailwind-config.js",

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
    ],

    presets: [
        [
            "classic",
            /** @type {import('@docusaurus/preset-classic').Options} */
            ({
                docs: {
                    path: "../content",
                    sidebarPath: "./sidebars.js",
                    editUrl: "https://github.com/MystenLabs/walrus/tree/main/docs/",
                    remarkPlugins: [
                        [remarkGlossary, { glossaryFile: "static/glossary.json" }],
                        remarkMath,
                    ],
                    rehypePlugins: [rehypeKatex],
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
        '/google-tag.js',
        {
      src: "https://widget.kapa.ai/kapa-widget.bundle.js",
      "data-website-id": "206d9923-4daf-4f2e-aeac-e7683daf5088",
      "data-project-name": "Walrus Knowledge",
      "data-project-color": "#37c3b0ff",
      "data-button-hide": "true",
      "data-modal-title": "Ask Walrus AI",
      "data-modal-ask-ai-input-placeholder": "Ask me anything about Walrus!",
      "data-modal-example-questions":"How do I store data on Walrus?,What is a blob?,What are Walrus Sites?,How much does storage cost?",
      "data-modal-body-bg-color": "#E0E2E6",
      "data-source-link-bg-color": "#FFFFFF",
      "data-source-link-border": "#37c3b0ff",
      "data-answer-feedback-button-bg-color": "#FFFFFF",
      "data-answer-copy-button-bg-color" : "#FFFFFF",
      "data-thread-clear-button-bg-color" : "#FFFFFF",
      "data-modal-image": "/img/logo.svg",
      async: true,
    },
    ],

    themeConfig:
        /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
        ({
            image: "img/docusaurus-social-card.jpg",
            navbar: {
                title: "Walrus Docs",
                logo: {
                    alt: "Walrus",
                    src: "img/logo.svg",
                },
                items: [
                    {
                        type: "docSidebar",
                        sidebarId: "docsSidebar",
                        position: "right",
                        label: "Data Storage",
                    },
                    {
                        type: "docSidebar",
                        sidebarId: "sitesSidebar",
                        label: "Walrus Sites",
                        position: "right",
                    },
                    {
                        type: "docSidebar",
                        sidebarId: "operatorSidebar",
                        label: "Service Providers",
                        position: "right",
                    },
                    {
                        type: "docSidebar",
                        sidebarId: "examplesSidebar",
                        label: "Example Apps",
                        position: "right",
                    },
                    { to: "/blog", label: "Blog", position: "right" },
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
                copyright:
                    `Copyright © ${new Date().getFullYear()}
                    Walrus Foundation. All rights reserved.`,
            },
            prism: {
                theme: prismThemes.github,
                darkTheme: prismThemes.dracula,
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
