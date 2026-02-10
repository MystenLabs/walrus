// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// @ts-check
// `@type` JSDoc annotations allow editor autocompletion and type checking
// (when paired with `@ts-check`).
// There are various equivalent ways to declare your Docusaurus config.
// See: https://docusaurus.io/docs/api/docusaurus-config

import { themes as prismThemes } from "prism-react-renderer";
import remarkGlossary from "./src/shared/plugins/remark-glossary.js";

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

    // Future flags, see https://docusaurus.io/docs/api/docusaurus-config#future
    future: {
        v4: true, // Improve compatibility with the upcoming Docusaurus v4
        experimental_faster: {
        swcJsMinimizer: true,
    },
    },

    // Set the production url of your site here
    url: "https://docs.wal.app",
    // Set the /<baseUrl>/ pathname under which your site is served
    // For GitHub pages deployment, it is often '/<projectName>/'
    baseUrl: process.env.DOCUSAURUS_BASE_URL || "/",

    // GitHub pages deployment config.
    // If you aren't using GitHub pages, you don't need these.
    // organizationName: 'Mysten Labs',
    // projectName: 'Walrus',

    onBrokenLinks: "throw",
    onBrokenMarkdownLinks: "throw",

    // Even if you don't use internationalization, you can use this field to set
    // useful metadata like html lang. For example, if your site is Chinese, you
    // may want to replace "en" with "zh-Hans".
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
                // Automatically redirect /foo.html -> /foo
                fromExtensions: ["html", "htm"],

                redirects: [
                    // explicit homepage legacy
                    { from: "/index.html", to: "/" },
                ],

                createRedirects(existingPath) {
                    if (existingPath === "/" || existingPath === "") return undefined;

                    // normalize (remove trailing slash except root)
                    const normalized =
                        existingPath.length > 1 && existingPath.endsWith("/")
                            ? existingPath.slice(0, -1)
                            : existingPath;

                    const redirects = [];

                    const addLegacy = (fromPath) => {
                        redirects.push(fromPath);
                        redirects.push(`${fromPath}.html`);
                    };

                    // OLD prefix → NEW prefix (this fixes /usage/setup.html#... etc.)
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
                  test: /\.mdx?$/, // run on .md and .mdx
                  enforce: "pre", // make sure it runs BEFORE @docusaurus/mdx-loader
                  include: [
                    // adjust these to match where your Markdown lives
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
                    // Please change this to your repo.
                    // Remove this to remove the "edit this page" links.
                    editUrl: "https://github.com/MystenLabs/walrus/tree/main/docs/",
                    remarkPlugins: [[remarkGlossary, { glossaryFile: "static/glossary.json" }]],
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
                    // Remove this to remove the "edit this page" links.
                    // editUrl: "https://github.com/MystenLabs/walrus/tree/main/docs",
                    // Useful options to enforce blogging best practices
                    onInlineTags: "warn",
                    onInlineAuthors: "warn",
                    onUntruncatedBlogPosts: "warn",
                },
                pages: {
                    remarkPlugins: [[remarkGlossary, { glossaryFile: "static/glossary.json" }]],
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
            // Replace with your project's social card
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
