// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// @ts-check
// `@type` JSDoc annotations allow editor autocompletion and type checking
// (when paired with `@ts-check`).
// There are various equivalent ways to declare your Docusaurus config.
// See: https://docusaurus.io/docs/api/docusaurus-config

import { themes as prismThemes } from "prism-react-renderer";
import remarkGlossary from "./src/plugins/remark-glossary.js";

import path from "path";
import { fileURLToPath } from "url";
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

/** @type {import('@docusaurus/types').Config} */
const config = {
    title: "Walrus Docs",
    tagline: "Where the world’s data becomes reliable, valuable, and governable",
    favicon: "img/favicon.ico",

    // Future flags, see https://docusaurus.io/docs/api/docusaurus-config#future
    future: {
        v4: true, // Improve compatibility with the upcoming Docusaurus v4
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
    onBrokenMarkdownLinks: "warn",

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
      '@docusaurus/plugin-client-redirects',
      {
        fromExtensions: ['html', 'htm'], // /myPage.html -> /myPage
        redirects: [
          {
            to: "/docs/design/:path*",
            from: "/design/:path*",
          },
          {
            to: "/docs/usage/:path*",
            from: "/usage/:path*",
          },
          {
            to: "/docs/dev-guide/:path*",
            from: "/dev-guide/:path*",
          },
          {
            to: "/docs/legal/:path*",
            from: "/legal/:path*",
          },
          {
            to: "/docs/operator-guide/:path*",
            from: "/operator-guide/:path*",
          },
          {
            to: "/docs/walrus-sites/:path*",
            from: "/walrus-sites/:path*",
          },
          {
            to: "/docs/design/encoding",
            from: "/design/encoding.html",
          },
          {
            to: "/docs/usage/client-cli",
            from: "/usage/client-cli.html",
          },
          {
            to: "/docs/usage/interacting",
            from: "/usage/interacting.html",
          },
          {
            to: "/docs/usage/quilt",
            from: "/usage/quilt.html",
          },
          {
            to: "/docs/usage/setup",
            from: "/usage/setup.html",
          },
          {
            to: "/docs/usage/web-api",
            from: "/usage/web-api.html",
          },
          {
            to: "/docs/walrus-sites/bring-your-own-domain",
            from: "/walrus-sites/bring-your-own-domain.html",
          },
          {
            to: "/docs/walrus-sites/intro",
            from: "/walrus-sites/intro.html",
          },
          {
            to: "/docs/walrus-sites/portal",
            from: "/walrus-sites/portal.html",
          },
        ],
      },
    ],
    [
      require.resolve("./src/plugins/plausible"),
      {
        domain: "docs.wal.app",
        enableInDev: false,
        trackOutboundLinks: true,
        hashMode: false,
        trackLocalhost: false,
      },
    ],
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
        path.resolve(__dirname, `./src/plugins/askcookbook/index.js`),
        path.resolve(__dirname, `./src/plugins/descriptions`),
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
                        label: "Docs",
                    },
                    {
                        type: "docSidebar",
                        sidebarId: "sitesSidebar",
                        label: "Sites",
                        position: "right",
                    },
                    {
                        type: "docSidebar",
                        sidebarId: "designSidebar",
                        label: "Design",
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
