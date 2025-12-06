// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// @ts-check

//import { type } from '@generated/site-storage'

// This runs in Node.js - Don't use client-side code here (browser APIs, JSX...)

/**
* Creating a sidebar enables you to:
- create an ordered group of docs
- render a sidebar for each doc of that group
- provide next/previous navigation

The sidebars can be generated from the filesystem, or explicitly defined here.

Create as many sidebars as you want.

@type {import('@docusaurus/plugin-content-docs').SidebarsConfig}
*/

const sidebars = {
    docsSidebar: [
        "usage/started",
        {
            type: "category",
            label: "Advanced Setup",
            link: {
                type: "doc",
                id: "usage/setup",
            },
            items: ["usage/networks"],
        },
        {
            type: "category",
            label: "Interacting with Walrus",
            link: {
                type: "doc",
                id: "usage/interacting",
            },
            items: ["usage/client-cli", "usage/json-api", "usage/web-api", "usage/sdks"],
        },
        {
            type: "category",
            label: "Developer Guide",
            link: {
                type: "doc",
                id: "dev-guide/dev-guide",
            },
            items: [
                "dev-guide/components",
                "dev-guide/dev-operations",
                "dev-guide/costs",
                "dev-guide/sui-struct",
                "dev-guide/data-security",
                "usage/quilt",
            ],
        },
        {
            type: "category",
            label: "Operator Guide",
            link: {
                type: "doc",
                id: "operator-guide/operator-guide",
            },
            items: [
                {
                    type: "category",
                    label: "Operating an Aggregator or Publisher",
                    link: {
                        type: "doc",
                        id: "operator-guide/aggregator",
                    },
                    items: ["operator-guide/auth-publisher"],
                },
                {
                    type: "category",
                    label: "Operating a Storage Node",
                    link: {
                        type: "doc",
                        id: "operator-guide/storage-node",
                    },
                    items: [
                        "operator-guide/commission-governance",
                        "operator-guide/backup-restore-guide",
                    ],
                },
                "operator-guide/upload-relay",
            ],
        },
        "usage/stake",
        "usage/examples",
        "usage/troubleshooting",
        "usage/glossary",
    ],
    sitesSidebar: [
        "walrus-sites/intro",
        {
            type: "category",
            label: "Your First Walrus Site",
            link: {
                type: "doc",
                id: "walrus-sites/tutorial",
            },
            items: [
                "walrus-sites/tutorial-install",
                "walrus-sites/tutorial-publish",
                "walrus-sites/tutorial-suins",
            ],
        },
        {
            type: "category",
            label: "Advanced Functionality",
            link: {
                type: "doc",
                id: "walrus-sites/advanced",
            },
            items: [
                "walrus-sites/commands",
                "walrus-sites/builder-config",
                "walrus-sites/routing",
                "walrus-sites/linking",
                "walrus-sites/redirects",
                {
                    type: "category",
                    label: "CI/CD",
                    link: {
                        type: "doc",
                        id: "walrus-sites/ci-cd",
                    },
                    items: ["walrus-sites/ci-cd-gh-secrets-vars", "walrus-sites/ci-cd-gh-workflow"],
                },
                {
                    type: "category",
                    label: "Technical Overview",
                    link: {
                        type: "doc",
                        id: "walrus-sites/overview",
                    },
                    items: [
                        "walrus-sites/portal",
                        "walrus-sites/bring-your-own-domain",
                        "walrus-sites/authentication",
                        "walrus-sites/avoid-duplicate-content-seo",
                        "walrus-sites/restrictions",
                    ],
                },
            ],
        },
    ],
    designSidebar: [
        "design/objectives_use_cases",
        {
            type: "category",
            label: "Overview",
            link: {
                type: "doc",
                id: "design/overview",
            },
            items: ["design/architecture", "design/encoding"],
        },
        {
            type: "category",
            label: "Operations",
            link: {
                type: "doc",
                id: "design/operations",
            },
            items: ["design/operations-sui", "design/operations-off-chain"],
        },
        "design/properties",
        "design/future",
    ],
};

export default sidebars;
