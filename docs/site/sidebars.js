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
      'getting-started/index',
      'getting-started/advanced-setup',
    {
      type: 'category',
      label: 'System Overview',
      collapsed: true,
      items: [
        'system-overview/core-concepts',
        'system-overview/components',
        'system-overview/operations',
        'system-overview/storage-costs',
        'system-overview/sui-object-and-blob-ids',
        'system-overview/public-aggregators-and-publishers',
        'system-overview/view-system-info',
        'system-overview/available-networks',
        'system-overview/quilt',
      ],
    },
    {
      type: 'category',
      label: 'Walrus Client',
      collapsed: true,
      items: [
        'walrus-client/storing-blobs',
        'walrus-client/reading-blobs',
        'walrus-client/managing-blobs',
        'walrus-client/json-mode',
        'walrus-client/configuration',
        'walrus-client/logging-and-metrics',
      ],
    },
    {
      type: 'category',
      label: 'HTTP API',
      collapsed: true,
      items: [
        'http-api/storing-blobs',
        'http-api/reading-blobs',
        'http-api/quilt-http-apis',
        'http-api/managing-blobs',
      ],
    },
    "typescript-sdk/sdks",
    {
      type: 'category',
      label: 'TypeScript SDK',
      collapsed: true,
      items: [
        'typescript-sdk/installation-and-setup',
        'typescript-sdk/storing-blobs',
        'typescript-sdk/reading-blobs',
        'typescript-sdk/managing-blobs',
        'typescript-sdk/configuring-network-requests',
        'typescript-sdk/wasm-module-loading',
        'typescript-sdk/fetch-limitations',
      ],
    },
    'data-security',
    'tusky-migration-guide',
    'troubleshooting/index',
    {
      type: 'category',
      label: 'Troubleshooting',
      collapsed: true,
      items: [

        'troubleshooting/network-errors',
        'troubleshooting/error-handling',
      ],
    },
    'glossary',
    ],
      sitesSidebar: [
    {
      type: 'category',
      label: 'Introduction to Walrus Sites',
      link: {
        type: "doc",
        id:'sites/introduction/components',
      },
      collapsed: false,
      items: [
        'sites/introduction/technical-overview',
      ],
    },
    {
      type: 'category',
      label: 'Getting Started',
      collapsed: true,
      items: [
        'sites/getting-started/installing-the-site-builder',
        'sites/getting-started/using-the-site-builder',
        'sites/getting-started/publishing-your-first-site',
        'sites/getting-started/viewing-and-testing-your-site',
        'sites/getting-started/updating-an-existing-site',
        'sites/getting-started/configuration-file-reference',
      ],
    },
    {
      type: 'category',
      label: 'Walrus Portals',
      collapsed: true,
      link: {
        type: "doc",
        id: "sites/portals/deploy-locally",
      },
      items: [
        'sites/portals/deploy-locally',
        'sites/portals/mainnet-testnet',
      ],
    },
    {
      type: 'category',
      label: 'Site Configuration',
      collapsed: true,
      items: [
        'sites/configuration/specifying-http-headers',
        'sites/configuration/setting-up-routing-rules',
        'sites/configuration/adding-metadata',
      ],
    },
    {
      type: 'category',
      label: 'Custom Domains',
      collapsed: true,
      items: [
        'sites/custom-domains/setting-a-suins-name',
        'sites/custom-domains/bringing-your-own-domain',
        'sites/custom-domains/dns-configuration',
      ],
    },
    {
      type: 'category',
      label: 'Linking and Navigation',
      collapsed: true,
      items: [
        'sites/linking/linking-from-walrus-sites',
        'sites/linking/linking-to-walrus-sites',
        'sites/linking/redirects',
        'sites/linking/avoiding-duplicate-content-seo',
      ],
    },
    {
      type: 'category',
      label: 'CI/CD and Automation',
      collapsed: true,
      items: [
        'sites/ci-cd/preparing-deployment-credentials',
        'sites/ci-cd/github-actions-workflow',
        'sites/ci-cd/other-ci-cd-platforms',
      ],
    },
    {
      type: 'category',
      label: 'Security and Authentication',
      collapsed: true,
      items: [
        'sites/security/site-data-authentication',
        'sites/security/access-control-options',
      ],
    },
    'sites/known-restrictions',
    'sites/troubleshooting',
  ],
    operatorSidebar: [
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
    ],
    examplesSidebar: [
      {
        items: [
          'examples/checkpoint-data',
          'examples/javascript',
          'examples/move',
          'examples/python',
          'examples/walrus-relay'
        ],
      },
    ],
};

export default sidebars;
