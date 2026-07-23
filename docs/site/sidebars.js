// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// @ts-check

/**
 * Docs sidebars — structured around 4 product areas.
 * Icons on category labels follow the Mintlify/Mem0 pattern.
 *
 * @type {import('@docusaurus/plugin-content-docs').SidebarsConfig}
 */

const sidebars = {
  // ── Walrus Console ────────────────────────────────────────────────
  consoleSidebar: [
    {
      type: "category",
      label: "Get Started",
      collapsed: false,
      link: { type: "doc", id: "console/index" },
      items: [
        "console/get-started/what-is-console",
        "console/get-started/create-account",
        "console/get-started/dashboard-tour",
        "console/get-started/first-upload",
      ],
    },
    {
      type: "category",
      label: "Files and Storage",
      collapsed: true,
      items: [
        "console/files/upload-browse",
        "console/files/manage-files",
        "console/files/epoch-renewal",
        "console/files/storage-limits",
      ],
    },
    {
      type: "category",
      label: "Walrus Memory",
      collapsed: true,
      items: [
        "console/memory/namespaces",
        "console/memory/browse-objects",
        "console/memory/epoch-renewal",
      ],
    },
    {
      type: "category",
      label: "Datasets",
      collapsed: true,
      items: [
        "console/datasets/what-are-datasets",
        "console/datasets/upload-dataset",
        "console/datasets/access-models",
        "console/datasets/list-on-marketplace",
      ],
    },
    {
      type: "category",
      label: "API and MCP",
      collapsed: true,
      items: [
        "console/api/api-keys",
        "console/api/mcp-server",
        "console/api/api-reference",
      ],
    },
    {
      type: "category",
      label: "Team Spaces",
      collapsed: true,
      items: [
        "console/teams/create-team",
        "console/teams/invite-members",
        "console/teams/shared-api-keys",
      ],
    },
    {
      type: "category",
      label: "Billing",
      collapsed: true,
      items: [
        "console/billing/free-tier",
        "console/billing/usage-pricing",
        "console/billing/plans-pricing",
      ],
    },
    {
      type: "category",
      label: "Security",
      collapsed: true,
      items: [
        "console/security/authentication",
        "console/security/security-audit",
      ],
    },
  ],

  // ── Walrus Marketplace ──────────────────────────────────────────
  marketplaceSidebar: [
    {
      type: "category",
      label: "Get Started",
      collapsed: false,
      link: { type: "doc", id: "marketplace/index" },
      items: [
        "marketplace/get-started/what-is-marketplace",
        "marketplace/get-started/browse-listings",
        "marketplace/get-started/first-purchase",
      ],
    },
    {
      type: "category",
      label: "For Publishers",
      collapsed: true,
      items: [
        "marketplace/publishers/create-listing",
        "marketplace/publishers/pricing-royalties",
        "marketplace/publishers/access-control",
        "marketplace/publishers/manage-listings",
        "marketplace/publishers/analytics",
      ],
    },
    {
      type: "category",
      label: "For Consumers",
      collapsed: true,
      items: [
        "marketplace/consumers/license-dataset",
        "marketplace/consumers/access-licensed-data",
        "marketplace/consumers/renewals",
      ],
    },
    {
      type: "category",
      label: "Build Integrations",
      collapsed: true,
      items: [
        "marketplace/build-integrations/api-reference",
        "marketplace/build-integrations/embed-widgets",
        "marketplace/build-integrations/webhooks-events",
        "marketplace/build-integrations/console-sync",
      ],
    },
  ],

  // ── Data Storage (core platform docs) ────────────────────────────
  docsSidebar: [
    {
      type: "category",
      label: "Get Started",
      collapsed: false,
      link: { type: "doc", id: "getting-started/index" },
      items: [
        "getting-started/advanced-setup",
        "system-overview/available-networks",
        "network-reference",
      ],
    },
    {
      type: "category",
      label: "Concepts",
      collapsed: true,
      link: { type: "doc", id: "system-overview/index" },
      items: [
        "system-overview/core-concepts",
        "system-overview/operations",
        "system-overview/storage-costs",
        "system-overview/quilt",
        "data-security",
      ],
    },
    {
      type: "category",
      label: "Store and Retrieve Data",
      collapsed: true,
      link: { type: "doc", id: "walrus-client/index" },
      items: [
        "walrus-client/storing-blobs",
        "walrus-client/reading-blobs",
        "walrus-client/managing-blobs",
        "large-uploads",
        "walrus-client/quilts",
      ],
    },
    {
      type: "category",
      label: "Examples and Tutorials",
      collapsed: true,
      link: { type: "doc", id: "examples/index" },
      items: [
        "examples/checkpoint-data",
        "examples/javascript",
        "examples/move",
        "examples/python",
        "examples/walrus-relay",
      ],
    },
    {
      type: "category",
      label: "Tools and SDKs",
      collapsed: true,
      items: [
        "walrus-client/walrus-cli",
        "typescript-sdk/sdks",
        {
          type: "category",
          label: "HTTP API",
          collapsed: true,
          items: [
            "http-api/storing-blobs",
            "http-api/reading-blobs",
            "http-api/quilt-http-apis",
          ],
        },
        "walrus-client/json-mode",
      ],
    },
    {
      type: "category",
      label: "Costs and Billing",
      collapsed: true,
      link: { type: "doc", id: "costs/index" },
      items: [
        "costs/storage-pricing",
        "costs/cost-calculator",
        "costs/billing-faq",
      ],
    },
    {
      type: "category",
      label: "Run Infrastructure",
      collapsed: true,
      link: { type: "doc", id: "operator-guide/index" },
      items: [
        "operator-guide/stake",
        {
          type: "category",
          label: "Storage Nodes",
          collapsed: true,
          link: {
            type: "doc",
            id: "operator-guide/storage-nodes/index",
          },
          items: [
            "operator-guide/storage-nodes/storage-node-setup",
            "operator-guide/storage-nodes/storage-node-maintenance",
            "operator-guide/storage-nodes/storage-node-migration",
            "operator-guide/storage-nodes/commission-governance",
            "operator-guide/storage-nodes/slashing",
            "operator-guide/storage-nodes/slashing-walkthrough",
            "operator-guide/storage-nodes/backup-restore-guide",
            "operator-guide/storage-nodes/storage-node-faq",
          ],
        },
        {
          type: "category",
          label: "Publishers",
          collapsed: true,
          items: [
            "operator-guide/publishers/operating-publisher",
            "operator-guide/publishers/auth-publisher",
            "operator-guide/publishers/mainnet-production-guide",
          ],
        },
        "operator-guide/aggregators/operating-aggregator",
        "operator-guide/upload-relay",
        "operator-guide/signed-binaries",
        "operator-guide/limitations",
      ],
    },
    {
      type: "category",
      label: "Protocol Reference",
      collapsed: true,
      items: [
        "system-overview/red-stuff",
        "system-overview/system-constraints",
        "system-overview/public-aggregators-and-publishers",
        "system-overview/view-system-info",
        "glossary",
      ],
    },
    {
      type: "category",
      label: "Troubleshooting",
      collapsed: true,
      link: { type: "doc", id: "troubleshooting/index" },
      items: [
        "troubleshooting/network-errors",
        "troubleshooting/error-handling",
        "troubleshooting/reading-blobs-after-upload",
      ],
    },
    "tusky-migration-guide",
  ],

  // ── Walrus Sites ───────────────────────────────────────────────
  sitesSidebar: [
    {
      type: "category",
      label: "Introduction to Walrus Sites",
      collapsed: false,
      link: { type: "doc", id: "sites/index" },
      items: [
        "sites/introduction/technical-overview",
        "sites/introduction/components",
      ],
    },
    {
      type: "category",
      label: "Getting Started",
      collapsed: true,
      items: [
        "sites/getting-started/installing-the-site-builder",
        "sites/getting-started/publishing-your-first-site",
        "sites/getting-started/using-the-site-builder",
      ],
    },
    {
      type: "category",
      label: "Site Configuration",
      collapsed: true,
      items: [
        "sites/configuration/site-configuration",
        "sites/configuration/specifying-http-headers",
        "sites/configuration/adding-metadata",
      ],
    },
    {
      type: "category",
      label: "Custom Domains",
      collapsed: true,
      items: [
        "sites/custom-domains/setting-a-suins-name",
        "sites/custom-domains/bringing-your-own-domain",
        "sites/custom-domains/dns-configuration",
      ],
    },
    {
      type: "category",
      label: "Walrus Portals",
      collapsed: true,
      link: { type: "doc", id: "sites/portals/deploy-locally" },
      items: [
        "sites/portals/deploy-locally",
        "sites/portals/mainnet-testnet",
      ],
    },
    {
      type: "category",
      label: "Linking and Navigation",
      collapsed: true,
      items: [
        "sites/linking/linking-from-walrus-sites",
        "sites/linking/linking-to-walrus-sites",
        "sites/linking/redirects",
        "sites/linking/avoiding-duplicate-content-seo",
      ],
    },
    {
      type: "category",
      label: "CI/CD and Automation",
      collapsed: true,
      items: [
        "sites/ci-cd/preparing-deployment-credentials",
        "sites/ci-cd/github-actions-workflow",
        "sites/ci-cd/other-ci-cd-platforms",
      ],
    },
    {
      type: "category",
      label: "Security and Authentication",
      collapsed: true,
      items: [
        "sites/security/site-data-authentication",
        "sites/security/access-control-options",
      ],
    },
    "sites/known-restrictions",
    "sites/troubleshooting",
  ],

};

export default sidebars;
