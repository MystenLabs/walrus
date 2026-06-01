// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// @ts-check

/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  walrusMemorySidebar: [
    {
      type: "category",
      label: "Getting Started",
      collapsed: false,
      link: {
        type: "doc",
        id: "getting-started/what-is-memwal",
      },
      items: [
        "getting-started/quick-start",
        "getting-started/choose-your-path",
        "examples/example-apps",
      ],
    },
    {
      type: "category",
      label: "Concepts & Architecture",
      collapsed: true,
      items: [
        "fundamentals/concepts/memory-space",
        "fundamentals/concepts/ownership-and-access",
        "fundamentals/architecture/core-components",
        "fundamentals/architecture/how-storage-works",
        "fundamentals/architecture/data-flow-security-model",
        "architecture/permanent-registry-design",
      ],
    },
    {
      type: "category",
      label: "TypeScript SDK",
      collapsed: true,
      items: [
        "sdk/overview",
        "sdk/quick-start",
        "sdk/usage/memwal",
        "sdk/usage/memwal-manual",
        "sdk/usage/with-memwal",
        "sdk/advanced-usage",
        "sdk/ai-integration",
        "sdk/examples",
        "sdk/api-reference",
        "sdk/changelog",
      ],
    },
    {
      type: "category",
      label: "Python SDK",
      collapsed: true,
      items: [
        "python-sdk/quick-start",
        "python-sdk/usage/memwal",
        "python-sdk/usage/memwal-manual",
        "python-sdk/usage/with-memwal",
        "python-sdk/api-reference",
        "python-sdk/changelog",
      ],
    },
    {
      type: "category",
      label: "Integrations",
      collapsed: true,
      items: [
        "mcp/overview",
        "mcp/quick-start",
        "mcp/how-it-works",
        "mcp/reference",
        "mcp/changelog",
        "openclaw/overview",
        "openclaw/quick-start",
        "openclaw/how-it-works",
        "openclaw/reference",
        "openclaw/changelog",
      ],
    },
    {
      type: "category",
      label: "Infrastructure",
      collapsed: true,
      items: [
        { type: "doc", id: "relayer/overview", label: "Relayer Overview" },
        { type: "doc", id: "relayer/public-relayer", label: "Relayer: Managed Service" },
        { type: "doc", id: "relayer/self-hosting", label: "Relayer: Self-Hosting" },
        { type: "doc", id: "relayer/nautilus-tee", label: "Relayer: TEE Deployment" },
        { type: "doc", id: "relayer/observability", label: "Relayer: Observability" },
        { type: "doc", id: "relayer/versioning-and-compatibility", label: "Relayer: Versioning" },
        { type: "doc", id: "relayer/api-reference", label: "Relayer: API Reference" },
        { type: "doc", id: "relayer/benchmark-ci-setup", label: "Relayer: Benchmark CI" },
        "contract/overview",
        "contract/delegate-key-management",
        "contract/ownership-and-permissions",
        "indexer/purpose",
        "indexer/onchain-events",
        "indexer/database-sync",
        "security/health-check-unsigned",
      ],
    },
    {
      type: "category",
      label: "Reference",
      collapsed: true,
      items: [
        "reference/configuration",
        "reference/environment-variables",
      ],
    },
  ],
};

export default sidebars;
