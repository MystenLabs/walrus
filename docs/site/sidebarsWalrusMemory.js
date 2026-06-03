// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// @ts-check

/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  walrusMemorySidebar: [
    {
      type: "doc",
      id: "index",
      label: "Walrus Memory",
    },
    {
      type: "category",
      label: "Get Started",
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
      label: "Fundamentals",
      collapsed: true,
      items: [
        {
          type: "category",
          label: "Concepts",
          collapsed: true,
          items: [
            "fundamentals/concepts/memory-space",
            "fundamentals/concepts/ownership-and-access",
          ],
        },
        {
          type: "category",
          label: "Architecture",
          collapsed: true,
          items: [
            "fundamentals/architecture/core-components",
            "fundamentals/architecture/how-storage-works",
            "fundamentals/architecture/data-flow-security-model",
            "architecture/permanent-registry-design",
          ],
        },
      ],
    },
    {
      type: "category",
      label: "TypeScript SDK",
      collapsed: true,
      items: [
        "sdk/overview",
        "sdk/quick-start",
        {
          type: "category",
          label: "Usage",
          collapsed: true,
          items: [
            "sdk/usage/memwal",
            { type: "doc", id: "sdk/usage/memwal-manual", label: "`MemWalManual`" },
            { type: "doc", id: "sdk/usage/with-memwal", label: "`withMemWal`" },
          ],
        },
        { type: "doc", id: "sdk/ai-integration", label: "`@ai-sdk` Integration" },
        "sdk/examples",
      ],
    },
    {
      type: "category",
      label: "Python SDK",
      collapsed: true,
      items: [
        "python-sdk/quick-start",
        {
          type: "category",
          label: "Usage",
          collapsed: true,
          items: [
            "python-sdk/usage/memwal",
            { type: "doc", id: "python-sdk/usage/memwal-manual", label: "`MemWalManual`" },
            { type: "doc", id: "python-sdk/usage/with-memwal", label: "`withMemWal`" },
          ],
        },
      ],
    },
    {
      type: "category",
      label: "Relayer",
      collapsed: true,
      items: [
        "relayer/overview",
        "relayer/public-relayer",
        "relayer/self-hosting",
        "relayer/nautilus-tee",
        "relayer/observability",
        "relayer/versioning-and-compatibility",
        "relayer/api-reference",
        "relayer/benchmark-ci-setup",
      ],
    },
    {
      type: "category",
      label: "MCP",
      collapsed: true,
      items: [
        "mcp/overview",
        "mcp/quick-start",
        "mcp/how-it-works",
        "mcp/reference",
      ],
    },
    {
      type: "category",
      label: "Contract",
      collapsed: true,
      items: [
        "contract/overview",
        "contract/delegate-key-management",
        "contract/ownership-and-permissions",
      ],
    },
    {
      type: "category",
      label: "Indexer",
      collapsed: true,
      items: [
        "indexer/purpose",
        "indexer/onchain-events",
        "indexer/database-sync",
      ],
    },
    {
      type: "category",
      label: "NemoClaw/OpenClaw Plugin",
      collapsed: true,
      items: [
        "openclaw/overview",
        "openclaw/quick-start",
        "openclaw/how-it-works",
        "openclaw/reference",
      ],
    },
    {
      type: "category",
      label: "Reference",
      collapsed: true,
      items: [
        { type: "doc", id: "sdk/api-reference", label: "TypeScript SDK API Reference" },
        { type: "doc", id: "python-sdk/api-reference", label: "Python SDK API Reference" },
        "reference/configuration",
        "reference/environment-variables",
      ],
    },
  ],
};

export default sidebars;
