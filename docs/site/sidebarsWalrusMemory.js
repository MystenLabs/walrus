// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0
//
// Walrus Memory sidebar — hardcoded for optimal IA ordering.
// Content is fetched from upstream MemWal repo at build time;
// this file controls the navigation structure independently.
//
// Ordering follows Mem0-style best practices:
//   Get Started → Concepts → AI Tools (MCP) → SDKs → Plugins →
//   Infrastructure → Reference → Contributing

// @ts-check

/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  walrusMemorySidebar: [
    { type: "doc", id: "index", label: "Walrus Memory" },

    // ── 1. Get Started ───────────────────────────────────────────
    {
      type: "category",
      label: "Get Started",
      collapsed: false,
      link: {
        type: "doc",
        id: "getting-started/what-is-walrus-memory",
      },
      items: [
        "getting-started/quick-start",
        "getting-started/choose-your-path",
        "examples/example-apps",
      ],
    },

    // ── 2. Core Concepts ─────────────────────────────────────────
    {
      type: "category",
      label: "Core Concepts",
      collapsed: true,
      items: [
        "fundamentals/concepts/memory-space",
        "fundamentals/concepts/ownership-and-access",
        "fundamentals/architecture/core-components",
        "fundamentals/architecture/how-storage-works",
        "fundamentals/architecture/data-flow-security-model",
      ],
    },

    // ── 3. AI Tools (MCP) — promoted for AI developer audience ──
    {
      type: "category",
      label: "AI Tools (MCP)",
      collapsed: true,
      link: { type: "doc", id: "mcp/overview" },
      items: [
        "mcp/claude-code",
        "mcp/claude-desktop",
        "mcp/cursor",
        "mcp/codex",
        "mcp/opencode",
        "mcp/antigravity",
        "mcp/reference",
      ],
    },

    // ── 4. TypeScript SDK ────────────────────────────────────────
    {
      type: "category",
      label: "TypeScript SDK",
      collapsed: true,
      link: { type: "doc", id: "sdk/overview" },
      items: [
        "sdk/quick-start",
        {
          type: "category",
          label: "Usage",
          collapsed: true,
          link: { type: "doc", id: "sdk/usage" },
          items: [
            "sdk/usage/memwal",
            "sdk/usage/memwal-manual",
            "sdk/usage/with-memwal",
          ],
        },
        "sdk/ai-integration",
        "sdk/examples",
        "sdk/cookbook-multi-tenant",
        "sdk/cloudflare-workers",
        "sdk/advanced-usage",
        "sdk/research-app-example",
        "sdk/example-map",
        "sdk/api-reference",
      ],
    },

    // ── 5. Python SDK ────────────────────────────────────────────
    {
      type: "category",
      label: "Python SDK",
      collapsed: true,
      items: [
        "python-sdk/quick-start",
        "python-sdk/colab",
        {
          type: "category",
          label: "Usage",
          collapsed: true,
          link: { type: "doc", id: "python-sdk/usage" },
          items: [
            "python-sdk/usage/memwal",
            "python-sdk/usage/memwal-manual",
            "python-sdk/usage/with-memwal",
          ],
        },
        "python-sdk/api-reference",
      ],
    },

    // ── 6. OpenClaw Plugin ───────────────────────────────────────
    {
      type: "category",
      label: "OpenClaw Plugin",
      collapsed: true,
      items: [
        "openclaw/overview",
        "openclaw/quick-start",
        "openclaw/how-it-works",
        "openclaw/reference",
      ],
    },

    // ── 7. Relayer (infrastructure) ──────────────────────────────
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
        "relayer/runbook-gas-pool",
      ],
    },

    // ── 8. Reference (collapsed, bottom) ─────────────────────────
    {
      type: "category",
      label: "Reference",
      collapsed: true,
      items: [
        "reference/configuration",
        "reference/environment-variables",
        "security/health-check-unsigned",
        {
          type: "category",
          label: "Smart Contract",
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
        "architecture/permanent-registry-design",
      ],
    },

    // ── 9. Contributing (very bottom) ────────────────────────────
    {
      type: "category",
      label: "Contributing",
      collapsed: true,
      items: [
        "contributing/docs-workflow",
        "contributing/run-docs-locally",
        "contributing/run-repo-locally",
      ],
    },
  ],
};

export default sidebars;
