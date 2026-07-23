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
      ],
    },

    // ── 4. TypeScript SDK ────────────────────────────────────────
    {
      type: "category",
      label: "TypeScript SDK",
      collapsed: true,
      link: { type: "doc", id: "sdk/overview" },
      items: [
        { type: "doc", id: "sdk/quick-start", label: "TypeScript SDK Quick Start" },
        "sdk/usage/memwal",
        "sdk/usage/memwal-manual",
        "sdk/usage/with-memwal",
        "sdk/examples",
        { type: "doc", id: "sdk/ai-integration", label: "AI SDK Integration" },
        { type: "doc", id: "sdk/cookbook-multi-tenant", label: "Multi-Tenant Apps" },
        { type: "doc", id: "sdk/cloudflare-workers", label: "Cloudflare Workers" },
        "sdk/advanced-usage",
      ],
    },

    // ── 5. Python SDK ────────────────────────────────────────────
    {
      type: "category",
      label: "Python SDK",
      collapsed: true,
      items: [
        { type: "doc", id: "python-sdk/quick-start", label: "Python SDK Quick Start" },
        "python-sdk/colab",
        "python-sdk/usage/memwal",
        "python-sdk/usage/memwal-manual",
        "python-sdk/usage/with-memwal",
      ],
    },

    // ── 6. OpenClaw Plugin ───────────────────────────────────────
    {
      type: "category",
      label: "OpenClaw Plugin",
      collapsed: true,
      items: [
        { type: "doc", id: "openclaw/overview", label: "What Is OpenClaw" },
        { type: "doc", id: "openclaw/quick-start", label: "Install and Configure" },
        { type: "doc", id: "openclaw/how-it-works", label: "Architecture and Hooks" },
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
        { type: "doc", id: "sdk/api-reference", label: "TypeScript SDK API" },
        { type: "doc", id: "python-sdk/api-reference", label: "Python SDK API" },
        { type: "doc", id: "mcp/reference", label: "MCP Tools Reference" },
        { type: "doc", id: "openclaw/reference", label: "OpenClaw Plugin Reference" },
        { type: "doc", id: "relayer/api-reference", label: "Relayer HTTP API" },
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
  ],
};

export default sidebars;
