> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Walrus Memory supports several integration modes depending on how much control you need. Pick the one that fits your use case.

> **Tip**
>
> These paths aren't mutually exclusive. You can combine them - for example, use the **Default SDK** with the **AI Middleware**, or start with the **Managed Relayer** and move to **Self-Hosting** later. They all share the same backend and data layer.
## 1. Default SDK

Use `@mysten-incubation/memwal` when you want the fastest working integration.

- relayer handles embedding, encryption, retrieval, and restore
- best starting point for most teams

Go to: [SDK Overview](/walrus-memory/sdk/overview)

## 2. Managed relayer

Use a hosted relayer, or deploy your own [self-hosted relayer](/walrus-memory/relayer/self-hosting) with access to a wallet funded with WAL and SUI.

> **Note**
>
> Following endpoints are provided as public good by Walrus Foundation.
| Network | Relayer URL |
| --- | --- |
| **Production** (Mainnet) | `https://relayer.memory.walrus.xyz` |
| **Staging** (Testnet) | `https://relayer-staging.memory.walrus.xyz` |

Go to: [Managed Relayer](/walrus-memory/relayer/public-relayer)

## 3. Manual client flow

Use `@mysten-incubation/memwal/manual` when you want full client-side control over encryption and embeddings. Recommended for Web3-native users who want to minimize trust in the relayer - it never sees your plaintext data.

- client handles embeddings and Seal encryption locally
- relayer only sees encrypted payloads and vectors

Go to: [SDK Usage](/walrus-memory/sdk/usage)

## 4. AI Middleware

Use `@mysten-incubation/memwal/ai` when you already use the AI SDK and want recall plus auto-save behavior.

Go to: [AI Integration](/walrus-memory/sdk/usage/with-memwal)

## 5. Self-host the relayer

Use this when you need full control over the trust boundary - your infrastructure, your credentials, no third party sees your data.

Go to: [Self-Hosting](/walrus-memory/relayer/self-hosting)

## 6. MCP Clients

Use Walrus Memory's MCP server when you want Cursor, Claude Desktop, Claude Code, Antigravity, or another MCP-aware agent to save and recall memory during tool use.

- connect directly to the hosted relayer with Streamable HTTP at `/api/mcp`
- or run the local stdio package with `npx -y @mysten-incubation/memwal-mcp`

Go to: [MCP](/walrus-memory/mcp/overview)