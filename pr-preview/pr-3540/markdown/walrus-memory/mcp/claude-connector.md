> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Claude's built-in custom connector flow adds Walrus Memory over [OAuth 2.1](https://oauth.net/2.1/). You approve access in the browser with your [Sui wallet](https://docs.sui.io/guides/developer/wallets/what-is-a-wallet), and Claude never asks you for a delegate private key or a custom header.

> **Note**
>
> The connector flow needs a relayer that has OAuth turned on. Confirm the endpoint you plan to use answers `GET /.well-known/oauth-authorization-server` before you hand the URL to someone else. A relayer without the OAuth configuration returns `404` on that route and works only with [header authentication](/walrus-memory/mcp/reference#streamable-http).
## Pick the right flow

| **Client** | **Flow** | **Where to look** |
| --- | --- | --- |
| Claude web and Claude Desktop | Custom connector over OAuth | The steps below |
| Claude Code | Header authentication | [Claude Code](/walrus-memory/mcp/claude-code) |
| Cursor, Codex, Antigravity, OpenCode | stdio MCP server | [Overview](/walrus-memory/mcp/overview) |

The OAuth path and the header path reach the same tools through the same relayer. They differ in [who holds the delegate key](#what-you-approve).

- [x] A [Sui wallet](https://docs.sui.io/guides/developer/wallets/what-is-a-wallet) in the browser you use for the consent screen.
- [x] A [Walrus Memory account](/walrus-memory/fundamentals/concepts/ownership-and-access) that the wallet owns. If the wallet owns no account yet, the consent screen sends you through the one-time setup and returns you to the connector flow.
- [x] The MCP URL of a relayer that has OAuth turned on.

| **Environment** | **Connector URL** | **OAuth status** |
| --- | --- | --- |
| Staging (Testnet) | `https://relayer-staging.memory.walrus.xyz/api/mcp` | Discovery routes serve traffic |
| Dev | `https://relayer.dev.memory.walrus.xyz/api/mcp` | Discovery routes serve traffic |
| Production (Mainnet) | `https://relayer.memory.walrus.xyz/api/mcp` | OAuth is live and the discovery routes serve traffic |

### Paste the connector URL in claude

    Open Claude's connector settings, choose **Add custom connector**, and paste the MCP URL for your environment. Claude fetches the relayer's OAuth metadata and registers itself as a client. The relayer accepts that registration only for Anthropic's own callback domain or a loopback address, so a stranger cannot register a connector that redirects your grant somewhere else.

  ### Review the consent screen

    Claude opens the Walrus Memory consent screen in your browser. Everything the screen shows comes from the relayer, not from the link you arrived on, so the values reflect what the relayer validated:

    - The name the connecting client gave for itself, which the screen labels as unverified.
    - The redirect host the relayer checked against its allowlist.
    - The scopes the client asked for.
    - The Sui address of the delegate key that the grant authorizes.

  ### Connect your wallet and authorize the delegate

    Connect the Sui wallet that owns your Walrus Memory account. Approve the `add_delegate_key` transaction, which records onchain that this delegate can act for your account. Walrus Memory sponsors the transaction through Enoki, so you pay no gas.

    If your account already has an active OAuth delegate from an earlier grant, the relayer reuses it and skips the transaction. The reuse is per account, not per client, so a second connector can end up sharing a delegate with the first.

  ### Finish in Claude

    The relayer verifies the transaction onchain and hands control back to Claude. The `memwal_*` tools appear in the session. Ask Claude what tools it has to confirm. To check that the grant reached your account, open the [Walrus Memory dashboard](https://memory.walrus.xyz) and look for a delegate at the address the consent screen showed.

## What you approve

The relayer supports three scopes, and a client requests the subset it needs. The consent screen shows what the client actually asked for, so read it there rather than assuming all three:

1. `memwal:read`, which lets the client recall memories.
2. `memwal:write`, which lets the client store memories.
3. `offline_access`, which lets a client refresh its access token without sending you back through consent.

Access tokens last 1 hour and refresh tokens last 30 days by default. A self-hosted relayer can change both. See [MCP OAuth 2.1 configuration](/walrus-memory/mcp/reference#mcp-oauth-2-1-configuration).

> **Warning**
>
> The connector flow puts a delegate private key on the server. Claude cannot hold a Sui wallet key, so the relayer generates a delegate keypair, encrypts the private key with AES-256-GCM before it stores the key, and decrypts it in memory to sign your MCP calls. The [stdio client](/walrus-memory/mcp/overview) and the header flow keep the delegate key on your own machine instead. Choose the flow whose trust boundary you accept, and use the dashboard to remove a delegate you no longer want.
## Disconnect

Disconnecting takes two steps, because removing the connector does not remove the onchain delegate.

### Remove the connector in claude

    This stops Claude from using it. Claude's documentation does not say whether removal also calls the relayer's revoke endpoint, so do not rely on removal alone to end the grant. On the relayer side, revoking a refresh token ends the whole grant and every access token the relayer issued under it, while revoking an access token ends only that token.

  ### Remove the delegate key in the dashboard

    Open [memory.walrus.xyz](https://memory.walrus.xyz), find the delegate that matches the address the consent screen showed, and remove it. Until you do, that delegate keeps its onchain authorization on your account.

> **Note**
>
> The same split applies to the [stdio client](/walrus-memory/mcp/overview): `memwal_logout` clears local credentials but leaves the onchain delegate in place. See [Logout semantics](/walrus-memory/mcp/reference#logout-semantics).
## Troubleshooting

- **Claude reports that it cannot find an authorization server**: that relayer might have no OAuth configuration. Check `GET /.well-known/oauth-authorization-server` on the host, and see [MCP OAuth 2.1 configuration](/walrus-memory/mcp/reference#mcp-oauth-2-1-configuration) for what an operator sets to enable it.
- **The consent screen rejects the link**: the session ID never arrived, or the relayer already expired it. Consent sessions last 15 minutes by default. Start the connector flow again from Claude.
- **The consent screen asks you to create an account**: the connected wallet owns no Walrus Memory account. Follow the setup link, create the account, and the app returns you to the connector flow.
- **Claude connects but the tools never appear**: restart the client. MCP clients load their tool list at startup.