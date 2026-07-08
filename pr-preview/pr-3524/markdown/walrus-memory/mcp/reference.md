> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

This page documents every tool, flag, environment variable, and transport route the Walrus Memory MCP package exposes. For per-client setup, start with the [MCP overview](/walrus-memory/mcp/overview).

## Tools

The MCP server exposes **eight tools**: six **relayer tools** (memory operations plus a health check) and two **session tools** served locally by the stdio package. For the lifecycle hooks that drive these tools automatically, see [Claude Code](/walrus-memory/mcp/claude-code) or [Codex](/walrus-memory/mcp/codex).

## First-run behavior

When `~/.memwal/credentials.json` is missing, the stdio package does **not** exit immediately if it was launched by an MCP host.

Instead it starts in an auth-required mode that:

- responds to MCP `initialize`
- exposes the memory tools plus `memwal_login`
- returns an actionable error for memory tool calls until sign-in completes

This is why many first-run sessions show `memwal_login` before the other tools are actually usable.

### Memwal_remember

Save a durable fact to the user's Walrus Memory. The agent calls this **proactively** whenever it learns something worth remembering across sessions (preference, decision, constraint, correction, identity), not only when the user explicitly asks. Pass the full statement; do not summarize.

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `text` | string | yes | The complete fact to save. |
| `namespace` | string | no | Namespace bucket. Defaults to the session namespace. |

### Memwal_remember_bulk

Save several durable facts in one batched call. Prefer this over repeated `memwal_remember` calls when the agent learns multiple distinct facts at once.

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `facts` | string[] (1–20) | yes | Array of complete fact statements, one full fact per entry, no summarizing. |
| `namespace` | string | no | Namespace bucket applied to every fact. Defaults to the session namespace. |

### Memwal_recall

Search the user's Walrus Memory for facts relevant to a query. The agent calls this **proactively** at the start of a task or when the user references past work, decisions, or preferences. Returns matches ranked by relevance.

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `query` | string | yes | Natural-language query to match against stored memories. |
| `limit` | integer (1–100) | no | Max memories to return. Default `10`. |
| `namespace` | string | no | Namespace bucket to search. |

### Memwal_analyze

Extract memorable facts from a longer passage of text (preferences, habits, biographical info, constraints) and save each as a separate memory.

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `text` | string | yes | Conversation transcript, note, or arbitrary text to extract from. |
| `namespace` | string | no | Namespace for the extracted facts. |

### Memwal_restore

Re-index a namespace from Walrus blobs back into the relayer's search index. Returns counts only (`restored` / `skipped` / `total`), does **not** return memory texts. Call `memwal_recall` afterwards to query the rebuilt index.

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `namespace` | string | yes | Namespace bucket to restore. |
| `limit` | integer (1–500) | no | Max memories to re-index. Default `10`. |

### Memwal_health

Lightweight connectivity check. Calls the relayer's public `/health` endpoint (no request signing, no search or decryption) and returns its `status` and `version`. Use this to confirm the server is reachable instead of `memwal_recall`, which is a full retrieval round-trip. Takes no parameters.

### Memwal_login

Open a browser to sign in (or re-sign in) with your Sui wallet. Use to switch wallets, refresh credentials, or sign in for the first time inline. Takes no parameters.

Returns a one-time URL valid for **5 minutes**. If it expires, call the tool again to mint a fresh URL.

### Memwal_logout

Remove the saved credentials from this machine (`~/.memwal/credentials.json`). Takes no parameters.

> **Warning**
>
> The onchain delegate key registration is **not** revoked by `memwal_logout`, only the local file is wiped. Visit the [Walrus Memory dashboard](https://memory.walrus.xyz) to remove the delegate key from your account.
> **Note**
>
> Both session tools (`memwal_login`, `memwal_logout`) are intercepted locally by the stdio package and never reach the relayer. They read and write files on the client machine only.
## CLI

The stdio package accepts CLI flags and environment variables. **CLI takes precedence** when both are set.

| CLI flag | Environment variable | Description |
| --- | --- | --- |
| `--relayer <url>` | `MEMWAL_SERVER_URL` | Override the relayer base URL. |
| `--web-url <url>` | `MEMWAL_WEB_URL` | Override the dashboard URL used during login. |
| `--label <text>` | `MEMWAL_CLIENT_LABEL` | Friendly delegate-key label shown in the Walrus Memory dashboard. |
| `--namespace <name>` (alias `--ns`) | `MEMWAL_NAMESPACE` | Default memory namespace injected into memory tool calls that omit one. See [Default namespace](#default-namespace). |
| `--login` (or `login` subcommand) | | Force a re-login even when credentials exist. |
| `--logout` | | Wipe `~/.memwal/credentials.json` and exit. |
| `--help`, `-h` | | Print usage and exit. |

Set `MEMWAL_MCP_DEBUG=1` to enable verbose stderr logging.

## Default namespace

Set a default memory namespace once in your client config instead of having
the agent pass `namespace` on every call. The package injects it into
`memwal_remember`, `memwal_recall`, `memwal_analyze`, and `memwal_restore`
calls that don't already carry one.

Precedence, highest first:

1. **Explicit per-call `namespace`**, a non-empty `namespace` in the tool
   call is used as-is. The configured default never overrides it.
2. **`--namespace` CLI flag** (alias `--ns`).
3. **`MEMWAL_NAMESPACE` environment variable**.
4. **Unset**, the call is forwarded without a `namespace` and the relayer
   applies its own `"default"` namespace.

CLI wins over the environment variable when both are set, matching every other
flag in the table above.

> **Note**
>
> `memwal_restore` still lists `namespace` as **required** in its tool schema,
> so agents normally pass one explicitly. A configured default is only used as a
> fallback if the agent calls `memwal_restore` without a namespace.
Example, pin every memory call to a `work` namespace:

```json
{
  "mcpServers": {
    "memwal": {
      "command": "npx",
      "args": ["-y", "@mysten-incubation/memwal-mcp", "--namespace", "work"]
    }
  }
}
```

## Credential file

The stdio package stores credentials at:

```text
~/.memwal/credentials.json
```

The file includes:

- delegate private key
- delegate public key
- delegate address
- wallet address
- account ID
- package ID
- relayer URL
- label
- creation timestamp

The file is written with restrictive permissions (`0600`) on supported systems.

> **Warning**
>
> Treat the delegate private key in this file like an API key. Anyone who gets it can act as this MCP client until the delegate is revoked.
## Client config paths

Common local config locations:

- **Cursor**: `~/.cursor/mcp.json`
- **Claude Desktop (macOS)**: `~/Library/Application Support/Claude/claude_desktop_config.json`
- **Codex**: `~/.codex/config.toml`
- **Claude Code**: stored through the `claude mcp add` registry

### Environment presets

Shortcut flags that set both the relayer and the dashboard URL in one switch:

| Flag | Relayer | Dashboard |
| --- | --- | --- |
| `--prod` | `https://relayer.memory.walrus.xyz` | `https://memory.walrus.xyz` |
| `--staging` | `https://relayer-staging.memory.walrus.xyz` | `https://staging.memory.walrus.xyz` |
| `--local` | `http://127.0.0.1:8000` | `http://localhost:5173` |

Explicit `--relayer` and `--web-url` override the preset. You can also pass either flag without a preset to point at a custom URL.

## Transports

Walrus Memory supports two MCP connection modes.

| Mode | Best for | Configured through |
| --- | --- | --- |
| **stdio package** | Clients that run local MCP commands (most clients today) | `npx -y @mysten-incubation/memwal-mcp` in the client config |
| **Streamable HTTP** | Clients that support remote HTTP MCP servers | `url: "https://relayer.memory.walrus.xyz/api/mcp"` + auth headers |

### Streamable HTTP

Use HTTP transport when your client supports remote MCP servers natively. Authentication is bearer-token + account ID per request:

```json
{
  "mcpServers": {
    "memwal": {
      "url": "https://relayer.memory.walrus.xyz/api/mcp",
      "headers": {
        "Authorization": "Bearer <YOUR_DELEGATE_PRIVATE_KEY>",
        "x-memwal-account-id": "<YOUR_ACCOUNT_ID>"
      }
    }
  }
}
```

The bearer token is the `delegatePrivateKey` from `~/.memwal/credentials.json`. The account ID is the `accountId` field in that same file. Run `npx -y @mysten-incubation/memwal-mcp login --prod` once to populate it.

> **Warning**
>
> The bearer token is a long-lived credential equivalent to an API key. **Never commit MCP configs with a real `Authorization` header to source control.** Treat it like any other secret.
For Claude Code, the equivalent registration command is:

```bash
$ claude mcp add --transport http memwal https://relayer.memory.walrus.xyz/api/mcp
```

If your client cannot attach headers from the CLI, edit the generated MCP config file to add them manually.

### When to prefer HTTP vs stdio

Prefer **stdio** when:

- the MCP host already supports local `command + args`
- you want inline `memwal_login` UX
- you do not want to paste long-lived bearer credentials into client config

Prefer **Streamable HTTP** when:

- the MCP host supports remote MCP servers and request headers cleanly
- you are wiring a shared hosted endpoint instead of a local package
- you intentionally want a config based on explicit bearer credentials

### Public routes

The hosted relayer (and any self-hosted relayer) exposes the same MCP routes:

| Route | Purpose |
| --- | --- |
| `GET /api/mcp/sse` | Legacy SSE session for the stdio bridge |
| `POST /api/mcp/messages` | JSON-RPC messages for the legacy SSE transport |
| `GET /api/mcp` | Streamable HTTP server-to-client stream |
| `POST /api/mcp` | Streamable HTTP JSON-RPC messages |
| `DELETE /api/mcp` | Close a Streamable HTTP session |

The Rust relayer auto-starts a TypeScript sidecar and forwards MCP traffic to it over loopback. The sidecar resolves MCP bearer credentials into normal Walrus Memory SDK sessions, so MCP tool calls go through the **same Seal, Walrus, and pgvector paths** as direct SDK calls.

### 401 behavior

If the relayer returns `401 Unauthorized`, the package surfaces a clear error but does **not** auto-delete `~/.memwal/credentials.json`.

That is intentional. A `401` can mean a revoked delegate key, but it can also come from a transient edge/proxy/network issue. Leaving the file untouched avoids turning a temporary failure into forced re-auth.

### `--relayer` override behavior

If a saved credentials file already points at one relayer and the current process is launched with a different `--relayer`, the override applies to the **current process only**.

The saved file is not silently rewritten. To rotate the saved relayer permanently, sign out and log in again on the target environment.

## Self-hosting

Self-hosted relayers expose the same public MCP routes as the hosted relayer. The most common operator-tunable settings:

| Variable | Default | Purpose |
| --- | --- | --- |
| `SIDECAR_URL` | `http://localhost:9000` | Loopback endpoint the Rust relayer uses to reach the sidecar |
| `MCP_MAX_TOTAL_SESSIONS` | `1000` | Cap on concurrent MCP sessions across SSE and Streamable HTTP |
| `MCP_MAX_SESSIONS_PER_IP` | `16` | Cap on concurrent sessions from one source IP |
| `MCP_MAX_NEW_SESSIONS_PER_IP_PER_MIN` | `30` | Rate cap on new sessions per source IP per minute |

See [Environment Variables](/walrus-memory/reference/environment-variables) for the full list including Seal, Walrus, embeddings, and database settings.

## Logout semantics

`memwal_logout` and `--logout` only delete local credentials from this machine.

They do **not**:

- revoke the onchain delegate key
- remove the delegate from the Walrus Memory dashboard

If the delegate itself should stop working, revoke it from the dashboard too.

### Tools aren't visible to the agent

Quit and relaunch your MCP client, MCP servers only load at startup. If you used `claude mcp add`, run `claude mcp list` to confirm `memwal` is registered before restarting Claude Code.

### Only `memwal_login` shows up

Credentials are missing. Ask the agent to call `memwal_login`, or run `npx -y @mysten-incubation/memwal-mcp login --prod` from your terminal.

### `memwal_login` URL expires before approval

The URL is valid for **5 minutes**. Call the tool again to mint a fresh one. Make sure your browser is logged into the wallet you intend to use before clicking.

### Recall returns "no matching memories found" right after a remember

`memwal_remember` waits for the Walrus upload to finish before returning, but under load the embedding/indexing step can lag a few seconds behind. Wait briefly, then retry the recall.

### 401 unauthorized from the relayer

Your delegate key was revoked from the dashboard, or your saved credentials point at the wrong environment. Run `--logout` then `login --<env>` for the env you want.

### Verbose logs for debugging

Set `MEMWAL_MCP_DEBUG=1` in the client's MCP server config `env` block (or in your terminal) to dump structured stderr logs covering credential loading, bridge connection, and per-request flow.