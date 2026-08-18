> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Add Walrus Memory to Claude Code so it recalls context and saves durable facts as you work. Install it as a **plugin** (recommended; adds automatic-memory hooks) or as **MCP-only** (just the tools).

- [x] Install Node.js 20+ with `npx` on your `PATH`; check with `node --version`.
- [x] Use a Claude Code version with plugin support if you want the plugin install; the `/plugin` command confirms support, and MCP-only works on any version with `claude mcp add`.
- [x] Have a [Walrus Memory account](/walrus-memory/fundamentals/concepts/ownership-and-access) ready. An unauthenticated memory-tool call returns sign-in instructions rather than signing you in, so ask the agent to run `memwal_login` and open the URL it returns. You can create the account during that flow at [memory.walrus.xyz](https://memory.walrus.xyz). Config files carry no keys: credentials land in `~/.memwal/credentials.json` after sign-in.

## Installation

### Add the marketplace

[Source: mcp/claude-code.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/mcp/claude-code.md)

```
        /plugin marketplace add MystenLabs/MemWal
        ```

      ### Install the plugin

[Source: mcp/claude-code.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/mcp/claude-code.md)

```
        /plugin install memwal@memwal-plugins
        ```

      ### Restart and sign in

Restart Claude Code, then ask the agent to run `memwal_login` and open the URL it returns to connect your wallet.

## What the plugin includes

| **Component** | **Plugin** | **MCP-only** |
|---|:-:|:-:|
| Walrus Memory MCP (memory tools) | ✓ | ✓ |
| Lifecycle hooks (automatic recall/save) | ✓ | ✗ |

MCP-only still saves and recalls on its own because the tools are proactive. The plugin adds hooks that reinforce the behavior and make the agent **prefer Walrus Memory over Claude Code's built-in memory**.

## Available tools

| **Tool** | **Description** |
|------|-------------|
| `memwal_remember` | Save a durable fact (preference, decision, constraint, identity). |
| `memwal_remember_bulk` | Save several distinct facts in one call. |
| `memwal_recall` | Semantic search across stored memories for relevant context. |
| `memwal_analyze` | Extract and save multiple facts from a passage of text. |
| `memwal_restore` | Rebuild the search index from Walrus (recovery). |
| `memwal_health` | Fast connectivity check. |
| `memwal_login` / `memwal_logout` | Connect or disconnect this client. |

See [Reference](/walrus-memory/mcp/reference) for full parameters.

## Lifecycle hooks (plugin only)

| **Hook** | **Event** | **What it does** |
|------|-------|--------------|
| Session start | `SessionStart` | Announces that memory is active and reminds the agent to use the `memwal_*` tools (preferring them over any built-in memory). |
| User prompt | `UserPromptSubmit` | Detects when your message references past work or states a durable fact, and reminds the agent to recall or save. |
| Post-tool | `PostToolUse` (Bash) | When a command's output looks like an error, reminds the agent to recall prior fixes and save the resolution. |

## Example workflow

**Session 1**

[Source: mcp/claude-code.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/mcp/claude-code.md)

```
You:   I prefer pnpm and always use TypeScript strict mode.
Agent: (calls memwal_remember on its own to store both preferences)
```

**Session 2: a brand-new chat**

[Source: mcp/claude-code.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/mcp/claude-code.md)

```
You:   set up a new package in this repo
Agent: (calls memwal_recall, finds your preferences)
       Scaffolding with pnpm and "strict": true, matching how you like to work.
```

## Verify

Work through these three checks in order; each one isolates a different layer.

### Server connected

    Run `/mcp` and confirm the list reports `memwal` as Connected. Expand its tools and confirm the list includes `memwal_remember_bulk` and `memwal_health`.

  ### Relayer reachable

    Ask the agent to run `memwal_health`. A healthy response returns within a few seconds; anything else points at network access to `relayer.memory.walrus.xyz`.

  ### End to end

    State a durable fact, for example a package-manager preference, confirm the agent calls `memwal_remember`, then open a brand-new session and confirm `memwal_recall` surfaces it.

## Troubleshooting FAQ

**`/mcp` reports memwal as failed or missing.**
Restart Claude Code first; MCP servers load at startup. If it still fails, run `npx -y @mysten-incubation/memwal-mcp --help` in a plain terminal: that surfaces the real error, most often a Node version below 20 or a `PATH` without `npx` in the environment Claude Code inherits. For a full trace, add `MEMWAL_MCP_DEBUG=1` to the server's environment.

**`/plugin` commands are not recognized.**
Your Claude Code version predates plugin support. Update Claude Code, or use the MCP-only install; the memory tools behave the same, you only lose the automatic-memory hooks.

**The browser sign-in cannot open (SSH, containers, headless machines).**
`memwal_login` needs a browser. Sign in once on a desktop machine, then copy `~/.memwal/credentials.json` to the same path on the headless machine.

That file holds the raw delegate private key, so treat it as a secret: transfer it over a secure channel such as `scp` rather than pasting it or sending it through chat, and restrict it on arrival.

[Source: mcp/claude-code.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/mcp/claude-code.md)

```sh
$ chmod 600 ~/.memwal/credentials.json
```

For fully headless servers, the [headless setup guide](/walrus-memory/sdk/headless-setup) covers credential-based configuration.

**Signed in with the wrong account.**
Ask the agent to run `memwal_logout`, which wipes `~/.memwal/credentials.json`, then run `memwal_login` again with the right wallet.

**The agent saves but recall returns nothing.**
Every recall runs inside one account and namespace. If you set `MEMWAL_NAMESPACE` (or `--namespace`) after saving, earlier memories live in the previous namespace. If the namespace matches and results are still missing, run `memwal_restore <namespace>` to rebuild the search index from Walrus; the stored memories are the source of truth, and you can rebuild the index at any time.

**Hooks are not firing.**
The lifecycle hooks ship only with the **plugin** install; MCP-only provides the tools without hooks. Confirm the plugin appears in `/plugin` and restart after installing.

**Tool calls fail with an authentication error after working before.**
The stored credential can lapse if you revoked its delegate key from the dashboard. Run `memwal_logout` then `memwal_login` to mint a fresh delegate key.

**Corporate proxy or restricted network.**
The server needs outbound HTTPS to `relayer.memory.walrus.xyz` (and the sign-in flow needs `memory.walrus.xyz`). If only the relayer is blocked, the HTTP transport option above fails identically; allowlist both hosts.