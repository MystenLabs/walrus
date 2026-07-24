> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Get the plugin running and test the memory loop in a few minutes.

- [x] [OpenClaw](https://openclaw.ai) `>=2026.3.11` installed and running

You also need a **delegate key**, **account ID**, and **relayer URL** from Walrus Memory, the steps below guides you through getting these.

### Install the plugin

    ```bash
    openclaw plugins install @mysten-incubation/oc-memwal
    ```

  ### Get your Walrus Memory credentials

    The plugin needs three values to connect to Walrus Memory:

    | Value | What it is |
    |-------|-----------|
    | **Delegate Key** | A private key (64-char hex) used to sign requests and encrypt memories |
    | **Account ID** | Your MemWalAccount object ID on Sui (`0x...`) |
    | **Relayer URL** | The Walrus Memory relayer endpoint that handles search, storage, and encryption |

    The easiest way to get your delegate key and account ID is through the [Walrus Memory dashboard](https://memory.walrus.xyz). See the [main Quick Start](/walrus-memory/getting-started/quick-start) for detailed setup instructions.

    For the relayer URL, use a managed endpoint or deploy your own:

    | Environment | Relayer URL |
    |-------------|-------------|
    | **Production** (Mainnet) | `https://relayer.memory.walrus.xyz` |
    | **Staging** (Testnet) | `https://relayer-staging.memory.walrus.xyz` |

    :::info
These managed relayer endpoints are provided as a public good by Walrus Foundation.
:::

  ### Set your delegate key

    Store your delegate key as an environment variable so it's never hardcoded in config files:

    ```bash
    # Add to your shell profile (.zshrc, .bashrc, and so on. )
    export MEMWAL_PRIVATE_KEY="your-64-char-hex-key"
    ```

  ### Configure OpenClaw

    Add the plugin config to `~/.openclaw/openclaw.json`:

    ```jsonc
    {
      "plugins": {
        "slots": { "memory": "oc-memwal" },
        "entries": {
          "oc-memwal": {
            "enabled": true,
            "config": {
              "privateKey": "${MEMWAL_PRIVATE_KEY}",           // References the env var
              "accountId": "0x3247e3da...",                     // Your account ID from the dashboard
              "serverUrl": "https://relayer-staging.memory.walrus.xyz"     // Or your self-hosted relayer
            }
          }
        }
      }
    }
    ```

    
Optional settings

You can add these to the `config` block to tune behavior. The defaults work well for most setups.

      | Option | Default | Description |
      |--------|---------|-------------|
      | `autoRecall` | `true` | Inject relevant memories before each turn |
      | `autoCapture` | `true` | Extract and store facts after each turn |
      | `maxRecallResults` | `5` | Max memories to inject per turn |
      | `minRelevance` | `0.3` | Relevance threshold (0-1) for memory injection |
      | `captureMaxMessages` | `10` | How many recent messages to analyze for facts |
      | `defaultNamespace` | `"default"` | Memory scope for the main agent |

  ### Start OpenClaw

    ```bash
    openclaw gateway stop && openclaw gateway
    ```

    You should see in the logs:

    ```
    oc-memwal: registered (server: https://..., key: e21d...ed9b, namespace: default)
    oc-memwal: connected (status: ok, version: ...)
    ```

    :::tip
If you see `health check failed`, check that your relayer URL is reachable and your `MEMWAL_PRIVATE_KEY` env var is set.
:::

### Check connectivity

Run the stats command to confirm the plugin is connected:

[Source: openclaw/quick-start.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/openclaw/quick-start.md)

```bash
$ openclaw memwal stats
```

This shows the relayer status, your key (masked), account ID, active namespace, and whether auto-recall/capture are enabled.

### Test the memory loop

The core value of the plugin is the automatic recall/capture cycle. Test it end-to-end:

**1. Store a fact**, start a conversation and share something memorable:

[Source: openclaw/quick-start.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/openclaw/quick-start.md)

```
You: I prefer TypeScript over JavaScript for backend work
Bot: (responds normally)
```

Check logs, you should see:
[Source: openclaw/quick-start.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/openclaw/quick-start.md)

```
oc-memwal: auto-captured 1 facts (agent: main, namespace: default)
```

**2. Recall it**, in a **new conversation**, ask about it:

[Source: openclaw/quick-start.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/openclaw/quick-start.md)

```
You: What programming languages do I like?
```

Check logs, you should see:
[Source: openclaw/quick-start.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/openclaw/quick-start.md)

```
oc-memwal: auto-recall injected 1 memories (agent: main, namespace: default)
```

**3. Search from terminal**, confirm the memory exists through CLI:

[Source: openclaw/quick-start.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/openclaw/quick-start.md)

```bash
$ openclaw memwal search "programming"
```

If all three steps work, the plugin is fully operational.

### Enable LLM tools (optional)

By default, the plugin works entirely through hooks, the LLM doesn't know about memory tools. To give the LLM explicit control, add tools to your agent profile:

[Source: openclaw/quick-start.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/openclaw/quick-start.md)

```json
{
  "tools": {
    "allow": ["memory_search", "memory_store"]
  }
}
```

Then the LLM can call `memory_search` and `memory_store` on its own when it decides to. This is a power-user feature, hooks handle the common case automatically.

## Next steps

- [How It Works](/walrus-memory/openclaw/how-it-works), understand the architecture, message flow, and hook mechanics
- [Reference](/walrus-memory/openclaw/reference), detailed breakdown of hooks, tools, CLI, and configuration