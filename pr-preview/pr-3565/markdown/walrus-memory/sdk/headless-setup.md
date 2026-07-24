> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

A server or agent runtime has no human to click through a wallet or paste a key at a prompt. The Walrus Memory SDK initializes entirely from configuration, so you can drop it into a backend service, a cron job, or an autonomous agent. For the full write-confirm-recall loop that builds on this setup, see [Agent Storage Loop](/walrus-memory/sdk/agent-storage-loop).

## Generate credentials once

The SDK authenticates every request with an Ed25519 delegate key tied to a `MemWalAccount` object on Sui. Generate the account ID and delegate key once through the dashboard, then store them as secrets:

- **Mainnet:** [memory.walrus.xyz](https://memory.walrus.xyz)
- **Testnet:** [staging.memory.walrus.xyz](https://staging.memory.walrus.xyz)

This is the only step that involves a browser. Your runtime never opens one.

## Initialize from the environment

Load the credentials from environment variables and construct the client at startup. Call `health()` to confirm the relayer is reachable before your service starts serving traffic:

[Source: sdk/headless-setup.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/sdk/headless-setup.md)

```ts title="service.ts"
import { MemWal } from "@mysten-incubation/memwal";

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}

const memwal = MemWal.create({
  key: requireEnv("MEMWAL_PRIVATE_KEY"),
  accountId: requireEnv("MEMWAL_ACCOUNT_ID"),
  // Mainnet relayer. Use https://relayer-staging.memory.walrus.xyz for Testnet.
  serverUrl: "https://relayer.memory.walrus.xyz",
  namespace: "service-memory",
});

// Confirm the relayer is reachable at boot.
await memwal.health();
```

> **Note**
>
> `health()` is an unauthenticated liveness and version check. It confirms the relayer is reachable, but it does not validate your delegate key or account ID. A bad key or account ID surfaces on the first authenticated call, such as `remember` or `recall`. If you want to validate credentials at boot, make a cheap authenticated call, for example a `recall` with a trivial query, and handle its error.
The `MemWal.create` config takes 4 fields:

| **Property** | **Type** | **Required** | **Description** |
| --- | --- | --- | --- |
| `key` | `string` | Yes | Ed25519 delegate private key in hex |
| `accountId` | `string` | Yes | `MemWalAccount` object ID on Sui |
| `serverUrl` | `string` | No | Relayer URL for your network. Pass it explicitly so the target network is unambiguous |
| `namespace` | `string` | No | Default namespace, falls back to `"default"` |

> **Note**
>
> The SDK reads the delegate key from the `key` config field, not from any specific environment variable. The examples in these docs use both `MEMWAL_PRIVATE_KEY` and `MEMWAL_KEY` as the variable name for that value. Pick one name and use it consistently across your project.
> **Warning**
>
> Recall is scoped per **account plus namespace**. Never hardcode an account ID copied from docs or another project, and never share one delegate key across tenants that should not read each other's memories. Load every credential from the environment.
## When to use the manual client

The default `MemWal` client lets the relayer handle embedding and Seal encryption on your behalf, which is the right choice for most runtimes. Use `MemWalManual` only when the runtime must hold its own keys and keep plaintext entirely client-side. With the manual client, the runtime embeds and Seal-encrypts locally, then the relayer uploads the resulting ciphertext to Walrus and stores the vector row, so the relayer never sees plaintext. It requires a few more fields, including a Sui key that authorizes Seal:

[Source: sdk/headless-setup.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/sdk/headless-setup.md)

```ts
import { MemWalManual } from "@mysten-incubation/memwal/manual";

const manual = MemWalManual.create({
  key: requireEnv("MEMWAL_PRIVATE_KEY"),
  accountId: requireEnv("MEMWAL_ACCOUNT_ID"),
  packageId: requireEnv("MEMWAL_PACKAGE_ID"),
  serverUrl: "https://relayer.memory.walrus.xyz",
  // The Sui key authorizes Seal encryption and decryption. The relayer still
  // handles the Walrus upload, registration, search, and restore.
  suiPrivateKey: requireEnv("SUI_PRIVATE_KEY"),
  embeddingApiKey: requireEnv("OPENAI_API_KEY"),
  suiNetwork: "mainnet",
  namespace: "service-memory",
});
```

For the complete client-managed flow, see [MemWalManual](/walrus-memory/sdk/usage/memwal-manual).

## References

- [Agent Storage Loop](/walrus-memory/sdk/agent-storage-loop)
- [Walrus Memory client](/walrus-memory/sdk/usage/memwal)
- [Public relayer](/walrus-memory/relayer/public-relayer)
- [Environment Variables](/walrus-memory/reference/environment-variables)
- [API Reference](/walrus-memory/sdk/api-reference)