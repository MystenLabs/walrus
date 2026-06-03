> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

The Walrus Memory SDK gives your agents portable memory that works across apps, sessions, and workflows. Store, recall, and analyze context, fully under your control. It exposes three entry points:

| Entry point | Import | When to use |
| --- | --- | --- |
| `MemWal` | `@mysten-incubation/memwal` | **Recommended default** for most integrations, relayer handles embeddings, Seal, and storage |
| `MemWalManual` | `@mysten-incubation/memwal/manual` | You need client-managed embeddings and local Seal operations |
| `withMemWal` | `@mysten-incubation/memwal/ai` | You already use the Vercel AI SDK and want memory as middleware |

## Installation

```bash
$ npm install @mysten-incubation/memwal
```

```bash
$ pnpm add @mysten-incubation/memwal
```

```bash
$ yarn add @mysten-incubation/memwal
```

For `MemWalManual`, you also need the optional peer dependencies:

```bash
$ npm install @mysten/sui @mysten/seal @mysten/walrus
```

```bash
$ pnpm add @mysten/sui @mysten/seal @mysten/walrus
```

```bash
$ yarn add @mysten/sui @mysten/seal @mysten/walrus
```

For `withMemWal`, you also need:

```bash
$ npm install ai zod
```

```bash
$ pnpm add ai zod
```

```bash
$ yarn add ai zod
```

## Configuration

Before wiring the SDK into your app:

- These hosted endpoints are provided by Walrus Foundation.
- Generate a Walrus Memory account ID and delegate private key for your client using the hosted endpoint:
  - Production (Mainnet): `https://memory.walrus.xyz`
  - Staging (Testnet): `https://staging.memory.walrus.xyz`
- Choose a relayer:
  - Use the hosted relayer at `https://relayer.memory.walrus.xyz` (Mainnet) or `https://relayer-staging.memory.walrus.xyz` (Testnet)
  - Or deploy your own relayer with access to a wallet funded with WAL and SUI

`MemWal.create` takes a config object with the following fields:

| Property | Type | Required | Description |
| --- | --- | --- | --- |
| `key` | `string` | Yes | Ed25519 private key in hex |
| `accountId` | `string` | Yes | MemWalAccount object ID on Sui |
| `serverUrl` | `string` | No | Relayer URL, use `https://relayer.memory.walrus.xyz` (Mainnet) or `https://relayer-staging.memory.walrus.xyz` (Testnet) for the [managed relayer](/walrus-memory/relayer/public-relayer) |
| `namespace` | `string` | No | Default namespace, falls back to `"default"` |

## First memory

```ts
import { MemWal } from "@mysten-incubation/memwal";

const memwal = MemWal.create({
  key: "<your-ed25519-private-key>",
  accountId: "<your-memwal-account-id>",
  serverUrl: "https://your-relayer-url.com",
  namespace: "demo",
});

await memwal.health();
const job = await memwal.remember("I live in Hanoi and prefer dark mode.");
await memwal.waitForRememberJob(job.job_id);

const result = await memwal.recall({ query: "What do we know about this user?" });
console.log(result.results);
```

## Next steps

- [Usage](/walrus-memory/sdk/usage), all three clients in detail, namespace rules, and restore
- [API Reference](/walrus-memory/sdk/api-reference), full method signatures and config fields