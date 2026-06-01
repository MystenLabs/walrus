> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

A managed relayer is a simpler experience for teams that want to get started without running infrastructure. If a managed relayer endpoint is available for your environment, it gives you the fastest path to integration.

## Walrus Foundation hosted endpoints

| Network | Relayer URL |
|---|---|
| **Production** (Mainnet) | `https://relayer.memwal.ai` |
| **Staging** (Testnet) | `https://relayer.staging.memwal.ai` |

## Minimal config

```ts
import { MemWal } from "@mysten-incubation/memwal";

const memwal = MemWal.create({
  key: "<your-ed25519-private-key>",
  accountId: "<your-memwal-account-id>",
  serverUrl: "https://relayer.memwal.ai",
  namespace: "demo",
});
```

## What to know

- **Shared App ID** - all users of the managed relayer share the same Walrus Memory package ID. Your data is isolated by your own `owner + namespace` (Memory Space), but the underlying deployment is shared.
- **Trust assumption** - the relayer sees plaintext during encryption and embedding. By using the managed relayer, you're trusting the Walrus Foundation-hosted instance with that data. See [Trust  and  Security Model](/walrus-memory/fundamentals/architecture/data-flow-security-model) for details.
- **Availability** - the managed relayer is a managed beta service. There are no SLA guarantees.
- **Storage costs** - the server wallet covers Walrus storage fees. Usage limits might apply during beta.

If you need full control over the trust boundary or your own dedicated instance, see [Self-Hosting](/walrus-memory/relayer/self-hosting).