> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Walrus Memory writes your encrypted memories to Walrus as blobs, as described in [How Storage Works](/walrus-memory/fundamentals/architecture/how-storage-works). Someone has to pay for that storage. This page explains who, and the trade-offs between an agent holding WAL itself and having a third party sponsor the cost.

> **Note**
>
> In a typical Walrus Memory setup, the relayer operates the Walrus write path for you, so most agents never touch WAL directly. Whether the storage is sponsored or self-funded then depends on how the relayer is deployed: a [public relayer](/walrus-memory/relayer/public-relayer) abstracts funding away from the agent, while [self-hosting](/walrus-memory/relayer/self-hosting) means you fund the writes. The models below explain what sits underneath either choice.
## What a write costs: 2 tokens, not 1

A Walrus write consumes 2 different tokens, and any funding design accounts for both:

- **WAL:** Pays for storage. You pay per encoded storage unit per epoch, plus a one-time write fee for each blob. Walrus prices storage at a fixed fiat rate (about 0.023 USD per GB per month) and adjusts the WAL amount automatically as the WAL price changes. The smallest unit of WAL is FROST, where 1 WAL equals 1 billion FROST.
- **SUI:** Pays gas for the Sui transactions that coordinate the write, such as `reserve_space`, `register_blob`, and `certify_blob`, plus any later `extend`, `delete`, or `burn`. The smallest unit of SUI is MIST, where 1 SUI equals 1 billion MIST.

WAL cost scales with blob size and the number of epochs. SUI gas stays roughly fixed for each transaction. A typical store runs as 2 transactions: one for `reserve_space` and `register_blob`, which changes both your SUI and WAL balances, and one for `certify_blob`, which changes only your SUI balance.

> **Note**
>
> The funding question is really 2 questions: who supplies the WAL, and who supplies the SUI gas. Different sponsored mechanisms cover different halves, so be explicit about which part a given mechanism handles.
A successful write also produces a `Blob` object on Sui. Whoever owns that object controls the blob lifecycle, such as extending its lifetime, deleting it, adding attributes, or burning it to reclaim Sui storage. Ownership matters as much as payment, so track it deliberately, especially in sponsored flows.

## Model a: the agent holds WAL directly

The agent has its own Sui address (key pair) that holds both SUI and WAL, and it signs and pays for its own writes. This is the default model for a long-lived, autonomous agent that owns and manages its own data.

Store through the CLI:

```bash
$ walrus store memory.json --epochs 10
```

Store through the TypeScript SDK, where the agent's key pair is the `signer` that pays both gas and storage fees:

```ts
await client.walrus.writeFiles({
  files: [file],
  epochs: 10,
  deletable: true,
  signer: keypair, // must hold enough SUI for gas and enough WAL for storage and the write fee
});
```

The agent's address needs WAL before it can write. The most basic path swaps SUI for WAL onchain:

```bash
$ walrus get-wal --amount AMOUNT_IN_FROST
```

You can also receive WAL through a transfer from another wallet, a bridge, or an exchange. On Testnet, WAL has no value and you obtain it 1:1 from Testnet SUI, which makes this model easy to develop against before any real funding exists.

This model gives the agent full autonomy, because no external service sits in the write path, and the agent owns the resulting `Blob` object outright, so it can extend, delete, or burn it at any time. In exchange, the agent takes on 2 responsibilities:

- Keep both balances funded. Running out of either SUI or WAL stalls writes.
- Manage the blob lifecycle. Blobs expire after the paid number of epochs (an epoch is about 2 weeks on Mainnet and about 1 day on Testnet, and you can buy up to about 2 years in advance), so the agent extends them before expiry or the network drops the data.

## Model b: sponsored storage

Sponsored storage is not a single feature. Several distinct patterns exist, and they sponsor different parts of the cost. Choose based on how much you want the agent to hold and own.

### Publisher: sponsor pays WAL and SUI

A publisher is an HTTP service that accepts raw bytes and runs the entire write flow for the caller. Its sub-wallets hold SUI and WAL and pay for everything, so the agent needs no tokens at all and only sends the data. A Walrus Memory relayer plays this role for your memories.

- This is the most complete form of sponsorship, because the sponsoring application carries 100% of the WAL and SUI cost.
- By default, the publisher's sub-wallet owns the resulting `Blob` object. To let the agent keep ownership, set the publisher's `send-object-to` parameter to the agent's Sui address so the publisher transfers the blob after upload.

> **Warning**
>
> Mainnet has no public, unauthenticated publisher, because each upload spends the operator's real WAL and SUI. In production, this is a private, authenticated publisher that the sponsoring application runs and gates for its own agents.
Use a publisher when the agent runs in a constrained or untrusted environment, or when you do not want the agent to custody tokens.

### Sui Sponsored transactions: sponsor pays SUI gas only

Sui natively supports sponsored transactions, where a sponsor account provides the gas coin for a transaction the agent signs. This covers only the SUI side. The WAL storage fee still comes from somewhere else, either the agent's own WAL or a publisher.

Use sponsored transactions when the agent can hold WAL but you do not want it to manage SUI gas, for example when a separate service handles all gas.

### Pre-funding: transfer WAL or storage resources

Because WAL is a coin and storage resources are transferable Sui objects, a sponsor can fund an agent ahead of time without sitting in the write path:

- Send WAL to the agent's address, and the agent then writes through Model A.
- Buy storage resources in bulk and transfer them to the agent. You can split, merge, transfer, and even trade storage resources on marketplaces. The agent then pays only SUI gas to call `register_blob` and `certify_blob` against pre-bought capacity. Buying larger resources once and splitting them amortizes gas, which suits predictable, high-volume agents.

Use pre-funding for fleets of agents or batch workloads where a treasury provisions capacity centrally.

### What an upload relay does not do

An upload relay is easy to mistake for a sponsor, but it is not one. A relay only distributes the encoded slivers to storage nodes for the client, which helps in browser, mobile, or edge environments where opening thousands of connections is impractical. The agent still registers and certifies the blob onchain and still pays the WAL and SUI. A relay can also charge a tip, configured as `const` (a flat amount for each blob) or `linear` (an amount that scales with blob size), paid in MIST or WAL.

In short, an upload relay reduces the agent's network load, not its funding load. Do not confuse a Walrus upload relay with the Walrus Memory relayer: the Walrus Memory relayer runs the full write flow and can fund it, while a bare Walrus upload relay only forwards slivers.

### Protocol subsidies are network-level, not per-agent

Walrus reserves a portion of WAL supply as subsidies that supplement storage-node rewards while the network grows. This lowers effective costs across the whole network, but it is not a way for one party to fund a specific agent's blob. Do not model subsidies as sponsored storage in your architecture.

## Choose a model

| **If the agent** | **Use** | **Pays WAL** | **Pays SUI gas** | **Owns the blob** |
| --- | --- | --- | --- | --- |
| Is long-lived, owns its data, and manages its own lifecycle | Hold WAL directly | Agent | Agent | Agent |
| Holds no tokens, such as in a browser or untrusted environment | Publisher or relayer | Sponsor | Sponsor | Sponsor, unless you set `send-object-to` the agent |
| Holds WAL but should not manage gas | Sui sponsored transactions | Agent | Sponsor | Agent |
| Is one of many, funded from a central treasury | Pre-funded WAL or storage resources | Sponsor (upfront) | Agent | Agent |
| Needs help distributing slivers and still self-funds | Upload relay | Agent | Agent | Agent |

## Operate an agent that funds its own storage

- Watch both balances. Monitor SUI and WAL together, because running dry on either blocks writes. The publisher reference design keeps each sub-wallet between 0.5 and 1.0 SUI and WAL in steady state with an automatic refill loop, which is a good pattern to copy for any self-funded agent.
- Plan for expiry. Blobs live only for the epochs you pay for. An agent that needs durable memory runs an extend-before-expiry loop, or hands that responsibility to its sponsor.
- Track ownership. Whoever owns the `Blob` object controls extend and delete. In sponsored uploads, confirm the object lands at the agent's address through `send-object-to` if the agent is meant to manage it.
- Estimate before you spend. The `walrus info` command shows current storage and write prices, and `walrus store --dry-run` reports the encoded size used for the WAL cost. Neither command submits a transaction.
- Develop on Testnet first. Free Testnet WAL that you swap 1:1 from Testnet SUI lets you exercise the full funding and lifecycle flow before you connect real treasury funds. Testnet might wipe data and uses 1-day epochs.

## Related links

- [How Storage Works](/walrus-memory/fundamentals/architecture/how-storage-works)
- [Public relayer](/walrus-memory/relayer/public-relayer)
- [Self-hosting the relayer](/walrus-memory/relayer/self-hosting)
- [Walrus storage costs](https://docs.wal.app/docs/system-overview/storage-costs)
- [Walrus network reference](https://docs.wal.app/docs/network-reference)
- [Walrus upload relay](https://docs.wal.app/docs/operator-guide/upload-relay)
- [Walrus TypeScript SDK](https://sdk.mystenlabs.com/walrus)