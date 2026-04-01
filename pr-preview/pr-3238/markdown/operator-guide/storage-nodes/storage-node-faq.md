## Which wallets do you need? {#wallets}

- The storage node must have a wallet compatible with the Sui Rust SDK. This **cannot** be a hardware wallet or anything that requires user interaction for transactions, because the node needs to send transactions as part of normal operation.
  - The `walrus-node setup` command creates such a wallet. You can replace it with a different wallet, but this is not recommended.
  - This wallet is stored on the storage node and should not hold too many funds.
- The publisher needs a **separate wallet** from the storage node, even if running on the same host. See [Publisher setup](/docs/operator-guide/publishers/operating-publisher) for details.
- The aggregator does not need a wallet because it never sends transactions.
- For collecting commission and performing governance operations, you can designate other wallets (including hardware or multi-sig wallets). See [Commission and Governance](/docs/operator-guide/commission-governance) for details.

## How can you use TLS? {#tls}

You can configure TLS paths in `walrus-node.yaml`:

```yaml
network_key_pair:
  path: /opt/walrus/config/tls/privkey.pem
tls:
  disable_tls: false
  certificate_path: /opt/walrus/config/tls/fullchain.pem
```

:::caution

Do not disable TLS. While the option exists, very few correct reverse-proxy configurations work without it.

:::

If you set up certbot as described in the [Storage Node Setup guide](/docs/operator-guide/storage-node-setup#tls-setup), it reuses the same key when renewing (`--reuse-key`).

If you ever must change the key, the node detects changes and updates onchain information automatically. It checks for changes every 15 minutes and during startup. Other nodes might take some time to pick up the change, so only do this when necessary.

The same automatic update works for all other configuration changes, including voting parameters, commission rate, and protocol key.

## What is the difference between `--sui-network` and `--sui-rpc`? {#sui-rpc}

`--sui-network` configures the wallet used by the storage node. It does not directly handle RPC requests.

`--sui-rpc` is the endpoint used for all Sui interactions, including reading events and transaction data, and executing transactions.

## How do you set the commission rate? {#commission}

The commission rate is set in basis points, where 100 basis points equals 1%:

| **Basis points** | **Percentage** |
|-------------|------------|
| 1 bp | 0.01% |
| 10 bp | 0.1% |
| 100 bp | 1% |
| 1000 bp | 10% |
| 6000 bp | 60% (default) |
| 10000 bp | 100% |

The default value (6000 bp = 60%) is higher than for most blockchain systems because higher stake on Walrus comes with higher hardware cost, in particular storage capacity.