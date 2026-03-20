Walrus Mainnet operates a production-quality storage network using corresponding resources on the Sui Mainnet. The Walrus Testnet operates in conjunction with the Sui Testnet and is used to test new features before they graduate to Mainnet. Alternatively, developers can operate a local instance of both Walrus and Sui for personalized testing.

## Network parameters

Important fixed system parameters for Mainnet and Testnet are summarized in the following table:

| Parameter                                                | Mainnet | Testnet |
|----------------------------------------------------------|---------|---------|
| Sui network                                              | Mainnet | Testnet |
| Number of shards                                         | 1000    | 1000    |
| Epoch duration                                           | 2 weeks | 1 day   |
| Maximum number of epochs for which storage can be bought | 53      | 53      |

Many other parameters, including the system capacity and prices, are dynamic. These parameters are stored in the system object and you can view them with tools like the [Walruscan explorer](https://walruscan.com/).

## Mainnet configuration

The client parameters for the Walrus Mainnet are:

<!-- IMPORT_CONTENT_RESOLVED source="/setup/client_config_mainnet.yaml" mode="code" -->
```yaml title="setup/client_config_mainnet.yaml"
# NOTE: walrus-service uses these IDs to detect network defaults. Changing them changes node
# behavior and must be coordinated.
system_object: 0x2134d52768ea07e8c43570ef975eb3e4c27a39fa6396bef985b5abc58d03ddd2
staking_object: 0x10b9d30c28448939ce6c4d6c6e0ffce4a7f8a4ada8248bdad09ef8b70e4a3904
n_shards: 1000
max_epochs_ahead: 53
rpc_urls:
  - https://fullnode.mainnet.sui.io:443
```
<!-- /IMPORT_CONTENT_RESOLVED -->

To explore the Walrus contracts, their package IDs are:

- WAL package: [`0x356a26eb9e012a68958082340d4c4116e7f55615cf27affcff209cf0ae544f59`](https://suiscan.xyz/mainnet/object/0x356a26eb9e012a68958082340d4c4116e7f55615cf27affcff209cf0ae544f59/tx-blocks)

- Walrus package: [`0xfdc88f7d7cf30afab2f82e8380d11ee8f70efb90e863d1de8616fae1bb09ea77`](https://suiscan.xyz/mainnet/object/0xfdc88f7d7cf30afab2f82e8380d11ee8f70efb90e863d1de8616fae1bb09ea77/tx-blocks)

- Subsidies package: [`0xd843c37d213ea683ec3519abe4646fd618f52d7fce1c4e9875a4144d53e21ebc`](https://suiscan.xyz/mainnet/object/0xd843c37d213ea683ec3519abe4646fd618f52d7fce1c4e9875a4144d53e21ebc/tx-blocks)

The Walrus client infers these package IDs automatically from the object IDs above, so you do not need to enter them manually in the configuration file. You can also find the latest published package IDs in the `Move.lock` files in the subdirectories of the [`contracts` directory on GitHub](https://github.com/MystenLabs/walrus/tree/main/contracts).

The configuration file described on the [setup page](/docs/getting-started) includes both Mainnet and Testnet configuration.

## Testnet configuration

All transactions run on the Sui Testnet and use Testnet WAL and SUI, which have no value.

:::danger

The state of the network can be wiped at any point and possibly without warning. Do not use this Testnet for any production purposes, as it comes with no availability or persistence guarantees. New features on Testnet might break deployed Testnet apps.

:::

See the [Testnet terms of service](/docs/legal/testnet_tos) under which this Testnet is made available.

The configuration parameters for the Walrus Testnet are included in the configuration file described on the [getting started guide](/docs/getting-started#step-2-configure-tooling-for-walrus-testnet). If you want only the Testnet configuration, you can get the Testnet-only configuration file. The parameters are:

<!-- IMPORT_CONTENT_RESOLVED source="/setup/client_config_testnet.yaml" mode="code" -->
```yaml title="setup/client_config_testnet.yaml"
# NOTE: walrus-service uses these IDs to detect network defaults. Changing them changes node
# behavior and must be coordinated.
system_object: 0x6c2547cbbc38025cf3adac45f63cb0a8d12ecf777cdc75a4971612bf97fdf6af
staking_object: 0xbe46180321c30aab2f8b3501e24048377287fa708018a5b7c2792b35fe339ee3
exchange_objects:
  - 0xf4d164ea2def5fe07dc573992a029e010dba09b1a8dcbc44c5c2e79567f39073
  - 0x19825121c52080bb1073662231cfea5c0e4d905fd13e95f21e9a018f2ef41862
  - 0x83b454e524c71f30803f4d6c302a86fb6a39e96cdfb873c2d1e93bc1c26a3bc5
  - 0x8d63209cf8589ce7aef8f262437163c67577ed09f3e636a9d8e0813843fb8bf1
n_shards: 1000
max_epochs_ahead: 53
rpc_urls:
  - https://fullnode.testnet.sui.io:443
communication_config:
  tail_handling: detached
  upload_mode: aggressive
  data_in_flight_auto_tune:
    enabled: true
```
<!-- /IMPORT_CONTENT_RESOLVED -->

You can find the current Testnet package IDs in the `Move.lock` files in the subdirectories of the [`testnet-contracts` directory on GitHub](https://github.com/MystenLabs/walrus/tree/main/testnet-contracts).

## Testnet WAL faucet

The Walrus Testnet uses Testnet WAL tokens for buying storage and staking. Testnet WAL tokens have no value and can be exchanged at a 1:1 rate for Testnet SUI tokens, which also have no value.

#### Prerequisites: Sui wallet and Testnet SUI {#prerequisites}

<Tabs>
<TabItem value="prereq" label="Prerequisites">

- [x] Download and install the [Sui CLI](/docs/getting-started#step-1-install-tooling).

- [x] Create a [Sui account](/docs/getting-started#step-3-understanding-your-sui-account).

- [x] Obtain [Testnet SUI](/docs/getting-started#step-4-fund-sui-account-with-tokens).

- [x] Download and install [Walrus](/docs/getting-started#step-1-install-tooling).

</TabItem>
</Tabs>

After completing the prerequisites, run the following command to exchange SUI for WAL:

```sh
$ walrus get-wal
```

You can verify that you received Testnet WAL by checking the Sui balances:

```sh
$ sui client balance
```

If successful, the console responds:

```sh
╭─────────────────────────────────────────╮
│ Balance of coins owned by this address  │
├─────────────────────────────────────────┤
│ ╭─────────────────────────────────────╮ │
│ │ coin  balance (raw)     balance     │ │
│ ├─────────────────────────────────────┤ │
│ │ Sui   8869252670        8.86 SUI    │ │
│ │ WAL   500000000         0.50 WAL    │ │
│ ╰─────────────────────────────────────╯ │
╰─────────────────────────────────────────╯
```

By default, 0.5 SUI are exchanged for 0.5 WAL. To exchange a different amount of SUI, use the `--amount` option. The value is in MIST/FROST. To use a specific SUI/WAL exchange object, use the `--exchange-id` option. Run `walrus get-wal --help` for more information about these options.

## Run a local Walrus network

You can deploy an instance of the Walrus network on your local machine for local testing. Run the script `scripts/local-testbed.sh` found in the [Walrus GitHub repository](https://github.com/MystenLabs/walrus). Run `scripts/local-testbed.sh -h` for further usage information. The script generates a configuration file that you can use when [running the Walrus client](/docs/walrus-client/storing-blobs).

You can also spin up a local Grafana instance to visualize the metrics collected by the storage nodes through `cd docker/grafana-local; docker compose up`. This works with the default storage node configuration.

The Walrus storage nodes of this local network run on your local machine. By default, the Sui Devnet deploys and interacts with the contracts. To run the local network fully locally, [start a local network with `sui start --with-faucet --force-regenesis`](https://docs.sui.io/guides/developer/sui-101/local-network) (requires `sui` version `v1.28.0` or higher) and specify `localnet` when starting the Walrus testbed.