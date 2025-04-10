# Available networks

Walrus Mainnet operates a production-quality storage network on the Sui Mainnet. The Walrus Testnet
operates on the Sui Testnet and is used to test new features of Walrus before they graduate to the
Mainnet. Finally, developers can operate local Walrus and Sui networks for testing.

## Network parameters

Some important fixed system parameters for Mainnet and Testnet are summarized in the following table:

| Parameter                                                | Mainnet | Testnet |
| -------------------------------------------------------- | ------- | ------- |
| Number of shards                                         | 1000    | 1000    |
| Epoch duration                                           | 2 weeks | 1 day   |
| Maximum number of epochs for which storage can be bought | 53      | 53      |

Many other parameters, including the system capacity and prices, are dynamic. Those are stored in
the system object (see [here](../dev-guide/sui-struct.md#system-and-staking-information)) and can be
viewed with various tools like the [Walruscan explorer](https://walruscan.com/).

## Mainnet configuration

### Mainnet parameters

The client parameters for the Walrus Mainnet are:

```yaml
{{ #include ../setup/client_config_mainnet.yaml }}
```

In case you wish to explore the Walrus contracts, their package IDs are the following:

- WAL package: `0x356a26eb9e012a68958082340d4c4116e7f55615cf27affcff209cf0ae544f59`
- Walrus package: `0xfdc88f7d7cf30afab2f82e8380d11ee8f70efb90e863d1de8616fae1bb09ea77`
- Subsidies package: `0xd843c37d213ea683ec3519abe4646fd618f52d7fce1c4e9875a4144d53e21ebc`

As these are inferred automatically from the object IDs above, there is no need to manually input
them into the Walrus client configuration file. The latest published package IDs can also be found
in the `Move.lock` files in the subdirectories of the [`contracts` directory on
GitHub](https://github.com/MystenLabs/walrus/tree/main/contracts).

The configuration file described on the [setup page](./setup.md#configuration) includes both Mainnet
and Testnet configuration. If you want *only* the Mainnet configuration, you can get the file
[here](../setup/client_config_mainnet.yaml).

## Testnet configuration

```admonish warning title="Walrus Testnet redeployment"
The Walrus Testnet is currently undergoing a redeployment. The configuration parameters included
here refer to the *new* Testnet v3, which will be operational after 2025-04-03T15:00:00Z.
```

```admonish danger title="Disclaimer about the Walrus Testnet"
All transactions are executed on the Sui Testnet and use Testnet WAL and SUI which have no
value. The state of the store **can and will be wiped** at any point and possibly with no warning.
Do not rely on this Testnet for any production purposes, it comes with no availability or
persistence guarantees.

New features on testnet may break deployed testnet apps, since this is the network in which we test
new updates including for compatibility with eco-system applications.

Also see the [Testnet terms of service](../legal/testnet_tos.md) under which this Testnet is made
available.
```

### Prerequisites: Sui wallet and Testnet SUI {#prerequisites}

Interacting with Walrus requires a valid Sui Testnet wallet with some amount of SUI tokens. The
normal way to set this up is via the Sui CLI; see the [installation
instructions](https://docs.sui.io/guides/developer/getting-started/sui-install) in the Sui
documentation.

After installing the Sui CLI, you need to set up a Testnet wallet by running `sui client`, which
prompts you to set up a new configuration. Make sure to point it to Sui Testnet, you can use the
full node at `https://fullnode.testnet.sui.io:443` for this. See
[here](https://docs.sui.io/guides/developer/getting-started/connect) for further details.

If you already have a Sui wallet configured, you can directly set up the Testnet environment (if you
don't have it yet),

```sh
sui client new-env --alias testnet --rpc https://fullnode.testnet.sui.io:443
```

and switch the active environment to it:

```sh
sui client switch --env testnet
```

After this, you should get something like this (everything besides the `testnet` line is optional):

```terminal
$ sui client envs
╭──────────┬─────────────────────────────────────┬────────╮
│ alias    │ url                                 │ active │
├──────────┼─────────────────────────────────────┼────────┤
│ devnet   │ https://fullnode.devnet.sui.io:443  │        │
│ localnet │ http://127.0.0.1:9000               │        │
│ testnet  │ https://fullnode.testnet.sui.io:443 │ *      │
│ mainnet  │ https://fullnode.mainnet.sui.io:443 │        │
╰──────────┴─────────────────────────────────────┴────────╯
```

Finally, make sure you have at least one gas coin with at least 1 SUI. You can obtain one from the
[Sui Testnet faucet](https://faucet.sui.io/?network=testnet) (you can find your address through the
`sui client active-address` command).

After some seconds, you should see your new SUI coins:

```terminal
$ sui client gas
╭─────────────────┬────────────────────┬──────────────────╮
│ gasCoinId       │ mistBalance (MIST) │ suiBalance (SUI) │
├─────────────────┼────────────────────┼──────────────────┤
│ 0x65dca966dc... │ 1000000000         │ 1.00             │
╰─────────────────┴────────────────────┴──────────────────╯
```

The system-wide wallet will be used by Walrus if no other path is specified. If you want to use a
different Sui wallet, you can specify this in the [Walrus configuration file](./setup.md#configuration)
or when [running the CLI](./interacting.md).

### Testnet parameters

The configuration parameters for the Walrus Testnet are included in the configuration file described
on the [setup page](./setup.md#configuration). If you want *only* the Testnet configuration, you can
get the file [here](../setup/client_config_testnet.yaml). The parameters are:

```yaml
{{ #include ../setup/client_config_testnet.yaml }}
```

The current Testnet package IDs can be
found in the `Move.lock` files in the subdirectories of the [`testnet-contracts` directory on
GitHub](https://github.com/MystenLabs/walrus/tree/main/testnet-contracts).

### Testnet WAL faucet

The Walrus Testnet uses Testnet WAL tokens to buy storage and stake. Testnet WAL tokens have no
value and can be exchanged (at a 1:1 rate) for some Testnet SUI tokens, which also have no value,
through the following command:

```sh
walrus get-wal
```

You can check that you have received Testnet WAL by checking the Sui balances:

```sh
$ sui client balance
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

By default, 0.5 SUI are exchanged for 0.5 WAL, but a different amount of SUI may be exchanged using
the `--amount` option (the value is in MIST/FROST), and a specific SUI/WAL exchange object may be
used through the `--exchange-id` option. The `walrus get-wal --help` command provides more
information about those.

## Running a local Walrus network

In addition to publicly deployed Walrus networks, you can deploy a Walrus testbed on your local
machine for local testing. All you need to do is run the script `scripts/local-testbed.sh` found
in the Walrus [git code repository](https://github.com/MystenLabs/walrus). See
`scripts/local-testbed.sh -h` for further usage information.

The script generates configuration that you can use when [running the Walrus client](./client-cli.md)
and prints the path to that configuration file.

In addition, one can spin up a local grafana instance to visualize the metrics collected by the
storage nodes. This can be done via `cd docker/grafana-local; docker compose up`. This should work
with the default storage node configuration.

Note that while the Walrus storage nodes of this testbed run on your local machine, the Sui Devnet
is used by default to deploy and interact with the contracts. To run the testbed fully locally,
simply [start a local network with `sui start --with-faucet --force-regenesis`](https://docs.sui.io/guides/developer/getting-started/local-network)
(requires `sui` to be `v1.28.0` or higher) and specify `localnet` when starting the Walrus testbed.
