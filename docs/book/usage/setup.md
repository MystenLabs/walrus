# Setup

We provide a pre-compiled `walrus` client binary for macOS (Intel and Apple CPUs) and Ubuntu, which
supports different usage patterns (see [the next chapter](./interacting.md)). This chapter describes
the [prerequisites](#prerequisites), [installation](#installation), and
[configuration](#configuration) of the Walrus client.

Walrus is open-source under an Apache 2 license, and can also be built and installed from the Rust
source code via cargo.

```admonish info title="Walrus networks"
This page describes how to connect to Walrus **Mainnet**. See [Available networks](./networks.md)
for an overview over all Walrus networks.
```

## Prerequisites: Sui wallet, SUI and WAL {#prerequisites}

```admonish tip title="Quick wallet setup"
If you just want to set up a new Sui wallet for Walrus, you can skip this section and use the
`walrus generate-sui-wallet` command after [installing Walrus](#installation). In that case, make
sure to set the `wallet_config` parameter in the [Walrus
configuration](#advanced-configuration-optional) to the newly generated wallet. Also, make sure to
obtain some SUI and WAL tokens.
```

Interacting with Walrus requires a valid Sui wallet with some amount of SUI tokens. The
normal way to set this up is via the Sui CLI; see the [installation
instructions](https://docs.sui.io/guides/developer/getting-started/sui-install) in the Sui
documentation.

After installing the Sui CLI, you need to set up a wallet by running `sui client`, which
prompts you to set up a new configuration. Make sure to point it to Sui Mainnet, you can use the
full node at `https://fullnode.mainnet.sui.io:443` for this. See
[here](https://docs.sui.io/guides/developer/getting-started/connect) for further details.

After this, you should get something like this (everything besides the `mainnet` line is optional):

```terminal
$ sui client envs
╭──────────┬─────────────────────────────────────┬────────╮
│ alias    │ url                                 │ active │
├──────────┼─────────────────────────────────────┼────────┤
│ devnet   │ https://fullnode.devnet.sui.io:443  │        │
│ localnet │ http://127.0.0.1:9000               │        │
│ testnet  │ https://fullnode.testnet.sui.io:443 │        │
│ mainnet  │ https://fullnode.mainnet.sui.io:443 │ *      │
╰──────────┴─────────────────────────────────────┴────────╯
```

Make sure you have at least one gas coin with at least 1 SUI.

```terminal
$ sui client gas
╭─────────────────┬────────────────────┬──────────────────╮
│ gasCoinId       │ mistBalance (MIST) │ suiBalance (SUI) │
├─────────────────┼────────────────────┼──────────────────┤
│ 0x65dca966dc... │ 1000000000         │ 1.00             │
╰─────────────────┴────────────────────┴──────────────────╯
```

Finally, to publish blobs on Walrus you will need some Mainnet WAL to pay for storage and upload
costs. You can buy WAL through a variety of centralized or decentralized exchanges.

<!-- TODO(WAL-710): how to get WAL. -->

The system-wide wallet will be used by Walrus if no other path is specified. If you want to use a
different Sui wallet, you can specify this in the [Walrus configuration file](#configuration) or
when [running the CLI](./interacting.md).

## Installation

We currently provide the `walrus` client binary for macOS (Intel and Apple CPUs), Ubuntu, and
Windows:

| OS      | CPU                   | Architecture                                                                                                                 |
| ------- | --------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| Ubuntu  | Intel 64bit           | [`ubuntu-x86_64`](https://storage.googleapis.com/mysten-walrus-binaries/walrus-mainnet-latest-ubuntu-x86_64)                 |
| Ubuntu  | Intel 64bit (generic) | [`ubuntu-x86_64-generic`](https://storage.googleapis.com/mysten-walrus-binaries/walrus-mainnet-latest-ubuntu-x86_64-generic) |
| MacOS   | Apple Silicon         | [`macos-arm64`](https://storage.googleapis.com/mysten-walrus-binaries/walrus-mainnet-latest-macos-arm64)                     |
| MacOS   | Intel 64bit           | [`macos-x86_64`](https://storage.googleapis.com/mysten-walrus-binaries/walrus-mainnet-latest-macos-x86_64)                   |
| Windows | Intel 64bit           | [`windows-x86_64.exe`](https://storage.googleapis.com/mysten-walrus-binaries/walrus-mainnet-latest-windows-x86_64.exe)       |

```admonish title="Windows"
We now offer a pre-built binary also for Windows. However, most of the remaining instructions assume
a UNIX-based system for the directory structure, commands, etc. If you use Windows, you may need to
adapt most of those.
```

### Install via script {#nix-install}

To download and install `walrus` to your `"$HOME"/.local/bin` directory, run one of the following
commands in your terminal then follow on-screen instructions. See [Windows
instructions](#windows-install) if you are on Windows.

```sh
# Run a first-time install.
curl -sSf https://raw.githubusercontent.com/MystenLabs/walrus/refs/heads/main/scripts/walrus-install.sh | sh

# Update an existing installation (overwrites prior version of walrus).
curl -sSf https://raw.githubusercontent.com/MystenLabs/walrus/refs/heads/main/scripts/walrus-install.sh | sh -s -- -f
```

Make sure that the `"$HOME"/.local/bin` directory is in your `$PATH`.

Once this is done, you should be able to run Walrus by using the `walrus` command in your terminal.

You can see usage instructions as follows (see [the next chapter](./interacting.md) for further
details):

```terminal
$ walrus --help
Walrus client

Usage: walrus [OPTIONS] <COMMAND>

Commands:
⋮
```

```admonish tip
Our latest Walrus binaries are also available on Walrus itself, namely on
<https://bin.walrus.site>, for example, <https://bin.walrus.site/walrus-mainnet-latest-ubuntu-x86_64>.
Note that due to DoS protection, it may not be possible to download the binaries with `curl` or
`wget`.
```

### Install on Windows {#windows-install}

To download `walrus` to your Microsoft Windows computer, run the following in a PowerShell.

```PowerShell
(New-Object System.Net.WebClient).DownloadFile(
  "https://storage.googleapis.com/mysten-walrus-binaries/walrus-testnet-latest-windows-x86_64.exe",
  "walrus.exe"
)
```

From there, you'll need to place `walrus.exe` somewhere in your `PATH`.

### Previous versions (optional)

In addition to the latest version of the `walrus` binary, the GCS bucket also contains previous
versions. An overview in XML format is available at
<https://storage.googleapis.com/mysten-walrus-binaries/>.

### Build from source

Walrus is open-source software published under the Apache 2 license. The code is developed in a
`git` repository at <https://github.com/MystenLabs/walrus>.

The latest version of Mainnet and Testnet are available under the branches `mainnet` and `testnet`
respectively, and the latest version under the `main` branch. We welcome reports of issues and bug
fixes. Follow the instructions in the `README.md` file to build and use Walrus from source.

## Configuration

The Walrus client needs to know about the Sui objects that store the Walrus system and staking
information, see the [developer guide](../dev-guide/sui-struct.md#system-and-staking-information).
These need to be configured in a file `~/.config/walrus/client_config.yaml`. Additionally, a
`subsidies` object can be specified, which will subsidize storage bought with the client.

You can access Testnet and Mainnet via the following configuration. Note that this example Walrus
CLI configuration refers to the standard location for Sui configuration
(`"~/.sui/sui_config/client.yaml"`).

```yaml
contexts:
  mainnet:
    system_object: 0x2134d52768ea07e8c43570ef975eb3e4c27a39fa6396bef985b5abc58d03ddd2
    staking_object: 0x10b9d30c28448939ce6c4d6c6e0ffce4a7f8a4ada8248bdad09ef8b70e4a3904
    subsidies_object: 0xb606eb177899edc2130c93bf65985af7ec959a2755dc126c953755e59324209e
    exchange_objects: []
    wallet_config:
      path: ~/.sui/sui_config/client.yaml
      active_env: mainnet
  testnet:
    system_object: 0x98ebc47370603fe81d9e15491b2f1443d619d1dab720d586e429ed233e1255c1
    staking_object: 0x20266a17b4f1a216727f3eef5772f8d486a9e3b5e319af80a5b75809c035561d
    exchange_objects:
      - 0x59ab926eb0d94d0d6d6139f11094ea7861914ad2ecffc7411529c60019133997
      - 0x89127f53890840ab6c52fca96b4a5cf853d7de52318d236807ad733f976eef7b
      - 0x9f9b4f113862e8b1a3591d7955fadd7c52ecc07cf24be9e3492ce56eb8087805
      - 0xb60118f86ecb38ec79e74586f1bb184939640911ee1d63a84138d080632ee28a
    subsidies_object: 0x4b23c353c35a4dde72fe862399ebe59423933d3c2c0a3f2733b9f74cb3b4933d
    wallet_config:
      path: ~/.sui/sui_config/client.yaml
      active_env: testnet
default_context: mainnet
```

<!-- markdownlint-disable code-fence-style -->
~~~admonish tip
The easiest way to obtain the latest configuration is by downloading it from GitHub:

```sh
curl https://raw.githubusercontent.com/MystenLabs/walrus/refs/heads/main/docs/book/config/client_config_mainnet.yaml \
    -o ~/.config/walrus/client_config.yaml
```
~~~
<!-- markdownlint-enable code-fence-style -->

### Custom path (optional) {#config-custom-path}

By default, the Walrus client will look for the `client_config.yaml` (or `client_config.yml`)
configuration file in the current directory, `$XDG_CONFIG_HOME/walrus/`, `~/.config/walrus/`, or
`~/.walrus/`. However, you can place the file anywhere and name it anything you like; in this case
you need to use the `--config` option when running the `walrus` binary.

### Advanced configuration (optional)

The configuration file currently supports the following parameters:

```yaml
# These are the only mandatory fields. These objects are specific for a particular Walrus
# deployment but then do not change over time.
system_object: 0x2134d52768ea07e8c43570ef975eb3e4c27a39fa6396bef985b5abc58d03ddd2
staking_object: 0x10b9d30c28448939ce6c4d6c6e0ffce4a7f8a4ada8248bdad09ef8b70e4a3904

# The subsidies object allows the client to use the subsidies contract to purchase storage
# which will reduce the cost of obtaining a storage resource and extending blobs and also
# adds subsidies to the rewards of the staking pools.
subsidies_object: 0xb606eb177899edc2130c93bf65985af7ec959a2755dc126c953755e59324209e

# You can define a custom path to your Sui wallet configuration here. If this is unset or `null`
# (default), the wallet is configured from `./sui_config.yaml` (relative to your current working
# directory), or the system-wide wallet at `~/.sui/sui_config/client.yaml` in this order. Both
# `active_env` and `active_address` can be omitted, in which case the values from the Sui wallet
# are used.
wallet_config:
  # The path to the wallet configuration file.
  path: ~/.sui/sui_config/client.yaml
  # The optional `active_env` to use to override whatever `active_env` is listed in the
  # configuration file.
  active_env: mainnet
  # The optional `active_address` to use to override whatever `active_address` is listed in the
  # configuration file.
  active_address: 0x...

# The following parameters can be used to tune the networking behavior of the client. There is no
# risk in playing around with these values. In the worst case, you may not be able to store/read
# blob due to timeouts or other networking errors.
communication_config:
  max_concurrent_writes: null
  max_concurrent_sliver_reads: null
  max_concurrent_metadata_reads: 3
  max_concurrent_status_reads: null
  max_data_in_flight: null
  reqwest_config:
    total_timeout_millis: 30000
    pool_idle_timeout_millis: null
    http2_keep_alive_timeout_millis: 5000
    http2_keep_alive_interval_millis: 30000
    http2_keep_alive_while_idle: true
  request_rate_config:
    max_node_connections: 10
    backoff_config:
      min_backoff_millis: 1000
      max_backoff_millis: 30000
      max_retries: 5
  disable_proxy: false
  disable_native_certs: false
  sliver_write_extra_time:
    factor: 0.5
    base_millis: 500
  registration_delay_millis: 200
  max_total_blob_size: 1073741824
  committee_change_backoff:
    min_backoff_millis: 1000
    max_backoff_millis: 5000
    max_retries: 5
refresh_config:
  refresh_grace_period_secs: 10
  max_auto_refresh_interval_secs: 30
  min_auto_refresh_interval_secs: 5
  epoch_change_distance_threshold_secs: 300
  refresher_channel_size: 100
```
