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
If you just want to set up a new Sui wallet for Walrus, you can generate one using the
`walrus generate-sui-wallet --network mainnet` command after [installing Walrus](#installation).
You still need to obtain some SUI and WAL tokens, but you do not have to install the Sui CLI.
```

Interacting with Walrus requires a valid Sui wallet with some amount of SUI and WAL tokens. The
normal way to set this up is via the Sui CLI; see the [installation
instructions](https://docs.sui.io/guides/developer/getting-started/sui-install) in the Sui
documentation.

After installing the Sui CLI, you need to set up a wallet by running `sui client`, which
prompts you to set up a new configuration. Make sure to point it to Sui Mainnet, you can use the
full node at `https://fullnode.mainnet.sui.io:443` for this. See [the Sui
documentation](https://docs.sui.io/guides/developer/getting-started/connect) for further details.

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

The system-wide wallet will be used by Walrus if no other path is specified. If you want to use a
different Sui wallet, you can specify this in the [Walrus configuration file](#configuration) or
when [running the CLI](./interacting.md).

## Installation

We currently provide the `walrus` client binary for macOS (Intel and Apple CPUs), Ubuntu, and
Windows. The Ubuntu version most likely works on other Linux distributions as well.

| OS      | CPU                   | Architecture                                                                                                                 |
| ------- | --------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| Ubuntu  | Intel 64bit           | [`ubuntu-x86_64`](https://storage.googleapis.com/mysten-walrus-binaries/walrus-mainnet-latest-ubuntu-x86_64)                 |
| Ubuntu  | Intel 64bit (generic) | [`ubuntu-x86_64-generic`](https://storage.googleapis.com/mysten-walrus-binaries/walrus-mainnet-latest-ubuntu-x86_64-generic) |
| MacOS   | Apple Silicon         | [`macos-arm64`](https://storage.googleapis.com/mysten-walrus-binaries/walrus-mainnet-latest-macos-arm64)                     |
| MacOS   | Intel 64bit           | [`macos-x86_64`](https://storage.googleapis.com/mysten-walrus-binaries/walrus-mainnet-latest-macos-x86_64)                   |
| Windows | Intel 64bit           | [`windows-x86_64.exe`](https://storage.googleapis.com/mysten-walrus-binaries/walrus-mainnet-latest-windows-x86_64.exe)       |

### Install via script {#nix-install}

To download and install `walrus` to your `"$HOME"/.local/bin` directory, run one of the following
commands in your terminal then follow on-screen instructions. See [Windows
instructions](#windows-install) if you are on Windows.

```sh
# Run a first-time install using the latest Mainnet version.
curl -sSf https://docs.wal.app/setup/walrus-install.sh | sh

# Install the latest Testnet version instead.
curl -sSf https://docs.wal.app/setup/walrus-install.sh | sh -s -- -n testnet

# Update an existing installation (overwrites prior version of walrus).
curl -sSf https://docs.wal.app/setup/walrus-install.sh | sh -s -- -f
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
<https://bin.wal.app>, for example, <https://bin.wal.app/walrus-mainnet-latest-ubuntu-x86_64>.
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

```admonish title="Windows"
Note that most of the remaining instructions assume a UNIX-based system for the directory structure,
commands, etc. If you use Windows, you may need to adapt most of those.
```

### GitHub releases

You can find all our releases including release notes on [GitHub](https://github.com/MystenLabs/walrus/releases).
Simply download the archive for your system and extract the `walrus` binary.

### Install via Cargo

You can also install Walrus via Cargo. For example, to install the latest Mainnet version:

```sh
cargo install --git https://github.com/MystenLabs/walrus --branch mainnet walrus-service --locked
```

In place of `--branch mainnet`, you can also specify specific tags (e.g., `--tag mainnet-v1.18.2`)
or commits (e.g., `--rev b2009ac73388705f379ddad48515e1c1503fc8fc`).

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
{{ #include ../setup/client_config.yaml }}
```

<!-- markdownlint-disable code-fence-style -->
~~~admonish tip
The easiest way to obtain the latest configuration is by downloading it directly from Walrus:

```sh
curl https://docs.wal.app/setup/client_config.yaml -o ~/.config/walrus/client_config.yaml
```
~~~
<!-- markdownlint-enable code-fence-style -->

```admonish warning title="Walrus Testnet redeployment"
The Walrus Testnet is currently undergoing a redeployment. The configuration parameters included
here refer to the *new* Testnet v3, which will be operational after 2025-04-03T15:00:00Z.
```

### Custom path (optional) {#config-custom-path}

By default, the Walrus client will look for the `client_config.yaml` (or `client_config.yml`)
configuration file in the current directory, `$XDG_CONFIG_HOME/walrus/`, `~/.config/walrus/`, or
`~/.walrus/`. However, you can place the file anywhere and name it anything you like; in this case
you need to use the `--config` option when running the `walrus` binary.

### Advanced configuration (optional)

The configuration file currently supports the following parameters for each of the contexts:

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
{{ #include ../setup/client_config_example.yaml:8: }}
```
