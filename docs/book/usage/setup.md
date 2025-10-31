# Advanced Setup

This page covers advanced setup options for Walrus, including building from source,
installing from binaries, or using Cargo. For standard setup instructions, see the
Walrus [Getting Started](./setup.md) guide.



Walrus is open source under an Apache 2 license. You can download and install it through
[`suiup`](./setup.md), or you can build and
install it from the Rust source code through Cargo.

## Walrus binaries

The `walrus` client binary is currently provided for macOS (Intel and Apple CPUs),
Ubuntu, and Windows. The Ubuntu version most likely works on other Linux distributions
as well.

| OS      | CPU                   | Architecture                                                                                                                 |
| ------- | --------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| Ubuntu  | Intel 64bit           | [`ubuntu-x86_64`](https://storage.googleapis.com/mysten-walrus-binaries/walrus-mainnet-latest-ubuntu-x86_64)                 |
| Ubuntu  | Intel 64bit (generic) | [`ubuntu-x86_64-generic`](https://storage.googleapis.com/mysten-walrus-binaries/walrus-mainnet-latest-ubuntu-x86_64-generic) |
| Ubuntu  | ARM 64bit             | [`ubuntu-aarch64`](https://storage.googleapis.com/mysten-walrus-binaries/walrus-mainnet-latest-ubuntu-aarch64)               |
| MacOS   | Apple Silicon         | [`macos-arm64`](https://storage.googleapis.com/mysten-walrus-binaries/walrus-mainnet-latest-macos-arm64)                     |
| MacOS   | Intel 64bit           | [`macos-x86_64`](https://storage.googleapis.com/mysten-walrus-binaries/walrus-mainnet-latest-macos-x86_64)                   |
| Windows | Intel 64bit           | [`windows-x86_64.exe`](https://storage.googleapis.com/mysten-walrus-binaries/walrus-mainnet-latest-windows-x86_64.exe)       |

```admonish tip
Our latest Walrus binaries are also available on Walrus itself, namely on
<https://bin.wal.app>, for example,
<https://bin.wal.app/walrus-mainnet-latest-ubuntu-x86_64>.
Because of DoS protection, it might not be possible to download the binaries with
`curl` or `wget`.
```

## Install via script {#nix-install}

To download and install `walrus` to your `"$HOME"/.local/bin` directory, follow the
instructions in the [Getting Started](./setup.md) guide.

## Install on Windows {#windows-install}

To download `walrus` to your Microsoft Windows computer, run the following in a
PowerShell.

```PowerShell
(New-Object System.Net.WebClient).DownloadFile(
  "https://storage.googleapis.com/mysten-walrus-binaries/walrus-testnet-latest-windows-x86_64.exe",
  "walrus.exe"
)
```

From there, place `walrus.exe` somewhere in your `PATH`.

```admonish title="Windows"
Most of the remaining instructions assume a UNIX-based system for the directory
structure, commands, and so on. If you use Windows, you might need to adapt most of
those.
```

## GitHub releases

You can find all the releases including release notes on
[GitHub](https://github.com/MystenLabs/walrus/releases). Download the archive for your
system and extract the `walrus` binary.

## Install via Cargo

You can also install Walrus through Cargo. For example, to install the latest Mainnet
version:

```sh
cargo install --git https://github.com/MystenLabs/walrus --branch mainnet walrus-service --locked
```

In place of `--branch mainnet`, you can also specify specific tags (for example,
`--tag mainnet-v1.18.2`) or commits (for example,
`--rev b2009ac73388705f379ddad48515e1c1503fc8fc`).

## Build from source

Walrus is open source software published under the Apache 2 license. The code is
developed in a `git` repository at <https://github.com/MystenLabs/walrus>.

The latest version of Mainnet and Testnet are available under the branches `mainnet` and
`testnet` respectively, and the latest version under the `main` branch. Reports of issues
and bug fixes are welcome. Follow the instructions in the `README.md` file to build and
use Walrus from source.

## Configuration

````admonish tip
The easiest way to obtain the latest configuration is by downloading it directly from
Walrus:

```sh
curl --create-dirs https://docs.wal.app/setup/client_config.yaml -o ~/.config/walrus/client_config.yaml
```
````

<!-- markdownlint-enable code-fence-style -->

The Walrus client needs to know about the Sui objects that store the Walrus system and
staking information, see the
[developer guide](../dev-guide/sui-struct.md#system-and-staking-information). Configure
these in a file at `~/.config/walrus/client_config.yaml`.

You can access Testnet and Mainnet through the following configuration. This example
Walrus CLI configuration refers to the standard location for Sui configuration
(`"~/.sui/sui_config/client.yaml"`).

```yaml
{{ #include ../setup/client_config.yaml }}
```

<!-- markdownlint-disable code-fence-style -->

### Custom path (optional) {#config-custom-path}

By default, the Walrus client looks for the `client_config.yaml` (or
`client_config.yml`) configuration file in the current directory,
`$XDG_CONFIG_HOME/walrus/`, `~/.config/walrus/`, or `~/.walrus/`. However, you can place
the file anywhere and name it anything you like. In this case, use the `--config` option
when running the `walrus` binary.

## Advanced configuration (optional)

The configuration file currently supports the following parameters for each of the
contexts:

```yaml
# These are the only mandatory fields. These objects are specific for a particular Walrus
# deployment but then do not change over time.
system_object: 0x2134d52768ea07e8c43570ef975eb3e4c27a39fa6396bef985b5abc58d03ddd2
staking_object: 0x10b9d30c28448939ce6c4d6c6e0ffce4a7f8a4ada8248bdad09ef8b70e4a3904

# You can specify a list of Sui RPC URLs for reads. If none is provided, the RPC URL in the Sui
# wallet is used.
rpc_urls:
  - https://fullnode.mainnet.sui.io:443

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

# [...]
```

There are some additional parameters that you can use to tune the networking behavior of
the client, see the
[full example client configuration](../setup/client_config_example.yaml). If you
experience excessively slow uploads, it might be worth experimenting with these values.
There is no risk in playing around with these values. In the worst case, you might not be
able to store or read blob because of timeouts or other networking errors.
