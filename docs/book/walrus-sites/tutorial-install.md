# Installing the site builder

This section describes the steps necessary to setup the Walrus Sites' `site-builder` tool and
prepare your environment for development.

## Prerequisites

Before you start, make sure you

- have a recent version of [Rust](https://www.rust-lang.org/tools/install) installed;
- followed all [Walrus setup instructions](../usage/setup.md).

Then, follow these additional setup steps.

## Installation

Similar to the `walrus` client CLI tool, we currently provide the `site-builder` client binary for
macOS (Intel and Apple CPUs), Ubuntu, and Windows:

### Mainnet Binaries

| OS      | CPU                   | Architecture |
|---------|-----------------------|-----------------------------------------------------------------------------------------------------------------------------------|
| Ubuntu  | Intel 64bit           | [`site-builder-mainnet-latest-ubuntu-x86_64`](https://storage.googleapis.com/mysten-walrus-binaries/site-builder-mainnet-latest-ubuntu-x86_64)                |
| MacOS   | Apple Silicon         | [`site-builder-mainnet-latest-macos-arm64`](https://storage.googleapis.com/mysten-walrus-binaries/site-builder-mainnet-latest-macos-arm64)                    |
| MacOS   | Intel 64bit           | [`site-builder-mainnet-latest-macos-x86_64`](https://storage.googleapis.com/mysten-walrus-binaries/site-builder-mainnet-latest-macos-x86_64)                  |
| Windows | Intel 64bit           | [`site-builder-mainnet-latest-windows-x86_64.exe`](https://storage.googleapis.com/mysten-walrus-binaries/site-builder-mainnet-latest-windows-x86_64.exe)      |

### Testnet Binaries

| OS      | CPU                   | Architecture |
|---------|-----------------------|-----------------------------------------------------------------------------------------------------------------------------------|
| Ubuntu  | Intel 64bit           | [`site-builder-testnet-latest-ubuntu-x86_64`](https://storage.googleapis.com/mysten-walrus-binaries/site-builder-testnet-latest-ubuntu-x86_64)                |
| MacOS   | Apple Silicon         | [`site-builder-testnet-latest-macos-arm64`](https://storage.googleapis.com/mysten-walrus-binaries/site-builder-testnet-latest-macos-arm64)                    |
| MacOS   | Intel 64bit           | [`site-builder-testnet-latest-macos-x86_64`](https://storage.googleapis.com/mysten-walrus-binaries/site-builder-testnet-latest-macos-x86_64)                  |
| Windows | Intel 64bit           | [`site-builder-testnet-latest-windows-x86_64.exe`](https://storage.googleapis.com/mysten-walrus-binaries/site-builder-testnet-latest-windows-x86_64.exe)      |

```admonish title="Windows"
We now offer a pre-built binary also for Windows. However, most of the remaining instructions assume
a UNIX-based system for the directory structure, commands, etc. If you use Windows, you may need to
adapt most of those.
```

You can download the latest build from our Google Cloud Storage (GCS) bucket (correctly setting the
`$SYSTEM` variable):

### Mainnet curl request

``` sh
SYSTEM= # set this to your system: ubuntu-x86_64, ubuntu-x86_64-generic, macos-x86_64, macos-arm64, windows-x86_64.exe
curl https://storage.googleapis.com/mysten-walrus-binaries/site-builder-mainnet-latest-$SYSTEM -o site-builder
chmod +x site-builder
```

### Testnet curl request

``` sh
SYSTEM= # set this to your system: ubuntu-x86_64, ubuntu-x86_64-generic, macos-x86_64, macos-arm64, windows-x86_64.exe
curl https://storage.googleapis.com/mysten-walrus-binaries/site-builder-testnet-latest-$SYSTEM -o site-builder
chmod +x site-builder
```

To be able to run it simply as `site-builder`, move the binary to any directory included in your
`$PATH` environment variable. Standard locations are `/usr/local/bin/`, `$HOME/bin/`, or
`$HOME/.local/bin/`.

```admonish note
The site builder will look for the default configuration file `sites-config.yaml` in the current
directory, the `$XDG_HOME/walrus/sites-config.yaml` and `$HOME/walrus/sites-config.yaml` directory.
In case you want to use explicitly a different `sites-config.yaml`, use the `--config` flag to
specify the path to the configuration file.
```

Once this is done, you should be able to simply type `site-builder` in your terminal.

```terminal
$ site-builder
Usage: site-builder [OPTIONS] <COMMAND>

Commands:
  publish  Publish a new site on Sui
  update   Update an existing site
  convert  Convert an object ID in hex format to the equivalent Base36
               format
  sitemap  Show the pages composing the Walrus site at the given object ID
  help     Print this message or the help of the given subcommand(s)

  ⋮
```

## Configuration

The `site-builder` tool needs a configuration file to work. This file is called `sites-config.yaml`
and looks like this:

```yaml
contexts:
  testnet:
    # module: site
    # portal: wal.app
    package: 0xf99aee9f21493e1590e7e5a9aea6f343a1f381031a04a732724871fc294be799
    staking_object: 0xbe46180321c30aab2f8b3501e24048377287fa708018a5b7c2792b35fe339ee3
    general:
       wallet_env: testnet
       walrus_context: testnet # Assumes a Walrus CLI setup with a multi-config containing the "testnet" context.
       walrus_package: 0xd84704c17fc870b8764832c535aa6b11f21a95cd6f5bb38a9b07d2cf42220c66
       # wallet_address: 0x1234...
       # rpc_url: https://fullnode.testnet.sui.io:443
       # wallet: /path/to/.sui/sui_config/client.yaml
       # walrus_binary: /path/to/walrus
       # walrus_config: /path/to/testnet/client_config.yaml
       # gas_budget: 500000000
  mainnet:
    # module: site
    # portal: wal.app
    package: 0x26eb7ee8688da02c5f671679524e379f0b837a12f1d1d799f255b7eea260ad27
    staking_object: 0x10b9d30c28448939ce6c4d6c6e0ffce4a7f8a4ada8248bdad09ef8b70e4a3904
    general:
       wallet_env: mainnet
       walrus_context: mainnet # Assumes a Walrus CLI setup with a multi-config containing the "mainnet" context.
       walrus_package: 0xfdc88f7d7cf30afab2f82e8380d11ee8f70efb90e863d1de8616fae1bb09ea77
       # wallet_address: 0x1234...
       # rpc_url: https://fullnode.mainnet.sui.io:443
       # wallet: /path/to/.sui/sui_config/client.yaml
       # walrus_binary: /path/to/walrus
       # walrus_config: /path/to/mainnet/client_config.yaml
       # gas_budget: 500000000

default_context: mainnet
```

As you can see, the configuration file is quite simple. You can define here different contexts and
their configurations, with just the package id field being the mandatory one, representing the Sui
object ID of the Walrus Sites smart contract. You can find the latest version of the package in the
[Walrus Sites repository](https://github.com/MystenLabs/walrus-sites/tree/mainnet) on the `mainnet`
branch.

```admonish danger title="Walrus Sites stable branch"
The stable branch of Walrus Sites is `mainnet`.
Make sure that you always pull the latest changes from there.
```

You can define the location of the `sites-config.yaml` file using the `--config` flag when running
the `site-builder` commands like so:

``` sh
site-builder --config /path/to/sites-config.yaml publish <build-directory-of-a-site>
```

However, if are not a fan of repeating the same flags over and over, it's always easier to have the
configuration file in one of the [default locations](./tutorial-install.md#admonition-note).

Download the `sites-config.yaml` file from the repository, and place it in one of the aforementioned
default locations. To illustrate, we will use the `~/.config/walrus` directory, like so:

```sh
curl https://raw.githubusercontent.com/MystenLabs/walrus-sites/refs/heads/mainnet/sites-config.yaml -o ~/.config/walrus/sites-config.yaml
```

You are now ready to start working on your Walrus Sites! 🎉
