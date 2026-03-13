This page covers advanced setup options for Walrus, including building from source, installing from binaries, or using Cargo. For standard setup instructions, see [Getting Started](/docs/getting-started).

Walrus is open source under an Apache 2 license. You can download and install it through [`suiup`](https://github.com/MystenLabs/suiup) on GitHub, or you can build and install it from the Rust source code through Cargo.

## Walrus binaries

The `walrus` client binary is currently provided for macOS (Intel and Apple CPUs), Ubuntu, and Windows. The Ubuntu version most likely works on other Linux distributions as well.

| **OS**      | **CPU**               | **Architecture**                                                                                                             |
| ------- | --------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| Ubuntu  | Intel 64bit           | [`ubuntu-x86_64`](https://storage.googleapis.com/mysten-walrus-binaries/walrus-mainnet-latest-ubuntu-x86_64)                 |
| Ubuntu  | Intel 64bit (generic) | [`ubuntu-x86_64-generic`](https://storage.googleapis.com/mysten-walrus-binaries/walrus-mainnet-latest-ubuntu-x86_64-generic) |
| Ubuntu  | ARM 64bit             | [`ubuntu-aarch64`](https://storage.googleapis.com/mysten-walrus-binaries/walrus-mainnet-latest-ubuntu-aarch64)               |
| macOS   | Apple Silicon         | [`macos-arm64`](https://storage.googleapis.com/mysten-walrus-binaries/walrus-mainnet-latest-macos-arm64)                     |
| macOS   | Intel 64bit           | [`macos-x86_64`](https://storage.googleapis.com/mysten-walrus-binaries/walrus-mainnet-latest-macos-x86_64)                   |
| Windows | Intel 64bit           | [`windows-x86_64.exe`](https://storage.googleapis.com/mysten-walrus-binaries/walrus-mainnet-latest-windows-x86_64.exe)       |

:::tip

The latest Walrus binaries are also available on Walrus itself at https://bin.wal.app (for example, https://bin.wal.app/walrus-mainnet-latest-ubuntu-x86_64). Because of DoS protection, downloading the binaries with `curl` or `wget` might not work.

You can also find all releases including release notes on [GitHub](https://github.com/MystenLabs/walrus/releases). Download the archive for your system and extract the `walrus` binary.

:::

## Install through script {#nix-install}

To download and install `walrus` to your `"$HOME"/.local/bin` directory, run one of the following commands in your terminal, then follow the on-screen instructions. If you use Windows, see the [Windows-specific instructions](#windows-install) or the [`suiup` installation](https://github.com/MystenLabs/suiup) on GitHub.

```bash
# Run a first-time install using the latest Mainnet version.
$ curl -sSf https://install.wal.app | sh

# Install the latest Testnet version instead.
$ curl -sSf https://install.wal.app | sh -s -- -n testnet

# Update an existing installation (overwrites prior version of walrus).
$ curl -sSf https://install.wal.app | sh -s -- -f
```

Make sure that the `"$HOME"/.local/bin` directory is in your `$PATH`.

After installation completes, you can run Walrus by using the `walrus` command in your terminal.

```console
$ walrus --help
```

## Install on Windows {#windows-install}

To download `walrus` to your Microsoft Windows computer, run the following in PowerShell:

```PowerShell
(New-Object System.Net.WebClient).DownloadFile(
  "https://storage.googleapis.com/mysten-walrus-binaries/walrus-testnet-latest-windows-x86_64.exe",
  "walrus.exe"
)
```

From there, place `walrus.exe` somewhere in your `PATH`.

:::info

Most of the remaining instructions assume a UNIX-based system for the directory structure and commands. If you use Windows, you might need to adapt most of those instructions.

:::

## Install through Cargo

You can also install Walrus through Cargo. For example, to install the latest Mainnet version:

```sh
$ cargo install --git https://github.com/MystenLabs/walrus --branch mainnet walrus-service --locked
```

In place of `--branch mainnet`, you can also specify specific tags (for example, `--tag mainnet-v1.18.2`) or commits (for example, `--rev b2009ac73388705f379ddad48515e1c1503fc8fc`).

## Build from source

Walrus is open source software published under the Apache 2 license. The code is developed in a `git` repository at https://github.com/MystenLabs/walrus.

The latest version of Mainnet and Testnet are available under the `mainnet` and `testnet` branches respectively, and the latest development version under the `main` branch. Reports of issues and bug fixes are welcome. Follow the instructions in the `README.md` file to build and use Walrus from source.

## Configure Walrus

After downloading, the Walrus binary must have a configuration file that defines the development environment parameters. [Learn more about configuring the Walrus client.](/docs/walrus-client/walrus-cli)