{/* https://linear.app/mysten-labs/issue/DOCS-629/getting-startedadvanced-setup */}

This page covers advanced setup options for Walrus, including building from source, installing from binaries, or using Cargo. For standard setup instructions, see the Walrus [Getting Started](/docs/getting-started) guide.

Walrus is open source under an Apache 2 license. You can download and install it through [`suiup`](https://github.com/MystenLabs/suiup) on GitHub, or you can build and install it from the Rust source code through Cargo.

## Walrus binaries

The `walrus` client binary is currently provided for macOS (Intel and Apple CPUs), Ubuntu, and Windows. The Ubuntu version most likely works on other Linux distributions as well.

| OS      | CPU                   | Architecture                                                                                                                 |
| ------- | --------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| Ubuntu  | Intel 64bit           | [`ubuntu-x86_64`](https://storage.googleapis.com/mysten-walrus-binaries/walrus-mainnet-latest-ubuntu-x86_64)                 |
| Ubuntu  | Intel 64bit (generic) | [`ubuntu-x86_64-generic`](https://storage.googleapis.com/mysten-walrus-binaries/walrus-mainnet-latest-ubuntu-x86_64-generic) |
| Ubuntu  | ARM 64bit             | [`ubuntu-aarch64`](https://storage.googleapis.com/mysten-walrus-binaries/walrus-mainnet-latest-ubuntu-aarch64)               |
| MacOS   | Apple Silicon         | [`macos-arm64`](https://storage.googleapis.com/mysten-walrus-binaries/walrus-mainnet-latest-macos-arm64)                     |
| MacOS   | Intel 64bit           | [`macos-x86_64`](https://storage.googleapis.com/mysten-walrus-binaries/walrus-mainnet-latest-macos-x86_64)                   |
| Windows | Intel 64bit           | [`windows-x86_64.exe`](https://storage.googleapis.com/mysten-walrus-binaries/walrus-mainnet-latest-windows-x86_64.exe)       |

:::tip

Our latest Walrus binaries are also available on Walrus itself, namely on https://bin.wal.app, for example, https://bin.wal.app/walrus-mainnet-latest-ubuntu-x86_64. Because of DoS protection, it might not be possible to download the binaries with `curl` or `wget`.

:::

## Install through script {#nix-install}

To download and install `walrus` to your `"$HOME"/.local/bin directory`, run 1 of the following commands in your terminal then follow on-screen instructions. If you are on Windows, see the Windows-specific instructions or the [`suiup` installation](https://github.com/MystenLabs/suiup) on GitHub.

```bash
# Run a first-time install using the latest Mainnet version.
$ curl -sSf https://install.wal.app | sh

# Install the latest Testnet version instead.
$ curl -sSf https://install.wal.app | sh -s -- -n testnet

# Update an existing installation (overwrites prior version of walrus).
$ curl -sSf https://install.wal.app | sh -s -- -f
```

Make sure that the `"$HOME"/.local/bin` directory is in your `$PATH`.

After this is done, you can run Walrus by using the `walrus` command in your terminal.

```console
$ walrus --help
```

## Install on Windows {#windows-install}

To download `walrus` to your Microsoft Windows computer, run the following in a PowerShell.

```PowerShell
(New-Object System.Net.WebClient).DownloadFile(
  "https://storage.googleapis.com/mysten-walrus-binaries/walrus-testnet-latest-windows-x86_64.exe",
  "walrus.exe"
)
```

From there, place `walrus.exe` somewhere in your `PATH`.

:::info

Most of the remaining instructions assume a UNIX-based system for the directory structure, commands, and so on. If you use Windows, you might need to adapt most of those.

:::

## Use GitHub releases

You can find all the releases including release notes on [GitHub](https://github.com/MystenLabs/walrus/releases). Download the archive for your system and extract the `walrus` binary.

## Install through Cargo

You can also install Walrus through Cargo. For example, to install the latest Mainnet version:

```sh
$ cargo install --git https://github.com/MystenLabs/walrus --branch mainnet walrus-service --locked
```

In place of `--branch mainnet`, you can also specify specific tags (for example, `--tag mainnet-v1.18.2`) or commits (for example, `--rev b2009ac73388705f379ddad48515e1c1503fc8fc`).

## Build from source

Walrus is open source software published under the Apache 2 license. The code is developed in a `git` repository at https://github.com/MystenLabs/walrus.

The latest version of Mainnet and Testnet are available under the branches `mainnet` and `testnet` respectively, and the latest version under the `main` branch. Reports of issues and bug fixes are welcome. Follow the instructions in the `README.md` file to build and use Walrus from source.

## Configure Walrus

:::tip

The easiest way to obtain the latest configuration is by downloading it directly from Walrus:

```sh
curl --create-dirs https://docs.wal.app/setup/client_config.yaml -o ~/.config/walrus/client_config.yaml
```

:::

The Walrus client needs to know about the Sui objects that store the Walrus system and staking information, see the [developer guide](/docs/system-overview/core-concepts#system-and-staking-information). Configure these in a file at `~/.config/walrus/client_config.yaml`.

You can access Testnet and Mainnet through the following configuration. This example Walrus CLI configuration refers to the standard location for Sui configuration (`"~/.sui/sui_config/client.yaml"`).

<!-- IMPORT_CONTENT_RESOLVED source="/setup/client_config.yaml" mode="code" -->
```yaml title="setup/client_config.yaml"
contexts:
  mainnet:
    system_object: 0x2134d52768ea07e8c43570ef975eb3e4c27a39fa6396bef985b5abc58d03ddd2
    staking_object: 0x10b9d30c28448939ce6c4d6c6e0ffce4a7f8a4ada8248bdad09ef8b70e4a3904
    n_shards: 1000
    max_epochs_ahead: 53
    wallet_config:
      # Optional path to the wallet config file.
      # path: ~/.sui/sui_config/client.yaml
      # Sui environment to use.
      active_env: mainnet
      # Optional override for the Sui address to use.
      # active_address: 0x0000000000000000000000000000000000000000000000000000000000000000
    rpc_urls:
      - https://fullnode.mainnet.sui.io:443
  testnet:
    system_object: 0x6c2547cbbc38025cf3adac45f63cb0a8d12ecf777cdc75a4971612bf97fdf6af
    staking_object: 0xbe46180321c30aab2f8b3501e24048377287fa708018a5b7c2792b35fe339ee3
    exchange_objects:
      - 0xf4d164ea2def5fe07dc573992a029e010dba09b1a8dcbc44c5c2e79567f39073
      - 0x19825121c52080bb1073662231cfea5c0e4d905fd13e95f21e9a018f2ef41862
      - 0x83b454e524c71f30803f4d6c302a86fb6a39e96cdfb873c2d1e93bc1c26a3bc5
      - 0x8d63209cf8589ce7aef8f262437163c67577ed09f3e636a9d8e0813843fb8bf1
    n_shards: 1000
    max_epochs_ahead: 53
    wallet_config:
      # Optional path to the wallet config file.
      # path: ~/.sui/sui_config/client.yaml
      # Sui environment to use.
      active_env: testnet
      # Optional override for the Sui address to use.
      # active_address: 0x0000000000000000000000000000000000000000000000000000000000000000
    rpc_urls:
      - https://fullnode.testnet.sui.io:443
default_context: testnet
```
<!-- /IMPORT_CONTENT_RESOLVED -->

### Use a custom path (optional) {#config-custom-path}

By default, the Walrus client looks for the `client_config.yaml` (or `client_config.yml`) configuration file in the current directory, `$XDG_CONFIG_HOME/walrus/`, `~/.config/walrus/`, or `~/.walrus/`. However, you can place the file anywhere and name it anything you like. In this case, use the `--config` option when running the `walrus` binary.

## Use advanced configuration (optional)

The configuration file currently supports the following parameters for each of the contexts:

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

There are some additional parameters that you can use to tune the networking behavior of the client. If you experience excessively slow uploads, it might be worth experimenting with these values. There is no risk in playing around with these values. In the worst case, you might not be able to store or read blob because of timeouts or other networking errors.

<summary>
`client_config_example.yaml`
</summary>
<!-- IMPORT_CONTENT_RESOLVED source="/crates/walrus-sdk/client_config_example.yaml" mode="code" -->
```yaml title="crates/walrus-sdk/client_config_example.yaml"
system_object: 0xa2637d13d171b278eadfa8a3fbe8379b5e471e1f3739092e5243da17fc8090eb
staking_object: 0xca7cf321e47a1fc9bfd032abc31b253f5063521fd5b4c431f2cdd3fee1b4ec00
n_shards: 1000
max_epochs_ahead: 53
cache_ttl_secs: 10
exchange_objects:
- 0xa9b00f69d3b033e7b64acff2672b54fbb7c31361954251e235395dea8bd6dcac
- 0x26a8a417b553b18d13027c23e8016c3466b81e7083225436b55143c127f3c0cb
wallet_config: null
rpc_urls:
- https://fullnode.testnet.sui.io:443
communication_config:
  max_concurrent_writes: null
  max_concurrent_sliver_reads: null
  max_concurrent_metadata_reads: 3
  max_concurrent_status_reads: null
  max_data_in_flight: 12500000
  reqwest_config:
    total_timeout_millis: 300000
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
  sliver_status_check_threshold: 5560
  tail_handling: blocking
  data_in_flight_auto_tune:
    enabled: false
    window_sample_target: 20
    window_timeout_millis: 10000
    increase_factor: 2.0
    lock_factor: 1.5
    min_permits: 50
    max_permits: 2000
    secondary_weight: 0.5
    min_blob_size_bytes: 52428800
  upload_mode: balanced
  pending_uploads_enabled: false
  pending_upload_grace_millis: 0
  confirmation_long_poll_millis: 0
  optimistic_upload_max_blob_bytes: 4194304
  registration_delay_millis: 200
  max_total_blob_size: 1073741824
  committee_change_backoff:
    min_backoff_millis: 1000
    max_backoff_millis: 5000
    max_retries: 5
  sui_client_request_timeout_millis: null
refresh_config:
  refresh_grace_period_secs: 10
  max_auto_refresh_interval_secs: 30
  min_auto_refresh_interval_secs: 5
  epoch_change_distance_threshold_secs: 300
  refresher_channel_size: 100
quilt_client_config:
  max_retrieve_slivers_attempts: 2
  timeout_secs: 10
  max_unavailable_slivers_to_recover: 100
byte_range_read_client_config:
  max_retrieve_slivers_attempts: 2
  timeout_secs: 10
  max_unavailable_slivers_to_recover: 100
streaming_config:
  max_sliver_retry_attempts: 5
  sliver_timeout_secs: 30
  prefetch_count: 4
  max_unavailable_slivers_to_recover: 100
```
<!-- /IMPORT_CONTENT_RESOLVED -->