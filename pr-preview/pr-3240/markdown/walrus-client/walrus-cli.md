Use the command-line interface (CLI) to interact with the Walrus client. The CLI is available by installing the `walrus` binary. To install Walrus, use the Mysten Labs [`suiup` tool](https://github.com/MystenLabs/suiup?tab=readme-ov-file#installation):
```sh
$ curl -sSfL https://raw.githubusercontent.com/Mystenlabs/suiup/main/install.sh | sh
```

Then install `sui` and `walrus`:
```sh
$ suiup install sui
$ suiup install walrus
```

View detailed usage information including a full list of available commands using the following command:
```sh
$ walrus --help
```

Each subcommand of `walrus` can also be called with `--help` to print its specific arguments and their meaning.

### Switching contexts

If you have multiple contexts in your configuration file, you can specify the context for each command using the `--context` option. Generate a `bash`, `zsh`, or `fish` completion script with `walrus completion` and place it in an appropriate directory like `~/.local/share/bash-completion/completions`.

## Configuration

The Walrus client needs to know about the Sui objects that store the Walrus system and staking information. Configure these in the `client_config.yaml` file.

By default, the Walrus client looks for the `client_config.yaml` (or `client_config.yml`) configuration file in the current directory, `$XDG_CONFIG_HOME/walrus/`, `~/.config/walrus/`, or `~/.walrus/`.

Obtain the latest configuration file by downloading it directly from Walrus and placing it in one of the default configuration file locations:
```sh
$ curl --create-dirs https://docs.wal.app/setup/client_config.yaml -o ~/.config/walrus/client_config.yaml
```

You can place the file anywhere and name it anything you like. In that case, use the `--config` option when running the `walrus` binary.

### Specify a wallet

Use the `--wallet <WALLET>` argument to specify a non-standard Sui wallet configuration file. The wallet configuration is taken from the path specified in the Walrus configuration, `./sui_config.yaml`, or `~/.sui/sui_config/client.yaml`.

### Set a gas budget

Use the `--gas-budget <GAS_BUDGET>` argument to change the maximum amount of Sui (in MIST) that the command is allowed to use. If not specified, the gas budget is estimated automatically.

### Print output as JSON

Use the `--json` flag to write a command's output as JSON. This is the default in [JSON mode](/docs/walrus-client/json-mode).

### Example

You can access Testnet and Mainnet through the following configuration. This example Walrus CLI configuration refers to the standard location for Sui configuration (`~/.sui/sui_config/client.yaml`).

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

## Logging and metrics

The `walrus` CLI supports multiple levels of logging, which you can toggle through an environment variable:
```sh
$ RUST_LOG=walrus=trace walrus info
```

By default, `info` level logs are enabled. The `debug` and `trace` levels can give a more in-depth understanding of what a command does or how it fails.