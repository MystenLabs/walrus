# Setup

This page covers the full setup for both Walrus Mainnet and Testnet. For a faster, beginner-friendly introduction, see the [Quick Start Guide](./quickstart.md).

```admonish tip title="New to Walrus? Start with Testnet"
**We strongly recommend starting with Testnet.** It's free, requires no real tokens, and is perfect for learning and testing. Once you're comfortable, you can move to Mainnet for production use.

- **Testnet:** Free tokens, 1-day epochs, perfect for testing
- **Mainnet:** Production network, requires purchasing WAL tokens, 2-week epochs

Follow the [Quick Start Guide](./quickstart.md) for the fastest path to storing your first blob on Testnet.
```

This chapter describes the [installation](#installation), [prerequisites](#prerequisites), and
[configuration](#configuration) of the Walrus client. We provide pre-compiled binaries for macOS (Intel and Apple CPUs), Ubuntu, and Windows.

Walrus is open-source under an Apache 2 license and can also be built from source via Cargo.

## Step 1: Installation {#installation}

Install the Walrus CLI first, then set up your wallet and configuration.

We provide the `walrus` client binary for macOS, Ubuntu (works on most Linux distributions), and Windows.

### Choose your network

**For Testnet (recommended for beginners):**

```sh
curl -sSf https://install.wal.app | sh -s -- -n testnet
```

**For Mainnet (production use):**

```sh
curl -sSf https://install.wal.app | sh
```

**For Windows:** See the [Windows installation instructions](#windows-install) below.

```admonish tip title="What does this do?"
This script downloads the `walrus` binary and places it in `~/.local/bin`. Make sure this directory is in your PATH.

To update an existing installation, add the `-f` flag: `curl -sSf https://install.wal.app | sh -s -- -f`
```

**Verify the installation:**

```sh
walrus --version
```

You should see output like:

```terminal
$ walrus --version
walrus v1.x.x
```

You can also run:

```sh
walrus --help
```

To see all available commands. See [Interacting with Walrus](./interacting.md) for details on using the CLI.

### Alternative installation methods

<details>
<summary>Windows installation</summary>

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

</details>

<details>
<summary>Install via suiup (experimental)</summary>

### Install via suiup (experimental) {#suiup-install}

`suiup` is a tool to install and manage different versions of CLI tools for working in the Sui
ecosystem, including the `walrus` CLI. After installing `suiup` as described in the [`suiup`
documentation](https://github.com/MystenLabs/suiup?tab=readme-ov-file#installation), you can install
specific versions of the `walrus` CLI:

```sh
suiup install walrus@testnet # install the latest testnet release
suiup install walrus@mainnet # install the latest mainnet release
suiup install walrus@testnet-v1.27.1 # install a specific release
```

</details>

<details>
<summary>Build from source or use Cargo</summary>

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

</details>

## Step 2: Set Up Your Wallet {#prerequisites}

```admonish info title="Why do I need a Sui wallet?"
Walrus uses the Sui blockchain for coordination and payments:
- **SUI tokens** pay for blockchain transaction fees (gas)
- **WAL tokens** pay for storage on Walrus
- Your wallet securely stores your tokens and identity

Think of your Sui wallet as your account for Walrus.
```

You have two options:

### Option A: Quick wallet generation (easiest)

Generate a new Sui wallet specifically for Walrus:

**For Testnet:**
```sh
walrus generate-sui-wallet --sui-network testnet
```

**For Mainnet:**
```sh
walrus generate-sui-wallet --sui-network mainnet
```

```admonish warning title="Save your recovery phrase!"
The command will display a recovery phrase. Write this down and store it safely! Without it, you cannot recover your wallet if you lose access.
```

### Option B: Use the Sui CLI (for advanced users)

If you want more control or already use Sui, install the [Sui CLI](https://docs.sui.io/guides/developer/getting-started/sui-install).

After installation, set up your wallet:

**For Testnet:**
```sh
sui client new-env --alias testnet --rpc https://fullnode.testnet.sui.io:443
sui client switch --env testnet
```

**For Mainnet:**
```sh
sui client new-env --alias mainnet --rpc https://fullnode.mainnet.sui.io:443
sui client switch --env mainnet
```

Verify your environment is active:

```sh
sui client envs
```

You should see a `*` next to your chosen network (testnet or mainnet).

### Get tokens

Now you need tokens to use Walrus.

#### For Testnet (free):

1. **Get SUI tokens from the faucet:**

   Find your address:
   ```sh
   sui client active-address
   ```

   Visit the [Sui Testnet Faucet](https://faucet.sui.io/?network=testnet) and enter your address.

2. **Verify you received SUI:**
   ```sh
   sui client balance
   ```

   You should see at least 1 SUI.

3. **Exchange SUI for WAL:**
   ```sh
   walrus get-wal
   ```

   This exchanges 0.5 SUI for 0.5 WAL (both are free test tokens).

4. **Verify you have both tokens:**
   ```sh
   sui client balance
   ```

   You should now see both SUI and WAL in your balance.

#### For Mainnet (requires purchase):

1. **Get SUI tokens:** Purchase SUI from a cryptocurrency exchange.

2. **Transfer SUI to your wallet:** Use the address from `sui client active-address`.

3. **Get WAL tokens:** Purchase WAL from a centralized or decentralized exchange.

4. **Verify your balance:**
   ```sh
   sui client balance
   ```

   You should see both SUI and WAL tokens.

```admonish tip title="How much do I need?"
- **For Testnet:** The faucet provides enough to get started
- **For Mainnet:** Start with at least 1 SUI for gas and enough WAL for your storage needs. Check current prices with `walrus info`.
```

## Step 3: Configuration {#configuration}

Walrus needs a configuration file that tells it which network to connect to (Testnet or Mainnet) and where your Sui wallet is located.

### Create the configuration

```sh
# Create the directory (this command is safe to run even if it exists)
mkdir -p ~/.config/walrus

# Download the configuration file
curl https://docs.wal.app/setup/client_config.yaml -o ~/.config/walrus/client_config.yaml
```

```admonish success title="That's it!"
The downloaded configuration file includes settings for both Testnet and Mainnet. Walrus will automatically use the correct network based on which Sui environment is active in your wallet.
```

### Verify your configuration

Check that Walrus can connect to the network:

```sh
walrus info
```

You should see information about the Walrus network, including:

```terminal
$ walrus info

Walrus system information

Epochs and storage duration
Current epoch: 42
...
```

If you see this, you're ready to use Walrus!

### Understanding the configuration file

The configuration file contains network information for both Testnet and Mainnet:

```yaml
{{ #include ../setup/client_config.yaml }}
```

```admonish tip title="Multiple networks"
The config file has two contexts: `mainnet` and `testnet`. Walrus automatically uses the one matching your active Sui environment. To switch networks, just change your Sui environment:

\`\`\`sh
sui client switch --env testnet  # or mainnet
\`\`\`
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

There are some additional parameters that can be used to tune the networking behavior of the client,
see the [full example client configuration](../setup/client_config_example.yaml). If you experience
excessively slow uploads, it may be worth experimenting with these values. There is no risk in
playing around with these values; in the worst case, you may not be able to store/read blob due to
timeouts or other networking errors.
