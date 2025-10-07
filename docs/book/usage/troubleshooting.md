# Troubleshooting

This guide covers the most common issues you might encounter when using Walrus and how to resolve them.

```admonish tip title="Debug logging"
Enable detailed logging to diagnose issues: `RUST_LOG=walrus=debug walrus <command>`

For configuration details: `RUST_LOG=info walrus <command>` shows which config file and wallet are being used.
```

## Common Errors

These are the most frequent issues users encounter, with typical resolution times of 15-45 minutes for configuration problems.

### "command not found: walrus"

**Problem:** The `walrus` binary is not in your PATH.

**Solution:**

1. Check if `~/.local/bin` is in your PATH:
   ```sh
   echo $PATH | grep ".local/bin"
   ```

2. If not found, add it to your shell configuration file (`~/.bashrc`, `~/.zshrc`, etc.):
   ```sh
   export PATH="$HOME/.local/bin:$PATH"
   ```

3. Reload your shell:
   ```sh
   source ~/.bashrc  # or ~/.zshrc
   ```

4. Verify installation:
   ```sh
   walrus --version
   ```

### "Network not recognized" or "not on testnet"

**Problem:** Your Sui wallet is on a different network than your Walrus configuration expects.

**Solution:**

1. Check your active Sui environment:
   ```sh
   sui client envs
   ```
   Look for the `*` to see which network is active.

2. Check which context your Walrus config uses. The config file at `~/.config/walrus/client_config.yaml` has a `default_context` that should match your Sui environment.

3. Switch your Sui wallet to match:
   ```sh
   sui client switch --env testnet  # or mainnet
   ```

**Common scenario:** Config says `default_context: mainnet` but wallet is on testnet, or vice versa.

### "Insufficient SUI balance" or "Insufficient WAL balance"

**Problem:** You don't have enough tokens for the operation.

**Solution for Testnet:**

1. Get SUI from the faucet:
   ```sh
   sui client active-address  # Get your address
   ```
   Visit [https://faucet.sui.io/?network=testnet](https://faucet.sui.io/?network=testnet)

2. Exchange SUI for WAL:
   ```sh
   walrus get-wal
   ```

3. Verify your balances:
   ```sh
   sui client balance
   ```

**Solution for Mainnet:** Purchase more SUI or WAL from an exchange.

### "the specified Walrus system object does not exist"

**Problem:** Your wallet is on the wrong Sui network, or your Walrus configuration is outdated.

**Solution:**

1. Make sure your Sui wallet matches the network you intend to use:
   ```sh
   sui client envs
   ```

2. Update your Walrus configuration:
   ```sh
   curl https://docs.wal.app/setup/client_config.yaml -o ~/.config/walrus/client_config.yaml
   ```

3. Verify the configuration:
   ```sh
   walrus info
   ```

### Storage command shows progress then fails

**Problem:** Usually means you ran out of WAL tokens during the upload, or network issues occurred.

**Common causes:**
- Insufficient WAL balance
- Network connectivity problems
- Blob too large for available storage

**Solution:**

1. Check your WAL balance:
   ```sh
   sui client balance
   ```

2. For Testnet, get more WAL:
   ```sh
   walrus get-wal
   ```

3. Check the blob size and compare to your available storage:
   ```sh
   walrus info  # Shows storage prices
   ```

4. Try again with sufficient tokens.

### "could not retrieve enough confirmations to certify the blob"

**Problem:** You're using an outdated Walrus configuration pointing to an inactive system.

**Solution:**

1. Download the latest configuration:
   ```sh
   curl https://docs.wal.app/setup/client_config.yaml -o ~/.config/walrus/client_config.yaml
   ```

2. Update to the latest Walrus binary:
   ```sh
   curl -sSf https://install.wal.app | sh -s -- -f
   ```

3. Verify it works:
   ```sh
   walrus info
   ```

### "Illegal instruction (core dumped)"

**Problem:** The standard binary is incompatible with your CPU or virtual machine.

**Solution:** Use the generic x86-64 binary:

```sh
curl -sSf https://install.wal.app | sh -s -- --generic
```

Or download manually:
[ubuntu-x86_64-generic binary](https://storage.googleapis.com/mysten-walrus-binaries/walrus-mainnet-latest-ubuntu-x86_64-generic)

### Can't find Blob ID or Object ID

**Problem:** You didn't save the IDs when storing the blob.

**Solution:**

1. List all your blobs:
   ```sh
   walrus list-blobs
   ```

2. If you have the file, derive the Blob ID:
   ```sh
   walrus blob-id <FILE>
   ```

### Configuration directory doesn't exist

**Problem:** The `~/.config/walrus/` directory hasn't been created yet.

**Solution:**

```sh
mkdir -p ~/.config/walrus
curl https://docs.wal.app/setup/client_config.yaml -o ~/.config/walrus/client_config.yaml
```

## General Troubleshooting Steps

### 1. Check your version

Always use the latest binary:

```sh
walrus --version
which walrus  # Make sure you're running the right one
```

Update if needed:
```sh
curl -sSf https://install.wal.app | sh -s -- -f
```

### 2. Verify your configuration

Check what configuration is being used:

```sh
RUST_LOG=info walrus info
```

This shows:
- Path to Walrus config file
- Path to Sui wallet
- Active network

### 3. Check network connectivity

Test your connection to Sui:

```sh
sui client gas
```

If this fails, you have connectivity issues with Sui, not Walrus.

### 4. Verify wallet state

```sh
sui client active-address  # Your address
sui client balance         # Your token balances
sui client envs            # Your configured networks
```
