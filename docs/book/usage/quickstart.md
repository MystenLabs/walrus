# Quick Start Guide

This guide will get you up and running with Walrus in about 10 minutes. You'll store your first blob on Walrus Testnet, which is free and perfect for testing.

```admonish tip title="What you'll accomplish"
By the end of this guide, you will:
1. Have a working Walrus installation
2. Have a configured wallet with test tokens
3. Successfully store and retrieve a blob on Walrus
```

```admonish info title="New to Walrus?"
**Walrus** is a decentralized storage network. Think of it like cloud storage, but your data is distributed across many nodes for high availability and reliability.

**What's the relationship between Sui and Walrus?**
- Walrus uses the Sui blockchain for coordination and payments
- You need a Sui wallet to interact with Walrus
- You'll need two types of tokens: SUI (for blockchain transactions) and WAL (for storage)
- Don't worry - on Testnet, both are free!
```

## Step 1: Install Walrus (2 minutes)

Choose your operating system:

### macOS or Linux

```sh
curl -sSf https://install.wal.app | sh -s -- -n testnet
```

The installer will download Walrus and place it in `~/.local/bin`. Make sure this directory is in your PATH.

**Verify the installation:**

```sh
walrus --version
```

You should see output like: `walrus v1.x.x`

### Windows

In PowerShell:

```PowerShell
(New-Object System.Net.WebClient).DownloadFile(
  "https://storage.googleapis.com/mysten-walrus-binaries/walrus-testnet-latest-windows-x86_64.exe",
  "walrus.exe"
)
```

Place `walrus.exe` in a directory in your PATH.

## Step 2: Set Up Your Wallet (3 minutes)

You need a Sui wallet to use Walrus. Here's the quickest way to get started:

```sh
walrus generate-sui-wallet --sui-network testnet
```

**What just happened?**
This command created a new Sui wallet for you and saved it to `~/.sui/sui_config/client.yaml`.

**Important:** You'll see a recovery phrase displayed. Save this somewhere safe if you plan to use this wallet later!

```admonish tip title="Already have a Sui wallet?"
If you already have a Sui wallet configured, you can skip the wallet generation. Just make sure it's connected to Testnet:
\`\`\`sh
sui client switch --env testnet
\`\`\`
```

## Step 3: Get Test Tokens (2 minutes)

You need two types of tokens:

### Get SUI tokens (for transaction fees)

Visit the [Sui Testnet Faucet](https://faucet.sui.io/?network=testnet) and enter your wallet address.

**To find your wallet address:**

```sh
sui client active-address
```

**Verify you received SUI:**

```sh
sui client balance
```

You should see at least 1 SUI in your balance.

### Get WAL tokens (for storage payments)

WAL is the Walrus storage token. On Testnet, you can exchange your free SUI for free WAL:

```sh
walrus get-wal
```

**Verify you received WAL:**

```sh
sui client balance
```

You should now see both SUI and WAL tokens in your balance. If the command fails, make sure you're on Testnet (see the tip in Step 2).

## Step 4: Configure Walrus (1 minute)

Walrus needs a configuration file. Let's create it:

```sh
# Create the config directory
mkdir -p ~/.config/walrus

# Download the Testnet configuration
curl https://docs.wal.app/setup/client_config.yaml -o ~/.config/walrus/client_config.yaml
```

**What does this config file do?**
It tells Walrus which network to connect to (Testnet) and where to find the Sui blockchain information.

**Verify your configuration:**

```sh
walrus info
```

You should see information about the Walrus network, including the current epoch and storage prices.

## Step 5: Store Your First Blob (2 minutes)

Let's create a simple text file and store it on Walrus:

```sh
# Create a test file
echo "Hello, Walrus!" > hello.txt

# Store it on Walrus for 1 epoch (1 day on Testnet)
walrus store hello.txt --epochs 1
```

**What you'll see:**

```
Storing file: hello.txt
File size: 14 bytes
...
Successfully stored blob!
Blob ID: BFkH3kXJF6K1...
Object ID: 0x1234abcd...
```

**Save your Blob ID!** You'll need it to retrieve your file.

```admonish tip title="Understanding epochs"
An **epoch** is Walrus's unit of time:
- Testnet: 1 epoch = 1 day
- Mainnet: 1 epoch = 2 weeks

When you store a blob for "1 epoch," it will be available until the next epoch begins. For testing, 1-5 epochs is usually sufficient.
```

## Step 6: Retrieve Your Blob

Now let's prove your data is stored by downloading it:

```sh
walrus read <YOUR_BLOB_ID> --out downloaded.txt
```

Replace `<YOUR_BLOB_ID>` with the Blob ID you received in Step 5.

**Verify it worked:**

```sh
cat downloaded.txt
```

You should see: `Hello, Walrus!`

Congratulations! You just stored and retrieved your first blob on Walrus.

## What's Next?

Now that you have Walrus working, you can:

- **Learn more about the CLI:** See [Using the Walrus client](./client-cli.md) for all available commands
- **Try larger files:** Store images, videos, or any other files
- **Use the HTTP API:** Build applications that interact with Walrus via HTTP, see [Using the client HTTP API](./web-api.md)
- **Move to Mainnet:** When you're ready for production, see [Setup](./setup.md) for Mainnet configuration

## Troubleshooting

### "command not found: walrus"

The `~/.local/bin` directory is not in your PATH. Add this to your `~/.bashrc`, `~/.zshrc`, or equivalent:

```sh
export PATH="$HOME/.local/bin:$PATH"
```

Then restart your terminal or run `source ~/.bashrc` (or equivalent).

### "default_context: testnet" but command uses mainnet

Your Walrus configuration is set to mainnet but your Sui wallet is on testnet. Either:
1. Switch your Sui wallet to mainnet: `sui client switch --env mainnet`
2. Or update `~/.config/walrus/client_config.yaml` to set `default_context: testnet`

### "Network not recognized" when getting WAL

Make sure your Sui wallet is connected to Testnet:

```sh
sui client envs
```

Look for a `*` next to `testnet`. If not, run:

```sh
sui client switch --env testnet
```

### "Insufficient SUI balance"

You need at least 0.1 SUI for transactions. Visit the [faucet](https://faucet.sui.io/?network=testnet) again.

### Storage command fails after showing progress

This usually means you don't have enough WAL tokens. Run:

```sh
walrus get-wal
```

For more help, see the [Troubleshooting page](./troubleshooting.md).
