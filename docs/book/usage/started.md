# Getting Started with Walrus

Walrus is a platform for building efficient and resilient data markets, where data is stored as *blobs*. Walrus stores all blobs as an array of bytes with a fixed size, and you can store any type of file, such as text, video, or source code.

Sui is a blockchain that supports programmability at a [fundamental level](https://docs.sui.io/concepts/transactions/prog-txn-blocks). Walrus binds all blobs to *objects* on the Sui blockchain.

## Walrus and Sui

Walrus depends on Sui, as it leverages Sui to track blobs, their respective owners, and their lifetimes.

Sui and Walrus are both decentralized, distributed systems made up of many independent servers that communicate and collectively establish shared state. A group of servers together is a *network*.

### Available networks

Sui and Walrus each have the following available networks:

- **Testnet** is a sandbox-like network where you can receive test tokens for free to use for the network's fees. You can build, test, and debug software packages on Testnet. Testnet does not guarantee data persistence and might wipe data at any time without warning.
- **Mainnet** is a production environment where you use real tokens and users or other applications rely on consistent functionality.

When you are getting started, you should use **Testnet.**

## Install tooling

To install Walrus and Sui, use the Mysten Labs `suiup` tool. First, [install suiup](https://github.com/MystenLabs/suiup?tab=readme-ov-file#installation):

```bash
curl -sSfL https://raw.githubusercontent.com/Mystenlabs/suiup/main/install.sh | sh
```

Then, install `sui` and `walrus`:

```bash
suiup install sui
suiup install walrus
```

### Configure tooling for Walrus Testnet

After you install Walrus, you need to configure the Walrus client, which tells it the RPC URLs to use to access Testnet or Mainnet. The easiest way to configure Walrus is to download the following prefilled configuration file.

```bash
mkdir ~/.config/walrus/
curl https://manansh11.github.io/walrus-docs/setup/client_config_testnet.yaml -o ~/.config/walrus/client_config.yaml
```

Next, you need to configure the Sui client to connect to Testnet. The Sui client configuration is separate from the Walrus client configuration. [Learn more about the Sui client configuration.](https://docs.sui.io/guides/developer/getting-started/connect#configure-sui-client)

Initialize the Sui client:

```bash
sui client
```

When prompted, enter the following:

- Connect to a Sui Full Node server? → `Y`
- Full node server URL → `https://fullnode.testnet.sui.io:443`
- Environment alias → `testnet`
- Select key scheme → `0` (for ed25519)

This creates your Sui client configuration file with a Testnet environment and generates your first address.

To confirm the Walrus configuration also uses Testnet, run the command:

```bash
walrus info
```

Make sure that this command's output includes `Epoch duration: 1day` to indicate connection to Testnet.

For detailed information about the `walrus` command-line tool, use the `walrus --help` command. Or, append `--help` to any `walrus` subcommand to get details about that specific command.

## Understanding your Sui account

When you ran `sui client` during setup, the system automatically created a Sui account for you. Sui uses addresses and accounts. When you store blobs on Walrus, Walrus binds them to an object on Sui that an address owns.

An *address* is a unique location on the blockchain. A 32-byte identifier (displayed as 64 hex characters with `0x` prefix) identifies the address, which can own objects. The system derives the address from a public key using a hash function.

Anyone can see addresses, and they are valid on all networks (Testnet, Mainnet, and so on), but networks do not share data and assets.

An *account* is an address plus the key to access it. If you have an address's private key, you can actually use what the address owns, such as tokens and objects.

To view your active address, run:

```bash
sui client active-address
```

To see all your addresses and their key schemes, run:

```bash
sui client addresses
```

> **Warning: Store your keys securely**
>
> You must store your private key and recovery passphrase securely, otherwise you might lose access to your address.
>
> [Learn more about addresses, available key pair options, and key storage.](https://docs.sui.io/guides/developer/getting-started/get-address)

### Creating additional addresses (Optional)

You can create additional addresses if needed:
```bash
sui client new-address ed25519
```

The argument `ed25519` specifies the key pair scheme to be of type `ed25519`.

## Fund Sui account with tokens

Before you can upload a file to Walrus and store it as a blob, you need SUI tokens to pay transaction fees and WAL tokens to pay for storage on the network. The Walrus Testnet uses Testnet WAL tokens that have no value and you can exchange them at a 1:1 rate for Testnet SUI tokens.

To get SUI tokens, navigate to the SUI Testnet faucet:

[https://faucet.sui.io/](https://faucet.sui.io/)

Ensure you select Testnet. Then, insert your Sui address. To print your Sui address, use the command:

```bash
sui client active-address
```

After you insert your address on the faucet and receive a message saying you have received SUI tokens, check your balance with the command:

```bash
sui client balance
```

> **Tip: Faucet alternatives**
>
> The Sui faucet is rate limited. If you encounter errors or have questions, you can request tokens from the Discord faucet or a third party faucet. [Learn more about the Sui faucet.](https://docs.sui.io/guides/developer/getting-started/get-coins#request-test-tokens-through-discord)

Now, convert some of those SUI tokens into WAL with the command:

```bash
walrus get-wal --context testnet
```

Then, check your balance again with `sui client balance` to confirm you now have WAL:

```bash
╭─────────────────────────────────────────╮
│ Balance of coins owned by this address  │
├─────────────────────────────────────────┤
│ ╭─────────────────────────────────────╮ │
│ │ coin  balance (raw)     balance     │ │
│ ├─────────────────────────────────────┤ │
│ │ Sui   8869252670        0.05 SUI    │ │
│ │ WAL   500000000         0.50 WAL    │ │
│ ╰─────────────────────────────────────╯ │
╰─────────────────────────────────────────╯
```

## Store a blob

Changes to objects on Sui happen through the use of transactions. Accounts sign these transactions on behalf of addresses and they result in the system creating, updating, transferring, and sometimes destroying objects. Learn more about [transactions](https://docs.sui.io/concepts/transactions).

To upload a file to Walrus and store it as a blob, run the following command:

```bash
walrus store file.txt --epochs 2 --context testnet
```

Replace `file.txt` with the file you want to store on Walrus. You can store any file type on Walrus.

You must specify the `--epochs` flag, as the system stores blobs for a certain number of epochs. An *epoch* is a certain period of time on the network. On Testnet, epochs are 1 day, and on Mainnet epochs are 2 weeks. You can extend the number of epochs the system stores a blob indefinitely.

The system uploads a blob in *slivers*, which are small pieces of the file the system stores on different servers through *erasure coding*. [Learn more](https://docs.wal.app/design/encoding.html) about the Walrus architecture and how the system implements erasure coding.

After you upload a blob to Walrus, it has 2 identifiers:

```bash
Blob ID: oehkoh0352bRGNPjuwcy0nye3OLKT649K62imNdAlXg
Sui object ID: 0x1c086e216c4d35bf4c1ea493aea701260ffa5b0070622b17271e4495a030fe83
```

- Blob ID: A way to reference the blob on Walrus. The system generates the blob ID based on the blob's contents, meaning any file you upload to the network twice results in the same blob ID.
- Sui Object ID: The blob's corresponding newly created Sui object identifier, as the system binds all blobs to one or more Sui objects.

You use blob IDs to read blob data, while you use Sui object IDs to make modifications to the blob's metadata, such as its storage duration. You might also use them to read blob data.

You can use the [Walrus Explorer](https://walruscan.com/) to view more information about a blob ID.

## Retrieve a blob

To retrieve a blob and save it on your local machine, run the following command:

```bash
walrus read <blob-id> --out file.txt --context testnet
```

Replace `<blob-id>` with the blob's identifier the `walrus store` command returns in its output, and replace `file.txt` with the name and file extension for storing the file locally.

## Extend a blob's storage duration

To extend a blob's storage duration, you must reference the Sui object ID and indicate how many epochs you want to extend the blob's storage for.

Run the following command to extend a blob's storage duration by 3 epochs. You must use the Sui object ID, not the blob ID:

```bash
walrus extend --blob-obj-id <blob-object-id> --epochs-extended 3 --context testnet
```

Replace `<blob-object-id>` with the blob's Sui object ID the `walrus store` command returns in its output.

## Delete a blob

To delete a blob, run the following command:

```bash
walrus delete --blob-id <blob-id> --context testnet
```

Replace `<blob-id>` with the blob's identifier the `walrus store` command returns in its output.

## Next steps

1. [Build your first Walrus application](https://docs.wal.app/dev-guide/dev-guide.html). Explore working examples:
   - [Python examples](https://github.com/MystenLabs/walrus/tree/main/docs/examples/python)
   - [JavaScript web form](https://github.com/MystenLabs/walrus/tree/main/docs/examples/javascript)
   - [Move smart contracts](https://github.com/MystenLabs/walrus/tree/main/docs/examples/move)

## Need help?

- [Troubleshooting guide](https://docs.wal.app/usage/troubleshooting.html)
- [Discord community](https://discord.gg/walrus)
