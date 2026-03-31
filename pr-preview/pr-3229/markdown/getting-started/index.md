Walrus is a verifiable data platform for high-stakes systems like AI and onchain finance, where data is stored as blobs.

Walrus uses an object storage architecture, where blobs are stored in a flat namespace rather than a hierarchy. There are no folders or directories. Each piece of data in an object storage model contains the data itself, metadata describing the data, and a unique identifier.

Sui is a blockchain that supports programmability at a [fundamental level](https://docs.sui.io/concepts/transactions/prog-txn-blocks). Walrus binds all blobs to objects on the Sui blockchain.

## Walrus and Sui

Walrus depends on Sui, as it leverages Sui to track blobs, their respective owners, and their lifetimes.

Sui and Walrus are both decentralized, distributed systems made up of many independent servers that communicate and collectively establish shared state. A group of servers together is a network.

### Available networks

Sui and Walrus each have the following available [networks](/docs/system-overview/available-networks):

- **Testnet:** A sandbox-like network where you can receive test tokens for free to use for the network fees. You can build, test, and debug software packages on Testnet. Testnet does not guarantee data persistence and might wipe data at any time without warning.
- **Mainnet:** A production environment where you use real tokens and users or other applications rely on consistent functionality.

When you are getting started, you should use Testnet.

##step Install tooling

:::info

You can use the Walrus HTTP API with a Testnet publisher endpoint to upload and retrieve blobs with no downloads, installation, wallet, or tokens required. [Learn more](/docs/http-api/storing-blobs).

:::

To install Walrus and Sui, use the Mysten Labs `suiup` tool.

Install `suiup`:

```sh
$ curl -sSfL https://raw.githubusercontent.com/Mystenlabs/suiup/main/install.sh | sh
```

Install `sui` and `walrus`:

```sh
$ suiup install sui
$ suiup install walrus
```

##step Configure tooling for Walrus Testnet

After installing Walrus, configure the Walrus client. The client configuration tells Walrus which RPC URLs to use to access Testnet or Mainnet and which Sui objects track the state of the Walrus network. The easiest way to configure Walrus is to download the following pre-filled configuration file.

Download the configuration file:

```sh
$ curl --create-dirs https://docs.wal.app/setup/client_config.yaml -o ~/.config/walrus/client_config.yaml
```

Configure the Sui client to connect to Testnet.

The Sui client configuration is separate from the Walrus client configuration. [Learn more about the Sui client configuration.](https://docs.sui.io/guides/developer/getting-started/configure-sui-client)

Initialize the Sui client:

```sh
$ sui client
```

When prompted, enter the following:

- Connect to a Sui Full Node server? Enter `Y`.
- Full node server URL: Enter `https://fullnode.testnet.sui.io:443`.
- Environment alias: Enter `testnet`.
- Select key scheme: Enter `0` (for ed25519).

This creates your Sui client configuration file with a Testnet environment and generates your first address.

To confirm the Walrus configuration also uses Testnet, run the following command:

```sh
$ walrus info
```

Make sure that the output of this command includes `Epoch duration: 1day` to indicate connection to Testnet.

For detailed information about the `walrus` CLI, use `walrus --help`. Append `--help` to any `walrus` subcommand to get details about that specific command.

##step Understand your Sui account

When you ran `sui client` during setup, the system automatically created a Sui account for you. Sui uses addresses and accounts. When you store blobs on Walrus, Walrus binds them to an object on Sui that an address owns.

An address is a unique location on the blockchain. A 32-byte identifier (displayed as 64 hex characters with a `0x` prefix) identifies the address, which can own objects. The system derives the address from a public key using a hash function.

Anyone can see addresses, and they are valid on all networks (Testnet, Mainnet, and others), but networks do not share data and assets.

An account is an address plus the key to access it. If you have the private key for an address, you have privileged access and control over what the address owns, such as tokens and objects.

To view your active address, run the following command:

```sh
$ sui client active-address
```

To see all your addresses and their key schemes, run the following command:

```sh
$ sui client addresses
```

:::caution Store your keys securely

You must store your private key and recovery passphrase securely, otherwise you might lose access to your address.

[Learn more about addresses, available key pair options, and key storage.](https://docs.sui.io/guides/developer/getting-started/get-address)

:::

#### Creating additional addresses

You can create additional addresses if needed:

```sh
$ sui client new-address ed25519
```

The argument `ed25519` specifies the key pair scheme to be of type ed25519.

##step Fund Sui account with tokens

Before you can upload a file to Walrus and store it as a blob, you need SUI tokens to pay transaction fees and WAL tokens to pay for storage on the network. Walrus Testnet uses Testnet WAL tokens that have no value. You can exchange them at a 1:1 rate for Testnet SUI tokens.

Navigate to the SUI Testnet faucet: https://faucet.sui.io/

Ensure you select Testnet.

Then, insert your Sui address. To print your Sui address, use the following command:

```sh
$ sui client active-address
```

After you insert your address on the faucet and receive a message confirming you received SUI tokens, check your balance with the following command:

```sh
$ sui client balance
```

:::tip Faucet alternatives

The Sui faucet is rate limited. If you encounter errors or have questions, you can request tokens from the Discord faucet or a third-party faucet. [Learn more about the Sui faucet.](https://docs.sui.io/guides/developer/getting-started/get-coins)

:::

Convert some of those SUI tokens into WAL with the following command:

```sh
$ walrus get-wal --context testnet
```

Check your balance again with `sui client balance` to confirm you now have WAL:

```sh
╭─────────────────────────────────────────╮
│ Balance of coins owned by this address  │
├─────────────────────────────────────────┤
│ ╭─────────────────────────────────────╮ │
│ │ coin  balance (raw)     balance     │ │
│ ├─────────────────────────────────────┤ │
│ │ Sui        497664604      0.49 SUI  │ │
│ │ WAL Token  500000000      0.50 WAL  │ │
│ ╰─────────────────────────────────────╯ │
╰─────────────────────────────────────────╯
```

##step Store a blob

Changes to objects on Sui happen through transactions. Accounts sign these transactions on behalf of addresses, and the transactions result in the system creating, updating, transferring, and sometimes destroying objects. Learn more about [transactions](https://docs.sui.io/concepts/transactions).

To upload a file to Walrus and store it as a blob, run the following command:

```sh
$ walrus store file.txt --epochs 2 --context testnet
```

Replace `file.txt` with the file you want to store on Walrus. You can store any file type on Walrus.

You must specify the `--epochs` flag, because the system stores blobs for a certain number of epochs. An epoch is a defined period of time on the network. On Testnet, epochs are 1 day, and on Mainnet, epochs are 2 weeks. You can extend the number of epochs the system stores a blob indefinitely.

The system uploads a blob in slivers, which are small pieces of the file the system stores on different servers through erasure coding. [Learn more](/docs/system-overview/red-stuff) about the Walrus architecture and how the system implements erasure coding.

<!-- IMPORT_CONTENT_RESOLVED source="blob-object-id" mode="snippet" -->
After you upload a blob to Walrus, it has 2 identifiers:

```sh
Blob ID: oehkoh0352bRGNPjuwcy0nye3OLKT649K62imdNAlXg
Sui object ID: 0x1c086e216c4d35bf4c1ea493aea701260ffa5b0070622b17271e4495a030fe83
```

- Blob ID: A way to reference the blob on Walrus. The system generates the blob ID based on the blob's contents, meaning any file you upload to the network twice results in the same blob ID.

- Sui Object ID: The blob's corresponding newly created Sui object identifier, as the system binds all blobs to one or more Sui objects.

You use blob IDs to read blob data, while you use Sui object IDs to make modifications to the blob's metadata, such as its storage duration. You might also use them to read blob data.
<!-- /IMPORT_CONTENT_RESOLVED -->

You can use [Walrus Explorer](https://walruscan.com/) to view more information about a blob ID.

##step Retrieve a blob

To retrieve a blob and save it on your local machine, run the following command:

```sh
$ walrus read <blob-id> --out file.txt --context testnet
```

Replace `<blob-id>` with the blob identifier the `walrus store` command returns in its output, and replace `file.txt` with the name and file extension for storing the file locally.

##step Extend a blob storage duration

To extend a blob storage duration, you must reference the Sui object ID and indicate how many epochs you want to extend the blob storage for.

Run the following command to extend a blob storage duration by 3 epochs. You must use the Sui object ID, not the blob ID:

```sh
$ walrus extend --blob-obj-id <blob-object-id> --epochs-extended 3 --context testnet
```

Replace `<blob-object-id>` with the blob Sui object ID the `walrus store` command returns in its output.

##step Delete a blob

All blobs stored in Walrus are public and discoverable by anyone. The `delete` command does not delete blobs from caches, slivers from past storage nodes, or copies that other users might have made before the blob was deleted.

To delete a blob, run the following command:

```sh
$ walrus delete --blob-id <blob-id> --context testnet
```

Replace `<blob-id>` with the blob identifier the `walrus store` command returns in its output.

## Next steps

[Build your first Walrus application](/docs/getting-started). Explore working examples:

- [Python examples](https://github.com/MystenLabs/walrus/tree/main/docs/examples/python)
- [JavaScript web form](https://github.com/MystenLabs/walrus/tree/main/docs/examples/javascript)
- [Move smart contracts](https://github.com/MystenLabs/walrus/tree/main/docs/examples/move)

## Need help?

- [Troubleshooting guide](/docs/troubleshooting)
- [Discord community](https://discord.com/invite/walrusprotocol)