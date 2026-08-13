> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Walrus does not ship an official Python SDK. Python reaches Walrus through the interfaces that are language-agnostic instead: the HTTP API, the CLI, and Sui JSON-RPC. The following examples show all three: storing and reading blobs with plain `requests` calls, driving the Walrus CLI's JSON mode as a subprocess, and querying Sui JSON-RPC to track Walrus storage events.

## Prerequisites

The complete, runnable example files live in the [`docs/examples/python`](https://github.com/MystenLabs/walrus/tree/main/docs/examples/python) directory of the Walrus repository.

- [x] The `walrus` CLI and a funded Sui wallet. See [Getting Started](/docs/getting-started).

- [x] The `requests` HTTP library: `pip install requests`. It is the only third-party dependency.

- [x] Update the constants in [`utils.py`](https://github.com/MystenLabs/walrus/blob/main/docs/examples/python/utils.py) to match your system: `PATH_TO_WALRUS` (the path to your `walrus` binary), `PATH_TO_WALRUS_CONFIG` (the path to your client configuration file), and `FULL_NODE_URL` (a Sui full node RPC URL, for example `https://fullnode.testnet.sui.io:443` for Testnet).

## Use the HTTP API

The HTTP API is the lightest integration path: you store blobs with `PUT` requests to a publisher and read them with `GET` requests to an aggregator, with no Walrus-specific dependencies in your Python code. The example below talks to a local `walrus daemon`, which combines the publisher and aggregator roles on a single address. Start the daemon first, pointing it at your client configuration and an existing directory for its sub-wallets:

```sh
walrus --config <PATH_TO_CLIENT_CONFIG> daemon \
    --bind-address "127.0.0.1:8899" \
    --sub-wallets-dir <SUB_WALLETS_DIR> \
    --n-clients 1
```

The script then uploads 1 MiB of random data with `PUT /v1/blobs?epochs=5` and reads it back with `GET /v1/blobs/<blob_id>`. A first-time store returns a `newlyCreated` JSON object that carries the blob ID; storing data that already exists on Walrus returns an `alreadyCertified` object instead. See [Storing Blobs](/docs/http-api/storing-blobs#understanding-the-response) for both response shapes.

<!-- ImportContent: GitHub source — resolve at export time or visit https://github.com/MystenLabs/walrus/blob/main/docs/examples/python/hello_walrus_webapi.py -->

The script prints the blob ID, the blob size, and the transfer times, similar to the following:

```txt
Blob ID: iIWkkUTzPZx-d1E_A7LqUynnYFD-ztk39_tP8MLdS2Y
Size 1048576 bytes
Upload time: 4.25s
Download time: 0.51s
```

To use public endpoints instead of a local daemon, send `PUT` requests to a Testnet publisher and `GET` requests to an aggregator from the [Network Reference](/docs/network-reference#aggregators-and-publishers). Note that public endpoints limit requests to 10 MiB, and Walrus has no public unauthenticated publisher on Mainnet.

## Use the JSON API

The JSON API runs the `walrus` CLI as a subprocess and exchanges JSON on standard input and output. JSON mode covers every CLI command, which makes it the right path when you need operations the HTTP API does not expose, such as checking a blob's onchain certification. The example passes a JSON object that names the client configuration file and the command to run, for example:

```json
{
  "config": "path/to/client_config.yaml",
  "command": {
    "store": {
      "files": ["some_file.bin"],
      "epochs": 2
    }
  }
}
```

The example stores a 1 MiB file, reads it back, and then checks the blob's certification. Every stored blob has a corresponding Sui object that records its storage lifetime and certification status, so the final part of the script fetches that object through Sui JSON-RPC (`sui_getObject`) and verifies that the onchain `blob_id` matches the uploaded blob.

<!-- ImportContent: GitHub source — resolve at export time or visit https://github.com/MystenLabs/walrus/blob/main/docs/examples/python/hello_walrus_jsonapi.py -->

## Track Walrus events

Walrus coordinates storage through smart contracts on Sui, and blob lifecycle operations emit Sui events such as `BlobRegistered` and `BlobCertified`. Tracking these events lets you monitor activity, for example to confirm certification of your own blobs or to index recent stores. The script needs no `walrus` binary at runtime; it only reads the system object ID from your client configuration file and queries a Sui full node over JSON-RPC.

The script proceeds in three steps:

1. Reads the `system_object` ID from your Walrus client configuration.
2. Fetches that object with `sui_getObject` to discover the Walrus package ID.
3. Queries the latest 100 events emitted by the package's `blob` module with `suix_queryEvents`, then prints each event's timestamp, type, size (for registrations), blob ID, and transaction digest.

<!-- ImportContent: GitHub source — resolve at export time or visit https://github.com/MystenLabs/walrus/blob/main/docs/examples/python/track_walrus_events.py -->