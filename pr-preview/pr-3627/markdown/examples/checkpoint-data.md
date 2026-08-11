> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

The Sui Archival application demonstrates how you can archive blockchain data on Walrus in a reliable, deterministic, and resilient manner. The service continuously downloads Sui checkpoints, the sequential batches of finalized transactions that the Sui network produces, bundles them into compressed blobs, and uploads the blobs to Walrus. Because Walrus erasure-codes every blob across a large committee of storage nodes, the archive stays readable even when individual nodes fail.

## How it works

Walrus stores data as blobs, immutable units of data that storage nodes keep available for a paid number of epochs (fixed-length periods: 2 weeks on Mainnet, 1 day on Testnet). The archival service runs the following components concurrently:

1. **Checkpoint downloader:** Downloads checkpoints from a configurable checkpoint source, such as the Sui data ingestion service, using multiple parallel workers.
2. **Checkpoint monitor:** Tracks downloaded checkpoints, handles out-of-order delivery, and triggers blob building when the pending checkpoints hit a configured threshold: total size, elapsed time, or the end of a Sui epoch. A backpressure mechanism pauses the downloader when too many checkpoints await bundling.
3. **Checkpoint blob publisher:** Bundles a contiguous range of checkpoints into a single compressed blob and uploads it to Walrus. The bundling follows a deterministic algorithm: a given checkpoint range, cut at the configured size, time, or end-of-epoch boundaries, always produces the same blob.
4. **Checkpoint blob extender:** Watches the expiration epoch of every archived blob and extends it automatically, so the archive stays available without manual renewals.
5. **Archival state snapshot creator:** Uploads snapshots of the archive's metadata and maintains an onchain pointer to the latest snapshot, so a fresh instance can rebuild its database for disaster recovery.
6. **REST API server:** Serves the web interface and the JSON endpoints described below.

The service records blob metadata, that is, the Walrus blob ID, the Sui object ID, the checkpoint range, and the size of each archived blob, in a local RocksDB database, and can optionally mirror it to PostgreSQL for fast indexed queries.

## Build and run the service

- [x] A recent Rust toolchain, because the service builds with Cargo.

- [x] A Walrus client configuration and a wallet funded with SUI and WAL, because the service pays for the storage it uses. See [Getting Started](/docs/getting-started).

- [x] Optional: a PostgreSQL database if you want indexed queries alongside the local RocksDB store.

Then build and run the service:

1. Clone the repository and build all crates:

   ```sh
   git clone https://github.com/MystenLabs/walrus-sui-archival.git
   cd walrus-sui-archival
   cargo build --release
   ```

2. Review the example configuration in `config/testnet_local_config.yaml`. The configuration file selects the checkpoint source, the blob building thresholds, the path to your Walrus client configuration, the local database path, the REST API bind address (default `0.0.0.0:9185`), and the optional PostgreSQL connection URL.

3. Run the archival service with your configuration:

   ```sh
   cargo run --release -p walrus-sui-archival -- run --config config/testnet_local_config.yaml
   ```

4. Open the web interface at `http://localhost:9185` to view archival statistics, browse archived blobs, and look up checkpoints.

## Query the API

The REST API server exposes JSON endpoints alongside the web pages:

| **Endpoint** | **Description** |
|---|---|
| `GET /v1/health` | Health check that returns `200 OK`. |
| `GET /v1/checkpoint?checkpoint=<number>` | Returns the metadata of the blob that contains the given checkpoint. Add `show_content=true` to include the full checkpoint data. |
| `GET /v1/blobs` | Lists all archived blobs with their metadata. |

For example, to find which Walrus blob holds checkpoint `12345` on a local instance:

```sh
curl "http://localhost:9185/v1/checkpoint?checkpoint=12345"
```

The service also provides inspection subcommands to examine its local database and archived blobs:

```sh
# List all archived blobs recorded in the local database
cargo run --release -p walrus-sui-archival -- inspect-db --db-path archival_db list-blobs

# Fetch a blob from Walrus by blob ID and inspect its bundled checkpoints
cargo run --release -p walrus-sui-archival -- inspect-blob \
  --blob-id "<BLOB_ID>" \
  --client-config config/local_testnet_client_config.yaml \
  --context testnet
```

## Main archival code

The following code drives the main archival functionality. It wires together the downloader, monitor, publisher, extender, snapshot creator, and REST API server described above:

<!-- ImportContent: GitHub source — resolve at export time or visit https://github.com/MystenLabs/walrus-sui-archival/blob/main/crates/walrus-sui-archival/src/archival.rs -->

[View the application's full code on GitHub](https://github.com/MystenLabs/walrus-sui-archival).