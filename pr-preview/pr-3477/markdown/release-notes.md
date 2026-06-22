> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

# Release Notes

Release notes from [Walrus](https://github.com/MystenLabs/walrus/releases), [Walrus Memory](https://github.com/MystenLabs/MemWal/releases), and the [Walrus blog](/blog).

---

## Walrus Platform

### Walrus v1.50.0

**Testnet** | June 3, 2026 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.50.0)

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/testnet-v1.50.0

---

### Walrus v1.49.1

**Mainnet** | May 27, 2026 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.49.1)

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/mainnet-v1.49.1

---

### Walrus v1.48.1

**Mainnet** | May 13, 2026 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.48.1)

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/mainnet-v1.48.1

---

### Walrus v1.47.1

**Mainnet** | April 29, 2026 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.47.1)

#### Storage node

[#3256](https://github.com/MystenLabs/walrus/pull/3256): enable storage pool support in storage node

#### Aggregator

[#3284](https://github.com/MystenLabs/walrus/pull/3284): The `GET /v1/blobs/{blob_id}` endpoint now returns
  HTTP 503 with reason `BLOB_UNAVAILABLE` when a blob is temporarily
  unretrievable (for example, mid-upload, mid-expiry, or when too many
  storage nodes are unreachable to reconstruct the blob or determine a
  definitive status). Previously these cases returned HTTP 500. The
  terminal 404 `BLOB_NOT_FOUND` response for blobs that do not exist
  is unchanged. Clients and CDNs should treat 503 as retryable with
  backoff.

[#3084](https://github.com/MystenLabs/walrus/pull/3084): Blob attribute headers (such as Content-Type) are now correctly included on range request responses when reading blobs by object ID.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/mainnet-v1.47.1

---

### Walrus v1.47.0

**Testnet** | April 22, 2026 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.47.0)

#### Storage node

[#3256](https://github.com/MystenLabs/walrus/pull/3256): enable storage pool support in storage node

#### Aggregator

[#3284](https://github.com/MystenLabs/walrus/pull/3284): The `GET /v1/blobs/{blob_id}` endpoint now returns
  HTTP 503 with reason `BLOB_UNAVAILABLE` when a blob is temporarily
  unretrievable (for example, mid-upload, mid-expiry, or when too many
  storage nodes are unreachable to reconstruct the blob or determine a
  definitive status). Previously these cases returned HTTP 500. The
  terminal 404 `BLOB_NOT_FOUND` response for blobs that do not exist
  is unchanged. Clients and CDNs should treat 503 as retryable with
  backoff.

[#3084](https://github.com/MystenLabs/walrus/pull/3084): Blob attribute headers (such as Content-Type) are now correctly included on range request responses when reading blobs by object ID.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/testnet-v1.47.0

---

### Walrus v1.46.1

**Mainnet** | April 15, 2026 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.46.1)

#### Publisher

[#3202](https://github.com/MystenLabs/walrus/pull/3202): allow publisher and daemon to add `reuse_resources` query parameters to try to reuse existing (unfinished) upload pipeline

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/mainnet-v1.46.1

---

### Walrus v1.45.2

**Mainnet** | April 1, 2026 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.45.2)

#### Storage node

[#3155](https://github.com/MystenLabs/walrus/pull/3155): Increases the number of RocksDB background threads from 2 (RocksDB default) to 16. Can be configured through the config file.

[#3160](https://github.com/MystenLabs/walrus/pull/3160): Add a new configuration parameter `garbage_collection.enable_immediate_data_deletion`, which defaults to `false`. When processing a `BlobDeleted` event, data is only deleted immediately if this parameter is set to true.

#### Publisher

[#3072](https://github.com/MystenLabs/walrus/pull/3072): Fixed body size limit in combined daemon mode (running publisher and aggregator together). The limit was set 1024x too small, causing uploads to be silently rejected with 413 Payload Too Large. Standalone publisher mode was not affected.

#### CLI

[#3122](https://github.com/MystenLabs/walrus/pull/3122): Added a new `info` subcommand to the CLI. `walrus info coin` provides the StructTag of the walrus coin given the CLI's configured context.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/mainnet-v1.45.2

---

### Walrus v1.45.1

**Testnet** | March 25, 2026 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.45.1)

#### Storage node

[#3155](https://github.com/MystenLabs/walrus/pull/3155): Increases the number of RocksDB background threads from 2 (RocksDB default) to 16. Can be configured through the config file.

[#3160](https://github.com/MystenLabs/walrus/pull/3160): Add a new configuration parameter `garbage_collection.enable_immediate_data_deletion`, which defaults to `false`. When processing a `BlobDeleted` event, data is only deleted immediately if this parameter is set to true.

#### Publisher

[#3072](https://github.com/MystenLabs/walrus/pull/3072): Fixed body size limit in combined daemon mode (running publisher and aggregator together). The limit was set 1024x too small, causing uploads to be silently rejected with 413 Payload Too Large. Standalone publisher mode was not affected.

#### CLI

[#3122](https://github.com/MystenLabs/walrus/pull/3122): Added a new `info` subcommand to the CLI. `walrus info coin` provides the StructTag of the walrus coin given the CLI's configured context.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/testnet-v1.45.1

---

### Walrus v1.44.3

**Mainnet** | March 19, 2026 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.44.3)

#### Storage node

[#3005](https://github.com/MystenLabs/walrus/pull/3005): Fixed an issue that crashed storage nodes due to outdated epoch state.

[#3156](https://github.com/MystenLabs/walrus/pull/3156): Increases the number of RocksDB background threads from 2 (RocksDB default) to 16. Can be configured through the config file.

[#3161](https://github.com/MystenLabs/walrus/pull/3161): Add a new configuration parameter `garbage_collection.enable_immediate_data_deletion`, which defaults to `false`. When processing a `BlobDeleted` event, data is only deleted immediately if this parameter is set to true.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/mainnet-v1.44.3

---

### Walrus v1.44.2

**Mainnet** | March 19, 2026 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.44.2)

#### Storage node

[#3005](https://github.com/MystenLabs/walrus/pull/3005): Fixed an issue that crashed storage nodes due to outdated epoch state.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/mainnet-v1.44.2

---

### Walrus v1.44.1

**Testnet** | March 11, 2026 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.44.1)

#### Storage node

[#3005](https://github.com/MystenLabs/walrus/pull/3005): Fixed an issue that crashed storage nodes due to outdated epoch state.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/testnet-v1.44.1

---

### Walrus v1.43.1

**Mainnet** | March 4, 2026 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.43.1)

#### Storage node

[#2960](https://github.com/MystenLabs/walrus/pull/2960): allow storage nodes to vote on pricing based on USD. This functionality can only be used after the contract supports this.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/mainnet-v1.43.1

---

### Walrus v1.42.1

**Mainnet** | February 18, 2026 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.42.1)

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/mainnet-v1.42.1

---

### Walrus v1.41.1

**Mainnet** | February 5, 2026 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.41.1)

#### Storage node

[#2851](https://github.com/MystenLabs/walrus/pull/2851): Garbage collection is now enabled by default on Mainnet.

#### Aggregator

[#2801](https://github.com/MystenLabs/walrus/pull/2801): There is a new alpha endpoint `/v1alpha/blobs/{blob_id}/stream` which is intended to reduce latency to first byte, and is intended to be used to stream blob data.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/mainnet-v1.41.1

---

### Walrus v1.41.0

**Testnet** | January 28, 2026 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.41.0)

#### Storage node

[#2851](https://github.com/MystenLabs/walrus/pull/2851): Garbage collection is now enabled by default on Mainnet.

#### Aggregator

[#2801](https://github.com/MystenLabs/walrus/pull/2801): There is a new alpha endpoint `/v1alpha/blobs/{blob_id}/stream` which is intended to reduce latency to first byte, and is intended to be used to stream blob data.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/testnet-v1.41.0

---

### Walrus v1.40.3

**Mainnet** | January 21, 2026 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.40.3)

#### Storage node

[#2772](https://github.com/MystenLabs/walrus/pull/2772): Enables DB transactions (implemented in [#247](https://github.com/MystenLabs/walrus/pull/247)5) and garbage collection (implemented in [#2542](https://github.com/MystenLabs/walrus/pull/2542), [#2599](https://github.com/MystenLabs/walrus/pull/2599), and [#272](https://github.com/MystenLabs/walrus/pull/272)5) by default on Testnet. The features can be disabled by adding the following to your node configuration:
  ```yaml
  db_config:
    global:
      use_optimistic_transaction_db: false
  garbage_collection:
    enable_blob_info_cleanup: false
    enable_data_deletion: false
  ```
  If you have `experimental_use_optimistic_transaction_db: true` in your configuration, you should remove that.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/mainnet-v1.40.3

---

### Walrus v1.39.1

**Mainnet** | January 7, 2026 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.39.1)

#### Storage node

[#2730](https://github.com/MystenLabs/walrus/pull/2730):
Storage nodes will need to remove the `use_legacy_event_provider` flag from their configuration if it is present.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/mainnet-v1.39.1

---

### Walrus v1.38.3

**Mainnet** | December 10, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.38.3)

#### Storage node

[#2715](https://github.com/MystenLabs/walrus/pull/2715):
remove deprecated get_recovery_symbol endpoint, which has not been used for several months

#### Aggregator

[#2687](https://github.com/MystenLabs/walrus/pull/2687):
Significant reduction in memory usage when reading blobs with strict consistency check.

[#2676](https://github.com/MystenLabs/walrus/pull/2676):
Includes a new endpoint `/v1alpha/blobs/concat` handling `GET` and `POST` methods. This endpoint streams the given blobs back as one concatenated response. This is an alpha endpoint and is not yet stable. We're looking for feedback!

[#2724](https://github.com/MystenLabs/walrus/pull/2724):
introduce a new aggregator API to read byte ranges of a blob

#### Publisher

[#2687](https://github.com/MystenLabs/walrus/pull/2687):
Significant reduction in memory usage and slight performance improvements when storing large blobs.

[#2717](https://github.com/MystenLabs/walrus/pull/2717):
Reduce number of Sui RPC requests and slightly decrease upload latency.

#### CLI

[#2687](https://github.com/MystenLabs/walrus/pull/2687):
Significant reduction in memory usage and slight performance improvements when storing blobs, computing the blob ID, and reading blobs with strict consistency check.

[#2710](https://github.com/MystenLabs/walrus/pull/2710):
Add optional static parameters `n_shards` and `max_epochs_ahead` to the `client_config.yaml` to reduce CLI start-up time. You can obtain the latest version of the config file at http://docs.wal.app/setup/client_config.yaml.

[#2717](https://github.com/MystenLabs/walrus/pull/2717):
Reduce number of Sui RPC requests and decrease latency when storing blobs.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/mainnet-v1.38.3

---

### Walrus v1.38.2

**Testnet** | December 9, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.38.2)

#### Storage node

[#2715](https://github.com/MystenLabs/walrus/pull/2715):
remove deprecated get_recovery_symbol endpoint, which has not been used for several months

#### Aggregator

[#2687](https://github.com/MystenLabs/walrus/pull/2687):
Significant reduction in memory usage when reading blobs with strict consistency check.

[#2676](https://github.com/MystenLabs/walrus/pull/2676):
Includes a new endpoint `/v1alpha/blobs/concat` handling `GET` and `POST` methods. This endpoint streams the given blobs back as one concatenated response. This is an alpha endpoint and is not yet stable. We're looking for feedback!

[#2724](https://github.com/MystenLabs/walrus/pull/2724):
introduce a new aggregator API to read byte ranges of a blob

#### Publisher

[#2687](https://github.com/MystenLabs/walrus/pull/2687):
Significant reduction in memory usage and slight performance improvements when storing large blobs.

[#2717](https://github.com/MystenLabs/walrus/pull/2717):
Reduce number of Sui RPC requests and slightly decrease upload latency.

#### CLI

[#2687](https://github.com/MystenLabs/walrus/pull/2687):
Significant reduction in memory usage and slight performance improvements when storing blobs, computing the blob ID, and reading blobs with strict consistency check.

[#2710](https://github.com/MystenLabs/walrus/pull/2710):
Add optional static parameters `n_shards` and `max_epochs_ahead` to the `client_config.yaml` to reduce CLI start-up time. You can obtain the latest version of the config file at http://docs.wal.app/setup/client_config.yaml.

[#2717](https://github.com/MystenLabs/walrus/pull/2717):
Reduce number of Sui RPC requests and decrease latency when storing blobs.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/testnet-v1.38.2

---

### Walrus v1.38.1

**Testnet** | December 3, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.38.1)

#### Storage node

[#2715](https://github.com/MystenLabs/walrus/pull/2715):
remove deprecated get_recovery_symbol endpoint, which has not been used for several months

#### Aggregator

[#2687](https://github.com/MystenLabs/walrus/pull/2687):
Significant reduction in memory usage when reading blobs with strict consistency check.

[#2676](https://github.com/MystenLabs/walrus/pull/2676):
Includes a new endpoint `/v1alpha/blobs/concat` handling `GET` and `POST` methods. This endpoint streams the given blobs back as one concatenated response. This is an alpha endpoint and is not yet stable. We're looking for feedback!

[#2724](https://github.com/MystenLabs/walrus/pull/2724):
introduce a new aggregator API to read byte ranges of a blob

#### Publisher

[#2687](https://github.com/MystenLabs/walrus/pull/2687):
Significant reduction in memory usage and slight performance improvements when storing large blobs.

[#2717](https://github.com/MystenLabs/walrus/pull/2717):
Reduce number of Sui RPC requests and slightly decrease upload latency.

#### CLI

[#2687](https://github.com/MystenLabs/walrus/pull/2687):
Significant reduction in memory usage and slight performance improvements when storing blobs, computing the blob ID, and reading blobs with strict consistency check.

[#2710](https://github.com/MystenLabs/walrus/pull/2710):
Add optional static parameters `n_shards` and `max_epochs_ahead` to the `client_config.yaml` to reduce CLI start-up time. You can obtain the latest version of the config file at http://docs.wal.app/setup/client_config.yaml.

[#2717](https://github.com/MystenLabs/walrus/pull/2717):
Reduce number of Sui RPC requests and decrease latency when storing blobs.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/testnet-v1.38.1

---

### Walrus v1.37.0

**Mainnet** | November 19, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.37.0)

#### Aggregator

[#2624](https://github.com/MystenLabs/walrus/pull/2624):
**Warning:** The default consistency check changes slightly in this release (`v1.37`). The previous "strict" consistency check can be enabled by using the query option `strict_consistency_check=true`. See the [encoding documentation](https://docs.wal.app/design/encoding.html#data-integrity-and-consistency) for the details of the different consistency checks.

#### CLI

[#2624](https://github.com/MystenLabs/walrus/pull/2624):
**Warning:** The default consistency check changes slightly in this release (`v1.37`). The previous "strict" consistency check can be enabled by using the CLI option `--strict-consistency-check`. See the [encoding documentation](https://docs.wal.app/design/encoding.html#data-integrity-and-consistency) for the details of the different consistency checks.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/mainnet-v1.37.0

---

### Walrus v1.36.1

**Mainnet** | November 7, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.36.1)

#### Storage node

[#2576](https://github.com/MystenLabs/walrus/pull/2576):
The `http_client_request_duration_*` metrics now omit the `server_address` and `server_port` labels for fast, successful responses.

#### Aggregator

[#2622](https://github.com/MystenLabs/walrus/pull/2622):
Add a new query option `skip_consistency_check=true` to skip blob consistency checks. This should only be used if the writer of the corresponding blob is trusted. See the [encoding documentation](https://docs.wal.app/design/encoding.html#data-integrity-and-consistency) for the details of the different consistency checks.

[#2576](https://github.com/MystenLabs/walrus/pull/2576):
The `http_client_request_duration_*` metrics now omit the `server_address` and `server_port` labels for fast, successful responses.

#### Publisher

[#2576](https://github.com/MystenLabs/walrus/pull/2576):
The `http_client_request_duration_*` metrics now omit the `server_address` and `server_port` labels for fast, successful responses.

#### CLI

[#2622](https://github.com/MystenLabs/walrus/pull/2622):
Add a new CLI option `--skip-consistency-check`.  to skip blob consistency checks. This should only be used if the writer of the corresponding blob is trusted. See the [encoding documentation](https://docs.wal.app/design/encoding.html#data-integrity-and-consistency) for the details of the different consistency checks.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/mainnet-v1.36.1

---

### Walrus v1.35.2

**Mainnet** | October 29, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.35.2)

#### Storage node

[#2576](https://github.com/MystenLabs/walrus/pull/2576):
The `http_client_request_duration_*` metrics now omit the `server_address` and `server_port` labels for fast, successful responses.

#### Aggregator

[#2622](https://github.com/MystenLabs/walrus/pull/2622):
Add a new query option `skip_consistency_check=true` to skip blob consistency checks. This should only be used if the writer of the corresponding blob is trusted. See the [encoding documentation](https://docs.wal.app/design/encoding.html#data-integrity-and-consistency) for the details of the different consistency checks.

[#2576](https://github.com/MystenLabs/walrus/pull/2576):
The `http_client_request_duration_*` metrics now omit the `server_address` and `server_port` labels for fast, successful responses.

#### Publisher

[#2576](https://github.com/MystenLabs/walrus/pull/2576):
The `http_client_request_duration_*` metrics now omit the `server_address` and `server_port` labels for fast, successful responses.

#### CLI

[#2622](https://github.com/MystenLabs/walrus/pull/2622):
Add a new CLI option `--skip-consistency-check`.  to skip blob consistency checks. This should only be used if the writer of the corresponding blob is trusted. See the [encoding documentation](https://docs.wal.app/design/encoding.html#data-integrity-and-consistency) for the details of the different consistency checks.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/mainnet-v1.35.2

---

### Walrus v1.35.1

**Mainnet** | October 23, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.35.1)

#### Aggregator

[#2549](https://github.com/MystenLabs/walrus/pull/2549):
Major reduction in memory usage for large blobs.

[#2592](https://github.com/MystenLabs/walrus/pull/2592):
**Warning:** The default consistency check will change slightly in `v1.37`. The current strict consistency check can be enabled by the new query option `strict_consistency_check=true`. See the [encoding documentation](https://docs.wal.app/design/encoding.html#data-integrity-and-consistency) for the details of the different consistency checks.

#### CLI

[#2549](https://github.com/MystenLabs/walrus/pull/2549):
Major reduction in memory usage when reading large blobs.

[#2561](https://github.com/MystenLabs/walrus/pull/2561):
Added range of slivers for store-quilt command in the output.

[#2592](https://github.com/MystenLabs/walrus/pull/2592):
**Warning:** The default consistency check will change slightly in `v1.37`. The current strict consistency check can be enabled by the new CLI option `--strict-consistency-check`. See the [encoding documentation](https://docs.wal.app/design/encoding.html#data-integrity-and-consistency) for the details of the different consistency checks.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/mainnet-v1.35.1

---

### Walrus v1.35.0

**Testnet** | October 16, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.35.0)

#### Aggregator

[#2549](https://github.com/MystenLabs/walrus/pull/2549):
Major reduction in memory usage for large blobs.

[#2592](https://github.com/MystenLabs/walrus/pull/2592):
**Warning:** The default consistency check will change slightly in `v1.37`. The current strict consistency check can be enabled by the new query option `strict_consistency_check=true`. See the [encoding documentation](https://docs.wal.app/design/encoding.html#data-integrity-and-consistency) for the details of the different consistency checks.

#### CLI

[#2549](https://github.com/MystenLabs/walrus/pull/2549):
Major reduction in memory usage when reading large blobs.

[#2561](https://github.com/MystenLabs/walrus/pull/2561):
Added range of slivers for store-quilt command in the output.

[#2592](https://github.com/MystenLabs/walrus/pull/2592):
**Warning:** The default consistency check will change slightly in `v1.37`. The current strict consistency check can be enabled by the new CLI option `--strict-consistency-check`. See the [encoding documentation](https://docs.wal.app/design/encoding.html#data-integrity-and-consistency) for the details of the different consistency checks.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/testnet-v1.35.0

---

### Walrus v1.34.2

**Mainnet** | October 9, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.34.2)

#### Aggregator

[#2554](https://github.com/MystenLabs/walrus/pull/2554):
Adds a `--max-blob-size` option to limit the size of blobs that can be requested through the aggregator.

#### Publisher

[#2444](https://github.com/MystenLabs/walrus/pull/2444):
**Warning:** Starting with this release (`v1.33`), blobs are stored as deletable by default; if you want to keep the previous behavior, you need to add the `permanent=true` query parameter.

#### CLI

[#2444](https://github.com/MystenLabs/walrus/pull/2444):
**Warning:** Starting with this release (`v1.33`), blobs are stored as deletable by default; if you want to keep the previous behavior, you need to add the `--permanent` option when storing blobs.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/mainnet-v1.34.2

---

### Walrus v1.34.1

**Testnet** | October 1, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.34.1)

#### Aggregator

[#2554](https://github.com/MystenLabs/walrus/pull/2554):
Adds a `--max-blob-size` option to limit the size of blobs that can be requested through the aggregator.

#### Publisher

[#2444](https://github.com/MystenLabs/walrus/pull/2444):
**Warning:** Starting with this release (`v1.33`), blobs are stored as deletable by default; if you want to keep the previous behavior, you need to add the `permanent=true` query parameter.

#### CLI

[#2444](https://github.com/MystenLabs/walrus/pull/2444):
**Warning:** Starting with this release (`v1.33`), blobs are stored as deletable by default; if you want to keep the previous behavior, you need to add the `--permanent` option when storing blobs.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/testnet-v1.34.1

---

### Walrus v1.33.3

**Mainnet** | September 29, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.33.3)

#### Aggregator

[#2554](https://github.com/MystenLabs/walrus/pull/2554):
Adds a `--max-blob-size` option to limit the size of blobs that can be requested through the aggregator.

#### Publisher && CLI
[#2523](https://github.com/MystenLabs/walrus/pull/2523) & [#2444](https://github.com/MystenLabs/walrus/pull/2444):
**Warning:** Starting with this release (`v1.33`), blobs are stored as deletable by default; if you want to keep the previous behavior, you need to add the `permanent=true` query parameter.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/mainnet-v1.33.3

---

### Walrus v1.33.2

**Testnet** | September 25, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.33.2)

#### Publisher

[#2523](https://github.com/MystenLabs/walrus/pull/2523): **Warning:** Starting with this release (`v1.33`), blobs are stored as deletable by default; if you want to keep the previous behavior, you need to add the `permanent=true` query parameter.

#### CLI

[#2523](https://github.com/MystenLabs/walrus/pull/2523): **Warning:** Starting with this release (`v1.33`), blobs are stored as deletable by default; if you want to keep the previous behavior, you need to add the `--permanent` option when storing blobs.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/testnet-v1.33.2

---

### Walrus v1.33.1

**Testnet** | September 17, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.33.1)

#### Publisher

[#2523](https://github.com/MystenLabs/walrus/pull/2523):
**Warning:** Starting with this release (`v1.33`), blobs are stored as deletable by default; if you want to keep the previous behavior, you need to add the `permanent=true` query parameter.

#### CLI

[#2523](https://github.com/MystenLabs/walrus/pull/2523):
**Warning:** Starting with this release (`v1.33`), blobs are stored as deletable by default; if you want to keep the previous behavior, you need to add the `--permanent` option when storing blobs.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/testnet-v1.33.1

---

### Walrus v1.32.0

**Mainnet** | September 15, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.32.0)

#### Aggregator

[#2405](https://github.com/MystenLabs/walrus/pull/2405): Added new endpoint in aggregator:  /quilts/\&lt;quilt-id\&gt;/patches to list all patches in a quilt.

#### CLI

[#2427](https://github.com/MystenLabs/walrus/pull/2427): *Note: This change is an internal code quality improvement and does not affect user-facing functionality. No release notes are required as the changes do not impact the external API or user experience.*

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/mainnet-v1.32.0

---

### Walrus v1.31.1

**Mainnet** | August 28, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.31.1)

#### Publisher & CLI

[#2424](https://github.com/MystenLabs/walrus/pull/2424):
**Warning:** In `v1.33`, blobs will be stored as deletable by default; if you want to keep the current behavior, you should already start to add the `permanent=true` query parameter.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/mainnet-v1.31.1

---

### Walrus v1.30.1

**Mainnet** | August 14, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.30.1)

#### Storage node

[#2360](https://github.com/MystenLabs/walrus/pull/2360): When the TLS certificate is updated, only restart the node if the currently loaded certificate is about to expire or the subject or extensions have changed. Also, add warning and error logs for (almost) expired certificates.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/mainnet-v1.30.1

---

### Walrus v1.30.0

**Testnet** | August 6, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.30.0)

#### Storage node

[#2360](https://github.com/MystenLabs/walrus/pull/2360): When the TLS certificate is updated, only restart the node if the currently loaded certificate is about to expire or the subject or extensions have changed. Also, add warning and error logs for (almost) expired certificates.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/testnet-v1.30.0

---

### Walrus v1.29.2

**Mainnet** | July 31, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.29.2)

#### Storage node

[#2273](https://github.com/MystenLabs/walrus/pull/2273): Fixes an issue that can occur when a node starts with deleted `events` and `event_blob_writer` DBs.

[#2193](https://github.com/MystenLabs/walrus/pull/2193): New storage nodes can start in epochs later than `MAX_EPOCHS_AHEAD` (53 on the current Testnet and Mainnet) without any additional action.

[#2290](https://github.com/MystenLabs/walrus/pull/2290): Add a new configuration parameter for the event processor config to control the interval at which sampled logs are emitted, with a default of 1 hour (previously 30s).

#### Aggregator

[#2222](https://github.com/MystenLabs/walrus/pull/2222): 
- Added `/v1/blobs/by-quilt-patch-id/{quilt_patch_id}` endpoint to read a blob from a quilt by its `QuiltPatchId`
- Added `/v1/blobs/by-quilt-id/{quilt_id}/{identifier}` endpoint to read a blob from a quilt by its `quilt_id and identifier`

#### Publisher

[#2281](https://github.com/MystenLabs/walrus/pull/2281): Add a `permanent` query parameter. **Warning:** In the future, blobs will be stored as deletable by default; to keep the current behavior, you need to add the `permanent=true` query parameter.

[#2279](https://github.com/MystenLabs/walrus/pull/2279): Added "/v1/quilts" publisher endpoint to store quilts.

#### CLI

[#2197](https://github.com/MystenLabs/walrus/pull/2197):
- Added walrus store-quilt command, to store multiple blobs as a single Quilt to Walrus
- Added walrus read-quilt command to read one or more blobs in a Quilt from Walrus
- Added walrus list-patches-in-quilt to check the blobs in a quilt

[#2151](https://github.com/MystenLabs/walrus/pull/2151): use subsidies contract to proxy register blob call

[#2281](https://github.com/MystenLabs/walrus/pull/2281): Add a `--permanent` option to the `store` and `store-quilt` commands. **Warning:** In the future, blobs will be stored as deletable by default; to keep the current behavior, you need to add the `--permanent` option when storing blobs.

[#2384](https://github.com/MystenLabs/walrus/pull/2384): Add the `--upload-relay` option to the `store` and `store-quilt` commands to allow uploading to Walrus via an [Upload Relay](https://docs.wal.app/operator-guide/upload-relay.html).

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/mainnet-v1.29.2

---

### Walrus v1.29.1

**Testnet** | July 23, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.29.1)

📣📣📣
As part of this release we are introducing Quilt - a batch storage solution with an intuitive API, designed to make storing and accessing large numbers of small files on Walrus both incredibly convenient, efficient, and cost-effective. For more details please see https://www.walrus.xyz/blog/introducing-quilt.

#### Storage node

[#2273](https://github.com/MystenLabs/walrus/pull/2273): Fixes an issue that can occur when a node starts with deleted `events` and `event_blob_writer` DBs.

[#2193](https://github.com/MystenLabs/walrus/pull/2193): New storage nodes can start in epochs later than `MAX_EPOCHS_AHEAD` (53 on the current Testnet and Mainnet) without any additional action.

[#2290](https://github.com/MystenLabs/walrus/pull/2290): Add a new configuration parameter for the event processor config to control the interval at which sampled logs are emitted with a default of 1 hour (previously 30s).

#### Aggregator

[#2222](https://github.com/MystenLabs/walrus/pull/2222):
- Added `/v1/blobs/by-quilt-patch-id/{quilt_patch_id}` endpoint to read a blob from a quilt by its `QuiltPatchId`
- Added `/v1/blobs/by-quilt-id/{quilt_id}/{identifier}` endpoint to read a blob from a quilt by its `quilt_id and identifier`

#### Publisher

[#2281](https://github.com/MystenLabs/walrus/pull/2281): Add a `permanent` query parameter. **Warning:** In the future, blobs will be stored as deletable by default; to keep the current behavior, you need to add the `permanent=true` query parameter.

[#2279](https://github.com/MystenLabs/walrus/pull/2279): Added "/v1/quilts" publisher endpoint to store quilts.

#### CLI

[#2197](https://github.com/MystenLabs/walrus/pull/2197):
- Added walrus store-quilt command, to store multiple blobs as a single Quilt to Walrus
- Added walrus read-quilt command to read one or more blobs in a Quilt from Walrus
- Added walrus list-patches-in-quilt to check the blobs in a quilt

[#2151](https://github.com/MystenLabs/walrus/pull/2151): Use subsidies contract to proxy register blob call

[#2281](https://github.com/MystenLabs/walrus/pull/2281): Add a `--permanent` option to the `store` and `store-quilt` commands. **Warning:** In the future, blobs will be stored as deletable by default; to keep the current behavior, you need to add the `--permanent` option when storing blobs.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/testnet-v1.29.1

---

### Walrus v1.28.4

**Mainnet** | July 15, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.28.4)

#### Aggregator

[#2205](https://github.com/MystenLabs/walrus/pull/2205):
Fixes an issue where a blob was sometimes reported as non-existing immediately after certification.

#### CLI

[#2205](https://github.com/MystenLabs/walrus/pull/2205):
Fixes an issue where a blob was sometimes reported as non-existing immediately after certification.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/mainnet-v1.28.4

---

### Walrus v1.28.3

**Testnet** | July 11, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.28.3)

#### Aggregator and CLI

[#2205](https://github.com/MystenLabs/walrus/pull/2205): Fixes an issue where a blob was sometimes reported as non-existent immediately after certification.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/testnet-v1.28.3

---

### Walrus v1.28.2

**Testnet** | July 10, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.28.2)

#### Aggregator

[#2205](https://github.com/MystenLabs/walrus/pull/2205):
Fixes an issue where a blob was sometimes reported as non-existing immediately after certification.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/testnet-v1.28.2

---

### Walrus v1.28.1

**Testnet** | July 2, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.28.1)

#### Storage node

[#2274](https://github.com/MystenLabs/walrus/pull/2274):
Fixes an issue that can occur when a node starts with deleted `events` and `event_blob_writer` DBs.

[#2274](https://github.com/MystenLabs/walrus/pull/2274):
Fixes an issue that can occur when a node starts with deleted `events` and `event_blob_writer` DBs.

[#2274](https://github.com/MystenLabs/walrus/pull/2274):
Fixes an issue that can occur when a node starts with deleted `events` and `event_blob_writer` DBs.

[#2274](https://github.com/MystenLabs/walrus/pull/2274):
Fixes an issue that can occur when a node starts with deleted `events` and `event_blob_writer` DBs.

[#2274](https://github.com/MystenLabs/walrus/pull/2274):
Fixes an issue that can occur when a node starts with deleted `events` and `event_blob_writer` DBs.

#### Aggregator

[#2205](https://github.com/MystenLabs/walrus/pull/2205):
Fixes an issue where a blob was sometimes reported as non-existing immediately after certification.

#### CLI

[#2205](https://github.com/MystenLabs/walrus/pull/2205):
Fixes an issue where a blob was sometimes reported as non-existing immediately after certification.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/testnet-v1.28.1

---

### Walrus v1.27.2

**Mainnet** | June 26, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.27.2)

#### Storage node

[#2020](https://github.com/MystenLabs/walrus/pull/2020):
* Added a  `CheckpointManager` component, which provides
    * Periodic checkpoint of the main database
    * Interface for manual checkpoint
* Introduced a  `node admin socket` so that operators can interact directly with a running node
    * Added Checkpoint commands in walrus-node command tool

[#2179](https://github.com/MystenLabs/walrus/pull/2179):
Fixes an issue where recovering nodes crash after being restarted.

[#2188](https://github.com/MystenLabs/walrus/pull/2188):
Fixes a bug in computing checkpoint lag that sometimes resulted in very high values.

#### CLI

[#2140](https://github.com/MystenLabs/walrus/pull/2140):
Adds node-admin command to compute package digest

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/mainnet-v1.27.2

---

### Walrus v1.27.1

**Testnet** | June 18, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.27.1)

#### Storage node

[#2020](https://github.com/MystenLabs/walrus/pull/2020):
* Added a  `CheckpointManager` component, which provides
    * Periodic checkpoint of the main database
    * Interface for manual checkpoint
* Introduced a  `node admin socket` so that operators can interact directly with a running node
    * Added Checkpoint commands in walrus-node command tool

[#2179](https://github.com/MystenLabs/walrus/pull/2179):
Fixes an issue where recovering nodes crash after being restarted.

[#2188](https://github.com/MystenLabs/walrus/pull/2188):
Fixes a bug in computing checkpoint lag that sometimes resulted in very high values.

#### CLI

[#2140](https://github.com/MystenLabs/walrus/pull/2140):
Adds node-admin command to compute package digest

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/testnet-v1.27.1

---

### Walrus v1.26.4

**Mainnet** | June 10, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.26.4)

#### Storage node

[#1630](https://github.com/MystenLabs/walrus/pull/1630):
Use the metrics and REST API address from the configuration file instead of always binding to `0.0.0.0`. Note that this bugfix may break some setups that relied on the previous incorrect behavior. To get the old behavior, set the `metrics_address` and `rest_api_address` to `0.0.0.0:<port>` explicitly.

[#2072](https://github.com/MystenLabs/walrus/pull/2072):
Increase default value of `http2_max_pending_accept_reset_streams` parameter to disable warnings from the `h2` crate.

[#2067](https://github.com/MystenLabs/walrus/pull/2067):
Reduce verbosity levels of problems with metrics and config sync from ERROR to WARN.

[#2182](https://github.com/MystenLabs/walrus/pull/2182):
Fixes an issue where recovering nodes crash after being restarted.

#### Aggregator

[#2066](https://github.com/MystenLabs/walrus/pull/2066):
Allow specifying Sui RPC URL in the configuration file in addition to the CLI option.

[#1630](https://github.com/MystenLabs/walrus/pull/1630):
Use the metrics address specified through the CLI options file instead of always binding to `0.0.0.0`. Note that this bugfix may break some setups that relied on the previous incorrect behavior. To get the old behavior, use `--metrics-address 0.0.0.0:<port>` when starting the aggregator.

#### Publisher

[#1630](https://github.com/MystenLabs/walrus/pull/1630):
Use the metrics address specified through the CLI options file instead of always binding to `0.0.0.0`. Note that this bugfix may break some setups that relied on the previous incorrect behavior. To get the old behavior, use `--metrics-address 0.0.0.0:<port>` when starting the aggregator.

#### CLI

[#2066](https://github.com/MystenLabs/walrus/pull/2066):
Add a new `rpc_urls` field to the client config, which allows setting multiple Sui RPC URLs. The Sui RPC URL(s) used for reads are now selected in the following order: (1) CLI argument, (2) client configuration file, (3) wallet config. If none of these are set, the client will **no longer fall back to the `fullnode.testnet.sui.io`** but instead exit with an error.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/mainnet-v1.26.4

---

### Walrus v1.26.3

**Mainnet** | June 5, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.26.3)

#### Storage node

[#2159](https://github.com/MystenLabs/walrus/pull/2159): feat: rate limit concurrent blob recovery during node recovery

[#1630](https://github.com/MystenLabs/walrus/pull/1630): Use the metrics and REST API address from the configuration file instead of always binding to `0.0.0.0`. Note that this bugfix may break some setups that relied on the previous incorrect behavior. To get the old behavior, set the `metrics_address` and `rest_api_address` to `0.0.0.0:<port>` explicitly.

[#2072](https://github.com/MystenLabs/walrus/pull/2072): Increase default value of `http2_max_pending_accept_reset_streams` parameter to disable warnings from the `h2` crate.

[#2067](https://github.com/MystenLabs/walrus/pull/2067): Reduce verbosity levels of problems with metrics and config sync from ERROR to WARN.

#### Aggregator

[#2066](https://github.com/MystenLabs/walrus/pull/2066): Allow specifying Sui RPC URL in the configuration file in addition to the CLI option.

[#1630](https://github.com/MystenLabs/walrus/pull/1630): Use the metrics address specified through the CLI options file instead of always binding to `0.0.0.0`. Note that this bugfix may break some setups that relied on the previous incorrect behavior. To get the old behavior, use `--metrics-address 0.0.0.0:<port>` when starting the aggregator.

#### Publisher

[#1630](https://github.com/MystenLabs/walrus/pull/1630): Use the metrics address specified through the CLI options file instead of always binding to `0.0.0.0`. Note that this bugfix may break some setups that relied on the previous incorrect behavior. To get the old behavior, use `--metrics-address 0.0.0.0:<port>` when starting the aggregator.

#### CLI

[#2066](https://github.com/MystenLabs/walrus/pull/2066): Add a new `rpc_urls` field to the client config, which allows setting multiple Sui RPC URLs. The Sui RPC URL(s) used for reads are now selected in the following order: (1) CLI argument, (2) client configuration file, (3) wallet config. If none of these are set, the client will **no longer fall back to the `fullnode.testnet.sui.io`** but instead exit with an error.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/mainnet-v1.26.3

---

### Walrus v1.26.2

**Testnet** | May 28, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.26.2)

#### Storage node

[#1630](https://github.com/MystenLabs/walrus/pull/1630):
Use the metrics and REST API address from the configuration file instead of always binding to `0.0.0.0`. Note that this bugfix may break some setups that relied on the previous incorrect behavior. To get the old behavior, set the `metrics_address` and `rest_api_address` to `0.0.0.0:<port>` explicitly.

[#2072](https://github.com/MystenLabs/walrus/pull/2072):
Increase default value of `http2_max_pending_accept_reset_streams` parameter to disable warnings from the `h2` crate.

[#2067](https://github.com/MystenLabs/walrus/pull/2067):
Reduce verbosity levels of problems with metrics and config sync from ERROR to WARN.

#### Aggregator

[#2066](https://github.com/MystenLabs/walrus/pull/2066):
Allow specifying Sui RPC URL in the configuration file in addition to the CLI option.

[#1630](https://github.com/MystenLabs/walrus/pull/1630):
Use the metrics address specified through the CLI options file instead of always binding to `0.0.0.0`. Note that this bugfix may break some setups that relied on the previous incorrect behavior. To get the old behavior, use `--metrics-address 0.0.0.0:<port>` when starting the aggregator.

#### Publisher

[#1630](https://github.com/MystenLabs/walrus/pull/1630):
Use the metrics address specified through the CLI options file instead of always binding to `0.0.0.0`. Note that this bugfix may break some setups that relied on the previous incorrect behavior. To get the old behavior, use `--metrics-address 0.0.0.0:<port>` when starting the aggregator.

#### CLI

[#2066](https://github.com/MystenLabs/walrus/pull/2066):
Add a new `rpc_urls` field to the client config, which allows setting multiple Sui RPC URLs. The Sui RPC URL(s) used for reads are now selected in the following order: (1) CLI argument, (2) client configuration file, (3) wallet config. If none of these are set, the client will **no longer fall back to the `fullnode.testnet.sui.io`** but instead exit with an error.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/testnet-v1.26.2

---

### Walrus v1.25.1

**Mainnet** | May 22, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.25.1)

#### Bug Fix

[#2121](https://github.com/MystenLabs/walrus/pull/2121): add options to update compaction write throttling, and set high write stalling threshold in metadata table 

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/mainnet-v1.25.1

---

### Walrus v1.25.0

**Mainnet** | May 22, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.25.0)

#### CLI

[#2010](https://github.com/MystenLabs/walrus/pull/2010): Improved performance for batch blob operations by optimizing storage selection. Operations with multiple blobs are now significantly faster with no change to the user workflow.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/mainnet-v1.25.0

---

### Walrus v1.24.2

**Mainnet** | May 12, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.24.2)

#### Aggregator

[#2005](https://github.com/MystenLabs/walrus/pull/2005): Improved documentation for shared blob retrieval.

#### Publisher

[#2015](https://github.com/MystenLabs/walrus/pull/2015): Adds the possibility to `force` a store operation, like with the CLI.

#### CLI

[#2004](https://github.com/MystenLabs/walrus/pull/2004): Major reduction of SUI gas cost when registering multiple blobs simultaneously.

[#2003](https://github.com/MystenLabs/walrus/pull/2003): Add new `completion` command to generate completion scripts for various shells.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/mainnet-v1.24.2

---

### Walrus v1.24.1

**Testnet** | May 6, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.24.1)

#### Aggregator

[#2005](https://github.com/MystenLabs/walrus/pull/2005): Improved documentation for shared blob retrieval.

#### Publisher

[#2015](https://github.com/MystenLabs/walrus/pull/2015): Adds the possibility to `force` a store operation, like with the CLI.

#### CLI

[#2004](https://github.com/MystenLabs/walrus/pull/2004): Major reduction of SUI gas cost when registering multiple blobs simultaneously.

[#2003](https://github.com/MystenLabs/walrus/pull/2003): Add new `completion` command to generate completion scripts for various shells.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/testnet-v1.24.1

---

### Walrus v1.22.1

**Mainnet** | April 25, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.22.1)

#### Storage node

[#1976](https://github.com/MystenLabs/walrus/pull/1976): Remove the `experimental_batch_symbol_recovery` config variable; operators must ensure that the field is not present in their configs.

#### CLI

[#1973](https://github.com/MystenLabs/walrus/pull/1973): Fetch the checkpoint progress info for storage nodes, in the health command.

[#1943](https://github.com/MystenLabs/walrus/pull/1943): Fix a bug that prevented the creation of a Sui wallet at the default location through `walrus generate-sui-wallet`.

[#2008](https://github.com/MystenLabs/walrus/pull/2008): Major reduction of SUI gas cost when registering multiple blobs simultaneously.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/mainnet-v1.22.1

---

### Walrus v1.21.2

**Mainnet** | April 17, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.21.2)

#### Storage node

[#1934](https://github.com/MystenLabs/walrus/pull/1934): Optimized default parameters for shard transfer and recovery.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/mainnet-v1.21.2

---

### Walrus v1.21.1

**Testnet** | April 15, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.21.1)

#### Storage node

[#1934](https://github.com/MystenLabs/walrus/pull/1934): Optimized default parameters for shard transfer and recovery.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/testnet-v1.21.1

---

### Walrus v1.20.2

**Mainnet** | April 10, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.20.2)

#### CLI

[#1805](https://github.com/MystenLabs/walrus/pull/1805): Added `Latest checkpoint sequence number` and `Estimated checkpoint lag` in health output.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/mainnet-v1.20.2

---

### Walrus v1.18.2

**Mainnet** | March 25, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.18.2)

#### Storage node

[#1703](https://github.com/MystenLabs/walrus/pull/1703): Storage node monitors TLS certificate and reboots storage node upon changes.

[#1715](https://github.com/MystenLabs/walrus/pull/1715): Added optional `--network-key-path` and `--certificate-path` options to the `setup` command, removed `--use-legacy-event-provider` and `--disable-event-blob-writer`. Adjusted defaults for storage prices and commission rates.

[#1793](https://github.com/MystenLabs/walrus/pull/1793): Adds a configurable timeout to checkpoint requests.

#### Publisher

[#1613](https://github.com/MystenLabs/walrus/pull/1613): Add `size` field of JWTToken

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/mainnet-v1.18.2

---

### Walrus v1.18.1

**Testnet** | March 20, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.18.1)

#### Storage node

[#1703](https://github.com/MystenLabs/walrus/pull/1703): Storage node monitors TLS certificate and reboots storage node upon changes.

[#1715](https://github.com/MystenLabs/walrus/pull/1715): Added optional `--network-key-path` and `--certificate-path` options to the `setup` command, removed `--use-legacy-event-provider` and `--disable-event-blob-writer`. Adjusted defaults for storage prices and commission rate.

#### Publisher

[#1613](https://github.com/MystenLabs/walrus/pull/1613): Add `size` field of JWTToken

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/testnet-v1.18.1

---

### Walrus v1.17.2

**Testnet** | March 15, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.17.2)

#### Storage node

[#1717](https://github.com/MystenLabs/walrus/pull/1717):
Added optional `--network-key-path` and `--certificate-path` options to the `setup` command, removed `--use-legacy-event-provider` and `--disable-event-blob-writer`. Adjusted defaults for storage prices and commission rate.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/testnet-v1.17.2

---

### Walrus v1.17.1

**Testnet** | March 12, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.17.1)

#### CLI

[#1685](https://github.com/MystenLabs/walrus/pull/1685): Fixes a bug where an explicitly set wallet path through the `--wallet` CLI option was ignored.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/testnet-v1.17.1

---

### Walrus v1.16.1

**Testnet** | March 5, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.16.1)

#### Storage node

[#1592](https://github.com/MystenLabs/walrus/pull/1592):
Enabling updating storage node metadata via local storage node config file.

[#1640](https://github.com/MystenLabs/walrus/pull/1640):
Fixes a bug in the blob info tracking, where the initial certified epoch was not updated correctly.

#### Publisher

[#1628](https://github.com/MystenLabs/walrus/pull/1628):
Changes the default post-store behavior of the publisher to keeping the created `Blob` objects in the main publisher wallet, instead of burning them immediately. Replaces the `--keep` flag with a `--burn-after-store` flag.

#### CLI

[#1644](https://github.com/MystenLabs/walrus/pull/1644):
Allow passing multiple files/blob IDs/object IDs to the walrus delete command.
    There is a breaking change for the `delete` json API, 
    
The current syntax is:
```
{
  "command": {
    "delete": {
      "file": [...],
      "blobId": [...],
      "objectId": [...],
      "yes": true,
      "noStatusCheck": false,
    }
  }
}
```

While one previous example could be:
```
{
  "command": {
    "delete": {
      "file": "/path/to/file.txt",
      "blobId": "VWr-EVsFCJCl1mdPM8cBZ_c4LkRcpUbmRxjGpiEPQ3w",
      "yes": true,
      "noStatusCheck": false
    }
  }
}
```

[#1532](https://github.com/MystenLabs/walrus/pull/1532):
support has been introduced for multiple contexts, and a default_context for walrus client invocations. Context can be overriden with the new --context &lt;context&gt; argument.

Here is an example (with some fake addresses) multi-context config file.

```
---
contexts:
  testnet:
    system_object: 0x98ebc47370603fe81d9e15491b2f1443d619d1dab720d586e429ed233e1255c1
    staking_object: 0x20266a17b4f1a216727f3eef5772f8d486a9e3b5e319af80a5b75809c035561d
    exchange_objects:
      - 0x59ab926eb0d94d0d6d6139f11094ea7861914ad2ecffc7411529c60019133997
      - 0x89127f53890840ab6c52fca96b4a5cf853d7de52318d236807ad733f976eef7b
      - 0x9f9b4f113862e8b1a3591d7955fadd7c52ecc07cf24be9e3492ce56eb8087805
      - 0xb60118f86ecb38ec79e74586f1bb184939640911ee1d63a84138d080632ee28a
    wallet_config:
      path: ~/.sui/sui_config/client.yaml
      active_env: testnet
  devnet:
    system_object: 0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
    staking_object: 0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
    exchange_objects:
      - 0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
      - 0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd
      - 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
      - 0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
    wallet_config:
      path: ~/.sui/sui_config/client.yaml
      active_env: devnet

default_context: testnet

```

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/testnet-v1.16.1

---

### Walrus v1.15.4

**Testnet** | February 28, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.15.4)

#### Storage node

[#1655](https://github.com/MystenLabs/walrus/pull/1655):
Fix an issue where keys were not properly loaded into memory when updating on-chain config parameters.

#### Publisher

[#1655](https://github.com/MystenLabs/walrus/pull/1655):
Use the correct encoding type by default.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/testnet-v1.15.4

---

### Walrus v1.15.3

**Testnet** | February 27, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.15.3)

#### Storage node

[#1548](https://github.com/MystenLabs/walrus/pull/1548): Automatically reload the StorageNodeConfig from disk and update the on-chain configuration when changes are detected. Some changes may result in an automatic restart of the storage nodes.

[#1608](https://github.com/MystenLabs/walrus/pull/1608): Storage node now supports multiple encoding types. Depending on features selected at compile time, it accepts a subset of them.

#### Aggregator

[#1591](https://github.com/MystenLabs/walrus/pull/1591): Exposed the aggregator endpoint to allow fetching blobs with associated attributes; if the attribute fields are in the allow-list, they will be included in the response headers.
  The current allow-list is:
  * "content-disposition"
  * "content-encoding"
  * "content-language"
  * "content-location"
  * "content-type"
  * "link"

#### Publisher

[#1608](https://github.com/MystenLabs/walrus/pull/1608): Added `encoding_type` query parameter. Added additional variants for the encoding type.

#### CLI

[#1591](https://github.com/MystenLabs/walrus/pull/1591): Added the allowed-headers option for daemon commands, allowing operators to specify custom allowed headers to filter key-value pairs from the blob attribute.

[#1594](https://github.com/MystenLabs/walrus/pull/1594): Added `--sort-by` and `--desc` options for `walrus health` and `walrus info` commands, to sort the storage nodes by node-id, node-name, node-status, or node-url.

[#1606](https://github.com/MystenLabs/walrus/pull/1606): Fix `walrus store` behavior when blobs can be extended.

[#1608](https://github.com/MystenLabs/walrus/pull/1608): The CLI supports multiple encoding types for reads and writes. Depending on features selected at compile time, it uses a different default type and accepts a subset of them for writes.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/testnet-v1.16.0

---

### Walrus v1.14.0

**Testnet** | February 19, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.14.0)

#### Storage node

[#1568](https://github.com/MystenLabs/walrus/pull/1568): The new batch symbol recovery is enabled by default.

#### CLI

[#1546](https://github.com/MystenLabs/walrus/pull/1546): Added the following new commands for blob attribute:
- `walrus set-blob-attribute <BLOB_OBJ_ID>`
- `walrus get-blob-attribute [OPTIONS] <BLOB_OBJ_ID>`
-  `walrus remove-blob-attribute-fields [OPTIONS] <BLOB_OBJ_ID>`
-  `walrus remove-blob-attribute [OPTIONS] <BLOB_OBJ_ID>`

[#1535](https://github.com/MystenLabs/walrus/pull/1535): The walrus store command now checks for matching owned Blob objects and extends their lifetime if necessary instead of registering the blob again.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/testnet-v1.14.0

---

### Walrus v1.13.2

**Testnet** | February 13, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.13.2)

[#1576](https://github.com/MystenLabs/walrus/pull/1576): fix: Update blob certified metric only if it is event blob

[#1575](https://github.com/MystenLabs/walrus/pull/1575): fix(publisher): retry if refresh fails

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/testnet-v1.13.2

---

### Walrus v1.13.1

**Testnet** | February 12, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.13.1)

#### Storage node

[#1509](https://github.com/MystenLabs/walrus/pull/1509):
The config block `tls.pem_files` has been removed. Provide a TLS certificate path by the `tls.certificate_path` config argument, and the PKCS#8 PEM-encoded TLS private using `network_key_pair.path`.

[#1461](https://github.com/MystenLabs/walrus/pull/1461):
Storage nodes automatically update and rotate the protocol key pair. This only requires setting the `next_protocol_key_pair` in the config file.

#### CLI

[#1551](https://github.com/MystenLabs/walrus/pull/1551):
introduces the `--ignore-resources` flag.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/testnet-v1.13.1

---

### Walrus v1.12.0

**Testnet** | February 6, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.12.0)

#### CLI

[#1486](https://github.com/MystenLabs/walrus/pull/1486):
Adds the ability to provide a list of node IDs or URLs for the `walrus health` command. This is a breaking change for the JSON mode because `nodeId` and `nodeUrl` are changed to `nodeIds` and `nodeUrls`, respectively. The "normal" CLI is backwards-compatible because `--node-id` and `--node-url` are set as aliases for the new options.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/testnet-v1.12.0

---

### Walrus v1.11.0

**Testnet** | January 29, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.11.0)

#### Storage node

[#1455](https://github.com/MystenLabs/walrus/pull/1455):
-- Added the feature to change storage node config parameter as following, by changing the local config file and restart node.

[#1463](https://github.com/MystenLabs/walrus/pull/1463):
Allow setting the metrics push configuration directly in the `walrus-node` setup command.

[#1476](https://github.com/MystenLabs/walrus/pull/1476):
Correctly set `walrus_current_node_status` metric during startup.

[#1456](https://github.com/MystenLabs/walrus/pull/1456):
Deprecate the recovery symbol endpoint in favour of a new endpoint. Integrations using recovery symbols should migrate to the new endpoint.

#### CLI

[#1406](https://github.com/MystenLabs/walrus/pull/1406):
Add option to extend owned blobs with WAL

[#1398](https://github.com/MystenLabs/walrus/pull/1398):
adds the `health` command to the CLI.

[#1428](https://github.com/MystenLabs/walrus/pull/1428):
Add `--earliest-expiry-time` to `store` command so a unix timestamp in ms can be specified. Show human readable time in GMT for estimated expiry time in `blob-status` command.

[#1474](https://github.com/MystenLabs/walrus/pull/1474):
Allow specifying 'global' options like `--json` anywhere in the command, for example `walrus info --json`.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/testnet-v1.11.0

---

### Walrus v1.10.0

**Testnet** | January 23, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.10.0)

#### Storage node

[#1407](https://github.com/MystenLabs/walrus/pull/1407): Update error responses returned by storage nodes to be machine-readable and contain more details

#### Aggregator

[#1407](https://github.com/MystenLabs/walrus/pull/1407): Add error statuses to the responses of failed requests; update endpoint URLs; and add more detailed OpenAPI schemas.

#### Publisher

[#1407](https://github.com/MystenLabs/walrus/pull/1407): Add error statuses to the responses of failed requests; update endpoint URLs; and add more detailed OpenAPI schemas.

[#1345](https://github.com/MystenLabs/walrus/pull/1345): Add optional JWT auth layer for the publisher

#### CLI

[#1402](https://github.com/MystenLabs/walrus/pull/1402): This fixes the cost computation for the `store --dry-run` command.

[#1404](https://github.com/MystenLabs/walrus/pull/1404): Allows staking with multiple nodes simultaneously.

[#1421](https://github.com/MystenLabs/walrus/pull/1421): Fix serialization of JSON output for the `walrus store` command.

[#1425](https://github.com/MystenLabs/walrus/pull/1425): Adds the option to specify `max` as the number of epochs.

[#1388](https://github.com/MystenLabs/walrus/pull/1388): changes to the syntax and output of the `info` command

#### Backup node

[#1397](https://github.com/MystenLabs/walrus/pull/1397): an entrypoint called walrus-backup will now be included in the build process. It has minimal (tailing Walrus-related Sui events) functionality at this point.

---
#### Full Log: https://github.com/MystenLabs/walrus/commits/testnet-v1.10.0

---

## Walrus Memory

### Walrus Memory SDK v0.0.5

June 15, 2026 | [GitHub](https://github.com/MystenLabs/MemWal/releases/tag/%40mysten-incubation/oc-memwal%400.0.5)

#### 0.0.5

#### Patch Changes

- Fix unresolvable `workspace:*` dependency in the published package. The release
  workflow used `npm publish`, which ships `package.json` verbatim and cannot
  rewrite the `workspace:` protocol, so the published artifact carried
  `"@mysten-incubation/memwal": "workspace:*"` and failed to install outside the
  monorepo (`Unsupported URL Type 'workspace:'`). Switched the release workflows
  to `pnpm publish`, which rewrites `workspace:*` to the concrete dependency
  version at pack time.

---

### Walrus Memory MCP v0.0.5

June 12, 2026 | [GitHub](https://github.com/MystenLabs/MemWal/releases/tag/%40mysten-incubation/memwal-mcp%400.0.5)

#### 0.0.5

#### Added

- Automatic memory plugin for Claude Code, Codex, Cursor, and Antigravity.
- New `memwal_remember_bulk` and `memwal_health` tools.

#### Fixed

- Ship the plugin's `.mcp.json` in the marketplace bundle. A root gitignore rule excluded it, so plugin installs loaded the lifecycle hooks but never registered the MCP server.

#### Changed

- Memory tools are now proactive — agents recall and save context on their own.

#### Fixed

- Plugin bundle now ships its `.mcp.json` so the MCP server registers on install.
- Automatically recover from dropped relayer connections that could hang tool calls.

---

### Walrus Memory MCP v0.0.4

June 5, 2026 | [GitHub](https://github.com/MystenLabs/MemWal/releases/tag/%40mysten-incubation/memwal-mcp%400.0.4)

#### 0.0.4

#### Fixed

- Accept HTTPS dashboard sign-in callbacks to the local `127.0.0.1` MCP listener.
- Reload credentials after `memwal_login` so memory tools work without restarting the MCP client.

---

### Walrus Memory MCP v0.0.3

June 4, 2026 | [GitHub](https://github.com/MystenLabs/MemWal/releases/tag/%40mysten-incubation/memwal-mcp%400.0.3)

#### 0.0.3

#### Changed

- Rebranded package metadata and documentation from MemWal to Walrus Memory.

---

### Walrus Memory SDK v0.0.4

June 2, 2026 | [GitHub](https://github.com/MystenLabs/MemWal/releases/tag/%40mysten-incubation/oc-memwal%400.0.4)

#### 0.0.4

#### Patch Changes

- [#218](https://github.com/MystenLabs/MemWal/pull/218) [`333d327`](https://github.com/MystenLabs/MemWal/commit/333d3279f59c2a033225bc99238b7586474333fb) Thanks [@hungtranphamminh](https://github.com/hungtranphamminh)! - Wire temporal anchoring through the agent-side memory tools.

  ### Added

  - `memory_store` tool now accepts an optional `occurredAt` argument (RFC-3339 / ISO-8601 string) so agents can anchor recounted past events to the date they actually occurred. Description tells the LLM to omit it when unknown rather than guess.

  ### Changed

  - Auto-capture hook (`agent_end`) now passes `new Date()` as `occurredAt` to `analyze()`. Every captured conversation now gets temporal anchoring automatically — the server extractor resolves in-turn relative references ("yesterday", "last Friday") into absolute dates inside the stored fact text. Facts captured by this version now carry resolved dates.
  - SDK dependency bumped from published `^0.0.2` to `workspace:*` to consume the new `AnalyzeOptions` signature.

- Updated dependencies [[`333d327`](https://github.com/MystenLabs/MemWal/commit/333d3279f59c2a033225bc99238b7586474333fb)]:
  - @mysten-incubation/memwal@0.0.7

---

### Walrus Memory python v0.1.4

June 2, 2026 | [GitHub](https://github.com/MystenLabs/MemWal/releases/tag/memwal-python%400.1.4)

Release memwal Python SDK v0.1.4

---

### Walrus Memory TypeScript SDK v0.0.7

June 2, 2026 | [GitHub](https://github.com/MystenLabs/MemWal/releases/tag/%40mysten-incubation/memwal%400.0.7)

#### 0.0.7

#### Patch Changes

- [#218](https://github.com/MystenLabs/MemWal/pull/218) [`333d327`](https://github.com/MystenLabs/MemWal/commit/333d3279f59c2a033225bc99238b7586474333fb) Thanks [@hungtranphamminh](https://github.com/hungtranphamminh)! - Add optional `occurredAt` to `analyze()` and `analyzeAndWait()` for temporal anchoring of extracted facts.

  - New `AnalyzeOptions` overload: `analyze(text, { namespace, occurredAt })` accepts `Date` or RFC-3339 string. The legacy `analyze(text, namespace?)` signature still works unchanged.
  - When `occurredAt` is supplied, the server resolves in-turn relative references ("last Friday", "yesterday") into absolute dates inside the extracted fact text before embedding and encryption.
  - Wire format is RFC-3339 UTC with millisecond precision (e.g. `"2023-05-25T17:50:00.000Z"`).
  - Invalid `Date` instances (constructed from malformed input) now throw a diagnostic `TypeError` from the SDK rather than an opaque `RangeError` from `toISOString()`.
  - Field is omitted from the request body when not supplied — existing callers see byte-identical wire payloads.

---

### Walrus Memory TypeScript SDK v0.0.6

May 28, 2026 | [GitHub](https://github.com/MystenLabs/MemWal/releases/tag/%40mysten-incubation/memwal%400.0.6)

#### 0.0.6

#### Added

- Added `RecallParams` for object-style `recall(...)` calls.

#### Changed

- Marked the positional `recall(...)` overload as deprecated in favor of `recall({ query, limit, namespace })`.
- Documented `restore()` response fields, default limit, pagination behavior, and performance expectations.

---

### Walrus Memory python v0.1.3

May 28, 2026 | [GitHub](https://github.com/MystenLabs/MemWal/releases/tag/memwal-python%400.1.3)

Release memwal Python SDK v0.1.3

---

### Walrus Memory SDK v0.0.3

May 25, 2026 | [GitHub](https://github.com/MystenLabs/MemWal/releases/tag/%40mysten-incubation/oc-memwal%400.0.3)

#### 0.0.3

#### Patch Changes

- Rebrand package metadata and documentation from MemWal to Walrus Memory.

---

### Walrus Memory MCP v0.0.2

May 25, 2026 | [GitHub](https://github.com/MystenLabs/MemWal/releases/tag/%40mysten-incubation/memwal-mcp%400.0.2)

#### 0.0.2

#### Added

- Added relayer compatibility metadata checks before opening the MCP bridge.

#### Changed

- Rebranded package metadata and documentation from MemWal to Walrus Memory.

---

### Walrus Memory TypeScript SDK v0.0.5

May 25, 2026 | [GitHub](https://github.com/MystenLabs/MemWal/releases/tag/%40mysten-incubation/memwal%400.0.5)

#### 0.0.5

#### Added

- Added relayer compatibility metadata checks before protected requests.
- Added `compatibility()` and exported compatibility types/errors so callers can inspect SDK/relayer support explicitly.
- Added `RecallOptions` for `topK`, namespace override, and `maxDistance`.

#### Changed

- Prefer Sui gRPC for SEAL sessions, with JSON-RPC fallback.
- Updated docs/examples for `MEMWAL_PRIVATE_KEY` and hosted relayer defaults.
- Rebranded package metadata and documentation from MemWal to Walrus Memory.

#### Fixed

- Made `401` relayer errors more actionable.

---

### Walrus Memory python v0.1.2

May 25, 2026 | [GitHub](https://github.com/MystenLabs/MemWal/releases/tag/memwal-python%400.1.2)

Release memwal Python SDK v0.1.2

---

### Walrus Memory python v0.1.0

May 21, 2026 | [GitHub](https://github.com/MystenLabs/MemWal/releases/tag/memwal-python%400.1.0)

Release memwal Python SDK v0.1.0

---

### Walrus Memory TypeScript SDK v0.0.4

May 15, 2026 | [GitHub](https://github.com/MystenLabs/MemWal/releases/tag/%40mysten-incubation/memwal%400.0.4)

#### 0.0.4

#### Added

- Added `getRememberStatus(jobId)` so clients can poll and display the full async remember state machine.
- Added `SealServerConfig` and `sealServerConfigs` for manual-mode SEAL committee aggregator configuration.

#### Changed

- Manual mode now normalizes full SEAL server configs, validates optional API key pairs, and caps the default threshold to configured server weight.
- Manual mode keeps testnet defaults on the legacy independent key servers for compatibility with hosted testnet relayer data.

---

### Walrus Memory MCP v0.0.1

May 15, 2026 | [GitHub](https://github.com/MystenLabs/MemWal/releases/tag/%40mysten-incubation/memwal-mcp%400.0.1)

#### 0.0.1

#### Initial Release

- Stdio MCP server for MemWal with browser-based wallet login.
- Inline `memwal_login` and `memwal_logout` session tools.
- Memory tools for remember, recall, analyze, and restore through the MemWal relayer.
- Environment presets for production, dev, staging, and local relayers.

---

### Walrus Memory TypeScript SDK v0.0.3

May 8, 2026 | [GitHub](https://github.com/MystenLabs/MemWal/releases/tag/%40mysten-incubation/memwal%400.0.3)

#### 0.0.3

#### Changed

- Updated `remember()` for the relayer's async `/api/remember` flow. It now returns the accepted job payload immediately.
- Added `rememberAsync()`, `waitForRememberJob()`, and `rememberAndWait()` for callers that need the final `blob_id`.
- Added bulk remember helpers: `rememberBulk()`, `rememberBulkAsync()`, `waitForRememberJobs()`, and `rememberBulkAndWait()`.
- Updated `analyze()` for async fact storage and added `analyzeAndWait()`.

#### Compatibility

- `recall()` and `restore()` remain wire-compatible with the existing relayer responses.
- The SDK continues to use `x-seal-session` for relayer-mode decrypt credentials.

---

### Walrus Memory SDK v0.0.2

April 30, 2026 | [GitHub](https://github.com/MystenLabs/MemWal/releases/tag/%40mysten-incubation/oc-memwal%400.0.2)

#### 0.0.1

#### Initial Release

- NemoClaw/OpenClaw memory plugin powered by MemWal
- Automatic memory recall via `before_prompt_build` hook
- Automatic fact capture via `agent_end` hook
- Session summary on `before_reset` hook
- CLI commands: `openclaw memwal stats`, `openclaw memwal search`
- LLM tools: `memory_search`, `memory_store`

---

### Walrus Memory TypeScript SDK v0.0.2

April 30, 2026 | [GitHub](https://github.com/MystenLabs/MemWal/releases/tag/%40mysten-incubation/memwal%400.0.2)

#### 0.0.2

#### Security

- Added per-request `x-nonce` signing to block replay within the timestamp window.
- Added `x-account-id` to the canonical signed message so account hints cannot be rebound in transit.
- Replaced relayer-mode `x-delegate-key` transport with ephemeral `x-seal-session`; manual-mode requests no longer send delegate private key material.
- SDK versions that do not send `x-nonce` are no longer supported by the server and receive `426 Upgrade Required`.

---

## Blog Announcements

### Announcing Mainnet

March 27, 2025 | [Read full post](/blog/06_mainnet)

The production Walrus Mainnet is now live, and operated by
a decentralized network of over 100 storage nodes. Epoch 1 begun on March 25, 2025. The
network can now be used to
[publish and retrieve blobs](/docs/walrus-client/storing-blobs),
[upload and browse Walrus Sites](/docs/sites/getting-started/publishing-your-first-site), as
well as [stake and unstake](https://stake-wal.wal.app/) to determine future committees using the
live
[Mainnet WAL token](https://www.walrus.xyz/wal-token).
On Mainnet, the Walrus security properties hold. And Walrus is now ready to satisfy
the needs of real applications.

---

### Announcing Testnet v2

January 16, 2025 | [Read full post](/blog/05_testnet_redeployment)

We are today redeploying the Walrus Testnet to incorporate various improvements, including some
backwards-incompatible changes. Make sure to get the latest binary and configuration as described
in the [setup section](/docs/getting-started/advanced-setup).

---

### Announcing Testnet

October 17, 2024 | [Read full post](/blog/04_testnet_update)

Today, a community of operators launches the first public Walrus Testnet.
This is an important milestone in validating the operation of Walrus as a decentralized blob store,
by operating it on a set of independent storage nodes, that change over time through a delegated
proof of stake mechanism. The Testnet also brings functionality updates relating to governance,
epochs, and blob deletion.

---

### Announcing the Official Walrus Whitepaper

September 17, 2024 | [Read full post](/blog/03_whitepaper)

In June, Mysten Labs announced Walrus, a new decentralized secure blob store design, and introduced
a developer preview that currently stores over [12TiB](https://capacity.walrus.site/) of data.
[Breaking the Ice](https://info.breakingtheice.sui.io/) gathered over 200 developers to build apps
leveraging decentralized storage.

---

### Devnet Update

August 12, 2024 | [Read full post](/blog/02_devnet_update)

We have redeployed the Walrus Devnet to incorporate various improvements to the Walrus storage nodes
and clients. In this process, all blobs stored on Walrus were wiped. Note that this may happen again
on Devnet and Testnet, but obviously *not on the future Mainnet*.

---

### Announcing Walrus: A Decentralized Storage and Data Availability Protocol

November 1, 2024 | [Read full post](/blog/01_announcing_walrus)

Walrus is an innovative decentralized storage network for blockchain apps and autonomous agents. The
Walrus storage system is being released today as a developer preview for Sui builders in order to
gather feedback. We expect a broad rollout to other web3 communities very soon!

---