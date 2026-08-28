> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

### Walrus v1.55.1

`Testnet` | August 28, 2026 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.55.1)

#### Changes

- [Adds a `node_recovery_baseline_invalidations_total` metric counting sync-and-recovery info publications that invalidate the node-recovery baseline and supersede in-flight node-recovery runs.](https://github.com/MystenLabs/walrus/pull/3643)

---

### Walrus v1.54.0

`Mainnet` | August 19, 2026 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.54.0)

#### Changes

- [Requests for quilt patches of expired or nonexistent quilts now return 404 `BLOB_NOT_FOUND` within about a second, instead of 503 `BLOB_UNAVAILABLE` after a 10-20 second fan-out.](https://github.com/MystenLabs/walrus/pull/3604)
- [`walrus read-quilt` fails immediately with "the blob ID does not exist" for expired or never-certified quilts, instead of timing out against the storage nodes.](https://github.com/MystenLabs/walrus/pull/3604)

---

### Walrus v1.53.0

`Mainnet` | August 5, 2026 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.53.0)

---

### Walrus v1.52.1

`Mainnet` | July 22, 2026 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.52.1)

---

### Walrus v1.52.0

`Testnet` | July 15, 2026 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.52.0)

---

### Walrus v1.51.2

`Mainnet` | July 9, 2026 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.51.2)

#### Changes

- [`walrus info committee` gains a `--commission-receivers` flag that prints, as CSV, the commission receiver of every node in the previous, current, and next committees (including the owning address for object receivers).](https://github.com/MystenLabs/walrus/pull/3479)

---

### Walrus v1.51.1

`Mainnet` | July 9, 2026 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.51.1)

#### Changes

- [`walrus info committee` gains a `--commission-receivers` flag that prints, as CSV, the commission receiver of every node in the previous, current, and next committees (including the owning address for object receivers).](https://github.com/MystenLabs/walrus/pull/3479)

---

### Walrus v1.50.0

`Mainnet` | June 24, 2026 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.50.0)

A maintenance release on Testnet with no user-facing changes documented in the release notes; see
the full commit log for internal updates.

---

### Walrus v1.49.1

`Mainnet` | May 27, 2026 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.49.1)

A maintenance release on Mainnet with no user-facing changes documented in the release notes; see
the full commit log for internal updates.

---

### Walrus v1.48.1

`Mainnet` | May 13, 2026 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.48.1)

A maintenance release on Mainnet with no user-facing changes documented in the release notes; see
the full commit log for internal updates.

---

### Walrus v1.47.1

`Mainnet` | April 29, 2026 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.47.1)

Enables storage pool support in the storage node and makes the aggregator return a retryable HTTP
503 `BLOB_UNAVAILABLE` (instead of 500) when a blob is only temporarily unretrievable. Blob
attribute headers such as `Content-Type` are now correctly included on range requests when reading
by object ID.

#### Changes

- [enable storage pool support in storage node](https://github.com/MystenLabs/walrus/pull/3256)
- [The `GET /v1/blobs/{blob_id}` endpoint now returns](https://github.com/MystenLabs/walrus/pull/3284)
- [Blob attribute headers (such as Content-Type) are now correctly included on range request responses when reading blobs by object ID.](https://github.com/MystenLabs/walrus/pull/3084)

---

### Walrus v1.47.0

`Testnet` | April 22, 2026 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.47.0)

The Testnet preview of storage pool support in the storage node, plus the aggregator's new retryable
HTTP 503 `BLOB_UNAVAILABLE` response for temporarily unretrievable blobs. Range requests by object
ID now return blob attribute headers such as `Content-Type` correctly.

#### Changes

- [enable storage pool support in storage node](https://github.com/MystenLabs/walrus/pull/3256)
- [The `GET /v1/blobs/{blob_id}` endpoint now returns](https://github.com/MystenLabs/walrus/pull/3284)
- [Blob attribute headers (such as Content-Type) are now correctly included on range request responses when reading blobs by object ID.](https://github.com/MystenLabs/walrus/pull/3084)

---

### Walrus v1.46.1

`Mainnet` | April 15, 2026 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.46.1)

Adds a `reuse_resources` query parameter so the publisher and daemon can resume an existing,
unfinished upload pipeline instead of starting over.

#### Changes

- [allow publisher and daemon to add `reuse_resources` query parameters to try to reuse existing (unfinished) upload pipeline](https://github.com/MystenLabs/walrus/pull/3202)

---

### Walrus v1.45.2

`Mainnet` | April 1, 2026 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.45.2)

Raises the storage node's RocksDB background threads from 2 to 16 and adds the opt-in
`garbage_collection.enable_immediate_data_deletion` parameter. Also fixes a combined-daemon body
size limit that was silently rejecting uploads with 413 errors and adds a `walrus info coin` CLI
subcommand.

#### Changes

- [Increases the number of RocksDB background threads from 2 (RocksDB default) to 16. Can be configured through the config file.](https://github.com/MystenLabs/walrus/pull/3155)
- [Add a new configuration parameter `garbage_collection.enable_immediate_data_deletion`, which defaults to `false`. When processing a `BlobDeleted` event, data is only deleted immediately if this parameter is set to true.](https://github.com/MystenLabs/walrus/pull/3160)
- [Fixed body size limit in combined daemon mode (running publisher and aggregator together). The limit was set 1024x too small, causing uploads to be silently rejected with 413 Payload Too Large. Standalone publisher mode was not affected.](https://github.com/MystenLabs/walrus/pull/3072)
- [Added a new `info` subcommand to the CLI. `walrus info coin` provides the StructTag of the walrus coin given the CLI's configured context.](https://github.com/MystenLabs/walrus/pull/3122)

---

### Walrus v1.45.1

`Testnet` | March 25, 2026 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.45.1)

The Testnet preview that raises RocksDB background threads from 2 to 16 and adds the opt-in
`garbage_collection.enable_immediate_data_deletion` parameter. Includes the combined-daemon 413
upload fix and the new `walrus info coin` CLI subcommand.

#### Changes

- [Increases the number of RocksDB background threads from 2 (RocksDB default) to 16. Can be configured through the config file.](https://github.com/MystenLabs/walrus/pull/3155)
- [Add a new configuration parameter `garbage_collection.enable_immediate_data_deletion`, which defaults to `false`. When processing a `BlobDeleted` event, data is only deleted immediately if this parameter is set to true.](https://github.com/MystenLabs/walrus/pull/3160)
- [Fixed body size limit in combined daemon mode (running publisher and aggregator together). The limit was set 1024x too small, causing uploads to be silently rejected with 413 Payload Too Large. Standalone publisher mode was not affected.](https://github.com/MystenLabs/walrus/pull/3072)
- [Added a new `info` subcommand to the CLI. `walrus info coin` provides the StructTag of the walrus coin given the CLI's configured context.](https://github.com/MystenLabs/walrus/pull/3122)

---

### Walrus v1.44.3

`Mainnet` | March 19, 2026 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.44.3)

Fixes a storage node crash caused by outdated epoch state, raises RocksDB background threads from 2
to 16, and adds the opt-in `garbage_collection.enable_immediate_data_deletion` parameter.

#### Changes

- [Fixed an issue that crashed storage nodes due to outdated epoch state.](https://github.com/MystenLabs/walrus/pull/3005)
- [Increases the number of RocksDB background threads from 2 (RocksDB default) to 16. Can be configured through the config file.](https://github.com/MystenLabs/walrus/pull/3156)
- [Add a new configuration parameter `garbage_collection.enable_immediate_data_deletion`, which defaults to `false`. When processing a `BlobDeleted` event, data is only deleted immediately if this parameter is set to true.](https://github.com/MystenLabs/walrus/pull/3161)

---

### Walrus v1.44.2

`Mainnet` | March 19, 2026 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.44.2)

#### Changes

- [Fixed an issue that crashed storage nodes due to outdated epoch state.](https://github.com/MystenLabs/walrus/pull/3005)

---

### Walrus v1.44.1

`Testnet` | March 11, 2026 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.44.1)

#### Changes

- [Fixed an issue that crashed storage nodes due to outdated epoch state.](https://github.com/MystenLabs/walrus/pull/3005)

---

### Walrus v1.43.1

`Mainnet` | March 4, 2026 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.43.1)

Allows storage nodes to vote on pricing denominated in USD, a capability that becomes usable once
the contract adds support for it.

#### Changes

- [allow storage nodes to vote on pricing based on USD. This functionality can only be used after the contract supports this.](https://github.com/MystenLabs/walrus/pull/2960)

---

### Walrus v1.42.1

`Mainnet` | February 18, 2026 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.42.1)

A maintenance release on Mainnet with no user-facing changes documented in the release notes; see
the full commit log for internal updates.

---

### Walrus v1.41.1

`Mainnet` | February 5, 2026 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.41.1)

Enables garbage collection by default on Mainnet and adds an alpha `/v1alpha/blobs/{blob_id}/stream`
endpoint designed to lower time to first byte when streaming blob data.

#### Changes

- [Garbage collection is now enabled by default on Mainnet.](https://github.com/MystenLabs/walrus/pull/2851)
- [There is a new alpha endpoint `/v1alpha/blobs/{blob_id}/stream` which is intended to reduce latency to first byte, and is intended to be used to stream blob data.](https://github.com/MystenLabs/walrus/pull/2801)

---

### Walrus v1.41.0

`Testnet` | January 28, 2026 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.41.0)

The Testnet preview enabling garbage collection by default and introducing the alpha
`/v1alpha/blobs/{blob_id}/stream` endpoint for lower-latency blob streaming.

#### Changes

- [Garbage collection is now enabled by default on Mainnet.](https://github.com/MystenLabs/walrus/pull/2851)
- [There is a new alpha endpoint `/v1alpha/blobs/{blob_id}/stream` which is intended to reduce latency to first byte, and is intended to be used to stream blob data.](https://github.com/MystenLabs/walrus/pull/2801)

---

### Walrus v1.40.3

`Mainnet` | January 21, 2026 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.40.3)

Enables database transactions and garbage collection by default on Testnet, both of which can be
turned off in the node configuration; operators using `experimental_use_optimistic_transaction_db`
should remove that flag.

#### Changes

- [Enables DB transactions (implemented in [#247](https://github.com/MystenLabs/walrus/pull/247)5) and garbage collection (implemented in [#2542](https://github.com/MystenLabs/walrus/pull/2542), [#2599](https://github.com/MystenLabs/walrus/pull/2599), and [#272](https://github.com/MystenLabs/walrus/pull/272)5) by default on Testnet. The features can be disabled by adding the following to your node configuration:](https://github.com/MystenLabs/walrus/pull/2772)

---

### Walrus v1.39.1

`Mainnet` | January 7, 2026 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.39.1)

Storage node operators must remove the obsolete `use_legacy_event_provider` flag from their
configuration if it is still present.

---

### Walrus v1.38.3

`Mainnet` | December 10, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.38.3)

Delivers significant memory-usage reductions across the aggregator, publisher, and CLI, plus lower
upload latency from fewer Sui RPC requests. Adds a new byte-range read API and an alpha
`/v1alpha/blobs/concat` endpoint, and removes the long-unused `get_recovery_symbol` endpoint.

---

### Walrus v1.38.2

`Testnet` | December 9, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.38.2)

The Testnet build with major memory-usage reductions across the aggregator, publisher, and CLI and
reduced upload latency. Adds a byte-range read API and the alpha `/v1alpha/blobs/concat` endpoint
while removing the deprecated `get_recovery_symbol` endpoint.

---

### Walrus v1.38.1

`Testnet` | December 3, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.38.1)

An earlier Testnet build of the memory-usage and latency improvements across the aggregator,
publisher, and CLI, alongside the new byte-range read API and alpha `/v1alpha/blobs/concat`
endpoint.

---

### Walrus v1.37.0

`Mainnet` | November 19, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.37.0)

Changes the default consistency check on reads; the previous strict behavior can be restored with
the `strict_consistency_check=true` query option or the `--strict-consistency-check` CLI flag.

---

### Walrus v1.36.1

`Mainnet` | November 7, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.36.1)

Adds a `skip_consistency_check` option (and `--skip-consistency-check` CLI flag) for use only with
trusted writers, and trims `server_address` and `server_port` labels from request-duration metrics
for fast, successful responses.

---

### Walrus v1.35.2

`Mainnet` | October 29, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.35.2)

Brings the trusted-writer `skip_consistency_check` option and `--skip-consistency-check` CLI flag to
Mainnet, and removes `server_address` and `server_port` labels from metrics for fast, successful
responses.

---

### Walrus v1.35.1

`Mainnet` | October 23, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.35.1)

Delivers a major reduction in memory usage when reading large blobs and adds the sliver range to
`store-quilt` output. Note the warning that the default consistency check will change in v1.37.

---

### Walrus v1.35.0

`Testnet` | October 16, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.35.0)

The Testnet preview with a major reduction in memory usage for large blobs and sliver-range
information in `store-quilt` output, plus advance notice that the default consistency check changes
in v1.37.

---

### Walrus v1.34.2

`Mainnet` | October 9, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.34.2)

Adds an aggregator `--max-blob-size` option to cap the size of requestable blobs. Note the warning
that blobs are stored as deletable by default starting in v1.33; pass `permanent=true` to keep the
old behavior.

---

### Walrus v1.34.1

`Testnet` | October 1, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.34.1)

The Testnet build adding the aggregator `--max-blob-size` option, with a reminder that blobs become
deletable by default in v1.33 unless `permanent=true` is set.

---

### Walrus v1.33.3

`Mainnet` | September 29, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.33.3)

Adds the aggregator `--max-blob-size` option and marks the switch to deletable-by-default blobs;
pass `permanent=true` (or `--permanent`) to retain permanent storage.

---

### Walrus v1.33.2

`Testnet` | September 25, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.33.2)

Marks the Testnet switch to deletable-by-default blob storage; add `permanent=true` or the
`--permanent` CLI option to keep blobs permanent.

#### Changes

- [**Warning:** Starting with this release (`v1.33`), blobs are stored as deletable by default; if you want to keep the previous behavior, you need to add the `permanent=true` query parameter.](https://github.com/MystenLabs/walrus/pull/2523)
- [**Warning:** Starting with this release (`v1.33`), blobs are stored as deletable by default; if you want to keep the previous behavior, you need to add the `--permanent` option when storing blobs.](https://github.com/MystenLabs/walrus/pull/2523)

---

### Walrus v1.33.1

`Testnet` | September 17, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.33.1)

An earlier Testnet build introducing deletable-by-default blob storage, with `permanent=true` or
`--permanent` available to preserve the previous behavior.

---

### Walrus v1.32.0

`Mainnet` | September 15, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.32.0)

Adds an aggregator endpoint, `/quilts/<quilt-id>/patches`, that lists all patches contained in a
quilt.

#### Changes

- [Added new endpoint in aggregator:  /quilts/\&lt;quilt-id\&gt;/patches to list all patches in a quilt.](https://github.com/MystenLabs/walrus/pull/2405)
- [*Note: This change is an internal code quality improvement and does not affect user-facing functionality. No release notes are required as the changes do not impact the external API or user experience.*](https://github.com/MystenLabs/walrus/pull/2427)

---

### Walrus v1.31.1

`Mainnet` | August 28, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.31.1)

An advance warning that blobs become deletable by default in v1.33; operators who want to keep
permanent storage should begin adding the `permanent=true` query parameter now.

---

### Walrus v1.30.1

`Mainnet` | August 14, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.30.1)

Storage nodes now restart on a TLS certificate update only when the certificate is near expiry or
its subject or extensions have changed, and they emit warning and error logs for nearly expired
certificates.

#### Changes

- [When the TLS certificate is updated, only restart the node if the currently loaded certificate is about to expire or the subject or extensions have changed. Also, add warning and error logs for (almost) expired certificates.](https://github.com/MystenLabs/walrus/pull/2360)

---

### Walrus v1.30.0

`Testnet` | August 6, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.30.0)

The Testnet preview of smarter TLS certificate handling, where a node restarts only on meaningful
certificate changes and logs warnings for nearly expired certificates.

#### Changes

- [When the TLS certificate is updated, only restart the node if the currently loaded certificate is about to expire or the subject or extensions have changed. Also, add warning and error logs for (almost) expired certificates.](https://github.com/MystenLabs/walrus/pull/2360)

---

### Walrus v1.29.2

`Mainnet` | July 31, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.29.2)

Brings Quilt batch storage to Mainnet with new publisher, aggregator, and CLI support for storing
and reading quilts, plus an `--upload-relay` option. Also fixes startup issues with deleted event
databases and lets new nodes start beyond `MAX_EPOCHS_AHEAD`.

#### Changes

- [Fixes an issue that can occur when a node starts with deleted `events` and `event_blob_writer` DBs.](https://github.com/MystenLabs/walrus/pull/2273)
- [New storage nodes can start in epochs later than `MAX_EPOCHS_AHEAD` (53 on the current Testnet and Mainnet) without any additional action.](https://github.com/MystenLabs/walrus/pull/2193)
- [Add a new configuration parameter for the event processor config to control the interval at which sampled logs are emitted, with a default of 1 hour (previously 30s).](https://github.com/MystenLabs/walrus/pull/2290)
- [Add a `permanent` query parameter. **Warning:** In the future, blobs will be stored as deletable by default; to keep the current behavior, you need to add the `permanent=true` query parameter.](https://github.com/MystenLabs/walrus/pull/2281)
- [Added "/v1/quilts" publisher endpoint to store quilts.](https://github.com/MystenLabs/walrus/pull/2279)
- [use subsidies contract to proxy register blob call](https://github.com/MystenLabs/walrus/pull/2151)
- [Add a `--permanent` option to the `store` and `store-quilt` commands. **Warning:** In the future, blobs will be stored as deletable by default; to keep the current behavior, you need to add the `--permanent` option when storing blobs.](https://github.com/MystenLabs/walrus/pull/2281)
- [Add the `--upload-relay` option to the `store` and `store-quilt` commands to allow uploading to Walrus via an [Upload Relay](https://docs.wal.app/operator-guide/upload-relay.html).](https://github.com/MystenLabs/walrus/pull/2384)

---

### Walrus v1.29.1

`Testnet` | July 23, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.29.1)

Introduces Quilt, a batch storage solution for storing and accessing many small files efficiently,
with new `store-quilt`, `read-quilt`, and quilt read endpoints across the CLI, publisher, and
aggregator. Also fixes deleted-event-database startup issues and relaxes the `MAX_EPOCHS_AHEAD`
constraint for new nodes.

#### Changes

- [Fixes an issue that can occur when a node starts with deleted `events` and `event_blob_writer` DBs.](https://github.com/MystenLabs/walrus/pull/2273)
- [New storage nodes can start in epochs later than `MAX_EPOCHS_AHEAD` (53 on the current Testnet and Mainnet) without any additional action.](https://github.com/MystenLabs/walrus/pull/2193)
- [Add a new configuration parameter for the event processor config to control the interval at which sampled logs are emitted with a default of 1 hour (previously 30s).](https://github.com/MystenLabs/walrus/pull/2290)
- [Add a `permanent` query parameter. **Warning:** In the future, blobs will be stored as deletable by default; to keep the current behavior, you need to add the `permanent=true` query parameter.](https://github.com/MystenLabs/walrus/pull/2281)
- [Added "/v1/quilts" publisher endpoint to store quilts.](https://github.com/MystenLabs/walrus/pull/2279)
- [Use subsidies contract to proxy register blob call](https://github.com/MystenLabs/walrus/pull/2151)
- [Add a `--permanent` option to the `store` and `store-quilt` commands. **Warning:** In the future, blobs will be stored as deletable by default; to keep the current behavior, you need to add the `--permanent` option when storing blobs.](https://github.com/MystenLabs/walrus/pull/2281)

---

### Walrus v1.28.4

`Mainnet` | July 15, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.28.4)

Fixes an issue where a blob could be reported as non-existent immediately after certification,
affecting both the aggregator and the CLI.

---

### Walrus v1.28.3

`Testnet` | July 11, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.28.3)

#### Changes

- [Fixes an issue where a blob was sometimes reported as non-existent immediately after certification.](https://github.com/MystenLabs/walrus/pull/2205)

---

### Walrus v1.28.2

`Testnet` | July 10, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.28.2)

Fixes an issue where a blob was sometimes reported as non-existing immediately after certification.

---

### Walrus v1.28.1

`Testnet` | July 2, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.28.1)

Fixes a storage node startup problem with deleted `events` and `event_blob_writer` databases, along
with the post-certification blob existence reporting fix.

---

### Walrus v1.27.2

`Mainnet` | June 26, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.27.2)

Adds a `CheckpointManager` and a node admin socket for direct operator interaction with a running
node, and fixes a crash affecting recovering nodes after restart as well as a checkpoint-lag
miscalculation.

---

### Walrus v1.27.1

`Testnet` | June 18, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.27.1)

The Testnet preview adding the `CheckpointManager` component and node admin socket, plus fixes for
recovering-node restart crashes and inflated checkpoint-lag values.

---

### Walrus v1.26.4

`Mainnet` | June 10, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.26.4)

Adds multi-URL Sui RPC configuration via `rpc_urls` and removes the implicit fallback to the public
Testnet full node. Note the breaking change where metrics and REST API endpoints now bind to the
configured address rather than always to `0.0.0.0`.

---

### Walrus v1.26.3

`Mainnet` | June 5, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.26.3)

Rate-limits concurrent blob recovery during node recovery and adds multi-URL Sui RPC configuration.
Includes the breaking change where services bind to the configured address instead of always to
`0.0.0.0`.

#### Changes

- [feat: rate limit concurrent blob recovery during node recovery](https://github.com/MystenLabs/walrus/pull/2159)
- [Use the metrics and REST API address from the configuration file instead of always binding to `0.0.0.0`. Note that this bugfix may break some setups that relied on the previous incorrect behavior. To get the old behavior, set the `metrics_address` and `rest_api_address` to `0.0.0.0:<port>` explicitly.](https://github.com/MystenLabs/walrus/pull/1630)
- [Increase default value of `http2_max_pending_accept_reset_streams` parameter to disable warnings from the `h2` crate.](https://github.com/MystenLabs/walrus/pull/2072)
- [Reduce verbosity levels of problems with metrics and config sync from ERROR to WARN.](https://github.com/MystenLabs/walrus/pull/2067)
- [Allow specifying Sui RPC URL in the configuration file in addition to the CLI option.](https://github.com/MystenLabs/walrus/pull/2066)
- [Use the metrics address specified through the CLI options file instead of always binding to `0.0.0.0`. Note that this bugfix may break some setups that relied on the previous incorrect behavior. To get the old behavior, use `--metrics-address 0.0.0.0:<port>` when starting the aggregator.](https://github.com/MystenLabs/walrus/pull/1630)
- [Use the metrics address specified through the CLI options file instead of always binding to `0.0.0.0`. Note that this bugfix may break some setups that relied on the previous incorrect behavior. To get the old behavior, use `--metrics-address 0.0.0.0:<port>` when starting the aggregator.](https://github.com/MystenLabs/walrus/pull/1630)
- [Add a new `rpc_urls` field to the client config, which allows setting multiple Sui RPC URLs. The Sui RPC URL(s) used for reads are now selected in the following order: (1) CLI argument, (2) client configuration file, (3) wallet config. If none of these are set, the client will **no longer fall back to the `fullnode.testnet.sui.io`** but instead exit with an error.](https://github.com/MystenLabs/walrus/pull/2066)

---

### Walrus v1.26.2

`Testnet` | May 28, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.26.2)

The Testnet build introducing multi-URL Sui RPC configuration and the breaking change where metrics
and REST API endpoints bind to the configured address rather than always to `0.0.0.0`.

---

### Walrus v1.25.1

`Mainnet` | May 22, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.25.1)

A bug fix adding options to tune compaction write throttling and setting a high write-stalling
threshold on the metadata table.

#### Changes

- [add options to update compaction write throttling, and set high write stalling threshold in metadata table](https://github.com/MystenLabs/walrus/pull/2121)

---

### Walrus v1.25.0

`Mainnet` | May 22, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.25.0)

Speeds up batch blob operations by optimizing storage selection, with no change to the user
workflow.

#### Changes

- [Improved performance for batch blob operations by optimizing storage selection. Operations with multiple blobs are now significantly faster with no change to the user workflow.](https://github.com/MystenLabs/walrus/pull/2010)

---

### Walrus v1.24.2

`Mainnet` | May 12, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.24.2)

Adds a publisher `force` store option and a CLI `completion` command, and substantially reduces SUI
gas costs when registering multiple blobs at once.

#### Changes

- [Improved documentation for shared blob retrieval.](https://github.com/MystenLabs/walrus/pull/2005)
- [Adds the possibility to `force` a store operation, like with the CLI.](https://github.com/MystenLabs/walrus/pull/2015)
- [Major reduction of SUI gas cost when registering multiple blobs simultaneously.](https://github.com/MystenLabs/walrus/pull/2004)
- [Add new `completion` command to generate completion scripts for various shells.](https://github.com/MystenLabs/walrus/pull/2003)

---

### Walrus v1.24.1

`Testnet` | May 6, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.24.1)

The Testnet preview adding a publisher `force` store option and CLI `completion` command, with a
major SUI gas reduction for registering multiple blobs simultaneously.

#### Changes

- [Improved documentation for shared blob retrieval.](https://github.com/MystenLabs/walrus/pull/2005)
- [Adds the possibility to `force` a store operation, like with the CLI.](https://github.com/MystenLabs/walrus/pull/2015)
- [Major reduction of SUI gas cost when registering multiple blobs simultaneously.](https://github.com/MystenLabs/walrus/pull/2004)
- [Add new `completion` command to generate completion scripts for various shells.](https://github.com/MystenLabs/walrus/pull/2003)

---

### Walrus v1.22.1

`Mainnet` | April 25, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.22.1)

Reduces SUI gas costs for simultaneous blob registration, surfaces storage node checkpoint progress
in the `health` command, and fixes default-location Sui wallet creation. Operators must remove the
`experimental_batch_symbol_recovery` config field.

#### Changes

- [Remove the `experimental_batch_symbol_recovery` config variable; operators must ensure that the field is not present in their configs.](https://github.com/MystenLabs/walrus/pull/1976)
- [Fetch the checkpoint progress info for storage nodes, in the health command.](https://github.com/MystenLabs/walrus/pull/1973)
- [Fix a bug that prevented the creation of a Sui wallet at the default location through `walrus generate-sui-wallet`.](https://github.com/MystenLabs/walrus/pull/1943)
- [Major reduction of SUI gas cost when registering multiple blobs simultaneously.](https://github.com/MystenLabs/walrus/pull/2008)

---

### Walrus v1.21.2

`Mainnet` | April 17, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.21.2)

#### Changes

- [Optimized default parameters for shard transfer and recovery.](https://github.com/MystenLabs/walrus/pull/1934)

---

### Walrus v1.21.1

`Testnet` | April 15, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.21.1)

#### Changes

- [Optimized default parameters for shard transfer and recovery.](https://github.com/MystenLabs/walrus/pull/1934)

---

### Walrus v1.20.2

`Mainnet` | April 10, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.20.2)

Adds the latest checkpoint sequence number and an estimated checkpoint lag to the CLI `health`
output.

#### Changes

- [Added `Latest checkpoint sequence number` and `Estimated checkpoint lag` in health output.](https://github.com/MystenLabs/walrus/pull/1805)

---

### Walrus v1.18.2

`Mainnet` | March 25, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.18.2)

Adds TLS certificate monitoring with automatic reboot, new `--network-key-path` and
`--certificate-path` setup options, and a configurable checkpoint request timeout. The publisher's
JWT token now includes a `size` field.

#### Changes

- [Storage node monitors TLS certificate and reboots storage node upon changes.](https://github.com/MystenLabs/walrus/pull/1703)
- [Added optional `--network-key-path` and `--certificate-path` options to the `setup` command, removed `--use-legacy-event-provider` and `--disable-event-blob-writer`. Adjusted defaults for storage prices and commission rates.](https://github.com/MystenLabs/walrus/pull/1715)
- [Adds a configurable timeout to checkpoint requests.](https://github.com/MystenLabs/walrus/pull/1793)
- [Add `size` field of JWTToken](https://github.com/MystenLabs/walrus/pull/1613)

---

### Walrus v1.18.1

`Testnet` | March 20, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.18.1)

The Testnet preview of TLS certificate monitoring with automatic reboot and the new key and
certificate setup options, plus the publisher JWT token `size` field.

#### Changes

- [Storage node monitors TLS certificate and reboots storage node upon changes.](https://github.com/MystenLabs/walrus/pull/1703)
- [Added optional `--network-key-path` and `--certificate-path` options to the `setup` command, removed `--use-legacy-event-provider` and `--disable-event-blob-writer`. Adjusted defaults for storage prices and commission rate.](https://github.com/MystenLabs/walrus/pull/1715)
- [Add `size` field of JWTToken](https://github.com/MystenLabs/walrus/pull/1613)

---

### Walrus v1.17.2

`Testnet` | March 15, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.17.2)

Adds `--network-key-path` and `--certificate-path` options to the `setup` command, removes legacy
event-provider flags, and adjusts default storage prices and commission rates.

---

### Walrus v1.17.1

`Testnet` | March 12, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.17.1)

#### Changes

- [Fixes a bug where an explicitly set wallet path through the `--wallet` CLI option was ignored.](https://github.com/MystenLabs/walrus/pull/1685)

---

### Walrus v1.16.1

`Testnet` | March 5, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.16.1)

Lets the CLI `delete` command accept multiple files, blob IDs, or object IDs (a breaking change to
the delete JSON API) and adds multi-context configuration with a `--context` override. The publisher
now keeps created `Blob` objects by default, replacing `--keep` with `--burn-after-store`.

---

### Walrus v1.15.4

`Testnet` | February 28, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.15.4)

Fixes an issue where keys were not loaded into memory when on-chain config parameters were updated,
and makes the publisher use the correct default encoding type.

---

### Walrus v1.15.3

`Testnet` | February 27, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.15.3)

Adds support for multiple encoding types across the storage node, publisher, and CLI, and lets the
aggregator return allow-listed blob attributes as response headers. Storage nodes can now
auto-reload configuration changes from disk, and the CLI gains `--sort-by` and `--desc` options for
`health` and `info`.

#### Changes

- [Automatically reload the StorageNodeConfig from disk and update the on-chain configuration when changes are detected. Some changes may result in an automatic restart of the storage nodes.](https://github.com/MystenLabs/walrus/pull/1548)
- [Storage node now supports multiple encoding types. Depending on features selected at compile time, it accepts a subset of them.](https://github.com/MystenLabs/walrus/pull/1608)
- [Exposed the aggregator endpoint to allow fetching blobs with associated attributes; if the attribute fields are in the allow-list, they will be included in the response headers.](https://github.com/MystenLabs/walrus/pull/1591)
- [Added `encoding_type` query parameter. Added additional variants for the encoding type.](https://github.com/MystenLabs/walrus/pull/1608)
- [Added the allowed-headers option for daemon commands, allowing operators to specify custom allowed headers to filter key-value pairs from the blob attribute.](https://github.com/MystenLabs/walrus/pull/1591)
- [Added `--sort-by` and `--desc` options for `walrus health` and `walrus info` commands, to sort the storage nodes by node-id, node-name, node-status, or node-url.](https://github.com/MystenLabs/walrus/pull/1594)
- [Fix `walrus store` behavior when blobs can be extended.](https://github.com/MystenLabs/walrus/pull/1606)
- [The CLI supports multiple encoding types for reads and writes. Depending on features selected at compile time, it uses a different default type and accepts a subset of them for writes.](https://github.com/MystenLabs/walrus/pull/1608)

---

### Walrus v1.14.0

`Testnet` | February 19, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.14.0)

Enables batch symbol recovery by default and adds CLI commands to set, get, and remove blob
attributes. The `walrus store` command now extends the lifetime of matching owned blobs instead of
re-registering them.

#### Changes

- [The new batch symbol recovery is enabled by default.](https://github.com/MystenLabs/walrus/pull/1568)
- [Added the following new commands for blob attribute:](https://github.com/MystenLabs/walrus/pull/1546)
- [The walrus store command now checks for matching owned Blob objects and extends their lifetime if necessary instead of registering the blob again.](https://github.com/MystenLabs/walrus/pull/1535)

---

### Walrus v1.13.2

`Testnet` | February 13, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.13.2)

Fixes the blob-certified metric so it updates only for event blobs and makes the publisher retry
when a refresh fails.

#### Changes

- [fix: Update blob certified metric only if it is event blob](https://github.com/MystenLabs/walrus/pull/1576)
- [fix(publisher): retry if refresh fails](https://github.com/MystenLabs/walrus/pull/1575)

---

### Walrus v1.13.1

`Testnet` | February 12, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.13.1)

Reworks TLS configuration by removing the `tls.pem_files` block in favor of `tls.certificate_path`
and `network_key_pair.path`, and enables automatic rotation of the protocol key pair. Adds a CLI
`--ignore-resources` flag.

---

### Walrus v1.12.0

`Testnet` | February 6, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.12.0)

Lets the `walrus health` command accept a list of node IDs or URLs; this is a breaking change for
JSON mode, where `nodeId` and `nodeUrl` become `nodeIds` and `nodeUrls`.

---

### Walrus v1.11.0

`Testnet` | January 29, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.11.0)

Adds a CLI `health` command and the ability to extend owned blobs with WAL, and lets storage node
operators change several config parameters by editing the local file and restarting. Deprecates the
recovery symbol endpoint in favor of a new one.

---

### Walrus v1.10.0

`Testnet` | January 23, 2025 | [GitHub](https://github.com/MystenLabs/walrus/releases/tag/testnet-v1.10.0)

Makes storage node error responses machine-readable, adds error statuses and richer OpenAPI schemas
to the aggregator and publisher, and introduces an optional JWT auth layer for the publisher. Also
adds a `walrus-backup` entrypoint and several CLI fixes and options, including staking with multiple
nodes.

#### Changes

- [Update error responses returned by storage nodes to be machine-readable and contain more details](https://github.com/MystenLabs/walrus/pull/1407)
- [Add error statuses to the responses of failed requests; update endpoint URLs; and add more detailed OpenAPI schemas.](https://github.com/MystenLabs/walrus/pull/1407)
- [Add error statuses to the responses of failed requests; update endpoint URLs; and add more detailed OpenAPI schemas.](https://github.com/MystenLabs/walrus/pull/1407)
- [Add optional JWT auth layer for the publisher](https://github.com/MystenLabs/walrus/pull/1345)
- [This fixes the cost computation for the `store --dry-run` command.](https://github.com/MystenLabs/walrus/pull/1402)
- [Allows staking with multiple nodes simultaneously.](https://github.com/MystenLabs/walrus/pull/1404)
- [Fix serialization of JSON output for the `walrus store` command.](https://github.com/MystenLabs/walrus/pull/1421)
- [Adds the option to specify `max` as the number of epochs.](https://github.com/MystenLabs/walrus/pull/1425)
- [changes to the syntax and output of the `info` command](https://github.com/MystenLabs/walrus/pull/1388)
- [an entrypoint called walrus-backup will now be included in the build process. It has minimal (tailing Walrus-related Sui events) functionality at this point.](https://github.com/MystenLabs/walrus/pull/1397)

---