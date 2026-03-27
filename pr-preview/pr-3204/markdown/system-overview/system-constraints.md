This page describes the practical limits and constraints you should consider when designing applications on Walrus. For current values, run `walrus info`.

## Blob size

Walrus supports blobs up to approximately 13.6 GiB. Check the current limit with `walrus info` under "Maximum blob size."

If your data exceeds this limit, learn more about [best practices for large data uploads](/docs/large-uploads).

## Memory requirements

Encoding and decoding blobs requires significant memory.

- **Encoding (upload):** Requires approximately 2-3x the blob size in available RAM.

- **Decoding (retrieval):** Requires approximately 1.5-2x the blob size in available RAM.

For example, encoding a 5 GB blob requires 10 to 15 GB of available RAM.

If the system runs out of memory during encoding, the operation fails with an error such as `Out of memory` or `Failed to allocate buffer`. Split large blobs into smaller chunks to reduce memory requirements, or run the CLI on a machine with more RAM.

Aggregators should be provisioned with enough memory to handle the largest blobs they are expected to reconstruct.

## Rate limiting

### Storage node rate limits

Storage nodes might rate-limit requests to prevent abuse. If you encounter `HTTP 429` or `Too many requests` errors, implement exponential backoff in your retry logic. Using publishers and aggregators avoids direct interaction with storage nodes and handles rate limiting internally.

### Sui RPC rate limits

Public Sui RPC endpoints have request quotas. If you encounter `RPC rate limit exceeded` errors, configure multiple RPC endpoints for failover, cache blockchain queries where possible, or use a paid RPC service for production workloads.

## Network bandwidth

Upload and download times are proportional to blob size and network speed. Walrus encodes data with approximately 4.5x expansion, so uploading a 1 GB blob transmits roughly 4.5 GB of encoded data across the network.

To reduce upload latency:

- Use a publisher to offload encoding. The client uploads the raw blob once, and the publisher handles encoding and sliver distribution.

- Compress data before uploading if your use case allows it. Compression reduces the stored size, which reduces both storage cost and upload time.

- Show progress indicators in your application for large uploads.

For downloads, aggregators cache frequently accessed blobs. If your application serves the same blobs to many users, place an aggregator with caching closer to your users to reduce latency.

## Epoch transitions

Storage node shard assignments change every epoch. During transitions, nodes migrate data to new shard owners. This process is automatic but can briefly affect retrieval latency. In rare cases, some slivers might be temporarily unavailable until migration completes.

Applications should implement retry logic to handle transient failures during epoch transitions. Avoid assuming instant retrieval at all times.

## Byzantine fault tolerance assumptions

Walrus guarantees hold as long as more than 2/3 of storage shards (by stake weight) are managed by honest storage nodes. The system tolerates up to 1/3 of shards being controlled by faulty or malicious nodes.

This assumption applies both within individual storage epochs and across epoch transitions. Under normal operation, the Sui staking mechanism and economic incentives maintain this property.

For data with extreme durability requirements, consider maintaining additional off-Walrus backups.

## Public infrastructure availability

Public publishers and aggregators do not have formal availability guarantees. They might go offline, experience performance issues, or enforce rate limits.

For production applications:

- Do not rely on a single publisher or aggregator.
- Run your own publisher and aggregator infrastructure.
- Implement failover across multiple endpoints.
- Maintain the ability to fall back to direct CLI or SDK usage.