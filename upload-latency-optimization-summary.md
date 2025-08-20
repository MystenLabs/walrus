# Upload Latency Optimization: Multi-Put and Cached Upload

## Executive Summary
This branch implements two major optimizations to reduce blob upload latency in Walrus: **Multi-Put batching** and **Cached blob upload**. These changes enable parallel processing of multiple blobs and eliminate redundant work during the upload pipeline, significantly improving throughput and reducing end-to-end latency.

## 1. Multi-Put Batching

### Problem
Previously, blob uploads were processed sequentially - each blob required separate HTTP requests to storage nodes for metadata and sliver storage, creating significant network overhead and limiting throughput.

### Solution
Implemented a batched multi-put API that allows sending multiple blobs to storage nodes in a single HTTP request:

- **API Changes**: Added `MultiPutRequest`/`MultiPutResponse` structures in `walrus-storage-node-client` to batch multiple blob bundles
- **Client Implementation**: New `reserve_and_store_encoded_blobs_parallel` method in SDK client that:
  - Groups sliver pairs by destination storage node
  - Constructs `MultiPutBundle` for each blob containing metadata and relevant slivers
  - Sends all blobs destined for a node in a single batched request
  - Processes responses in parallel across all nodes

### Benefits
- **Reduced Network Overhead**: One HTTP request per node instead of one per blob
- **Improved Throughput**: Multiple blobs processed simultaneously
- **Better Resource Utilization**: Amortizes connection setup and TLS handshake costs

## 2. Cached Blob Upload

### Problem
The blob upload process requires coordination between two asynchronous events:
1. Blob data arrival (slivers and metadata from client)
2. Blob registration event (from blockchain)

Previously, these were processed sequentially, causing unnecessary delays when data arrived before registration.

### Solution
Implemented `CachedBlobManager` that decouples data arrival from registration:

- **State Management**: Tracks blob state through lifecycle:
  - `DataOnly`: Data arrived, waiting for registration
  - `RegisteredOnly`: Registration arrived, waiting for data
  - `Processing`: Both available, computing storage confirmation
  - `Completed`: Confirmation cached for deduplication

- **Parallel Processing**: When both data and registration are available, immediately triggers background processing
- **Deduplication**: Caches successful confirmations to handle duplicate requests without reprocessing

### Key Components
- **Multi-Put Handler** (`cache_blob_slivers`): Accepts batched blob data, stores in cache, returns futures that resolve when registration arrives
- **Registration Handler**: Processes blockchain events, triggers processing for cached data
- **Background Processing**: Asynchronously stores metadata, writes slivers, and computes confirmations

### Benefits
- **Reduced Latency**: Processing begins immediately when both conditions are met
- **Improved Parallelism**: Multiple blobs process concurrently
- **Server-Side Deduplication**: Cached confirmations eliminate redundant work

## 3. Supporting Changes

### BlobPersistence Simplification
- Replaced `BlobPersistenceType` with simpler `BlobPersistence` enum in `MultiPutBundle`
- New type only indicates Permanent/Deletable without requiring object_id upfront
- Simplifies API for clients setting up multi-put bundles

### Implementation Details
- **Graceful Shutdown**: CancellationToken ensures clean termination of background tasks
- **Memory Management**: Automatic cache cleanup after configurable duration
- **Error Handling**: Failed processing removes entries from cache to allow retries

## Performance Impact

The combined optimizations enable:
- **Parallel blob upload**: Multiple blobs sent to all storage nodes simultaneously
- **Overlapped processing**: Data storage can begin before blockchain registration completes
- **Batched network operations**: Significantly reduced HTTP request count
- **Zero redundant work**: Deduplication via cached confirmations

These improvements are particularly effective for workloads involving multiple blob uploads, where the batching and caching effects compound to deliver substantial latency reductions.

## Code Locations
- Multi-Put API: `/crates/walrus-storage-node-client/src/api.rs`
- Parallel Upload: `/crates/walrus-sdk/src/client.rs::reserve_and_store_encoded_blobs_parallel`
- Cached Manager: `/crates/walrus-service/src/node/cached_blob_manager.rs`
- Node Integration: `/crates/walrus-service/src/node.rs::cache_blob_slivers`
