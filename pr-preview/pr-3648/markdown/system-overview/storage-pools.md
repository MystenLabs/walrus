> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

A storage pool is a single storage reservation that many blobs share. In the standard flow, every blob you store owns its own storage resource, bought for that blob's exact size and lifetime. With a storage pool, you instead reserve a block of encoded capacity for a range of epochs once, then register any number of blobs against it. Each registration pays only the one-off write fee, and deleting a deletable blob frees its capacity for the next blob to reuse. This makes pools a good fit for workloads that continuously write and retire blobs against a stable amount of total storage.

> **Warning**
>
> Storage pools are a preview feature. They are currently available through the Rust SDK (`walrus-sdk` and `walrus-sui` crates) and the Move contracts. The `walrus` CLI, the HTTP publisher and aggregator APIs, and the TypeScript SDK do not expose storage pool operations yet.
## How storage pools work

A storage pool is a Sui object that wraps a storage resource with a fixed encoded capacity and a lifetime of `[start_epoch, end_epoch)`. Blobs stored in a pool are represented by `PooledBlob` objects that live inside the pool rather than as independently owned `Blob` objects. The pool tracks its reserved capacity, the encoded bytes currently in use, and the number of registered blobs.

Pooled blobs differ from regular blobs in a few important ways:

- **Shared lifetime.** A pooled blob has no storage resource of its own; it is backed entirely by the pool. All blobs in a pool expire together at the pool's end epoch, and extending the pool extends every blob in it. There is no per-blob extension.
- **Reusable capacity.** Deleting a deletable pooled blob immediately frees its encoded size for reuse within the pool, without any onchain splitting or merging of storage resources.
- **Not independently owned.** A `PooledBlob` is only reachable through its pool, so it cannot be transferred or shared on its own. Whoever controls the pool object controls all blobs in it.
- **Identical reads.** A pooled blob has a normal blob ID, so [reading it](/docs/walrus-client/reading-blobs) works exactly like reading any other blob, through the CLI, an aggregator, or any SDK.

Capacity is measured in *encoded* bytes: the size of a blob after erasure coding, which is roughly five times the original size plus a fixed metadata overhead. See [storage costs](/docs/system-overview/storage-costs) for how encoded size is calculated.

## Costs

Storage pools use the same two prices as regular blobs, but split them differently across operations:

- **Storage fee.** You pay the per-epoch storage price for the pool's full reserved capacity when you create it, and again when you extend its lifetime or increase its capacity. You pay for what you reserve, whether or not blobs fill it.
- **Write fee.** Each blob registration pays only the one-off write fee for its encoded size. This is the entire marginal cost of adding a blob to a pool that already has room.

There are no refunds. Deleting a blob frees capacity for reuse inside the pool but does not return WAL, and shrinking a pool's unused capacity returns a storage resource object that you can use elsewhere, not WAL.

## When to use a storage pool

Use a storage pool when:

- You continuously write and delete blobs, and your total footprint stays within a predictable bound. The pool absorbs the churn while capacity is reused in place.
- You want to pre-purchase storage once and make each subsequent write as cheap as possible, paying only the write fee per blob.
- Your blobs share a lifetime, so expiring and extending them as one unit is acceptable.

Prefer regular blobs when blobs need independent lifetimes, when you need to transfer or share individual blob objects, or when your capacity needs are unpredictable, because reserved but unused pool capacity still costs the full storage fee. If your goal is to reduce the cost of many small blobs rather than to reuse capacity, [Quilt](/docs/system-overview/quilt) is usually the better tool, and the two can be combined by storing quilts in a pool.

## Pool operations

The `walrus::system` Move module exposes the full set of pool operations, and `SuiContractClient` in the `walrus-sui` crate provides Rust bindings for the most common ones.

| Operation | What it does |
|---|---|
| `create_storage_pool` | Buys a new pool with the given encoded capacity and lifetime, paying the storage fee. |
| `register_pooled_blob` | Adds a blob to the pool, paying only the write fee. Fails if the pool lacks capacity. |
| `certify_pooled_blob` | Certifies a registered pooled blob with a storage confirmation certificate. |
| `delete_pooled_blob` | Removes a deletable blob and frees its capacity for reuse. |
| `extend_storage_pool` | Extends the pool's end epoch, paying the storage fee for the full capacity over the added epochs. Extends every blob in the pool. |
| `increase_storage_pool_capacity` | Adds encoded capacity, paying the storage fee for the remaining epochs. |
| `decrease_storage_pool_capacity_by_size` | Splits unused capacity out of the pool and returns it as a storage resource (Move only). |
| `burn_expired_pooled_blob` | Cleans up blob objects after the pool expires, regardless of deletability (Move only). |
| `destroy` | Destroys an empty pool and returns its underlying storage resource (Move only). |

## Store blobs in a pool with the Rust SDK

The following example creates a pool and stores a blob in it. The `reserve_and_store_blobs_in_storage_pool` method handles encoding, registration, sliver upload, and certification in one call.

```rust
use walrus_sdk::node_client::{
    StoreBlobsInStoragePoolApi,
    responses::PooledBlobStoreResult,
};

// Create a pool with 100 MiB of encoded capacity for 10 epochs.
// This pays the storage fee from the client's wallet.
let pool_id = client
    .sui_client()
    .create_storage_pool(100 * 1024 * 1024, 10)
    .await?;

// Store blobs in the pool; each blob pays only the write fee.
let results = client
    .reserve_and_store_blobs_in_storage_pool(
        vec![blob_data],
        pool_id,
        &StoreArgs::default_with_epochs(5),
    )
    .await?;

for result in &results {
    if let PooledBlobStoreResult::NewlyCreated { pooled_blob_object } = result {
        println!("stored blob {}", pooled_blob_object.blob_id);
    }
}
```

The store call enforces a few rules that differ from the regular store path:

- The pool's end epoch must already cover the requested `epochs_ahead`; otherwise the call fails with `StoragePoolInsufficientLifetime`. The SDK never extends a pool's lifetime automatically, so call `extend_storage_pool` first if needed.
- If the pool lacks capacity for the new blobs, the SDK automatically calls `increase_storage_pool_capacity`, which pays the storage fee for the additional capacity from your wallet.
- The call does not automatically retry across an epoch change, because pooled registration is not idempotent and a blind retry could register duplicate blobs. If it fails with a committee-change error, check the pool state before retrying.
- Only `PostStoreAction::Keep` is supported, because pooled blobs live inside the pool and cannot be transferred or shared.

You can inspect a pool at any time with `storage_pool_status`, which returns its lifetime, reserved capacity, used bytes, and blob count, and delete blobs with `delete_pooled_blob`:

```rust
let status = client.sui_client().storage_pool_status(pool_id).await?;
println!(
    "pool has {} of {} encoded bytes used across {} blobs, expires at epoch {}",
    status.used_encoded_bytes,
    status.reserved_encoded_capacity_bytes,
    status.blob_count,
    status.end_epoch,
);

// Free the blob's capacity for reuse (deletable blobs only).
client.sui_client().delete_pooled_blob(pool_id, blob_id).await?;
```

## Call the Move contracts directly

All pool operations are public functions on the `walrus::system` module, so you can compose them in programmable transaction blocks. The Move layer also offers a few capabilities the Rust SDK does not expose yet:

- `create_storage_pool_with_storage` builds a pool from a storage resource you already own, and `increase_storage_pool_capacity_with_storage` grows a pool by absorbing one, both without further payment.
- The `storage_pool` module provides a metadata API to attach key-value metadata to individual pooled blobs.
- The separate `blob_bucket` package wraps a pool in a shared object gated by a `BlobBucketCap`, so an application can operate a pool as shared onchain infrastructure while keeping mutations restricted to the capability holder.

Because a `StoragePool` object has no built-in access control, anyone who can pass a mutable reference to it can register, delete, and mutate blobs in it. Keep pools address-owned, or wrap them in an access-controlled object such as a `BlobBucket`, rather than sharing them directly.

## Constraints and considerations

- A blob ID can be registered only once per pool. Registering the same content in two pools creates two independent `PooledBlob` objects with the same blob ID, and the data remains readable until the last certified reference expires or is deleted.
- Blobs registered as permanent cannot be deleted while the pool is active; their capacity is only reclaimable after the pool expires. Register blobs as deletable if you want to reuse their capacity.
- Once a pool expires, all mutations are rejected; the remaining lifecycle steps are burning the expired blob objects and destroying the empty pool to recover its storage resource for accounting purposes.
- A pool's remaining lifetime plus any extension cannot exceed the system's maximum epochs ahead, the same bound that applies to [regular blob lifetimes](/docs/walrus-client/managing-blobs).
- Storage pools currently support only the RS2 encoding type, which is the default.