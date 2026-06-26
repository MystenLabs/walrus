> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Quilt is a batch storage feature designed to optimize the storage cost and efficiency of large numbers of small blobs. Before Quilt, storing small blobs (less than 10 MB) on Walrus involved higher per-byte costs due to internal system data overhead. Quilt addresses this by encoding multiple blobs (up to 666 for QuiltV1) into a single unit called a **quilt**, significantly reducing Walrus storage overhead and lowering costs to purchase Walrus and Sui storage, as well as Sui computation gas fees.

Each blob within a quilt can be accessed and retrieved individually without downloading the entire quilt. The blob boundaries in a quilt align with Walrus internal structures and Walrus storage nodes, allowing for retrieval latency that is comparable to, or lower than, that of a regular blob.

Quilt introduces custom, immutable Walrus-native blob metadata, allowing you to assign different types of metadata to each blob in a quilt, for example, unique identifiers and tags of key-value pairs. This metadata is functionally similar to the existing blob metadata store onchain, but there are some fundamental distinctions. Walrus-native metadata is stored alongside the blob data, which reduces costs and simplifies management. This metadata can also be used for efficient lookup of blobs within a quilt, for example, reading blobs with a particular tag. When storing a quilt, you can set the Walrus-native metadata using the Quilt APIs.

> **Warning**
>
> An identifier must start with an alphanumeric character, contain no trailing whitespace, and not exceed 64 KiB in length.
> 
> The total size of all tags combined must not exceed 64 KB.
## Important considerations

### Per-blob size limit

Each individual blob within a quilt is limited to approximately 4 GiB. This limit is separate from the maximum blob size shown by `walrus info`, which applies to regular blobs and to the quilt as a whole. The per-blob limit comes from the quilt's internal header format, which uses a 4-byte field to store each blob's length. A small amount of this space is used for per-blob metadata (identifier and tags), so the usable data capacity is slightly less. You can check this limit by running `walrus info` and looking for the "Maximum blob size in quilt" field.

If you need to store data larger than 4 GiB, store it as a regular blob instead of within a quilt.

### Quilt patch IDs

Blobs stored in a quilt are assigned a unique ID called `QuiltPatchId`, which differs from the `BlobId` used for regular Walrus blobs. A `QuiltPatchId` is determined by the composition of the entire quilt rather than the single blob, so it can change if the blob is stored in a different quilt. Individual blobs cannot be deleted, extended, or shared separately. These operations can only be applied to the entire quilt.

## Target use cases

Using Quilt requires minimal additional effort beyond standard procedures. The primary consideration is that the unique ID assigned to each blob within a quilt cannot be directly derived from its contents.

### Lower cost

Quilt is especially advantageous for managing large volumes of small blobs, as long as they can be grouped together. The cost savings come from 2 sources:

- **Walrus storage and write fees:** By consolidating multiple small blobs into a single quilt, storage costs can be reduced dramatically — more than 400x for files around 10 KiB — making it an efficient solution for cost-sensitive applications.

- **Sui computation and object storage fees:** Storing many blobs as a single quilt significantly reduces Sui gas costs. In test runs with 600 files stored in a quilt, 238x savings in Sui fees were observed compared to storing them as individual blobs. Sui cost savings depend only on the number of files per quilt rather than the individual file sizes.

The following table demonstrates the potential cost savings in WAL when storing 600 small blobs for 1 epoch as a quilt compared to storing them as separate blobs.

| Blob size | Regular blob storage cost | Quilt storage cost | Cost saving factor |
|----------:|--------------------------:|-------------------:|-------------------:|
|      10KiB |                 2.088 WAL |          0.005 WAL |               409x |
|      50KiB |                 2.088 WAL |          0.011 WAL |               190x |
|     100KiB |                 2.088 WAL |          0.020 WAL |               104x |
|     200KiB |                 2.088 WAL |          0.036 WAL |                58x |
|     500KiB |                 2.136 WAL |          0.084 WAL |                25x |
|       1MiB |                 2.208 WAL |          0.170 WAL |                13x |

> **Info**
>
> The costs shown in this table are for illustrative purposes only and were obtained from test runs on Walrus Testnet. Actual costs can vary due to changes in smart contract parameters, networks, and other factors. The comparison is between storing 600 files as a single quilt versus storing them as individual blobs in batches of 25.
### Store agent memory

AI agents tend to produce a steady stream of small, independent writes: individual conversation turns, tool call outputs, embedding vectors, and periodic state checkpoints. Each item is small on its own, but an agent can generate them continuously, so the count grows quickly.

Storing each item as its own Walrus blob means every item pays the same fixed overhead regardless of its size: onchain registration on Sui, blob metadata, and the minimum overhead of erasure coding. For tiny blobs, this fixed per-blob cost dominates the cost of the data itself, so writing many small blobs individually means paying that overhead over and over.

Quilt addresses this by batching many small items into a single Walrus blob, so the per-blob overhead is paid once for the batch rather than once for each item. Each item remains individually retrievable by its identifier or `QuiltPatchId`, so the agent can still read back a single turn, output, or checkpoint without reconstructing the whole batch. This makes Quilt a good fit for accumulating agent memory in batches, for example flushing a buffer of recent turns or a group of embeddings as one quilt.

### Organize collections

Quilt provides a straightforward way to organize and manage collections of small blobs within a single unit. This can simplify data handling and improve operational efficiency when working with related small files, such as NFT image collections.

### Walrus-native blob metadata

Quilt supports immutable, custom metadata stored directly in Walrus, including identifiers and tags. These features facilitate better organization, enable flexible lookup, and assist in managing blobs within each quilt, improving retrieval and management.

For details on how to use the CLI to interact with Quilt, see the [Batch-storing blobs with quilts](/docs/walrus-client/storing-blobs#batch-store) section.

## When to use Quilt

Quilt fits workloads that generate many small blobs you can group and write together. Whether Quilt is the right choice depends on how each item is written, retrieved, and retired.

Use Quilt when:

- You write many small blobs that you can group into batches, such as agent memory or other collections of related small files.
- The items in a batch share a lifetime, so you can store, extend, and eventually retire them together.
- You want individual retrieval and Walrus-native metadata (identifiers and tags) without paying per-blob overhead for every item.

Use a regular blob instead when:

- A single item is large on its own. Store data that approaches or exceeds the per-blob size limit as a regular blob. For more information, see [Important considerations](#important-considerations).
- Items need independent, one-at-a-time lifetimes. The `delete`, `extend`, and `share` operations apply to the whole quilt rather than to individual items within it, so an item you need to delete, extend, or share on its own schedule should be a regular blob.
- You need each item addressed by a content-derived blob ID. An item inside a quilt is retrieved by its `QuiltPatchId`, which depends on the composition of the whole quilt and is not derived from the item's contents.