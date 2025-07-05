# Quilt: Walrus Native Batch Store Tool

Quilt is a batch storage feature, aiming to optimize the storage cost and efficiency of large
numbers of small blobs. Prior to Quilt, storing small blobs (less than 10MB) in Walrus
involved higher per-byte costs due to internal system data overhead. Quilt addresses this by
encoding multiple (up to 666 for QuiltV1) blobs into a single unit called a quilt, significantly
reducing Walrus storage overhead and lowering costs to purchase Walrus/Sui storage, as well as Sui
computation gas fees.

Importantly, each blob within a quilt can be accessed and retrieved individually, without needing
to download the entire quilt. Moreover, the blob boundaries in a quilt aligns with Walrus
internal structures as well as Walrus storage nodes, this allows for retrieval latency that is
comparable to, or even lower than, that of a regular blob.

In addition, Quilt introduces custom, immutable `Walrus-native` blob metadata, allowing user to
assign different types of `metadata` to each blob in a quilt, for example, unique identifiers, as
well as tags of key-value pairs. Note, this metadata is functionally similar to the existing
Blob Metadata store on-chain, however, there are some fundamental distinctions. First,
`Walrus-native` metadata is stored alongside the blob data, and hence it reduces costs and
simplifies management. Second, this metadata can be used for efficient lookup of blobs within a
quilt, for example, reading blobs with a particular tag. When storing a quilt, users can set
the `Walrus-native` metadata via the Quilt Apis.

## Important Considerations

It's important to note that blobs stored in a quilt are assigned a unique ID, called `QuiltPatchId`,
that differs from the `BlobId` used for regular Walrus blobs, and a `QuiltPatchId` is determined by
the composition of the entire quilt, ranther than the single blob, and hence it may change if the
blob is stored in a different quilt. Moreover, individual blobs cannot be deleted, extended or shared
separately; these operations can only be applied to the entire quilt.

## Target Use Cases

Using Quilt requires minimal additional effort beyond standard procedures. The primary
considerations are that the unique ID assigned to each blob within a quilt cannot be
directly derived from its contents, unlike a regular Walrus blob_id, deletion, extension
and share operations are not allowed on individual blobs in the quilt, only the quilt
can be the target.

### Lower Cost

This is the most clear and significant use case. Quilt is especially advantageous for
managing large volumes of small blobs, as long as they can be grouped together by the
user. By consolidating multiple small blobs into a single quilt, storage costs can be
reduced dramatically—up to 50 times for files around 100KB—making it an efficient
solution for cost-sensitive applications.

The following table demonstrates the potential cost savings when blobs are stored with
Quilt:

> **Note:** The costs shown in this table are for illustrative purposes only. Actual
> costs may vary due to changes in smart contract parameters, network conditions, and
> other factors.

| # | Blob Size | # of Blobs | Regular Blob Cost¹ | Quilt Cost | Cost Saving Factor |
|---|-----------|------------|-------------------|------------|-------------------|
| 1 | 10KB | 600 | 1.725 SUI + 2.088 WAL | 0.00725 SUI + 0.0051 WAL | SUI: 238x WAL: 409x |
| 2 | 50KB | 600 | 1.725 SUI + 2.088 WAL | 0.00725 SUI + 0.011 WAL | SUI: 238x WAL: 190x |
| 3 | 100KB | 600 | 1.725 SUI + 2.088 WAL | 0.00725 SUI + 0.02 WAL | SUI: 238x WAL: 104x |
| 4 | 200KB | 600 | 1.869 SUI + 2.088 WAL | 0.00725 SUI + 0.036 WAL | SUI: 238x WAL: 58x |
| 5 | 500KB | 600 | 2.085 SUI + 2.136 WAL | 0.00725 SUI + 0.084 WAL | SUI: 238x WAL: 25x |
| 6 | 1MB | 600 | 2.085 SUI + 2.208 WAL | 0.00725 SUI + 0.17 WAL | SUI: 238x WAL: 13x |

*Table: Cost savings when storing 600 blobs of the same size for 1 epoch, comparing
the traditional approach (batching 25 blobs at a time) versus using Quilt.*

¹ Regular blob costs are calculated based on batching 25 blobs at a time.

As shown in the table, Quilt provides significant cost savings for small blobs, with
the greatest benefits for blobs under 100KB where both SUI and WAL costs can be
reduced by over 50x.

### Organizing Collections

Quilt provides a straightforward way to organize and manage collections of small blobs
within a single unit. This can simplify data handling and improve operational efficiency
when working with related small files, such as NFT image collections.

### Walrus Native Blob Metadata

Quilt supports immutable, custom metadata stored directly in Walrus, including
identifiers and tags. These features facilitate better organization, enable flexible
lookup, and assist in managing blobs within each quilt, enhancing retrieval and
management processes.

For details on how to use the CLI to interact with quilts, see the
[CLI documentation](./client-cli.md#batch-storing-blobs-with-quilt).
