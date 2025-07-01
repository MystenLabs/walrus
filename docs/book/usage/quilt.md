# Quilt: Walrus Native Batch Store Tool

Quilt is designed to optimize the storage cost and efficiency of large numbers of small blobs
by enabling batch store of up to 666 blobs within a single unit called a quilt. Prior to
Quilt, storing small blobs (less than 10MB) in Walrus involved higher per-byte costs due
to internal system data overhead. Quilt addresses this by encoding multiple small blobs
into a single quilt, significantly reducing storage overhead and lowering costs to purchase
Walrus storage as well as Sui gas fees.

Although stored together as one unit, each blob within a quilt can be accessed and
retrieved individually, without needing to download the entire quilt. This allows for
retrieval latency that is comparable to, or even lower than, that of a regular blob.

Quilt introduces custom, immutable Walrus-native blob metadata, distinct from the
currently used on-chain metadata. First, by storing this metadata alongside the blob
data itself, it reduces costs and simplifies management. Second, this metadata enables
efficient lookup of blobs within a quilt, for example, reading blobs with a particular
tag. When storing a quilt, you can set Walrus-native metadata for each individual blob,
including assigning unique identifiers and arbitrary key-value tags.

## Important Considerations

It's important to note that blobs stored in a quilt are assigned a unique ID that differs
from the one used for regular Walrus blobs, and this ID may change if the blob is moved
to a different quilt. Moreover, individual blobs cannot be deleted, extended or shared
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
