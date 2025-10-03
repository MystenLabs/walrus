# Encoding, overheads, and verification

Walrus uses a bespoke erasure-code construction (RedStuff) based on efficiently computable
Reed-Solomon codes. The full details of the encoding are described in the
[whitepaper](../walrus.pdf). This page summarizes some of the basic techniques and terminology used
by Walrus. See also the [glossary](../glossary.md).

## Basics

The following list summarizes the basic encoding and cryptographic techniques used in Walrus:

- An [erasure code](https://en.wikipedia.org/wiki/Erasure_code) encode algorithm takes a blob,
  splits it into a number \(k\) of symbols, and encodes it into \(n>k\) symbols in such a way that a
  subset of these \(n\) symbols can be used to reconstruct the blob.

- Walrus uses a highly efficient erasure code and selects \(k\) such that a third of symbols can be
  used to reconstruct the blob by the decode algorithm.

- The encoding is *systematic*, meaning that some storage nodes hold part of the original blob,
  allowing for fast random-access reads.

- All encoding and decoding operations are deterministic, and encoders have no discretion about it.

- For each blob, multiple symbols are combined into a **sliver**, which is then assigned to a shard.

- Storage nodes manage one or more shards, and corresponding slivers of each blob are distributed
  to all the storage shards.

The detailed encoding setup results in an expansion of the blob size by a factor of \(4.5 \sim 5\).
This is independent of the number of shards and the number of storage nodes.

## Metadata and data authentication

Each blob is also associated with some metadata including a **blob ID** to allow data authenticity
verification:

- The blob ID is computed as an authenticator of the set of all shard data and metadata (byte size,
  encoding, blob hash).

  Walrus hashes a sliver representation in each of the shards and adds the resulting hashes into a
  Merkle tree. Then the root of the Merkle tree is the blob hash used to derive the blob ID that
  identifies the blob in the system.

- Each storage node can use the blob ID to check if some shard data belongs to a blob using the
  authenticated structure corresponding to the blob hash (Merkle tree). A successful check means
  that the data is indeed as intended by the writer of the blob.

## Data integrity and consistency

Slivers, metadata, and the blob ID are computed by the client when writing a blob. As clients are
generally untrusted, any step in this computation can be incorrect, either through a bug or on
purpose. Concretely, the client can make one or more of the following mistakes:

1. Incorrectly compute slivers.
1. Incorrectly compute the sliver hashes based on the slivers.
1. Incorrectly compute the blob ID based on the sliver hashes and other metadata.

Walrus has several mechanisms to detect each of these:

- Inconsistencies of the last type (incorrect blob-ID computation) are detected immediately when the
  client attempts to upload metadata to storage nodes. Such blobs are never even certified.

- Inconsistencies of the second type (incorrect sliver-hash computation) are generally detected
  either before or shortly after certification.

  In the latter case, storage nodes can extract one symbol per sliver to form an *inconsistency
  proof*, which is then used to mark the blob as *invalid*. After this, storage nodes can delete
  slivers belonging to inconsistently encoded blobs, and upon request return either the
  inconsistency proof or an inconsistency certificate posted on chain.

- Inconsistencies of the first type can also be detected by storage nodes. The process is the same
  as in the case above, but it is not guaranteed to be triggered immediately after certification.

  As a result, such inconsistencies may persist within the network for longer periods of time,
  requiring additional client-side checks. These integrity checks performed by clients are described
  in the following sections.

### Data integrity check

When reading a blob from Walrus, a client first checks the authenticity of the metadata and then
requests a subset of primary slivers checking their authenticity using the metadata. Using 334
authentic primary slivers, the original blob data can be decoded.

The encoding used by Walrus has the property that *the first 334 primary slivers contain the
(potentially padded) unencoded data*. This means that the (cryptographic) hashes of these slivers
together with the blob length uniquely determine the content of the blob. Therefore, by recomputing
the sliver hashes of the first 334 slivers and checking them against the metadata, the client can
verify that the decoded data is correct. If this check succeeds, the data is provided to the user,
otherwise an error is returned. This check provides the following guarantee:

```admonish tip title="Data integrity property"
Any correct client attempting to read a blob will either read the specific value authenticated by
the writer or return an error.
```

### Strict integrity check

If the blob was encoded correctly, any set of 334 primary slivers decode to the same data. However,
if the writer of a blob encoded the data incorrectly, different sets of slivers may decode to
different data. Even in that case, there is only one correct value that can be read by an honest
client (guarantee stated above), but some read attempts may result in a failure while others
succeed. Furthermore, the blob may be detected as inconsistent by storage nodes at a later point,
after which all read attempts would fail.

For this reason, Walrus optionally offers a *strict integrity check*: After decoding the blob, the
client fully *re-encodes* it and recomputes all hashes and the blob ID. Only if that computation
results in the blob ID they are trying to read is the read considered successful.

This process *ensures that the original writer encoded the blob correctly*. Therefore, after a blob
was read successfully with the strict integrity check enabled, the following property holds:

```admonish tip title="Guarantee after strict integrity succeeded"
Any correct client attempting to read a blob during its lifetime will always succeed and read the
same data intended by the writer.
```

```admonish info title="Select the appropriate integrity check"
In the majority of cases, the "basic" integrity check above is sufficient, in particular if the
writer of the blob is trusted. The strict integrity check is only needed if specific availability
guarantees are required.
```
