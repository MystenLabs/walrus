> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

This glossary defines key terms used throughout the Walrus documentation, covering storage concepts,
erasure coding, cryptographic primitives, token economics, and network roles.

**Storage node**: Entity storing data for Walrus; holds one or several *shards*.

**Blob**: Single unstructured data object stored on Walrus.

**Permanent blob**: Blob that cannot be deleted by its owner and is guaranteed to be available until at least its expiry epoch (assuming it is valid).

**Deletable blob**: Blob that can be deleted by its owner at any time to reuse the storage resource.

**Shard**: (Disjoint) Subset of erasure-encoded data of all blobs; at every point in time, a shard is assigned to and stored on a single storage node.

**RedStuff**: Erasure-encoding approach, which uses two different encodings (primary and secondary) to enable shard recovery; details are available in the [whitepaper](./walrus.pdf).

**Sliver**: Erasure-encoded data of one shard corresponding to a single blob for one of the two encodings; this contains several erasure-encoded symbols of that blob but not the blob metadata.

**Sliver pair**: The combination of a shard's primary and secondary sliver.

**Blob ID**: Cryptographic ID computed from a blob's slivers.

**Blob metadata**: Metadata of one blob; in particular, this contains a hash per shard to enable the authentication of slivers and recovery symbols.

**User**: Any entity or person that wants to store or read blobs on or from Walrus; can act as a Walrus client itself or use the simple interface exposed by publishers and caches.

**Publisher**: Service interacting with Sui and the storage nodes to store blobs on Walrus; offers a basic `HTTP POST` endpoint to end users.

**Aggregator**: Service that reconstructs blobs by interacting with storage nodes and exposes a basic `HTTP GET` endpoint to end users.

**Cache**: An aggregator with additional caching capabilities.

**Client**: Entity interacting directly with the storage nodes; this can be an aggregator or cache, a publisher, or an end user.

**Reconstruction**: Decoding of the primary slivers to obtain the blob; includes re-encoding the blob and checking the Merkle proofs.

**Shard recovery**: Process of a storage node recovering a sliver or full shard by obtaining recovery symbols from other storage nodes.

**Storage attestation**: Process where storage nodes exchange challenges and responses to demonstrate that they are storing their currently assigned shards.

**Certificate of availability**: A blob ID with signatures of storage nodes holding at least \(2f+1\) shards in a specific epoch.

**Point of availability**: Point in time when a certificate of availability is submitted to Sui and the corresponding blob is guaranteed to be available until its expiration.

**Inconsistency proof**: Set of several recovery symbols with their Merkle proofs such that the decoded sliver does not match the corresponding hash; this proves an incorrect/inconsistent encoding by the client.

**Inconsistency certificate**: An aggregated signature from 2/3 of storage nodes (weighted by their number of shards) that they have seen and stored an inconsistency proof for a blob ID.

**Storage committee**: The set of storage nodes for a storage epoch, including metadata about the shards they are responsible for and other metadata.

**Member**: A storage node that is part of a committee at some epoch.

**Storage epoch**: The epoch for Walrus as distinct to the epoch for Sui.

**Availability period**: The period specified in storage epochs for which a blob is certified to be available on Walrus.

**Expiry**: The end epoch at which a blob is no longer available and can be deleted; the end epoch is always exclusive.

**WAL**: The native token of Walrus.

**FROST**: The smallest unit of WAL (similar to MIST for SUI); 1 WAL is equal to 1 billion (1000000000) FROST.