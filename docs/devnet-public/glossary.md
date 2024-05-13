# Walrus Glossary

To make communication as clear and efficient as possible, we make sure to use a single term for every Walrus entity/concept and *do not* use any synonyms. The following table lists various concepts, their approved name, how they relate to / differ from other terms, and forbidden names (that were sometimes used in the past).

| Approved name | Description | Forbidden names |
| --- | --- | --- |
| storage node (SN) | entity storing data for Walrus; it has an associated staking pool and at any time holds one or several shards | validator |
| blob | single unstructured data object stored on Walrus | file |
| shard | (disjoint) subset of erasure-encoded data of all blobs; at every point in time, a shard is assigned to and stored on a single SN | medallion, chunk |
| sliver | erasure-encoded data of one shard corresponding to a single blob for one of the two encodings; this contains several erasure-encoded symbols of that blob but not the blob metadata  | chunk, part, block |
| blob ID | cryptographic ID computed from a blob’s slivers | not BlobID (one word) |
| blob metadata | metadata of one blob; in particular, this contains a hash per shard to enable the authentication of slivers and recovery symbols |  |
| (end) user | any entity/person that wants to store or read blobs on/from Walrus; can act as a Walrus client itself or use the simple interface exposed by publishers and caches |  |
| publisher | service interacting with Sui and the SNs to store blobs on Walrus; offers a simple HTTP POST endpoint to end users | gateway |
| cache | service that reconstructs blobs by interacting with SNs and exposes a simple HTTP GET endpoint to end users | gateway, aggregator |
| (Walrus) client | entity interacting directly with the SNs; this can be a cache, a publisher, or an end user |  |
| (blob) reconstruction | decoding of the primary slivers to obtain the blob; includes re-encoding the blob and checking the Merkle proofs | recovery |
| (shard/sliver) recovery | process of an SN recovering a sliver or full shard by obtaining recovery symbols from other SNs | reconstruction |
| storage attestation | process where SNs exchange challenges and responses to demonstrate that they are storing their currently assigned shards |  |
| certificate of availability (CoA) | a blob ID with signatures of SNs holding at least $2f+1$ shards in a specific epoch | attestation |
| point of availability (PoA) | point in time when a CoA is submitted to Sui and the corresponding blob is guaranteed to be available until its expiration | attestation |
| inconsistency proof | set of several recovery symbols with their Merkle proofs such that the decoded sliver does not match the corresponding hash; this proves an incorrect/inconsistent encoding by the client | fraud proof, corruption proof |
| inconsistency certificate | an aggregated signature from 2/3 of storage nodes (weighted by their number of shards) that they have seen and stored an inconsistency proof for a blob ID |  |
| storage committee | the set of storage nodes for a storage epoch, including meta-data about the shards they are responsible for and other meta-data. |  |
| members | a storage node that is part of a committee at some epoch. |  |
| storage epoch | the epoch for Warus as distinct to the epoch for Sui | epoch |
| availability period | the period specified in storage epochs for which a blob is certified to be available on Walrus. |  |