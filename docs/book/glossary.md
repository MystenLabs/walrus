# Walrus Glossary

To make communication as clear and efficient as possible, we make sure to use a single term for
every Walrus entity/concept and *do not* use any synonyms. The following table lists various
concepts, their canonical name, and how they relate to or differ from other terms.

Italicized terms in the description indicate other specific Walrus terms contained in the table.

| Approved name                     | Description                                                                                                                                                                                 |
|-----------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 2D encoding                       | encoding scheme used in *RedStuff* where each *blob* is encoded in two dimensions—*primary* and *secondary*—to ensure robust recovery and redundancy                                        |
| aggregator                        | service that reconstructs *blobs* by interacting with *storage nodes* and exposes a simple `HTTP GET` endpoint to *users*                                                                   |
| availability period               | the period specified in (Walrus) *epochs* for which a *blob* is certified to be available on Walrus                                                                                         |
| blob                              | single unstructured data object stored on Walrus                                                                                                                                            |
| blob ID                           | cryptographic ID computed from a *blob*’s *slivers*                                                                                                                                         |
| blob metadata                     | metadata of one *blob*; in particular, this contains a hash per *shard* to enable the authentication of *slivers* and recovery symbols                                                      |
| cache                             | an *aggregator* with additional caching capabilities                                                                                                                                        |
| certificate of availability (CoA) | a *blob ID* with signatures of *storage nodes* holding at least \(2f+1\) *shards* in a specific *epoch*                                                                                     |
| client                            | entity interacting directly with the *storage nodes*; this can be an *aggregator*/*cache*, a *publisher*, or an *user*                                                                      |
| committee                         | the set of *storage nodes* for a particular *epoch*, including metadata about the *shards* they are responsible for and other metadata                                                      |
| deletable blob                    | blob which can be deleted by its owner at any time to be able to reuse the storage resource                                                                                                 |
| end epoch                         | the *epoch* at which a blob will no longer be available for reads; the end epoch is always **exclusive**                                                                                    |
| epoch                             | Walrus uses epochs to track committee changes (like Sui) and measure lifetimes of blobs; the epoch duration for Walrus differs from Sui; on Mainnet the epoch duration is two weeks         |
| expiry                            | a blob becomes unavailable and can be deleted by storage nodes at its *end epoch*                                                                                                           |
| FROST                             | the smallest unit of WAL (similar to MIST for SUI); 1 WAL is equal to 1 billion (1000000000) FROST                                                                                          |
| inconsistency certificate         | an aggregated signature from 2/3 of *storage nodes* (weighted by their number of *shards*) that they have seen and stored an *inconsistency proof* for a *blob ID*                          |
| inconsistency proof               | set of several recovery symbols with their Merkle proofs such that the decoded *sliver* does not match the corresponding hash; this proves an incorrect/inconsistent encoding by the client |
| member                            | an *storage node* that is part of a *committee* at some *epoch*                                                                                                                             |
| permanent blob                    | blob which cannot be deleted by its owner and is guaranteed to be available until at least its expiry epoch (assuming it is valid)                                                          |
| point of availability (PoA)       | point in time when a *CoA* is submitted to Sui and the corresponding *blob* is guaranteed to be available until its expiration                                                              |
| publisher                         | service interacting with Sui and the *storage nodes* to store *blobs* on Walrus; offers a simple `HTTP POST` endpoint to *users*                                                            |
| quilt                             | structured data object consisting of one or multiple unstructured *blobs*                                                                                                                   |
| quilt ID                          | *blob ID* of a *quilt*                                                                                                                                                                      |
| quilt patch                       | a single *blob* stored in a *quilt*, including the unstructured data and metadata                                                                                                           |
| quilt patch ID                    | a unique ID to identify a *quilt patch* in Walrus; the *quilt patch ID* is computed from the entire *quilt*, not a single *quilt patch*                                                     |
| reconstruction (of a blob)        | decoding of the primary *slivers* to obtain the blob; includes re-encoding the *blob* and checking the Merkle proofs                                                                        |
| recovery                          | process of an *storage node* recovering one or multiple *blobs*, *slivers*, or full *shards* by obtaining recovery symbols from other *storage nodes*                                       |
| RedStuff                          | our erasure-encoding approach, which uses two different encodings (*primary* and *secondary*) to enable shard recovery; details are available in the [whitepaper](./walrus.pdf)             |
| shard                             | (disjoint) subset of erasure-encoded data of all *blobs*; at every point in time, a *shard* is assigned to and stored on a single *storage node*                                            |
| sliver                            | erasure-encoded data of one *shard* corresponding to a single blob for one of the two encodings; this contains several erasure-encoded symbols of that blob but not the *blob metadata*     |
| sliver pair                       | the combination of a shard’s primary and secondary sliver                                                                                                                                   |
| staker                            | entity that locks (stakes) *WAL* tokens to support the network's integrity and participate in the selection or validation of *storage nodes* and *committees*.                              |
| storage challenge                 | cryptographic proof mechanism where *storage nodes* periodically verify each other’s storage of assigned *shards* by issuing random challenges during *storage attestation*.                |
| storage attestation               | process where *storage nodes* exchange challenges and responses to demonstrate that they are storing their currently assigned *shards*                                                      |
| storage node                      | entity storing data for Walrus; holds one or several *shards*                                                                                                                               |
| user                              | any entity/person that wants to store or read *blobs* on/from Walrus; can act as a Walrus client itself or use the simple interface exposed by *publishers* and *caches*                    |
| WAL                               | the native Token of Walrus                                                                                                                                                                  |
| Walrus-native metadata            | user-defined metadata per *quilt patch*, stored on Walrus together with the corresponding *quilt*                                                                                           |
