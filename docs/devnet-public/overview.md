# Walrus overview

## Walrus objectives

This document presents a technical overview of Walrus, a decentralized store and availability
protocol for blobs, namely larger binary files with unstructured content. Walrus can affordably
store blobs of 100s of KiB to many GiB.

Walrus supports operations to store and retrieve blobs, as well as to prove and verify their
availability. It ensures content survives storage nodes suffering Byzantine faults and remains
available and retrievable. It provides APIs to access the stored content over a CLI, SDKs and over
web2 HTTP technologies, and supports contend delivery infrastructures like caches and CDNs.

Under the hood, storage cost is a small fixed multiple of the size of blobs (around 5x) thanks to
advanced error correction coding, in contrast to the full replication of data traditional to
blockchains such as >100x for data stored in Sui objects. As a result much bigger resources may be
stored on Walrus at lower cost compared to Sui or other blockchains. Since encoded blobs are stored
on all storage nodes Walrus also provides superior robustness compared with designs with a small
amount of replicas storing the full blob.

Walrus uses the Sui chain for coordination and payments. Available storage is represented as Sui
objects that can be acquired, owned, split, merged and transferred. Storage space can be tied to
a stored blob for a period of time. And the resulting Sui object may be used to prove
availability on chain in smart contracts, or off chain using light clients.

In this document we discuss in details the above operations relating to storage, retrieval
and availability.

In the future, we plan to include in Walrus some minimal governance to allow storage nodes to
change between storage epochs. Walrus is also compatible with periodic payments for continued
storage. We also plan to implement storage attestation based on challenges to get confidence that
that blobs are stored or at least available. Walrus also allows light-nodes that store small parts
of blobs to get rewards for proving availability and assisting recovery. We will cover these
topics in later documents. We also provide details of the encoding scheme in a separate document.

There are a few things that Walrus explicitly is not. Walrus does not reimplement a Content
Distribution Network (CDN) that might be geo-replicated or have less than tens of milliseconds
of latency. Traditional
CDNs should be usable and compatible with Walrus caches. Walrus does not re-implement a full
smart contracts platform with consensus or execution. It relies on Sui smart contracts when
necessary, to manage Walrus resources and processes including payments, storage epochs etc.
Walrus supports storage of any blob including encrypted blobs, however Walrus itself is not the
distributed key management infrastructure that manages and distributed encryption or decryption
keys to support a full private storage eco-system. It can however provide the storage layer of
for such infrastructures.

## Use-cases

App builders may use Walrus in conjunction with Sui to build experiences that require large
amounts of  data to be stored in a decentralized manner and possibly certified as available:

- **Storage of media for NFT or dapps**: Walrus can directly store and serve media such as images,
  sounds, sprites, videos, other game assets, etc. This is publicly available media that can be
  accessed using HTTP requests at caches to create multimedia dapps.
- **AI related use cases**: Walrus can store clean data sets of training data, datasets with a
  known and verified provenance, models weights, and proofs of correct training for AI models.
  Or it may be used to store and ensure the availability of an AI model output.
- **Storage of long term archival of blockchain history**: Walrus can be used as a lower cost
  decentralized store to store blockchain history. For Sui this can include sequences of
  checkpoints with all associated transaction and effects content. As well as historic snapshots
  of the blockchain state, code or binaries.
- **Support availability for L2s**: Walrus allows parties to certify the availability of blobs, as
  required by L2s that need data to be stored and be attested as available to all. This may also
  include availability of extra audit data such as validity proofs, zero knowledge proofs of
  correct execution or large fraud proofs.
- **Support a full decentralized web experience**: Walrus can host full decentralized web
  experiences including all resources (such as js, css, html, media). These can provide content but
  also host the UX of dapps to build dapps with fully decentralized front end and back ends on
  chain. It brings the full "web" back in "web3".
- **Support subscription models for media**: Creators can store encrypted media on Walrus and only
  provide access via decryption keys to parties that have paid a subscription fee or have paid for
  contents. (Note that Walrus provides the storage, encryption and decryption needs to happen off
  Walrus).

## Basic architecture and security assumptions

The key actors in the Walrus architecture are:

- **Users** through **clients** want to store and read **blobs**. They are ready to pay for service
  when it comes to writes, and when it comes to non-best-effort reads. Users also want to prove
  the **availability** of a blob to third parties without the cost of sending or receiving the full
  blob. Users may be malicious in various ways: they may wish to not pay for services, prove the
  availability of an unavailable blobs, or modify / delete blobs without authorization, try to
  exhaust resources of storage nodes, etc.
- **Storage nodes** hold one or many **Shards** within a **storage epoch**. Each blob is erasure
  encoded in many **slivers** and slivers from each stored blob become part of all shards. A shard
  at any storage epoch is associated with a **storage node** that actually stores all slivers of
  the shard, and is ready to serve them. The assignment of storage nodes to shards within
  **storage epochs** is controlled by a Sui smart contract and we assume that more than 2/3 of the
  shards are managed by correct storage nodes within each storage epoch. This means that we must
  tolerate up to 1/3 Byzantine storage nodes within each storage epoch and across storage epochs.
- All clients and storage nodes operate a **blockchain** client (specifically on Sui), and mediate
  payments, resources (space), mapping of shards to storage nodes, and metadata through blockchain
  smart contracts. Users interact with the blockchain to get storage resources and certify stored
  blobs, and storage nodes listen to the blockchain events to coordinate their operations.

Walrus supports any additional number of optional infrastructure actors that can operate in a
permissionless way:

- **Caches** are **clients** that store one or more full blobs and make them available to users
  over traditional web2 (HTTP, etc) technologies. They are optional in that end-users may also
  operate a local cache, and perform Walrus reads over web2 technologies locally. However, cache
  infrastructures may also act as CDNs, share the cost of blob reconstruction over many requests,
  have better connectivity, etc. A client can always verify that reads from such infrastructures
  are correct.
- **Publishers** are **clients** that help end-users store a blob using web2 technologies, and
  using less bandwidth and custom logic. They in effect receive the blob to be published. over
  traditional web2 protocols (eg. HTTP), and perform the Walrus store protocol on their behalf,
  including the encoding, distribution of slivers to shards, creation of certificate of certificate,
  and other on-chain actions. They are optional in that a user may directly interact with both Sui
  and storage nodes to store blobs directly. An end user can always verify that a publisher
  performed their duties correctly by attesting availability.

Neither caches, publishers or end-users are trusted components of the system, and they may deviate
from the protocol arbitrarily. However, some of the security properties of Walrus only hold for
honest end-users that use honest intermediaries (caches and publishers). We provide means
for end-users to audit the correct operation of both caches and publishers.

## Walrus assurance / security properties

The properties below hold true subject to the assumption that for all storage epochs 2/3 of shards
are operated by storage nodes that faithfully and correctly follow the Walrus protocol.

Each blob is encoded using error correction into slivers and a **blob ID** is cryptographically
derived. For a given blob ID there is a **point of availability** (PoA) and an **availability
period**, observable through an event on the Sui chain. The following properties relate to the PoA:

- After the PoA, for a blob ID, any correct user that performs a read within the availability
  period will eventually terminate and get a value V which is either blob contents F or None.
- After PoA if two correct users perform a read and get V and V’ then V = V’.
- A correct user with an appropriate storage resource can always perform store for a blob F with a
  blob ID and advance the protocol until the PoA.
- A read after PoA for a blob F stored by a correct user, will result in F.

Some assurance properties ensure the correct internal processes of Walrus storage nodes.
For the purposes of defining these, an **inconsistency proof** proves that a blob ID was
stored by an incorrect user (and was incorrectly encoded).

- After PoA and for a blob ID stored by a correct user, a storage node is always able to recover
  the correct sliver for its shards for this blob ID.
- After PoA if a correct storage node cannot recover a sliver, it can produce an inconsistency proof
  for the blob ID.
- If a blob ID is stored by a correct user, an inconsistently proof cannot be derived for it.
- A read by a correct user for a blob ID for which an inconsistency proof may exist returns None.

Note that there is no delete operation and a blob ID past PoA will be available for the full
availability period.

As a rule of thumb: before PoA it is the responsibility of a client to ensure the availability of
a blob and its upload to Walrus. After PoA it is the responsibility of Walrus as a system to
maintain the availability of the blob ID as part of its operation for the full availability period
remaining. Emission of the event corresponding to the PoA for a blob ID attests its
availability.

## Encoding, overheads, and verification

We summarize here the basic encoding and cryptographic techniques used in Walrus.

- **Storage nodes** hold one or many **shards** in a storage epoch, out of a larger total (say 1000)
  and each shard contains one blob **sliver** for each blob past PoA. Each shard is assigned to a
  storage node in a storage epoch.
- An [erasure code](https://en.wikipedia.org/wiki/Online_codes) **encode algorithm** takes a blob,
  and encodes it as $K$ symbols, such that any fraction $p$ of symbols can be used to reconstruct
  the blob. Each blob sliver contains a fixed number of such symbols.
- We select $p<1/3$ so that a third of symbols and also slivers may be used to reconstruct the blob
  by the **decode algorithm**. The matrix used to produce the erasure code is fixed and the same
  for all blobs by the Walrus system, and encoders have no discretion about it.
- Storage nodes manage one or more shards, and corresponding sliver of each blob are distributed
  to all the storage shards. As a result, the overhead of the distributed store is ~5x that of
  the blob itself, no matter how many shards we have. The encoding is systematic meaning that some
  storage nodes hold part of the plain blob, allowing for fast random access reads.

Each blob is also associated with some metadata including a blob ID to allow verification:

- A blob ID is computed as an authenticator of the set of all shard data and metadata (byte size,
  encoding, blob hash). We hash a sliver representation in each of the shards and add the resulting
  hashes into a Merkle tree. Then the root of the Merkle tree is the blob hash used to derive the
  blob ID that identifies the blob in the system.
- Each storage node may use the blob ID to check if some shard data belongs to a blob using the
  authenticated structure corresponding to the blob hash (Merkle tree). A successful check means
  that the data is indeed as intended by the writer of the blob (who, remember, may be corrupt).
- When any party reconstructs a blob ID from shards data and slivers, or accepts any blob purporting
  to be a specific blob ID, it must check that it encodes to the correct blob ID. This process
  involves re-coding the blob using the erasure correction code, and re-deriving the blob ID to
  check the blob indeed matches it. This prevents a malformed blob (i.e., incorrectly erasure coded)
  from ever being read with a blob ID at any correct recipient.
- A set of slivers above the reconstruction threshold belonging to a blob ID that are either
  inconsistent or lead to the reconstruction of a different ID represent an incorrect encoding
  (this may happen if the user that encoded the blob was malicious and encoded it incorrectly).
  Storage nodes may delete slivers belonging to inconsistently encoded blobs, and upon request
  return an inconsistency proof.

## Walrus operations on Sui

Walrus uses Sui smart contracts to coordinate storage operations as resources that have a lifetime,
and payments. As well as to facilitate governance to determine the storage nodes holding each
storage shard. We outline here these operations and refer to them below as part of the read / write
paths. As a reminder, only blob metadata is ever exposed to Sui or its validators, and the content
of blobs is always stored off-chain on Walrus storage nodes and caches. The storage nodes or caches
do not have to overlap with any Sui infra (validators etc), and the storage epochs may be of
different lengths and not have the same start / end times as Sui epochs.

### Storage resource life cycle on Sui

A number of Sui smart contracts hold the metadata of the Walrus system and all its entities.

- A **Walrus system object** holds the committee of storage nodes for the current storage epoch. The
  system object also holds the total available space on Walrus and the price per unit of storage (1KiB).
  These values are determined by 2/3 agreement between the storage nodes for the storage epoch.
  Users can pay to purchase storage space for some time duration. These space resources may be
  split, merged and transferred. Later they can be used to place a blob ID into Walrus.
- The **storage fund** holds funds for storing blobs over one, multiple storage epochs or
  perpetually. When purchasing storage space from the system object users pay into the storage fund
  separated over multiple storage epochs, and payments are made each epoch to storage nodes
  according to performance (see below).
- A user acquires some storage through the contracts or transfer, and can assign to it a blob ID,
  signifying they wish to store this blob ID into it. This emits a Move **resource event** that
  both caches and storage nodes listen to to expect and authorize off-chain storage operations.
- Eventually a user holds an off-chain **availability certificate** from storage nodes for a blob
  ID. The user **uploads the certificate on chain** to signal that the blob ID is available for an
  availability period. The certificate is checked against the latest Walrus committee,
  and an **availability event** is emitted for the blob ID if correct. This is the PoA for the
  blob.
- In case a blob ID is not correctly encoded a **inconsistency proof certificate** may be uploaded
  on chain at a later time, and an **inconsistent blob event** is emitted signaling to all that the
  blob ID read results will always return None. This indicates that its slivers may be deleted by
  storage nodes, except for an indicator to return None.

Users writing to Walrus, need to perform Sui transactions to acquire storage and certify blobs.
Users creating or consuming proofs for attestations of blob availability read the chain
only to prove or verify emission of events. A node that reads Walrus resources only needs to read
the blockchain to get committee metadata once per epoch, and then they request slivers directly
from storage nodes by blob ID to perform reads.

### Governance operations on Sui

Each Walrus storage epoch is represented by the Walrus system object that contains a storage
committee and various metadata or storage nodes like the mapping between shards and storage nodes,
available space and current costs. User may go to the system object for the period and **buy some
storage** amount for one or more storage epochs. At each storage epoch there is a price for storage,
and the payment provided becomes part of a **storage fund** for all the storage epochs that span
the storage bought. There is a maximum number of storage epochs in the future for which storage can
be bought (~2 years). Storage is a resource that can be split and merged, and transferred.

At the end of the storage epoch part of the funds in the **storage fund need to be allocated to
storage nodes**. The idea here is for storage nodes to perform light audits of each others,
and suggest which nodes are to be paid based on the performance of these audits.

## Walrus operations

Walrus operations happen off Sui, but may interact with the Sui flows defining the resource life
cycle.

### Write paths

![Write paths of Walrus](assets/WriteFlow.png)

Systems overview of writes, illustrated above:

- A user acquires a storage resource of appropriate size and duration on-chain, either by directly
  buying it on the Walrus system object, or a secondary market. A user can split, merge, and
  transfer storage acquired storage resources.
- When a user wants to write a blob, it first erasure codes it using encode, and computes its
  blob ID. Then they can perform the following steps itself, or use a publisher to perform steps
  on their behalf.
- The user goes on chain (Sui) and updates a storage resource to register the blob ID with the
  appropriate size and lifetime desired. This emits an event, received by storage nodes. Once the
  user receives it then continues the upload.
- The user sends each of the blob slivers and metadata to the storage nodes that currently
  manages the corresponding shards.
- A storage node managing a shard receives a sliver and checks it against the blob ID of the overall
  blob. It also checks that there is a blob resource with that blob ID that is authorized to store
  a blob. If correct, then it signs a statement that it holds the sliver for blob ID (and metadata)
  and returns it to the user.
- The user puts together the signatures returned from storage nodes into an availability certificate
  and sends it on chain. When successfully checked an availability event for the blob ID is emitted,
  and all other storage nodes seek to download any missing shards for the blob ID. This event being
  emitted on Sui is the Point of Availability (PoA) for the blob ID.
- After the PoA, and without user involvement, storage nodes sync and recover any missing slivers
  that are certified.

The user waits for 2/3 of shards signatures to return a certificate of availability. The rate of the
code is below 1/3 allowing for reconstruction if even 1/3 of shards only return the sliver. Since at
most 1/3 of the storage nodes can fail, this ensures reconstruction if a reader requests slivers
from all storage nodes that have signed the ID of the blob. Note that the full process can be
mediated by a publisher, that receives a blob and drives the process to completion.

### Refresh availability

Since no content data is required to refresh the period of storage, refresh is conducted fully on
chain within the protocol. To request an extension to the availability period of a blob, a user
provides an appropriate storage resource. Upon success this emits an event that storage nodes
receive to extend the period each sliver is stored for.

### Inconsistent resource flow

When a correct storage node tries to reconstruct a shard it may fail if the encoding of a blob ID
past PoA was incorrect, and will be able to extract an inconsistency proof for the blob ID. It then
generates a inconsistency certificate and uploads it on chain. The flow is as follows:

- A storage node fails to reconstruct a shard, and generates an inconsistency proof.
- The storage node sends the blob ID and inconsistency proof to all storage nodes of the storage
  epoch, and gets a signature, that it aggregates to an inconsistency certificate.
- The storage node sends the inconsistency certificate to the Walrus smart contract, that checks it
  and emits a inconsistent resource event.
- Upon receiving a inconsistent resource event correct storage nodes delete sliver data and only
  keep a metadata record to return None for the blob ID for the availability period. No storage
  attestation challenges are issued for this blob ID.

Note that a blob ID that is inconsistent will always resolve to None upon reading: this is due to
the read process running the decoding algorithm, and then re-encoding to check the blob ID is
correctly derived from a consistent encoding. This means that an inconsistency proof only reveals a
true fact to storage nodes (that may not otherwise have ran decoding), and does not change the
output of read in any case.

Note however that partial reads leveraging the systematic nature of the encoding may return partial
reads for inconsistently encoded files. Thus if consistency and availability of reads is important
dapps should do full reads rather than partial reads.

### Read paths

A user can read stored blobs either directly or through a cache. We discuss here the direct user
journey since this is also the operation of the cache in case of a cache miss. We assume that most
reads will happen through caches, for blobs that are hot, and will not result in requests to
storage nodes.

- The reader gets the metadata for the blob ID from any storage node, and authenticates it using
  the blob ID.
- The reader then sends a request for the shards corresponding to blob ID to storage nodes, and
  waits for f+1 to respond. Sufficient requests are sent in parallel to ensure low latency for
  reads.
- The reader authenticates the slivers returned with the blob ID, reconstructs the blob, and decides
  whether the contents are a valid blob or inconsistent.
- Optionally, for a cache, the result is cached and can be served without re-construction for some
  time, until it is removed from the cache. Requests for the blob to the cache return the blob
  contents, or a proof the blob is inconsistently encoded.

### Challenge mechanism for storage attestation

During an epoch a correct storage node challenges all shards to provide blob slivers past PoA:

- The list of available blobs for the period is determined by the sequence of Sui events up
  to the past period. Inconsistent blobs are not challenged, and a record proving this status
  can be returned instead.
- A challenge sequence is determined by providing a seed to the challenged shard. The sequence is
  then computed based both on the seed AND the content of each challenged blob ID. This creates a
  sequential read dependency.
- The response to the challenge provides the sequence of shard contents for the blob IDs in a
  timely manner.
- The challenger node uses thresholds to determine whether the challenge was passed, and reports
  the result on chain.
- The challenge / response communication is authenticated.

Challenges provide some reassurance that the storage node actually can recover shard data in a
probabilistic manner, avoiding storage nodes getting payment without any evidence they may retrieve
shard data. The sequential nature of the challenge and some reasonable timeout also ensure that
the process is timely.

## Future discussion

In this document, we left out details of the following features:

- Shard transfer and recovery upon storage epoch change. The encoding scheme used has been designed
  to allow this operation to be efficient. A storage node needs to only get data of the same
  magnitude to the missing sliver data to reconstruct them.
- Details of light clients that can be used to sample availability. Individual clients may sample
  the certified blobs from Sui metadata, and sample the availability of some slivers that they
  store. On-chain bounties may be used to retrieve these slivers for missing blobs.
