{/* https://linear.app/mysten-labs/issue/DOCS-630/system-overviewcore-concepts */}

Walrus uses Sui to manage blob metadata. Smart contract developers can read information on Sui about the [Walrus system](https://github.com/MystenLabs/walrus/tree/main/contracts) and stored blobs, or you can interact with Walrus through the client CLI, JSON and HTTP APIs without querying or executing transactions directly on Sui. 

The following sections demonstrate how you can use Walrus objects in your own Sui smart contracts. Alternatively, you can view an [example Sui smart contract](https://github.com/MystenLabs/walrus/tree/main/examples) that implements a [wrapped](https://docs.sui.io/guides/developer/objects/object-ownership/wrapped) blob.

## Blob and storage objects

Walrus blobs are represented as Sui objects of type `Blob`. A blob is first registered, indicating that the storage nodes should expect slivers from a blob ID to be stored. Then a blob is certified, indicating that a sufficient number of slivers have been stored to guarantee the blob's availability. When a blob is certified, its `certified_epoch` field contains the epoch in which it was certified.

A `Blob` object is always associated with a `Storage` object, reserving enough space for the configured time period the blob will be stored. A certified blob remains available for the duration specified by its associated storage resource.

Concretely, `Blob` and `Storage` objects have the following fields, which can be queried using the [Sui SDKs](https://sdk.mystenlabs.com/typescript):

```move
/// Reservation for storage for a given period, which is inclusive start, exclusive end.
public struct Storage has key, store {
    id: UID,
    start_epoch: u32,
    end_epoch: u32,
    storage_size: u64,
}

/// The blob structure represents a blob that has been registered to with some storage,
/// and then may eventually be certified as being available in the system.
public struct Blob has key, store {
    id: UID,
    registered_epoch: u32,
    blob_id: u256,
    size: u64,
    encoding_type: u8,
    // Stores the epoch first certified if any.
    certified_epoch: option::Option<u32>,
    storage: Storage,
    // Marks if this blob can be deleted.
    deletable: bool,
}
```

Public functions associated with these objects can be found in the respective [`storage_resource`](https://github.com/MystenLabs/walrus/tree/main/contracts/walrus/sources/system/storage_resource.move) and [`blob`](https://github.com/MystenLabs/walrus/tree/main/contracts/walrus/sources/system/blob.move) Move modules. Storage resources can be split and merged in time and data capacity, and can be transferred between users allowing complex contracts to be created.

## Events

Walrus uses [custom Sui events](https://github.com/MystenLabs/walrus/blob/main/contracts/walrus/sources/system/events.move) to notify storage nodes of updates concerning stored blobs and the state of the network. Applications can also use [Sui RPC facilities](https://docs.sui.io/references/sui-api) to observe Walrus related events.

When a blob is first registered, a [`BlobRegistered`](https://github.com/MystenLabs/walrus/blob/main/contracts/walrus/sources/system/events.move#L12-L23) event is emitted that informs storage nodes that they should expect slivers associated with its Blob ID. Eventually when the blob is certified, a [`BlobCertified`](https://github.com/MystenLabs/walrus/blob/main/contracts/walrus/sources/system/events.move#L25-L35) is emitted containing information about the blob ID and the epoch after which the blob is deleted. Before that epoch the blob is guaranteed to be available.

The [`BlobCertified`](https://github.com/MystenLabs/walrus/blob/main/contracts/walrus/sources/system/events.move#L25-L35) event with `deletable` set to false and an `end_epoch` in the future indicates that the blob is available until this epoch. A light client proof that this event was emitted for a blob ID constitutes a proof of availability for the data with this blob ID. When a deletable blob is deleted, a [`BlobDeleted`](https://github.com/MystenLabs/walrus/blob/main/contracts/walrus/sources/system/events.move#L37-L46) event is emitted.

The [`InvalidBlobID`](https://github.com/MystenLabs/walrus/blob/main/contracts/walrus/sources/system/events.move#L48-L52) event is emitted when storage nodes detect an incorrectly encoded blob. Anyone attempting a read on such a blob is guaranteed to also detect it as invalid.

System level events such as [`EpochChangeStart`](https://github.com/MystenLabs/walrus/blob/main/contracts/walrus/sources/system/events.move#L54-L57) and [`EpochChangeDone`](https://github.com/MystenLabs/walrus/blob/main/contracts/walrus/sources/system/events.move#L59-L63) indicate transitions between epochs. And associated events such as [`ShardsReceived`](https://github.com/MystenLabs/walrus/blob/main/contracts/walrus/sources/system/events.move#L65-L69), [`EpochParametersSelected`](https://github.com/MystenLabs/walrus/blob/main/contracts/walrus/sources/system/events.move#L71-L74), and [`ShardRecoveryStart`](https://github.com/MystenLabs/walrus/blob/main/contracts/walrus/sources/system/events.move#L76-L80) indicate storage node level events related to epoch transitions, shard migrations, and epoch parameters.

## System and staking information

The [Walrus system object](https://github.com/MystenLabs/walrus/blob/main/contracts/walrus/sources/system/system_state_inner.move) contains metadata about the available and used storage and the price of storage per KiB of storage in [FROST](/docs/walrus-client/storing-blobs). The committee structure within the system object can be used to read the current epoch number and information about the committee. Committee changes between epochs are managed by a set of [staking contracts](https://github.com/MystenLabs/walrus/tree/main/contracts/walrus/sources/staking) that implement a full delegated proof of stake system based on the WAL token.