> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

A data marketplace lets sellers publish datasets and buyers pay to acquire them. Walrus provides the building blocks for all three layers of such an application: Walrus itself stores and serves the dataset bytes, a Move package on Sui handles listings, payments, and ownership, and client-side encryption, for example with [Seal](https://seal-docs.wal.app/), keeps paid content confidential. Every stored blob has a corresponding `Blob` object on Sui with the `key` and `store` abilities, so datasets become assets that smart contracts can escrow, price, and transfer.

> **Blob data is public**
>
> Anyone who knows a blob ID can read the blob through any aggregator. Owning the `Blob` object does not restrict reads; the object controls management rights such as extending, deleting, and setting attributes. To sell access to the content itself, encrypt the data before you store it and gate decryption onchain, as described in [Gate paid content with encryption](#gate-paid-content-with-encryption).
## Architecture

A minimal marketplace consists of four parts:

1. **Seller clients** encrypt (optionally) and upload datasets through a publisher, the CLI, or the TypeScript SDK, and receive the resulting `Blob` objects.
2. **A marketplace Move package** escrows `Blob` objects in shared listing objects, verifies payment, pays the seller, and transfers the blob to the buyer.
3. **Buyer clients** purchase a listing, then read the dataset through an aggregator using the blob ID or the object ID.
4. **Walrus infrastructure** does the heavy lifting: publishers accept uploads, storage nodes hold the encoded slivers, and aggregators serve reads.

## Store the dataset

Sellers first store the dataset on Walrus. The following `curl` command stores a file through a publisher for 10 epochs as a permanent blob and sends the created `Blob` object to the seller's address:

```sh
$ curl -X PUT \
  "$PUBLISHER/v1/blobs?epochs=10&permanent=true&send_object_to=$SELLER_ADDRESS" \
  --upload-file "dataset.bin"
```

Three query parameters matter for marketplace listings:

- `epochs` sets the storage duration. The publisher stores the blob for 1 epoch if you omit it.
- `permanent=true` rules out deletion before expiry. The publisher stores new blobs as deletable by default, and buyers expect that a seller cannot delete data after a sale.
- `send_object_to` sends the created `Blob` object to the given Sui address, so the seller's wallet holds the object to list.

Set `$PUBLISHER` to an endpoint from the [Network Reference](/docs/network-reference#aggregators-and-publishers). Walrus has no public unauthenticated publisher on Mainnet, and most public endpoints limit requests to 10 MiB; for production stores or larger datasets, run your own publisher or use the [CLI](/docs/walrus-client/storing-blobs) or [TypeScript SDK](/docs/typescript-sdk/sdks). The response carries the `blobId`, the handle buyers later use to read the data, and, for newly created blobs, the Sui object ID of the `Blob` object. See [Understanding the response](/docs/http-api/storing-blobs#understanding-the-response).

## Escrow and sell the blob in Move

The marketplace contract escrows the `Blob` object inside a shared `Listing` object so that any buyer can purchase it. The following module shows the core pattern:

```move
module marketplace::listing {
    use sui::{coin::Coin, sui::SUI};
    use walrus::blob::Blob;

    const EIncorrectPayment: u64 = 0;

    /// A shared listing that escrows a Walrus `Blob` object until a buyer
    /// pays the asking price.
    public struct Listing has key {
        id: UID,
        price: u64,
        seller: address,
        blob: Blob,
    }

    /// Escrows the blob in a shared `Listing` that any buyer can purchase.
    public fun list(blob: Blob, price: u64, ctx: &mut TxContext) {
        transfer::share_object(Listing {
            id: object::new(ctx),
            price,
            seller: ctx.sender(),
            blob,
        })
    }

    /// Pays the seller and transfers the escrowed `Blob` object to the buyer.
    public fun buy(listing: Listing, payment: Coin<SUI>, ctx: &mut TxContext) {
        let Listing { id, price, seller, blob } = listing;
        assert!(payment.value() == price, EIncorrectPayment);
        transfer::public_transfer(payment, seller);
        transfer::public_transfer(blob, ctx.sender());
        id.delete();
    }
}
```

The module works as follows:

- `list` takes a `Blob` by value, wraps it with a price and the seller's address, and shares the `Listing` so any buyer can call `buy`. While escrowed, not even the seller can extend or delete the blob, unless you add functions for that.
- `buy` unpacks the `Listing`, checks the payment, pays the seller, and transfers the `Blob` object to the buyer. After the purchase, the buyer owns the object and can extend its lifetime, set attributes, resell it, or wrap it again.
- The example prices listings in SUI for brevity; change the `Coin` type parameter to price in WAL or any other coin.

Your package needs the Walrus dependency in its `Move.toml`; see [Add the Walrus dependency](/docs/examples/move#add-the-walrus-dependency) for the exact declaration and build commands.

## Gate paid content with encryption

Because reads are public, a marketplace that sells the content itself, rather than provenance or management rights, must encrypt datasets before storing them. [Seal](https://seal-docs.wal.app/) provides threshold encryption with onchain access control:

1. The seller encrypts the dataset with Seal under an access policy: a Move function named `seal_approve` in your package decides who can decrypt. Only the ciphertext goes to Walrus.
2. The marketplace contract records each purchase onchain, for example by adding the buyer to an allowlist that the policy checks.
3. The buyer downloads the ciphertext from an aggregator and requests key shares from the Seal key servers, which check the onchain policy before answering. The buyer then decrypts locally.

For a runnable end-to-end example that encrypts with Seal and stores the ciphertext on Walrus, see the [Seal example code](https://github.com/MystenLabs/walrus/tree/main/docs/examples/seal) in the Walrus repository.

## Attach product metadata

Marketplace listings need titles, descriptions, and content types. You can attach this metadata directly to the `Blob` object:

- With the CLI, set key-value attributes on a blob you own:

  ```sh
  $ walrus set-blob-attribute <BLOB_OBJ_ID> --attr "content-type" "text/csv"
  ```

- From Move, the `walrus::blob` module provides `insert_or_update_metadata_pair` and related functions to manage metadata on a blob your contract holds.

When buyers read a blob by object ID, the aggregator returns recognized attribute keys, such as `content-type` and `content-disposition`, in the corresponding HTTP response headers. For many small preview files or product cards, [Quilt](/docs/system-overview/quilt) batches them into a single stored unit and cuts per-blob overhead.

## Deliver the data to buyers

Buyers read blobs with a GET request to any aggregator:

```sh
# Read by blob ID
$ curl "$AGGREGATOR/v1/blobs/<BLOB_ID>" -o dataset.bin

# Read by the Blob object ID, which also returns attribute headers
$ curl "$AGGREGATOR/v1/blobs/by-object-id/<BLOB_OBJECT_ID>" -o dataset.bin
```

For encrypted listings, the buyer decrypts locally after Seal releases the key shares. If a read right after upload returns a `404` through a CDN-fronted aggregator, retry with backoff; see [Reading Blobs Right After Upload](/docs/troubleshooting/reading-blobs-after-upload).

## Keep listings available

Walrus stores each blob for a fixed number of epochs, so a marketplace must plan for expiry:

- The blob owner extends a blob with `walrus extend --blob-obj-id <ID>`, and a contract extends a blob it holds through `walrus::system::extend_blob` with a WAL payment, for example funded from sale proceeds.
- A [shared blob](/docs/walrus-client/managing-blobs#shared-blobs) wraps a blob in a shared object that anyone can fund and extend, which suits listings the whole marketplace wants to keep alive. Create one with `walrus share --blob-obj-id <SUI_OBJ_ID>`.
- Storage resources themselves transfer between users, so a marketplace can also acquire and trade storage capacity; see [Storage Costs](/docs/system-overview/storage-costs).