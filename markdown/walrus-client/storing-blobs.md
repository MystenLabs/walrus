All blobs stored in Walrus are public and discoverable by all. To store sensitive data, use [Seal](/docs/data-security#seal-data-confidentially-and-access-control) or [Nautilus](/docs/data-security#nautilus-secure-and-verifiable-off-chain-computation) to encrypt the data before storing it on Walrus.

Store blobs on Walrus with the following command:

```sh
$ walrus store <FILES> --epochs <EPOCHS>
```

<!-- IMPORT_CONTENT_RESOLVED source="blob-object-id" mode="snippet" -->
After you upload a blob to Walrus, it has 2 identifiers:

```sh
Blob ID: oehkoh0352bRGNPjuwcy0nye3OLKT649K62imdNAlXg
Sui object ID: 0x1c086e216c4d35bf4c1ea493aea701260ffa5b0070622b17271e4495a030fe83
```

- Blob ID: A way to reference the blob on Walrus. The system generates the blob ID based on the blob's contents, meaning any file you upload to the network twice results in the same blob ID.

- Sui Object ID: The blob's corresponding newly created Sui object identifier, as the system binds all blobs to one or more Sui objects.

You use blob IDs to read blob data, while you use Sui object IDs to make modifications to the blob's metadata, such as its storage duration. You might also use them to read blob data.
<!-- /IMPORT_CONTENT_RESOLVED -->

You can store a single file or multiple files, separated by spaces. This is compatible with glob patterns:

```sh
$ walrus store *.png --epochs <EPOCHS>
```

This example stores all PNG files in the current directory.

## Blob lifetimes

You must set a mandatory CLI argument to specify the lifetime for the blob. There are currently 3 methods for setting a blob's lifetime:

1. The `--epochs <EPOCHS>` option indicates the number of epochs the blob should be stored for. There is an upper limit on the number of epochs a blob can be stored for, which is 53 and corresponds to 2 years. In addition to a positive integer, you can also use `--epochs max` to store the blob for the maximum number of epochs. The end epoch is defined as the current epoch plus the specified number of epochs.

2. The `--earliest-expiry-time <EARLIEST_EXPIRY_TIME>` option takes a date in either RFC 3339 format (for example, `2024-03-20T15:00:00Z`) or a more relaxed format (for example, `2024-03-20 15:00:00`). It ensures the blob expires after the specified date if possible.

3. The `--end-epoch <END_EPOCH>` option takes a specific end epoch for the blob.

A blob expires at the beginning of its end epoch. For example, a blob with end epoch `314` becomes unavailable at the beginning of epoch `314`. One consequence of this is that when you store a blob with `--epochs 1` immediately before an epoch change, it expires and becomes unavailable almost immediately. You can [extend](/docs/walrus-client/managing-blobs#extend-the-lifetime-of-a-blob) blobs only if they have not expired.

## Blob permanence

You can specify whether a newly stored blob is deletable or permanent through the `--deletable` and `--permanent` options:

- **Permanent:** The blob remains available until its expiry epoch. Not even the uploader can delete it beforehand.
- **Deletable:** The blob can be deleted at any point during its lifetime by the owner of the corresponding Sui object. See [deletable blobs](/docs/walrus-client/managing-blobs#delete-blobs) for more details.

Newly stored blobs are deletable by default.

## Automatic optimizations

When storing a blob, the client performs a number of automatic optimizations, including the following:

- If the blob is already stored as a permanent blob on Walrus for a sufficient number of epochs, the command does not store it again. You can override this behavior with the `--force` CLI option, which stores the blob again and creates a fresh Sui object belonging to the wallet address.
- If your wallet has a storage resource of suitable size and duration, it is used instead of buying a new one.
- If the blob is already certified on Walrus but is a deletable blob or is not stored for a sufficient number of epochs, the command skips sending encoded blob data to the storage nodes and just collects the availability certificate.

## Use a Walrus upload relay

A Walrus upload relay is a third-party service that helps clients with limited bandwidth and networking capabilities, such as a browser, store blobs on Walrus.

Asset management on-chain still happens on the client. The upload relay takes the unencoded blob, encodes it, and sends the slivers to the storage nodes before returning the certificate. See in-depth details in the [Walrus upload relay](/docs/operator-guide/upload-relay) documentation.

When storing blobs with the `walrus store` command or when storing quilts, you can use the `--upload-relay` flag with a URL to specify an upload relay server for the CLI to use.

The Walrus upload relay functionality is only available in Walrus CLI version v1.29 or higher.

The upload relay is a third-party service that might require a fee or tip. This tip might be a constant SUI amount per blob stored, or it might depend on the size of the blob being stored. The Walrus CLI shows you how much tip the upload relay requires and asks for confirmation before continuing.

View technical details on how the tip is [computed and paid](/docs/operator-guide/upload-relay).