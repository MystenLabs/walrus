{/* https://linear.app/mysten-labs/issue/DOCS-646/http-apireading-blobs */}

Walrus supports storing and retrieving multiple blobs as a single unit called a [quilt](/docs/system-overview/quilt). Publishers and aggregators both support quilt operations.

### Storing quilts

All query parameters available for [storing regular blobs](#store) can also be used when storing quilts.

Use the following publisher API to store multiple blobs as a quilt.

```sh
# Store 2 files `document.pdf` and `image.png`, with custom identifiers `contract-v2` and `logo-2024`, respectively:
$ curl -X PUT "$PUBLISHER/v1/quilts?epochs=5" \
  -F "contract-v2=@document.pdf" \
  -F "logo-2024=@image.png"
```

Identifiers must be unique within a quilt and cannot start with `_`. The field name `_metadata` is reserved for Walrus native metadata and does not conflict with user-defined identifiers. See the [Quilt documentation](/docs/system-overview/quilt) for complete identifier restrictions.

```sh
# Store 2 files with Walrus-native metadata. `_metadata` must be used as the field name for Walrus native metadata
$ curl -X PUT "$PUBLISHER/v1/quilts?epochs=5" \
  -F "quilt-manual=@document.pdf" \
  -F "logo-2025=@image.png" \
  -F '_metadata=[
    {"identifier": "quilt-manual", "tags": {"creator": "walrus", "version": "1.0"}},
    {"identifier": "logo-2025", "tags": {"type": "logo", "format": "png"}}
  ]'
```

The quilt store API returns a JSON response with information about the stored quilt, including the quilt ID (`blobId`) and individual blob patch IDs that can be used to retrieve specific blobs later. The following example shows the command and response. The actual JSON output is returned as a single line and is formatted on this page for readability:

```sh
$ curl -X PUT "http://127.0.0.1:31415/v1/quilts?epochs=1" \
  -F "walrus.jpg=@./walrus-33.jpg" \
  -F "another_walrus.jpg=@./walrus-46.jpg"
```

If successful, the console responds:

```sh
{
  "blobStoreResult": {
    "newlyCreated": {
    "blobObject": {
        "id": "0xe6ac1e1ac08a603aef73a34328b0b623ffba6be6586e159a1d79c5ef0357bc02",
        "registeredEpoch": 103,
        "blobId": "6XUOE-Q5-nAXHRifN6n9nomVDtHZQbGuAkW3PjlBuKo",
        "size": 1782224,
        "encodingType": "RS2",
        "certifiedEpoch": null,
        "storage": {
        "id": "0xbc8ff9b4071927689d59468f887f94a4a503d9c6c5ef4c4d97fcb475a257758f",
        "startEpoch": 103,
        "endEpoch": 104,
        "storageSize": 72040000
        },
        "deletable": false
    },
    "resourceOperation": {
        "registerFromScratch": {
        "encodedLength": 72040000,
        "epochsAhead": 1
        }
    },
    "cost": 12075000
    }
  },
  "storedQuiltBlobs": [
    {
    "identifier": "another_walrus.jpg",
    "quiltPatchId": "6XUOE-Q5-nAXHRifN6n9nomVDtHZQbGuAkW3PjlBuKoBAQDQAA"
    },
    {
    "identifier": "walrus.jpg",
    "quiltPatchId": "6XUOE-Q5-nAXHRifN6n9nomVDtHZQbGuAkW3PjlBuKoB0AB7Ag"
    }
  ]
}
```

### Reading quilts

You can retrieve blobs from a quilt through the aggregator APIs using their quilt patch ID or their quilt ID and unique identifier. Currently, only 1 blob can be retrieved per request. Bulk retrieval of multiple blobs from a quilt in a single request is not yet supported.

##### Quilt patch ID

Each blob in a quilt has a unique patch ID. You can retrieve a specific blob using its patch ID.

```sh
# Retrieve a blob using its quilt patch ID:
$ curl "$AGGREGATOR/v1/blobs/by-quilt-patch-id/6XUOE-Q5-nAXHRifN6n9nomVDtHZQbGuAkW3PjlBuKoBAQDQAA" \
```

You can obtain `QuiltPatchIds` from the store quilt output or by using the [`list-patches-in-quilt`](/docs/walrus-client/storing-blobs#batch-store) CLI command.

##### Quilt ID and identifier

You can also retrieve a blob using the quilt ID and the blob's identifier.

```sh
# Retrieve a blob with identifier `walrus.jpg` from the quilt:
$ curl "$AGGREGATOR/v1/blobs/by-quilt-id/6XUOE-Q5-nAXHRifN6n9nomVDtHZQbGuAkW3PjlBuKo/walrus.jpg" \
```

Both methods return the raw blob bytes in the response body. Metadata such as the blob ID and tags are returned as HTTP headers:

- `X-Quilt-Patch-Identifier`: The identifier of the blob within the quilt

- `ETag`: The patch ID or quilt ID for caching purposes

- Additional custom headers from blob tags, if configured