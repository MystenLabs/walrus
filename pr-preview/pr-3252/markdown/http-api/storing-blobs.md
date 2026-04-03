You can store data using HTTP PUT requests. The following examples use `curl` to store blobs through a publisher:

```sh
# Store the string `some string` for 1 storage epoch
$ curl -X PUT "$PUBLISHER/v1/blobs" -d "some string"
# Store file `some/file` for 1 storage epoch
$ curl -X PUT "$PUBLISHER/v1/blobs" --upload-file "some/file"
```

## Configuring storage options

Control how the new blob is created through a combination of query parameters as documented in the [OpenAPI specification](#http-api-usage).

### Storage duration

Specify the lifetime of the blob through the `epochs` parameter. If you omit the parameter, blobs are stored for 1 epoch.

```sh
# Store file `some/file` for 5 storage epochs
$ curl -X PUT "$PUBLISHER/v1/blobs?epochs=5" --upload-file "some/file"
```

### Deletable and permanent blobs

Specify whether a blob is stored as permanent or deletable through a query parameter `permanent=true` or `deletable=true`:

```sh
# Store file `some/file` as a deletable blob:
$ curl -X PUT "$PUBLISHER/v1/blobs?deletable=true" --upload-file "some/file"
```

```sh
# Store file `some/file` as a permanent blob:
$ curl -X PUT "$PUBLISHER/v1/blobs?permanent=true" --upload-file "some/file"
```

:::caution

Newly stored blobs are deletable by default.

:::

### Sending the blob object to another address

Specify an address to which the resulting `Blob` object is sent using the `send_object_to` parameter:

```sh
# Store file `some/file` and send the blob object to `$ADDRESS`:
$ curl -X PUT "$PUBLISHER/v1/blobs?send_object_to=$ADDRESS" --upload-file "some/file"
```

## Understanding the response

The store HTTP API endpoints return information about stored blobs in JSON format.

### Newly created blobs

When a blob is stored for the first time, the response contains a `newlyCreated` field with information about it:

```sh
$ curl -X PUT "$PUBLISHER/v1/blobs" -d "some other string"
```

If successful, the response includes the content stored in the blob's corresponding [Sui object](/docs/system-overview/core-concepts):

```json
{
  "newlyCreated": {
    "blobObject": {
      "id": "0xe91eee8c5b6f35b9a250cfc29e30f0d9e5463a21fd8d1ddb0fc22d44db4eac50",
      "registeredEpoch": 34,
      "blobId": "M4hsZGQ1oCktdzegB6HnI6Mi28S2nqOPHxK-W7_4BUk",
      "size": 17,
      "encodingType": "RS2",
      "certifiedEpoch": 34,
      "storage": {
        "id": "0x4748cd83217b5ce7aa77e7f1ad6fc5f7f694e26a157381b9391ac65c47815faf",
        "startEpoch": 34,
        "endEpoch": 35,
        "storageSize": 66034000
      },
      "deletable": false
    },
    "resourceOperation": {
      "registerFromScratch": {
        "encodedLength": 66034000,
        "epochsAhead": 1
      }
    },
    "cost": 132300
  }
}
```

### Already certified blobs

When the publisher finds a certified blob with the same blob ID and a sufficient validity period, it returns an `alreadyCertified` structure:

```json
{
  "alreadyCertified": {
    "blobId": "M4hsZGQ1oCktdzegB6HnI6Mi28S2nqOPHxK-W7_4BUk",
    "event": {
      "txDigest": "4XQHFa9S324wTzYHF3vsBSwpUZuLpmwTHYMFv9nsttSs",
      "eventSeq": "0"
    },
    "endEpoch": 35
  }
}
```

The `event` field returns the [Sui event ID](/docs/system-overview/core-concepts) that you can use to find the object creation transaction through [Suiscan](https://suiscan.xyz/) or a [Sui SDK](https://docs.sui.io/references/sui-sdks).