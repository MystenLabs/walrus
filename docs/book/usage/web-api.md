# Client Daemon mode & HTTP API

In addition to the CLI and JSON modes, the Walrus client offers a *daemon mode*. In this mode, it
runs a simple web server offering HTTP interfaces to store and read blobs in an *aggregator* and
*publisher* role respectively. We also offer
[public aggregator and publisher services](#public-services) to try the Walrus HTTP APIs without
the need to run a local client. See how to
[operate an aggregator or publisher](../operator-guide/aggregator.md) in the operator documentation.

## HTTP API Usage

For the following examples, we assume you set the `AGGREGATOR` and `PUBLISHER` environment variables
to your desired aggregator and publisher, respectively. For example, the instances run by Mysten
Labs on Walrus Testnet (see [below](#public-services) for further public options):

```sh
AGGREGATOR=https://aggregator.walrus-testnet.walrus.space
PUBLISHER=https://publisher.walrus-testnet.walrus.space
```

```admonish tip title="API specification"
Walrus aggregators and publishers expose their API specifications at the path `/v1/api`. You can
view this in the browser, for example, at <https://aggregator.walrus-testnet.walrus.space/v1/api>.
```

### Store

You can interact with the daemon through simple HTTP PUT requests. For example, with
[cURL](https://curl.se), you can store blobs using a publisher or daemon as follows:

```sh
# store the string `some string` for 1 storage epoch
curl -X PUT "$PUBLISHER/v1/blobs" -d "some string"
# store file `some/file` for 5 storage epochs
curl -X PUT "$PUBLISHER/v1/blobs?epochs=5" --upload-file "some/file"
# store file `some/file` and send the blob object to $ADDRESS
curl -X PUT "$PUBLISHER/v1/blobs?send_object_to=$ADDRESS" --upload-file "some/file"
# store file `some/file` as a deletable blob, instead of a permanent one and send the blob object to $ADDRESS
curl -X PUT "$PUBLISHER/v1/blobs?deletable=true&send_object_to=$ADDRESS" --upload-file "some/file"
```

The store HTTP API end points return information about the blob stored in JSON format. When a blob
is stored for the first time, a `newlyCreated` field contains information about the
new blob:

```sh
$ curl -X PUT "$PUBLISHER/v1/blobs" -d "some other string"
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

The information returned is the content of the [Sui blob object](../dev-guide/sui-struct.md).

When the publisher finds a certified blob with the same blob ID and a sufficient validity period,
it returns a `alreadyCertified` JSON structure:

```sh
$ curl -X PUT "$PUBLISHER/v1/blobs" -d "some other string"
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

The field `event` returns the [Sui event ID](../dev-guide/sui-struct.md) that can be used to
find the transaction that created the Sui Blob object on the Sui explorer or using a Sui SDK.

### Read

Blobs may be read from an aggregator or daemon using HTTP GET using their blob ID.
For example, the following cURL command reads a blob and writes it to an output file:

```sh
curl "$AGGREGATOR/v1/blobs/<some blob ID>" -o <some file name>
```

Alternatively you may print the contents of a blob in the terminal with the cURL command:

```sh
curl "$AGGREGATOR/v1/blobs/<some blob ID>"
```

```admonish tip title="Content sniffing"
Modern browsers will attempt to sniff the content type for such resources, and will generally do a
good job of inferring content types for media. However, the aggregator on purpose prevents such
sniffing from inferring dangerous executable types such as JavaScript or style sheet types.
```

Blobs may also be read by using the object ID of a Sui blob object or a shared blob.
For example the following
cURL command downloads the blob corresponding to a Sui blob with a specific object ID:

```sh
curl "$AGGREGATOR/v1/blobs/by-object-id/<object-id>" -o <some file name>
```

Downloading blobs by object ID allows the use of attributes to set some HTTP headers.
The aggregator recognizes the attribute keys `content-disposition`, `content-encoding`,
`content-language`, `content-location`, `content-type`, and `link`, and when present returns
the values in the corresponding HTTP headers.

### Quilt HTTP APIs

Walrus supports storing and retrieving multiple blobs as a single unit called a [quilt](./quilt.md).
The publishers and aggregators support *quilt* related operations.

**Note** Quilt APIs are supported with Walrus *1.29* and higher.

```admonish warning
The identifiers must be unique within a quilt.
```

#### Storing quilts

```admonish tip
All the query parameters available for storing regular blobs (see [Store](#store) section above) can
also be used when storing quilts.
```

Use the following publisher API to store multiple blobs as a quilt:

```sh
# Store two files `document.pdf` and `image.png`, with custom identifiers `contract-v2` and
# `logo-2024, # respectively.
curl -X PUT "$PUBLISHER/v1/quilts?epochs=5&quilt_version=V1" \
  -F "contract-v2=@document.pdf" \
  -F "logo-2024=@image.png"

# Store two files with Walrus-native metadata. The metadata are assigned to the blob with
# the same identifier. Note: `_metadata` must be used as the field name for Walrus-native metadata.
curl -X PUT "$PUBLISHER/v1/quilts?epochs=5" \
  -F "quilt-manual=@document.pdf" \
  -F "logo-2025=@image.png" \
  -F '_metadata=[
    {"identifier": "quilt-manual", "tags": {"creator": "walrus", "version": "1.0"}},
    {"identifier": "logo-2025", "tags": {"type": "logo", "format": "png"}}
  ]'
```

The quilt store API returns a JSON response with information about the stored quilt, including
the quilt ID (blobId) and individual blob patch IDs that can be used to retrieve specific blobs
later.

```sh
$ curl -X PUT "http://127.0.0.1:31415/v1/quilts?epochs=1&quilt_version=V1" \
  -F "walrus.jpg=@./walrus-33.jpg" \
  -F "another_walrus.jpg=@./walrus-46.jpg"
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

#### Reading quilts

There are two ways to retrieve blobs from a quilt via the aggregator APIs:

##### By Quilt Patch ID

Each blob in a quilt has a unique patch ID. You can retrieve a specific blob using its patch ID:

```sh
# Retrieve a blob using its quilt patch ID.
curl "$AGGREGATOR/v1/blobs/by-quilt-patch-id/6XUOE-Q5-nAXHRifN6n9nomVDtHZQbGuAkW3PjlBuKoBAQDQAA" \
```

```admonish tip
QuiltPatchIds can be obtained from the store quilt output (as shown above) or by using the
[`list-patches-in-quilt`](./client-cli.md#batch-storing-blobs-with-quilt) CLI command.
```

##### By Quilt ID and Identifier

You can also retrieve a blob using the quilt ID and the blob's identifier:

```sh
# Retrieve a blob with identifier `walrus.jpg` from the quilt.
curl "$AGGREGATOR/v1/blobs/by-quilt-id/6XUOE-Q5-nAXHRifN6n9nomVDtHZQbGuAkW3PjlBuKo/walrus.jpg" \
```

Both methods return the raw blob bytes in the response body. Metadata such as the blob identifier
and tags are returned as HTTP headers:

- `X-Quilt-Patch-Identifier`: The identifier of the blob within the quilt
- `ETag`: The patch ID or quilt ID for caching purposes
- Additional custom headers from blob tags (if configured)

## Using a public aggregator or publisher {#public-services}

For some use cases (e.g., a public website), or to just try out the HTTP API, a publicly accessible
aggregator and/or publisher is required. On Walrus Testnet, many entities run public aggregators and
publishers. On Mainnet, there are no public publishers without authentication, as they consume both
SUI and WAL.

See the following subsections for [public aggregators on Mainnet](#mainnet) and public
[aggregators](#aggregators-testnet) and [publishers](#publishers-testnet) on Testnet. We also
provide the [operator lists in JSON format](../assets/operators.json).

<!-- markdownlint-disable proper-names -->
### Mainnet

The following is a list of known public aggregators on Walrus Mainnet; they are checked
periodically, but each of them may still be temporarily unavailable:

{{ #mainnet.aggregators }}
{{ /mainnet.aggregators }}

### Testnet

#### Aggregators {#aggregators-testnet}

The following is a list of known public aggregators on Walrus Testnet; they are checked
periodically, but each of them may still be temporarily unavailable:

{{ #testnet.aggregators }}
{{ /testnet.aggregators }}

#### Publishers {#publishers-testnet}

The following is a list of known public publishers on Walrus Testnet; they are checked
periodically, but each of them may still be temporarily unavailable:

{{ #testnet.publishers }}
{{ /testnet.publishers }}
<!-- markdownlint-enable proper-names -->

Most of these limit requests to 10 MiB by default. If you want to upload larger files, you need to
[run your own publisher](../operator-guide/aggregator.md#local-daemon) or use
the [CLI](./client-cli.md).
