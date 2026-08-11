> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

You can store and read Walrus blobs from JavaScript with plain HTTP calls: a publisher accepts uploads through PUT requests, and an aggregator serves downloads through GET requests. The built-in `fetch` API covers both, so you need no SDK or other dependencies in the browser, in Node.js, or in any other JavaScript runtime.

## Choose your endpoints

The examples on the rest of the page use the public Testnet endpoints:

```js
const PUBLISHER = "https://publisher.walrus-testnet.walrus.space";
const AGGREGATOR = "https://aggregator.walrus-testnet.walrus.space";
```

Two constraints apply when you pick endpoints:

1. Walrus has no public unauthenticated publisher on Mainnet. Use a Testnet publisher for experiments; on Mainnet, run your own publisher, use an upload relay, or use the [TypeScript SDK](/docs/typescript-sdk/sdks).
2. Most public aggregators and publishers limit requests to 10 MiB. To store larger blobs, run your own publisher or use the [CLI](/docs/walrus-client/storing-blobs).

## Upload a blob

Send the raw bytes as the body of a PUT request to the publisher's `/v1/blobs` endpoint. The following function condenses the store call from the [runnable example](#complete-example-browser-upload-form) below:

```js
async function storeBlob(data, epochs = 1) {
  const response = await fetch(`${PUBLISHER}/v1/blobs?epochs=${epochs}`, {
    method: "PUT",
    body: data, // a File, Blob, ArrayBuffer, Uint8Array, or string
  });
  if (!response.ok) {
    throw new Error(`Store failed with status ${response.status}`);
  }
  return response.json();
}
```

Control the resulting blob through query parameters:

| **Parameter** | **Effect** |
| --- | --- |
| `epochs` | The number of storage epochs. Defaults to 1 if you omit it. |
| `deletable=true` or `permanent=true` | Whether the owner can delete the blob before it expires. The publisher stores new blobs as deletable by default. |
| `send_object_to` | A Sui address that receives the created `Blob` object. |

## Handle the store response

A successful store returns JSON in one of two shapes. A `newlyCreated` field describes a blob that Walrus stored for the first time, while an `alreadyCertified` field describes a blob that some user already stored and certified earlier. The two shapes nest the blob ID differently, so handle both:

```js
function parseStoreResponse(info) {
  if ("alreadyCertified" in info) {
    return {
      status: "Already certified",
      blobId: info.alreadyCertified.blobId,
      endEpoch: info.alreadyCertified.endEpoch,
    };
  }
  if ("newlyCreated" in info) {
    return {
      status: "Newly created",
      blobId: info.newlyCreated.blobObject.blobId,
      endEpoch: info.newlyCreated.blobObject.storage.endEpoch,
      suiObjectId: info.newlyCreated.blobObject.id,
    };
  }
  throw new Error("Unexpected store response");
}
```

The `blobId` identifies the data on Walrus: pass it to an aggregator to read the blob back. The `endEpoch` tells you when the storage period ends. For newly created blobs, the response also carries the Sui object ID of the `Blob` object. See [Understanding the response](/docs/http-api/storing-blobs#understanding-the-response) for the full response format.

## Download a blob

Read the blob back with a GET request to an aggregator's `/v1/blobs/<blobId>` endpoint. The [runnable example](#complete-example-browser-upload-form) builds the same URL to link each stored blob:

```js
async function readBlob(blobId) {
  const response = await fetch(`${AGGREGATOR}/v1/blobs/${blobId}`);
  if (!response.ok) {
    throw new Error(`Read failed with status ${response.status}`);
  }
  return response.arrayBuffer();
}
```

Call `response.text()` or `response.json()` instead of `arrayBuffer()` when you stored text or JSON. The aggregator serves blobs read by blob ID as `application/octet-stream`, so specify the media type yourself when you embed the blob in a page. To receive stored headers such as `content-type`, read by the Sui object ID instead, with `/v1/blobs/by-object-id/<objectId>`; the aggregator returns recognized blob attributes in the corresponding HTTP headers. See [Reading Blobs](/docs/http-api/reading-blobs) for details.

> **Reading a blob right after upload?**
>
> A CDN-fronted aggregator might briefly serve a cached `404` from before the blob propagated. If your app just stored the blob, retry the read with backoff. See [Reading Blobs Right After Upload](/docs/troubleshooting/reading-blobs-after-upload).
## Complete example: browser upload form

The following single-file example combines the upload and download calls into a web form. The form lets you set the publisher and aggregator URLs, pick a file and a number of epochs, and optionally enter a Sui address that receives the created `Blob` object through the `send_object_to` parameter. After a successful upload, the script parses both response shapes and renders each stored blob with a download link that points at the aggregator. To try it, save the file locally and open it in a browser.

<!-- ImportContent: GitHub source — resolve at export time or visit https://github.com/MystenLabs/walrus/blob/main/docs/examples/javascript/blob_upload_download_webapi.html -->

## Read Walrus system state

The next example reads Walrus system state on Sui instead of blob data. The script calls the `sui_getObject` JSON-RPC method on a public Sui full node and renders the per-epoch rewards and used storage capacity from the system state into a table.

<!-- ImportContent: GitHub source — resolve at export time or visit https://github.com/MystenLabs/walrus/blob/main/docs/examples/javascript/system_stats.html -->