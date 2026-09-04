> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

You can store and read Walrus blobs from JavaScript with plain HTTP calls: a publisher accepts uploads through PUT requests, and an aggregator serves downloads through GET requests. The built-in `fetch` API covers both.

## Store and read a blob

The following file holds the whole round trip: the endpoint constants, the store call, the response parser, and the read call.

<!-- ImportContent: GitHub source — resolve at export time or visit https://github.com/MystenLabs/walrus/blob/main/docs/examples/javascript/store_and_read_blob.js -->

Two constraints apply when you pick endpoints:

1. Walrus has no public unauthenticated publisher on Mainnet. Use a Testnet publisher for experiments; on Mainnet, run your own publisher, use an upload relay, or use the [TypeScript SDK](/docs/typescript-sdk/sdks).
2. Most public aggregators and publishers limit requests to 10 MiB. To store larger blobs, run your own publisher or use the [CLI](/docs/walrus-client/storing-blobs).

Control the resulting blob through query parameters on the store call:

| **Parameter** | **Effect** |
| --- | --- |
| `epochs` | The number of storage epochs. Defaults to 1 if you omit it. |
| `deletable=true` or `permanent=true` | Whether the owner can delete the blob before it expires. The publisher stores new blobs as deletable by default. |
| `send_object_to` | A Sui address that receives the created `Blob` object. |

The `blobId` the parser returns identifies the data on Walrus, and `endEpoch` tells you when the storage period ends. See [Understanding the response](/docs/http-api/storing-blobs#understanding-the-response) for the full format.

Reading by blob ID returns `application/octet-stream`, so set the media type yourself when you embed a blob in a page. To get stored headers such as `content-type` back, read by Sui object ID instead with `/v1/blobs/by-object-id/<objectId>`. See [Reading Blobs](/docs/http-api/reading-blobs).

> **Reading a blob right after upload?**
>
> A CDN-fronted aggregator might briefly serve a cached `404` from before the blob propagated. If your app just stored the blob, retry the read with backoff. See [Reading Blobs Right After Upload](/docs/troubleshooting/reading-blobs-after-upload).
## Example: Browser upload form

The following example combines the upload and download calls into a web form. The form lets you set the publisher and aggregator URLs, pick a file and a number of epochs, and optionally enter a Sui address. The address receives the created `Blob` object through the `send_object_to` parameter. After a successful upload, the script parses both response shapes and renders each stored blob with a download link that points at the aggregator. To try it, save the file locally and open it in a browser.

<!-- ImportContent: GitHub source — resolve at export time or visit https://github.com/MystenLabs/walrus/blob/main/docs/examples/javascript/blob_upload_download_webapi.html -->

## Example: Read Walrus system state

The following example reads Walrus system state on Sui instead of blob data. The script calls the `sui_getObject` JSON-RPC method on a public Sui full node and renders the per-epoch rewards and used storage capacity from the system state into a table.

<!-- ImportContent: GitHub source — resolve at export time or visit https://github.com/MystenLabs/walrus/blob/main/docs/examples/javascript/system_stats.html -->