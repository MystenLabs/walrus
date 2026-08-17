> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

The following JavaScript example shows how to upload and download a blob through a web form using the HTTP API.

The page takes a publisher URL, an aggregator URL, and a storage duration in epochs. Uploading sends the selected file as the body of a `PUT` request to the publisher's `/v1/blobs` endpoint, and the response reports whether the publisher registered a new blob or found one already certified. Either way the response carries a blob ID, which the page turns into a link to `/v1/blobs/{blobId}` on the aggregator so you can read the blob back.

Two limits shape what the example can do. The default publisher caps uploads at 10 MiB, and the aggregator returns every blob as `application/octet-stream`, so the page infers a media type before it can display an image inline.

<!-- ImportContent: GitHub source — resolve at export time or visit https://github.com/MystenLabs/walrus/blob/main/docs/examples/javascript/blob_upload_download_webapi.html -->