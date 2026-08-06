> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

The Walrus client lets you check a blob's storage status and certification, download its data, and control the consistency checks that protect reads.

See also:

- [Reading blobs over HTTP](/docs/http-api/reading-blobs) to read through an aggregator instead
- [Verify Blob Availability Before Acting](/docs/walrus-client/verifying-availability) for the onchain checks to run before depending on a blob
- [Reading Blobs Right After Upload](/docs/troubleshooting/reading-blobs-after-upload) if a freshly stored blob returns `404` from an aggregator

## Check blob status

You can query the status of a blob through one of the following commands:

```sh
$ walrus blob-status --blob-id <BLOB_ID>
$ walrus blob-status --file <FILE>
```

Each command returns output that indicates whether Walrus has stored the specified blob, along with its availability period. If you specify a file with the `--file` option, the CLI re-encodes the content of the file and derives the blob ID before checking the status. For a permanent blob, the output also includes an estimated expiry timestamp.

For an available blob, the `blob-status` command also returns the `BlobCertified` Sui event ID, which consists of a transaction ID and a sequence number in the events emitted by the transaction. The existence of this event certifies the availability of the blob.

Status requests to storage nodes time out after 1 second by default. Use the `--timeout <TIMEOUT>` option (for example, `--timeout 5s`) to adjust this on slow connections.

## Read blobs

Read blobs from Walrus using the following command:

```sh
$ walrus read <BLOB_ID>
```

The client fetches slivers directly from storage nodes and reconstructs the blob locally, so reads do not depend on an aggregator or its caches.

By default, the client writes the blob data to the standard output. Use the `--out <OUT>` CLI option to specify an output file name:

```sh
$ walrus read <BLOB_ID> --out <FILE>
```

Use `--rpc-url <URL>` to specify a Sui RPC node instead of the currently configured RPC node set in the CLI configuration file or wallet configuration.

With the `--json` flag, or in [JSON mode](/docs/walrus-client/json-mode), the `read` command prints a JSON object containing the blob ID and, when no output file is set, the blob content as a Base64-encoded string.

To read individual blobs stored inside a quilt, use the `read-quilt` command instead. See [quilts](/docs/walrus-client/quilts#read-blobs-from-a-quilt) for details.

## Check consistency

Walrus performs integrity and consistency checks to ensure that any data read from Walrus is what the writer intended, and that the writer encoded the blob correctly. See the [data consistency](/docs/system-overview/red-stuff) documentation for further details.

Prior to `v1.37`, the Walrus CLI and aggregator always performed the [strict consistency check](/docs/system-overview/red-stuff). Starting with `v1.37`, the default is a [more performant consistency check](/docs/system-overview/red-stuff), which is sufficient for most cases. You can enable the strict consistency check through the `--strict-consistency-check` flag:

```sh
$ walrus read <BLOB_ID> --out <FILE> --strict-consistency-check
```

You can disable consistency checks completely with the `--skip-consistency-check` flag. Only use this if the writer of the blob is known and trusted. Skipping the consistency check does not affect the authentication checks for data received from storage nodes, which the client always performs. The two flags conflict, so pass at most one of them.

When reading through an aggregator instead of the CLI, the equivalent query parameters are `strict_consistency_check=true` and `skip_consistency_check=true`. See [consistency checks over HTTP](/docs/http-api/reading-blobs#consistency-checks).