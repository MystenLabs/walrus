> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

An aggregator serves blob reads over HTTP, and a publisher accepts blob uploads over HTTP. Both
roles run through the Walrus client's daemon mode: `walrus aggregator` for reads, `walrus publisher`
for stores, or `walrus daemon` for both (see
[Operate an Aggregator](/docs/operator-guide/aggregators/operating-aggregator) and
[Operate a Publisher](/docs/operator-guide/publishers/operating-publisher)). You do not need to run
your own daemon to get started: community operators expose public aggregator and publisher services
that you can call directly with any HTTP client.

See also:

- [Network Reference](/docs/network-reference#aggregators-and-publishers) for the Mysten Labs
  reference endpoints and the stable HTTP API paths
- [Storing Blobs with the HTTP API](/docs/http-api/storing-blobs) for the full store request and
  response reference
- [Reading Blobs with the HTTP API](/docs/http-api/reading-blobs) for reads by blob ID or Sui
  object ID

## Using a public aggregator or publisher {#public-services}

To read or store a blob through a public service:

1. Pick an endpoint from the [aggregators and publishers list](#agg-list) below, or use a Mysten
   Labs reference endpoint from the [Network Reference](/docs/network-reference#aggregators-and-publishers).
2. Set `$AGGREGATOR` or `$PUBLISHER` to the endpoint URL.
3. Send a standard HTTP request to the `/v1` API paths, as in the following examples.

Read a blob through an aggregator:

```sh
$ curl "$AGGREGATOR/v1/blobs/<BLOB_ID>" -o <FILE_NAME>
```

Store a blob through a Testnet publisher, in this example for 5 storage epochs:

```sh
$ curl -X PUT "$PUBLISHER/v1/blobs?epochs=5" --upload-file "some/file"
```

The same services also expose the [quilt endpoints](/docs/http-api/quilt-http-apis) for storing and
reading batches of small blobs.

Walrus aggregators and publishers expose their full API specifications at the path `/v1/api`. View
this path in a browser, for example, at https://aggregator.walrus-testnet.walrus.space/v1/api. The
[Walrus GitHub repository](https://github.com/MystenLabs/walrus/tree/main/crates/walrus-service)
hosts the latest version of these specifications in HTML and YAML format.

### Network availability

On Walrus Testnet, many entities run public aggregators and publishers. On Mainnet, community
operators run public aggregators, but no one runs a public publisher without authentication,
because a publisher consumes both SUI and WAL on the service side. For production upload options,
see [Choose your upload path](/docs/getting-started#choose-your-upload-path) and the
[Mainnet Publisher Production Guide](/docs/operator-guide/publishers/mainnet-production-guide).

### Request size limits

Most aggregators and publishers limit requests to 10 MiB by default. If you want to upload larger
files, [run your own publisher](/docs/operator-guide/publishers/operating-publisher#local-daemon)
or use the [CLI](/docs/walrus-client/storing-blobs).

> **Tip**
>
> Do not hardcode a single community endpoint in production code, because community endpoints change
> over time. Use the operator list below, run your own service, or use the Mysten Labs reference
> endpoints.
### Machine-readable operator list

Walrus also provides the [operator lists in JSON format](pathname:///operators.json), grouped by
network and service type. For each aggregator, the JSON list records whether the operator deploys
it with [caching functionality](/docs/system-overview/caching) and whether it currently passes a
functionality check. The list receives updates once per week.

### Aggregators and publishers list {#agg-list}