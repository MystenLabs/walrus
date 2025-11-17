# Operating an aggregator or publisher
<!-- TODO (WAL-118): Add further details and example cache setup. -->

This page describes how you can run a Walrus aggregator or publisher exposing the [HTTP
API](../usage/web-api.md).

## Starting the daemon locally {#local-daemon}

You can run a local Walrus daemon through the `walrus` binary. There are three different commands:

- `walrus aggregator` starts an aggregator that offers an HTTP interface to read blobs from
Walrus.
- `walrus publisher` starts a publisher that offers an HTTP interface to store blobs in Walrus.
- `walrus daemon` offers the combined functionality of an aggregator and publisher on the same
address and port.

The aggregator does not perform any on-chain actions, and only requires specifying the address on
which it listens:

```sh
walrus aggregator --bind-address "127.0.0.1:31415"
```

The publisher and daemon perform on-chain actions and thus require a Sui Testnet wallet with
sufficient SUI and WAL balances. To enable handling many parallel requests without object
conflicts, they create internal sub-wallets since version 1.4.0, which are funded from the main
wallet. These sub-wallets are persisted in a directory specified with the `--sub-wallets-dir`
argument. Any existing directory can be used. If it already contains sub-wallets, they are
reused.

By default, 8 sub-wallets are created and funded. You can change this with the `--n-clients`
argument. For simple local testing, 1 or 2 sub-wallets are usually sufficient.

For example, you can run a publisher with a single sub-wallet stored in the Walrus configuration
directory with the following command:

```sh
PUBLISHER_WALLETS_DIR=~/.config/walrus/publisher-wallets
mkdir -p "$PUBLISHER_WALLETS_DIR"
walrus publisher \
  --bind-address "127.0.0.1:31416" \
  --sub-wallets-dir "$PUBLISHER_WALLETS_DIR" \
  --n-clients 1
```

Replace `publisher` by `daemon` to run both an aggregator and publisher on the same address and
port.

```admonish warning
While the aggregator does not perform Sui on-chain actions, and therefore consumes no gas, the
publisher does perform actions on-chain and consumes both SUI and WAL tokens.
Ensure only authorized parties can access it, or use other measures to manage gas costs,
especially in a future Mainnet deployment.
```

### Limitations

By default, [store blob](../usage/web-api.md#store) requests are limited to 10 MiB. You can
increase this limit through the `--max-body-size` option.
[Store quilt](../usage/web-api.md#storing-quilts) requests are limited to 100 MiB by default, and
you can increase them using the `--max-quilt-body-size` option.

If the aggregator or publisher is hosted on a third-party platform, ensure that any
additional platform-imposed limits or constraints are compatible with your intended use
case.

### Daemon metrics

Services by default export a metrics endpoint accessible through
`curl http://127.0.0.1:27182/metrics`. You can change it using the
`--metrics-address <METRICS_ADDRESS>` CLI option.

### Sample systemd configuration

Below is an example of an aggregator node that hosts an HTTP endpoint that you can use
to fetch data from Walrus over the web.

The aggregator process is run through the `walrus` client binary as discussed
previously. You can run it in many ways, one example being through a systemd service:

```ini
[Unit]
Description=Walrus Aggregator

[Service]
User=walrus
Environment=RUST_BACKTRACE=1
Environment=RUST_LOG=info
ExecStart=/opt/walrus/bin/walrus --config /opt/walrus/config/client_config.yaml aggregator --bind-address 0.0.0.0:9000
Restart=always

LimitNOFILE=65536
```

## Publisher operation and configuration

This section lists a few important details on how the publisher deals with funds and
objects on Sui.

### Number of sub-wallets and upload concurrency

As mentioned previously, the publisher uses sub-wallets to allow storing blobs in parallel.
By default, the publisher uses 8 sub-wallets, meaning it can handle 8 blob store HTTP requests concurrently.

In order to operate a high-performance and concurrency publisher, the following options
might be of interest:

- The `--n-clients <NUM>` option creates a number of separate wallets used to perform concurrent
   Sui chain operations. Increase this to allow more parallel uploads. A higher number
   requires more SUI and WAL coins initially to be distributed to more wallets.

- The `--max-concurrent-requests <NUM>` determines how many concurrent requests can be handled,
  including Sui operations (limited by number of clients) but also uploads. After this is exceeded,
  more requests are queued up to the `--max-buffer-size <NUM>`, after which requests are rejected
  with an HTTP 429 code.

### SUI coin management in sub-wallets

Each of the sub-wallets requires funds to interact with the chain and purchase storage.
For this reason, a background process checks periodically if the sub-wallets have enough
funds. In steady state, each of the sub-wallets has a balance of 0.5-1.0 SUI and WAL.
You can configure the amount and triggers for coin refills through CLI arguments.

To tweak how refills are handled, you can use the `--refill-interval <REFILL_INTERVAL>`,
`--gas-refill-amount <GAS_REFILL_AMOUNT>`, `--wal-refill-amount <WAL_REFILL_AMOUNT>`,
and `--sub-wallets-min-balance <SUB_WALLETS_MIN_BALANCE>` arguments.

### Lifecycle of created `Blob` on-chain objects

Each store operation in Walrus creates a `Blob` object on Sui. This blob object represents the
(partial) ownership over the associated data, and allows certain data management operations
(for example, in the case of deletable blobs).

When the publisher stores a blob on behalf of a client, the `Blob` object is initially owned by the
sub-wallet that stored the blob. Then, the following cases are possible, depending on the
configuration:

- If the client requests to store a blob and specifies the `send_object_to` query
  parameter (see [the relevant section](../usage/web-api.md#store) for examples), then
  the `Blob` object is transferred to the specified address. This is a way for clients
  to get back the created object for their data.
- If the `send_object_to` query parameter is not specified, two cases are possible:
  - By default the sub-wallet transfers the
    newly-created blob object to the main wallet, such that all these objects are kept there.
    This behavior can be changed by setting the `--burn-after-store` flag, and the blob object
    is then immediately deleted.
  - However, note that this flag *does not affect* the use of the `send_object_to` query parameter:
    Regardless of this flag's status, the publisher will send created objects to the address in
    the `send_object_to` query parameter, if it is specified in the PUT request.

### Advanced publisher uses

The setup and use of an authenticated publisher is covered in a [separate section](./auth-publisher.md).

### Advanced aggregator uses

#### Large file support

As of Walrus `v1.38.0` the aggregator can perform concatenation of multiple blobs via the
`/v1alpha/blobs/concat` endpoint. This endpoint enables delivery of very large files - which would
otherwise be unsupported due to individual blob size restrictions. See
[`walrus-store-sliced.sh`](../../examples/shell/walrus-store-sliced.sh) for an example of how a very
large file can be sliced and uploaded to Walrus. Once the slices are uploaded, the very large file
can be read directly by your downstream users by `GET` URL construction (listing the blob slices in
the query params), or via a `POST` request with a JSON body listing the ids. Details of this api are
available in the online aggregator docs. These can be found at `<your-aggregator-url>/v1/api`. Note
that this endpoint is still under development and its specifications or behavior may change prior to
becoming stable.

For more information about maximum blob sizes, run `walrus info`.
