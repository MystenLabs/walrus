# Client Daemon mode & HTTP API

In addition to the CLI and JSON modes, the Walrus client offers a *daemon mode*. In this mode, it
runs a simple web server offering HTTP interfaces to store and read blobs in an *aggregator* and
*publisher* role respectively. We also offer
[public aggregator and publisher services](#public-services) to try the Walrus HTTP APIs without
the need to run a local client.

## Starting the daemon locally {#local-daemon}

You can run a local Walrus daemon through the `walrus` binary. There are three different commands:

- `walrus aggregator` starts an "aggregator" that offers an HTTP interface to read blobs from
  Walrus.
- `walrus publisher` starts a "publisher" that offers an HTTP interface to store blobs in Walrus.
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
argument; any existing directory can be used. If it already contains sub-wallets, they will be
reused.

By default, 8 sub-wallets are created and funded. This can be changed with the `--n-clients`
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
publisher does perform actions on-chain and will consume both SUI and WAL tokens. It is therefore
important to ensure only authorized parties may access it, or other measures to manage gas costs,
especially in a future Mainnet deployment.
```

By default, PUT requests are limited to 10 MiB; you can increase this limit through the
`--max-body-size` option.

### Daemon metrics

Services by default export a metrics end-point accessible via `curl http://127.0.0.1:27182/metrics`.
It can be changed using the `--metrics-address <METRICS_ADDRESS>` CLI option.

## Publisher operation and configuration

We list here a few important details on how the publisher deals with funds and objects on Sui.

### Number of sub-wallets and upload concurrency

As mentioned above, the publisher uses sub-wallets to allow storing blobs in parallel. By default,
the publisher uses 8 sub-wallets, meaning it can handle 8 blob store HTTP requests concurrently.

### SUI coin management in sub-wallets

Each of the sub-wallets requires funds to interact with the chain and purchase storage. For this
reason, a background process checks periodically if the sub-wallets have enough funds. In steady
state, each of the sub-wallets will have a balance of 0.5-1.0 SUI and WAL. The amount and triggers
for coin refills can be configured through CLI arguments.

### Lifecycle of created `Blob` on-chain objects

Each store operation in Walrus creates a `Blob` object on Sui. This blob object represents the
(partial) ownership over the associated data, and allows certain data management operations (e.g.,
in the case of deletable blobs).

When the publisher stores a blob on behalf of a client, the `Blob` object is initially owned by the
sub-wallet that stored the blob. Then, the following cases are possible, depending on the
configuration:

- If the client requests to store a blob and specifies the `send_object_to` query parameter (see
  [the relevant section](#store) for examples), then the `Blob` object is transferred to the
  specified address. This is a way for clients to get back the created object for their data.
- If the `send_object_to` query parameter is not specified, two cases are possible:
  - If the publisher was run with the `--keep` flag, then the sub-wallet transfers the
    newly-created blob object to the main wallet, such that all these objects are kept there.
  - If the `--keep` flag was omitted, then the sub-wallet *immediately burns* the `Blob` object.
    Since no one has requested the object, and the availability of the data on Walrus is independent
    of the existence of such object, it is safe to do so. This is to avoid cluttering the sub-wallet
    with many blob objects.

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
curl -X PUT "$PUBLISHER/v1/blobs" -d "some string" # store the string `some string` for 1 storage epoch
curl -X PUT "$PUBLISHER/v1/blobs?epochs=5" --upload-file "some/file" # store file `some/file` for 5 storage epochs
curl -X PUT "$PUBLISHER/v1/blobs?send_object_to=$ADDRESS" --upload-file "some/file" # store file `some/file` and send the blob object to $ADDRESS
curl -X PUT "$PUBLISHER/v1/blobs?deletable=true" --upload-file "some/file" # store file `some/file` as a deletable blob, instead of a permanent one
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

Blobs may be read from an aggregator or daemon using HTTP GET. For example, the following cURL
command reads a blob and writes it to an output file:

```sh
curl "$AGGREGATOR/v1/blobs/<some blob ID>" -o <some file name>
```

Alternatively you may print the contents of a blob in the terminal with the cURL command:

```sh
curl "$AGGREGATOR/v1/blobs/<some blob ID>"
```

<!-- TODO(WAL-710): Mention blob metadata attribute. -->

```admonish tip title="Content sniffing"
Modern browsers will attempt to sniff the content type for such resources, and will generally do a
good job of inferring content types for media. However, the aggregator on purpose prevents such
sniffing from inferring dangerous executable types such as JavaScript or style sheet types.
```

## Using a public aggregator or publisher {#public-services}

For some use cases (e.g., a public website), or to just try out the HTTP API, a publicly accessible
aggregator and/or publisher is required. On Walrus Testnet, many entities run public aggregators and
publishers. On Mainnet, there are no public publishers without authentication, as they consume both
SUI and WAL.

See the following subsections for [public aggregators on Mainnet](#mainnet) and public
[aggregators](#aggregators-testnet) and [publishers](#publishers-testnet) on Testnet. All of these
are also available in JSON format [here](../assets/operators.json).

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
[run your own publisher](#local-daemon) or use the [CLI](./client-cli.md).
