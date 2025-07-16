# Walrus Upload Relay

## Overview

A goal of Walrus is to enable dApps to `store` to Walrus from within their end-users’ browsers
having low to moderate machine specifications (mobile devices, low-powered laptops, etc.) Currently
this browser-based scenario is difficult to achieve in practice due to the high number of network
connections required to upload all slivers to all shards.

The Upload Relay is a downloadable program that community members, Mysten Labs, and/or dApp writers
themselves can run on internet-facing hosts to facilitate performing this fan-out on behalf of dApp
end-users, thus mitigating browser resource consumption and enabling Web-based `store` operations.

## Design

### Outline

At a high level, a client store a blob using an Upload Relay as follows:

- First, the client locally encodes the blob and registers it on Sui;
- then, the client sends the blob to the Upload Relay via an HTTP POST request to the blob-relay
  endpoint (`/v1/blob-upload-relay`);
- the Upload Relay then encodes the blob, sends the slivers to the storage nodes, collects a storage
  confirmation certificate, and sends it back to the client;
- finally, the client uses the confirmation certificate to certify the blob on Sui.

Therefore, the Upload Relay _does not_ perform any on-chain operation, and only helps clients
distribute the slivers of their blobs to storage nodes.

### Upload Relay Operation

The Upload Relay can be operated in two ways:

- **Free service**: It accepts simple HTTP POST requests with blobs from clients, and relays them to
  the storage nodes for free.
- **Paid service** (for public relays): In this configuration, the Upload Relay requires a _tip_ to
  relay a blob. The tip can be used by the relay operator to cover the costs of running the
  infrastructure and turn a profit on the service. At the moment, we allow a constant tip, and a tip
  that is linear in the unencoded data size.

Upload Relays expose a tip-config endpoint (`/v1/tip-config`) that returns the tipping configuration. For example:

```json
{
  "send_tip": {
    "address": "0x1234...",
    "kind": {
      "const": 105
    }
  }
}
```

The configuration above specifies that every store operation requires a tip of `105 MIST` (arbitrary
value), paid to the set address (`0x1234..`). Note that this configuration is provided even for free
Upload Relays (returning `"no_tip"`).

### Paying the Tip

This step is only necessary if the relay requires a tip.

To pay the tip, the client proceeds as follows:

- it computes the `blob_digest = SHA256(blob)`
- it generates a random `nonce`, and hashes it `nonce_digest = SHA256(nonce)`
- it computes the `unencoded_length = blob.len()`

Then, it creates a PTB, where the first input (`input 0`) is the `bcs` encoded representation of
`blob_digest || nonce_digest || unencoded_length`. This will later be used by the relay to
authenticate the sender of the store request. In the same PTB, the client transfers the appropriate
tip amount to the relay’s wallet (also found through the `/v1/tip-config` endpoint). Usually, the
client would also register the blob in this transaction. This is not mandatory, and the blob can be
registered in another transaction, but it is commonly convenient and cheaper to perform all these
operations together.

Once the transaction is executed, the client keeps the transaction ID `tx_id` , the `nonce`, and the
`blob_id`, which are required for the next phase.

Note: the relay will enforce a freshness check on the transaction that paid the tip (currently 1h by
default, but each relay can independently configure this).

### Sending Data to the Upload Relay

See the full OpenAPI spec for the upload relay for the full details ([yaml](./upload_relay_openapi.yaml), [html](./upload_relay_openapi.html))

Essentially, the client sends a POST request to the `/v1/blob-upload-relay` API endpoint on the relay,
containing the bytes of the blob to be stored in the body.

These additional parameters have to be specified in the URL’s query string:

- **blob_id** (required): The blob ID of the blob to be stored. Example:
  `blob_id=E7_nNXvFU_3qZVu3OH1yycRG7LZlyn1-UxEDCDDqGGU`
- **tx_id** (required if the relay requires tip): The transaction ID (Base58 encoded) of the
  transaction that transferred the TIP to the relay. Example:
  `tx_id=EmcFpdozbobqH61w76T4UehhC4UGaAv32uZpv6c4CNyg`
- **nonce** (required if the relay requires tip): The `nonce`, the preimage of the hash added to the
  transaction inputs created above, as a Base64 URL-encoded string without padding.
  `nonce=rw8xIuqxwMpdOcF_3jOprsD9TtPWfXK97tT_lWr1teQ`
- **deletable_blob_object** (required if the blob is registered as deletable): If the blob being
  stored is deletable, then the client must specify the object ID (as a Hex string) of the Blob. If
  the blob is to be stored as a permanent one, this parameter should not be specified. Example:
  `deletable_blob_object=0x56ae1c86e17db174ea002f8340e28880bc8a8587c56e8604a4fa6b1170b23a60`
- **encoding_type** (optional): The encoding type to be used for the blob. The default value, and at
  the moment the only value, is `RS2` .

## Receiving the certificate

Once the relay is done storing the blob, it will collect the confirmation certificate from the
storage nodes.

Then, it will send a response to the client, containing the `blob_id` of the stored blob, along with
the `confirmation_certificate`. The client can then use this certificate to certify the blob on
chain.

## Installation

### Download the Binary

If you'd like to download a pre-built binary in order manually run `walrus-upload-relay`, you'll need to download it from
[here](https://github.com/MystenLabs/walrus/releases). Note that `walrus-upload-relay` does not daemonize
itself, and requires some supervisor process to ensure boot at startup, to restart in the event of
failures, etc.

### Docker

The docker image for `walrus-upload-relay` is available on Docker Hub as `mysten/walrus-upload-relay`.

```
$ docker run -it --rm mysten/walrus-upload-relay --help
```

### Build from Source

Of course, if you'd like to build from sources, that is always an option, as well. The sources for
the `walrus-upload-relay` are available on [GitHub](https://github.com/MystenLabs/walrus) in the
`crates/walrus-upload-relay` subdirectory. Running the following from the root of the Walrus repo should
get you a working binary.

```
cargo build --release --bin walrus-upload-relay
./target/release/walrus-upload-relay --help
```

## Configuration

Notice that `walrus-upload-relay` requires some configuration to get started. Below is an example of how
you might place the configuration such that it is reachable when invoking Docker to run the relay.
For the sake of the example below, we're assuming that:

- `$HOME/.config/walrus/walrus_upload_relay_config.yaml` exists on the host machine and contains the specification for
  the `walrus-upload-relay` configuration, as described [here](about:blank).
- `$HOME/.config/walrus/client_config.yaml` exists on the host machine and contains Walrus client
  configuration as specified [here](https://mystenlabs.github.io/walrus-docs/usage/setup.html#configuration).
- Port 3000 is available for the relay to bind to (change this to whichever port you'd like to
  expose from your host.)

```
docker run \
  -p 3000:3000 \
  -v $HOME/.config/walrus/walrus_upload_relay_config.yaml:/opt/walrus/walrus_upload_relay_config.yaml \
  -v $HOME/.config/walrus/client_config.yaml:/opt/walrus/client_config.yaml \
  mysten/walrus-upload-relay \
    --context testnet \
    --walrus-config /opt/walrus/client_config.yaml \
    --server-address 0.0.0.0:3000 \
    --relay-config /opt/walrus/walrus_upload_relay_config.yaml
```

### Configuration

An example `walrus-upload-relay` configuration file can be found
[here](./walrus_upload_relay_config_example.yaml). The
various options are described
[here](./src/controller.rs#L63) and
[here](./src/tip/config.rs#L57).

## Conclusion

Setting up the `walrus-upload-relay` should be straightforward. Please reach out to the Walrus team if you
encounter any difficulties or have questions/concerns.
