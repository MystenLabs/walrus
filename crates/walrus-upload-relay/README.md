# Walrus Upload Relay

# Overview

A goal of Walrus is to enable dApps to `store` to Walrus from within their end-users’ browsers
having low to moderate machine specifications (mobile devices, low-powered laptops, etc.) Currently
this browser-based scenario is difficult to achieve in practice due to the high number of network
connections required to upload all slivers to all shards.

The Upload Relay is a downloadable program that community members, Mysten Labs, and/or dApp writers
themselves can run on internet-facing hosts to facilitate performing this fan-out on behalf of dApp
end-users - thus mitigating browser resource consumption and enabling Web-based `store` operations.

## Design

### Outline

The store sequence is as follows:

- On the client side:
  - The client creates a transaction:
    - Mandatory: The first input, which is not used as the input of any actual contract call,
      contains the hash of the data `h = Hash(blob)`
    - Optional: Any transaction that registers or extends blobs, in any order
    - Mandatory per configuration: Any transaction that will result in the balance of the relay’s
      tip address to increase by the tip amount.
  - The client then executes the transaction, obtaining the transaction ID `tx_id`
  - The client sends the `blob` and `tx_id` to the relay
- On the relay side:
  - The relay requests the effects and balance changes of the transaction `tx_id` from a trusted
    full node, then checks:
    - that the balance changes for its address are sufficient to cover its tip (as described in its
      tip configuration) of storing the blob (possibly considering the length of the blob).
    - that the data at input zero matches `Hash(blob)` of the received data, confirming that the
      received data is the data the tip was paid for.
  - If everything matches, the relay proceeds to storing the blobs, and if successful, in creating
    the certificate.
  - The relay returns the certificate to the client
- Finally, the client certifies the blob

# Usage

There are various ways to run the `walrus-upload-relay`.

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

# Conclusion

Setting up the `walrus-upload-relay` should be straightforward. Please reach out to the Walrus team if you
encounter any difficulties or have questions/concerns.
