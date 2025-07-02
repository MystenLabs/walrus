# Walrus Fan-Out Proxy

# Overview

A goal of Walrus is to enable dApps to `store` to Walrus from within their end-users’ browsers
having low to moderate machine specifications (mobile devices, low-powered laptops, etc.) Currently
this browser-based scenario is difficult to achieve in practice due to the high number of network
connections required to upload all slivers to all shards.

The Fan-out Proxy is a downloadable program that community members, Mysten Labs, and/or dApp writers
themselves can run on internet-facing hosts to facilitate performing this fan-out on behalf of dApp
end-users - thus mitigating browser resource consumption and enabling Web-based `store` operations.

## Design

### Outline

The store sequence is as follows:

- On the client side:
  - The client creates a transaction:
    - Mandatory: The first input, which is not used as the input of any actual contract call, contains the hash of the data `h = Hash(blob)`
    - Optional: Any transaction that registers or extends blobs, in any order
    - Mandatory per configuration: Any transaction that will result in the balance of the proxy’s tip address to increase by the tip amount.
  - The client then executes the transaction, obtaining the transaction ID `tx_id`
  - The client sends the `blob` and `tx_id` to the proxy
- On the proxy side:
  - The proxy requests the effects and balance changes of the transaction `tx_id` from a trusted full node, then checks:
    - that the balance changes for its address are sufficient to cover its tip (as described in its tip configuration) of storing the blob (possibly considering the length of the blob).
    - that the data at input zero matches `Hash(blob)` of the received data, confirming that the received data is the data the tip was paid for.
  - If everything matches, the proxy proceeds to storing the blobs, and if successful, in creating the certificate.
  - The proxy returns the certificate to the client
- Finally, the client certifies the blob

# Usage

There are various ways to run the `fan-out-proxy`.

## Installation

### Download the Binary

If you'd like to download a pre-built binary in order manually run `fan-out-proxy`, you'll need to download it from
[here](https://github.com/MystenLabs/walrus/releases). Note that `fan-out-proxy` does not daemonize
itself, and requires some supervisor process to ensure boot at startup, to restart in the event of
failures, etc.

### Docker

The docker image for `fan-out-proxy` is available on Docker Hub as `mysten/fan-out-proxy`.

```
$ docker run -it --rm mysten/fan-out-proxy --help

Run the Walrus Fan-out Proxy

Usage: fan-out-proxy proxy [OPTIONS] --walrus-config <WALRUS_CONFIG> --fan-out-config <FAN_OUT_CONFIG>

Options:
      --context <CONTEXT>
          The configuration context to use for the client, if omitted the
          default_context is used
      --walrus-config <WALRUS_CONFIG>
          The file path to the Walrus read client configuration
      --server-address <SERVER_ADDRESS>
          The address to listen on. Defaults to 0.0.0.0:57391
      --fan-out-config <FAN_OUT_CONFIG>
          The file path to the configuration of the fan-out proxy
  -h, --help
          Print help
```

### Build from Source

Of course, if you'd like to build from sources, that is always an option, as well. The sources for
the `fan-out-proxy` are available on [GitHub](https://github.com/MystenLabs/walrus) in the
`crates/fan-out-proxy` subdirectory. Running the following from the root of the Walrus repo should
get you a working binary.

```
cargo build --release --bin fan-out-proxy
./target/release/fan-out-proxy --help
```

## Configuration

Notice that `fan-out-proxy` requires some configuration to get started. Below is an example of how
you might place the configuration such that it is reachable when invoking Docker to run the proxy.
For the sake of the example below, we're assuming that:

- `$HOME/.config/fan-out-config.yaml` exists on the host machine and contains the specification for
  the `fan-out-proxy` configuration, as described [here](about:blank).
- `$HOME/.config/walrus/client_config.yaml` exists on the host machine and contains Walrus client
  configuration as specified [here](https://mystenlabs.github.io/walrus-docs/usage/setup.html#configuration).
- Port 3000 is available for the proxy to bind to (change this to whichever port you'd like to
  expose from your host.)

```
docker run \
  -p 3000:3000 \
  -v $HOME/.config/fan-out-config.yaml:/opt/walrus/fan-out-config.yaml \
  -v $HOME/.config/walrus/client_config.yaml:/opt/walrus/client_config.yaml \
  mysten/fan-out-proxy \
    proxy \
      --context testnet \
      --walrus-config /opt/walrus/client_config.yaml \
      --server-address 0.0.0.0:3000 \
      --fan-out-config /opt/walrus/fan-out-config.yaml
```

### Tip Configuration

An example Tip configuration can be found
[here](https://github.com/MystenLabs/walrus/crates/fan-out-proxy/fan_out_config_example.yaml). The
various options are described
[here](https://github.com/MystenLabs/walrus/crates/fan-out-proxy/src/controller.rs#L63) and
[here](https://github.com/MystenLabs/walrus/crates/fan-out-proxy/src/tip/config.rs#L57).

# Conclusion

Setting up the `fan-out-proxy` should be straightforward. Please reach out to the Walrus team if you
encounter any difficulties or have questions/concerns.
