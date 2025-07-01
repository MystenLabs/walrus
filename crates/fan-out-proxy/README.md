# Walrus Fan-Out Proxy

# Overview

The `fan-out-proxy` binary is a stateless HTTP proxy that does the work of encoding blobs, and
distributing their slivers out to the Walrus network. The need for this proxy arises from the
reality that some clients will not be able to handle the number of outbound sockets required to
effectively upload large files to the network in a timely manner.

By operating a `fan-out-proxy` you can help the Walrus ecosystem and be rewarded. Fan-out proxy
operators can require clients to pay a small fee (referred to as a "tip") for each blob upload that
the perform.

TODO: include notes on the trust model for fan-out proxies.

# Usage

1. Download the latest release of the `fan-out-proxy` binary from the [releases
   page](https://github.com/MystenLabs/walrus/releases).
2. Start the `fan-out-proxy` binary.

```
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

forwards blob uploads
