# Announcing Mainnet

Published on: 2025-03-27

## A decentralized network: Mainnet

The production Walrus Mainnet is now live, and operated by
a decentralized network of over 100 storage nodes. Epoch 1 begun on March 25, 2025. The
network can now be used to [publish and retrieve blobs](), [upload and browse Walrus Sites](), as
well as [stake and unstake]() to determine future committees using the live [Mainnet WAL token]().
The Walrus protocol health is overseen by an [independent foundation]() that is now
[well resourced]() to support future development and growth.

All this to say: Walrus is ready for prime time. On Mainnet the Walrus security properties
should hold. And Walrus is now ready to satisfy the needs of real applications.

This is a significant milestone a little over 12 months after an initial small team started
designing the Walrus protocol. And a special thanks is due to the community of storage operators
that supported the development of Walrus through consecutive Testnet(s), as well as all
community members that integrated the protocol early and provided feedback to improve it.

## New features

Besides the promise of stability and security, the Mainnet release of Walrus comes with a few
notable new features and changes:

- **Blob attributes.** Each Sui blob objects can have multiple attributes and values attached to it,
  to encode application meta-data. In due course the aggregator will use this facility to return
  correct content types and file names.

- **Burn blob objects on Sui**. The command line `walrus` tool is extended with commands to
  "burn" Sui blob objects to reclaim the associated storage fee, making it cheaper to store blobs.

- **CLI expiry time improvements.** Now blob expiry can be expressed more flexibly when storing
  blobs, through `--epochs max`, an RFC3339 date with `--earliest-expiry-time`, or the concrete end
  epoch with `--end-epoch`.

- **RedStuff with Reed-Solomon codes.** The erasure code underlying
  RedStuff was changed from RaptorQ to Reed-Solomon (RS) codes. Extended benchmarking suggest the
  performance is similar, and the use of RS codes ensures blobs can always be reconstructed
  given a threshold of slivers.

- TLS handing for storage node.

- **JWT authentication for publisher.** Now the publisher can be configured to only provide
  services to authenticated users via consuming JWT tokens that can be distributed through any
  authentication mechanism.

- **Extensive logging and metrics.** All services, from the storage node to aggregator and publisher
  export metrics that can be used to build dashboards, and logs at multiple levels to troubleshoot
  their operation.

- **Health endpoint.** Storage nodes and the CLI include a `health` command to check the status and
  basic information of storage nodes. This is useful when allocating stake as well as monitoring
  the network.

- Walrus Sites.


## Testnet future plans

The current Walrus Testnet will soon be wiped and restarted to align the codebase to the Mainnet
release. Going forward we will regularly wipe the Testnet, every few months. Developers should use
the Walrus Mainnet to get any level of stability. The Walrus Testnet is only there to test new
features before they are deployed in production.

Walrus Sites on Testnet will also follow similar restrictions to Mainnet, and require a Suins name
to be displayed through the Testnet portal. We may also deploy additional restrictions on the
public portal to reduce abuse. Developers are encourages to use a local Sites portal for tests
and development activities.

## Open source Walrus codebase

The Walrus codebase, including all smart contracts in Move, services in Rust, and documentation, is
now open sourced under an Apache 2.0 license, and hosted on [github]().

Developers may find the Rust CLI client, and associated aggregator and publisher services of most
interest. These can be extended to specialize services to specific operational needs. They are
also a good illustration of how to interface with Walrus using Rust clients. A cleaner and more
stable Rust SDK is in the works as well.

## Publisher improvements and plans

Publishing blobs on Mainnet consumes real WAL and SUI. To avoid uncontrolled costs to operations the
publisher service was augmented with facilities to authenticate requests and account for costs per
user.
