# Walrus Rust SDK

## Requirements Gathering

Here are some thoughts on this initial "design" commit:

1. This crate should provide a minimum viable interface for builders to use Walrus as a durable
   object store. {store, read, metadata, ?}.
2. We might want to move this crate to its own repo to strengthen the decoupling between this code
   and the whole Walrus codebase. Compiling (or even just downloading) the official Walrus repo to
   build the Rust SDK is a relatively significant point of friction for potential users that we
   could avoid.
3. TODO: We should consider how Sui and Wal coins integrate into the interface. So far, they are
   only represented in the error interface.
4. TODO: Consider whether the Sites, Publisher or Aggregator interfaces should be treated here,
   etc...

## Implementation

Next steps might involve hacking together the existing codebase with this interface in order to get
something working end-to-end. Or, we can start chipping away at the shared dependencies to decouple
them. This is a potential point for further discussion.
