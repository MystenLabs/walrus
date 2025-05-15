# Walrus Testnet Move contracts

This is the Move source code for the current Walrus Testnet instance, which are deployed on Sui
Testnet. The latest version information can be found at the bottom of the
[`walrus/Move.lock`](./walrus/Move.lock) file.

## Updating the contracts

To update the contracts, you need access to the wallet that published the contracts (address
`0x181816cd2efb860628385e8653b37260d0d065c844803b23852799cc19ee2c28`). Then, do the following:

1. Modify the source files in this directory and commit your changes.
1. Create a draft PR and have it reviewed.
1. Publish the updated contracts:
   - Either as quorum-based upgrade by having the storage nodes vote on the upgrade (using the
     `walrus node-admin vote-for-upgrade` command, and then deploying the upgrade using
     `walrus-deploy upgrade`).
   - Or by using the `walrus-deploy emergency-upgrade` command.
1. Create a commit, push your changes, get the PR approved, and merge your changes.
