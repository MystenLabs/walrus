# Walrus Testnet Move contracts

This is the Move source code for the current Walrus Testnet instance, which are deployed on Sui
Testnet. The latest version information can be found at the bottom of the
[`walrus/Move.lock`](./walrus/Move.lock) file.

## Updating the contracts

To update the contracts, you need access to the wallet that published the contracts (address
`0x181816cd2efb860628385e8653b37260d0d065c844803b23852799cc19ee2c28`). Then, do the following:

1. Modify the source files in this directory and commit your changes.
1. Create a draft PR and have it reviewed.
1. Publish the updated contracts and update the version information in the `walrus/Move.lock`:

   ```sh
   WALRUS_ORIGINAL_PKG_ID=0xd84704c17fc870b8764832c535aa6b11f21a95cd6f5bb38a9b07d2cf42220c66
   ADMIN_WALLET=/path/to/admin/wallet.yaml
   sui client --client.config "$ADMIN_WALLET" switch --env testnet
   UPGRADE_CAP=$(sui client --client.config "$ADMIN_WALLET" objects --json | jq -r '.[] | select(.data.type|test("UpgradeCap")) | .data.objectId')
   sui client --client.config "$ADMIN_WALLET" upgrade --upgrade-capability "$UPGRADE_CAP" --with-unpublished-dependencies

   # Take the latest package ID and version number from the output of the above command.
   NEW_PKG_ID=
   NEW_PKG_VERSION=

   sui move manage-package --environment "$(sui client active-env)" \
   --network-id "$(sui client --client.config "$ADMIN_WALLET" chain-identifier)" \
   --original-id "$WALRUS_ORIGINAL_PKG_ID" \
   --latest-id "$NEW_PKG_ID" \
   --version-number "$NEW_PKG_VERSION"
   ```

1. Create a commit, push your changes, get the PR approved, and merge your changes.
