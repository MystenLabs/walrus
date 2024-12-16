# Walrus Testnet Move contracts

This is the Move source code for the current Walrus Testnet instance, which are deployed on Sui
Testnet. The latest version information can be found at the bottom of the
[`walrus/Move.lock`](./walrus/Move.lock) file.

## Updating the contracts

To update the contracts, you need access to the wallet that published the contracts (address
`0x4e7e5b9737bab476d216a36f2980627b4060ea486de8e4b0cd8dbdd3c768b138`). Then, do the following:

1. Modify the source files in this directory and commit your changes.
1. Create a draft PR and have it reviewed.
1. Publish the updated contracts and update the version information in the `walrus/Move.lock`:

   ```sh
   PACKAGE=0xa30c13a4a3c4710804db42701101efb75b2799e20d7d803c6a6b1ad3c0407c8d
   ADMIN_WALLET=/path/to/admin/wallet.yaml
   sui client --client.config "$ADMIN_WALLET" switch --env testnet
   UPGRADE_CAP=$(sui client --client.config "$ADMIN_WALLET" objects --json | jq -r '.[] | select(.data.type|test("UpgradeCap")) | .data.objectId')
   sui client --client.config "$ADMIN_WALLET" upgrade --upgrade-capability "$UPGRADE_CAP" --with-unpublished-dependencies

   # Take the latest package ID and version number from the output of the above command.
   NEW_PACKAGE_ID=
   NEW_PACKAGE_VERSION=

   sui move manage-package --environment "$(sui client active-env)" \
   --network-id "$(sui client --client.config "$ADMIN_WALLET" chain-identifier)" \
   --original-id "$PACKAGE" \
   --latest-id "$NEW_PACKAGE_ID" \
   --version-number "$NEW_PACKAGE_VERSION"
   ```

1. Create a commit, push your changes, get the PR approved, and merge your changes.
1. Update the contracts in the [walrus-docs](https://github.com/MystenLabs/walrus-docs) repository.
