You can change most node parameters using `StorageNodeCap`, and the storage node automatically updates them based on the values in the node configuration. However, authorization for contract upgrades and withdrawing the storage node commission is handled separately. This ensures that the hot wallet on the storage node does not need authorization for these operations.

You should designate secure wallets for these operations that are not stored on the storage node machines.

This page explains how to update the entities authorized for governance (contract upgrade) and commission operations, and how to perform these operations.

## Update the commission receiver and entity authorized for governance

You can either designate an arbitrary object for governance and commission operations, or designate an address to be authorized.

:::caution

After you set the authorization, only the authorized entity can change it again. Only set authorization to addresses or objects that you control. Make sure they remain accessible.

:::

To set the authorization to receive the commission and perform governance operations, you can use the `walrus` binary in your CLI or the [node operations web interface](https://stake-wal.wal.app/node-operations).

### Use the CLI

The following assumes that you have the `walrus` binary [correctly set up](/docs/getting-started/advanced-setup) and are using the wallet that is currently authorized to perform these operations. If this is the first time updating the authorized entities, this is the wallet that you used to set up the storage node. To specify a wallet and configuration that are not in the standard locations, use the `--wallet` and `--config` command line arguments.

The authorized entities for commission and governance are independent and do not need to be set to the same address or object.

```sh
$ NODE_ID=            # Set this to your node ID.
$ COMMISSION_AUTHORIZED_ADDRESS= # Set this to a secure wallet address that you control.
$ GOVERNANCE_AUTHORIZED_ADDRESS= # Set this to a secure wallet address that you control.
$ walrus node-admin set-commission-authorized --node-id $NODE_ID --address $COMMISSION_AUTHORIZED_ADDRESS
$ walrus node-admin set-governance-authorized --node-id $NODE_ID --address $GOVERNANCE_AUTHORIZED_ADDRESS
```

Instead of specifying an authorized address using the `--address` flag, you can designate an arbitrary object as a capability using the `--object` flag:

```sh
$ NODE_ID=           # Set this to your node ID.
$ COMMISSION_AUTHORIZED_OBJECT= # Set this to the ID of an object that you own in a secure wallet.
$ GOVERNANCE_AUTHORIZED_OBJECT= # Set this to the ID of an object that you own in a secure wallet.
$ walrus node-admin set-commission-authorized --node-id $NODE_ID --object $COMMISSION_AUTHORIZED_OBJECT
$ walrus node-admin set-governance-authorized --node-id $NODE_ID --object $GOVERNANCE_AUTHORIZED_OBJECT
```

### Use the web interface

Go to the [operator panel on the staking app](https://stake-wal.wal.app/node-operations), connect your wallet, and select your node through the drop-down menu or by pasting your node ID. Then select **Set Commission Receiver** or **Set Governance Authorized** and follow the steps to send the transaction.

### Verify onchain

You can verify that the authorization was set correctly by looking up your node `StakingPool` object on [Suiscan](https://suiscan.xyz). The object ID is your node ID. Check the `Commission receiver` and `Governance authorized` fields to confirm they match the addresses or objects you configured.

## Collect commission

You can collect your commission using the CLI or the web interface.

### Use the CLI

Make sure that `walrus` is configured to use the authorized wallet and run the following command:

```sh
$ NODE_ID=           # Set this to your node ID.
$ walrus node-admin --node-id $NODE_ID collect-commission
```

### Use the web interface

Go to the [operator panel on the staking app](https://stake-wal.wal.app/node-operations), connect your wallet, and select your node through the drop-down menu or by pasting your node ID. Then select **Collect Commission** and follow the steps to send the transaction.

## Manage contract upgrades

Contract upgrades in Walrus are managed through a quorum-based voting system. This ensures that upgrades are only applied after sufficient consensus among node operators. The process requires computing a digest of the proposed upgrade and voting on it.

When a contract upgrade is proposed, the proposer generally shares the code of the proposed upgrade with other node operators. For example, if the Walrus Foundation proposes an upgrade, it shares a specific commit hash in a branch on the [Walrus GitHub Repository](https://github.com/MystenLabs/walrus). The branch contains the proposed change in either the `testnet-contracts/walrus` or the `mainnet-contracts/walrus` directory, depending on whether Testnet or Mainnet contracts are being upgraded.

The vote needs to complete within 1 epoch. Otherwise, the committee changes and the vote needs to be repeated.

To vote for an upgrade, complete the following steps.

### Compute the upgrade digest

You should compute the package digest of the package to upgrade. You must use the same compiler version and specify the correct Sui network. If you use a standard [Walrus configuration](/docs/getting-started/advanced-setup#configuration), the Sui network is selected automatically when you specify the Walrus network using the `--context` flag. Using the up-to-date `walrus` version ensures that the compiler version is consistent across all voters.

To compute the digest of a proposed upgrade, use the `walrus node-admin` command. Assuming that your current working directory is the root of the Walrus repository and you have checked out the correct commit, use the following command for Testnet upgrades:

```sh
$ walrus node-admin package-digest --package-path testnet-contracts/walrus --context testnet
```

Use the following for Mainnet upgrades:

```sh
$ walrus node-admin package-digest --package-path mainnet-contracts/walrus --context mainnet
```

The command outputs a digest as hexadecimal and base64 that you can use to verify the proposal and vote on the upgrade.

### Vote on upgrades using the web interface

Voting on an upgrade through the web interface is the easiest method and also allows you to use any wallet supported by your browser (for example, hardware wallets).

To vote through the web interface:

1. Go to the [operator panel on the staking app](https://stake-wal.wal.app/node-operations).

2. Connect your wallet and select your node.

3. Navigate to the **Vote on Upgrade** section.

4. Paste the Base64 package digest from the previous step.

5. Follow the prompts to submit your vote.

### Vote on upgrades using the CLI

To vote on an upgrade using the CLI, ensure that your `walrus` binary is configured with the authorized wallet and that you are on the correct branch in the root directory of the Walrus repository.

Run the following for Testnet:

```sh
$ NODE_ID=   # Set this to your node ID.
$ PACKAGE_PATH=testnet-contracts/walrus
$ UPGRADE_MANAGER_OBJECT_ID=0xc768e475fd1527b7739884d7c3a3d1bc09ae422dfdba6b9ae94c1f128297283c
$ walrus node-admin vote-for-upgrade \
    --node-id $NODE_ID \
    --upgrade-manager-object-id $UPGRADE_MANAGER_OBJECT_ID \
    --package-path $PACKAGE_PATH \
    --context testnet
```

Run the following for Mainnet upgrades:

```sh
$ NODE_ID=   # Set this to your node ID.
$ PACKAGE_PATH=mainnet-contracts/walrus
$ UPGRADE_MANAGER_OBJECT_ID=0xc42868ad4861f22bd1bcd886ae1858d5c007458f647a49e502d44da8bbd17b51
$ walrus node-admin vote-for-upgrade \
    --node-id $NODE_ID \
    --upgrade-manager-object-id $UPGRADE_MANAGER_OBJECT_ID \
    --package-path $PACKAGE_PATH \
    --context mainnet
```

### Complete the upgrade

After a quorum of node operators has voted in favor of a proposal, the contract upgrade can be finalized. The proposer usually does this using a programmable transaction block (PTB) that calls the respective functions to authorize, execute, and commit the upgrade. Then, depending on the upgrade, either at the start of the next epoch or immediately, the system and staking objects are migrated to the new version by calling the `migrate` function in the `init` module of the `walrus` package. You can perform the upgrade and migration using the `walrus-deploy` binary.