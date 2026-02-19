:::tip Debug logging

You can enable debug logging for Walrus by setting the environment variable `RUST_LOG=walrus=debug`.

:::

## Use the latest binary

Before undertaking any other steps, make sure you have the [latest `walrus` binary](/docs/getting-started/advanced-setup). If you have multiple versions in different locations, find the binary that is actually used with `which walrus`.

## Check for old hardware or incompatible VMs

Our standard Ubuntu binary is known to cause problems on certain old hardware and in certain virtualized environments. If you experience errors like `Illegal instruction (core dumped)`, [install](/docs/getting-started/advanced-setup) the `ubuntu-x86_64-generic` version instead, which is compiled specifically to be compatible with almost all physical and virtual x86-64 CPUs.

## Verify correct Sui network configuration

If you get an error like `the specified Walrus system object does not exist`, make sure your wallet is set up for the correct Sui network (Mainnet or Testnet as you might require) and you use the latest [configuration](/docs/getting-started).

## Update to latest Walrus configuration

The Walrus Testnet is wiped periodically and requires updating to the latest binary and configuration. If you get an error like `could not retrieve enough confirmations to certify the blob`, you are probably using an outdated configuration pointing to an inactive Walrus system. In this case, update your configuration file with the latest [configuration](/docs/getting-started) and make sure the CLI uses the intended configuration.

:::tip

When setting `RUST_LOG=info`, the `walrus` client binary prints information about the used configuration when starting execution, including the path to the Walrus configuration file and the Sui wallet.

:::