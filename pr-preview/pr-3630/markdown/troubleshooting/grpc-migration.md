> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Sui has deprecated JSON-RPC. The following sections cover the failures that deprecation causes across the Walrus CLI, the TypeScript SDK, `site-builder`, and self-hosted full nodes, with the cause and fix for each.

Start here, because it answers most reports in one line: **the fix is almost always an upgrade, not a configuration change.** Current Walrus clients already reach Sui over gRPC, and they do it at the same full node URL your configuration already lists. Walrus has no separate gRPC endpoint setting to point somewhere new.

For the canonical explanation of the transport and what it means for configuration, see [Transport: JSON-RPC deprecation](/docs/network-reference#transport-json-rpc-deprecation).

## Walrus client errors

Errors in this section occur when a Walrus client talks to a Sui full node that no longer serves JSON-RPC.

#### A `walrus` command fails against a full node that stopped serving JSON-RPC

**Cause:** The installed release is old enough to still build a JSON-RPC client. Current releases do not, so they keep working against full nodes that have dropped the protocol.

**Solution:** Upgrade the client. The transport moves to gRPC on its own, and your configuration file stays as it is:

```sh
$ suiup install walrus
$ walrus --version
```

Then rerun the command that failed. If it still fails, confirm the full node you point at serves gRPC, covered in [Self-hosted full nodes](#self-hosted-full-nodes) below.

#### Looking for the setting that selects gRPC

**Cause:** There is no such setting. One URL per network serves both protocols, so `rpc_urls` in the Walrus client configuration keeps the same values it always had.

**Solution:** Leave the configuration alone. Read the current URLs from the [Network Reference](/docs/network-reference#sui-rpc-endpoints), and use the pre-filled configuration file if you want to be sure it matches:

```sh
$ curl --create-dirs https://docs.wal.app/setup/client_config.yaml -o ~/.config/walrus/client_config.yaml
```

#### `rpc.discover` returns `404` against a working full node

**Cause:** The public full nodes serve Sui client and SDK traffic rather than JSON-RPC method discovery. A `404` from `rpc.discover` does not mean the node is down or misconfigured, and normal client calls against the same URL succeed.

**Solution:** Ignore the discovery call and drive the node through `sui client` and the Walrus configuration file instead of calling JSON-RPC methods against it by hand.

## Site builder errors

#### `site-builder` fails against a full node that stopped serving JSON-RPC

**Cause:** The same one as the CLI: an older `site-builder` build still constructs a JSON-RPC client.

**Solution:** Upgrade the tooling. The optional `rpc_url` field in `sites-config.yaml` takes a Sui full node URL, and `site-builder` reaches that node over gRPC, so the deprecation needs no change to the file:

```yaml
contexts:
  mainnet:
    general:
        # rpc_url: https://fullnode.mainnet.sui.io:443
```

For the full configuration file, see the [Site Builder Reference](/docs/sites/getting-started/using-the-site-builder).

## Self-hosted full nodes

#### A custom full node rejects Walrus client traffic

**Cause:** The node does not serve the gRPC API. The public endpoints in the [Network Reference](/docs/network-reference#sui-rpc-endpoints) do, but a self-hosted node has to enable it.

**Solution:** Enable the gRPC API on the node, then point the client at it as before. No Walrus-side configuration changes.

## Do not force the old code paths

`WALRUS_GRPC_MIGRATION_LEVEL` exists as a temporary escape hatch for debugging the migration. If you set it below the client's default, the client falls back to the older JSON-RPC code paths that Sui full nodes are removing, which reintroduces the failures above.

> **Caution**
>
> Do not set `WALRUS_GRPC_MIGRATION_LEVEL` in production. If you set it while debugging, unset it before you deploy.
## See also

- [Transport: JSON-RPC deprecation](/docs/network-reference#transport-json-rpc-deprecation) for the canonical explanation and the full node URLs.
- [Getting Started](/docs/getting-started) for installing a current client.
- [Site Builder Reference](/docs/sites/getting-started/using-the-site-builder) for the `site-builder` configuration file.
- [Troubleshooting Common Errors](/docs/troubleshooting/network-errors) for connectivity and configuration errors unrelated to the transport.