This page provides solutions to common errors when deploying and browsing Walrus Sites.

## Configuration errors

Errors in this section relate to `site-builder` configuration.

### `site-builder: command not found`

The `site-builder` binary is not in your `$PATH`. Your shell cannot find the binary because it is not in a directory that your shell searches when running commands.

#### Solution

Move the binary to a directory included in your `$PATH`, or add its current location to `$PATH`:

```sh
export PATH=$PATH:/path/to/site-builder-directory
```

Confirm the installation succeeded:

```sh
site-builder --version
```

**Resources**

- [Install the Site Builder](/docs/sites/getting-started/installing-the-site-builder)

---

### `No such file or directory: sites-config.yaml`

The `site-builder` cannot find its configuration file. By default, it looks for `sites-config.yaml` in the following locations, in order: the current directory, `$XDG_HOME/walrus/sites-config.yaml`, and `$HOME/.config/walrus/sites-config.yaml`.

#### Solution

Download a fresh copy of the configuration file for your target network:

```sh
curl https://raw.githubusercontent.com/MystenLabs/walrus-sites/refs/heads/mainnet/sites-config.yaml \
  -o ~/.config/walrus/sites-config.yaml
```

Replace `mainnet` with `testnet` if you are deploying to Testnet. If you store the file in a non-default location, pass its path explicitly using the `--config` flag:

```sh
site-builder --config /path/to/sites-config.yaml deploy --epochs <EPOCHS> <DIRECTORY>
```

**Resources**

- [Install the Site Builder](/docs/sites/getting-started/installing-the-site-builder)

---

### `the specified Walrus system object does not exist; make sure you have the latest configuration and you are using the correct Sui network`

The `site-builder` is pointing to a Walrus system object that does not exist on the current network. This happens when you use an outdated `sites-config.yaml` or when the network has been wiped, which occurs periodically on Testnet.

#### Solution

Download the latest `sites-config.yaml` for your target network (see the solution above). Verify that the `wallet_env` and `walrus_context` fields in `sites-config.yaml` match the network you intend to deploy to.

**Resources**

- [Install the Site Builder](/docs/sites/getting-started/installing-the-site-builder)

---

## Deployment errors

Errors in this section occur when running `site-builder deploy`.

### `could not retrieve enough confirmations to certify the blob`

The `site-builder` cannot gather enough confirmations from Walrus storage nodes to certify the uploaded blob. This typically indicates that your configuration points to an inactive or outdated Walrus system, or that the Testnet is temporarily unavailable.

#### Solution

Download the latest configuration file for your target network and retry. If the problem persists on Testnet, check the [Walrus Discord](https://discord.gg/walrusprotocol) for announcements about network availability.

**Resources**

- [Install the Site Builder](/docs/sites/getting-started/installing-the-site-builder)

---

### `InsufficientCoinBalance` or `GasBudgetTooLow`

Your wallet does not have enough SUI to cover the transaction gas fee, or does not have enough WAL to pay for Walrus storage.

#### Solution

Check your wallet balance:

```sh
sui client balance
```

On Testnet, get SUI from the faucet and exchange it for WAL:

```sh
walrus get-wal
```

On Mainnet, acquire WAL through an exchange or transfer before deploying.

**Resources**

- [Get SUI from Faucet](https://docs.sui.io/guides/developer/getting-started/get-coins)

---

### `Error: The wallet must own the site object to update it`

You are trying to update a Walrus Site using a wallet that does not own the site's Sui object. Only the wallet that owns the site object can run `deploy` in update mode.

#### Solution

Verify ownership by looking up the site's object ID in a Sui network explorer. If you need to move ownership to a different wallet, transfer the site object using the Sui CLI:

```sh
sui client transfer --object-id <SITE_OBJECT_ID> --to <NEW_WALLET_ADDRESS> --gas-budget 10000000
```

**Resources**

- [Sui Object Explorer](https://suiscan.xyz)

---

### Deploy publishes a new site instead of updating the existing one

The `deploy` command publishes a new site when it cannot find an existing object ID. It looks for an object ID in the `--object-id` flag first, then in the `object_id` field of `ws-resources.json`. If neither is present, a new site object is created.

#### Solution

Pass the object ID of your existing site directly:

```sh
site-builder deploy --object-id <OBJECT_ID> --epochs <EPOCHS> <DIRECTORY>
```

After the first deploy, `site-builder` writes the object ID to `ws-resources.json` automatically. For subsequent deploys, verify this file is present in your site root and contains the correct `object_id` value.

**Resources**

- [Site Builder Reference](/docs/sites/getting-started/using-the-site-builder)

---

### `No index.html found in the build directory`

The `site-builder` requires an `index.html` at the root of the build directory. If your build directory does not include one, the deploy fails.

#### Solution

If you are deploying a web app, confirm your build step produces an `index.html` at the root of the output directory. If you are deploying raw files, use the `--list-directory` flag to generate an automatic directory index:

```sh
site-builder deploy --list-directory --epochs <EPOCHS> <DIRECTORY>
```

**Resources**

- [Site Builder Reference](/docs/sites/getting-started/using-the-site-builder)

---

## Browsing errors

Errors in this section occur when accessing a site through a portal.

### Site does not load on `wal.app`

There are two common causes. First, the site may be deployed to Testnet: the `wal.app` portal only serves Mainnet sites. Second, the site may not have a SuiNS name: the `wal.app` portal resolves sites by SuiNS subdomain and does not support Base36 object ID subdomains.

#### Solution

If your site is on Testnet, run a local portal to access it. If your site is on Mainnet but has no SuiNS name, assign one through the [SuiNS app](https://suins.io/) and link it to your site's object ID.

**Resources**

- [Custom Domains](/docs/sites/custom-domains/setting-a-suins-name)
- [Portals](/docs/sites/portals/mainnet-testnet)

---

### SuiNS name does not resolve to the correct site

Your SuiNS name points to a different object ID than your current site, or the record has not propagated yet.

#### Solution

Open the [SuiNS app](https://suins.io/), navigate to your name's settings, and verify the linked object ID matches the output from your most recent `site-builder deploy`. Allow a few minutes for changes to propagate, then hard-refresh your browser (`Ctrl+Shift+R` or `Cmd+Shift+R`).

**Resources**

- [Custom Domains](/docs/sites/custom-domains/setting-a-suins-name)

---

### Pages return a 404 error on direct URL access

The portal fetches the requested URL path as a file from Walrus. For single-page applications that use client-side routing, those paths do not correspond to real files, so the request returns a 404 error.

#### Solution

Add a catch-all routing rule to `ws-resources.json` that maps unmatched paths to `index.html`, then redeploy:

```json
{
  "routes": {
    "/*": "/index.html"
  }
}
```

:::caution
Walrus Sites does not support server-side redirects. Redirect plugins used by frameworks like Docusaurus resolve routes at the server level, which is not available on Walrus Sites. Use the `routes` field in `ws-resources.json` for client-side routing instead.
:::

**Resources**

- [Site Configuration](/docs/sites/configuration/site-configuration)

---

### Site content has disappeared

Walrus stores blobs for a fixed number of epochs. When the storage period expires, the blobs become unavailable and the site stops loading. On Mainnet, each epoch is approximately 14 days. On Testnet, each epoch is approximately 1 day.

#### Solution

Redeploy the site with additional epochs:

```sh
site-builder deploy --epochs <EPOCHS> <DIRECTORY>
```

The maximum storage duration is 53 epochs. To check whether a specific blob is still available:

```sh
walrus blob-status --blob-id <BLOB_ID>
```

**Resources**

- [Site Builder Reference](/docs/sites/getting-started/using-the-site-builder)

---

## Getting help

- Run `site-builder --help` or `site-builder <command> --help` for CLI usage details.
- Check the [Walrus Sites GitHub repository](https://github.com/MystenLabs/walrus-sites) for open issues or to file a new one.
- Ask in the [Walrus Discord](https://discord.gg/walrusprotocol) developer channels.