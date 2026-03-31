Walrus Site Portals are used to access and browse a Walrus Site. The portal you run **must** match the network on which your site is deployed. Each network has its own onchain package, default RPC endpoint, and Walrus aggregator. 

The public portal `https://wal.app` is provided by the Walrus Foundation and only serves sites deployed on Mainnet that also have a SuiNS domain name configured. Walrus Foundation does not maintain a portal that serves Testnet Walrus Sites. 

To access sites that are deployed on Mainnet without a SuiNS domain name, or sites deployed to Testnet, you must run your own portal through self-hosting or within a local environment.

## Public portals

A list of public portals is maintained in the [main `walrus` repository](https://github.com/MystenLabs/walrus). Portals can self-identify by opening a PR that adds an entry to `docs/site/static/portals.json`.

## Mainnet

Mainnet is the production network. Sites deployed on Mainnet are publicly accessible.

 Property | Description | Value |
|---|---|---|
| Site package | The onchain address of the Walrus Sites Move package. Changes after package upgrades. | `0x26eb7ee8688da02c5f671679524e379f0b837a12f1d1d799f255b7eea260ad27` |
| Default RPC | The Sui full node RPC endpoint used to read onchain site data. | `https://fullnode.mainnet.sui.io:443` |
| Default aggregator | The Walrus aggregator endpoint used to fetch site blob data. | `https://aggregator.walrus-mainnet.walrus.space` |
| Default landing page (Base36) | The Base36-encoded object ID of the site served at the portal root domain. | `46f3881sp4r55fc6pcao9t93bieeejl4vr4k2uv8u4wwyx1a93` |
| Epoch duration | How long each storage epoch lasts. Determines the minimum storage billing period. | 14 days |
| Public portal | The Walrus Foundation-operated portal. Serves Mainnet sites with SuiNS names only. | Yes, [wal.app](https://wal.app) |

:::info

[wal.app](https://wal.app) only serves Mainnet sites linked with SuiNS names.

:::

## Testnet

Testnet is the development and staging network. Sites deployed on Testnet are only accessible through a Testnet portal. The Walrus Foundation does not operate a public Testnet portal. To access sites deployed on Testnet you can self-host a portal or [run one locally](/docs/sites/portals/deploy-locally).

 Property | Description | Value |
|---|---|---|
| Site package | The onchain address of the Walrus Sites Move package. May change after package upgrades. | `0xf99aee9f21493e1590e7e5a9aea6f343a1f381031a04a732724871fc294be799` |
| Default RPC | The Sui full node RPC endpoint used to read onchain site data. | `https://fullnode.testnet.sui.io:443` |
| Default aggregator | The Walrus aggregator endpoint used to fetch site blob data. | `https://aggregator.walrus-testnet.walrus.space` |
| Default landing page (Base36) | The Base36-encoded object ID of the site served at the portal root domain. | `1p3repujoigwcqrk0w4itsxm7hs7xjl4hwgt3t0szn6evad83q` |
| Epoch duration | How long each storage epoch lasts. Shorter than Mainnet, suited for development and testing. | 1 day |
| Public portal | The Walrus Foundation does not operate a public Testnet portal. | No, self-host or run locally |

## Portal configuration {#configuration}

The `portal-config.yaml` file is the primary configuration file for a Walrus Sites server portal. It sets the network, onchain addresses, URL endpoints, and feature flags the portal uses at runtime.

:::tip

Environment variables override any value set in `portal-config.yaml`. You can use this to apply per-deployment overrides without modifying the file. See [Environment variable overrides](#environment-variable-overrides) for details.

:::

### Setup

Clone the `walrus-sites` repository and check out the stable branch:

```sh
$ git clone https://github.com/MystenLabs/walrus-sites.git
$ cd walrus-sites
$ git checkout mainnet
```

Copy the example file for your target network and rename it to `portal-config.yaml`:

<Tabs>
<TabItem value="mainnet" label="Mainnet">

```sh
$ cp portal/server/portal-config.mainnet.example.yaml portal/server/portal-config.yaml
```

</TabItem>

<TabItem value="testnet" label="Testnet">

```sh
$ cp portal/server/portal-config.testnet.example.yaml portal/server/portal-config.yaml
```

</TabItem>
</Tabs>

### Customize the configuration

Edit the file to adjust values for your deployment using the following parameters.

#### `network`

The Sui network the portal connects to. This value determines which chain the portal reads site objects and SuiNS names from.

**Type:** `string`
**Required:** Yes
**Accepted values:** `mainnet`, `testnet`

```yaml
network: mainnet
```

#### `site_package`

The onchain address of the Walrus Sites Move package. This address differs between Mainnet and Testnet and changes after package upgrades. 

```sh
$ git pull origin mainnet && grep 'site_package' portal/server/portal-config.<network>.example.yaml | awk '{print $2}' | tr -d '"'
```

Check the [`walrus-sites` releases](https://github.com/MystenLabs/walrus-sites/releases) for the current address.

**Type:** `string`  
**Required:** Yes

<Tabs>
<TabItem value="mainnet" label="Mainnet">

```yaml
site_package: "0x26eb7ee8688da02c5f671679524e379f0b837a12f1d1d799f255b7eea260ad27"
```

</TabItem>

<TabItem value="testnet" label="Testnet">

```yaml
site_package: "0xf99aee9f21493e1590e7e5a9aea6f343a1f381031a04a732724871fc294be799"
```

</TabItem>
</Tabs>

#### `landing_page_oid_b36`

The base-36 encoded object ID of the site the portal serves at its root domain. This is the page users see when they navigate to the portal directly, without a site-specific subdomain.

```sh
$ git pull origin mainnet && grep 'landing_page_oid_b36' portal/server/portal-config.<network>.example.yaml | awk '{print $2}' | tr -d '"'
```

**Type:** `string` 
**Required:** Yes

<Tabs>
<TabItem value="mainnet" label="Mainnet default">

```yaml
landing_page_oid_b36: "46f3881sp4r55fc6pcao9t93bieeejl4vr4k2uv8u4wwyx1a93"
```

</TabItem>

<TabItem value="testnet" label="Testnet default">

```yaml
landing_page_oid_b36: "1p3repujoigwcqrk0w4itsxm7hs7xjl4hwgt3t0szn6evad83q"
```

</TabItem>
</Tabs>

#### `domain_name_length`

The number of characters in a valid base-36 subdomain used for direct object ID addressing (for example, `<oid>.walrus.site`). The default value is `21`.

**Type:** `integer` 
**Required:** Yes

```yaml
domain_name_length: 21
```

#### `b36_domain_resolution`

When `true`, the portal resolves base-36 encoded object IDs used as subdomains directly to onchain site objects. Set to `false` to disable this resolution method and rely solely on SuiNS name resolution.

**Type:** `boolean`  
**Required:** Yes  
**Default:** `true`

```yaml
b36_domain_resolution: true
```

#### `bring_your_own_domain`

When `true`, the portal supports resolving custom domains mapped to onchain site objects, in addition to standard subdomain-based addressing.

**Type:** `boolean` 
**Required:** Yes  
**Default:** `false`

```yaml
bring_your_own_domain: false
```

#### `enable_blocklist`

When `true`, the portal rejects requests for sites on the blocklist. Enabling this requires setting the `BLOCKLIST_REDIS_URL` environment variable, as the blocklist is stored in Redis and has no YAML equivalent.

**Type:** `boolean` 
**Required:** Yes  
**Default:** `false`

```yaml
enable_blocklist: false
```

:::info

`BLOCKLIST_REDIS_URL` must be set as an environment variable. There is no `portal-config.yaml` equivalent.

:::

#### `enable_allowlist`

When `true`, the portal serves only sites on the allowlist and requires `premium_rpc_urls` to be configured. Enabling this also requires setting the `ALLOWLIST_REDIS_URL` environment variable.

**Type:** `boolean`  
**Required:** Yes  
**Default:** `false`

```yaml
enable_allowlist: false
```

:::info

`ALLOWLIST_REDIS_URL` must be set as an environment variable. It has no `portal-config.yaml` equivalent.

:::

#### `rpc_urls`

The Sui full node RPC endpoints the portal uses to read onchain data. The portal tries endpoints in ascending `metric` order and retries failed requests up to the number of times specified by `retries`.

Each entry has the following fields:

| Field | Type | Description |
|---|---|---|
| `url` | string | The full node RPC URL. |
| `retries` | integer | The number of times to retry a failed request. Must be 0 or greater. |
| `metric` | integer | The priority value. Lower values have higher priority. |

**Type:** `list` of objects  
**Required:** Yes

```yaml
rpc_urls:
  - url: https://fullnode.mainnet.sui.io
    retries: 2
    metric: 100
```

To configure multiple endpoints with fallback ordering, add additional entries with increasing `metric` values:

```yaml
rpc_urls:
  - url: https://my-primary-rpc.example.com
    retries: 2
    metric: 50
  - url: https://fullnode.mainnet.sui.io
    retries: 2
    metric: 100
```

#### `premium_rpc_urls`

A separate set of Sui full node RPC endpoints used exclusively for allowlisted sites. This field uses the same structure as [`rpc_urls`](#rpc_urls). Omit this field when `enable_allowlist` is `false`.

**Type:** `list` of objects  
**Required:** Only when `enable_allowlist: true`

```yaml
# Uncomment and configure when enable_allowlist is true
# premium_rpc_urls:
#   - url: https://fullnode.mainnet.sui.io
#     retries: 2
#     metric: 100
```

#### `aggregator_urls`

The Walrus aggregator endpoints the portal uses to fetch site blob data. The portal tries endpoints in ascending `metric` order and retries failed requests up to the number of times specified by `retries`.

Each entry uses the same structure as [`rpc_urls`](#rpc_urls).

**Type:** `list` of objects  
**Required:** Yes

<Tabs>
<TabItem value="mainnet" label="Mainnet">

```yaml
aggregator_urls:
  - url: https://aggregator.walrus-mainnet.walrus.space
    retries: 3
    metric: 100
```

</TabItem>

<TabItem value="testnet" label="Testnet">

```yaml
aggregator_urls:
  - url: https://aggregator.walrus-testnet.walrus.space
    retries: 3
    metric: 100
```

</TabItem>
</Tabs>

## Environment variable overrides {#environment-variable-overrides}

The environment variables are set in the `.env.local` file at the root of each portal directory. All fields in `portal-config.yaml` can be overridden at runtime by the corresponding environment variable. When both are set, the environment variable takes precedence.

The following variables have no `portal-config.yaml` equivalent and must always be set as environment variables:

| Variable | Required when |
|---|---|
| `BLOCKLIST_REDIS_URL` | `enable_blocklist: true` |
| `ALLOWLIST_REDIS_URL` | `enable_allowlist: true` |
| `EDGE_CONFIG` | Using Vercel Edge Config |
| `EDGE_CONFIG_ALLOWLIST` | Using Vercel Edge Config allowlist |

## Constants

You can find the `constants.ts` file in the `portal/common/lib` directory. It holds key configuration parameters for the portal. Typically, you won't need to modify these, but if you do, these are the available parameters:

| Constant | Description |
|---|---|
| `MAX_REDIRECT_DEPTH` | The number of [redirects](/docs/sites/linking/redirects) the portal follows before stopping. |
| `SITE_NAMES` | Hard-coded `name: objectID` mappings that override SuiNS names. For development only. Use at your own risk, as it might render some sites with legitimate SuiNS names unusable. |
| `FALLBACK_PORTAL` | Applies to the service-worker portal only. The fallback portal is a server-side portal used when a browser does not support service workers. |