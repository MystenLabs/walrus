Walrus Sites can be used to deploy almost any form of traditional static Web2 website built for modern browsers. There are, however, a number of restrictions that developers should keep in mind when creating or porting a website to Walrus Sites.

## Static sites only

Walrus Sites store all site resources (HTML, CSS, JS, images, fonts) as blobs on Walrus, with metadata managed by a [Sui smart contract](https://docs.sui.io/guides/developer/packages/package-overview). There is no server-side runtime. As a consequence:

- **No server-side rendering (SSR):** Frameworks that require a Node.js (or equivalent) server to render pages per request (such as Next.js in SSR mode, Nuxt.js, or Remix) cannot run their server component on Walrus Sites. Only the pre-built static output of those frameworks (i.e., exported HTML/CSS/JS) can be deployed.
- **No server-side logic:** There are no endpoints, no request handlers, and no databases. Operations that would normally be handled by a backend (such as form processing, authentication sessions, or database queries) must be implemented using client-side code integrated with external APIs or Sui smart contracts.
- **No runtime redirects:** Redirect plugins that execute at serve time (for example, the Docusaurus redirect plugin that routes `/old-path` to `/new-path` at the server level) do not function as expected. Client-side redirect workarounds must be used instead. See the [routing documentation](https://docs.wal.app/walrus-sites/routing.html) for supported approaches.

## No secret values

All content stored on Walrus Sites is fully public. Site files are stored as blobs on Walrus, and site metadata is stored on Sui. Because there is no private execution environment, developers must not embed secret values anywhere in a site's source files. This includes:

- API keys or tokens
- Private keys or wallet seeds
- Passwords or credentials
- Environment variables intended to be private

Any backend-specific operations that require secrets (such as authentication flows or privileged API calls) must be handled through integration with Sui smart contracts and a [Sui-compatible wallet](https://docs.sui.io/guides/developer/wallets/what-is-a-wallet), keeping secrets out of the deployed site assets entirely.

## Maximum redirect depth

The number of consecutive redirects a Walrus Site can perform is capped by the [portal](/docs/sites/portals/mainnet-testnet). This prevents infinite redirect loops during site loading.

- The public portal at `https://wal.app` enforces a maximum redirect depth of 3.
- Other portals may configure this limit differently. Self-hosted portals can adjust the value as needed.

Redirect chains deeper than the portal's limit will result in the site failing to load.

## Routing constraints in `ws-resources.json`

The [`ws-resources.json` configuration file](/docs/sites/configuration/site-configuration) controls client-side routing, custom headers, and site metadata. It has several notable constraints:

- **Wildcard position is fixed:** Route wildcards (`*`) may only appear at the end of a path pattern. `/path/*` is valid; `/path/*/to` and `*/path/*` are not.
- **Route targets must exist:** Every route value must reference a resource that actually exists among the site's deployed files. The Walrus Sites smart contract will abort if you define a route pointing to a non-existent resource.
- **No server-side redirect support:** The `routes` section implements client-side routing rules only. Framework-level redirect plugins that run server-side at request time will not function as expected.
- **Header scope is per-resource:** Custom HTTP headers defined in the `headers` section apply to individual resource paths, not globally. Each path that needs custom headers must be explicitly configured.

## Service-worker portal limitations

The following restrictions apply only to portals based on service workers (such as a self-hosted instance of the `worker` portal from the [`walrus-sites` repository](https://github.com/MystenLabs/walrus-sites)). If any of these features are required, use a server-side portal instead.

### Service-worker portals cannot serve sites that use service workers

A service-worker portal installs its own service worker in the browser to resolve site metadata from Sui and fetch content from Walrus. Because browsers allow only one service worker per origin, a Walrus Site accessed through a service-worker portal cannot register its own service worker. Attempting to install a site-level service worker from within such a portal results in a broken site experience for the user.

### iOS Sui mobile wallet compatibility

Service workers cannot be loaded inside in-app browsers on iOS due to a limitation of the WebKit engine. As a result:

- Walrus Sites accessed through a service-worker portal cannot be used within Sui-compatible wallet apps on iOS.
- This means iOS wallet users cannot interact with onchain features (wallet connection, transaction signing) from within a service-worker portal.
- Browsing a Walrus Site on iOS through a standard browser (Safari, Chrome, etc.) still works correctly.

**Recommended mitigation:** If using a service-worker portal as your primary access point, redirect iOS wallet users to a server-side portal (for example, `https://wal.app`). This allows the wallet interaction to function correctly via the `<site_name>.wal.app` server-side endpoint.

### Progressive web apps are not supported

Service-worker portals are fundamentally incompatible with Progressive Web Apps (PWA) due to two structural conflicts:

1. The portal's own service worker must be registered for the site to work at all. This prevents the PWA manifest from being loaded correctly by the browser.
2. There can only be one service worker registered per origin. Registering a PWA's service worker would overwrite the portal's service worker, breaking the site entirely.

The server-side portal does not share these limitations. However, because Walrus Sites must be compatible with both portal types and must therefore target the more restrictive service-worker feature set, PWAs cannot currently be deployed to Walrus Sites while maintaining full portal compatibility.

## Domain and naming constraints

- **SuiNS dependency for human-readable URLs:** Without a [SuiNS name](/docs/sites/custom-domains/setting-a-suins-name), a site is only accessible via a Base36-encoded Sui object ID subdomain (e.g., `58gr4pinoayuijgdixud23441t55jd94ugep68fsm72b8mwmq2.wal.app`). This is because subdomains are case-insensitive (ruling out Base64/Base58) and limited to 63 characters (ruling out hex, which requires 64).
- **No traditional DNS control:** Walrus Sites do not support custom DNS records or traditional domain name configuration. Human-readable names are exclusively managed through SuiNS.
- **Subdomain isolation required:** Each site must be accessed through a unique subdomain of the portal domain to ensure security isolation between sites and correct wallet connection behavior in the browser. Shared-origin access is not supported.