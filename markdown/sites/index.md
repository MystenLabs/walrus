> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Walrus Sites are decentralized websites built on [Sui](https://docs.sui.io/) and Walrus. Your site's files are stored on Walrus, a decentralized storage network, while a Sui smart contract records ownership and maps each resource path to its content. Anyone can publish a Walrus Site using the [`site-builder` CLI tool](/docs/sites/getting-started/installing-the-site-builder) and browse it through a [portal](/docs/sites/portals/deploy-locally).

Because there is no server behind a Walrus Site, no single host can take it offline, and no one but the wallet that owns the Sui site object can change it. The same absence of a server sets the limits: a Walrus Site serves static files only, so backends, databases, and server-side rendering are out of scope. Any framework that produces a static build works once you point `site-builder` at its output directory, and client-side routing works through the `routes` field in `ws-resources.json`.

## Where to start

If you want to deploy something now, [install the site-builder](/docs/sites/getting-started/installing-the-site-builder) and follow [Publishing Your First Site](/docs/sites/getting-started/publishing-your-first-site). The two together take you from an empty machine to a live site.

If you want to understand the system first, [Technical Overview](/docs/sites/introduction/technical-overview) traces a single page load from the browser to Walrus, and [Walrus Sites Components](/docs/sites/introduction/components) describes each moving part on its own.

If you already have a site running, [Setting a SuiNS Name](/docs/sites/custom-domains/setting-a-suins-name) replaces the Base36 subdomain with a readable name, [Site Configuration](/docs/sites/configuration/site-configuration) covers headers, routes, and metadata, and [Creating a GitHub Actions Workflow](/docs/sites/ci-cd/github-actions-workflow) redeploys the site on every push.

Before you commit to Walrus Sites for production, read [Known Restrictions](/docs/sites/known-restrictions) so the constraints are not a surprise later.

- [Known Restrictions](/docs/sites/known-restrictions): A comprehensive reference of known restrictions and limitations when developing and deploying Walrus Sites.
- [Production Checklist](/docs/sites/production): Take a Walrus Site from a local deploy to production, covering portal and network choice, SPA routing, caching and version pinning, SuiNS naming, and automated updates from CI.
- [Troubleshooting Walrus Sites](/docs/sites/troubleshooting): Solutions to common errors when deploying and browsing Walrus Sites.