> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

A working deploy and a production site differ in a handful of predictable ways: which portal
serves it, how client-side routes resolve, how caches behave across updates, and how new versions
ship. Work through each section below before you share a production URL.

## Pick the network and portal

The public portal at [wal.app](https://wal.app) serves only Mainnet sites that have a
[SuiNS name](/docs/sites/custom-domains/setting-a-suins-name) configured. It does not serve
Testnet sites, and it does not serve Mainnet sites by object ID alone.

- **Production:** deploy to Mainnet and attach a SuiNS name, or serve the site under
  [your own domain](/docs/sites/custom-domains/bringing-your-own-domain).
- **Testnet or previews:** run a [local portal](/docs/sites/portals/deploy-locally) for
  development, or self-host a Testnet portal when others need to open the site. See
  [Mainnet and Testnet portals](/docs/sites/portals/mainnet-testnet).

## Configure SPA routing

A single-page app serves one `index.html` and routes in the client, so deep links like
`/dashboard/settings` return `404` unless the site falls back to the index resource. Walrus Sites
support this natively with the `routes` section of `ws-resources.json`:

```json
{
  "routes": {
    "/*": "/index.html"
  }
}
```

More specific patterns win over the wildcard, so you can mix real files with client routes. See
[site configuration](/docs/sites/configuration/site-configuration) for the full route syntax and
[redirects](/docs/sites/linking/redirects) for sending old paths to new ones.

## Control caching and pin versions

Portals and any CDN in front of them cache aggressively, so treat every deployed resource as
long-lived:

- **Use content-hashed filenames** for scripts, styles, and other assets (the default in most
  bundlers). A stale cache then serves a consistent old version instead of mixing old HTML with
  new assets.
- **Pin a version by keeping the old site object.** When `ws-resources.json` carries your
  site's object ID, each deploy updates that site in place; when you need a guaranteed rollback
  target or a frozen release, publish the release as its own site object and point your name at
  the object you want to serve.
- **Expect propagation delay after an update.** Caches refresh on their own schedule; verify a
  fresh deploy from a private browser window or with cache-busting query parameters before
  concluding an update failed. See [troubleshooting](/docs/sites/troubleshooting) if an update
  never appears.

## Name the site with SuiNS

The public portal resolves sites at `<name>.wal.app` from the SuiNS name that points at your site
object. Nested subnames, such as `app.myname.wal.app`, do not resolve on the public portal; use a
separate SuiNS name per site, or serve nested structures under
[your own domain](/docs/sites/custom-domains/bringing-your-own-domain) where your DNS controls
the hostname.

## Automate updates from CI

There is no built-in watch mode; the supported path to update-on-push is running
`site-builder deploy` from your CI pipeline on every push to your production branch. The
[GitHub Actions workflow guide](/docs/sites/ci-cd/github-actions-workflow) walks through the
setup, including [preparing deployment credentials](/docs/sites/ci-cd/preparing-deployment-credentials)
so the pipeline signs with a dedicated wallet.

One trap decides whether redeploys update your site or duplicate it: `site-builder` records
your site's object ID in `ws-resources.json` inside the deploy directory, and CI recreates that
directory on every run. Commit `ws-resources.json` to your framework's `public/` directory so
builds carry it into the output, or pass `--object-id` explicitly; otherwise every pipeline run
creates a new site object and pays for it. See
[`ws-resources.json` in pipelines](/docs/sites/ci-cd/other-ci-cd-platforms#understanding-ws-resourcesjson-in-pipelines).

## Keep the toolchain small

One installer manages the whole toolchain: [`suiup`](https://github.com/MystenLabs/suiup)
installs and pins `sui`, `walrus`, and `site-builder` together, replacing separate per-tool
installs. See [Install the Site Builder](/docs/sites/getting-started/installing-the-site-builder).
Day-to-day deploys then come down to one command against your build directory.

## Port a Next.js app

Walrus Sites serve static files, so a Next.js app must build with
[static export](https://nextjs.org/docs/app/building-your-application/deploying/static-exports):

```js title="next.config.js"
const nextConfig = {
  output: "export",
  images: { unoptimized: true },
};
```

`next build` then writes plain HTML and assets to `out/`, which you deploy directly
(`--epochs` is required; place `ws-resources.json` in `public/` so redeploys update the same
site):

```sh
$ site-builder deploy --epochs <NUMBER> ./out
```

Features that need a server at request time, such as API routes, server actions, and on-demand
rendering, do not run on a static host; move that logic to external services or client-side
calls. Client-side navigation works with the SPA routing configuration above.