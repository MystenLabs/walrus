Create a GitHub Actions workflow to automatically deploy your Walrus Site whenever you push to your repository. The workflow uses the official [Deploy Walrus Site action](https://github.com/MystenLabs/walrus-sites-github-actions) maintained by Mysten Labs.

<Tabs>
<TabItem value="prereq" label="Prerequisites">

- [x] A GitHub repository containing your site's source files.
- [x] `SUI_KEYSTORE` secret and `SUI_ADDRESS` variable added to your repository. See [Preparing Deployment Credentials](/docs/sites/ci-cd/preparing-deployment-credentials).

</TabItem>
</Tabs>

## How the action works

The Deploy Walrus Site action (`MystenLabs/walrus-sites-github-actions/deploy`) takes a directory of static files and uploads them to Walrus using [`site-builder`](/docs/sites/getting-started/using-the-site-builder). **It does not build your site**. You are responsible for producing the deployable output before calling the action.

The action performs the following steps:

1. Downloads the `site-builder` binary for the selected version.
2. Writes the Sui keystore to the runner environment.
3. Runs [`site-builder deploy`](/docs/sites/getting-started/publishing-your-first-site) against your `DIST` directory.
4. If `GITHUB_TOKEN` is provided and [`ws-resources.json`](/docs/sites/configuration/site-configuration) changed, opens a pull request with the updated file.

### Required inputs

- `DIST`: Path to the directory containing your built static files. This directory must exist before the action runs.

### Authentication inputs

Only choose 1 of the following authentication methods. Providing more than 1 causes the action to fail.

- **Method 1 (recommended for CI/CD):** `SUI_ADDRESS` and `SUI_KEYSTORE`. Both must be present. `SUI_ADDRESS` is read from a repository variable; `SUI_KEYSTORE` is read from an encrypted secret.
- **Method 2:** `SUI_BECH32_PRIVATE_KEY`. Pass the bech32-encoded private key (starting with `suiprivkey`) directly as a secret. No address input is needed.
- **Method 3:** `SUI_SECRET_PHRASE`. Pass the mnemonic recovery phrase as a secret. Use this method only when no other credential format is available.

### Optional inputs

| Input | Description | Default |
|---|---|---|
| `SUI_NETWORK` | Target network: `mainnet` or `testnet` | `mainnet` |
| `EPOCHS` | Number of epochs to keep the site stored | `5` |
| `WALRUS_CONFIG` | Custom Walrus configuration file content | Downloaded from Walrus repo |
| `SITES_CONFIG` | Custom `sites-config.yaml` content | Downloaded from walrus-sites repo |
| `WS_RESOURCES` | Full path to the `ws-resources.json` file | `DIST/ws-resources.json` |
| `GITHUB_TOKEN` | Enables automatic pull request creation when resources change | (none) |
| `SITE_BUILDER_VERSION` | Site-builder version: `v1` or `v2` | `v2` |

## Understanding `ws-resources.json`

The `site-builder` tool uses a file called `ws-resources.json` to track the Sui object ID of your deployed site. This file determines whether a deployment creates a new site or updates an existing one.

**First deployment (no `ws-resources.json`):** The action creates a new Walrus Site object on Sui and writes `ws-resources.json` to your `DIST` directory. If `GITHUB_TOKEN` is provided with the correct permissions, the action opens a pull request containing this new file.

**Subsequent deployments (existing `ws-resources.json`):** The action reads the `object_id` from the file and updates the same site in place. Only changed resources are re-uploaded.

:::caution

Merge the pull request from your first deployment before the next workflow run. If `ws-resources.json` is not committed, each run creates a new site object instead of updating the existing one.

:::

To place the file in your repository so that it survives across builds, add it under your framework's `public/` directory (or equivalent). Most build tools copy everything in `public/` directly into the output directory, which makes the file available at `DIST/ws-resources.json` without any extra configuration.

Once your site is deployed and you have its `object_id`, you can link it to a SuiNS name so that it is accessible at `<suins>.wal.app`. See [Setting a SuiNS Name](/docs/sites/custom-domains/setting-a-suins-name) for details.

## Using `GITHUB_TOKEN` for pull request creation

Providing `GITHUB_TOKEN` enables the action to open a pull request automatically when `ws-resources.json` changes. This is most useful for the initial deployment, when the file is first created.

To enable this feature, add the `GITHUB_TOKEN` input and grant the workflow the required permissions:

```yaml
permissions:
  contents: write
  pull-requests: write
```

Without `GITHUB_TOKEN`, the action still deploys your site but does not open pull requests.

## Workflow structure

A Walrus Sites workflow follows this general structure:

1. Trigger definition (push, pull request, manual dispatch, and so on).
2. Permissions block (required if using `GITHUB_TOKEN`).
3. Checkout step.
4. Build steps (required for frameworks; skip for pre-built static sites).
5. Deploy Walrus Site action.

Create a file at `.github/workflows/deploy-walrus-site.yml` in your repository and adapt one of the examples below.

## Example workflows

### Static site (no build step)

Use this pattern when your repository already contains deployable static files with no compilation required:

```yaml
name: Deploy to Walrus

on:
  push:
    branches: [main]
  workflow_dispatch:

permissions:
  contents: write
  pull-requests: write

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Deploy to Walrus
        uses: MystenLabs/walrus-sites-github-actions/deploy@v1
        with:
          SUI_ADDRESS: ${{ vars.SUI_ADDRESS }}
          SUI_KEYSTORE: ${{ secrets.SUI_KEYSTORE }}
          DIST: ./site
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
          EPOCHS: '5'
```

Replace `./site` with the path to your static files directory.

### Framework site with build step (frameworks like Vite, React, Vue, Svelte, and others)

Use this pattern when your site requires a compilation step before deployment. The example below uses a Vite + React + TypeScript setup but applies to any Node.js-based framework:

```yaml
name: Deploy to Walrus

on:
  push:
    branches: [main]
  workflow_dispatch:

permissions:
  contents: write
  pull-requests: write

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Node.js
        uses: actions/setup-node@v4
        with:
          node-version: 'lts/*'
          cache: 'npm'

      - name: Install dependencies
        run: npm ci

      - name: Build site
        run: npm run build

      - name: Deploy to Walrus
        uses: MystenLabs/walrus-sites-github-actions/deploy@v1
        with:
          SUI_ADDRESS: ${{ vars.SUI_ADDRESS }}
          SUI_KEYSTORE: ${{ secrets.SUI_KEYSTORE }}
          DIST: ./dist
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
          EPOCHS: '5'
```

Adjust the `node-version`, package manager commands, and `DIST` path to match your project.

### Testnet deployment

To target Testnet instead of Mainnet, set `SUI_NETWORK` to `testnet`. Make sure the `SUI_ADDRESS` and `SUI_KEYSTORE` values correspond to a Testnet-funded address:

```yaml
      - name: Deploy to Walrus (Testnet)
        uses: MystenLabs/walrus-sites-github-actions/deploy@v1
        with:
          SUI_ADDRESS: ${{ vars.SUI_ADDRESS }}
          SUI_KEYSTORE: ${{ secrets.SUI_KEYSTORE }}
          DIST: ./dist
          SUI_NETWORK: testnet
          EPOCHS: '5'
```

## Trigger strategies

GitHub Actions supports several trigger types. Choose the one that fits your release process:

- **Push to main:** Deploy on every merge. Suitable for continuous deployment.
- `workflow_dispatch`: Trigger manually from the Actions tab. Useful for controlled or infrequent releases.
- **Tag push:** Deploy only on tagged releases by adding `tags: ['v*']` under `push`. Suitable for versioned sites.
- **Pull request (dry run):** Combine with a separate job that builds and validates the site without deploying, so you can catch build errors before merging.

Most projects use `push` on `main` combined with `workflow_dispatch` so that both automatic and manual deployments are available.