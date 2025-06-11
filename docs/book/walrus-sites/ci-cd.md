# CI/CD

If you need to automate the deployment of your Walrus Sites, you can use a CI/CD pipeline. You can
use the following example how you can set up a pipeline using GitHub Actions.

``` admonish warning
The following example includes a GitHub action that deploys a website on Walrus Sites that depends
on node and pnpm. Depending on your use case, you may need to adjust the workflow steps to fit your
own tools and dependencies.
```

```yaml
name: "Deploy Walrus Site"
on:
  push:
    branches:
      - main

jobs:
  deploy:
    runs-on: ubuntu-latest
    env:
      NO_COLOR: 1
    steps:
      - name: Checkout to the walrus site repo
        uses: actions/checkout@11bd71901bbe5b1630ceea73d27597364c9af683 # pin@v4
      - name: Setup pnpm
        uses: pnpm/action-setup@fe02b34f77f8bc703788d5817da081398fad5dd2 # pin@v4
        with:
          version: 9.9.0
      - name: Setup node
        uses: actions/setup-node@39370e3970a6d050c480ffad4ff0ed4d3fdee5af # pin@v4
        with:
          node-version: 22
          cache: "pnpm"
      - name: Install dependencies
        run: pnpm install --frozen-lockfile
      - name: Build
        run: pnpm build
      - name: Setup Walrus
        uses: "MystenLabs/walrus-docs/.github/actions/set-up-walrus@0b4c86f621ace9e1b8dc4be8ada251a18fee3086" # pin@main
        with:
          SUI_ADDRESS: "${{ vars.SUI_ADDRESS }}"
          SUI_KEYSTORE: "${{ secrets.SUI_KEYSTORE }}"
          WALRUS_CONFIG: "${{ vars.WALRUS_CONFIG }}"
      - name: "Publish the site"
        run: >
          site-builder update ${{ vars.BUILD_DIR }}
          --context=${{ vars.CONTEXT }}
          --epochs ${{ vars.EPOCHS }}
          --check-extend
```

## Environment variables and secrets

For the above workflow will have to set the following environment variables and secrets:

Variables:

- `SUI_ADDRESS`: The address of the Sui wallet you want to use to deploy the site.
- `WALRUS_CONFIG`: The configuration of the Walrus Site you want to deploy.
- `BUILD_DIR`: The directory where the built site is located (e.g. `dist` or `build`).
- `CONTEXT`: Define the context of the site you want to deploy (`mainnet` or `testnet`).
- `EPOCHS`: The number of epochs to deploy the site.

Secrets:

- `SUI_KEYSTORE`: The keystore of the Sui wallet you want to use to deploy the site.

## Additional Examples

You can find more examples of GitHub workflows here:

1. [Workflow for Deploying Walrus Documentation](https://github.com/MystenLabs/walrus/blob/main/.github/workflows/publish-docs.yaml)
1. [Sui Potatoes Deployment Workflow](https://github.com/sui-potatoes/app/blob/main/.github/workflows/walrus.yml)
1. [Community-Driven Walrus Sites Provenance](https://github.com/zktx-io/walrus-sites-provenance)
