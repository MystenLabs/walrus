> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

The official [Deploy Walrus Site action](https://github.com/MystenLabs/walrus-sites-github-actions) targets GitHub Actions only. On any other CI/CD platform, such as GitLab CI/CD, CircleCI, or Bitbucket Pipelines, you drive deployment directly through the [`site-builder` CLI](/docs/sites/getting-started/using-the-site-builder). Because `site-builder` is a standard Linux binary, the deployment steps are identical on every platform. Only the pipeline syntax differs.

- [x] A Sui address funded with SUI (gas fees) and WAL (storage). See [Preparing Deployment Credentials](/docs/sites/ci-cd/preparing-deployment-credentials).
- [x] The private key in `base64WithFlag` format and the corresponding Sui address. See [Exporting your private key](/docs/sites/ci-cd/preparing-deployment-credentials#exporting-your-private-key).
- [x] A CI/CD platform with access to a Linux runner.

## How platform-based deployment works

Because there is no pre-built action for non-GitHub platforms, each pipeline must perform the same steps that the GitHub Action handles automatically:

1. **Reconstruct the Sui configuration:** Write the keystore secret from the CI environment to disk so `site-builder` can read it.
2. **Download `site-builder`:** Fetch the binary for the runner's architecture from the [Mysten Labs GCS bucket](/docs/sites/getting-started/installing-the-site-builder#using-a-pre-built-binary).
3. **Download `sites-config.yaml`:** Fetch the network configuration from the [walrus-sites repository](https://github.com/MystenLabs/walrus-sites).
4. **Build the site:** Compile or bundle your site output (skip for repositories that already contain deployable static files).
5. **Run `site-builder deploy`:** Upload the static output directory to Walrus.

## Storing credentials on your platform

Every platform stores secrets differently. In all cases, you need 2 values:

- `SUI_KEYSTORE`: The `base64WithFlag` private key string wrapped as a JSON array: `["AXXXXXXXXXX..."]`. Store this as a masked or protected secret. Never expose it in logs.
- `SUI_ADDRESS`: The corresponding Sui address. This is not sensitive, but masking it is acceptable.

| Platform | Secret location |
|---|---|
| GitLab | Settings → CI/CD → Variables (set visibility to **Masked**) |
| CircleCI | Project Settings → Environment Variables, or a shared **Context** |
| Bitbucket | Repository settings → Repository variables (enable **Secured**) |

> **Danger**
>
> Never paste `SUI_KEYSTORE` or any private key directly into a pipeline configuration file. Configuration files are committed to version control and visible to anyone with repository access.
## The deploy script

The following shell commands contain all setup and deployment logic. Each platform example below inlines these steps in its own syntax.

```sh
#!/usr/bin/env bash
set -euo pipefail

# 1. Reconstruct Sui client configuration from CI secrets.
#    SUI_KEYSTORE must be a JSON array string, for example: ["AXXXXX..."]
#    Replace 'mainnet' with 'testnet' throughout if targeting Testnet.
WALRUS_NETWORK="${WALRUS_NETWORK:-mainnet}"

mkdir -p ~/.sui/sui_config

echo "$SUI_KEYSTORE" > ~/.sui/sui_config/sui.keystore

cat > ~/.sui/sui_config/client.yaml <<EOF
keystore:
  File: "$HOME/.sui/sui_config/sui.keystore"
envs:
  - alias: ${WALRUS_NETWORK}
    rpc: "https://fullnode.${WALRUS_NETWORK}.sui.io:443"
    ws: ~
    basic_auth: ~
active_env: ${WALRUS_NETWORK}
active_address: "${SUI_ADDRESS}"
EOF

# 2. Download site-builder for ubuntu-x86_64, which matches standard Linux CI runners.
curl -fsSL \
  "https://storage.googleapis.com/mysten-walrus-binaries/site-builder-${WALRUS_NETWORK}-latest-ubuntu-x86_64" \
  -o /usr/local/bin/site-builder
chmod +x /usr/local/bin/site-builder

# 3. Download the network-specific sites-config.yaml.
mkdir -p ~/.config/walrus
curl -fsSL \
  "https://raw.githubusercontent.com/MystenLabs/walrus-sites/refs/heads/${WALRUS_NETWORK}/sites-config.yaml" \
  -o ~/.config/walrus/sites-config.yaml

# 4. Deploy. DIST must point to your built static files directory, not the repository root.
site-builder deploy --epochs "${EPOCHS:-5}" "${DIST:?DIST must be set}"
```

Set the following environment variables before running the script:

- `SUI_KEYSTORE`: The keystore secret from your platform's secret store.
- `SUI_ADDRESS`: The Sui address that corresponds to your keystore.
- `WALRUS_NETWORK`: `mainnet` (default) or `testnet`.
- `EPOCHS`: Number of storage epochs (default `5`; maximum `53`). On Mainnet, 1 epoch is 14 days. On Testnet, 1 epoch is 1 day.
- `DIST`: Path to your build output directory, for example `dist` or `build`.

> **Caution**
>
> Always point `site-builder deploy` at your build output directory, not your repository root. Uploading the project root includes source files, `node_modules/`, and other artifacts that significantly increase upload time and storage costs.
## Understanding `ws-resources.json` in pipelines

The `site-builder deploy` command writes a [`ws-resources.json`](/docs/sites/configuration/site-configuration) file to your `DIST` directory after the first deployment. This file stores the Sui object ID of your deployed site. On subsequent runs, `site-builder` reads that ID to update the same site rather than creating a new one.

In a CI/CD pipeline, the build output directory is recreated on every run, so `ws-resources.json` is lost unless you take one of the following approaches:

- **Commit the file to your repository** (recommended): Place `ws-resources.json` in your framework's `public/` directory (or equivalent). Most build tools copy everything in `public/` into `DIST` automatically, making the file available at `DIST/ws-resources.json` on every run without extra configuration.
- **Pass the object ID explicitly:** After the first deployment, copy the `object_id` value from `ws-resources.json` and store it as a CI variable named `SITE_OBJECT_ID`. Then add `--object-id "$SITE_OBJECT_ID"` to the deploy command: `site-builder deploy --object-id "$SITE_OBJECT_ID" --epochs "$EPOCHS" "$DIST"`.

If neither approach is in place, each pipeline run creates a new site object and incurs additional WAL storage costs.

## Platform examples
 
Store your credentials before adding the pipeline file. Then create the configuration file shown for your platform in your repository root.
 

Store `SUI_KEYSTORE` and `SUI_ADDRESS` as masked CI/CD variables under **Settings** → **CI/CD** → **Variables**.
 
Create `.gitlab-ci.yml`:
 
```yaml
stages:
  - build
  - deploy
 
variables:
  WALRUS_NETWORK: mainnet
  EPOCHS: "5"
  DIST: dist
 
build:
  stage: build
  image: node:lts
  script:
    - npm ci
    - npm run build
  artifacts:
    paths:
      - dist/
    expire_in: 1 hour
  rules:
    - if: $CI_COMMIT_BRANCH == $CI_DEFAULT_BRANCH
 
deploy-walrus:
  stage: deploy
  image: ubuntu:24.04
  needs:
    - build
  before_script:
    - apt-get update -qq && apt-get install -y -qq curl ca-certificates
  script:
    - mkdir -p ~/.sui/sui_config
    - echo "$SUI_KEYSTORE" > ~/.sui/sui_config/sui.keystore
    - |
      cat > ~/.sui/sui_config/client.yaml <<EOF
      keystore:
        File: "$HOME/.sui/sui_config/sui.keystore"
      envs:
        - alias: ${WALRUS_NETWORK}
          rpc: "https://fullnode.${WALRUS_NETWORK}.sui.io:443"
          ws: ~
          basic_auth: ~
      active_env: ${WALRUS_NETWORK}
      active_address: "${SUI_ADDRESS}"
      EOF
    - |
      curl -fsSL \
        "https://storage.googleapis.com/mysten-walrus-binaries/site-builder-${WALRUS_NETWORK}-latest-ubuntu-x86_64" \
        -o /usr/local/bin/site-builder
      chmod +x /usr/local/bin/site-builder
    - |
      mkdir -p ~/.config/walrus
      curl -fsSL \
        "https://raw.githubusercontent.com/MystenLabs/walrus-sites/refs/heads/${WALRUS_NETWORK}/sites-config.yaml" \
        -o ~/.config/walrus/sites-config.yaml
    - site-builder deploy --epochs "$EPOCHS" "$DIST"
  rules:
    - if: $CI_COMMIT_BRANCH == $CI_DEFAULT_BRANCH
```
 
The `build` job produces a `dist/` artifact that GitLab makes available to the `deploy-walrus` job through the `needs` dependency. The `rules` key restricts both jobs to the default branch, preventing deployments from feature branches.
 
For static sites with no build step, remove the `build` stage, remove the `needs` key from `deploy-walrus`, and point `DIST` at your static files directory.

 
## Adapting to any other platform
 
Any CI/CD platform that provides Linux runners can deploy Walrus Sites using the same pattern. The following table maps the concepts from the 3 examples above to their equivalents on other platforms:
 
| Concept | GitLab | CircleCI | Bitbucket | General approach |
|---|---|---|---|---|
| Secret storage | CI/CD Variables (Masked) | Environment Variables or Context | Repository variables (Secured) | Platform secret store |
| File sharing between jobs | `artifacts` + `needs` | `persist_to_workspace` + `attach_workspace` | `artifacts` (automatic) | Platform artifact or cache mechanism |
| Branch restriction | `rules` on `$CI_DEFAULT_BRANCH` | `filters` on branch name | `branches` key | Push trigger scoped to main branch |
| Runner image | `image:` per job | `docker:` executor per job | `image:` per step | Any Ubuntu-based Linux image |
 
The deploy script itself does not change between platforms. Copy the 5-step script from [The deploy script](#the-deploy-script) section and inline it into your platform's job definition using that platform's multiline script syntax.