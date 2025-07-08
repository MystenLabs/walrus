# Writing your workflow

Now that you have configured your secrets and variables, you can create GitHub workflows to automate
your Walrus Site deployments using the official "Deploy Walrus Site" GitHub Action.

## Key Requirement: Pre-built Site Directory

The Deploy Walrus Site action operates on an **already built site directory**. The action does not
build your site - it deploys existing static files to Walrus.

This means:

- If your site consists of ready-to-deploy static files (HTML, CSS, JS), you can use the action
  directly
- If your site requires a build step (React, Vue, Svelte, etc.), you **must** include build steps in
  your workflow before calling the deploy action

## Using the Deploy Walrus Site GitHub Action

The action (`MystenLabs/walrus-sites/.github/actions/deploy`) requires these inputs:

- **`SUI_ADDRESS`**: Your Sui address (GitHub variable)
- **`SUI_KEYSTORE`**: Your private key in base64 format (GitHub secret)  
- **`DIST`**: Path to your **built** site directory

Optional inputs include:

- **`SUI_NETWORK`**: Target network (`mainnet` or `testnet`, defaults to `mainnet`)
- **`EPOCHS`**: Number of epochs to keep the site stored (defaults to `5`)
- **`WALRUS_CONFIG`**: Custom Walrus configuration (downloads default if not provided)
- **`SITES_CONFIG`**: Custom sites configuration (downloads default if not provided)
- **`WS_RESOURCES`**: Full path to the `ws-resources.json` file (defaults to
  `DIST/ws-resources.json`)
- **`GITHUB_TOKEN`**: Enables automatic pull request creation when site resources change

### About `GITHUB_TOKEN`

The `GITHUB_TOKEN` input is particularly useful for tracking changes to your site's resources. When
you deploy a site, site-builder creates or updates a `ws-resources.json` file that tracks the site's
data. If this file changes during deployment, the action can automatically create a pull request
with the updated file.

To use this feature:

1. Set `GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}` in your workflow
1. Add these [permissions](https://docs.github.com/en/actions/writing-workflows/choosing-what-your-workflow-does/controlling-permissions-for-github_token)
   to your workflow:

   ```yaml
   permissions:
     contents: write
     pull-requests: write
   ```

```admonish note
If you don't provide the `GITHUB_TOKEN`, the action will still deploy your site successfully but
won't create pull requests for resource file changes.
```

### First Run vs Subsequent Runs

Understanding how the workflow behaves differently on the first run is important:

**First run (new site):**

- Creates a new Walrus Site on Sui
- Generates a `ws-resources.json` file in your `DIST` directory (or updates the file specified by
  `WS_RESOURCES`)
- This file contains the `object_id` of your newly created site
- If `GITHUB_TOKEN` is provided with correct permissions, creates a pull request with these changes

**Subsequent runs (updates):**

- Uses the existing `object_id` from `ws-resources.json` to update the same site
- Only creates PRs if the resource file changes during deployment

```admonish tip
The `ws-resources.json` file is crucial - it tells the action which site to update. Make sure to
merge the PR from the first run so future deployments update the same site instead of creating new
ones.
```

```admonish tip
Once your site is deployed and you have the `object_id`, you can link it with a SuiNS name to make
your site accessible at `<suins>.wal.app`. See [Set a SuiNS name](./tutorial-suins.md) for details
on how to set this up.
```

## Creating Your Workflow

1. Create `.github/workflows/deploy-site.yml` in your repository
1. Add checkout step to get your repository files
1. If your site needs building, add build steps:
   - Add build environment setup (Node.js, etc.)
   - Add build commands
1. Set your `DIST` path:
   - For sites requiring build: Point to your build output directory (e.g., `dist/`, `build/`)
   - For static sites: Point directly to your static files directory
1. Add the Deploy Walrus Site action with your configured secrets and variables

The key is ensuring your `DIST` path points to a directory containing the final, deployable static
files that should be published to Walrus.

## Example Workflows

- [deploy-snake.yml](https://github.com/MystenLabs/walrus-sites/blob/main/.github/workflows/deploy-snake.yml)
  \- Simple static site deployment
- [deploy-vite-react-ts.yml](
  https://github.com/MystenLabs/walrus-sites/blob/main/.github/workflows/deploy-vite-react-ts.yml)
  \- React application with build step

---

For more information about creating and configuring GitHub workflows, check the [official GitHub
Actions documentation](https://docs.github.com/en/actions/writing-workflows).
