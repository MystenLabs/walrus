# Deploying your site via Github Action

Automating the deployment of your Walrus Site can be easily achieved using GitHub Actions. This guide will walk you through the necessary steps to set up a workflow that automatically publishes your site.

The process involves two main parts:

1.  **Setting up secrets and variables in your GitHub repository:** To allow the GitHub Action to interact with the Sui network on your behalf, you need to securely store your private key. We'll show you how to export your private key from both the Sui CLI and the Sui wallet extension, convert it to base64 format, and save it as a GitHub secret named `SUI_KEYSTORE`. We will also configure a GitHub variable `SUI_ADDRESS` with the public address corresponding to your key.

2.  **Creating a GitHub workflow file:** We'll provide you with example workflow files that you can add to your repository. These examples will demonstrate how to use our dedicated GitHub Action to build and deploy your Walrus Site on demand.

