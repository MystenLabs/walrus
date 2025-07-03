# Preparing Your Deployment Credentials

To allow the GitHub Action to deploy your Walrus Site, it needs to be able to sign transactions on
your behalf. This requires securely providing it with your private key and the corresponding public
address.

This guide will show you how to:

1. Export a private key from your Sui Wallet or CLI.
2. Correctly format the key and add it as a `SUI_KEYSTORE` secret in your GitHub repository.
3. Add the matching public address as a `SUI_ADDRESS` variable in your GitHub repository.

## Prerequisites

Before you start, you must have the `sui` binary installed. If you haven't installed it yet, please
follow the official [Sui installation
guide](https://docs.sui.io/guides/developer/getting-started/sui-install).

## Exporting Your Private Key

> **Best Practice**: It's recommended to use a dedicated Sui address for each GitHub workflow rather
> than reusing addresses across different projects or purposes. This provides better security
> isolation and helps avoid [gas-coin
> equivocation](https://docs.sui.io/guides/developer/sui-101/avoid-equivocation) issues that can
> occur when multiple workflows try to use the same gas coins concurrently.

---

{{#tabs }}
{{#tab name="From Sui CLI" }}

To export a private key using the command line:

1. Generate a new key by running the following command in your terminal:

   ```sh
   sui keytool generate ed25519 # Or secp256k1 or secp256r1
   ```

2. This command creates a file in your current directory named `<SUI_ADDRESS>.key` (e.g.,
   `0x123...abc.key`). The filename is your new Sui Address.

3. The content of this file is the private key in the `base64WithFlag` format. This is the value
   you need for the `SUI_KEYSTORE` secret.

4. You now have both the address (from the filename) for the `SUI_ADDRESS` variable and the key
   (from the file's content) for the `SUI_KEYSTORE` secret.

> **Note on existing keys**
> If you wish to use a key you already own, you can find it in the `~/.sui/sui_config/sui.keystore`
> file. This file contains a JSON array of all your keys. To find the address for a specific key,
> you would need to use the `sui keytool unpack "<the base64 key from sui.keystore>"` command.

{{#endtab }}
{{#tab name="From Slush Wallet" }}

This method is recommended if you manage your keys through the Slush browser extension.

1.  Open your Slush extension and select the account you want to use for deployments. Make sure to
    copy the corresponding Sui Address, as you will need it later for the `SUI_ADDRESS` variable.
2.  Navigate to the account management screen and select **Export Private Key**.
3.  Copy the provided private key (it will be in bech32 format, starting with `suiprivkey`).
4.  Use the `sui keytool convert <suiprivkey...>` command to transform your key into the required
    Base64 format. Paste your copied key in place of `suiprivkey...`:

    ```sh
    sui keytool convert `suiprivkey...`
    ```

5.  The command will produce an output similar to this:

    ```text
    ╭────────────────┬──────────────────────────────────────────────────────────────────────────╮
    │ bech32WithFlag │  suiprivkey............................................................  │
    │ base64WithFlag │  A...........................................                            │
    │ hexWithoutFlag │  ................................................................        │
    │ scheme         │  ed25519                                                                 │
    ╰────────────────┴──────────────────────────────────────────────────────────────────────────╯
    ```

    Copy the **`base64WithFlag`** value. This is what you will use for the `SUI_KEYSTORE` secret.

{{#endtab }}
{{#endtabs }}

---

## Funding Your Address

Before the GitHub Action can deploy your site, the address you generated needs to be funded with
both SUI tokens (for network gas fees) and WAL tokens (for storing your site's data). The method for
acquiring these tokens differs between Testnet and Mainnet.

---

{{#tabs }}
{{#tab name="Testnet Funding" }}

1.  **Get SUI tokens**: Use the [official Sui faucet](https://faucet.sui.io/) to get free Testnet
    SUI.
2.  **Get WAL tokens**: Exchange your new Testnet SUI for Testnet WAL at a 1:1 rate by running the
    `walrus get-wal` command with the `walrus` CLI.

{{#endtab }}
{{#tab name="Mainnet Funding" }}

For a Mainnet deployment, you will need to acquire both SUI and WAL tokens from an exchange and
transfer them to your deployment address.

{{#endtab }}
{{#endtabs }}

---

## Adding credentials to GitHub

Now, let's add the key and address to your GitHub repository.

1. Navigate to your GitHub repository in a web browser.
2. Click on the **Settings** tab (located in the top navigation bar of your repository).
3. In the left sidebar, click **Secrets and variables**, then select **Actions**.
4. You'll see two tabs: **Secrets** and **Variables**. Start with the **Secrets** tab.
5. Click the **New repository secret** button.
6. Name the secret `SUI_KEYSTORE`.
7. In the **Value** field, paste the `Base64 Key with Flag` you copied earlier. It must be
   formatted as a JSON array containing a single string:

   ```json
   ["AXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"]
   ```

8. Click **Add secret** to save it.

   ```admonish warning
   Make sure to format the keystore as a JSON array with a single string element, not just the raw
   key value. Include the square brackets and quotes exactly as shown above.
   ```

9. Next, switch to the **Variables** tab and click **New repository variable**.
10. Name the variable `SUI_ADDRESS`.
11. In the **Value** field, paste the Sui address that corresponds to your private key (for example:
    `0x123abc...def789`).
12. Click **Add variable** to save it.

```admonish danger title="Security reminder"
Never share your private key or commit it to version control. GitHub secrets are encrypted and only
accessible to your workflows, but always verify you're adding secrets correctly.
```

---

For more information about managing secrets and variables in GitHub Actions, check the official
GitHub documentation:
- [About secrets](https://docs.github.com/en/actions/concepts/security/about-secrets)
- [Using secrets in GitHub Actions](https://docs.github.com/en/actions/security-for-github-actions/security-guides/using-secrets-in-github-actions)
