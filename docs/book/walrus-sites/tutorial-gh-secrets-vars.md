# Preparing Your Deployment Credentials

To allow the GitHub Action to deploy your Walrus Site, it needs to be able to sign transactions on your behalf. This requires securely providing it with your private key and the corresponding public address.

This guide will show you how to:
1.  Export a private key from your Sui Wallet or CLI.
2.  Correctly format the key and add it as a `SUI_KEYSTORE` secret in your GitHub repository.
3.  Add the matching public address as a `SUI_ADDRESS` variable in your GitHub repository.

## Prerequisites

Before you start, you must have the `sui` binary installed. If you haven't installed it yet, please follow the official [Sui installation guide](TODO: Find link)

## Exporting Your Private Key

You can use a dedicated key for your GitHub Actions or an existing one. We will cover two methods to export your private key in the required format.

{{#tabs }}
{{#tab name="From Sui CLI" }}

If you prefer using the command line, we recommend generating a new, dedicated key-pair for your GitHub deployment workflow. This approach is cleaner and more secure.

1.  Generate a new key by running the following command in your terminal:
    ```sh
    sui keytool generate ed25519
    ```

2.  This command creates a file in your current directory named `<SUI_ADDRESS>.key` (e.g., `0x123...abc.key`). The filename is your new Sui Address.

3.  The content of this file is the private key in the `base64WithFlag` format. This is the value you need for the `SUI_KEYSTORE` secret.

4.  You now have both the address (from the filename) for the `SUI_ADDRESS` variable and the key (from the file's content) for the `SUI_KEYSTORE` secret.

> **Note on existing keys**
> If you wish to use a key you already own, you can find it in the `~/.sui/sui_config/sui.keystore` file. This file contains a JSON array of all your keys. To find the address for a specific key, you would need to use the `sui keytool unpack "<the base64 key from sui.keystore>"` command.

{{#endtab }}
{{#tab name="From Slush Wallet" }}

This method is recommended if you manage your keys through the Slush browser extension.

1.  Open your Slush extension and select the account you want to use for deployments. Make sure to copy the corresponding Sui Address, as you will need it later for the `SUI_ADDRESS` variable.
2.  Navigate to the account management screen and select **Export Private Key**.
3.  Copy the provided private key (it will be in bech32 format, starting with `suiprivkey`).
4.  Use the `sui keytool convert <suiprivkey...>` command to transform your key into the required Base64 format. Paste your copied key in place of `suiprivkey...`:

    ```sh
    sui keytool convert `suiprivkey...`
    ```

5.  The command will produce an output similar to this:
    ```text
    ╭────────────────┬──────────────────────────────────────────────────────────────────────────╮
    │ bech32WithFlag │  suiprivkey............................................................  │
    │ base64WithFlag │  A...........................................                            │
    │ hexWithoutFlag │  ................................................................        │
    │ scheme         │  ed25519|secp256k1|secp256r1                                             │
    ╰────────────────┴──────────────────────────────────────────────────────────────────────────╯
    ```
    The `scheme` field in the output will indicate which of the three possible signature schemes (`ed25519`, `secp256k1`, or `secp256r1`) your key uses.
    Copy the **`base64WithFlag`** value. This is what you will use for the `SUI_KEYSTORE` secret.

{{#endtab }}
{{#endtabs }}

## Funding Your Address

Before the GitHub Action can deploy your site, the address you generated needs to be funded with both SUI tokens (for network gas fees) and WAL tokens (for storing your site's data). The method for acquiring these tokens differs between Testnet and Mainnet.

### Testnet Funding
1.  **Get SUI tokens**: Use the [official Sui faucet](https://faucet.sui.io/) to get free Testnet SUI.
2.  **Get WAL tokens**: Exchange your new Testnet SUI for Testnet WAL at a 1:1 rate by running the `walrus get-wal` command with the `walrus` CLI.

### Mainnet Funding
For a Mainnet deployment, you will need to acquire both SUI and WAL tokens from an exchange and transfer them to your deployment address.

## Adding credentials to GitHub

Now, let's add the key and address to your GitHub repository.

1.  In your GitHub repository, go to **Settings** > **Secrets and variables** > **Actions**.
2.  Switch to the **Secrets** tab and click **New repository secret**.
3.  Name the secret `SUI_KEYSTORE`.
4.  In the **Value** field, paste the `Base64 Key with Flag` you copied earlier. It must be formatted as a JSON array containing a single string. For example: `["AXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"]`
    ![TODO: Adding SUI_KEYSTORE secret](assets/sui-keystore-secret.png)

5.  Next, switch to the **Variables** tab and click **New repository variable**.
6.  Name the variable `SUI_ADDRESS`.
7.  In the **Value** field, paste the Sui address that corresponds to your private key.
    ![TODO: Adding SUI_ADDRESS variable](assets/sui-address-variable.png)

---

For more information about managing secrets and variables in GitHub Actions, check the official GitHub documentation:
- [About secrets](https://docs.github.com/en/actions/concepts/security/about-secrets)
- [Using secrets in GitHub Actions](https://docs.github.com/en/actions/security-for-github-actions/security-guides/using-secrets-in-github-actions)
