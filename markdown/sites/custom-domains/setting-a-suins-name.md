[Sui Name Service (SuiNS)](https://suins.io/) provides human-readable names for Walrus Sites, similar to how DNS works for websites. Instead of sharing a long Base36 subdomain like `1lupgq2auevjruy7hs9z7tskqwjp5cc8c5ebhci4v57qyl4piy.wal.app`, you can link a SuiNS name to your site's object ID and browse it at a readable address like `my-project.wal.app`.

:::info

Base36 subdomains are not supported on the `wal.app` portal. You must use a SuiNS name to browse Walrus Sites through `wal.app`. Base36 subdomains still work on [local servers and alternative portals](/docs/sites/portals/deploy-locally).

:::

<Tabs>
<TabItem value="prereq" label="Prerequisites">
- [x] A [deployed Walrus Site](/docs/sites/getting-started/publishing-your-first-site) with a known object ID
- [x] A [Sui-compatible wallet](https://docs.sui.io/guides/developer/wallets/what-is-a-wallet) funded with SUI to pay for the SuiNS name registration and transaction fees
- [x] A wallet connected to [suins.io](https://suins.io) (Mainnet) or [testnet.suins.io](https://testnet.suins.io) (Testnet) to register and manage names
</TabItem>
</Tabs>

## SuiNS names

A [SuiNS name](https://docs.suins.io/) is an on-chain name registered on the Sui blockchain. When you link a SuiNS name to a Walrus Site, you set the Walrus Site object ID on the SuiNS name record. The `wal.app` portal reads that record and routes visitors to your site's content.

SuiNS names follow these rules:

- **Allowed characters:** Letters `a-z` and numbers `0-9` only.
- **No special characters:** Hyphens, underscores, and other symbols are not supported.
- **Immutable on registration:** Once a name is registered, the name itself cannot be changed, only the target it points to.
- **Maximum length for `wal.app`:** The `wal.app` portal resolves SuiNS names up to 21 characters long. Names longer than 21 characters are valid SuiNS names but do not resolve on `wal.app`.

Choose a name before you start. Because names are registered on-chain, each registration costs SUI. Verify the name is available and matches your intended use before completing the purchase.

## Register a SuiNS name

To register a SuiNS name, follow these steps:

##### Step 1: Open the SuiNS registration portal

Navigate to the registration portal for your target network:

- **Mainnet:** [suins.io](https://suins.io)
- **Testnet:** [testnet.suins.io](https://testnet.suins.io)

##### Step 2: Connect your wallet

Click **Connect Wallet** in the top-right corner of the page and select your wallet. Make sure the wallet is set to the correct network (Mainnet or Testnet) before connecting.

##### Step 3: Search for a name

Enter your preferred name in the search bar. The portal shows whether the name is available or already registered.

##### Step 4: Purchase the name

Select the name from the search results and follow the prompts to complete the purchase. The portal shows the registration cost in SUI before you confirm. The cost depends on the length of the name:

- **3 characters:** 500 SUI per year
- **4 characters:** 100 SUI per year
- **5 or more characters:** 20 SUI per year

Review the total cost, then approve the transaction in your wallet to finalize the registration.

After the transaction confirms, the name appears under **Names You Own** in your account.

## Find your Walrus Site object ID

If you deployed your site using the [`site-builder`](/docs/sites/getting-started/using-the-site-builder) CLI, the object ID is printed in the deployment output. Look for a line similar to the following:
 
```
New site object ID: 0x617221edd060dafb4070b73160ebf535e1516bf7f246890ed35190eba786d7ac
```
 
You can also find the object ID by searching your wallet address in [Sui Explorer](https://suivision.xyz) and locating the Walrus Site object in your owned objects.
 
:::tip

Copy and save the object ID before you start the linking process. You need to paste it exactly as it appears, without extra spaces or characters.

:::

## Link your SuiNS name to your Walrus Site

After you have both a SuiNS name and a Walrus Site object ID, link them together on the SuiNS portal.

##### Step 1: Open your account

Go to the **Names You Own** section:

- **Mainnet:** [suins.io/account/my-names](https://suins.io/account/my-names)
- **Testnet:** [testnet.suins.io/account/my-names](https://testnet.suins.io/account/my-names)

Connect your wallet if prompted.

##### Step 2: Open the name menu

Find the SuiNS name you want to link. Click the **3-dot menu** icon above the name to open its action menu.

##### Step 3: Select the Walrus Site option

Click **Link To Walrus Site** from the menu. A dialog opens with a text field for the site object ID.

##### Step 4: Paste the object ID

Paste your Walrus Site object ID into the text field. Verify that the ID matches exactly what was shown in your deployment output or Sui Explorer. An incorrect object ID points visitors to the wrong site or returns an error.

##### Step 5: Apply the link

Click **Apply**. Your wallet prompts you to approve the transaction. Review and confirm the transaction to write the link on-chain.

After the transaction confirms, the SuiNS name points to your Walrus Site.

## Browse your Walrus Site

Once the link is confirmed on-chain, visit your site at the following address:

```
https://YOUR_SUINS_NAME.wal.app
```

Replace `YOUR_SUINS_NAME` with the name you registered. For example, if you registered `myproject`, browse to `https://myproject.wal.app`.

The portal resolves the SuiNS name to your Walrus Site object ID and displays your site's content.

:::info
On-chain transactions take a few seconds to finalize. If your site does not load immediately after approving the transaction, wait a moment and refresh the page.
:::

## Update the link

You can update the Walrus Site your SuiNS name points to at any time. This is useful when you run `site-builder deploy` to publish a new version of your site and the command produces a new site object ID.
 
To update the link, repeat the [linking steps](#link-your-suins-name-to-your-walrus-site) with the new object ID. Each update requires a new on-chain transaction and costs a small gas fee.