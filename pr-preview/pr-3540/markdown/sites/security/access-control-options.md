> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Walrus stores every blob publicly by default. Anyone who knows a blob ID can fetch its contents directly from a Walrus aggregator, and Walrus does not enforce access control at the storage layer. Site metadata, including every resource path and blob ID, lives in public objects on Sui. Choose an approach based on what each part of your site needs: publish public content as-is, keep sensitive files out of the deployment, and encrypt confidential application data before it reaches Walrus.

## Choosing an approach

| **Requirement** | **Approach** |
|---|---|
| Serve website content to everyone | Publish normally; this is the default model |
| Keep development files or drafts out of the deployment | Exclude them with the `ignore` field in `ws-resources.json` |
| Protect confidential data your app reads and writes | Encrypt on the client, for example with Seal, before storing on Walrus |
| Use secrets such as API keys or credentials | Never place them in site files; keep them in systems outside Walrus |
| Detect tampering with served content | [Site data authentication](/docs/sites/security/site-data-authentication), active by default |

## Fully public content (default)

When you publish a Walrus Site using [`site-builder`](/docs/sites/getting-started/publishing-your-first-site), the tool stores all resources as publicly readable blobs on Walrus. The portal serves them to any visitor without authentication. This is appropriate for most static sites, documentation, open-source project pages, and other content that has no confidentiality requirement.

For these use cases, [site data authentication](/docs/sites/security/site-data-authentication) already provides integrity guarantees: the [portal](/docs/sites/portals/mainnet-testnet) verifies each resource's SHA-256 hash against the value stored on Sui before serving it, ensuring no one tampered with the content in transit.

> **Info**
>
> Integrity verification and access control are separate concerns. Authentication confirms that no one modified the content. It does not restrict who can retrieve it.
## Keeping files out of the deployment

The most direct form of access control is not publishing a file at all. The `ignore` field in [`ws-resources.json`](/docs/sites/configuration/site-configuration#ignoring-files-from-being-uploaded) excludes files and folders from the upload, which keeps development files, drafts, and temporary assets out of the published site:

```json
"ignore": [
  "/private/*",
  "/drafts/*",
  "/notes.txt"
]
```

This only controls what `site-builder` uploads. Anything you have already published remains readable for its full storage period, so review your build output before you deploy, not after.

> **Caution**
>
> Never embed secret values (API keys, tokens, private keys, credentials) anywhere in a site's source files. Site files and their metadata are fully public. See [Known Restrictions](/docs/sites/known-restrictions#no-secret-values).
## Encrypting application data

Site resources themselves (the HTML, CSS, and JavaScript that the portal serves) must stay in plaintext for browsers to render them. Confidential data that your site's application code reads and writes as blobs is a different matter: encrypt it on the client before storing it on Walrus, and treat the blob ID as public. Your application then fetches the ciphertext and decrypts it locally for users who hold the necessary keys.

[Seal](https://github.com/MystenLabs/seal) supports this pattern. Your application owns key management and encryption: Walrus stores and serves whatever bytes you upload. See [Data Security](/docs/data-security#data-confidentiality) for the underlying guarantees.

## Serving private content outside Walrus

If content requires per-user authentication and cannot be public even in encrypted form, do not store it on Walrus. Serve it from a backend you control, behind your own authentication, and let your Walrus Site call that service from client-side code. Privileged operations can also run through Sui smart contracts instead of a backend. Your site's client-side code connects the visitor's wallet with the [Sui dApp Kit](https://sdk.mystenlabs.com/dapp-kit) and asks them to sign a transaction, so the contract enforces the permission onchain and the visitor's key never leaves their wallet. See [Transactions](https://docs.sui.io/guides/developer/transactions/txn-overview) for how signing works. This keeps secrets out of the deployed site assets entirely, because there is no secret to deploy.

## What does not restrict access

Some approaches look like access control but do not protect the underlying data:

- **Gating a portal:** You can put HTTP authentication, IP restrictions, or rate limits in front of a [self-hosted portal](/docs/sites/portals/deploy-locally), but this only restricts that one portal. Anyone can run their own portal or fetch the site's blobs directly from any aggregator, because Sui exposes the resource paths and blob IDs publicly.
- **Obscure URLs:** Unlisted paths offer no protection. The site object on Sui publicly maps every resource path to its blob ID.
- **Blob IDs as secrets:** Blob IDs are content-derived and discoverable. Do not rely on keeping a blob ID private.