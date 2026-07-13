> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Walrus Console signs you in with a familiar identity provider and provisions everything you
need to start storing data. You do not create a wallet, manage a seed phrase, or hold tokens
to sign up.

> **Note**
>
> Walrus Console is in an invited beta on Mainnet. Google sign-in is available today, and Apple
> sign-in joins at beta. For what happens after you sign in, see the
> [quickstart](./quickstart). For the product model, see the [concepts and overview](./overview).
## How sign-in works

Console uses Sui zkLogin. You authenticate with an identity provider you already have, and
Console derives a Sui address from that identity without exposing your provider account onchain
and without asking you to manage a private key. The result is a standard Sui address that owns
your data, backed by a sign-in you already know how to use.

## Sign in with Google

1. Visit [testnet.harbor.walrus.xyz](https://testnet.harbor.walrus.xyz/).
2. Choose **Continue with Google** and complete the Google sign-in.
3. Console provisions your account and a Personal Space, then takes you to the dashboard.

## Sign in with Apple

Apple sign-in joins at beta and works the same way. Console derives your Sui address from your
Apple identity and provisions your account and Personal Space.

Console accepts Apple's private email relay, so you can use Apple's **Hide My Email** option.
Console treats the relay address as your account email and delivers any account email through
it.

## Your Pearl wallet

On first sign-in, Console silently provisions a Pearl wallet for your derived Sui address. This
wallet holds the storage resources your data uses. Console sponsors gas and manages the wallet
for you, so you do not fund it or sign transactions to get started. The exceptions are the
explicit signing steps in the encrypted-bucket flow, where you sign with your own service key.

## Accounts stay separate per identity

> **Warning**
>
> Each identity provider maps to a separate Console account and a separate Sui address. If you
> sign in with Google and later sign in with Apple, you get two independent accounts, not one
> merged account. Data stored under one is not visible under the other.
Console shows an account-separation notice the first time you sign in so this is clear before
you store anything. Choose one provider and use it consistently. Linking multiple providers to
a single account is planned for a later release.

## Wallet sign-in

Signing in with an existing Sui wallet is planned for a later release. For now, sign in with
Google or Apple through zkLogin. If you hold assets in a personal Sui wallet, note that data
you store through Console lives under your zkLogin-derived address, which is separate from a
wallet you connect later.

## After you sign in

You land on the dashboard with a Personal Space ready to use. The next step is to mint an API
key and make your first request. Follow the [quickstart](./quickstart) to create an encrypted
bucket and upload a file.

## Next steps

- Continue to the [quickstart](./quickstart) to mint a key and upload your first file.
- See the [concepts and overview](./overview) for spaces, buckets, and asset types.