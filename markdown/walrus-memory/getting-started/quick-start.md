> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

The fastest way to get Walrus Memory running is through the TypeScript SDK.

- [x] [Node.js](https://nodejs.org/) v18+ or [Bun](https://bun.sh/) v1+

### Install the SDK

    ```bash
        pnpm add @mysten-incubation/memwal
        ```

    **Optional packages**

    For AI middleware with [Vercel AI SDK](https://sdk.vercel.ai/) (`@mysten-incubation/memwal/ai`):

    ```bash
        pnpm add ai
        ```

    For the [manual client flow](/walrus-memory/getting-started/choose-your-path) (`@mysten-incubation/memwal/manual`):

    ```bash
        pnpm add @mysten/sui @mysten/seal @mysten/walrus
        ```

  ### Generate your account ID and delegate key

    Create a Walrus Memory account ID and delegate private key for your SDK client using one of the hosted endpoints below.

    :::note
The following endpoints are provided as a public good by Walrus Foundation.
:::

    | App | URL |
    | --- | --- |
    | **Walrus Memory Playground** | [memory.walrus.xyz](https://memory.walrus.xyz) |

    For the contract-based setup flow, see [Delegate Key Management](/walrus-memory/contract/delegate-key-management) and [Walrus Memory smart contract](/walrus-memory/contract/overview).

  ### Choose a relayer

    Use a hosted relayer, or deploy your own [self-hosted relayer](/walrus-memory/relayer/self-hosting) with access to a wallet funded with WAL and SUI:

    :::note
Following endpoints are provided as public good by Walrus Foundation.
:::

    | Network | Relayer URL |
    | --- | --- |
    | **Production** (Mainnet) | `https://relayer.memory.walrus.xyz` |
    | **Staging** (Testnet) | `https://relayer-staging.memory.walrus.xyz` |

  ### Configure the SDK

    Set up the SDK with your delegate key, account ID, and relayer URL:

    ```ts
    import { Walrus Memory } from "@mysten-incubation/memwal";

    const memwal = Walrus Memory.create({
      // Load your own credentials from the environment; don't hardcode an example ID.
      key: process.env.MEMWAL_KEY ?? "<your-ed25519-private-key>",
      accountId: process.env.MEMWAL_ACCOUNT_ID ?? "<your-memwal-account-id>",
      serverUrl: "https://relayer.memory.walrus.xyz",
      namespace: "my-app",
    });
    ```

    :::warning
Use the `accountId` **you** generated in the previous step. Recall is scoped per **account + namespace**. Reusing an account ID copied from docs or another project puts your memories in a shared space instead of isolating them to you.
:::

  ### Verify your connection

    Run a health check to confirm everything is working:

    ```ts
    await memwal.health();
    ```

  ### Store and recall your first memory

    ```ts
    const job = await memwal.remember("User prefers dark mode and works in TypeScript.");
    await memwal.waitForRememberJob(job.job_id);

    const result = await memwal.recall({ query: "What do we know about this user?" });
    console.log(result.results);
    ```

    That's it - you're up and running.