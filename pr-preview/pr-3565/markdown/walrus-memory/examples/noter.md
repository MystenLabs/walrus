> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

The noter example (`apps/noter`) is a note-taking app with zkLogin sign-in, so users authenticate with OAuth instead of managing a wallet. It shows the note-to-memory pattern: free-form writing becomes structured facts that recall can find later.

## How it uses Walrus memory

Noter keeps a shared server-side Walrus Memory client and uses `analyze` to turn note content into discrete facts, which the relayer stores asynchronously:

[Source: examples/noter.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/examples/noter.md)

```ts
export const extractMemories = async (text: string): Promise<string[]> => {
  const memwal = getMemWalClient();
  const result = await memwal.analyze(text);
  return (result.facts ?? []).map((f) => f.text);
};
```

Because `analyze` stores each extracted fact as its own memory, later recall returns the specific fact that matches a query instead of a whole note. The user configures the delegate key and account at runtime.

## Run it locally

Noter ships without an `.env.example`, so create `apps/noter/.env` yourself with the variables the [noter README](https://github.com/MystenLabs/MemWal/tree/main/apps/noter) lists: `DATABASE_URL` for PostgreSQL, Google OAuth credentials (`GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`), the zkLogin endpoints, `OPENROUTER_API_KEY`, `NEXT_PUBLIC_APP_URL`, and the Walrus Memory values from the dashboard (`MEMWAL_PRIVATE_KEY`, `MEMWAL_ACCOUNT_ID`, `MEMWAL_SERVER_URL`).

With the environment configured, create the database tables and start the app from the repo root:

[Source: examples/noter.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/examples/noter.md)

```bash
$ pnpm install
$ pnpm --filter noter db:push
$ pnpm dev:noter
```

The app runs on [localhost:3002](http://localhost:3002).