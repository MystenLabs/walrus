> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

The researcher example (`apps/researcher`) is a research assistant that works in sprints. It shows long-form memory and session rehydration: each sprint's findings persist as a structured report, and a fresh session pulls back the relevant history before it starts.

## How it uses Walrus memory

Researcher composes each sprint into one structured report and stores it with `remember`, then generates recall queries from sprint metadata to rebuild context:

[Source: examples/researcher.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/examples/researcher.md)

```ts
const fullText =
  `Sprint Report: ${title}\n\n` +
  `${content}\n\n` +
  `References:\n${references}\n\n` +
  `Sources: ${sourceList}`;

const job = await memwal.remember(fullText);
await memwal.waitForRememberJob(job.job_id);
const { results } = await memwal.recall({ query, limit: 5 });
```

The structured report format matters: because the whole sprint lives in one memory, recall returns complete findings with their references and sources attached, and the assistant can cite where earlier conclusions came from.

## Run it locally

From the repo root:

[Source: examples/researcher.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/examples/researcher.md)

```bash
$ pnpm install
$ cp apps/researcher/.env.example apps/researcher/.env
```

Fill in the required values in `apps/researcher/.env`: `OPENROUTER_API_KEY`, `POSTGRES_URL` (a PostgreSQL database with the pgvector extension), `AUTH_SECRET`, and the Walrus Memory values from the dashboard (`MEMWAL_PRIVATE_KEY`, `MEMWAL_ACCOUNT_ID`, `MEMWAL_SERVER_URL`). `REDIS_URL`, `BLOB_READ_WRITE_TOKEN`, and the Enoki zkLogin variables are optional.

With the environment configured, apply the database migrations and start the app:

[Source: examples/researcher.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/examples/researcher.md)

```bash
$ pnpm --filter researcher db:migrate
$ pnpm dev:researcher
```

The [researcher source](https://github.com/MystenLabs/MemWal/tree/main/apps/researcher) documents each variable.