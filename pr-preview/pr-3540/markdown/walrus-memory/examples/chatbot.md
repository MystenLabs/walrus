> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

The chatbot example (`apps/chatbot`) is a production-style AI chat app built on Next.js and the Vercel AI SDK. It shows the lightest-touch Walrus Memory integration: wrap the model once, and memory works for every conversation turn.

## How it uses Walrus memory

The server wraps the selected model with `withMemWal`, so recall runs before each generation and the middleware can save new context after each turn:

[Source: examples/chatbot.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/examples/chatbot.md)

```ts
import { withMemWal } from "@mysten-incubation/memwal/ai";

const model = withMemWal(baseModel, {
  key,
  accountId,
  serverUrl,
  maxMemories: 5,
  autoSave: true,
});
```

The UI lets the user enable Walrus Memory, collect a delegate key and account ID, and pass them to the chat API. Everything else stays a normal AI SDK chat app, which is the point: the middleware adds memory without changing the generation code. See [AI Integration](/walrus-memory/sdk/usage/with-memwal) for the middleware options.

## Run it locally

From the repo root:

[Source: examples/chatbot.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/examples/chatbot.md)

```bash
$ pnpm install
$ cp apps/chatbot/.env.example apps/chatbot/.env
```

Fill in `apps/chatbot/.env`: `AUTH_SECRET`, `OPENROUTER_API_KEY`, `BLOB_READ_WRITE_TOKEN`, `POSTGRES_URL`, `REDIS_URL`, and the Walrus Memory values from the dashboard (`MEMWAL_PRIVATE_KEY`, `MEMWAL_ACCOUNT_ID`, `MEMWAL_SERVER_URL`). Run `pnpm --filter chatbot verify:memwal` to catch credential mismatches before the first relayer call.

With the environment configured, apply the database migrations and start the app:

[Source: examples/chatbot.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/examples/chatbot.md)

```bash
$ pnpm --filter chatbot db:migrate
$ pnpm dev:chatbot
```

The app runs on [localhost:3001](http://localhost:3001). The [chatbot source](https://github.com/MystenLabs/MemWal/tree/main/apps/chatbot) documents each variable.