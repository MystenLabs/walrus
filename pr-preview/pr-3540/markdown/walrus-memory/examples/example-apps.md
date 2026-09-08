> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

The repo includes ready-to-run apps in `apps/` that show different Walrus Memory integration patterns. Start here for app-level patterns; [Quick Start](/walrus-memory/sdk/quick-start) and [Walrus Memory Usage](/walrus-memory/sdk/usage/memwal) cover the basic SDK flow.

## Run locally

Each app needs its own environment file and database setup before it starts; the chatbot, noter, and researcher pages below cover the exact steps. After configuring an app, run it from the repo root:

[Source: examples/example-apps.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/examples/example-apps.md)

```bash
$ pnpm dev:app
$ pnpm dev:chatbot
$ pnpm dev:noter
$ pnpm dev:researcher
```

## [Playground](https://github.com/MystenLabs/MemWal/tree/main/apps/app)

Dashboard, playground, and interactive demo for Walrus Memory.

[Source: examples/example-apps.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/examples/example-apps.md)

```ts
const memwal = MemWal.create({
  key: delegateKey,
  accountId: accountObjectId,
  serverUrl,
  namespace,
});

const job = await memwal.remember(rememberText);
await memwal.waitForRememberJob(job.job_id);
await memwal.recall({ query: recallQuery, limit: 5 });
await memwal.analyze(analyzeText);
```

This app covers the full getting-started flow in one place. It signs users in, sets up delegate keys, shows SDK credentials, and includes a live playground for `remember()`, `recall()`, `analyze()`, `restore()`, AI middleware, and manual mode.

## The demo apps

Each demo app has its own page with the integration pattern it shows, the key code, and run instructions:

- **Chatbot:** [An AI chat app](/walrus-memory/examples/chatbot) that wraps its model with the AI middleware, so recall runs before each generation and new context saves automatically.
- **Noter:** [A note-taking app](/walrus-memory/examples/noter) with zkLogin sign-in that turns note content into structured, searchable facts through `analyze`.
- **Researcher:** [A research assistant](/walrus-memory/examples/researcher) that saves each sprint as a structured report and rebuilds context for fresh sessions through recall.