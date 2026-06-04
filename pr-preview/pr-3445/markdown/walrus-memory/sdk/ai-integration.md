> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Walrus Memory includes an AI SDK integration for applications that already use model middleware.

## `withMemWal`

```ts
import { generateText } from "ai";
import { withMemWal } from "@mysten-incubation/memwal/ai";
import { openai } from "@ai-sdk/openai";

const model = withMemWal(openai("gpt-4o"), {
  key: process.env.MEMWAL_PRIVATE_KEY!,
  accountId: process.env.MEMWAL_ACCOUNT_ID!,
  serverUrl: process.env.MEMWAL_SERVER_URL,
  namespace: "chatbot-prod",
  maxMemories: 5,
  autoSave: true,
});

const result = await generateText({
  model,
  messages: [{ role: "user", content: "What do you know about me?" }],
});
```

## What it does

Before generation:

- reads the last user message
- runs `recall()` against Walrus Memory
- filters by relevance
- injects memory context into the prompt

After generation:

- optionally runs `analyze()` on the user message
- saves extracted facts asynchronously

## Why namespace matters here

Set a namespace explicitly for each product surface that uses the middleware. Otherwise recalled
and auto-saved memories fall back to `"default"`.

## When to use direct SDK calls instead

Use direct SDK methods when your app needs precise control over:

- when memory is stored
- which text is analyzed
- how recall results are displayed or filtered