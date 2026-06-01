> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

`with_memwal_langchain` and `with_memwal_openai` wrap an existing LLM client with automatic memory management. Before each call relevant memories are recalled and injected; after each call the user message is analyzed for new facts (fire-and-forget).

Both integrations import their dependency lazily, install only what you use:

```bash
$ pip install memwal[langchain]
```

```bash
$ pip install memwal[openai]
```

## LangChain

```python
import os
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage
from memwal import with_memwal_langchain

llm = ChatOpenAI(model="gpt-4o")
smart_llm = with_memwal_langchain(
    llm,
    key=os.environ["MEMWAL_PRIVATE_KEY"],
    account_id=os.environ["MEMWAL_ACCOUNT_ID"],
    env="prod",
    namespace="chatbot-prod",
    max_memories=5,
    min_relevance=0.3,
)

response = await smart_llm.ainvoke([HumanMessage("What are my food allergies?")])
```

Patches both `_agenerate` (async) and `_generate` (sync) on the model instance.

## OpenAI SDK

Works with both `openai.OpenAI` (sync) and `openai.AsyncOpenAI` (async), the wrapper detects which and patches `chat.completions.create` accordingly.

```python
import os
from openai import AsyncOpenAI
from memwal import with_memwal_openai

client = AsyncOpenAI()
smart_client = with_memwal_openai(
    client,
    key=os.environ["MEMWAL_PRIVATE_KEY"],
    account_id=os.environ["MEMWAL_ACCOUNT_ID"],
    env="prod",
)

response = await smart_client.chat.completions.create(
    model="gpt-4o",
    messages=[{"role": "user", "content": "What are my food allergies?"}],
)
```

> The JS-style alias `withMemWal` is exported as a shortcut for `with_memwal_langchain`.

## What it does

**Before generation:**

- Reads the last user message
- Runs `recall()` against Walrus Memory
- Filters by `min_relevance` (default `0.3`)
- Injects matching memories as a system message before the last user message

**After generation:**

- If `auto_save` (default `True`), runs `analyze()` on the user message fire-and-forget
- Extracted facts are stored asynchronously

## Options

Both wrappers accept the same keyword arguments:

| Option | Default | Description |
| --- | --- | --- |
| `server_url` | `http://localhost:8000` | Explicit relayer URL (wins over `env`) |
| `env` |, | Relayer preset: `prod` / `dev` / `staging` / `local` |
| `namespace` | `"default"` | Memory namespace |
| `max_memories` | `5` | Max memories injected per request |
| `auto_save` | `True` | Auto-save new facts from the conversation |
| `min_relevance` | `0.3` | Minimum similarity (0–1) to include a memory |
| `debug` | `False` | Verbose logging through the `memwal` logger |

## When to use direct SDK calls instead

Use direct `MemWal` methods when you need precise control over when memory is stored, which text is analyzed, or how recall results are filtered and displayed.