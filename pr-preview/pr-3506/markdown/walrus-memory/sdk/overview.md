> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Walrus Memory exposes SDK surfaces for TypeScript and Python. The SDKs give your agents portable memory that works across apps, sessions, and workflows, fully under your control.

### `@mysten-incubation/memwal`

Use this first.

- relayer-backed
- best path for most teams
- main methods: `remember`, `recall`, `analyze`, `restore`, `health`

```ts
import { MemWal } from "@mysten-incubation/memwal";
```

### `@mysten-incubation/memwal/manual`

Use this when the client must handle embeddings and local Seal operations.

- relayer still handles upload relay, registration, search, and restore

```ts
import { MemWalManual } from "@mysten-incubation/memwal/manual";
```

### `@mysten-incubation/memwal/ai`

Use this when you already use the AI SDK.

```ts
import { withMemWal } from "@mysten-incubation/memwal/ai";
```

---

### `memwal`

The Python SDK mirrors the TypeScript `MemWal` client exactly, same methods, same relayer, same auth flow. Built for the Python AI/ML ecosystem.

- relayer-backed (same managed relayer endpoints)
- Ed25519 signing through PyNaCl
- async-native with a sync convenience wrapper
- LangChain and OpenAI SDK middleware included

```bash
$ pip install memwal
$ pip install memwal[langchain]   # LangChain middleware
$ pip install memwal[openai]      # OpenAI SDK middleware
$ pip install memwal[all]         # everything
```

```python
from memwal import MemWal

memwal = MemWal.create(
    key="<your-ed25519-private-key>",
    account_id="<your-memwal-account-id>",
    server_url="https://relayer.memory.walrus.xyz",
    namespace="demo",
)
```

Main methods: `remember`, `recall`, `analyze`, `ask`, `restore`, `health`

Middleware: `with_memwal_langchain`, `with_memwal_openai`

---

## Namespace

All clients support a default namespace. If you omit it, it falls back to `"default"`.

## Recommended path

1. Start with `MemWal` (TypeScript) or `memwal` (Python)
2. Set a namespace explicitly
3. Validate `remember`, `recall`, `analyze`, and `restore`
4. Move to `MemWalManual` only if you need client-managed embeddings and local Seal work