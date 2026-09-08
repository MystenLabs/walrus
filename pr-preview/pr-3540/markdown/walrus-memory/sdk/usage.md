> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Walrus Memory exposes three entry points:

| Entry point | Import | When to use |
| --- | --- | --- |
| `MemWal` | `@mysten-incubation/memwal` | **Recommended default**, relayer handles embeddings, Seal, and storage |
| `MemWalManual` | `@mysten-incubation/memwal/manual` | You need client-managed embeddings and local Seal operations |
| `withMemWal` | `@mysten-incubation/memwal/ai` | You already use the Vercel AI SDK and want memory as middleware |

## Namespace rules

- Set a default namespace in `create(...)` when one app or agent uses one boundary
- Pass `namespace` per call when one client needs multiple boundaries
- If omitted, namespace falls back to client config, then to `"default"`

Good namespace examples: `todo`, `personal`, `password`, `project-x`. Avoid keeping everything in `"default"` after early testing.