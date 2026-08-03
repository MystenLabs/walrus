> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

When you give an AI agent long-term memory, you have to decide where that data lives. The choice shapes what the agent can do later: whether its memory survives a restart, moves with the user to another app, stays under the user's control, and can be trusted.

## The options

Most teams choose among four approaches. Provider-native memory is the feature built into an agent platform or model provider. It is the least work to turn on, but the data lives inside that provider and follows its rules. A managed vector database, such as a hosted cloud vector store, hands the running of the database to a provider while you design the schema, generate embeddings, and manage access in your own application. A self-hosted vector database such as pgvector, Qdrant, or Chroma gives you full control of the stack in exchange for operating all of it yourself. Walrus Memory takes a different shape: it runs the embed, store, and recall loop for you, stores your memories on Walrus, and enforces ownership and access onchain through Sui.

## Compare the approaches

The right choice depends on which properties your agent actually needs. The table compares the approaches at the capability level.

| **Property** | **Provider-native memory** | **Vector database (managed or self-hosted)** | **Walrus Memory** |
| --- | --- | --- | --- |
| Persists across sessions | Yes, within that provider | Yes | Yes |
| Semantic recall | Usually | Yes | Yes |
| Portable across apps and providers | No, locked to the provider | You own the data, but you migrate it yourself | Yes, control follows the onchain account |
| Ownership enforced independently | No, the provider decides | At the application layer, by your code | Yes, onchain through Sui |
| Integrity independently verifiable | No | No | Yes, content-addressed blobs plus onchain records |
| Infrastructure you operate | None | Managed: the database. Self-hosted: the full stack | None by default, self-hosting optional |

A vector database already gives you persistence and semantic recall, which is often all a single application needs. What sets Walrus Memory apart is that it adds portability, owner-controlled access enforced onchain, and verifiable integrity without asking you to run storage infrastructure.

## Choose based on what you need

Reach for provider-native memory when you are prototyping inside one platform and the data never needs to leave it, accepting that you give up portability and independent control. A managed vector database fits when you are building a single application, you want semantic search, and keeping the data inside your own account is enough, because you own the access-control model that surrounds it. Run a self-hosted vector database when you need full control of the stack and are prepared to secure it, and you do not need portability across apps or onchain ownership. Choose Walrus Memory when memory should travel with the user or agent across apps, when ownership and access should hold independently of whoever runs the server, when integrity should be verifiable, and when you would rather not operate storage yourself.

These approaches are not mutually exclusive. An agent can keep a small local cache for speed and use Walrus Memory as the durable store of record. Because recall returns a blob ID for each memory, you can always point back to the exact stored item.

## Where Walrus memory fits against a vector database

Teams often frame this decision as choosing the best vector database for AI agents, but a vector database and Walrus Memory solve overlapping but different problems. A vector database is a search index: it stores vectors and returns nearest neighbors. Walrus Memory uses a vector index internally for exactly that, then adds the layer most agent products end up needing anyway. The data outlives the index, because the memories live on Walrus and the vector index is a search layer that `restore` rebuilds from them, so losing the database does not lose the memory. Ownership stops being an application concern, because Sui smart contracts enforce who can read or write a memory space. That rule holds across apps and relayers rather than each application reimplementing it. And portability comes built in, because control follows the onchain account, so a user can move between apps or relayers without exporting and re-importing data. If you only need nearest-neighbor search inside one app, a vector database is simpler. If you need memory that is durable, portable, owner-controlled, and verifiable, that is what Walrus Memory adds on top.

To go deeper, see [How AI Agent Memory Works](/walrus-memory/fundamentals/concepts/how-agent-memory-works) for the concepts, [Persistent, Verifiable Memory](/walrus-memory/fundamentals/concepts/verifiable-memory) for how durability and ownership work, and the [Agent Storage Loop](/walrus-memory/sdk/agent-storage-loop) to build the write-confirm-recall loop with the SDK.