> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

AI agent memory is a durable, searchable store of what an agent has learned, kept outside the model so it survives across sessions, apps, and workflows. A model on its own starts every session blank, with no record of the last conversation or the decisions it made, and memory is the layer that gives it continuity.

## The context window is not memory

A large language model reads and writes through its context window, the block of tokens it sees on each request. Everything the model knows in the moment lives there, and it disappears when the request ends. The next request starts fresh unless you resend the earlier text.

That makes the context window a poor substitute for memory. It holds only a fixed token budget, so an agent's entire history cannot fit into it, and padding it with old turns crowds out room for the current task. It keeps nothing after the request ends, so you must provide persistence from somewhere the model does not control. And it costs tokens and latency to refill, because replaying a long history on every call spends both on material that mostly has no bearing on the current question. Long-term memory sidesteps all three problems by keeping the agent's knowledge outside the prompt and pulling only the relevant pieces back into the context window when the agent needs them.

| **Aspect** | **Context window** | **Long-term memory** |
| --- | --- | --- |
| Lifetime | One request | Persists across sessions |
| Capacity | Fixed token budget | Effectively unbounded |
| Retrieval | Everything, every call | Only the relevant pieces, on demand |
| Where it lives | The model provider, transiently | A store you control |

## Short-term and long-term memory

It helps to think of an agent as having two layers of memory, much as people do. Short-term, or working, memory is the context window: the current conversation, the task at hand, and whatever you have just retrieved. It is fast and immediate but small and fleeting. Long-term memory is a durable store that sits outside the model and holds past conversations, extracted facts, embeddings, and checkpoints. It is large and persistent, and the agent reaches into it deliberately. A working agent uses both, keeping the active task in the context window and retrieving from long-term memory whatever it does not currently hold.

## How long-term memory works

Long-term memory for agents is usually semantic, which means the agent stores and retrieves by meaning rather than by exact keywords. The mechanism behind it is embeddings and vector search. When you store a piece of text, an embedding model converts it into a vector, a list of numbers that captures its meaning, so that two texts about the same idea end up close together even when they share no words. The store saves that vector alongside the original text. Later, when the agent needs context, it embeds the query the same way, and the store returns the entries whose vectors sit closest to the query, ranked by similarity. The agent adds those retrieved memories to its context window and answers with them in hand, which is the retrieve-then-answer pattern at the core of retrieval-augmented generation.

### Writing a memory

  
  
    Writing a memory: an embedding model converts the text into a vector, a list of numbers representing its meaning. The vector goes into a vector store that finds nearest neighbors quickly, and the store keeps the original text alongside it. You search the vector, but the agent reads the text.
  

### Recalling a memory

  
  
    Recalling a memory: the same embedding model converts the question into a vector so it lands in the same space as the stored memories. The store ranks stored vectors by distance, and the closest ones are the most similar in meaning even when they share no words with the question. The agent adds their original text to its context window.
  

Semantic recall is what lets an agent answer "How are session tokens issued?" from a memory written as "createSession signs a JWT with a 30-minute TTL," even though the two share almost no words. The embeddings encode meaning, so the match is by concept rather than by string.

### Kinds of long-term memory

Not every memory is a conversation turn. Agents accumulate a few different kinds of long-term memory. Semantic memory is general knowledge and facts, such as how a system works or a user's preferences. Episodic memory is a record of what happened, including past conversations, actions taken, and their outcomes. Procedural memory is how to do things, from reusable steps to learned routines. A single store can hold all three, as long as each item stays individually retrievable by meaning.

## How Walrus memory implements this

Walrus Memory is a long-term memory layer built for agents. It runs the embed, store, and recall loop for you and adds durability, ownership, and verifiability that a plain database does not. Storing a memory with `remember` embeds and saves it, while `recall` embeds your query and returns the closest matches, scoped to a [memory space](/walrus-memory/fundamentals/concepts/memory-space). The `analyze` operation goes further, extracting discrete facts from a longer passage and storing each as its own memory so that recall stays precise rather than returning one large blob. Because Walrus stores memories durably, they outlive any single process or provider and travel with the agent across apps, and because Sui enforces ownership and access onchain rather than whoever runs the server, control of a memory space stays with its owner. For the durability and ownership details, see [Persistent, Verifiable Memory](/walrus-memory/fundamentals/concepts/verifiable-memory) and [Ownership and Delegates](/walrus-memory/fundamentals/concepts/ownership-and-access).

To build this in practice, follow the [Agent Storage Loop](/walrus-memory/sdk/agent-storage-loop) for the full write-confirm-recall loop, or [Headless SDK Setup](/walrus-memory/sdk/headless-setup) to initialize memory in a server or agent runtime. To weigh Walrus Memory against vector databases and other options, see [Where to Store AI Agent Data](/walrus-memory/fundamentals/concepts/where-to-store-agent-data).