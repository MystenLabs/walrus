> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

> **Note**
>
> Walrus Memory is currently in beta and actively evolving. While fully usable today, the team continues to refine the developer experience and operational guidance. the team welcomes feedback from early builders as the team continues to improve the product.
Walrus Memory enables AI agents to operate reliably across apps and sessions, without losing context. Portable, verifiable, and fully controlled by you, it's the memory layer that lets agents handle complex workflows and coordinate using data they can trust.

  

**Portable by Design**

Memory operates across agents, apps, and workflows, not locked to a single runtime or provider

  

**Fully Under Your Control**

Programmable permissions and explicit ownership define how memory is shared, accessed, and updated

  

**Built for Agent Coordination**

Shared memory spaces help agents coordinate across long-running and multi-step workflows

  

**Verifiable Integrity**

Memory integrity can be independently verified without centralized trust

## Motivation

AI agents today lose context between sessions, every conversation starts from scratch. When memory does exist, it's locked inside platform-specific databases that the user doesn't control. Walrus Memory solves this by giving agents:

- **Portable memory**, memory persists outside prompts and context windows, moving across agents, apps, and workflows
- **Full owner control**, programmable access control and explicit ownership, with delegate access for agents and workflows
- **Agent coordination**, shared memory spaces help agents coordinate across long-running and multi-step workflows
- **Verifiable integrity**, memory integrity can be independently verified without centralized trust

### Memory operations

  

**Remember**

Store memories with semantic understanding. The relayer generates vector embeddings so your data is searchable by meaning, not just keywords.

  

**Recall**

Retrieve relevant memories using natural language queries. Finds the closest matches based on meaning, scoped to your memory space.

  

**Analyze**

Extract structured facts from text automatically. Each fact is stored as a separate memory for more precise recall later.

  

**Ask**

Query your memories and get an AI-generated answer with the relevant context attached. Combines recall with LLM reasoning.

### Ownership  and  access control

  

**End-to-End Encryption**

All content is encrypted through SEAL before it reaches Walrus. Only the owner and authorized delegates can decrypt it.

  

**Decentralized Storage**

Encrypted blobs stored on Walrus, no single point of failure, no central operator holding your data.

  

**Programmable Permissions**

Ownership and access rules are enforced by Sui smart contracts, giving you explicit, programmable control over who can read and write.

  

**Delegate Access**

Grant scoped access to other agents, users, or services, all managed onchain by the owner, enabling agent coordination and cross-app workflows.

### Infrastructure

  

**Restore**

Rebuild your index from Walrus if it's ever lost. Rediscovers blobs by owner and namespace, re-embeds only missing entries.

  

**AI Middleware**

Drop-in memory for Vercel AI SDK apps. Automatically saves and recalls context around AI conversations.

## What's included

- **TypeScript SDK**: integrate memory into any app with a few lines of code
- **Relayer**: handles encryption, storage, and retrieval behind a basic API
- **Smart Contract**: enforces ownership and delegate access onchain
- **Indexer**: keeps onchain state synced for fast lookups
- **Dashboard**: manage accounts, memory, and delegate keys visually

## Use cases

Walrus Memory fits any app where agents need memory that travels with them:

- **AI chat apps**, capture valuable knowledge from conversations so agents remember context across sessions and apps
- **Multi-agent workflows**, shared memory spaces let agents coordinate on task lists, knowledge bases, and coordination state
- **Personal AI assistants**, build agents that learn and adapt over time, with memory the user fully controls
- **Cross-app memory**, let users carry their memory between different apps and services, not locked to any single provider
- **Note-taking and knowledge tools**, save user insights, summaries, and references as portable, verifiable memory

And many more, check out the example apps below to see Walrus Memory in action.

## Example apps

The repo ships with ready-to-run apps in the [`/apps`](https://github.com/MystenLabs/MemWal/tree/main/apps) directory:

- **Playground**, dashboard demo for Walrus Memory
- **Chatbot**, AI chat app with portable memory across sessions
- **Noter**, note-taking tool that stores knowledge as verifiable memory
- **Researcher**, research assistant that builds and recalls a knowledge base

See [Example Apps](/walrus-memory/examples/example-apps) for short code examples from each app.

## Explore the docs

  
    Memory spaces, ownership and delegates
  
  
    System overview, component responsibilities, core flows, data flow security
  
  
    Quickstart, usage patterns, AI integration, and examples
  
  
    Managed relayer, installation and setup, self-hosting
  
  
    Onchain ownership model, delegate key management, permissions
  
  
    Event indexing, onchain events, database sync
  
  
    SDK API, relayer API, configuration, environment variables