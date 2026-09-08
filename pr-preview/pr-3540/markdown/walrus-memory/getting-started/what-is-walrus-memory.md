> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Walrus Memory enables AI agents to operate reliably across apps and sessions, without losing context. Portable, verifiable, and fully controlled by you, it's the memory layer that lets agents handle complex workflows and coordinate using data they can trust.

  

**Portable by Design**

Memory operates across agents, apps, and workflows without binding to a single runtime or provider

  

**Fully Under Your Control**

Programmable permissions and explicit ownership define how memory is shared, accessed, and updated

  

**Built for Agent Coordination**

Shared memory spaces help agents coordinate across long-running and multi-step workflows

  

**Verifiable Integrity**

Anyone can independently verify memory integrity without centralized trust

## Motivation

AI agents today lose context between sessions: every conversation starts from scratch. When memory does exist, vendors lock it inside platform-specific databases that the user doesn't control. Walrus Memory solves this by giving agents:

- **Portable memory:** Memory persists outside prompts and context windows, moving across agents, apps, and workflows.
- **Full owner control:** Programmable access control and explicit ownership, with delegate access for agents and workflows.
- **Agent coordination:** Shared memory spaces help agents coordinate across long-running and multi-step workflows.
- **Verifiable integrity:** Anyone can independently verify memory integrity without centralized trust.

## Features

Walrus Memory groups its features into memory operations, ownership and access control, and infrastructure.

### Memory operations

  

**Remember**

Store memories with semantic understanding. The relayer generates vector embeddings so your data is searchable by meaning, not just keywords.

  

**Recall**

Retrieve relevant memories using natural language queries. Finds the closest matches based on meaning, scoped to your memory space.

  

**Analyze**

Extract structured facts from text automatically. Each fact lands as a separate memory for more precise recall later.

  

**Ask**

Query your memories and get an AI-generated answer with the relevant context attached. Combines recall with LLM reasoning.

### Ownership and access control

  

**Decentralized Storage**

Walrus stores the blobs, with no single point of failure and no central operator holding your data.

  

**Programmable Permissions**

Sui smart contracts enforce ownership and access rules, giving you explicit, programmable control over who can read and write.

  

**Delegate Access**

Grant scoped access to other agents, users, or services. The owner manages every grant onchain, enabling agent coordination and cross-app workflows.

### Infrastructure

  

**Restore**

Rebuild your index from Walrus if it's ever lost. Rediscovers blobs by owner and namespace, re-embeds only missing entries.

  

**AI Middleware**

Drop-in memory for Vercel AI SDK apps. Automatically saves and recalls context around AI conversations.

## What's included

- **TypeScript SDK:** Integrate memory into any app with a few lines of code.
- **Relayer:** Handles storage and retrieval behind a basic API.
- **Smart Contract:** Enforces ownership and delegate access onchain.
- **Indexer:** Keeps onchain state synced for fast lookups.
- **Dashboard:** Manage accounts, memory, and delegate keys visually.

## Use cases

Walrus Memory fits any app where agents need memory that travels with them:

- **AI chat apps:** Capture valuable knowledge from conversations so agents remember context across sessions and apps.
- **Multi-agent workflows:** Shared memory spaces let agents coordinate on task lists, knowledge bases, and coordination state.
- **Personal AI assistants:** Build agents that learn and adapt over time, with memory the user fully controls.
- **Cross-app memory:** Let users carry their memory between different apps and services instead of binding them to a single provider.
- **Note-taking and knowledge tools:** Save user insights, summaries, and references as portable, verifiable memory.

Check out the example apps below to see Walrus Memory in action.

## Example apps

The repo ships with ready-to-run apps in the [`/apps`](https://github.com/MystenLabs/MemWal/tree/main/apps) directory:

- **Playground:** Dashboard demo for Walrus Memory.
- **Chatbot:** AI chat app with portable memory across sessions.
- **Noter:** Note-taking tool that stores knowledge as verifiable memory.
- **Researcher:** Research assistant that builds and recalls a knowledge base.

See [Example Apps](/walrus-memory/examples/example-apps) for short code examples from each app.

If Walrus Memory is useful to you, [a star on the GitHub repo ⭐](https://github.com/MystenLabs/MemWal) helps others find it.

## Explore the docs

  
    Memory spaces, ownership and delegates
  
  
    System overview, component responsibilities, core flows, data flow security
  
  
    Quickstart, usage patterns, AI integration, and examples
  
  
    Managed relayer, installation and setup, self-hosting
  
  
    Onchain ownership model, delegate key management, permissions
  
  
    Event indexing, onchain events, database sync
  
  
    SDK API, relayer API, configuration, environment variables