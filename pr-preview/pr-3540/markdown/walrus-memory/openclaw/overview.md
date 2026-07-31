> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

The Walrus Memory plugin adds **portable, verifiable agent memory** to OpenClaw agents. It works alongside OpenClaw's existing file-based memory, automatically recalling relevant context and capturing new facts in the background, with no user action needed. Memory is not locked to a single runtime: it operates across agents, apps, and workflows.

## Features

  

**Automatic Recall**

Relevant memories are injected into the LLM's context before each conversation turn

  

**Automatic Capture**

Facts are extracted from conversations and stored as encrypted memories after each turn

  

**Fully Under Your Control**

Seal-encrypted, stored on Walrus, programmable permissions and explicit ownership over your data

  

**Portable Across Apps**

Memories stored from any Walrus Memory-connected app are accessible to your OpenClaw agent, not locked to a single provider

  

**Multi-Agent Isolation**

Each agent gets its own memory space through namespaces, no cross-contamination

  

**Prompt Injection Protection**

Detection and HTML escaping on both read and write paths

  

**Agent Tools**

Optional `memory_search` and `memory_store` tools for explicit LLM control

  

**CLI Commands**

`openclaw memwal search` and `openclaw memwal stats` for debugging and inspection

## When to use this

- You want your OpenClaw agents to **remember across conversations**, preferences, decisions, context
- You need **encrypted, user-owned memory** instead of plaintext files or platform-managed storage
- You want **cross-app continuity**, memories from other Walrus Memory-connected apps (chatbot, noter, researcher) surface in OpenClaw
- You're running **multiple agents** and need each to have its own isolated memory space

## Get started

  
    Install, configure, and verify the plugin in minutes
  
  
    Architecture, message flow, hooks vs tools
  
  
    Hooks, tools, CLI, configuration, and troubleshooting
  
  
    Browse the source on GitHub