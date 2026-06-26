---
title: "Walrus Memory for OpenClaw: Editorial Release Summary"
description: "An editorial overview of the Walrus Memory OpenClaw plugin releases, from hook-driven automatic memory to temporal anchoring and a packaging fix."
keywords: [Walrus Memory, OpenClaw, NemoClaw, release notes, plugin, hooks, temporal anchoring]
---

# Walrus Memory for OpenClaw: Memory That Hooks Itself In

_An editorial summary of the Walrus Memory OpenClaw (OC) plugin releases (v0.0.2, April 2026 to v0.0.5, June 2026)._

The OpenClaw plugin is Walrus Memory's answer to a specific question: how do you give an agent framework a memory without asking the developer to wire up every call? Its short release history answers by leaning entirely on lifecycle hooks, so the plugin listens to the agent's natural events and remembers and recalls on its own. The four releases trace that idea from initial implementation to a notable feature add and, finally, a very honest packaging fix.

## Automatic by design (v0.0.2, "Initial Release")

The first published release lays out the whole philosophy. The plugin, for NemoClaw and OpenClaw and powered by what was then MemWal, performs automatic memory recall through a `before_prompt_build` hook, automatic fact capture through an `agent_end` hook, and session summaries on `before_reset`. It exposes CLI commands (`openclaw memwal stats`, `openclaw memwal search`) and LLM tools (`memory_search`, `memory_store`). The point is that memory is ambient: the developer installs the plugin and the agent simply starts remembering.

## Folding into the Walrus brand (v0.0.3)

v0.0.3 is a clean rebrand release, aligning the plugin's package metadata and documentation with the Walrus Memory name alongside the rest of the family.

## Teaching memory about time (v0.0.4)

v0.0.4 is the substantive one. It wires temporal anchoring through the agent-side tools: the `memory_store` tool gains an optional `occurredAt` argument so agents can pin recounted past events to when they actually happened, with the description pointedly telling the model to omit the field when it does not know rather than guess. More importantly, the auto-capture hook now passes the current time to `analyze()` automatically, so every captured conversation gets temporal anchoring for free and the server can resolve relative references like "yesterday" into absolute dates inside the stored fact. This is the OpenClaw plugin inheriting the TypeScript SDK's `occurredAt` capability and making it automatic.

## An honest packaging fix (v0.0.5)

The latest release is a small but instructive bugfix. The published package had shipped with an unresolvable `workspace:*` dependency because the release workflow used `npm publish`, which writes `package.json` verbatim and cannot rewrite the workspace protocol, so the artifact failed to install outside the monorepo. The fix: switch the workflows to `pnpm publish`, which rewrites `workspace:*` to a concrete version at pack time. It is the kind of release-engineering detail that only surfaces once outside users try to install you, and the notes describe it without flinching.

## The arc, in one line

The OpenClaw plugin's releases are about making memory effortless and then making it accurate: hook-driven capture from the start, time-aware facts by v0.0.4, and a candid packaging fix that lets the whole thing actually install. For an agent framework, that is memory you do not have to think about.
