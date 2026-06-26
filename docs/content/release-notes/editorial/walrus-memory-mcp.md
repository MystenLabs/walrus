---
title: "Walrus Memory MCP: Editorial Release Summary"
description: "An editorial overview of the Walrus Memory MCP server releases, from the first stdio server to proactive, plugin-installed memory for coding agents."
keywords: [Walrus Memory, MCP, release notes, coding agents, Claude Code, proactive memory, relayer]
---

# Walrus Memory MCP: Giving Coding Agents a Memory

_An editorial summary of the Walrus Memory MCP releases (v0.0.1, May 2026 to v0.0.5, June 2026)._

The Walrus Memory MCP server is the newest thing in the Walrus orbit, and its short release history is the story of a product finding its shape in real time. In the space of a month it goes from a bare stdio server to a proactive memory layer that installs itself into the major coding agents, and, true to its 0.0.x version numbers, it does so while ironing out the packaging and connection bugs that come with shipping something genuinely new.

## A working server first (v0.0.1)

The initial release is refreshingly concrete about what it is: a stdio MCP server with browser-based wallet login, inline `memwal_login` and `memwal_logout` session tools, and the core memory verbs (remember, recall, analyze, and restore) routed through the relayer. It even ships environment presets for production, dev, staging, and local relayers. This is a product that wanted to be usable on day one rather than a teaser.

## Hardening the bridge and the brand (v0.0.2 to v0.0.3)

The next two releases are about trust and identity. v0.0.2 adds relayer compatibility checks before the MCP bridge opens, a small but telling sign that the team expects clients and servers to drift in version and wants to fail clearly rather than mysteriously. v0.0.3 completes the rebrand from "MemWal" to "Walrus Memory," folding the product formally into the Walrus family.

## Smoothing the login friction (v0.0.4)

v0.0.4 reads like feedback from real users: accept HTTPS dashboard sign-in callbacks to the local listener, and reload credentials after login so the memory tools start working without restarting the whole MCP client. These are the rough edges you only discover once people actually try to log in.

## The leap to proactive memory (v0.0.5)

The most interesting release is the latest. v0.0.5 ships an automatic memory plugin for Claude Code, Codex, Cursor, and Antigravity, plus new `memwal_remember_bulk` and `memwal_health` tools, and, crucially, it changes the posture of the whole product: memory tools become **proactive**, with agents recalling and saving context on their own rather than waiting to be told. The same release candidly documents a packaging bug it fixes: a root gitignore rule had been excluding the plugin's `.mcp.json` from the marketplace bundle, so installs were loading lifecycle hooks but never registering the MCP server. It also adds automatic recovery from dropped relayer connections.

## The arc, in one line

In five quick releases, Walrus Memory MCP moves from "a server you can connect" to "a memory that installs into your agent and manages itself." It is early-stage software wearing its version numbers honestly, but the trajectory, toward proactive, agent-native memory, is unmistakable.
