---
title: "Walrus Memory TypeScript SDK: Editorial Release Summary"
description: "An editorial overview of the Walrus Memory TypeScript SDK releases, tracing its security-first foundation, async remember flow, and temporal anchoring."
keywords: [Walrus Memory, TypeScript SDK, release notes, SEAL, nonce, temporal anchoring, recall]
---

# Walrus Memory TypeScript SDK: Security First, Ergonomics Close Behind

_An editorial summary of the Walrus Memory TypeScript SDK releases (v0.0.2, April 2026 to v0.0.7, June 2026)._

The TypeScript SDK is where the Walrus Memory relayer meets application developers, and its release history has a clear personality: it leads with security, then spends the following releases making the resulting API pleasant to use. For a memory product, where the data being stored is by definition personal and persistent, that ordering is the right one.

## A security-first foundation (v0.0.2)

The earliest recorded release is almost entirely about hardening the wire protocol. v0.0.2 adds per-request nonce signing to block replay within the timestamp window, binds the account ID into the canonical signed message so account hints cannot be rebound in transit, and replaces a delegate-key transport with ephemeral SEAL sessions so manual-mode requests stop sending private key material at all. It also draws a hard line: SDK versions that do not send a nonce are simply no longer supported and receive a `426 Upgrade Required`. This is a team willing to break old clients to close a replay vector.

## Rethinking how memory is written (v0.0.3)

v0.0.3 reworks `remember()` around the relayer's asynchronous flow: the call now returns an accepted job immediately, and a family of helpers (`rememberAsync()`, `waitForRememberJob()`, `rememberAndWait()`, and bulk variants) let callers choose between fire-and-forget and waiting for the final blob ID. `analyze()` gets the same async treatment. Importantly, the release is explicit that `recall()` and `restore()` stay wire-compatible, so the new write path does not disturb readers.

## Configurability and clearer errors (v0.0.4 to v0.0.5)

The middle releases add the knobs that serious integrators ask for. v0.0.4 introduces `getRememberStatus(jobId)` for polling the full async state machine, plus manual-mode SEAL committee configuration with validation and sensible threshold caps. v0.0.5 adds explicit compatibility introspection (`compatibility()` and exported types), recall options for `topK`, namespace overrides, and a distance threshold, and prefers Sui gRPC for SEAL sessions with a JSON-RPC fallback. It also makes `401` errors more actionable and completes the MemWal-to-Walrus-Memory rebrand. The theme here is removing guesswork about versions, about failures, and about transport.

## Ergonomics and temporal anchoring (v0.0.6 to v0.0.7)

The two most recent releases polish the developer experience. v0.0.6 introduces an object-style `recall({ query, limit, namespace })` and deprecates the positional form, and documents the pagination and performance behavior of `restore()`. v0.0.7 adds the most conceptually interesting feature in the SDK's short life: optional **temporal anchoring** via `occurredAt` on `analyze()`. Supply a date, or a phrase the server can resolve such as "last Friday," and extracted facts are pinned to when they actually happened before they are embedded and encrypted. The release sweats the details: RFC-3339 UTC on the wire, a clean `TypeError` for malformed dates instead of an opaque one, and byte-identical payloads for callers who do not use the field.

## The arc, in one line

This SDK's releases move from "make it safe" to "make it precise and pleasant": replay-proofing and SEAL sessions first, then async write semantics, configurability, and finally a memory that understands _when_ a fact is true. It is a small library maturing along exactly the axis a memory product should.
