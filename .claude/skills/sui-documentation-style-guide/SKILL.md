---
name: sui-documentation-style-guide
description: "Apply Sui Documentation style guide requirements to all documentation files. Do not edit code snippets within backticks.  When revising existing documentation drafts, explicitly print what revisions were made in order to adhere to this style guide.  When writing new documentation for source code, new features, or tooling, be sure to write the documentation for: * Audience: Broader developer audience. Do not over-define common developer terms like CLI, terminal, SDK, etc. Assume the reader is familiar with the idea of code development, but not necessarily Sui-specific code bases, tooling, or SDKs. * Consider AI ingestion: The documentation page will be ingested by agents and the docs.sui.io custom chatbot. Ensure the content is parseable for agents when exposed as markdown. * Consider character count: Finished pages should be under 50,000 characters. When writing new documentation pages, leave room for human revisions and additions."
---

# Sui Documentation Style Guide

**Critical:** Never edit code, code blocks, inline code, or anything in backticks. Leave all code lines as-is.

## Editorial Principles

- Use plain, direct language. Short sentences. Write for non-native English speakers.
- Do not redefine common words or use jargon/slang/idioms.
- Introduce technical terms only when necessary; define on first use, then use consistently.
- Be explicit: "Deploy the contract" not "do the thing."
- Write for a global audience. Favor clarity over cleverness. Avoid culturally specific references.

## Spelling and Grammar

- **US English** spelling.
- **No Latin abbreviations** (no e.g., i.e., etc., et al.). Use "for example", "and so on", or "ex."
- **Active voice** always. Not passive. Passive voice means the subject receives the action ("the package was published", "the payment is never spent"). Rewrite so the subject performs the action ("you published the package", "the function never spends the payment"). Only flag actual passive constructions. Do not flag or report sentences that are already active voice. If a sentence is already correct, omit it from any audit results entirely rather than listing it as "(already active voice)" or "(skip)". Exception: text inside mermaid diagram nodes, labels, and notes is exempt because diagrams require terse labels.
- **Second person** ("you"). Never first ("I"/"we") or third person.
- **Present tense** always. No future tense for product behavior or instructions.
- **Oxford commas:** Always use serial commas.
- **Numbers:** Use numerals for counts (7 files, 24 items). Write out numbers only when grammatically part of the sentence ("One can always...").
- **No quotation marks** (exception: "Hello, World!"). Use backticks for error messages in headings and prose: `` ### Fails with `InvalidSignature` ``, not `"InvalidSignature"`.
- **No ampersands** in prose. Use "and".
- **No exclamation marks.**
- **No em dashes** in prose. Rewrite using commas, parentheses, or split sentences. Em dashes inside code blocks, inline code, and CLI commands are fine (for example, `--gas-budget`).

### Punctuation Rules

- Period after complete sentences. Single space after periods.
- Lists: period if full sentence; no period if fragment. Don't mix.
- Parentheses: period inside if entire sentence is parenthetical; outside otherwise. Never place a period before the closing parenthesis.
- No periods after headings/titles.
- Use parentheses sparingly for supplemental info. Avoid "(s)" for plurals — just use plural. Use "(s)" only if required for legal, contractual, or regulatory text.

## Terminology and Vocabulary

**Global exception — code and URLs are untouchable:** All terminology, capitalization, spelling, punctuation, and formatting rules in this section apply to **prose only**. Never apply them to code blocks, inline code, CLI commands, command output, anything inside backticks, or URLs. Code must match what the compiler, CLI, or runtime expects. URLs must match the actual address. For example, `devnet`, `mainnet`, `testnet` are correct inside code, CLI flags, and URLs like `https://fullnode.testnet.sui.io` even though prose requires Devnet, Mainnet, Testnet.

### Always Capitalized
Proper nouns, product names, example app names (Coin Flip, Blackjack), Archival Store and Service, Archival Store, Archival Service, Coin Registry, Co-Founder, Currency Standard, DeepBook Indexer, DeepBookV3, DeFi, Devnet, GraphQL RPC, General-purpose Indexer, ID, Localnet, Kiosk (the standard), Mainnet, Mysticeti, One-Time Witness, Operation Cap, Sui, Sui Foundation, Sui CLI, Sui Client PTB CLI, Sui Closed-Loop Token / Closed-Loop Token, Sui dApp Kit, Sui Explorer, SuiJSON, Sui Keystore, Sui Keytool, SuiLink, Sui Object Display, SuiPlay0X1, SUI, Sui Stack, SUI token, Testnet, WAL, Walrus Foundation, Wallet Standard, Web2, Web3, Verified Data Platform, zkSend SDK

### Always Lowercase
casual history, casual order, certificate, epoch, equivocation, eventual consistency, finality, gas, genesis, kiosk (instance), object, oracle, recovery passphrase (mnemonic), smart contract, soulbound, Sui framework, Sui object, total order, transaction, transfer, validator, wallet

### Never Hyphenated
key pair, layer 1, offchain, onchain, open source, use case

### Always Hyphenated
burn-only, depth-first search, multi-writer objects, off-device, on-device, One-Time Witness, peer-to-peer, proof-of-stake, single-writer objects

### Word Preferences
| Instead of | Use |
|---|---|
| may | might |
| "Please note" / "Note" at start of sentence | (remove or rewrite) |
| via | through |
| since (causal) | because |
| simple | basic |
| dApp | app |

**dApp Kit exception:** "dApp Kit" is a product name. Always write "dApp Kit" (not "app Kit", "dapp Kit", "DApp Kit", or "Dapp Kit"). This applies in all contexts: "Sui dApp Kit", "dApp Kit provider", "the dApp Kit hooks", and so on. The `dApp → app` replacement only applies when "dApp" appears as a standalone word meaning "decentralized application", never when it is part of the "dApp Kit" product name.
| Spelled-out data storage units | Abbreviations: 500GB (two letters only, no GiB or TiB) |

### Nodes
- Lowercase "full node" for the conceptual role.
- Capitalize "Sui Full Node" for the official software binary.

### Product Names
- Product names are proper nouns. Capitalize all words. No "the" before product names.
- Specify wallet by name (Slush Wallet, Coinbase Wallet). Use "wallet" generically for the concept.

### Acronyms
- Spell out on first use with acronym in parentheses, then use acronym thereafter.
- Always use as acronyms: CLI, SDK.
- Do not abbreviate words (write "information" not "info").

## Capitalization

- **Page titles:** Title case. Do not capitalize short conjunctions/prepositions (a, an, and, but, for, in, or, so, to, with, yet) unless first/last word. Capitalize verbs including "Is" and "Be". Capitalize after hyphens. Match casing for commands/API elements.
- **Section headings, table cells, list items, captions, alt text, error messages:** Sentence case.
- **Body text:** Capitalize first word of sentences and proper nouns/product names. No ALL CAPS for emphasis (use bold). No bicapitalization unless brand (YouTube, DreamWorks). Don't capitalize spelled-out acronyms unless proper nouns.

## Body Text Styling

- **Bold:** Use for term:definition pairs (bold the term before the colon, only when followed by a colon, not when the term is part of the sentence). Links go in the definition text, not wrapping the bold label: write `- **Term:** [Link text](/path) rest of definition.` not `- **[Term](/path):** Definition.` Use sparingly for emphasis. Bold UI elements (buttons, menus, labels). Bold port references: **port 3000**.
- **Keyboard keys:** Use `<kbd>` tags: `<kbd>Enter</kbd>`.
- **No italic text.** Use the Glossary component for first-time term definitions.
- **No slashes** for "and"/"or". Write "True or False" or "True | False" in code docs.
- **Variables:** Uppercase with underscores for placeholders: `NETWORK_NAME`, `YOUR_API_KEY`. Keep consistent within a page. Don't alternate between `NETWORK` and `NETWORK_NAME` for the same value.

## Frontmatter

- Frontmatter fields (`title`, `description`, `keywords`, `sidebar_label`) must be **plain text only**. Never use markdown links, HTML, or special formatting in frontmatter values. Docusaurus renders frontmatter as React props; markdown links in `description` cause "Function components cannot have string refs" crashes.
- Every prerequisite bullet in a `<Tabs>/<TabItem>` prerequisites block must include an inline link to the corresponding install or setup page. No unlinked prerequisites.

## Titles and Headings

- Use descriptive titles (not just "Overview" or one-word titles). Prefer action-based titles ("Using Packages" not "Package Overview").
- Shorter titles for nav; use `sidebar_label` in frontmatter for different nav title.
- Section headings: sentence case. Acronyms keep their standard capitalization in sentence-case headings (for example, "Using the PTB CLI" not "Using the ptb cli"). Never stack headings without body text between them.
- If something is inline code in body text, keep it as inline code in the heading.
- Do not reuse a page title as a heading on a different page.

### Heading Hierarchy
- `#` (H1): Page title only (set in frontmatter).
- `##` (H2): Top-level sections.
- `###` (H3): Sub-topics. Use for sections with 3+ lines of prose or complex explanations.
- `####` (H4): Short-form content, examples, bullet-point sections.
- `#####` (H5): Step headings in multi-procedure pages. Also usable for styled elements inside blockquotes.

## Lists

- Introduce lists with a description ending in a colon.
- Use lists instead of serial comma sentences with 4+ items.
- Sentence case (unless listing page titles in title case).

### Types
- **Numbered lists:** For sequences. Use `##step` component or H5 headings for steps.
- **Bulleted lists:** For related items. Periods only on full sentences.
- **Term lists:** Bold term, colon, definition. `- **Term:** Definition.`
- **Attribute lists:** Inline code for attribute name (not bolded), colon, description. `- \`id\`: Description.`

## Tables

- Bold labels in header row. Capitalize first word of heading. Follow body text style rules for cell content.

## Code

- **Inline code:** Backticks around object names, function names, file names with extensions, file extensions, CLI tool names, CLI commands in sentences, variable names, file paths. Apply in both body and headings. Do not bold inline code.
- **Console commands:** Triple backticks, start with `$`. Keep commands and output in separate blocks.
- **Codeblocks:** Introduce with descriptive text including file placement context. Use triple backticks with language identifier and `title='filename.ext'`. Follow with explanation.
- **Source from GitHub** when possible using `<ImportContent>` component instead of copying inline.

### `<ImportContent>` Component

Where possible, source code samples directly from their original GitHub repository using the `<ImportContent>` component rather than copying code inline. This ensures samples stay in sync with the source and reduces maintenance burden.

Example usage:
```mdx
<ImportContent source="src/lib/walrus.ts" mode="code" org="MystenLabs" repo="walrus-sdk-relay-example-app" />
```

Use targeting attributes (`fun`, `struct`, `variable`, and so on) to extract a specific code component rather than the entire file.

#### `<ImportContent>` Attributes

| Attribute | Type | Description |
|---|---|---|
| `source` | String | Path to the file within the repository. For `mode="snippet"`, a path under `/snippets`. For `mode="code"`, a repo-relative path. |
| `mode` | `"snippet"` \| `"code"` | Use `code` to render the file as a code block. Use `snippet` to pull from the local snippets directory. |
| `org` | String | The GitHub organization that owns the repository. |
| `repo` | String | The GitHub repository name. |
| `branch` | String | The git branch (or tag) to pull from. Defaults to the repo's default branch. **Do not use `ref` for this attribute.** React treats `ref` as a reserved prop, which causes a "Function components cannot have string refs" crash in strict mode. Always use `branch` instead. |
| `language` | String | Syntax highlighting language for the code block. Only applies in `code` mode. |
| `tag` | String | Targets a specific tagged block using the `docs::` comment format. |
| `fun` | String | Targets a specific function by name. |
| `variable` | String | Targets a specific variable by name. |
| `struct` | String | Targets a specific struct by name. |
| `impl` | String | Targets a specific `impl` block by name. |
| `type` | String | Targets a specific type alias by name. |
| `trait` | String | Targets a specific trait by name. |
| `enumeration` | String | Targets a specific enum by name. |
| `module` | String | Targets a specific module by name. |
| `component` | String | Targets a specific component by name. |
| `dep` | String | Targets a specific dependency block. |
| `test` | String | Targets a specific test block by name. |
| `highlight` | String | Highlights specific lines or ranges in the rendered code block. |
| `signatureOnly` | Boolean | Displays only the function signature rather than the full body. |
| `noComments` | Boolean | Strips all code comments from the output. |
| `noTests` | Boolean | Excludes test blocks from the output. |
| `noTitle` | Boolean | Omits the filename title from the code block header. |
| `style` | String | Applies custom inline styles to the rendered code block container. |

## Procedures and Instructions

- Introduce procedures with an infinitive verb. Format as numbered/ordered lists.
- **Single procedure per page:** Use `##step` component.
- **Multiple procedures per page:** Use H5 headings for each step within each procedure in the format `##### Step X:`
- **Keyboard keys in procedures:** Uppercase, bold: Press **Enter**.
- **UI elements:** Bold, match exact text/capitalization. Omit special characters like ellipses from element labels.

## Prerequisites

### Sui Docs
```mdx
<Tabs className="tabsHeadingCentered--small">
<TabItem value="prereq" label="Prerequisites">
- [x] Prerequisite one
- [x] Prerequisite two
</TabItem>
</Tabs>
```

### Walrus Docs
```mdx
<div className="outlined-tabs">
<Tabs>
<TabItem value="prereq" label="Prerequisites">
- [x] Prerequisite one
- [x] Prerequisite two
</TabItem>
</Tabs>
</div>
```

## Links and References

- Use full relative links for docs.sui.io topics.
- Link text: use target topic title (title case) or descriptive sentence fragment. Never use a bare URL as link text.
- Use keywords from target topic title for inline links.
- Provide URLs only when reader needs to copy them (example code, config files).
- **Crosslink Sui concepts on first mention.** When a Sui-specific concept appears for the first time in body text (programmable transaction blocks, shared objects, gRPC, zkLogin, Walrus, Seal, and so on), link it to its docs page using a relative path like `/develop/transactions/ptbs/prog-txn-blocks`. Do not repeat the link on subsequent mentions.

## Special Components

### Collapsible (`<details><summary>`)
- Use for: large code snippets, verbose output, extended reference content.
- Do not use for: required procedure steps, short examples, critical content.
- Short descriptive summary, sentence case. Do not nest collapsibles.

### Alerts (Admonitions)
All alert content must be complete sentences, sentence case. Do not overuse alerts. Maximum 4 per page.
- **`:::caution`** — Risk of data loss, errors, or breaking changes. Explain the risk.
- **`:::danger`** — Critical/irreversible consequences (permanent data loss, security vulnerabilities).
- **`:::info`** — Important neutral context or conditions.
- **`:::note`** — Avoid; prefer `:::tip` or `:::info` instead.
- **`:::tip`** — Best practices, shortcuts, helpful advice.

## Images and Graphics

- Images supplement text, never replace it.
- Format: `.png` preferred, otherwise `.jpg`. Min 400px wide.
- Alt text describes what the image shows. Caption explains why it matters in context.
- Use Mermaid for flowcharts in Markdown.

## Index Pages

Every sidebar category with `link.type: 'doc'` must have a corresponding index page at all hierarchy levels.

Required format:
```mdx
---
title: Page Title
description: Brief description.
keywords: [ keywords, here ]
pagination_prev: null
---

Brief intro sentence.

import DocCardList from '@theme/DocCardList';

<DocCardList />
```

## Accessibility

- No color or special symbols for emphasis. Use `<strong>` and `<em>`.
- Alt text + captions on all images describing content and context.
- Images never substitute for text content.
