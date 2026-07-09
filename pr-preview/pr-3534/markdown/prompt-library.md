> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Each Walrus skill teaches your coding agent how to complete one class of task. Each prompt
on this page exercises the core task of one skill. Paste one into your agent after you
install the skills, and the agent loads the matching skill on its own. None of the prompts
name a skill directly, so they also confirm that skill routing works in your setup.

## Install the skills

```sh
npx skills add mystenlabs/walrus-skills --all
```

To install a single skill, replace `--all` with the skill name.

> **Tip**
>
> Review skills before you use them. They run with your agent's full permissions.
## Get started

### Learn what Walrus is and choose a tool

Loads `walrus-overview`.

```text
I have a 200 GB archive of research data and a React dashboard that reads from it.
I've never used Walrus. Explain what it is, and tell me which of the CLI, HTTP API,
TypeScript SDK, or Move should handle each half of this.
```

### Install and configure the CLI

Loads `walrus-cli`.

```text
Install the Walrus CLI on my machine, point it at Testnet, and set up the client config.
Then verify it works by printing the current epoch and storage price.
```

## Store and read blobs

### Store and read over HTTP

Loads `walrus-http-api`.

```text
Store report.pdf on Testnet for 10 epochs as a permanent blob using nothing but curl,
then read it back to out.pdf. Show me how to tell the difference between a first-time
store and a store where the blob already existed.
```

### Store and read from TypeScript

Loads `walrus-ts-sdk`.

```text
Write a TypeScript script that stores a JSON payload on Mainnet for 30 epochs and reads
it back by blob ID. It runs in a Vite browser app with a connected wallet, so configure
whatever the browser needs.
```

### Batch many small files into a quilt

Loads `walrus-quilts`.

```text
I have 400 JSON files averaging 3 KB in ./records/. Store them as one storage unit for
20 epochs, tag each by year, then show me how to pull back just the 2025 ones by identifier.
```

## Manage storage

### Extend, annotate, and share a blob

Loads `walrus-blob-lifecycle`.

```text
I stored a blob 40 epochs ago and it expires in two. Extend it another 20 epochs, set
content-type: application/pdf on it, then convert it to something anyone can top up
when it runs low again.
```

### Estimate storage cost before you spend

Loads `walrus-storage-costs`.

```text
Estimate what it costs to keep 50 GB on Mainnet for a year, in WAL and SUI. Break out
the write fee, the encoding expansion, and the per-blob overhead separately, and check
the estimate against a dry run before I spend anything.
```

## Build on Walrus

### Reference a blob from a Move contract

Loads `walrus-move-integration`.

```text
Set up a new Move package targeting Testnet that depends on Walrus, wrap a Blob in a
custom object with an owner field, and get sui move build and sui move test passing.
```

### Deploy a site

Loads `walrus-sites`.

```text
Deploy my Vite SPA in ./dist to Walrus Sites on Testnet for the maximum storage duration,
make client-side routes like /dashboard resolve on direct navigation, then run a local
portal so I can open the site in a browser.
```

### Give an agent persistent memory

Loads `walrus-memory`.

```text
Give my Vercel AI SDK agent memory that survives restarts. Set up an account and delegate
key, use a work namespace, write facts from a meeting transcript, and query them back.
Confirm the write landed before you read.
```

## Debug

### Diagnose an error

Loads `walrus-troubleshooting`.

```text
My store call fails with RetryableWalrusClientError: Too many failures while writing blob
from a browser app, and sui move build on the same project fails with
VMVerificationOrDeserializationError. Diagnose both and give me the fixes.
```

## Write your own prompts

A prompt that exercises a skill well does three things:

- **Names an outcome, not a topic.** Ask the agent to install the CLI and print the current epoch,
  rather than asking how installation works.
- **Leaves out the skill name.** If you name the skill, you have tested the agent's obedience rather
  than the skill's description.
- **Includes the details the skill cares about.** Naming Testnet, a browser environment,
  or a file count gives the agent the constraints that make it reach for the right guidance.

> **Note**
>
> `walrus-overview` and `walrus-troubleshooting` route rather than execute. Judge them on
> whether the agent picks the right tool or the right cause, not on whether it produces a
> working artifact.