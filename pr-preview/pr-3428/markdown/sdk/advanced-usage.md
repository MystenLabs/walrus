> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

## Use this when

- you already have a vector or encrypted payload
- you want fact extraction with `analyze()`
- you want memory inside an AI SDK pipeline

## Manual registration

Use:

- `rememberManual()` when you already have encrypted payload plus vector
- `recallManual()` when you already have a query vector

## Analyze

Use `analyze()` when you want the relayer to extract facts from longer text and store them as
memories.

## AI middleware

Use `withMemWal` when you want:

- recall before generation
- optional auto-save after generation

## Read next

- [SDK Usage](/walrus-memory/sdk/usage)
- [AI Integration](/walrus-memory/sdk/ai-integration)