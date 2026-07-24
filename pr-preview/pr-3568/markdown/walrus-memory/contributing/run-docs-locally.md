> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Run these commands from the repository root:

[Source: contributing/run-docs-locally.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/contributing/run-docs-locally.md)

```bash
$ npx -p node@20 -c 'node -v'
$ pnpm install
$ pnpm dev:docs
```

This starts the Mintlify site using the docs in this repository.

Use Node 20 LTS for Mintlify local preview. Mintlify fails on Node 25+.

## Build the docs

[Source: contributing/run-docs-locally.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/contributing/run-docs-locally.md)

```bash
$ pnpm build:docs
```