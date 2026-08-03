> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Walrus Memory is still in beta, so documentation is an active part of product hardening.
If you see unclear guidance, outdated flows, or missing examples, contributions are welcome.

## Source of truth

The docs source of truth is the markdown content under `docs/` in this repository.

## Working rules

- update the docs site and README together when entry points change
- keep old stub pages temporarily when URL changes would otherwise break links
- prefer linking readers into the new IA rather than expanding legacy sections forever

## Before shipping

- run `pnpm dev:docs`
- run `pnpm build:docs`
- click through nav and sidebar links