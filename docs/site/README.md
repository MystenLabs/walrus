# Website

The Docusaurus project for [docs.wal.app](https://docs.wal.app/). See the
[documentation README](../README.md) for local development, the build pipeline, and deployment.

## Interactive diagram fallback assets

Interactive diagrams live in `static/diagrams/` and follow the interactive diagrams standard: every
diagram ships as three artifacts in the same directory, sharing one base name.

1. `interactive_<topic>_v<n>.html`: the self-contained interactive HTML.
2. `interactive_<topic>_v<n>.svg`: the static fallback SVG source.
3. `interactive_<topic>_v<n>.png`: the fallback PNG export.

The fallback assets carry accessibility: they are what a screen reader, a print export, and a
reduced-motion reader depend on, so they are required, not optional.

CI enforces this. The `Check interactive diagram fallback assets` job in
`.github/workflows/lint.yml` runs `src/scripts/check-diagram-fallbacks.mjs` against the files a PR
adds or changes, and fails when an `interactive_*.html` file lacks its sibling `.svg` and `.png`.
Diagrams that predate the standard only start failing once a PR touches them; when you modify one,
add its fallback assets in the same PR.

To audit every diagram in the repository locally, run:

```bash
node src/scripts/check-diagram-fallbacks.mjs --all
```
