# Website

This website is built using [Docusaurus](https://docusaurus.io/), a modern static website generator.

## Installation

```bash
yarn
```

## Local Development

```bash
yarn start
```

This command starts a local development server and opens up a browser window. Most changes are reflected live without having to restart the server.

## Build

```bash
yarn build
```

This command generates static content into the `build` directory and can be served using any static contents hosting service.

## Deployment

Using SSH:

```bash
USE_SSH=true yarn deploy
```

Not using SSH:

```bash
GIT_USER=<Your GitHub username> yarn deploy
```

If you are using GitHub pages for hosting, this command is a convenient way to build the website and push to the `gh-pages` branch.

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

### Generating the fallbacks

`src/scripts/generate-diagram-fallbacks.mjs` produces both assets from the interactive HTML, so you
do not export them by hand. Each diagram renders its content as a single inline SVG that already
carries `xmlns`, a `viewBox`, `role="img"`, a `<title>`, and a `<desc>`, and styles itself with
presentation attributes rather than document CSS, so the fallback serializes what the reader sees
rather than re-drawing it.

Playwright is not a dependency of this package, because the generation step runs rarely. Install it
for the run:

```bash
npm install --no-save playwright-core
node src/scripts/generate-diagram-fallbacks.mjs --all
```

Pass `--missing-only` to skip diagrams that already have both assets, and `--scale <n>` to change
the PNG device scale factor, which defaults to 2.

Expect it to take a while. Several diagrams animate a walkthrough that reveals elements in sequence
over roughly 24 seconds and then loops, so a fixed wait captures an arbitrary frame with parts of
the diagram still faded out. The generator polls until nothing sits below full opacity and captures
that frame instead, and fails the diagram rather than freezing a partial walkthrough into its
fallback. It also refuses to write anything for a page that threw while rendering, or whose SVG has
no `<title>`.

### Fallback quality

Presence alone proves little. A diagram passes the gate above with a blank SVG and a one-pixel PNG,
which satisfies a presence check without giving a reader anything.
`src/scripts/eval-diagram-quality.mjs` evaluates the fallbacks that do exist:

| **Dimension** | **What it catches** |
| --- | --- |
| Substance | Placeholder assets: an SVG under 1 KiB, an SVG with no drawing elements, a PNG under 4 KiB |
| Accessibility | An SVG with no `<title>`, which announces nothing to a screen reader |
| Fidelity | A fallback whose last commit predates the diagram it stands in for, so it shows stale content while reporting as compliant |
| Weight | Interactive HTML over 2 MiB, which a reader waits on |

```bash
node src/scripts/eval-diagram-quality.mjs --all
```

Findings do not fail by default, because teams switch off a gate nobody can pass. Pass `--strict`
to exit non-zero once coverage arrives, and `--json` for machine-readable output. The script
reports a diagram missing its fallbacks as unevaluated rather than failed, so it and the presence
gate never disagree about the same file.

### Scheduled health report

`.github/workflows/diagram-health.yml` runs both scripts across the whole repository every Monday,
and on demand through **Run workflow**. It writes the results to the job summary and never fails,
because the lint gate only checks diffs and would otherwise hide the true coverage number until
someone happened to touch a diagram.
