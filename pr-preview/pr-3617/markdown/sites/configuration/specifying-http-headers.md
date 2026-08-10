> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

The `headers` section of [`ws-resources.json`](/docs/sites/configuration/site-configuration) sets custom HTTP response headers for individual resources on your Walrus Site. Each key in the `headers` object is the exact path of a resource, always starting from the root `/` (no wildcards), and each value is an object that maps header names to the values the portal attaches to the response.

Custom headers let you control how browsers and other clients handle each resource, for example caching, encoding, content types, and download behavior.

```json
{
  "headers": {
    "/index.html": {
      "Content-Type": "text/html; charset=utf-8",
      "Cache-Control": "max-age=3500"
    },
    "/assets/index.a1b2c3d4.js": {
      "Content-Type": "application/javascript; charset=utf-8",
      "Cache-Control": "max-age=31536000, immutable"
    },
    "/assets/index.e5f6a7b8.css": {
      "Content-Type": "text/css; charset=utf-8",
      "Cache-Control": "max-age=31536000, immutable"
    },
    "/downloads/report.pdf": {
      "Content-Type": "application/pdf",
      "Content-Disposition": "attachment; filename=\"report.pdf\""
    }
  }
}
```

In this example, the portal serves `index.html` with the `Content-Type` header set to `text/html; charset=utf-8` and the `Cache-Control` header set to `max-age=3500`.

## How the portal applies headers

Headers live onchain, not in a server configuration file:

1. You define the headers in `ws-resources.json`.
2. When you run [`site-builder deploy`](/docs/sites/getting-started/using-the-site-builder), the tool writes the headers into your site's resource entries on Sui, alongside each resource path and blob ID.
3. When a visitor requests a resource, the [portal](/docs/sites/portals/mainnet-testnet) reads the headers from the Sui object and attaches them to the HTTP response.

Because the headers live on Sui, editing `ws-resources.json` alone changes nothing on your live site. Run `site-builder deploy` again to apply the new headers.

## Defaults

By default, you do not need to specify any headers. The `site-builder` automatically tries to infer the `Content-Type` header based on the file extension, and sets the `Content-Encoding` to `identity` (no transformation).

If the `site-builder` cannot infer the content type, it sets the `Content-Type` to `application/octet-stream`. Headers you specify in the `ws-resources.json` file override these defaults.

## `Content-Type`

Set the `Content-Type` explicitly when the inferred type is incorrect, when you need to specify a charset, or when you serve a file type the `site-builder` does not recognize.

```json
"/feed.xml":      { "Content-Type": "application/rss+xml; charset=utf-8" }
"/app.wasm":      { "Content-Type": "application/wasm" }
"/fonts/x.woff2": { "Content-Type": "font/woff2" }
"/data.csv":      { "Content-Type": "text/csv; charset=utf-8" }
```

For [raw markdown files served for LLM ingestion](/docs/sites/configuration/site-configuration#markdown), use lowercase `content-type`.

## `Cache-Control`

Walrus blobs are immutable, so browsers can safely cache assets with build-hashed filenames forever. Do not let browsers cache entry points such as `/index.html`, because their content changes on every deployment while their paths stay the same.

```json
"/index.html":              { "Cache-Control": "no-cache" }
"/assets/app.a1b2c3d4.js":  { "Cache-Control": "max-age=31536000, immutable" }
"/assets/style.e5f6a7b8.css": { "Cache-Control": "max-age=31536000, immutable" }
"/data/prices.json":        { "Cache-Control": "max-age=300" }
```

| **Value** | **Meaning** |
|---|---|
| `no-cache` | Revalidate before each use |
| `no-store` | Never cache |
| `max-age=31536000, immutable` | Cache for 1 year (content never changes) |
| `max-age=3600` | Cache for 1 hour |

## `Content-Disposition`

The `Content-Disposition` header controls whether the browser renders a resource inline or downloads it as a file.

```json
"/docs/guide.md":    { "Content-Disposition": "inline" }
"/whitepaper.pdf":   { "Content-Disposition": "inline" }
"/exports/data.csv": { "Content-Disposition": "attachment; filename=\"data-export.csv\"" }
```

## `Content-Encoding`

Set `Content-Encoding` when you serve pre-compressed assets from your build pipeline, so browsers decompress them correctly.

```json
"/assets/app.js.gz": {
  "Content-Type": "application/javascript; charset=utf-8",
  "Content-Encoding": "gzip"
},
"/assets/styles.css.br": {
  "Content-Type": "text/css; charset=utf-8",
  "Content-Encoding": "br"
}
```