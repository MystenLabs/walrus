The optional `routes` section allows you to specify client-side routing rules for your site. This is useful when you want to use a single-page application (SPA) framework, such as React or Angular. The configuration in the `routes` object is a mapping from route keys to resource path values.

```json
{
  "routes": {
    "/*": "/index.html",
    "/accounts/*": "/accounts.html",
    "/admin/settings/*": "/admin.html"
  }
}
```

## Syntax

**Keys:**
- Must start with `/`
- `*` wildcard is trailing only
- More specific patterns win over less specific ones

**Values:**
- Must start with `/`
- Must be a real uploaded file. Deployment aborts if the target doesn't exist
- No wildcards

## Matching

The simple routing algorithm is as follows:

- Whenever a resource path is not found among the sites resources, the portal tries to match the path to the `routes`.

- All matching routes are then lexicographically ordered, and the longest match is chosen.

- The resource corresponding to this longest match is then served.

In the example [`ws-resources.json`](/docs/sites/configuration/site-configuration):

```json
"routes": {
  "/*": "/index.html",
  "/path/*": "/accounts.html",
  "/path/assets/*": "/assets/asset_router.html"
}
```

| Incoming URL | Served file | Reason |
|---|---|---|
| `/any/other/test.html` | `/index.html` | only `/*` matches |
| `/path/test.html` | `/accounts.html` | `/path/*` is a longer match than `/*` |
| `/path/assets/test.html` | `/assets/asset_router.html` | `/path/assets/*` is the longest match |

`/index.html`, `/accounts.html`, and `/assets/asset_router.html` are all existing resources on the Walrus Sites object on Sui.

## Common configurations

**SPA catch-all** (React, Vue history mode, Angular, SvelteKit):
```json
{ "routes": { "/*": "/index.html" } }
```

**Multi-section SPA** (separate entry points per section):
```json
{
  "routes": {
    "/*": "/index.html",
    "/admin/*": "/admin.html",
    "/docs/*": "/docs.html"
  }
}
```

**Static multi-page site:** Omit `routes` entirely. Each URL resolves directly to its file.

**Vue Router in hash mode:** Omit `routes` entirely. The hash fragment is handled client-side.

## Redirects

Walrus Sites serves static files directly. There is no server-side logic to handle URL resolution at runtime. This means web framework routing plugins do not work on Walrus Sites. For example, if you are using Docusaurus, its routing plugin will not function as expected when deployed to Walrus Sites.

To simulate redirect behavior, workaround approaches require creating a resource file at the old path that performs the redirect client-side, then adding a route rule so the old URL pattern is served that file.

### Workarounds

Since Walrus Sites has no native redirect support, redirects must be implemented at the framework level. The approach varies by framework.

#### React Router

Create a redirect component at the old path and add it to your router:

```jsx
// src/pages/OldPage.jsx

export default function OldPage() {
  return ;
}
```

Then register the old path in your router:

```jsx
} />
```

#### Vue Router

Add a redirect entry directly in your route definitions:

```js
const routes = [
  { path: '/old/path', redirect: '/new/path' },
  // ...other routes
]
```

#### SvelteKit

Create a `+page.js` at the old route that redirects client-side:

```js
// src/routes/old/path/+page.js

export function load() {
  redirect(301, '/new/path');
}
```

#### Docusaurus

Create a placeholder `.mdx` file at the old path:

```mdx
---
title: Redirecting...
sidebar_class_name: hidden
---

```

### Adding routes 

Placeholder redirect pages created this way will appear in your site's build output. Make sure you also add a corresponding route entry in `ws-resources.json` so the old URL serves the redirect file:

```json
{
  "routes": {
    "/docs/old/path": "/docs/old/path/index.html"
    "/docs/old/path.html": "/docs/old/path/index.html"
  }
}
```

Each key is a URL path a visitor might request, and each value is the static file that should be served for that path. You need an entry for every URL you want to support, including both `.html` variants and clean path variants (without the extension).