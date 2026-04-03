Walrus Sites supports 3 kinds of redirects: internal routing redirects, HTTP redirects, and object redirects. Internal routing redirects map URL paths within your site to specific resources. HTTP redirects return 3xx responses to send the client to a different URL. Object redirects allow arbitrary Sui objects to point to a Walrus Site. These work differently and serve different use cases.

## Internal routing redirects

Define routing rules in your site's [`ws-resources.json` configuration file](/docs/sites/configuration/site-configuration). These rules map incoming URL path patterns to specific resources stored in the site object. The portal applies these rules when resolving requests.

### Defining routes

The `routes` field in `ws-resources.json` maps path patterns to resource paths. Place the `ws-resources.json` file in the root of your site directory before deploying.

```json title='ws-resources.json'
{
  "routes": {
    "/*": "/index.html",
    "/accounts/*": "/accounts.html",
    "/path/assets/*": "/assets/asset_router.html"
  }
}
```

Each key is a path pattern ending in `/*`. The `*` wildcard matches any characters at the end of the path but can only appear at the end. Patterns like `/path/*/to` are not valid.

Each value is the resource path that the portal serves when the pattern matches. The resource must exist in the site object. The Walrus Sites contract aborts if a route points to a nonexistent resource.

### Single-page application routing

The most common use of routes is to support single-page application (SPA) frameworks such as React or Angular, where the frontend router handles all navigation client-side. To catch all paths and serve `index.html`:

```json title='ws-resources.json'
{
  "routes": {
    "/*": "/index.html"
  }
}
```

With this configuration, any path on your site returns `index.html`, and the SPA router in the browser resolves the path from there.

### Path-specific routing

You can define multiple, more specific rules alongside a catch-all. More specific patterns take priority over broader ones:

```json title='ws-resources.json'
{
  "routes": {
    "/app/*": "/app.html",
    "/docs/*": "/docs.html",
    "/*": "/index.html"
  }
}
```

Requests to `/app/anything` serve `app.html`. Requests to `/docs/anything` serve `docs.html`. All other paths serve `index.html`.

:::caution
Route values must point to resources that exist in the site object. Deploying a configuration that references a missing resource causes the transaction to abort.
:::

## HTTP redirects

The `redirects` field in `ws-resources.json` defines server-side HTTP redirect rules for your site. Redirect rules are stored as dynamic fields on the onchain `Site` object. The portal evaluates redirects before fetching resources and serves a 3xx response to the client.

```json title='ws-resources.json'
{
  "redirects": {
    "/old-game": { "location": "/index.html", "status_code": 308 },
    "/walrus-docs": { "location": "https://docs.wal.app", "status_code": 301 },
    "/redirects/**/*": { "location": "/walrus.svg", "status_code": 302 }
  }
}
```

Each key is a URL pattern to match against incoming requests. Each value is an object with the following fields:

- `location`: The destination URL. This can be a path on the same site or a full external URL.
- `status_code`: The HTTP status code to return. Common values are `301` (permanent redirect), `302` (temporary redirect), and `308` (permanent redirect that preserves the HTTP method).

### Pattern matching

Redirect patterns support glob-style matching:

- `*` matches any single path segment
- `**/*` matches any number of path segments

The portal evaluates redirect rules before attempting to fetch a resource. If a request matches a redirect pattern, the portal returns the configured status code and location header without looking up the resource.

### Common redirect configurations

To permanently redirect a renamed page:

```json title='ws-resources.json'
{
  "redirects": {
    "/old-page": { "location": "/new-page", "status_code": 301 }
  }
}
```

To redirect to an external site:

```json title='ws-resources.json'
{
  "redirects": {
    "/external-docs": { "location": "https://docs.example.com", "status_code": 301 }
  }
}
```

To redirect an entire directory:

```json title='ws-resources.json'
{
  "redirects": {
    "/legacy/**/*": { "location": "/index.html", "status_code": 302 }
  }
}
```

## Redirecting Sui objects to a Walrus Site

Any Sui object can point to a Walrus Site through its [`Display`](https://docs.sui.io/standards/display#sui-utility-objects) property. When a portal resolves an object ID that is not itself a Walrus Site, it checks the object's `Display` for the `walrus site address` key. If that key is present, the portal fetches and renders the Walrus Site at the address stored in that key.

This pattern allows you to give each object in a collection its own Walrus Site page without deploying a separate site object for every item. Consider the NFT collection at [flatland.wal.app](https://flatland.wal.app): each minted NFT has its own Walrus Site page, personalized based on the contents of the NFT (for example, its color).

### How object redirects work

When a user browses the object ID of an NFT through a portal, the portal performs the following steps:

1. Fetches the object's onchain data.
2. Reads the object's `Display` fields.
3. Checks for the `walrus site address` key.
4. If the key exists, fetches and serves the Walrus Site at that address.

The URL in the browser retains the NFT's object ID as the subdomain. This means the Walrus Site being served can read its own origin in JavaScript, extract the subdomain (the NFT's object ID), fetch that object's onchain data, and use its properties to personalize the page.

### Setting up the redirect in Move

When creating the `Display` for your object type, include the `walrus site address` key pointing to the object ID of your Walrus Site:

```move
const VISUALIZATION_SITE: address = @0x901fb0...;
 
display.add(b"walrus site address".to_string(), VISUALIZATION_SITE.to_string());
```

Replace `@0x901fb0...` with the actual object ID of the Walrus Site you want to serve for objects of this type.

### Personalizing the page per object

The Walrus Site at `VISUALIZATION_SITE` is a single shared site, but it can render differently for each NFT. Because the portal keeps the NFT's object ID as the subdomain in the URL, the site's JavaScript can check its `origin`, use the subdomain to identify the NFT, fetch it from chain, and use its internal fields to modify what is displayed. To personalize the page:

1. Read `window.location.hostname` to get the subdomain.
2. Decode the Base36 subdomain back to the original hex object ID.
3. Query the Sui RPC for that object's onchain data.
4. Render the page using the object's properties (color, image, name, and so on).

This produces a unique page for every object without requiring a separate Walrus Site deployment per item. For a complete implementation, see the [`flatland` example](https://github.com/MystenLabs/example-walrus-sites/tree/main/flatland).

:::info
The portal follows a configurable maximum redirect depth to prevent infinite redirect chains. Self-hosted portals expose this as the `MAX_REDIRECT_DEPTH` environment variable.
:::