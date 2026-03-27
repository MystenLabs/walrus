Walrus Sites supports 2 kinds of redirects: internal routing redirects, which map URL paths within your site to specific resources, and object redirects, which allow arbitrary Sui objects to point to a Walrus Site. These work differently and serve different use cases.

## Internal routing redirects

Walrus Sites serves static files directly. There is no server-side logic to intercept requests and resolve routes at runtime. Because of this, dynamic redirects do not work on Walrus Sites. Framework redirect plugins, such as the Docusaurus redirect plugin, do not function as expected when deployed to a Walrus Site.

To simulate redirect behavior, you define routing rules in your site's [`ws-resources.json` configuration file](/docs/sites/configuration/site-configuration). These rules map incoming URL path patterns to specific resources stored in the site object. The portal applies these rules when resolving requests.

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

Each value is the resource path that the portal serves when the pattern matches. The resource must exist in the site object. The Walrus Sites contract aborts if a route points to a non-existing resource.

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

Requests to `/app/anything` are served `app.html`. Requests to `/docs/anything` are served `docs.html`. All other paths are served `index.html`.

:::caution
Route values must point to resources that exist in the site object. Deploying a configuration that references a missing resource causes the transaction to abort.
:::

### What routing redirects cannot do

Because routes are resolved by the portal serving static files, they do not issue HTTP redirect responses with status codes like 301 or 302. The portal serves the mapped resource directly at the requested path, without changing the URL in the browser.

Framework redirect plugins that generate redirect files at build time also do not work, because Walrus Sites has no server-side logic to process them at request time.

### Implementing path redirects in Docusaurus
 
[Docusaurus](https://docusaurus.io/) is an open source static site generator built on React, commonly used for documentation sites (including the [Walrus Docs](https://docs.wal.app/)). It has a built-in redirect plugin that generates HTML stub files at old paths, each containing a server-side or meta-refresh redirect to the new path. Because Walrus Sites has no server-side logic, this plugin does not function: the stub files are generated correctly at build time but the portal cannot honor the redirect instructions they rely on.
 
To redirect an old path to a new one in Docusaurus, create a placeholder `.mdx` file at the old path instead. The file uses the Docusaurus `<Redirect>` component to perform a client-side redirect when the page loads.
 
For example, to redirect `/docs/file/path/1` to `/docs/new/file/path/2`, create the file `docs/file/path/1.mdx` with the following content:
 
```mdx title='docs/file/path/1.mdx'
---
title: Redirecting...
sidebar_class_name: hidden
---
 

```
 
The `sidebar_class_name: hidden` frontmatter field hides the placeholder page from the sidebar. The `<Redirect>` component navigates the user to the new path as soon as the browser renders the page.
 
### Other static site generators
 
Whether you need a similar workaround depends on how your static site generator handles redirects. Generators that implement redirects by writing stub HTML files that rely on server-side processing — such as HTTP 301 or 302 responses from a web server — require a client-side alternative when deployed to Walrus Sites.
 
Generators that implement redirects entirely in the browser, such as through a framework router component like the Docusaurus `<Redirect>` approach above, work without modification.
 
If you are unsure whether your generator's redirect mechanism is compatible, check whether its output requires a server or `.htaccess` file to function. If it does, you need a client-side workaround. Common options are a meta-refresh tag or a small JavaScript snippet in a placeholder HTML file at the old path:
 
```html title='old-path/index.html'
<!DOCTYPE html>

  <head>
    
    <script>window.location.replace("/new/path/");</script>
  </head>

```
 
Place this file at the path that should redirect, and include it in your site directory before deploying.
 
## Redirecting Sui objects to a Walrus Site
 
Any Sui object can be made to point to a Walrus Site through its [`Display`](https://docs.sui.io/standards/display#sui-utility-objects) property. When a portal resolves an object ID that is not itself a Walrus Site, it checks the object's Display for the `walrus site address` key. If that key is present, the portal fetches and renders the Walrus Site at the address stored in that key.
 
This pattern allows you to give each object in a collection its own Walrus Site page without deploying a separate site object for every item. Consider the NFT collection at [flatland.wal.app](https://flatland.wal.app): each minted NFT has its own Walrus Site page, personalized based on the contents of the NFT — for example, its color.
 
### How it works
 
When a user browses the object ID of an NFT through a portal, the portal:
 
1. Fetches the object's onchain data.
2. Reads the object's Display fields.
3. Checks for the `walrus site address` key.
4. If found, fetches and serves the Walrus Site at that address.
 
The URL in the browser retains the NFT's object ID as the subdomain. This means the Walrus Site being served can read its own origin in JavaScript, extract the subdomain (the NFT's object ID), fetch that object's onchain data, and use its properties to personalize the page.
 
### Setting up the redirect in Move
 
When creating the Display for your object type, include the `walrus site address` key pointing to the object ID of your Walrus Site:
 
```move
const VISUALIZATION_SITE: address = @0x901fb0...;
 
display.add(b"walrus site address".to_string(), VISUALIZATION_SITE.to_string());
```
 
Replace `@0x901fb0...` with the actual object ID of the Walrus Site you want to serve for objects of this type.
 
### Personalizing the page per object
 
The Walrus Site at `VISUALIZATION_SITE` is a single shared site, but it can render differently for each NFT. Because the portal keeps the NFT's object ID as the subdomain in the URL, the site's JavaScript can check its `origin`, use the subdomain to identify the NFT, fetch it from chain, and use its internal fields to modify what is displayed. The steps are:
 
1. Read `window.location.hostname` to get the subdomain.
2. Decode the Base36 subdomain back to the original hex object ID.
3. Query the Sui RPC for that object's onchain data.
4. Render the page using the object's properties (color, image, name, and so on).
 
This produces a unique page for every object without requiring a separate Walrus Site deployment per item. For a complete implementation, see the [`flatland` example](https://github.com/MystenLabs/example-walrus-sites/tree/main/flatland).
 
:::info
The portal follows a configurable maximum redirect depth to prevent infinite redirect chains. Self-hosted portals expose this as the `MAX_REDIRECT_DEPTH` environment variable.
:::