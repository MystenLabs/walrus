Walrus Sites serves content stored on Walrus through a [portal](/docs/sites/portals/mainnet-testnet). All relative URLs in your site resolve against the onchain site object. Anything outside that object, such as third-party pages, APIs, CDN assets, other Walrus Sites, requires an absolute URL.

## Linking to external pages
 
Standard HTML `` tags with absolute `https://` URLs work without any special configuration. The portal does not intercept or rewrite absolute URLs in rendered HTML.
 
```html
<a href="https://example.com">Visit Example
```
 
When a user clicks this link, the browser navigates directly to `https://example.com`. The portal is not involved after the initial page render.
 
To open an external link in a new tab:
 
```html

  Open in new tab

```
 
Including `rel="noopener noreferrer"` is a standard security practice when using `target="_blank"`.
 
## Linking to other Walrus Sites
 
Each Walrus Site resolves through a subdomain on a portal host. To link from one Walrus Site to another, use the full subdomain URL.
 
```html
Other site
```
 
You can also link to a site using its Base36-encoded Sui object ID directly through the portal:
 
```html
Site by object ID
```
 
Using Base36 object IDs produces stable links that do not break if the [SuiNS name](/docs/sites/custom-domains/setting-a-suins-name) associated with the site changes.
 
## Loading external resources
 
External assets such as images, fonts, and scripts must use absolute URLs.
 
| Type | Example | Resolves to |
|---|---|---|
| Relative (within site) | `/assets/logo.png` | Blob inside site object |
| Absolute external | `https://cdn.example.com/logo.png` | External host |
| Protocol-relative | `//cdn.example.com/logo.png` | Avoid — behavior depends on portal scheme |
 
Avoid protocol-relative URLs (`//example.com`). Their behavior depends on the scheme of the portal serving the site, which can produce unexpected results.
 
The portal serves pages with standard browser security policies, so cross-origin resource sharing (CORS) restrictions apply. The external host must allow cross-origin requests for assets to load.
 
:::caution
Do not use relative paths to reference off-chain resources. A path like `../external/resource` resolves within the Walrus Site object and returns a 404 if the resource does not exist there.
:::
 
## Common patterns
 
The following examples show typical outbound links from a Walrus Site.
 
### Linking to a GitHub repository
 
```html
Source code
```
 
### Linking to Sui Explorer for an object
 
```html
View on Sui Explorer
```
 
### Loading a font from a CDN
 
```html
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Inter&display=swap">
```
 
### Linking between pages within the same site
 
```html
About
Read post
```
 
Relative links within the same site resolve against the site object and do not require absolute URLs.