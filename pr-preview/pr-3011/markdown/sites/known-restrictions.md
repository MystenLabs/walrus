{/* https://linear.app/mysten-labs/issue/DOCS-686/sitesknown-restrictions */}

Walrus Sites can be used to deploy almost any form of traditional static Web2 website built for modern browsers. There are, however, a number of restrictions that a developer should keep in mind when creating or porting a website to Walrus Sites.

## No secret values

Walrus Sites are fully publicly accessible, as the metadata is stored on Sui, and the site content is stored on Walrus. Therefore, developers **must not** store secret values within the sites.

Any such backend-specific operations, like storing secret values, authentication, and so on, are achievable by leveraging the integration with the Sui blockchain and a Sui-compatible wallet.

## Maximum redirect depth

The number of consecutive redirects a Walrus Site can perform is capped by the [portal](/docs/sites/portals/deploy-locally). This measure ensures that loading a Walrus Site does not result in an infinite loading loop.

Different portals can set this limit as they desire. The limit for the portal hosted at https://wal.app has a maximum redirect depth of 3.

## Special `__wal__` path

Anything under the `/__wal__/*` path is reserved by the portal for health checks and other portal-specific operations. If you use this path inside your site, the resources served under that path are not loaded correctly.

## Service-worker portal limitations

The following limitations only apply to portals based on service-workers. If you need to support any of the features listed below, you should use a server-side portal.

### Service-worker portals cannot serve sites based on service-workers

Service-worker portals leverage service-workers in the clients' browsers to perform essential operations:

- Reading the site metadata from Sui

- Fetching the page content from Walrus

- Serving the content to the browser

Therefore, a site accessed by a service-worker portal cannot use service-workers itself. That is, you can't "stack" service-workers. Installing a service-worker from within a Walrus Site results in a dysfunctional site and a poor experience for the user.

### iOS Sui mobile wallets do not work with the service-worker portal

Service workers cannot be loaded inside an in-app browser on iOS because of a limitation of the WebKit engine. As a consequence, Walrus Sites that are accessed through a service-worker portal cannot be used within Sui-compatible wallet apps on iOS. Therefore, Sui wallets cannot currently be used on a service-worker portal on iOS. However, browsing a Walrus Site is still possible on iOS through any browser.

Given that you decided to use a service-worker portal as your main point of access to your sites, to provide a seamless experience for iOS users and other users on browsers that do not support service-workers, it is recommended to redirect to a server-side portal (https://wal.app). Whenever a user on an iOS wallet browses a Walrus Site, the redirect automatically takes them to the `<site_name>.wal.app` server-side portal. This way, the user can still use the wallet.

### Service worker portals do not support progressive web apps (PWAs)

With the current design, service-worker portals cannot be used to access progressive web apps (PWAs). Two characteristics of the service-worker portal prevent support for PWAs:

- Since the service-worker needs to be registered for the page to work, the PWA's manifest file cannot be loaded by the browser directly.

- There can only be 1 service-worker registered per origin. Therefore, registering a PWA's service-worker would remove the Walrus Sites service-worker, breaking Walrus Sites' functionality.

The server-side portal does not share these limitations. However, for the moment, both technologies are supported. Walrus Sites must be able to load from both a service-worker portal and a server-side portal, and therefore have to be built with the more restrictive feature set. For more details, see the [installation requirements for PWAs](https://en.wikipedia.org/wiki/Progressive_web_app#Installation_criteria).