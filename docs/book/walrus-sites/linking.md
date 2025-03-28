# Linking from and to Walrus Sites

``` admonish warning title="Walrus Sites links are currently unavailable"
This feature is currently unavailable on server-side portals. So if you are browsing a Walrus Site
on <https://wal.app>, you will not be able to use Walrus Sites links.

We are working on enabling this feature. Stay tuned!
```

Links in Walrus Sites work *almost* as you would expect in a regular website. We specify here a few
of the details.

## Linking to resources within the same site

Relative and absolute links (`href="/path/to/resource.html"`) work as usual.

## Linking to resources on the web

Linking to a resource on the web (`href="https://some.cdn.example.com/stylesheet.css"`) also works
as usual.

## Linking to resources in other Walrus Sites

Here is the part that is a bit different. Assume there is some image that you can browse at
`https://gallery.wal.app/walrus_arctic.webp`, and you want to link it from your own Walrus Site.

Recall that, however, `https://wal.app` is just one of the possibly many portals. I.e., the same
resource is browsable from a local portal (`http://gallery.localhost:8080/walrus_arctic.webp`).
Therefore, how can you link the resource in a *portal-independent way*? This is important for
interoperability, availability, and respecting the user's choice of portal.

### The solution: Walrus Sites links

``` admonish warning
This feature is only available for service-worker based portals.
```

We solve this problem by having the portals interpret special links that are normally invalid on
the web and redirect to the corresponding Walrus Sites resource in the portal itself.

Consider the example above, where the resource `/walrus_arctic.webp` is browsed from the Walrus Site
with SuiNS name `gallery`, which points to the object ID `abcd123…` (in Base36 encoding). Then,
the portal-independent link is: `https://gallery.suiobj/walrus_arctic.webp`. To fix the object ID
instead of the SuiNS name, you can use `https://abcd123….suiobj/walrus_arctic.webp`.

Another possibility is to directly point to the Walrus *blob ID* of the resource, and have the
browser "sniff" the content type. This works for images, for example, but not for script or
stylesheets. For example to point to the blob ID (e.g., containing an image) `qwer5678…`, use the
URL `https://blobid.walrus/qwer5678…`.

With such a link, the portal will extract the blob ID and redirect the request to the aggregator it
is using to fetch blobs.
