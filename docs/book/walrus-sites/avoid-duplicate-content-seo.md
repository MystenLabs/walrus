# Avoiding Duplicate-Content SEO in Custom Portals

When you deploy your own Walrus Sites portal, you may expose the same site under multiple hostnames:

> **Note:** This guidance does not require any changes to the Walrus Sites portal itself.
> It is focused on **site-level fixes** (HTML tags or HTTP headers) that developers can
> add when hosting their own portals.

- **[SuiNS](https://suins.io/)** – human-readable names registered on-chain, e.g. `snowreads.wal.app`.
- **Base36** – subdomains derived directly from the site object ID, e.g. `1myb…xd.portal.com`.
- **[BYOD](https://docs.wal.app/walrus-sites/bring-your-own-domain.html)** – Bring Your Own Domain,
  e.g. `example.com`.

Search engines treat the same content served at multiple hosts as duplicates,
which can dilute ranking signals.

The official [`wal.app`](https://wal.app) portal avoids this issue by **only serving SuiNS domains**,
with base36 and BYOD disabled.
If you run your own portal and enable base36 or BYOD, you must take steps to signal a canonical host.

---

## Recommended Practices

- **Canonical host policy**
  Choose one hostname type per site (BYOD > SuiNS > base36 is a good priority).
  All other hosts should include canonical hints pointing to that choice.

- **Base36 subdomains**

  - Disable if not needed: `B36_DOMAIN_RESOLUTION_SUPPORT=false`.
  - If enabled, add either:

    - An HTML `<link rel="canonical">` in your pages, or
    - An HTTP header:

      ```http
      Link: <https://example.com/page>; rel="canonical"
      ```

  - Alternatively, block them from indexing with:

    ```http
    X-Robots-Tag: noindex
    ```

- **BYOD domains**

  - Enable with care: `BRING_YOUR_OWN_DOMAIN=true`.
  - Decide if BYOD or SuiNS is canonical, and ensure non-canonical hosts send canonical hints or `noindex`.

- **Multiple SuiNS names → one site**
  This can happen permissionlessly. If you own the extra names, add canonical hints to consolidate them.
  Otherwise, add a `<link rel="canonical">` in the HTML to indicate your preferred host.

---

## Example Scenario

**Problem:**
You run a site that is reachable at both:

- `https://example.wal.app/math` (SuiNS)
- `https://example.com/math` (BYOD)

A web crawler will see identical content at two different URLs and may treat them as duplicates.

**Solution:**
Add a canonical hint so crawlers know which host to index:

In the HTML `<head>`:

```html
<link rel="canonical" href="https://example.com/math" />
```
