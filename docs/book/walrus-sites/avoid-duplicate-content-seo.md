# Avoiding Duplicate-Content SEO in Custom Portals

When you deploy your own Walrus Sites portal, you may expose the same site under multiple hostnames
(e.g., **SuiNS**, **base36**, or **BYOD**). Search engines treat this as duplicate content,
which can dilute ranking signals.

The official `wal.app` portal avoids this by serving only **SuiNS domains** with base36 and BYOD disabled.
If you enable base36 or BYOD on your own portal, you should enforce canonicalization.

## Recommended Practices

- **Canonical host policy**

  - Choose one canonical type per site (e.g., BYOD > SuiNS > base36).
  - Redirect all other hosts to the canonical one.

- **Base36 subdomains**

  - Disable if not needed: `B36_DOMAIN_RESOLUTION_SUPPORT=false`.
  - If enabled, redirect (`301`) base36 → canonical host, or add `X-Robots-Tag: noindex` to base36 responses.

- **BYOD domains**

  - Enable with care: `BRING_YOUR_OWN_DOMAIN=true`.
  - Decide if BYOD or SuiNS is canonical and redirect the rest.
  - If redirect is not possible, inject canonical hints (headers or `<link rel="canonical">`).

- **Implementation points**

  - Add redirects or headers in `portal/server/src/main.ts` after host parsing.
  - Example redirect:

    ```ts
    return new Response(null, {
      status: 301,
      headers: { Location: canonicalUrl },
    });
    ```

  - Example noindex header:

    ```ts
    return new Response(body, {
      headers: {
        ...existingHeaders,
        "X-Robots-Tag": "noindex",
      },
    });
    ```

- **Multiple SuiNS names → one site**
  - This can happen permissionlessly. If you control the extra names, redirect them to your canonical.
    Otherwise, add a `<link rel="canonical">` in the site HTML as a best-effort signal.
