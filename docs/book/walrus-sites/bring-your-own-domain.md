# Bringing your own domain

In the previous section, we learned how to [deploy a Walrus Sites portal](./portal.md). Now,
there might be cases where you want your Walrus Site under a specific domain, without following
the default naming convention of `https://<walrus-site-domain>.<portal-domain>`.

For example, you might want to use a domain like `https://example.com`, instead of
`https://example.wal.app`, where `example.com` is a classic DNS domain that you can purchase
from any domain registrar. It will point to the IP and port of your portal.

Finally, you will need to configure your portal so that it only accepts requests for your own site,
and other subdomains (i.e. other Walrus Sites) will not be served.

The steps you have to follow are:

1. [Deploy a local portal](./portal.md) and include these additional environment variables to
your `.env.local`:

    ```bash
    LANDING_PAGE_OID_B36=34kgqayivjvwuwtk6lckk792g4qhpnimt58eol8i0b2tdgb0y # Example b36 ID
    BRING_YOUR_OWN_DOMAIN=true
    ```

    The `LANDING_PAGE_OID_B36`, should be the b36 object ID of your Walrus Site.

    Set `BRING_YOUR_OWN_DOMAIN` to true to ensure the portal only serves your domain and no other
    sites. i.e. `https://<some-site>.example.com` should throw a 404 error.

1. Configure your DNS provider to point your domain to the IP and port of your portal.

And that's it! You now have a Walrus Site under your own domain.
